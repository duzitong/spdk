/*-
 *   BSD LICENSE
 *
 *   Copyright (c) Intel Corporation.
 *   All rights reserved.
 *   Copyright (c) 2021 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 *
 *   Redistribution and use in source and binary forms, with or without
 *   modification, are permitted provided that the following conditions
 *   are met:
 *
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in
 *       the documentation and/or other materials provided with the
 *       distribution.
 *     * Neither the name of Intel Corporation nor the names of its
 *       contributors may be used to endorse or promote products derived
 *       from this software without specific prior written permission.
 *
 *   THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 *   "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 *   LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 *   A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 *   OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 *   SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 *   LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 *   DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 *   THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 *   (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 *   OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

#include "spdk/stdinc.h"

#include "bdev_persist.h"
#include "spdk/bdev.h"
#include "spdk/endian.h"
#include "spdk/env.h"
#include "spdk/nvme.h"
#include "spdk/nvme_ocssd.h"
#include "spdk/nvme_zns.h"
#include "spdk/accel_engine.h"
#include "spdk/json.h"
#include "spdk/thread.h"
#include "spdk/queue.h"
#include "spdk/string.h"
#include "spdk/trace.h"
#include "spdk/rdma_connection.h"
#include "spdk/likely.h"
#include "spdk/memory.h"

#include "spdk/bdev_module.h"
#include "spdk/log.h"
#include <infiniband/verbs.h>
#include <rdma/rdma_cma.h>
#include <errno.h>

// TODO: make it configurable
#define PERSIST_MEM_BLOCK_SIZE 512
#define PERSIST_MEM_BLOCK_CNT (20LL * 1024 * 1024)
// TODO: make it configurable
#define LOG_BLOCKCNT 1310720
#define LOG_BLOCKSIZE 512

struct persist_destage_context {
	int remaining;
};

enum persist_pmem_status {
	PERSIST_PMEM_NORMAL,
	PERSIST_PMEM_NEED_INITIALIZE,
	PERSIST_PMEM_ERROR,
	PERSIST_PMEM_IN_CATCHUP,
};

struct persist_peer_memcpy_job {
	uint64_t start_offset;
	uint64_t end_offset;
	bool ongoing;
};

struct persist_disk {
	struct spdk_bdev		disk;
	// act as circular buffer
	void *malloc_buf;
	struct spdk_poller* destage_poller;
	struct spdk_poller* rdma_poller;
	struct spdk_poller* nvme_poller;
	struct spdk_poller* cq_poller;
	struct spdk_nvme_ctrlr* ctrlr;
	struct spdk_nvme_ns* ns;
	struct spdk_nvme_qpair* qpair;
	struct destage_info* destage_tail;
	struct destage_info* commit_tail;
	// struct destage_info recover_tail;
	struct persist_destage_context destage_context;
	uint64_t prev_seq;
	enum persist_pmem_status pmem_status;
	struct rdma_connection* peer_conns[RPC_MAX_PEERS];
	size_t num_peers;
	struct rdma_connection* client_conn;
	struct persist_peer_memcpy_job memcpy_job;
	size_t pmem_total_size;
	
	// if false, then all nvme-related fields are null.
	bool attach_disk;
	void* disk_buf;
	// uint32_t io_queue_head;
	// uint32_t io_queue_size;
	// uint64_t* io_queue_offset;

	TAILQ_ENTRY(persist_disk)	link;
};

struct persist_rdma_context {
	struct persist_disk* pdisk;
};

struct persist_io {
	/** array of iovecs to transfer. */
	struct iovec *iovs;

	/** Number of iovecs in iovs array. */
	int iovcnt;

	/** Current iovec position. */
	int iovpos;

	/** Offset in current iovec. */
	uint32_t iov_offset;
};

struct persist_channel {
	struct persist_disk* pdisk;
};


int persist_disk_count = 0;

static int bdev_persist_initialize(void);
static void bdev_persist_deinitialize(void);

static int
bdev_persist_get_ctx_size(void)
{
	return sizeof(struct persist_io);
}

static struct spdk_bdev_module persist_if = {
	.name = "persist",
	.module_init = bdev_persist_initialize,
	.module_fini = bdev_persist_deinitialize,
	.get_ctx_size = bdev_persist_get_ctx_size,
};

SPDK_BDEV_MODULE_REGISTER(persist, &persist_if)

static void
persist_disk_free(struct persist_disk *persist_disk)
{
	if (!persist_disk) {
		return;
	}

	if (persist_disk->rdma_poller) {
		spdk_poller_unregister(&persist_disk->rdma_poller);
	}
	if (persist_disk->destage_poller) {
		spdk_poller_unregister(&persist_disk->destage_poller);
	}
	if (persist_disk->nvme_poller) {
		spdk_poller_unregister(&persist_disk->nvme_poller);
	}
	// TODO: find a way to detach the controller. right now the call seems to hang forever
	// spdk_nvme_detach(persist_disk->ctrlr);

	free(persist_disk->disk.name);
	spdk_free(persist_disk->malloc_buf);
	free(persist_disk);
}

static int
bdev_persist_destruct(void *ctx)
{
	struct persist_disk *persist_disk = ctx;

	// TAILQ_REMOVE(&g_persist_disks, persist_disk, link);
	persist_disk_free(persist_disk);
	spdk_io_device_unregister(persist_disk, NULL);
	return 0;
}

static int
bdev_persist_check_iov_len(struct iovec *iovs, int iovcnt, size_t nbytes)
{
	int i;

	for (i = 0; i < iovcnt; i++) {
		if (nbytes < iovs[i].iov_len) {
			return 0;
		}

		nbytes -= iovs[i].iov_len;
	}

	return nbytes != 0;
}

static void
bdev_persist_reset_sgl(void *ref, uint32_t sgl_offset) {
	struct persist_io *pio = ref;
	struct iovec *iov;

	pio->iov_offset = sgl_offset;
	for (pio->iovpos = 0; pio->iovpos < pio->iovcnt; pio->iovpos++) {
		iov = &pio->iovs[pio->iovpos];
		if (pio->iov_offset < iov->iov_len) {
			break;
		}

		pio->iov_offset -= iov->iov_len;
	}
}

static int
bdev_persist_next_sge(void *ref, void **address, uint32_t *length) {
	struct persist_io *pio = ref;
	struct iovec *iov;

	assert(pio->iovpos < pio->iovcnt);

	iov = &pio->iovs[pio->iovpos];

	*address = iov->iov_base;
	*length = iov->iov_len;

	if (pio->iov_offset) {
		assert(pio->iov_offset <= iov->iov_len);
		*address += pio->iov_offset;
		*length -= pio->iov_offset;
	}

	pio->iov_offset += *length;
	if (pio->iov_offset == iov->iov_len) {
		pio->iovpos++;
		pio->iov_offset = 0;
	}

	return 0;
}

static void bdev_persist_read_done(void *ref, const struct spdk_nvme_cpl *cpl) {
	struct persist_io *pio = ref;
	struct spdk_bdev_io *bdev_io = spdk_bdev_io_from_ctx(pio);
	if (spdk_likely(spdk_nvme_cpl_is_success(cpl))) {
		// SPDK_NOTICELOG("read successful\n");
		spdk_bdev_io_complete(bdev_io, SPDK_BDEV_IO_STATUS_SUCCESS);
	}
	else {
		// TODO: if the read failed?
		SPDK_ERRLOG("persist bdev failed to read from disk\n");
		spdk_bdev_io_complete(bdev_io, SPDK_BDEV_IO_STATUS_FAILED);
	}
}

static void bdev_persist_destage_done(void *ref, const struct spdk_nvme_cpl *cpl) {
	if (spdk_unlikely(spdk_nvme_cpl_is_error(cpl))) {
		// TODO: check which kind of error and recover/retry
		SPDK_ERRLOG("Write failed: %p %p\n", ref, cpl);
	}
	struct persist_disk* pdisk = ref;
	pdisk->destage_context.remaining--;
}

static void
bdev_persist_readv(struct persist_disk *pdisk, 
		  struct persist_io *pio,
		  struct iovec *iov, int iovcnt, size_t lba_count, uint64_t lba, uint64_t block_size)
{
	if (bdev_persist_check_iov_len(iov, iovcnt, lba_count * block_size)) {
		spdk_bdev_io_complete(spdk_bdev_io_from_ctx(pio),
				      SPDK_BDEV_IO_STATUS_FAILED);
		return;
	}

	if (pdisk->attach_disk) {
		pio->iovs = iov;
		pio->iovcnt = iovcnt;
		pio->iovpos = 0;
		pio->iov_offset = 0;
		int rc;

		rc = spdk_nvme_ns_cmd_readv(pdisk->ns,
			pdisk->qpair,
			lba,
			lba_count,
			bdev_persist_read_done,
			pio,
			0,
			bdev_persist_reset_sgl,
			bdev_persist_next_sge
			);

		if (spdk_unlikely(rc != 0)) {
			SPDK_ERRLOG("read io failed: %d\n", rc);
			// spdk_bdev_io_complete(spdk_bdev_io_from_ctx(pio), SPDK_BDEV_IO_STATUS_FAILED);
			return;
		}
	}
	else {
		uint64_t bytes_read = 0;
		void* start = pdisk->disk_buf + lba * pdisk->disk.blocklen;
		for (int i = 0; i < iovcnt; i++) {
			memcpy(iov[i].iov_base, start, iov[i].iov_len);
			start += iov[i].iov_len;
		}
		spdk_bdev_io_complete(spdk_bdev_io_from_ctx(pio), SPDK_BDEV_IO_STATUS_SUCCESS);
	}
}

static int _bdev_persist_submit_request(struct spdk_bdev_io *bdev_io)
{
	uint32_t block_size = bdev_io->bdev->blocklen;
	// _log_md(bdev_io);

	if (bdev_io->u.bdev.iovs[0].iov_base == NULL) {
		SPDK_ERRLOG("Received read req where iov_base is null\n");
		return -1;
	}

	switch (bdev_io->type) {
	case SPDK_BDEV_IO_TYPE_READ:
		bdev_persist_readv((struct persist_disk *)bdev_io->bdev->ctxt,
				  (struct persist_io *)bdev_io->driver_ctx,
				  bdev_io->u.bdev.iovs,
				  bdev_io->u.bdev.iovcnt,
				  bdev_io->u.bdev.num_blocks,
				  bdev_io->u.bdev.offset_blocks,
				  block_size);
		return 0;
	default:
		SPDK_NOTICELOG("Req type %d not supported\n", bdev_io->type);
		return -1;
	}
	return 0;
}

static void bdev_persist_submit_request(struct spdk_io_channel *ch, struct spdk_bdev_io *bdev_io)
{
	if (_bdev_persist_submit_request(bdev_io) != 0) {
		spdk_bdev_io_complete(bdev_io,
				     SPDK_BDEV_IO_STATUS_FAILED);
	}
}

static bool
bdev_persist_io_type_supported(void *ctx, enum spdk_bdev_io_type io_type)
{
	switch (io_type) {
	case SPDK_BDEV_IO_TYPE_READ:
		return true;
	default:
		return false;
	}
}

static struct spdk_io_channel *
bdev_persist_get_io_channel(void *ctx)
{
	return spdk_get_io_channel(ctx);
}

static void
bdev_persist_write_json_config(struct spdk_bdev *bdev, struct spdk_json_write_ctx *w)
{
	char uuid_str[SPDK_UUID_STRING_LEN];

	spdk_json_write_object_begin(w);

	spdk_json_write_named_string(w, "method", "bdev_persist_create");

	spdk_json_write_named_object_begin(w, "params");
	spdk_json_write_named_string(w, "name", bdev->name);
	spdk_uuid_fmt_lower(uuid_str, sizeof(uuid_str), &bdev->uuid);
	spdk_json_write_named_string(w, "uuid", uuid_str);

	spdk_json_write_object_end(w);

	spdk_json_write_object_end(w);
}

static const struct spdk_bdev_fn_table persist_fn_table = {
	.destruct		= bdev_persist_destruct,
	.submit_request		= bdev_persist_submit_request,
	.io_type_supported	= bdev_persist_io_type_supported,
	.get_io_channel		= bdev_persist_get_io_channel,
	.write_config_json	= bdev_persist_write_json_config,
};

static void persist_rdma_context_created_cb(void* context, void* arg) {
	struct persist_rdma_context* p_context = context;
	p_context->pdisk = arg;
}

static void persist_rdma_cli_connected_cb(struct rdma_connection* rdma_conn) {
	// TODO
	struct persist_rdma_context* context = rdma_conn->rdma_context;
	struct rdma_handshake* remote_handshake = rdma_conn->handshake_buf + 1;
	if (remote_handshake->is_reconnected) {
		SPDK_NOTICELOG("Reconnected to client. Trigger reconnect routine\n");
		context->pdisk->pmem_status = PERSIST_PMEM_NEED_INITIALIZE;
	}
}

static void persist_rdma_peer_connected_cb(struct rdma_connection* rdma_conn) {
	// TODO
}

static void persist_rdma_disconnect_cb(struct rdma_connection* rdma_conn) {
	// TODO
}

static bool
probe_cb(void *cb_ctx, const struct spdk_nvme_transport_id *trid,
	 struct spdk_nvme_ctrlr_opts *opts)
{
	SPDK_DEBUGLOG(bdev_persist, "Probing %s\n", trid->traddr);

	return true;
}

/*
 * Callback when a nvme controller is returned.
 * Find the namespace of the controller.
 */
static void
attach_cb(void *cb_ctx, const struct spdk_nvme_transport_id *trid,
	  struct spdk_nvme_ctrlr *ctrlr, const struct spdk_nvme_ctrlr_opts *opts)
{
	int nsid;
	struct spdk_nvme_ns *ns;
	struct persist_disk* disk = (struct persist_disk*)cb_ctx;

	SPDK_DEBUGLOG(bdev_persist, "Attached to %s\n", trid->traddr);
	SPDK_NOTICELOG("IO queue = %d, IO request = %d\n",
		opts->io_queue_size,
		opts->io_queue_requests);
	disk->ctrlr = ctrlr;

	/*
	 * Each controller has one or more namespaces.  An NVMe namespace is basically
	 *  equivalent to a SCSI LUN.  The controller's IDENTIFY data tells us how
	 *  many namespaces exist on the controller.  For Intel(R) P3X00 controllers,
	 *  it will just be one namespace.
	 *
	 * Note that in NVMe, namespace IDs start at 1, not 0.
	 */
	int num_ns = 0;
	for (nsid = spdk_nvme_ctrlr_get_first_active_ns(ctrlr); nsid != 0;
	     nsid = spdk_nvme_ctrlr_get_next_active_ns(ctrlr, nsid)) {
		ns = spdk_nvme_ctrlr_get_ns(ctrlr, nsid);
		if (ns == NULL) {
			continue;
		}
		SPDK_NOTICELOG("Namespace ID: %d size: %juGB\n", spdk_nvme_ns_get_id(ns),
	       spdk_nvme_ns_get_size(ns) / 1000000000);
		disk->ns = ns;
		disk->disk.blocklen = spdk_nvme_ns_get_sector_size(ns);
		disk->disk.blockcnt = spdk_nvme_ns_get_num_sectors(ns);

		if (disk->disk.blocklen != spdk_nvme_ns_get_extended_sector_size(ns)) {
			SPDK_NOTICELOG("disk sector size mismatch with extended sector size\n");
		}
		num_ns++;
	}

	if (num_ns != 1) {
		SPDK_ERRLOG("Unexpected # of namespaces %d\n", num_ns);
	}
}

static int
persist_nvme_poller(void* ctx) {
	struct persist_disk* pdisk = ctx;
	int rc = spdk_nvme_qpair_process_completions(pdisk->qpair, 0);
	if (rc < 0) {
		SPDK_ERRLOG("Poll nvme failed\n");
	}

	return rc == 0 ? SPDK_POLLER_IDLE : SPDK_POLLER_BUSY;
}

// only poll from peer conn[0] right now
static int persist_rdma_cq_poller(void* ctx) {
    struct ibv_wc wc_buf[PERSIST_WC_BATCH_SIZE];
	struct persist_disk* pdisk = ctx;
	struct rdma_connection* rdma_conn = pdisk->peer_conns[0];
	if (!rdma_connection_is_connected(rdma_conn)) {
		return SPDK_POLLER_IDLE;
	}

	pthread_rwlock_rdlock(&rdma_conn->lock);
	int cnt = ibv_poll_cq(rdma_conn->cq, PERSIST_WC_BATCH_SIZE, wc_buf);
	pthread_rwlock_unlock(&rdma_conn->lock);
	if (cnt < 0) {
		SPDK_ERRLOG("Fail to poll ibv_cq\n");
	}
	for (int i = 0; i < cnt; i++) {
		if (wc_buf[i].wr_id == (uint64_t)&pdisk->memcpy_job) {
			SPDK_NOTICELOG("Finish catchup from %ld to %ld\n",
				pdisk->memcpy_job.start_offset,
				pdisk->memcpy_job.end_offset);
			pdisk->pmem_status = PERSIST_PMEM_NORMAL;
			pdisk->memcpy_job.ongoing = false;
			return SPDK_POLLER_BUSY;
		}
		else {
			SPDK_ERRLOG("Unknown wr_id %ld\n", wc_buf[i].wr_id);
		}
	}
	return SPDK_POLLER_IDLE;
}

static int persist_peer_memcpy(int peer_id, struct persist_disk* pdisk, uint64_t start_offset, uint64_t end_offset) {
	if (start_offset == end_offset) {
		return 0;
	}

	if (pdisk->memcpy_job.ongoing) {
		// wait for previous one to complete.
		return 0;
	}

	pdisk->memcpy_job.ongoing = true;
	pdisk->memcpy_job.start_offset = start_offset;
	pdisk->memcpy_job.end_offset = end_offset;

	struct ibv_send_wr read_wr, *bad_read_wr = NULL;
	struct ibv_sge read_sge;
	memset(&read_wr, 0, sizeof(read_wr));
	struct rdma_handshake* remote_handshake = pdisk->peer_conns[peer_id]->handshake_buf + 1;

	// currently only initializing needs signal. Be lazy about wr_id.
	read_wr.wr_id = (uint64_t)&pdisk->memcpy_job;
	read_wr.send_flags = IBV_SEND_SIGNALED;
	read_wr.opcode = IBV_WR_RDMA_READ;
	read_wr.sg_list = &read_sge;
	read_wr.num_sge = 1;
	read_wr.wr.rdma.remote_addr = (uint64_t)remote_handshake->base_addr + start_offset * LOG_BLOCKSIZE;
	read_wr.wr.rdma.rkey = remote_handshake->rkey;

	pthread_rwlock_rdlock(&pdisk->peer_conns[peer_id]->lock);
	rdma_connection_construct_sge(pdisk->peer_conns[peer_id],
		&read_sge,
		pdisk->malloc_buf + start_offset * LOG_BLOCKSIZE,
		(end_offset - start_offset) * LOG_BLOCKSIZE);

	int rc = ibv_post_send(pdisk->peer_conns[peer_id]->cm_id->qp, &read_wr, &bad_read_wr);
	if (rc != 0) {
		SPDK_ERRLOG("post send failed: %d\n", rc);
	}
	pthread_rwlock_unlock(&pdisk->peer_conns[peer_id]->lock);

	return rc;
}

static int
persist_destage_poller(void *ctx)
{
	struct persist_disk* pdisk = ctx;

	struct destage_info old_info = {
		.offset = pdisk->destage_tail->offset,
		.round = pdisk->destage_tail->round
	};

	// ignore all the info on-the-fly
	struct destage_info commit_tail_copy = {
		.offset = pdisk->commit_tail->offset,
		.round = pdisk->commit_tail->round
	};

	if (pdisk->pmem_status == PERSIST_PMEM_NEED_INITIALIZE) {
		if (!rdma_connection_is_connected(pdisk->peer_conns[0])) {
			return SPDK_POLLER_IDLE;
		}

		SPDK_NOTICELOG("Memcpy blocks from 0 to %ld\n", pdisk->pmem_total_size / LOG_BLOCKSIZE);
		persist_peer_memcpy(0, pdisk, 0, pdisk->pmem_total_size / LOG_BLOCKSIZE);
		pdisk->pmem_status = PERSIST_PMEM_IN_CATCHUP;
	}

	if (pdisk->pmem_status != PERSIST_PMEM_NORMAL) {
		return SPDK_POLLER_IDLE;
	}

	if (commit_tail_copy.round < pdisk->destage_tail->round ||
		(commit_tail_copy.offset < pdisk->destage_tail->offset && commit_tail_copy.round == pdisk->destage_tail->round)) {
		SPDK_NOTICELOG("Reading part of commit tail. Abort this cycle\n");
		return SPDK_POLLER_IDLE;
	}

	int rc;

	if (!rdma_connection_is_connected(pdisk->client_conn)) {
		// need to establish RDMA connection first
		return SPDK_POLLER_IDLE;
	}

	// reset the counter
	pdisk->destage_context.remaining = 0;

	while (true) {
		if (pdisk->destage_tail->offset == commit_tail_copy.offset && 
			pdisk->destage_tail->round == commit_tail_copy.round) {
			// Done.
			break;
		}

		struct wals_metadata* metadata = pdisk->malloc_buf + pdisk->destage_tail->offset * pdisk->disk.blocklen;
		// if we get unlucky (lucky?), then the next block may be the one for the last
		// round.

		if (metadata->version != PERSIST_METADATA_VERSION
			|| metadata->seq < pdisk->prev_seq) {
			struct wals_metadata* next_round_metadata = pdisk->malloc_buf;
			if (next_round_metadata->round == pdisk->destage_tail->round) {
				// head not in next round yet
				// it means that no IOs have arrived
				// do nothing and wait for the next IO
				break;
			}
			else if (next_round_metadata->round == pdisk->destage_tail->round + 1) {
				metadata = next_round_metadata;
				if (metadata->version != PERSIST_METADATA_VERSION) {
					// should not happen even before any IO comes, because of the round
					SPDK_ERRLOG("Buffer head corrupted\n");
					break;
				}

				if (metadata->seq != pdisk->prev_seq + 1) {
					// wait for previous IO that is still in the last round
					if (metadata->seq < pdisk->prev_seq + 1) {
						SPDK_ERRLOG("should not happen\n");
					}
					break;
				}
				SPDK_NOTICELOG("Round advance to %ld\n", pdisk->destage_tail->round);
				pdisk->destage_tail->offset = 0;
				pdisk->destage_tail->round++;
			}
			else {
				// should not happen
				// TODO: error handling
				SPDK_ERRLOG("Next round %ld is not expected when this round is %ld\n",
					next_round_metadata->round,
					pdisk->destage_tail->round);
				break;
			}
		}
		SPDK_NOTICELOG("Getting md %ld %ld %ld %ld %ld %ld %ld\n",
			metadata->version,
			metadata->seq,
			metadata->next_offset,
			metadata->round,
			metadata->length,
			metadata->core_offset,
			metadata->md_blocknum);

		void* payload = (void*)(metadata) + metadata->md_blocknum * pdisk->disk.blocklen;

		if (metadata->seq != pdisk->prev_seq + 1) {
			// TODO: what to do about it?
			SPDK_NOTICELOG("Reconnection or data loss! Previous seq is %ld while the current is %ld. Offset: (%ld, %ld, %ld)\n",
				pdisk->prev_seq,
				metadata->seq,
				pdisk->destage_tail->offset,
				metadata->next_offset,
				pdisk->destage_tail->round);
			
			// ignore the error for now, otherwise the error prints forever
			pdisk->prev_seq = metadata->seq - 1;
		}

		if (metadata->next_offset != pdisk->destage_tail->offset + metadata->md_blocknum + metadata->length) {
			SPDK_NOTICELOG("next offset mismatch: %ld != %ld + %ld + %ld\n",
				metadata->next_offset,
				pdisk->destage_tail->offset,
				metadata->md_blocknum,
				metadata->length);
		}

		// TODO: CRC check
		size_t md_size = offsetof(struct wals_metadata, md_checksum);
		SPDK_NOTICELOG("client checksum: %ld \n, server checksum: %ld", metadata->md_checksum, wals_bdev_calc_crc(metadata, md_size));
		if (wals_bdev_calc_crc(metadata, md_size) != metadata->md_checksum) {
			SPDK_ERRLOG("ERROR: CRC check failed");
		}

		if (pdisk->attach_disk) {
			rc = spdk_nvme_ns_cmd_write(pdisk->ns,
				pdisk->qpair,
				payload,
				metadata->core_offset,
				metadata->length,
				bdev_persist_destage_done,
				pdisk,
				0);
			
			if (rc != 0) {
				// TODO: what to do when writing SSD fails?
				if (spdk_unlikely(rc != -ENOMEM)) {
					// we expect ENOMEM, as it always happens when client submit 
					// too many requests.
					SPDK_ERRLOG("Write SSD failed with rc = %d\n", rc);
				}
				break;
			}

			pdisk->destage_context.remaining++;
		}
		else {
			memcpy(pdisk->disk_buf + metadata->core_offset * pdisk->disk.blocklen, payload, metadata->length * pdisk->disk.blocklen);
		}
		// only updating head pointer
		// as round only changes when going back to the start of the array
		pdisk->destage_tail->offset = metadata->next_offset;
		pdisk->prev_seq++;
	}

	if (pdisk->attach_disk) {
		// wait for every IO to complete
		while (pdisk->destage_context.remaining != 0) {
			// note that it may also complete some read requests, but we don't care.
			rc = spdk_nvme_qpair_process_completions(pdisk->qpair, 0);
			if (rc < 0) {
				SPDK_ERRLOG("qpair failed %d\n", rc);
				break;
			}
		}
	}

	if (!(pdisk->destage_tail->offset == commit_tail_copy.offset && 
		pdisk->destage_tail->round == commit_tail_copy.round)) {
		SPDK_NOTICELOG("Need to catchup from (%ld, %ld) to (%ld, %ld)\n",
			pdisk->destage_tail->offset, 
			pdisk->destage_tail->round, 
			commit_tail_copy.offset,
			commit_tail_copy.round
			);

		if (pdisk->num_peers == 0) {
			SPDK_ERRLOG("Cannot catchup because there are no peers\n");
			pdisk->pmem_status = PERSIST_PMEM_ERROR;
			return SPDK_POLLER_IDLE;
		}

		if (!rdma_connection_is_connected(pdisk->peer_conns[0])) {
			// wait for peer connection
			return SPDK_POLLER_IDLE;
		}

		// if (destage_info_gt(&pdisk->recover_tail, pdisk->destage_tail)) {
		// 	// still recovering from previous catchup result
		// 	return SPDK_POLLER_BUSY;
		// }

		// need catchup from destage_tail to commit_tail
		if (pdisk->destage_tail->offset < commit_tail_copy.offset) {
			// case 1: no need to wrap around
			if (pdisk->destage_tail->round != commit_tail_copy.round) {
				// still continue
				SPDK_ERRLOG("Unexpected destage tail (%ld, %ld) and commit tail (%ld, %ld)\n",
					pdisk->destage_tail->offset, pdisk->destage_tail->round,
					commit_tail_copy.offset, commit_tail_copy.round);
			}
			persist_peer_memcpy(0, pdisk, pdisk->destage_tail->offset, commit_tail_copy.offset);
		}
		else {
			// case 2: need to wrap around
			if (pdisk->destage_tail->round != commit_tail_copy.round - 1) {
				// still continue
				SPDK_ERRLOG("Unexpected destage tail (%ld, %ld) and commit tail (%ld, %ld)\n",
					pdisk->destage_tail->offset, pdisk->destage_tail->round,
					commit_tail_copy.offset, commit_tail_copy.round);
			}
			persist_peer_memcpy(0, pdisk, pdisk->destage_tail->offset, LOG_BLOCKCNT);
			persist_peer_memcpy(0, pdisk, 0, commit_tail_copy.offset);
		}

		// Never look back.
		// TODO: what will happen with peer memcpy fail? (low prob)
		// pdisk->recover_tail.offset = commit_tail_copy.offset;
		// pdisk->recover_tail.round = commit_tail_copy.round;
	}
	// else {
	// 	pdisk->recover_tail.offset = pdisk->destage_tail->offset;
	// 	pdisk->recover_tail.round = pdisk->destage_tail->round;
	// }

	return SPDK_POLLER_BUSY;
}

static int persist_catchup_poller(void* ctx) {

}

// static int persist_rdma_poller(void* ctx) {
// 	struct persist_disk* pdisk = ctx;

// 	rdma_connection_connect(pdisk->client_conn);

// 	// update_persist_rdma_connection(&pdisk->client_conn, pdisk);
// 	for (size_t i = 0; i < pdisk->num_peers; i++) {
// 		rdma_connection_connect(pdisk->peer_conns[i]);
// 		// update_persist_rdma_connection(pdisk->peer_conns + i, pdisk);
// 	}

// 	return SPDK_POLLER_BUSY;
// }

static int
persist_create_channel_cb(void *io_device, void *ctx)
{
	SPDK_NOTICELOG("enter create channel\n");
	struct persist_disk* pdisk = io_device;
	struct persist_channel *ch = ctx;
	ch->pdisk = pdisk;


	return 0;
}

static void
persist_destroy_channel_cb(void *io_device, void *ctx)
{
	SPDK_NOTICELOG("enter destroy\n");

	// spdk_poller_unregister(&pdisk->destage_poller);
	// spdk_poller_unregister(&pdisk->rdma_poller);
}


int
create_persist_disk(struct spdk_bdev **bdev, const char *name, const char* ip, const char* port,
			const struct spdk_uuid *uuid, bool attach_disk, struct rpc_persist_peer_info* peer_info_array,
			size_t num_peers)
{
	SPDK_DEBUGLOG(bdev_persist, "in create disk\n");
	struct persist_disk	*pdisk;
	int rc;

	pdisk = calloc(1, sizeof(*pdisk));
	pdisk->attach_disk = attach_disk;
	if (!pdisk) {
		SPDK_ERRLOG("pdisk calloc() failed\n");
		return -ENOMEM;
	}

	if (attach_disk) {
		/*
		* Attach a nvme controller locally
		*/
		struct spdk_nvme_transport_id trid = {};
		spdk_nvme_trid_populate_transport(&trid, SPDK_NVME_TRANSPORT_PCIE);
		snprintf(trid.subnqn, sizeof(trid.subnqn), "%s", SPDK_NVMF_DISCOVERY_NQN);
		SPDK_DEBUGLOG(bdev_persist, "before probe\n");

		rc = spdk_nvme_probe(&trid, pdisk, probe_cb, attach_cb, NULL);
		if (rc != 0) {
			SPDK_ERRLOG("spdk_nvme_probe() failed");
			return rc;
		}

		pdisk->qpair = spdk_nvme_ctrlr_alloc_io_qpair(pdisk->ctrlr, NULL, 0);
		if (pdisk->qpair == NULL) {
			SPDK_ERRLOG("ERROR: spdk_nvme_ctrlr_alloc_io_qpair() failed");
			return -EINVAL;
		}
		SPDK_NOTICELOG("alloc nvme qp successful\n");
	}
	else {
		pdisk->disk.blockcnt = PERSIST_MEM_BLOCK_CNT;
		pdisk->disk.blocklen = PERSIST_MEM_BLOCK_SIZE;
		pdisk->disk_buf = spdk_zmalloc(pdisk->disk.blockcnt * pdisk->disk.blocklen, 
			2 * 1024 * 1024,
			NULL,
			SPDK_ENV_LCORE_ID_ANY,
			SPDK_MALLOC_DMA);
		if (pdisk->disk_buf == NULL) {
			SPDK_ERRLOG("Failed to alloc disk buffer in mem mode\n");
			return -EINVAL;
		}
	}

	size_t log_size = LOG_BLOCKCNT * LOG_BLOCKSIZE;
	size_t total_size = LOG_BLOCKCNT * LOG_BLOCKSIZE + VALUE_2MB;

	if (total_size % VALUE_2MB != 0) {
		SPDK_ERRLOG("Log buffer size is not multiple of %lld\n", VALUE_2MB);
		return -EINVAL;
	}

	pdisk->pmem_total_size = total_size;

	// one more block for current destage position
	// TODO: another block for commit tail
	// NOTE: the buffer length must not change when reconnecting
	pdisk->malloc_buf = spdk_zmalloc(total_size,
		2 * 1024 * 1024,
		NULL,
		SPDK_ENV_LCORE_ID_ANY,
		SPDK_MALLOC_DMA);
	pdisk->destage_tail = pdisk->malloc_buf + log_size;
	pdisk->commit_tail = pdisk->destage_tail + 1;

	if (2 * sizeof(struct destage_info) > LOG_BLOCKSIZE) {
		SPDK_ERRLOG("One more block is not enough\n");
		return -EINVAL;
	}

	if (name) {
		pdisk->disk.name = strdup(name);
	} else {
		/* Auto-generate a name */
		pdisk->disk.name = spdk_sprintf_alloc("Persist%d", persist_disk_count);
		persist_disk_count++;
	}
	if (!pdisk->disk.name) {
		persist_disk_free(pdisk);
		return -ENOMEM;
	}
	pdisk->disk.product_name = "Persist disk";

	pdisk->disk.write_cache = 1;

	if (uuid) {
		pdisk->disk.uuid = *uuid;
	} else {
		spdk_uuid_generate(&pdisk->disk.uuid);
	}

	pdisk->client_conn = rdma_connection_alloc(true,
		ip,
		port,
		sizeof(struct persist_rdma_context),
		pdisk->malloc_buf,
		LOG_BLOCKSIZE,
	   	LOG_BLOCKCNT,
		persist_rdma_context_created_cb,
		pdisk,
		persist_rdma_cli_connected_cb,
		persist_rdma_disconnect_cb,
		false);

	SPDK_NOTICELOG("num_peers = %ld\n", num_peers);

	pdisk->num_peers = num_peers;
	for (size_t i = 0; i < num_peers; i++) {
		// the smaller ip one is the server
		if (strcmp(ip, peer_info_array[i].remote_ip) < 0
			|| (strcmp(ip, peer_info_array[i].remote_ip) == 0 && strcmp(port, peer_info_array[i].remote_port) < 0)) {
			
			pdisk->peer_conns[i] = rdma_connection_alloc(true,
				ip,
				peer_info_array[i].local_port,
				sizeof(struct persist_rdma_context),
				pdisk->malloc_buf,
				LOG_BLOCKSIZE,
				LOG_BLOCKCNT,
				persist_rdma_context_created_cb,
				pdisk,
				persist_rdma_peer_connected_cb,
				persist_rdma_disconnect_cb,
				false);
		}
		else {
			pdisk->peer_conns[i] = rdma_connection_alloc(false,
				peer_info_array[i].remote_ip,
				peer_info_array[i].remote_port,
				sizeof(struct persist_rdma_context),
				pdisk->malloc_buf,
				LOG_BLOCKSIZE,
				LOG_BLOCKCNT,
				persist_rdma_context_created_cb,
				pdisk,
				persist_rdma_peer_connected_cb,
				persist_rdma_disconnect_cb,
				false);
		}
	}

	pdisk->disk.ctxt = pdisk;
	pdisk->disk.fn_table = &persist_fn_table;
	pdisk->disk.module = &persist_if;

	*bdev = &(pdisk->disk);

	/* This needs to be reset for each reinitialization of submodules.
	 * Otherwise after enough devices or reinitializations the value gets too high.
	 * TODO: Make malloc bdev name mandatory and remove this counter. */
	persist_disk_count = 0;

	spdk_io_device_register(pdisk, persist_create_channel_cb,
				persist_destroy_channel_cb, sizeof(struct persist_channel),
				"bdev_persist");

	// pdisk->rdma_poller = SPDK_POLLER_REGISTER(persist_rdma_poller, pdisk, 5 * 1000);
	// if (!pdisk->rdma_poller) {
	// 	SPDK_ERRLOG("Failed to register persist rdma poller\n");
	// 	return -ENOMEM;
	// }

	pdisk->cq_poller = SPDK_POLLER_REGISTER(persist_rdma_cq_poller, pdisk, 5 * 1000);
	if (!pdisk->cq_poller) {
		SPDK_ERRLOG("Failed to register persist rdma poller\n");
		return -ENOMEM;
	}

	if (attach_disk) {
		pdisk->nvme_poller = SPDK_POLLER_REGISTER(persist_nvme_poller, pdisk, 5);
		if (!pdisk->nvme_poller) {
			SPDK_ERRLOG("Failed to register persist nvme poller\n");
			return -ENOMEM;
		}
	}

	pdisk->destage_poller = SPDK_POLLER_REGISTER(persist_destage_poller, pdisk, 0);
	if (!pdisk->destage_poller) {
		SPDK_ERRLOG("Failed to register persist destage poller\n");
		return -ENOMEM;
	}

	rc = spdk_bdev_register(&pdisk->disk);
	if (rc) {
		persist_disk_free(pdisk);
		return rc;
	}

	SPDK_NOTICELOG("finish creating disk\n");

	return rc;
}

void
delete_persist_disk(const char *name, spdk_delete_persist_complete cb_fn, void *cb_arg)
{
	int rc;

	rc = spdk_bdev_unregister_by_name(name, &persist_if, cb_fn, cb_arg);
	if (rc != 0) {
		cb_fn(cb_arg, rc);
	}
}

static int bdev_persist_initialize(void)
{
	return 0;
}

static void
bdev_persist_deinitialize(void)
{
	// spdk_io_device_unregister(&g_persist_disks, NULL);
}

SPDK_LOG_REGISTER_COMPONENT(bdev_persist)
