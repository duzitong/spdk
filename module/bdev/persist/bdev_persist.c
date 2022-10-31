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
#include "spdk/rdma.h"
#include "spdk/likely.h"

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

enum persist_disk_status {
	PERSIST_DISK_NORMAL,
	PERSIST_DISK_IN_RECOVERY,
};

enum rdma_status {
	RDMA_SERVER_INITIALIZED,
	RDMA_SERVER_LISTENING,
	RDMA_SERVER_ACCEPTED,
	RDMA_SERVER_ESTABLISHED,
	RDMA_SERVER_CONNECTED,
	RDMA_SERVER_ERROR,

	RDMA_CLI_INITIALIZED,
	RDMA_CLI_ADDR_RESOLVING,
	RDMA_CLI_ROUTE_RESOLVING,
	RDMA_CLI_CONNECTING,
	RDMA_CLI_ESTABLISHED,
	RDMA_CLI_CONNECTED,
	RDMA_CLI_ERROR,
};

struct persist_rdma_connection {
	// one node acts as recover server,
	// the other one acts as the client.
	// both should support reconnection.
	bool is_server;
	struct rdma_event_channel* channel;
	// if it is server, then it is local address
	// otherwise it is the server address
	struct addrinfo* server_addr;
	struct rdma_cm_id* cm_id;
	struct rdma_cm_id* parent_cm_id;
	struct ibv_mr* mr;
	struct ibv_mr* mr_handshake;
	struct ibv_cq* cq;
	struct rdma_handshake* handshake_buf;
	enum rdma_status status;
	// only used in client
	uint64_t reject_cnt;
};

struct persist_disk {
	struct spdk_bdev		disk;
	// act as circular buffer
	void *malloc_buf;
	struct spdk_poller* destage_poller;
	struct spdk_poller* rdma_poller;
	struct spdk_poller* nvme_poller;
	struct ibv_wc wc_buf[PERSIST_WC_BATCH_SIZE];
	struct spdk_nvme_ctrlr* ctrlr;
	struct spdk_nvme_ns* ns;
	struct spdk_nvme_qpair* qpair;
	struct destage_info* destage_info;
	struct persist_destage_context destage_context;
	uint64_t prev_seq;
	enum persist_disk_status disk_status;
	struct persist_rdma_connection peer_conns[RPC_MAX_PEERS];
	size_t num_peers;
	struct persist_rdma_connection client_conn;
	
	// if false, then all nvme-related fields are null.
	bool attach_disk;
	void* disk_buf;
	// uint32_t io_queue_head;
	// uint32_t io_queue_size;
	// uint64_t* io_queue_offset;

	TAILQ_ENTRY(persist_disk)	link;
};

// struct wal_metadata {
// 	uint64_t	version;
	
// 	uint64_t	seq;

// 	uint64_t	next_offset;

// 	uint64_t	length;

// 	uint64_t	core_offset;

// 	uint64_t	core_length;

// 	uint64_t	round;
// };

struct wals_metadata {
	uint64_t	version;
	
	uint64_t	seq;

	uint64_t	next_offset;

	uint64_t	length;

	uint64_t	core_offset;

	uint64_t	round;
};

static struct destage_info {
	uint64_t destage_head;
	uint64_t destage_round;
	uint32_t checksum;
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

static int
persist_destage_poller(void *ctx)
{
	struct persist_disk* pdisk = ctx;
	struct destage_info old_info = {
		.destage_head = pdisk->destage_info->destage_head,
		.destage_round = pdisk->destage_info->destage_round
	};
	int rc;

	if (pdisk->client_conn.status != RDMA_SERVER_CONNECTED) {
		// need to establish RDMA connection and alloc buffer first
		return SPDK_POLLER_IDLE;
	}

	// reset the counter
	pdisk->destage_context.remaining = 0;

	while (true) {
		struct wals_metadata* metadata = pdisk->malloc_buf + pdisk->destage_info->destage_head * pdisk->disk.blocklen;
		// if we get unlucky (lucky?), then the next block may be the one for the last
		// round.

		if (metadata->version != PERSIST_METADATA_VERSION
			|| metadata->seq < pdisk->prev_seq) {
			struct wals_metadata* next_round_metadata = pdisk->malloc_buf;
			if (next_round_metadata->round == pdisk->destage_info->destage_round) {
				// head not in next round yet
				// it means that no IOs have arrived
				// do nothing and wait for the next IO
				break;
			}
			else if (next_round_metadata->round == pdisk->destage_info->destage_round + 1) {
				SPDK_NOTICELOG("Go back to block '0' during move.\n");
				metadata = next_round_metadata;
				if (metadata->version != PERSIST_METADATA_VERSION) {
					// should not happen even before any IO comes, because of the round
					SPDK_ERRLOG("Buffer head corrupted\n");
					break;
				}
				pdisk->destage_info->destage_head = 0;
				pdisk->destage_info->destage_round++;
			}
			else {
				// should not happen
				// TODO: error handling
				SPDK_ERRLOG("Next round %ld is not expected when this round is %ld\n",
					next_round_metadata->round,
					pdisk->destage_info->destage_round);
				break;
			}
		}
		// SPDK_NOTICELOG("Getting md %ld %ld %ld %ld %ld %ld\n",
		// 	metadata->version,
		// 	metadata->seq,
		// 	metadata->next_offset,
		// 	metadata->round,
		// 	metadata->length,
		// 	metadata->core_offset);

		// metadata should contain good info from now.
		// payload is always one block after metadata
		void* payload = (void*)(metadata) + pdisk->disk.blocklen;

		if (metadata->seq != pdisk->prev_seq + 1) {
			// TODO: what to do about it?
			SPDK_ERRLOG("Possible data loss! Previous seq is %ld while the current is %ld\n",
				pdisk->prev_seq,
				metadata->seq);
			
			// ignore the error for now, otherwise the error prints forever
			pdisk->prev_seq = metadata->seq - 1;
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
		pdisk->destage_info->destage_head = metadata->next_offset;
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

	return SPDK_POLLER_BUSY;
}

static int update_persist_rdma_connection(struct persist_rdma_connection* rdma_conn, struct persist_disk* pdisk) {
	int rc;
	switch (rdma_conn->status) {
		case RDMA_CLI_INITIALIZED:
		case RDMA_SERVER_INITIALIZED:
		{
			struct rdma_cm_id* cm_id = NULL;
			rc = rdma_create_id(rdma_conn->channel, &cm_id, NULL, RDMA_PS_TCP);
			if (rc != 0) {
				SPDK_ERRLOG("rdma_create_id failed\n");
				return 1;
			}
			if (rdma_conn->is_server) {
				rdma_conn->parent_cm_id = cm_id;
				struct sockaddr_in addr;
				memcpy(&addr, rdma_conn->server_addr->ai_addr, sizeof(addr));
				rc = rdma_bind_addr(cm_id, (struct sockaddr*)&addr);
				if (rc != 0) {
					SPDK_ERRLOG("rdma bind addr failed\n");
					rdma_conn->status = RDMA_SERVER_ERROR;
					break;
				}
				rc = rdma_listen(cm_id, 3);
				if (rc != 0) {
					SPDK_ERRLOG("rdma listen failed\n");
					rdma_conn->status = RDMA_SERVER_ERROR;
					break;
				}

				SPDK_NOTICELOG("listening on port %d\n", ntohs(addr.sin_port));
				rdma_conn->status = RDMA_SERVER_LISTENING;
			}
			else {
				rdma_conn->cm_id = cm_id;
                rc = rdma_resolve_addr(cm_id, NULL, rdma_conn->server_addr->ai_addr, 1000);
                if (rc != 0) {
                    SPDK_ERRLOG("rdma_resolve_addr failed\n");
                    rdma_conn->status = RDMA_CLI_ERROR;
                    break;
                }
                rdma_conn->status = RDMA_CLI_ADDR_RESOLVING;
			}
			break;
		}
		case RDMA_SERVER_LISTENING:
		{
			struct rdma_cm_event* connect_event;
			rc = rdma_get_cm_event(rdma_conn->channel, &connect_event);
			if (rc != 0) {
				if (errno == EAGAIN) {
					// waiting for connection
				}
				else {
					SPDK_ERRLOG("Unexpected CM error %d\n", errno);
				}
				return SPDK_POLLER_IDLE;
			}

			if (connect_event->event != RDMA_CM_EVENT_CONNECT_REQUEST) {
				// TODO: reconnection
				SPDK_ERRLOG("invalid event type %d\n", connect_event->event);
				return SPDK_POLLER_IDLE;
			}

			SPDK_NOTICELOG("received conn request\n");

			struct ibv_context* ibv_context = connect_event->id->verbs;
			struct ibv_device_attr device_attr = {};
			ibv_query_device(ibv_context, &device_attr);

			struct ibv_cq* ibv_cq = ibv_create_cq(ibv_context, 4096, NULL, NULL, 0);
			assert(ibv_cq != NULL);
			assert(ibv_context != NULL);

			// no SRQ here - only one qp
			// TODO: fine-tune these params; 
			struct ibv_qp_init_attr init_attr = {
				.send_cq = ibv_cq,
				.recv_cq = ibv_cq,
				.qp_type = IBV_QPT_RC,
				.cap = {
					.max_recv_sge = device_attr.max_sge,
					.max_send_sge = device_attr.max_sge,
					.max_send_wr = 256,
					.max_recv_wr = 256,
				}
			};

			rc = rdma_create_qp(connect_event->id, NULL, &init_attr);
			SPDK_NOTICELOG("rdma_create_qp returns %d\n", rc);

			// the original cm id becomes useless from here.
			struct rdma_cm_id* child_cm_id = connect_event->id;
			rc = rdma_ack_cm_event(connect_event);
			SPDK_NOTICELOG("acked conn request\n");

			void* handshake_buffer = spdk_zmalloc(2 * sizeof(struct rdma_handshake), 2 * 1024 * 1024, NULL,
							SPDK_ENV_LCORE_ID_ANY, SPDK_MALLOC_DMA);
			struct ibv_mr* ibv_mr_handshake = ibv_reg_mr(child_cm_id->qp->pd,
				handshake_buffer,
				2 * sizeof(struct rdma_handshake),
				IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ);

			struct ibv_recv_wr wr, *bad_wr = NULL;
			struct ibv_sge sge;

			struct rdma_handshake* handshake = handshake_buffer;

			wr.wr_id = 1;
			wr.next = NULL;
			wr.sg_list = &sge;
			wr.num_sge = 1;

			sge.addr = (uint64_t)(handshake + 1);
			sge.length = sizeof(struct rdma_handshake);
			sge.lkey = ibv_mr_handshake->lkey;
			rc = ibv_post_recv(child_cm_id->qp, &wr, &bad_wr);
			if (rc != 0) {
				SPDK_ERRLOG("post recv failed\n");
				return -1;
			}

			struct rdma_conn_param conn_param = {};

			conn_param.responder_resources = 16;
			conn_param.initiator_depth = 16;
			conn_param.retry_count = 7;
			conn_param.rnr_retry_count = 7;
			rc = rdma_accept(child_cm_id, &conn_param);

			if (rc != 0) {
				SPDK_ERRLOG("accept err\n");
				return -EINVAL;
			}
			rdma_conn->cm_id = child_cm_id;
			rdma_conn->cq = ibv_cq;
			rdma_conn->handshake_buf = handshake;
			rdma_conn->mr_handshake = ibv_mr_handshake;
			rdma_conn->status = RDMA_SERVER_ACCEPTED;
			break;
		}
		case RDMA_SERVER_ACCEPTED:
		{
			struct rdma_cm_event* established_event;
			rc = rdma_get_cm_event(rdma_conn->channel, &established_event);
			if (rc != 0) {
				if (errno == EAGAIN) {
					// waiting for establish event
				}
				else {
					SPDK_ERRLOG("Unexpected CM error %d\n", errno);
				}
				return SPDK_POLLER_IDLE;
			}

			if (established_event->event != RDMA_CM_EVENT_ESTABLISHED) {
				SPDK_ERRLOG("incorrect established event %d\n", established_event->event);
				return 1;
			}

			SPDK_NOTICELOG("connected. waiting for handshake ...\n");
			size_t total_size = (LOG_BLOCKCNT + 1) * LOG_BLOCKSIZE;
			
			struct ibv_send_wr send_wr, *bad_send_wr = NULL;
			struct ibv_mr* ibv_mr_circular;
			memset(&send_wr, 0, sizeof(send_wr));

			ibv_mr_circular = ibv_reg_mr(
				rdma_conn->cm_id->qp->pd,
				pdisk->malloc_buf,
				total_size,
				IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ);

			rdma_conn->mr = ibv_mr_circular;
			struct ibv_sge send_sge;
			
			rdma_conn->handshake_buf->base_addr = pdisk->malloc_buf;
			rdma_conn->handshake_buf->rkey = ibv_mr_circular->rkey;
			rdma_conn->handshake_buf->block_cnt = LOG_BLOCKCNT;
			rdma_conn->handshake_buf->block_size = LOG_BLOCKSIZE;

			send_wr.wr_id = 2;
			send_wr.opcode = IBV_WR_SEND;
			send_wr.sg_list = &send_sge;
			send_wr.num_sge = 1;
			send_wr.send_flags = IBV_SEND_SIGNALED;

			send_sge.addr = (uint64_t)rdma_conn->handshake_buf;
			send_sge.length = sizeof(struct rdma_handshake);
			send_sge.lkey = rdma_conn->mr_handshake->lkey;
			
			rc = ibv_post_send(rdma_conn->cm_id->qp, &send_wr, &bad_send_wr);
			if (rc != 0) {
				SPDK_ERRLOG("post send failed\n");
				return 1;
			}
			SPDK_NOTICELOG("sent local addr %p rkey %d length %ld\n",
				rdma_conn->handshake_buf->base_addr,
				rdma_conn->handshake_buf->rkey,
				log_size);

			rdma_conn->status = RDMA_SERVER_ESTABLISHED;
			break;
		}
		case RDMA_SERVER_ESTABLISHED:
		{
			struct ibv_wc wc;
			int ret = ibv_poll_cq(rdma_conn->cq, 1, &wc);
			if (ret < 0) {
				SPDK_ERRLOG("ibv_poll_cq failed\n");
				return SPDK_POLLER_IDLE;
			}

			if (ret == 0) {
				return SPDK_POLLER_IDLE;
			}

			if (wc.status != IBV_WC_SUCCESS) {
				SPDK_ERRLOG("WC bad status %d\n", wc.status);
				return SPDK_POLLER_IDLE;
			}

			if (wc.wr_id == 1) {
				// recv complete
				struct rdma_handshake* remote_handshake = rdma_conn->handshake_buf + 1;
				SPDK_NOTICELOG("received remote addr %p rkey %d\n",
					remote_handshake->base_addr,
					remote_handshake->rkey);

				if (remote_handshake->reconnect_cnt > 0) {
					// TODO
					SPDK_NOTICELOG("The node is reconnected\n");
					pdisk->disk_status = PERSIST_DISK_IN_RECOVERY;
				}

				SPDK_NOTICELOG("rdma handshake complete\n");
				rdma_conn->status = RDMA_SERVER_CONNECTED;
			}
			else if (wc.wr_id == 2) {
				SPDK_NOTICELOG("send req complete\n");
			}
			break;
		}
		case RDMA_SERVER_CONNECTED:
		{
			struct rdma_cm_event* event;
			rc = rdma_get_cm_event(rdma_conn->channel, &event);

			if (rc != 0) {
				if (errno == EAGAIN) {
					// wait for server to accept. do nothing
				}
				else {
					SPDK_ERRLOG("Unexpected CM error %d\n", errno);
				}
				break;
			}

			rc = rdma_ack_cm_event(event);
			if (rc != 0) {
				SPDK_ERRLOG("failed to ack event\n");
				rdma_conn->status = RDMA_SERVER_ERROR;
				break;
			}

			if (event->event == RDMA_CM_EVENT_DISCONNECTED) {
				SPDK_NOTICELOG("Received disconnect event\n");
				rdma_destroy_qp(rdma_conn->cm_id);
				if (rdma_conn->cm_id->qp != NULL) {
					SPDK_NOTICELOG("cannot free qp\n");
					rdma_conn->status = RDMA_SERVER_ERROR;
					break;
				}

				// rc = rdma_destroy_id(pdisk->cm_id);
				// if (rc != 0) {
				// 	SPDK_ERRLOG("cannot destroy id\n");
				// 	pdisk->status = PERSIST_RDMA_ERROR;
				// 	break;
				// }
				// pdisk->cm_id = NULL;
				// SPDK_NOTICELOG("2\n");

				rc = rdma_destroy_id(rdma_conn->parent_cm_id);
				if (rc != 0) {
					SPDK_ERRLOG("cannot destroy id\n");
					rdma_conn->status = RDMA_SERVER_ERROR;
					break;
				}
				rdma_conn->parent_cm_id = NULL;

				rc = ibv_destroy_cq(rdma_conn->cq);
				if (rc != 0) {
					SPDK_ERRLOG("destroy cq failed\n");
					rdma_conn->status = RDMA_SERVER_ERROR;
					break;
				}
				rdma_conn->cq = NULL;

				rc = ibv_dereg_mr(rdma_conn->mr);
				if (rc != 0) {
					SPDK_ERRLOG("failed to dereg mr\n");
					rdma_conn->status = RDMA_SERVER_ERROR;
					break;
				}
				rdma_conn->mr = NULL;

				rc = ibv_dereg_mr(rdma_conn->mr_handshake);
				if (rc != 0) {
					SPDK_ERRLOG("failed to dereg mr\n");
					rdma_conn->status = RDMA_SERVER_ERROR;
					break;
				}
				rdma_conn->mr_handshake = NULL;

				// spdk_free(pdisk->malloc_buf);
				spdk_free(rdma_conn->handshake_buf);

				rdma_conn->status = RDMA_SERVER_INITIALIZED;
			}
			else {
				SPDK_ERRLOG("Should not receive event %d when connected\n", event->event);
			}
			break;
		}
		case RDMA_SERVER_ERROR:
		{
			// no way to recover now.
			break;
		}
		case RDMA_CLI_ADDR_RESOLVING:
		case RDMA_CLI_ROUTE_RESOLVING:
		{
			int rc;
			struct rdma_cm_event* event;
			rc = rdma_get_cm_event(rdma_conn->channel, &event);
			if (rc != 0) {
				if (errno == EAGAIN) {
					// wait for server to accept. do nothing
				}
				else {
					SPDK_ERRLOG("Unexpected CM error %d\n", errno);
				}
				break;
			}
			enum rdma_cm_event_type expected_event_type;
			if (rdma_conn->status == RDMA_CLI_ADDR_RESOLVING) {
				expected_event_type = RDMA_CM_EVENT_ADDR_RESOLVED;
			}
			else if (rdma_conn->status == RDMA_CLI_ROUTE_RESOLVING) {
				expected_event_type = RDMA_CM_EVENT_ROUTE_RESOLVED;
			}
			else {
				// should not happen
				SPDK_ERRLOG("ERROR\n");
				rdma_conn->status = RDMA_CLI_ERROR;
				break;
			}

			// suppose addr and resolving never fails
			if (event->event != expected_event_type) {
				SPDK_ERRLOG("unexpected event type %d (expect %d)\n",
					event->event,
					expected_event_type);
				rdma_conn->status = RDMA_CLI_ERROR;
				break;
			}
			if (rdma_conn->cm_id != event->id) {
				SPDK_ERRLOG("CM id mismatch\n");
				rdma_conn->status = RDMA_CLI_ERROR;
				break;
			}
			rc = rdma_ack_cm_event(event);
			if (rc != 0) {
				SPDK_ERRLOG("ack cm event failed\n");
				rdma_conn->status = RDMA_CLI_ERROR;
				break;
			}

			if (rdma_conn->status == RDMA_CLI_ADDR_RESOLVING) {
				rc = rdma_resolve_route(rdma_conn->cm_id, 1000);
				if (rc != 0) {
					SPDK_ERRLOG("resolve route failed\n");
					rdma_conn->status = RDMA_CLI_ERROR;
					break;
				}
				rdma_conn->status = RDMA_CLI_ROUTE_RESOLVING;
			}
			else if (rdma_conn->status == RDMA_CLI_ROUTE_RESOLVING) {
				struct ibv_context* ibv_context = rdma_conn->cm_id->verbs;
				struct ibv_device_attr device_attr = {};
				ibv_query_device(ibv_context, &device_attr);
				// SPDK_NOTICELOG("max wr sge = %d, max wr num = %d, max cqe = %d, max qp = %d\n",
				//     device_attr.max_sge, device_attr.max_qp_wr, device_attr.max_cqe, device_attr.max_qp);
				struct ibv_cq* ibv_cq = ibv_create_cq(ibv_context, 256, NULL, NULL, 0);
				if (ibv_cq == NULL) {
					SPDK_ERRLOG("Failed to create cq\n");
					rdma_conn->status = RDMA_CLI_ERROR;
					break;
				}
				rdma_conn->cq = ibv_cq;
				// no SRQ here - only one qp
				// TODO: fine-tune these params; 
				struct ibv_qp_init_attr init_attr = {
					.send_cq = ibv_cq,
					.recv_cq = ibv_cq,
					.qp_type = IBV_QPT_RC,
					.cap = {
						.max_send_sge = device_attr.max_sge,
						.max_send_wr = 256,
						.max_recv_sge = device_attr.max_sge,
						.max_recv_wr = 256,
					}
				};

				rc = rdma_create_qp(rdma_conn->cm_id, NULL, &init_attr);
				if (rc != 0) {
					SPDK_ERRLOG("rdma_create_qp fails %d\n", rc);
					rdma_conn->status = RDMA_CLI_ERROR;
					break;
				}

				struct ibv_recv_wr wr, *bad_wr = NULL;
				struct ibv_sge sge;

				struct rdma_handshake* handshake = spdk_zmalloc(2 * sizeof(struct rdma_handshake), 2 * 1024 * 1024, NULL,
					SPDK_ENV_LCORE_ID_ANY, SPDK_MALLOC_DMA);
				
				rdma_conn->handshake_buf = handshake;
				struct rdma_handshake* remote_handshake = handshake + 1;
				struct ibv_mr* ibv_mr_circular = ibv_reg_mr(
					rdma_conn->cm_id->qp->pd,
					pdisk->malloc_buf,
					(LOG_BLOCKCNT + 1) * LOG_BLOCKSIZE,
					IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ);
				
				if (ibv_mr_circular == NULL) {
					SPDK_ERRLOG("Failed to register mr\n");
					rdma_conn->status = RDMA_CLI_ERROR;
					break;
				}

				rdma_conn->mr = ibv_mr_circular;

				handshake->base_addr = pdisk->malloc_buf;
				handshake->rkey = rdma_conn->mr->rkey;
				// TODO: server should provide this value
				handshake->block_cnt = LOG_BLOCKCNT;
				handshake->block_size = LOG_BLOCKSIZE;
				// TODO: does peer connection need this?
				handshake->reconnect_cnt = 0;

				struct ibv_mr* ibv_mr_handshake = ibv_reg_mr(rdma_conn->cm_id->qp->pd,
					handshake,
					2 * sizeof(struct rdma_handshake),
					IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ);
				
				if (ibv_mr_handshake == NULL) {
					SPDK_ERRLOG("Failed to register mr\n");
					rdma_conn->status = RDMA_CLI_ERROR;
					break;
				}

				rdma_conn->mr_handshake = ibv_mr_handshake;

				wr.wr_id = (uintptr_t)2;
				wr.next = NULL;
				wr.sg_list = &sge;
				wr.num_sge = 1;

				sge.addr = (uint64_t)remote_handshake;
				sge.length = sizeof(struct rdma_handshake);
				sge.lkey = ibv_mr_handshake->lkey;

				rc = ibv_post_recv(rdma_conn->cm_id->qp, &wr, &bad_wr);
				if (rc != 0) {
					SPDK_ERRLOG("post recv failed\n");
					rdma_conn->status = RDMA_CLI_ERROR;
					break;
				}

				struct rdma_conn_param conn_param = {};

				conn_param.responder_resources = 16;
				conn_param.initiator_depth = 16;
				conn_param.retry_count = 7;
				conn_param.rnr_retry_count = 7;

				rc = rdma_connect(rdma_conn->cm_id, &conn_param);
				if (rc != 0) {
					SPDK_ERRLOG("rdma_connect failed\n");
					rdma_conn->status = RDMA_CLI_ERROR;
					break;
				}
				rdma_conn->status = RDMA_CLI_CONNECTING;
			}
			break;
		}
		case RDMA_CLI_CONNECTING:
		{
			int rc;
			struct rdma_cm_event* connect_event;
			rc = rdma_get_cm_event(rdma_conn->channel, &connect_event);
			if (rc != 0) {
				if (errno == EAGAIN) {
					// wait for server to accept. do nothing
				}
				else {
					SPDK_ERRLOG("Unexpected CM error %d\n", errno);
				}
				break;
			}
			rc = rdma_ack_cm_event(connect_event);
			if (rc != 0) {
				SPDK_ERRLOG("failed to ack event\n");
				rdma_conn->status = RDMA_CLI_ERROR;
				break;
			}

			if (connect_event->event == RDMA_CM_EVENT_REJECTED) {
				rdma_conn->reject_cnt++;
				if (rdma_conn->reject_cnt % 100 == 1) {
					SPDK_NOTICELOG("Rejected %ld. Try again...\n", rdma_conn->reject_cnt);
				}
				rdma_destroy_qp(rdma_conn->cm_id);
				if (rdma_conn->cm_id->qp != NULL) {
					SPDK_NOTICELOG("cannot free qp\n");
					rdma_conn->status = RDMA_CLI_ERROR;
					break;
				}

				rc = rdma_destroy_id(rdma_conn->cm_id);
				if (rc != 0) {
					SPDK_ERRLOG("cannot destroy id\n");
					rdma_conn->status = RDMA_CLI_ERROR;
					break;
				}
				rdma_conn->cm_id = NULL;

				rc = ibv_destroy_cq(rdma_conn->cq);
				if (rc != 0) {
					SPDK_ERRLOG("destroy cq failed\n");
					rdma_conn->status = RDMA_CLI_ERROR;
					break;
				}
				rdma_conn->cq = NULL;

				rc = ibv_dereg_mr(rdma_conn->mr);
				if (rc != 0) {
					SPDK_ERRLOG("failed to dereg mr\n");
					rdma_conn->status = RDMA_CLI_ERROR;
					break;
				}
				rdma_conn->mr = NULL;

				rc = ibv_dereg_mr(rdma_conn->mr_handshake);
				if (rc != 0) {
					SPDK_ERRLOG("failed to dereg mr\n");
					rdma_conn->status = RDMA_CLI_ERROR;
					break;
				}
				rdma_conn->mr_handshake = NULL;

				// remote not ready yet
				// destroy all rdma resources and try again
				rdma_conn->status = RDMA_CLI_INITIALIZED;
				break;
			}
			else if (connect_event->event != RDMA_CM_EVENT_ESTABLISHED) {
				SPDK_ERRLOG("invalid event type %d\n", connect_event->event);
				break;
			}

			SPDK_NOTICELOG("connected. posting send...\n");
			SPDK_NOTICELOG("sending local addr %p rkey %d block_cnt %ld block_size %ld\n",
				rdma_conn->handshake_buf->base_addr,
				rdma_conn->handshake_buf->rkey,
				rdma_conn->handshake_buf->block_cnt,
				rdma_conn->handshake_buf->block_size);

			struct ibv_send_wr send_wr, *bad_send_wr = NULL;
			struct ibv_sge send_sge;
			memset(&send_wr, 0, sizeof(send_wr));

			send_wr.wr_id = 1;
			send_wr.opcode = IBV_WR_SEND;
			send_wr.sg_list = &send_sge;
			send_wr.num_sge = 1;
			send_wr.send_flags = IBV_SEND_SIGNALED;

			send_sge.addr = (uint64_t)rdma_conn->handshake_buf;
			send_sge.length = sizeof(struct rdma_handshake);
			send_sge.lkey = rdma_conn->mr_handshake->lkey;

			rc = ibv_post_send(rdma_conn->cm_id->qp, &send_wr, &bad_send_wr);
			if (rc != 0) {
				SPDK_ERRLOG("post send failed\n");
				break;
			}
			rdma_conn->status = RDMA_CLI_ESTABLISHED;
			break;
		}
		case RDMA_CLI_ESTABLISHED:
		{
			struct ibv_wc wc;
			struct rdma_handshake* remote_handshake = rdma_conn->handshake_buf + 1;
			int cnt = ibv_poll_cq(rdma_conn->cq, 1, &wc);
			if (cnt < 0) {
				SPDK_ERRLOG("ibv_poll_cq failed\n");
				break;
			}

			if (cnt == 0) {
				break;
			}

			if (wc.status != IBV_WC_SUCCESS) {
				SPDK_ERRLOG("WC bad status %d\n", wc.status);
				break;
			}

			if (wc.wr_id == 2) {
				// recv complete
				SPDK_NOTICELOG("received remote addr %p rkey %d block_cnt %ld block_size %ld\n",
					remote_handshake->base_addr,
					remote_handshake->rkey,
					remote_handshake->block_cnt,
					remote_handshake->block_size);
			}
			else if (wc.wr_id == 1) {
				// send cpl
				SPDK_NOTICELOG("send req complete\n");
				SPDK_NOTICELOG("rdma handshake complete\n");
				rdma_conn->status = RDMA_CLI_CONNECTED;
			}
			else {
				SPDK_ERRLOG("Should not complete wrid = %ld\n", wc.wr_id);
			}
			break;
		}
		case RDMA_CLI_CONNECTED:
		{
			struct rdma_cm_event* cm_event;
			int rc = rdma_get_cm_event(rdma_conn->channel, &cm_event);
			if (rc != 0) {
				if (errno == EAGAIN) {
					// wait for server to accept. do nothing
				}
				else {
					SPDK_ERRLOG("Unexpected CM error %d\n", errno);
				}
				break;
			}
			rc = rdma_ack_cm_event(cm_event);
			if (rc != 0) {
				SPDK_ERRLOG("failed to ack event\n");
				rdma_conn->status = RDMA_CLI_ERROR;
				break;
			}

			if (cm_event->event == RDMA_CM_EVENT_DISCONNECTED) {
				SPDK_NOTICELOG("Received disconnect event\n");
				rdma_destroy_qp(rdma_conn->cm_id);
				if (rdma_conn->cm_id->qp != NULL) {
					SPDK_NOTICELOG("cannot free qp\n");
					rdma_conn->status = RDMA_CLI_ERROR;
					break;
				}

				rc = rdma_destroy_id(rdma_conn->cm_id);
				if (rc != 0) {
					SPDK_ERRLOG("cannot destroy id\n");
					rdma_conn->status = RDMA_CLI_ERROR;
					break;
				}
				rdma_conn->cm_id = NULL;

				rc = ibv_destroy_cq(rdma_conn->cq);
				if (rc != 0) {
					SPDK_ERRLOG("destroy cq failed\n");
					rdma_conn->status = RDMA_CLI_ERROR;
					break;
				}
				rdma_conn->cq = NULL;

				rc = ibv_dereg_mr(rdma_conn->mr);
				if (rc != 0) {
					SPDK_ERRLOG("failed to dereg mr\n");
					rdma_conn->status = RDMA_CLI_ERROR;
					break;
				}
				rdma_conn->mr = NULL;

				rc = ibv_dereg_mr(rdma_conn->mr_handshake);
				if (rc != 0) {
					SPDK_ERRLOG("failed to dereg mr\n");
					rdma_conn->status = RDMA_CLI_ERROR;
					break;
				}
				rdma_conn->mr_handshake = NULL;

				rdma_conn->status = RDMA_CLI_INITIALIZED;
			}
			else {
				SPDK_ERRLOG("Should not receive event %d when connected\n", cm_event->event);
			}
			break;
		}
		case RDMA_CLI_ERROR:
		{
			// SPDK_NOTICELOG("In error state. Cannot recover by now\n");
			break;
		}
	}

	return SPDK_POLLER_BUSY;
}

static int persist_rdma_poller(void* ctx) {
	struct persist_disk* pdisk = ctx;

	update_persist_rdma_connection(&pdisk->client_conn, pdisk);
	for (size_t i = 0; i < pdisk->num_peers; i++) {
		update_persist_rdma_connection(pdisk->peer_conns + i, pdisk);
	}

	return SPDK_POLLER_BUSY;
}

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
		// just some fake data, as it doesn't serve IO requests anyway.
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
	size_t total_size = (LOG_BLOCKCNT + 1) * LOG_BLOCKSIZE;

	// one more block for current destage position
	// TODO: another block for commit tail
	// NOTE: the buffer length must not change when reconnecting
	pdisk->malloc_buf = spdk_zmalloc(total_size,
		2 * 1024 * 1024,
		NULL,
		SPDK_ENV_LCORE_ID_ANY,
		SPDK_MALLOC_DMA);
	pdisk->destage_info = pdisk->malloc_buf + log_size;

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

	struct rdma_event_channel* channel = rdma_create_event_channel();
	pdisk->client_conn.channel = channel;

	// need to make sure that the fd is set to non-blocking before 
	// entering the poller.
	int flags = fcntl(pdisk->client_conn.channel->fd, F_GETFL);
	rc = fcntl(pdisk->client_conn.channel->fd, F_SETFL, flags | O_NONBLOCK);
	if (rc != 0) {
		SPDK_ERRLOG("fcntl failed\n");
		return -EINVAL;
	}

	pdisk->client_conn.status = RDMA_SERVER_INITIALIZED;
	pdisk->client_conn.is_server = true;

	struct addrinfo hints = {};
	struct addrinfo* addr_res = NULL;
	hints.ai_family = AF_INET;
	hints.ai_flags = AI_PASSIVE;
	rc = getaddrinfo(ip, port, &hints, &addr_res);
	if (rc != 0) {
		SPDK_ERRLOG("getaddrinfo failed\n");
		return 1;
	}
	pdisk->client_conn.server_addr = addr_res;

	SPDK_NOTICELOG("num_peers = %d\n", num_peers);

	pdisk->num_peers = num_peers;
	for (size_t i = 0; i < num_peers; i++) {
		struct rdma_event_channel* channel = rdma_create_event_channel();
		pdisk->peer_conns[i].channel = channel;

		// need to make sure that the fd is set to non-blocking before 
		// entering the poller.
		int flags = fcntl(pdisk->peer_conns[i].channel->fd, F_GETFL);
		rc = fcntl(pdisk->peer_conns[i].channel->fd, F_SETFL, flags | O_NONBLOCK);
		if (rc != 0) {
			SPDK_ERRLOG("fcntl failed\n");
			return -EINVAL;
		}

		struct addrinfo hints = {};
		struct addrinfo* addr_res = NULL;
		hints.ai_family = AF_INET;
		hints.ai_flags = AI_PASSIVE;

		// the smaller ip one is the server
		if (strcmp(ip, peer_info_array[i].remote_ip) < 0
			|| (strcmp(ip, peer_info_array[i].remote_ip) == 0 && strcmp(port, peer_info_array[i].remote_port) < 0)) {
			pdisk->peer_conns[i].is_server = true;
			rc = getaddrinfo(ip, peer_info_array[i].local_port, &hints, &addr_res);
			if (rc != 0) {
				SPDK_ERRLOG("getaddrinfo failed\n");
				return 1;
			}
			pdisk->peer_conns[i].server_addr = addr_res;
			pdisk->peer_conns[i].status = RDMA_SERVER_INITIALIZED;
		}
		else {
			pdisk->peer_conns[i].is_server = false;
			rc = getaddrinfo(peer_info_array[i].remote_ip, peer_info_array[i].remote_port, &hints, &addr_res);
			if (rc != 0) {
				SPDK_ERRLOG("getaddrinfo failed\n");
				return 1;
			}
			pdisk->peer_conns[i].server_addr = addr_res;
			pdisk->peer_conns[i].status = RDMA_CLI_INITIALIZED;
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

	pdisk->rdma_poller = SPDK_POLLER_REGISTER(persist_rdma_poller, pdisk, 100);
	if (!pdisk->rdma_poller) {
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
