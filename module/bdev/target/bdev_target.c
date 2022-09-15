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

#include "bdev_target.h"
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

#include "spdk/bdev_module.h"
#include "spdk/log.h"
#include <infiniband/verbs.h>
#include <rdma/rdma_cma.h>
#include <errno.h>

enum target_rdma_status {
	TARGET_RDMA_CONNECTING,
	TARGET_RDMA_ESTABLISHED,
	TARGET_RDMA_CONNECTED,
	TARGET_RDMA_INITIALIZED,
	TARGET_RDMA_ERROR,
	TARGET_RDMA_ADDR_RESOLVING,
	TARGET_RDMA_ROUTE_RESOLVING,
};

struct target_disk {
	struct spdk_bdev		disk;
	// act as circular buffer
	void				*malloc_buf;
	struct rdma_cm_id* cm_id;
	struct rdma_event_channel* rdma_channel;
	struct ibv_mr* mr;
	struct ibv_cq* cq;
	struct ibv_pd* pd;
	struct rdma_handshake* remote_handshake;
	struct spdk_poller* cq_poller;
	struct spdk_poller* reconnect_poller;
	struct ibv_wc wc_buf[TARGET_WC_BATCH_SIZE];
	enum target_rdma_status rdma_status;
	struct addrinfo* server_addr;

	TAILQ_ENTRY(target_disk)	link;
};


struct wal_metadata {
	uint64_t	version;
	
	uint64_t	seq;

	uint64_t	next_offset;

	uint64_t	length;

	uint64_t	core_offset;

	uint64_t	core_length;

	uint64_t	round;
};

struct target_task {
};

struct target_channel {
	struct target_disk* tdisk;
};

int target_disk_count = 0;

static int bdev_target_initialize(void);
static void bdev_target_deinitialize(void);

static int
bdev_target_get_ctx_size(void)
{
	return sizeof(struct target_task);
}

static struct spdk_bdev_module target_if = {
	.name = "target",
	.module_init = bdev_target_initialize,
	.module_fini = bdev_target_deinitialize,
	.get_ctx_size = bdev_target_get_ctx_size,

};

SPDK_BDEV_MODULE_REGISTER(target, &target_if)

static void
target_disk_free(struct target_disk *target_disk)
{
	if (!target_disk) {
		return;
	}
	spdk_poller_unregister(&target_disk->cq_poller);
	spdk_poller_unregister(&target_disk->reconnect_poller);

	free(target_disk->disk.name);
	spdk_free(target_disk->malloc_buf);
	free(target_disk);
}

static int
bdev_target_destruct(void *ctx)
{
	struct target_disk *target_disk = ctx;

	// TAILQ_REMOVE(&g_target_disks, target_disk, link);
	target_disk_free(target_disk);
	spdk_io_device_unregister(target_disk, NULL);
	return 0;
}

static int
bdev_target_check_iov_len(struct iovec *iovs, int iovcnt, size_t nbytes)
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

static int bdev_target_is_io_oob(uint64_t offset, size_t len, struct target_disk* tdisk, bool is_read) {
	uint64_t offset_end = is_read ? (offset + len) : (offset + len + tdisk->disk.md_len);
	return offset_end > tdisk->disk.blockcnt * tdisk->disk.blocklen;
}

static void
bdev_target_readv(struct target_disk *tdisk, 
		  struct spdk_bdev_io *bdev_io,
		  struct iovec *iov, int iovcnt, size_t len, uint64_t offset)
{
	void *src = tdisk->malloc_buf + offset;
	int rc;

	if (bdev_target_check_iov_len(iov, iovcnt, len)) {
		spdk_bdev_io_complete(bdev_io,
				      SPDK_BDEV_IO_STATUS_FAILED);
		return;
	}

	if (bdev_target_is_io_oob(offset, len, tdisk, true)) {
		SPDK_ERRLOG("read OOB\n");
		spdk_bdev_io_complete(bdev_io,
				      SPDK_BDEV_IO_STATUS_FAILED);
		return;
	}

	SPDK_DEBUGLOG(bdev_target, "read %zu bytes from offset %#" PRIx64 ", iovcnt=%d\n",
		      len, offset, iovcnt);

	if (offset / tdisk->disk.blocklen == tdisk->disk.blockcnt - 1) {
		// read last block (destage metadata)
		// first, RDMA read from remote
		// then in the RDMA cq poll callback, do a memcpy into iovs
		// and then succeed.
		if (len != tdisk->disk.blocklen) {
			SPDK_ERRLOG("client should only read one block of destage metadata\n");
			spdk_bdev_io_complete(bdev_io,
						SPDK_BDEV_IO_STATUS_FAILED);
			return;
		}

		struct ibv_send_wr wr, *bad_wr = NULL;
		struct ibv_sge sge;
		memset(&wr, 0, sizeof(wr));
		wr.wr_id = (uint64_t)bdev_io;
		wr.next = NULL;
		// TODO: inline?
		wr.send_flags = IBV_SEND_SIGNALED;
		wr.opcode = IBV_WR_RDMA_READ;
		wr.num_sge = 1;
		wr.sg_list = &sge;
		wr.wr.rdma.remote_addr = (uint64_t)tdisk->remote_handshake->base_addr + offset;
		wr.wr.rdma.rkey = tdisk->remote_handshake->rkey;

		sge.addr = (uint64_t)src;
		sge.length = len;
		sge.lkey = tdisk->mr->lkey;
		rc = ibv_post_send(tdisk->cm_id->qp, &wr, &bad_wr);
		if (rc != 0) {
			SPDK_ERRLOG("RDMA read failed with errno = %d\n", rc);
			SPDK_NOTICELOG("Local: %p %d; Remote: %p %d; Len = %d\n",
				(void*)sge.addr, sge.lkey, (void*)wr.wr.rdma.remote_addr, wr.wr.rdma.rkey,
				sge.length);
			spdk_bdev_io_complete(bdev_io,
						SPDK_BDEV_IO_STATUS_FAILED);
		}
	}
	else {
		// simply memcpy from local memory
		// need to use RDMA read if the memory is from Application.
		for (int i = 0; i < iovcnt; i++) {
			memcpy(iov[i].iov_base, src, iov[i].iov_len);
			src += iov[i].iov_len;
		}
		spdk_bdev_io_complete(bdev_io, SPDK_BDEV_IO_STATUS_SUCCESS);
	}
}

static void
bdev_target_writev_with_md(struct target_disk *tdisk, 
		   struct spdk_bdev_io *bdev_io,
		   struct wal_metadata* md, struct iovec *iov, int iovcnt, size_t len, uint64_t offset)
{
	// if (offset > 512 * 1024 * 1024) {
	// 	SPDK_ERRLOG("offset %ld is too large\n", offset);
	// 	spdk_bdev_io_complete(spdk_bdev_io_from_ctx(task),
	// 			      SPDK_BDEV_IO_STATUS_FAILED);
	// 	return;
	// }
	void *dst = tdisk->malloc_buf + offset;
	// for RDMA write
	void* rdma_src = dst;
	void* rdma_dst = tdisk->remote_handshake->base_addr + offset;

	int rc;
	
	if (bdev_target_check_iov_len(iov, iovcnt, len)) {
		spdk_bdev_io_complete(bdev_io, SPDK_BDEV_IO_STATUS_FAILED);
		return;
	}

	if (bdev_target_is_io_oob(offset, len, tdisk, false)) {
		SPDK_ERRLOG("write OOB\n");
		spdk_bdev_io_complete(bdev_io, SPDK_BDEV_IO_STATUS_FAILED);
		return;
	}

	SPDK_DEBUGLOG(bdev_target, "wrote %zu bytes to offset %#" PRIx64 ", iovcnt=%d\n",
		      len, offset, iovcnt);
	
	spdk_trace_record_tsc(spdk_get_ticks(), TRACE_BDEV_WRITE_MEMCPY_START, 0, 0, (uintptr_t)bdev_io);
	if (md != NULL) {
		memcpy(dst, md, tdisk->disk.md_len);
		dst += tdisk->disk.md_len;
		len += tdisk->disk.md_len;
	}

	for (int i = 0; i < iovcnt; i++) {
		memcpy(dst, iov[i].iov_base, iov[i].iov_len);
		dst += iov[i].iov_len;
	}
	spdk_trace_record_tsc(spdk_get_ticks(), TRACE_BDEV_WRITE_MEMCPY_END, 0, 0, (uintptr_t)bdev_io);

	struct ibv_send_wr wr, *bad_wr = NULL;
	struct ibv_sge sge;
	memset(&wr, 0, sizeof(wr));
	wr.wr_id = (uint64_t)bdev_io;
	wr.next = NULL;
	// TODO: inline?
	wr.send_flags = IBV_SEND_SIGNALED;
	wr.opcode = IBV_WR_RDMA_WRITE;
	wr.num_sge = 1;
	wr.sg_list = &sge;
	wr.wr.rdma.remote_addr = (uint64_t)rdma_dst;
	wr.wr.rdma.rkey = tdisk->remote_handshake->rkey;

	sge.addr = (uint64_t)rdma_src;
	sge.length = len;
	sge.lkey = tdisk->mr->lkey;

	spdk_trace_record_tsc(spdk_get_ticks(), TRACE_BDEV_RDMA_POST_SEND_WRITE_START, 0, 0, (uintptr_t)bdev_io);
	rc = ibv_post_send(tdisk->cm_id->qp, &wr, &bad_wr);
	spdk_trace_record_tsc(spdk_get_ticks(), TRACE_BDEV_RDMA_POST_SEND_WRITE_END, 0, 0, (uintptr_t)bdev_io);
	if (rc != 0) {
		SPDK_ERRLOG("RDMA write failed with errno = %d\n", rc);
		SPDK_NOTICELOG("Local: %p %d; Remote: %p %d; Len = %d\n",
			(void*)sge.addr, sge.lkey, (void*)wr.wr.rdma.remote_addr, wr.wr.rdma.rkey,
			sge.length);
		spdk_bdev_io_complete(bdev_io,
				      SPDK_BDEV_IO_STATUS_FAILED);
	}
	// else {
	// 	SPDK_NOTICELOG("RDMA write succeed\n");
	// 	SPDK_NOTICELOG("IO: %p, Local: %p %d; Remote: %p %d; Len = %d\n",
	// 		bdev_io, (void*)sge.addr, sge.lkey, (void*)wr.wr.rdma.remote_addr, wr.wr.rdma.rkey,
	// 		sge.length);
	// 	// spdk_bdev_io_complete(spdk_bdev_io_from_ctx(task),
	// 	// 		      SPDK_BDEV_IO_STATUS_PENDING);
	// }
}

static int _bdev_target_submit_request(struct target_channel *mch, struct spdk_bdev_io *bdev_io)
{
	uint32_t block_size = bdev_io->bdev->blocklen;
	// _log_md(bdev_io);

	if (bdev_io->u.bdev.iovs[0].iov_base == NULL) {
		SPDK_ERRLOG("Received read req where iov_base is null\n");
		return 0;
	}

	switch (bdev_io->type) {
	case SPDK_BDEV_IO_TYPE_READ:
		// spdk_bdev_io_complete(bdev_io,
		// 				SPDK_BDEV_IO_STATUS_SUCCESS);
		if (bdev_io->u.bdev.iovs[0].iov_base == NULL) {
			// TODO: in which case will the code enter this branch?
			// in theory, I would read from disk instead.
			SPDK_DEBUGLOG(bdev_target, "Received read req where iov_base is null\n");
			assert(bdev_io->u.bdev.iovcnt == 1);
			bdev_io->u.bdev.iovs[0].iov_base =
				((struct target_disk *)bdev_io->bdev->ctxt)->malloc_buf +
				bdev_io->u.bdev.offset_blocks * block_size;
			bdev_io->u.bdev.iovs[0].iov_len = bdev_io->u.bdev.num_blocks * block_size;
			spdk_bdev_io_complete(bdev_io,
					     SPDK_BDEV_IO_STATUS_SUCCESS);
			return 0;
		}

		bdev_target_readv((struct target_disk *)bdev_io->bdev->ctxt,
				  bdev_io,
				  bdev_io->u.bdev.iovs,
				  bdev_io->u.bdev.iovcnt,
				  bdev_io->u.bdev.num_blocks * block_size,
				  bdev_io->u.bdev.offset_blocks * block_size);
		return 0;

	case SPDK_BDEV_IO_TYPE_WRITE:
		bdev_target_writev_with_md((struct target_disk *)bdev_io->bdev->ctxt,
				   bdev_io,
				   bdev_io->u.bdev.md_buf,
				   bdev_io->u.bdev.iovs,
				   bdev_io->u.bdev.iovcnt,
				   bdev_io->u.bdev.num_blocks * block_size,
				   bdev_io->u.bdev.offset_blocks * block_size);
		return 0;
	default:
		return -1;
	}
	return 0;
}

static void bdev_target_submit_request(struct spdk_io_channel *ch, struct spdk_bdev_io *bdev_io)
{
	struct target_channel *mch = spdk_io_channel_get_ctx(ch);

	if (_bdev_target_submit_request(mch, bdev_io) != 0) {
		spdk_bdev_io_complete(bdev_io,
				     SPDK_BDEV_IO_STATUS_FAILED);
	}
}

static bool
bdev_target_io_type_supported(void *ctx, enum spdk_bdev_io_type io_type)
{
	switch (io_type) {
	case SPDK_BDEV_IO_TYPE_READ:
	case SPDK_BDEV_IO_TYPE_WRITE:
		return true;
	default:
		return false;
	}
}

static struct spdk_io_channel *
bdev_target_get_io_channel(void *ctx)
{
	return spdk_get_io_channel(ctx);
}

static void
bdev_target_write_json_config(struct spdk_bdev *bdev, struct spdk_json_write_ctx *w)
{
	char uuid_str[SPDK_UUID_STRING_LEN];

	spdk_json_write_object_begin(w);

	spdk_json_write_named_string(w, "method", "bdev_target_create");

	spdk_json_write_named_object_begin(w, "params");
	spdk_json_write_named_string(w, "name", bdev->name);
	spdk_json_write_named_uint64(w, "num_blocks", bdev->blockcnt);
	spdk_json_write_named_uint32(w, "block_size", bdev->blocklen);
	spdk_uuid_fmt_lower(uuid_str, sizeof(uuid_str), &bdev->uuid);
	spdk_json_write_named_string(w, "uuid", uuid_str);
	spdk_json_write_named_uint32(w, "optimal_io_boundary", bdev->optimal_io_boundary);

	spdk_json_write_object_end(w);

	spdk_json_write_object_end(w);
}

static const struct spdk_bdev_fn_table target_fn_table = {
	.destruct		= bdev_target_destruct,
	.submit_request		= bdev_target_submit_request,
	.io_type_supported	= bdev_target_io_type_supported,
	.get_io_channel		= bdev_target_get_io_channel,
	.write_config_json	= bdev_target_write_json_config,
};

static void target_complete_io_success(void* ctx) {
	SPDK_DEBUGLOG(bdev_target, "completing io from other thread\n");
	spdk_bdev_io_complete(ctx, SPDK_BDEV_IO_STATUS_SUCCESS);
}

static int
target_cq_poller(void *ctx)
{
	struct target_disk *tdisk = ctx;

	if (tdisk->rdma_status != TARGET_RDMA_CONNECTED) {
		return SPDK_POLLER_IDLE;
	}

	struct spdk_bdev_io* io;
	int cnt = ibv_poll_cq(tdisk->cq, TARGET_WC_BATCH_SIZE, tdisk->wc_buf);
	if (cnt < 0) {
		// TODO: what to do when poll cq fails?
		SPDK_ERRLOG("ibv_poll_cq failed\n");
	}
	else if (cnt == 0) {
		return SPDK_POLLER_IDLE;
	}
	else {
		for (int i = 0; i < cnt; i++) {
			io = (struct spdk_bdev_io*)tdisk->wc_buf[i].wr_id;
			spdk_trace_record_tsc(spdk_get_ticks(), TRACE_BDEV_CQ_POLL, 0, 0, (uintptr_t)io, tdisk->disk.name, spdk_thread_get_id(spdk_get_thread()));
			if (tdisk->wc_buf[i].status != IBV_WC_SUCCESS) {
				SPDK_ERRLOG("IO %p RDMA op %d failed with status %d\n",
					io,
					tdisk->wc_buf[i].opcode,
					tdisk->wc_buf[i].status);
				spdk_bdev_io_complete(io, SPDK_BDEV_IO_STATUS_FAILED);
			}
			else {
				if (tdisk->wc_buf[i].opcode == IBV_WC_RDMA_READ) {
					// read cpl needs to do a memcpy back to iovs
					void* src = tdisk->malloc_buf + io->u.bdev.offset_blocks * io->bdev->blocklen;
					for (int j = 0; j < io->u.bdev.iovcnt; j++) {
						memcpy(io->u.bdev.iovs[j].iov_base, src, io->u.bdev.iovs[j].iov_len);
						src += io->u.bdev.iovs[j].iov_len;
					}
				}

				if (spdk_bdev_io_get_thread(io) == spdk_get_thread()) {
					spdk_bdev_io_complete(io, SPDK_BDEV_IO_STATUS_SUCCESS);
				}
				else {
					spdk_thread_send_msg(spdk_bdev_io_get_thread(io), target_complete_io_success, io);
				}
			}
		}
	}

	return SPDK_POLLER_BUSY;
}

static int 
target_reconnect_poller(void* ctx) {
	struct target_disk* tdisk = ctx;
	switch (tdisk->rdma_status) {
		case TARGET_RDMA_INITIALIZED:
		{
			int rc;
			struct rdma_cm_id* cm_id = NULL;
			rc = rdma_create_id(tdisk->rdma_channel, &cm_id, NULL, RDMA_PS_TCP);
			if (rc != 0) {
				SPDK_ERRLOG("rdma_create_id failed\n");
				tdisk->rdma_status = TARGET_RDMA_ERROR;
				return SPDK_POLLER_BUSY;
			}
			tdisk->cm_id = cm_id;

			rc = rdma_resolve_addr(cm_id, NULL, tdisk->server_addr->ai_addr, 1000);
			if (rc != 0) {
				SPDK_ERRLOG("rdma_resolve_addr failed\n");
				tdisk->rdma_status = TARGET_RDMA_ERROR;
				return SPDK_POLLER_BUSY;
			}
			tdisk->rdma_status = TARGET_RDMA_ADDR_RESOLVING;
			break;
		}
		case TARGET_RDMA_ADDR_RESOLVING:
		case TARGET_RDMA_ROUTE_RESOLVING:
		{
			int rc;
			struct rdma_cm_event* event;
			rc = rdma_get_cm_event(tdisk->rdma_channel, &event);
			if (rc != 0) {
				if (errno == EAGAIN) {
					// wait for server to accept. do nothing
				}
				else {
					SPDK_ERRLOG("Unexpected CM error %d\n", errno);
				}
				return SPDK_POLLER_IDLE;
			}
			enum rdma_cm_event_type expected_event_type;
			if (tdisk->rdma_status == TARGET_RDMA_ADDR_RESOLVING) {
				expected_event_type = RDMA_CM_EVENT_ADDR_RESOLVED;
			}
			else if (tdisk->rdma_status == TARGET_RDMA_ROUTE_RESOLVING) {
				expected_event_type = RDMA_CM_EVENT_ROUTE_RESOLVED;
			}

			// suppose addr and resolving never fails
			if (event->event != expected_event_type) {
				SPDK_ERRLOG("unexpected event type %d (expect %d)\n",
					event->event,
					expected_event_type);
				tdisk->rdma_status = TARGET_RDMA_ERROR;
				break;
			}
			if (tdisk->cm_id != event->id) {
				SPDK_ERRLOG("CM id mismatch\n");
				tdisk->rdma_status = TARGET_RDMA_ERROR;
				break;
			}
			rc = rdma_ack_cm_event(event);
			if (rc != 0) {
				SPDK_ERRLOG("ack cm event failed\n");
				tdisk->rdma_status = TARGET_RDMA_ERROR;
				break;
			}

			if (tdisk->rdma_status == TARGET_RDMA_ADDR_RESOLVING) {
				rc = rdma_resolve_route(tdisk->cm_id, 1000);
				if (rc != 0) {
					SPDK_ERRLOG("resolve route failed\n");
					tdisk->rdma_status = TARGET_RDMA_ERROR;
					break;
				}
				tdisk->rdma_status = TARGET_RDMA_ROUTE_RESOLVING;
			}
			else if (tdisk->rdma_status == TARGET_RDMA_ROUTE_RESOLVING) {
				struct ibv_context* ibv_context = tdisk->cm_id->verbs;
				struct ibv_device_attr device_attr = {};
				ibv_query_device(ibv_context, &device_attr);
				SPDK_NOTICELOG("max wr sge = %d, max wr num = %d, max cqe = %d, max qp = %d\n",
					device_attr.max_sge, device_attr.max_qp_wr, device_attr.max_cqe, device_attr.max_qp);
				struct ibv_cq* ibv_cq = ibv_create_cq(ibv_context, 256, NULL, NULL, 0);
				if (ibv_cq == NULL) {
					SPDK_ERRLOG("Failed to create cq\n");
					tdisk->rdma_status = TARGET_RDMA_ERROR;
					break;
				}
				tdisk->cq = ibv_cq;
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

				rc = rdma_create_qp(tdisk->cm_id, NULL, &init_attr);
				if (rc != 0) {
					SPDK_ERRLOG("rdma_create_qp fails %d\n", rc);
					tdisk->rdma_status = TARGET_RDMA_ERROR;
					break;
				}

				struct ibv_mr* ibv_mr = ibv_reg_mr(tdisk->cm_id->qp->pd,
					tdisk->malloc_buf,
					tdisk->disk.blockcnt * tdisk->disk.blocklen + 2 * sizeof(struct rdma_handshake),
					IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ);

				if (ibv_mr == NULL) {
					SPDK_ERRLOG("failed to reg mr\n");
					tdisk->rdma_status = TARGET_RDMA_ERROR;
					break;
				}
				tdisk->mr = ibv_mr;
				
				struct ibv_recv_wr wr, *bad_wr = NULL;
				struct ibv_sge sge;

				struct rdma_handshake* handshake = tdisk->malloc_buf + tdisk->disk.blockcnt * tdisk->disk.blocklen;
				struct rdma_handshake* remote_handshake = handshake + 1;
				handshake->base_addr = tdisk->malloc_buf;
				handshake->rkey = ibv_mr->rkey;
				handshake->block_cnt = tdisk->disk.blockcnt;
				handshake->block_size = tdisk->disk.blocklen;

				wr.wr_id = (uintptr_t)1;
				wr.next = NULL;
				wr.sg_list = &sge;
				wr.num_sge = 1;

				sge.addr = (uint64_t)remote_handshake;
				sge.length = sizeof(struct rdma_handshake);
				sge.lkey = ibv_mr->lkey;
				tdisk->remote_handshake = remote_handshake;

				rc = ibv_post_recv(tdisk->cm_id->qp, &wr, &bad_wr);
				if (rc != 0) {
					SPDK_ERRLOG("post recv failed\n");
					tdisk->rdma_status = TARGET_RDMA_ERROR;
					break;
				}

				struct rdma_conn_param conn_param = {};

				conn_param.responder_resources = 16;
				conn_param.initiator_depth = 16;
				conn_param.retry_count = 7;
				conn_param.rnr_retry_count = 7;

				rc = rdma_connect(tdisk->cm_id, &conn_param);
				if (rc != 0) {
					SPDK_ERRLOG("rdma_connect failed\n");
					tdisk->rdma_status = TARGET_RDMA_ERROR;
					break;
				}
				tdisk->rdma_status = TARGET_RDMA_CONNECTING;
			}
			break;
		}
		case TARGET_RDMA_CONNECTED:
		{
			struct rdma_cm_event* cm_event;
			int rc = rdma_get_cm_event(tdisk->rdma_channel, &cm_event);
			if (rc != 0) {
				if (errno == EAGAIN) {
					// wait for server to accept. do nothing
				}
				else {
					SPDK_ERRLOG("Unexpected CM error %d\n", errno);
				}
				return SPDK_POLLER_IDLE;
			}
			rc = rdma_ack_cm_event(cm_event);
			if (rc != 0) {
				SPDK_ERRLOG("failed to ack event\n");
				tdisk->rdma_status = TARGET_RDMA_ERROR;
				break;
			}

			if (cm_event->event == RDMA_CM_EVENT_DISCONNECTED) {
				SPDK_NOTICELOG("Received disconnect event\n");
				rdma_destroy_qp(tdisk->cm_id);
				if (tdisk->cm_id->qp != NULL) {
					SPDK_NOTICELOG("cannot free qp\n");
					tdisk->rdma_status = TARGET_RDMA_ERROR;
					return SPDK_POLLER_BUSY;
				}

				rc = rdma_destroy_id(tdisk->cm_id);
				if (rc != 0) {
					SPDK_ERRLOG("cannot destroy id\n");
					tdisk->rdma_status = TARGET_RDMA_ERROR;
					return SPDK_POLLER_BUSY;
				}
				tdisk->cm_id = NULL;

				rc = ibv_destroy_cq(tdisk->cq);
				if (rc != 0) {
					SPDK_ERRLOG("destroy cq failed\n");
					tdisk->rdma_status = TARGET_RDMA_ERROR;
					return SPDK_POLLER_BUSY;
				}
				tdisk->cq = NULL;

				rc = ibv_dereg_mr(tdisk->mr);
				if (rc != 0) {
					SPDK_ERRLOG("failed to dereg mr\n");
					tdisk->rdma_status = TARGET_RDMA_ERROR;
					return SPDK_POLLER_BUSY;
				}
				tdisk->mr = NULL;

				tdisk->rdma_status = TARGET_RDMA_INITIALIZED;
			}
			else {
				SPDK_ERRLOG("Should not receive event %d when connected\n", cm_event->event);
			}
			break;
		}
		case TARGET_RDMA_CONNECTING:
		{
			struct rdma_handshake* handshake = tdisk->remote_handshake - 1;
			int rc;
			struct rdma_cm_event* connect_event;
			struct ibv_send_wr send_wr, *bad_send_wr = NULL;
			struct ibv_sge send_sge;
			rc = rdma_get_cm_event(tdisk->rdma_channel, &connect_event);
			if (rc != 0) {
				if (errno == EAGAIN) {
					// wait for server to accept. do nothing
				}
				else {
					SPDK_ERRLOG("Unexpected CM error %d\n", errno);
				}
				return SPDK_POLLER_IDLE;
			}
			rc = rdma_ack_cm_event(connect_event);
			if (rc != 0) {
				SPDK_ERRLOG("failed to ack event\n");
				tdisk->rdma_status = TARGET_RDMA_ERROR;
				break;
			}

			if (connect_event->event == RDMA_CM_EVENT_REJECTED) {
				SPDK_NOTICELOG("Rejected. Try again...\n");
				// remote not ready yet
				// destroy all rdma resources and try again
				rdma_destroy_qp(tdisk->cm_id);
				if (tdisk->cm_id->qp != NULL) {
					SPDK_NOTICELOG("cannot free qp\n");
					tdisk->rdma_status = TARGET_RDMA_ERROR;
					break;
				}

				rc = rdma_destroy_id(tdisk->cm_id);
				if (rc != 0) {
					SPDK_ERRLOG("cannot destroy id\n");
					tdisk->rdma_status = TARGET_RDMA_ERROR;
					break;
				}
				tdisk->cm_id = NULL;

				rc = ibv_destroy_cq(tdisk->cq);
				if (rc != 0) {
					SPDK_ERRLOG("destroy cq failed\n");
					tdisk->rdma_status = TARGET_RDMA_ERROR;
					break;
				}
				tdisk->cq = NULL;

				rc = ibv_dereg_mr(tdisk->mr);
				if (rc != 0) {
					SPDK_ERRLOG("failed to dereg mr\n");
					tdisk->rdma_status = TARGET_RDMA_ERROR;
					break;
				}
				tdisk->mr = NULL;

				tdisk->rdma_status = TARGET_RDMA_INITIALIZED;
				break;
			}
			else if (connect_event->event != RDMA_CM_EVENT_ESTABLISHED) {
				SPDK_ERRLOG("invalid event type %d\n", connect_event->event);
				break;
			}

			assert(connect_event->id == tdisk->cm_id);

			SPDK_NOTICELOG("connected. posting send...\n");
			SPDK_NOTICELOG("sending local addr %p rkey %d block_cnt %ld block_size %ld\n",
				handshake->base_addr,
				handshake->rkey,
				handshake->block_cnt,
				handshake->block_size);

			memset(&send_wr, 0, sizeof(send_wr));

			send_wr.wr_id = 2;
			send_wr.opcode = IBV_WR_SEND;
			send_wr.sg_list = &send_sge;
			send_wr.num_sge = 1;
			send_wr.send_flags = IBV_SEND_SIGNALED;

			send_sge.addr = (uint64_t)handshake;
			send_sge.length = sizeof(struct rdma_handshake);
			send_sge.lkey = tdisk->mr->lkey;

			rc = ibv_post_send(tdisk->cm_id->qp, &send_wr, &bad_send_wr);
			if (rc != 0) {
				SPDK_ERRLOG("post send failed\n");
				break;
			}
			tdisk->rdma_status = TARGET_RDMA_ESTABLISHED;
			break;
		}
		case TARGET_RDMA_ESTABLISHED:
		{
			struct ibv_wc wc;
			struct rdma_handshake* handshake = tdisk->remote_handshake - 1;
			int cnt = ibv_poll_cq(tdisk->cq, 1, &wc);
			if (cnt < 0) {
				SPDK_ERRLOG("ibv_poll_cq failed\n");
				return SPDK_POLLER_IDLE;
			}

			if (cnt == 0) {
				return SPDK_POLLER_IDLE;
			}

			if (wc.status != IBV_WC_SUCCESS) {
				SPDK_ERRLOG("WC bad status %d\n", wc.status);
				return SPDK_POLLER_IDLE;
			}

			if (wc.wr_id == 1) {
				// recv complete
				SPDK_NOTICELOG("received remote addr %p rkey %d block_cnt %ld block_size %ld\n",
					tdisk->remote_handshake->base_addr,
					tdisk->remote_handshake->rkey,
					tdisk->remote_handshake->block_cnt,
					tdisk->remote_handshake->block_size);
				
				if (tdisk->remote_handshake->block_cnt != handshake->block_cnt ||
					tdisk->remote_handshake->block_size != handshake->block_size) {
					SPDK_ERRLOG("buffer config handshake mismatch\n");
					return -EINVAL;
				}
				SPDK_NOTICELOG("rdma handshake complete\n");
				tdisk->rdma_status = TARGET_RDMA_CONNECTED;
			}
			else if (wc.wr_id == 2) {
				SPDK_NOTICELOG("send req complete\n");
			}
			break;
		}
		case TARGET_RDMA_ERROR:
		{
			SPDK_NOTICELOG("In error state. Cannot recover by now\n");
			break;
		}
	}

	return SPDK_POLLER_BUSY;
}

static int
target_create_channel_cb(void *io_device, void *ctx)
{
	SPDK_DEBUGLOG(bdev_target, "enter\n");
	struct target_disk* tdisk = io_device;
	struct target_channel *ch = ctx;
	ch->tdisk = tdisk;

	return 0;
}

static void
target_destroy_channel_cb(void *io_device, void *ctx)
{
	SPDK_DEBUGLOG(bdev_target, "enter\n");
}


int
create_target_disk(struct spdk_bdev **bdev, const char *name, const char* ip, const char* port,
			const struct spdk_uuid *uuid, uint64_t num_blocks, uint32_t block_size, uint32_t optimal_io_boundary, bool has_md)
{
	SPDK_DEBUGLOG(bdev_target, "in create disk\n");
	SPDK_NOTICELOG("has_md = %d\n", has_md);
	struct target_disk	*tdisk;
	int rc;

	if (num_blocks == 0) {
		SPDK_ERRLOG("Disk num_blocks must be greater than 0");
		return -EINVAL;
	}

	if (block_size % 512) {
		SPDK_ERRLOG("block size must be 512 bytes aligned\n");
		return -EINVAL;
	}

	tdisk = calloc(1, sizeof(*tdisk));
	if (!tdisk) {
		SPDK_ERRLOG("tdisk calloc() failed\n");
		return -ENOMEM;
	}

	/*
	 * Allocate the large backend memory buffer from pinned memory.
	 *
	 * TODO: need to pass a hint so we know which socket to allocate
	 *  from on multi-socket systems.
	 */
	tdisk->malloc_buf = spdk_zmalloc(num_blocks * block_size + 2 * sizeof(struct rdma_handshake), 2 * 1024 * 1024, NULL,
					 SPDK_ENV_LCORE_ID_ANY, SPDK_MALLOC_DMA);
	if (!tdisk->malloc_buf) {
		SPDK_ERRLOG("malloc_buf spdk_zmalloc() failed\n");
		target_disk_free(tdisk);
		return -ENOMEM;
	}
	struct rdma_event_channel* rdma_channel = rdma_create_event_channel();
	tdisk->rdma_channel = rdma_channel;

	int flags = fcntl(rdma_channel->fd, F_GETFL);
	rc = fcntl(rdma_channel->fd, F_SETFL, flags | O_NONBLOCK);
	if (rc != 0) {
		SPDK_ERRLOG("fcntl failed\n");
		return -EINVAL;
	}
	tdisk->rdma_status = TARGET_RDMA_INITIALIZED;

	struct sockaddr_in addr;
	struct addrinfo hints = {};
	struct addrinfo* addr_res = NULL;
	hints.ai_family = AF_INET;
	hints.ai_flags = AI_PASSIVE;

	getaddrinfo(ip, port, &hints, &addr_res);
	memcpy(&addr, addr_res->ai_addr, sizeof(addr));

	tdisk->server_addr = addr_res;

	if (name) {
		tdisk->disk.name = strdup(name);
	} else {
		/* Auto-generate a name */
		tdisk->disk.name = spdk_sprintf_alloc("Target%d", target_disk_count);
		target_disk_count++;
	}
	if (!tdisk->disk.name) {
		target_disk_free(tdisk);
		return -ENOMEM;
	}
	tdisk->disk.product_name = "Target disk";

	tdisk->disk.write_cache = 1;
	tdisk->disk.blocklen = block_size;
	if (has_md) {
		tdisk->disk.md_len = block_size;
		tdisk->disk.md_interleave = false;
		tdisk->disk.dif_type = SPDK_DIF_DISABLE;
	}
	tdisk->disk.blockcnt = num_blocks;

	if (optimal_io_boundary) {
		tdisk->disk.optimal_io_boundary = optimal_io_boundary;
		tdisk->disk.split_on_optimal_io_boundary = true;
	}
	if (uuid) {
		tdisk->disk.uuid = *uuid;
	} else {
		spdk_uuid_generate(&tdisk->disk.uuid);
	}

	tdisk->disk.ctxt = tdisk;
	tdisk->disk.fn_table = &target_fn_table;
	tdisk->disk.module = &target_if;

	*bdev = &(tdisk->disk);

	// TAILQ_INSERT_TAIL(&g_target_disks, tdisk, link);

	/* This needs to be reset for each reinitialization of submodules.
	 * Otherwise after enough devices or reinitializations the value gets too high.
	 * TODO: Make malloc bdev name mandatory and remove this counter. */
	target_disk_count = 0;

	spdk_io_device_register(tdisk, target_create_channel_cb,
				target_destroy_channel_cb, sizeof(struct target_channel),
				"bdev_target");

	tdisk->cq_poller = SPDK_POLLER_REGISTER(target_cq_poller, tdisk, 0);
	if (!tdisk->cq_poller) {
		SPDK_ERRLOG("Failed to register target rdma poller\n");
		return -ENOMEM;
	}

	tdisk->reconnect_poller = SPDK_POLLER_REGISTER(target_reconnect_poller, tdisk, 1000 * 1000);
	if (!tdisk->reconnect_poller) {
		SPDK_ERRLOG("Failed to register target reconnect poller\n");
		return -ENOMEM;
	}

	SPDK_DEBUGLOG(bdev_target, "before reg\n");
	rc = spdk_bdev_register(&tdisk->disk);
	if (rc) {
		target_disk_free(tdisk);
		return rc;
	}
	SPDK_DEBUGLOG(bdev_target, "after reg\n");

	SPDK_DEBUGLOG(bdev_target, "leave create disk\n");

	return rc;
}

void
delete_target_disk(const char *name, spdk_delete_target_complete cb_fn, void *cb_arg)
{
	int rc;

	rc = spdk_bdev_unregister_by_name(name, &target_if, cb_fn, cb_arg);
	if (rc != 0) {
		cb_fn(cb_arg, rc);
	}
}

static int bdev_target_initialize(void)
{
	return 0;
}

static void
bdev_target_deinitialize(void)
{
	// spdk_io_device_unregister(&g_target_disks, NULL);
}

SPDK_LOG_REGISTER_COMPONENT(bdev_target)
