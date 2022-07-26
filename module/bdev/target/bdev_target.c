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

#include "spdk/bdev_module.h"
#include "spdk/log.h"
#include <infiniband/verbs.h>
#include <rdma/rdma_cma.h>

static struct rdma_handshake {
	void* base_addr;
	uint32_t rkey;
};

struct target_disk {
	struct spdk_bdev		disk;
	// act as circular buffer
	void				*malloc_buf;
	struct rdma_cm_id* cm_id;
	struct ibv_mr* mr;
	struct ibv_cq* cq;
	struct rdma_handshake* local_handshake;
	struct rdma_handshake* remote_handshake;
	struct spdk_poller* rdma_poller;
	struct ibv_wc wc_buf[TARGET_WC_BATCH_SIZE];

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
	int				num_outstanding;
	enum spdk_bdev_io_status	status;
	TAILQ_ENTRY(target_task)	tailq;
};

struct target_channel {
	struct target_disk* tdisk;
};

// static TAILQ_HEAD(, target_disk) g_target_disks = TAILQ_HEAD_INITIALIZER(g_target_disks);

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

	free(target_disk->disk.name);
	spdk_free(target_disk->malloc_buf);
	if (target_disk->local_handshake) {
		spdk_free(target_disk->local_handshake);
	}
	if (target_disk->remote_handshake) {
		spdk_free(target_disk->remote_handshake);
	}
	free(target_disk);
}

static int
bdev_target_destruct(void *ctx)
{
	struct target_disk *target_disk = ctx;

	// TAILQ_REMOVE(&g_target_disks, target_disk, link);
	target_disk_free(target_disk);
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

static void
bdev_target_readv(struct target_disk *mdisk, 
		  struct target_task *task,
		  struct iovec *iov, int iovcnt, size_t len, uint64_t offset)
{
	void *src = mdisk->malloc_buf + offset;

	if (bdev_target_check_iov_len(iov, iovcnt, len)) {
		spdk_bdev_io_complete(spdk_bdev_io_from_ctx(task),
				      SPDK_BDEV_IO_STATUS_FAILED);
		return;
	}

	SPDK_DEBUGLOG(bdev_target, "read %zu bytes from offset %#" PRIx64 ", iovcnt=%d\n",
		      len, offset, iovcnt);

	// simply memcpy from local memory
	// need to use RDMA read if the memory is from Application.
	for (int i = 0; i < iovcnt; i++) {
		memcpy(iov[i].iov_base, src, iov[i].iov_len);
		src += iov[i].iov_len;
	}

	spdk_bdev_io_complete(spdk_bdev_io_from_ctx(task), SPDK_BDEV_IO_STATUS_SUCCESS);
}

static void
bdev_target_writev_with_md(struct target_disk *mdisk, 
		   struct target_task *task,
		   struct wal_metadata* md, struct iovec *iov, int iovcnt, size_t len, uint64_t offset)
{
	// if (offset > 512 * 1024 * 1024) {
	// 	SPDK_ERRLOG("offset %ld is too large\n", offset);
	// 	spdk_bdev_io_complete(spdk_bdev_io_from_ctx(task),
	// 			      SPDK_BDEV_IO_STATUS_FAILED);
	// 	return;
	// }
	void *dst = mdisk->malloc_buf + offset;
	// for RDMA write
	void* rdma_src = dst;
	void* rdma_dst = mdisk->remote_handshake->base_addr + offset;
	int rc;
	struct spdk_bdev_io* bdev_io = spdk_bdev_io_from_ctx(task);
	int cnt = 0;
	
	if (bdev_target_check_iov_len(iov, iovcnt, len)) {
		spdk_bdev_io_complete(spdk_bdev_io_from_ctx(task),
				      SPDK_BDEV_IO_STATUS_FAILED);
		return;
	}

	SPDK_DEBUGLOG(bdev_target, "wrote %zu bytes to offset %#" PRIx64 ", iovcnt=%d\n",
		      len, offset, iovcnt);
	
	task->status = SPDK_BDEV_IO_STATUS_SUCCESS;
	task->num_outstanding = 0;

	spdk_trace_record_tsc(spdk_get_ticks(), TRACE_BDEV_WRITE_MEMCPY_START, 0, 0, (uintptr_t)bdev_io);
	if (md != NULL) {
		memcpy(dst, md, mdisk->disk.md_len);
		dst += mdisk->disk.md_len;
		len += mdisk->disk.md_len;
	}

	for (int i = 0; i < iovcnt; i++) {
		memcpy(dst, iov[i].iov_base, iov[0].iov_len);
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
	wr.wr.rdma.rkey = mdisk->remote_handshake->rkey;

	sge.addr = (uint64_t)rdma_src;
	sge.length = len;
	sge.lkey = mdisk->mr->lkey;

	spdk_trace_record_tsc(spdk_get_ticks(), TRACE_BDEV_RDMA_POST_SEND_WRITE_START, 0, 0, (uintptr_t)bdev_io);
	rc = ibv_post_send(mdisk->cm_id->qp, &wr, &bad_wr);
	spdk_trace_record_tsc(spdk_get_ticks(), TRACE_BDEV_RDMA_POST_SEND_WRITE_END, 0, 0, (uintptr_t)bdev_io);

	if (rc != 0) {
		SPDK_ERRLOG("RDMA write failed with errno = %d\n", rc);
		SPDK_NOTICELOG("Local: %p %d; Remote: %p %d; Len = %d\n",
			(void*)sge.addr, sge.lkey, (void*)wr.wr.rdma.remote_addr, wr.wr.rdma.rkey,
			sge.length);
		spdk_bdev_io_complete(spdk_bdev_io_from_ctx(task),
				      SPDK_BDEV_IO_STATUS_FAILED);
		return;
	}

	while (cnt == 0) {
		spdk_trace_record_tsc(spdk_get_ticks(), TRACE_BDEV_CQ_POLL_START, 0, 0, (uintptr_t)mdisk->cq);
		cnt = ibv_poll_cq(mdisk->cq, 1, mdisk->wc_buf);
		spdk_trace_record_tsc(spdk_get_ticks(), TRACE_BDEV_CQ_POLL_END, 0, 0, (uintptr_t)mdisk->cq);
		for (int i = 0; i < cnt; i++) {
			struct spdk_bdev_io* io = (struct spdk_bdev_io*)mdisk->wc_buf[i].wr_id;
			assert(io == bdev_io);
			spdk_trace_record_tsc(spdk_get_ticks(), TRACE_BDEV_CQ_POLL, 0, 0, (uintptr_t)io);
			// SPDK_NOTICELOG("received io %p\n", io);
			spdk_bdev_io_complete(io, SPDK_BDEV_IO_STATUS_SUCCESS);
		}
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

static void _log_md(struct spdk_bdev_io* bdev_io) {
	SPDK_DEBUGLOG(bdev_target, "iov len = %d, iov[0].len = %ld\n", bdev_io->u.bdev.iovcnt,
	bdev_io->u.bdev.iovs[0].iov_len);

	SPDK_DEBUGLOG(bdev_target, "MD = %s\n", (char*)bdev_io->u.bdev.iovs[0].iov_base);
}

static int _bdev_target_submit_request(struct target_channel *mch, struct spdk_bdev_io *bdev_io)
{
	uint32_t block_size = bdev_io->bdev->blocklen;
	// _log_md(bdev_io);

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
				  (struct target_task *)bdev_io->driver_ctx,
				  bdev_io->u.bdev.iovs,
				  bdev_io->u.bdev.iovcnt,
				  bdev_io->u.bdev.num_blocks * block_size,
				  bdev_io->u.bdev.offset_blocks * block_size);
		return 0;

	case SPDK_BDEV_IO_TYPE_WRITE:
		bdev_target_writev_with_md((struct target_disk *)bdev_io->bdev->ctxt,
				   (struct target_task *)bdev_io->driver_ctx,
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

// static bool
// probe_cb(void *cb_ctx, const struct spdk_nvme_transport_id *trid,
// 	 struct spdk_nvme_ctrlr_opts *opts)
// {
// 	SPDK_DEBUGLOG(bdev_target, "Attaching to %s\n", trid->traddr);

// 	return true;
// }

/*
 * Callback when a nvme controller is returned.
 * Find the namespace of the controller.
 */
// static void
// attach_cb(void *cb_ctx, const struct spdk_nvme_transport_id *trid,
// 	  struct spdk_nvme_ctrlr *ctrlr, const struct spdk_nvme_ctrlr_opts *opts)
// {
// 	int nsid;
// 	struct spdk_nvme_ns *ns;
// 	struct target_disk* disk = (struct target_disk*)cb_ctx;

// 	SPDK_DEBUGLOG(bdev_target, "Attached to %s\n", trid->traddr);
// 	disk->ns_entry.ctrlr = ctrlr;

// 	/*
// 	 * Each controller has one or more namespaces.  An NVMe namespace is basically
// 	 *  equivalent to a SCSI LUN.  The controller's IDENTIFY data tells us how
// 	 *  many namespaces exist on the controller.  For Intel(R) P3X00 controllers,
// 	 *  it will just be one namespace.
// 	 *
// 	 * Note that in NVMe, namespace IDs start at 1, not 0.
// 	 */
// 	int num_ns = 0;
// 	for (nsid = spdk_nvme_ctrlr_get_first_active_ns(ctrlr); nsid != 0;
// 	     nsid = spdk_nvme_ctrlr_get_next_active_ns(ctrlr, nsid)) {
// 		ns = spdk_nvme_ctrlr_get_ns(ctrlr, nsid);
// 		if (ns == NULL) {
// 			continue;
// 		}
// 		SPDK_DEBUGLOG(bdev_target, "  Namespace ID: %d size: %juGB\n", spdk_nvme_ns_get_id(ns),
// 	       spdk_nvme_ns_get_size(ns) / 1000000000);
// 		disk->ns_entry.ns = ns;
// 		num_ns++;
// 	}

// 	if (num_ns != 1) {
// 		SPDK_ERRLOG("Unexpected # of namespaces %d\n", num_ns);
// 	}
// }

static int
target_rdma_poller(void *ctx)
{
	struct target_disk *tdisk = ctx;

	// TODO: batch polling may be faster?
	int cnt = ibv_poll_cq(tdisk->cq, 1, tdisk->wc_buf);
	if (cnt < 0) {
		// TODO: what to do when poll cq fails?
		SPDK_ERRLOG("ibv_poll_cq failed\n");
	}
	else if (cnt == 0) {
		SPDK_DEBUGLOG(bdev_target, "no item in cq\n");
	}
	else {
		for (int i = 0; i < cnt; i++) {
			struct spdk_bdev_io* io = (struct spdk_bdev_io*)tdisk->wc_buf[i].wr_id;
			spdk_trace_record_tsc(spdk_get_ticks(), TRACE_BDEV_CQ_POLL, 0, 0, (uintptr_t)io);
			// SPDK_NOTICELOG("received io %p\n", io);
			spdk_bdev_io_complete(io, SPDK_BDEV_IO_STATUS_SUCCESS);
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

	// tdisk->rdma_poller = SPDK_POLLER_REGISTER(target_rdma_poller, tdisk, 0);
	// if (!tdisk->rdma_poller) {
	// 	SPDK_ERRLOG("Failed to register target rdma poller\n");
	// 	return -ENOMEM;
	// }

	return 0;
}

static void
target_destroy_channel_cb(void *io_device, void *ctx)
{
	struct target_disk *tdisk = io_device;

	//spdk_poller_unregister(&tdisk->rdma_poller);
}


int
create_target_disk(struct spdk_bdev **bdev, const char *name, const char* ip, const char* port,
			const struct spdk_uuid *uuid, uint64_t num_blocks, uint32_t block_size, uint32_t optimal_io_boundary)
{
	SPDK_DEBUGLOG(bdev_target, "in create disk\n");
	struct target_disk	*mdisk;
	int rc;

	if (num_blocks == 0) {
		SPDK_ERRLOG("Disk num_blocks must be greater than 0");
		return -EINVAL;
	}

	if (block_size % 512) {
		SPDK_ERRLOG("block size must be 512 bytes aligned\n");
		return -EINVAL;
	}

	mdisk = calloc(1, sizeof(*mdisk));
	if (!mdisk) {
		SPDK_ERRLOG("mdisk calloc() failed\n");
		return -ENOMEM;
	}

	/*
	 * Allocate the large backend memory buffer from pinned memory.
	 *
	 * TODO: need to pass a hint so we know which socket to allocate
	 *  from on multi-socket systems.
	 */
	mdisk->malloc_buf = spdk_zmalloc(num_blocks * block_size + 1, 2 * 1024 * 1024, NULL,
					 SPDK_ENV_SOCKET_ID_ANY, SPDK_MALLOC_DMA);
	mdisk->local_handshake = spdk_zmalloc(sizeof(*mdisk->local_handshake), 0, NULL, SPDK_ENV_SOCKET_ID_ANY, SPDK_MALLOC_DMA);
	mdisk->remote_handshake = spdk_zmalloc(sizeof(*mdisk->remote_handshake), 0, NULL, SPDK_ENV_SOCKET_ID_ANY, SPDK_MALLOC_DMA);
	if (!mdisk->malloc_buf || !mdisk->local_handshake || !mdisk->remote_handshake) {
		SPDK_ERRLOG("malloc_buf spdk_zmalloc() failed\n");
		target_disk_free(mdisk);
		return -ENOMEM;
	}

	struct rdma_event_channel* rdma_channel = rdma_create_event_channel();

	struct rdma_cm_id* cm_id = NULL;
	
	rdma_create_id(rdma_channel, &cm_id, NULL, RDMA_PS_TCP);
	struct sockaddr_in addr;
	struct addrinfo hints = {};
	struct addrinfo* addr_res = NULL;
	hints.ai_family = AF_INET;
	hints.ai_flags = AI_PASSIVE;

	getaddrinfo(ip, port, &hints, &addr_res);
	memcpy(&addr, addr_res->ai_addr, sizeof(addr));

	rdma_resolve_addr(cm_id, NULL, addr_res->ai_addr, 1000);
	struct rdma_cm_event* resolve_addr_event, *resolve_route_event, *connect_event;
	rdma_get_cm_event(rdma_channel, &resolve_addr_event);
	if (resolve_addr_event->event != RDMA_CM_EVENT_ADDR_RESOLVED) {
		SPDK_ERRLOG("invalid event type\n");
		return -EINVAL;
	}
	assert(cm_id == resolve_addr_event->id);
	rdma_ack_cm_event(resolve_addr_event);

	rdma_resolve_route(cm_id, 1000);
	rdma_get_cm_event(rdma_channel, &resolve_route_event);
	if (resolve_route_event->event != RDMA_CM_EVENT_ROUTE_RESOLVED) {
		SPDK_ERRLOG("invalid event type\n");
		return -EINVAL;
	}
	assert(cm_id == resolve_route_event->id);
	rdma_ack_cm_event(resolve_route_event);

	struct ibv_context* ibv_context = cm_id->verbs;
	struct ibv_device_attr device_attr = {};
	ibv_query_device(ibv_context, &device_attr);
	SPDK_DEBUGLOG(bdev_target, "max wr sge = %d, max wr num = %d, max cqe = %d\n",
		device_attr.max_sge, device_attr.max_qp_wr, device_attr.max_cqe);
	struct ibv_pd* ibv_pd = ibv_alloc_pd(ibv_context);
	struct ibv_mr* data_mr = ibv_reg_mr(ibv_pd,
		mdisk->malloc_buf,
		num_blocks * block_size + 1,
		IBV_ACCESS_LOCAL_WRITE);
	struct ibv_mr* local_handshake_mr = ibv_reg_mr(ibv_pd,
		mdisk->local_handshake,
		sizeof(struct rdma_handshake),
		IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE);
	struct ibv_mr* remote_handshake_mr = ibv_reg_mr(ibv_pd,
		mdisk->remote_handshake,
		sizeof(struct rdma_handshake),
		IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE);

	struct ibv_cq* ibv_cq = ibv_create_cq(ibv_context, 256, NULL, NULL, 0);

	mdisk->cq = ibv_cq;
	mdisk->mr = data_mr;

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

	int x = rdma_create_qp(cm_id, ibv_pd, &init_attr);
	SPDK_DEBUGLOG(bdev_target, "rdma_create_qp returns %d\n", x);

	struct ibv_recv_wr wr, *bad_wr = NULL;
	struct ibv_sge sge, send_sge;

	wr.wr_id = 1;
	wr.next = NULL;
	wr.sg_list = &sge;
	wr.num_sge = 1;

	sge.addr = (uint64_t)mdisk->remote_handshake;
	sge.length = sizeof(struct rdma_handshake);
	sge.lkey = remote_handshake_mr->lkey;

	ibv_post_recv(cm_id->qp, &wr, &bad_wr);

	struct rdma_conn_param conn_param = {};

	conn_param.responder_resources = 16;
	conn_param.initiator_depth = 16;
	conn_param.retry_count = 7;
	conn_param.rnr_retry_count = 7;

	rdma_connect(cm_id, &conn_param);
	rdma_get_cm_event(rdma_channel, &connect_event);
	if (connect_event->event != RDMA_CM_EVENT_ESTABLISHED) {
		SPDK_ERRLOG("invalid event type %d\n", connect_event->event);
		return -EINVAL;
	}

	assert(connect_event->id == cm_id);

	SPDK_DEBUGLOG(bdev_target, "connected. posting send...\n");

	struct ibv_send_wr send_wr, *bad_send_wr = NULL;
	memset(&send_wr, 0, sizeof(send_wr));

	send_wr.wr_id = 2;
	send_wr.opcode = IBV_WR_SEND;
	send_wr.sg_list = &send_sge;
	send_wr.num_sge = 1;
	send_wr.send_flags = IBV_SEND_SIGNALED;

	send_sge.addr = (uint64_t)mdisk->local_handshake;
	send_sge.length = sizeof(struct rdma_handshake);
	send_sge.lkey = local_handshake_mr->lkey;

	// TODO: use separate buffer
	mdisk->local_handshake->base_addr = mdisk->malloc_buf;
	mdisk->local_handshake->rkey = data_mr->rkey;

	SPDK_NOTICELOG("sending local addr %p rkey %d\n", mdisk->local_handshake->base_addr, mdisk->local_handshake->rkey);
	ibv_post_send(cm_id->qp, &send_wr, &bad_send_wr);
	struct ibv_wc wc;
	bool handshake_send_cpl = false;
	bool handshake_recv_cpl = false;
	while (!handshake_send_cpl || !handshake_recv_cpl)
	{
		int ret = ibv_poll_cq(ibv_cq, 1, &wc);
		if (ret < 0) {
			SPDK_ERRLOG("ibv_poll_cq failed\n");
			return -EINVAL;
		}

		if (ret == 0) {
			continue;
		}

		if (wc.wr_id == 1) {
			// recv complete
			SPDK_NOTICELOG("received remote addr %p rkey %d\n", mdisk->remote_handshake->base_addr, mdisk->remote_handshake->rkey);
			handshake_recv_cpl = true;
		}
		else if (wc.wr_id == 2) {
			SPDK_NOTICELOG("send req complete\n");
			handshake_send_cpl = true;
		}
	}

	SPDK_DEBUGLOG(bdev_target, "rdma handshake complete\n");

	// /*
	//  * Attach a nvme controller locally
	//  */
	// spdk_nvme_trid_populate_transport(&trid, SPDK_NVME_TRANSPORT_PCIE);
	// snprintf(trid.subnqn, sizeof(trid.subnqn), "%s", SPDK_NVMF_DISCOVERY_NQN);
	// SPDK_DEBUGLOG(bdev_target, "before probe\n");

	// rc = spdk_nvme_probe(&trid, mdisk, probe_cb, attach_cb, NULL);
	// if (rc != 0) {
	// 	SPDK_ERRLOG("spdk_nvme_probe() failed");
	// 	rc = 1;
	// 	return rc;
	// }

	// mdisk->ns_entry.qpair = spdk_nvme_ctrlr_alloc_io_qpair(mdisk->ns_entry.ctrlr, NULL, 0);
	// if (mdisk->ns_entry.qpair == NULL) {
	// 	SPDK_ERRLOG("ERROR: spdk_nvme_ctrlr_alloc_io_qpair() failed");
	// }
	// SPDK_DEBUGLOG(bdev_target, "before alloc qp\n");

	if (name) {
		mdisk->disk.name = strdup(name);
	} else {
		/* Auto-generate a name */
		mdisk->disk.name = spdk_sprintf_alloc("Target%d", target_disk_count);
		target_disk_count++;
	}
	if (!mdisk->disk.name) {
		target_disk_free(mdisk);
		return -ENOMEM;
	}
	mdisk->disk.product_name = "Target disk";

	mdisk->disk.write_cache = 1;
	mdisk->disk.blocklen = block_size;
	mdisk->disk.md_len = block_size;
	mdisk->disk.md_interleave = false;
	mdisk->disk.dif_type = SPDK_DIF_DISABLE;
	mdisk->disk.blockcnt = num_blocks;

	mdisk->cm_id = cm_id;

	if (optimal_io_boundary) {
		mdisk->disk.optimal_io_boundary = optimal_io_boundary;
		mdisk->disk.split_on_optimal_io_boundary = true;
	}
	if (uuid) {
		mdisk->disk.uuid = *uuid;
	} else {
		spdk_uuid_generate(&mdisk->disk.uuid);
	}

	mdisk->disk.ctxt = mdisk;
	mdisk->disk.fn_table = &target_fn_table;
	mdisk->disk.module = &target_if;

	*bdev = &(mdisk->disk);

	// TAILQ_INSERT_TAIL(&g_target_disks, mdisk, link);

	/* This needs to be reset for each reinitialization of submodules.
	 * Otherwise after enough devices or reinitializations the value gets too high.
	 * TODO: Make malloc bdev name mandatory and remove this counter. */
	target_disk_count = 0;

	spdk_io_device_register(mdisk, target_create_channel_cb,
				target_destroy_channel_cb, sizeof(struct target_channel),
				"bdev_target");

	SPDK_DEBUGLOG(bdev_target, "before reg\n");
	rc = spdk_bdev_register(&mdisk->disk);
	if (rc) {
		target_disk_free(mdisk);
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

SPDK_TRACE_REGISTER_FN(target_trace, "target", TRACE_GROUP_BDEV)
{
	struct spdk_trace_tpoint_opts opts[] = {
		{
			"BDEV_IO_START", TRACE_BDEV_IO_START,
			OWNER_BDEV, OBJECT_BDEV_IO, 1,
			{
				{ "type", SPDK_TRACE_ARG_TYPE_INT, 8 },
				{ "ctx", SPDK_TRACE_ARG_TYPE_PTR, 8 },
				{ "offset", SPDK_TRACE_ARG_TYPE_INT, 8 },
				{ "len", SPDK_TRACE_ARG_TYPE_INT, 8 }
			}
		},
		{
			"BDEV_IO_DONE", TRACE_BDEV_IO_DONE,
			OWNER_BDEV, OBJECT_BDEV_IO, 0,
			{{ "ctx", SPDK_TRACE_ARG_TYPE_PTR, 8 }}
		},
		{
			"BDEV_IOCH_CREATE", TRACE_BDEV_IOCH_CREATE,
			OWNER_BDEV, OBJECT_NONE, 1,
			{
				{ "name", SPDK_TRACE_ARG_TYPE_STR, 40 },
				{ "thread_id", SPDK_TRACE_ARG_TYPE_INT, 8}
			}
		},
		{
			"BDEV_IOCH_DESTROY", TRACE_BDEV_IOCH_DESTROY,
			OWNER_BDEV, OBJECT_NONE, 0,
			{
				{ "name", SPDK_TRACE_ARG_TYPE_STR, 40 },
				{ "thread_id", SPDK_TRACE_ARG_TYPE_INT, 8}
			}
		},
		{
			"TARGET_W_MEMCPY_START", TRACE_BDEV_WRITE_MEMCPY_START,
			OWNER_BDEV, OBJECT_BDEV_IO, 1,
			{
			}
		},
		{
			"TARGET_W_MEMCPY_END", TRACE_BDEV_WRITE_MEMCPY_END,
			OWNER_BDEV, OBJECT_BDEV_IO, 0,
			{
			}
		},
		{
			"TARGET_IB_WRITE_START", TRACE_BDEV_RDMA_POST_SEND_WRITE_START,
			OWNER_BDEV, OBJECT_BDEV_IO, 1,
			{
			}
		},
		{
			"TARGET_IB_WRITE_END", TRACE_BDEV_RDMA_POST_SEND_WRITE_END,
			OWNER_BDEV, OBJECT_BDEV_IO, 0,
			{
			}
		},
		{
			"TARGET_CQ_POLL", TRACE_BDEV_CQ_POLL,
			OWNER_BDEV, OBJECT_BDEV_IO, 0,
			{
			}
		},
		{
			"TARGET_CQ_POLL_START", TRACE_BDEV_CQ_POLL_START,
			OWNER_BDEV, OBJECT_BDEV_IO, 1,
			{
			}
		},
		{
			"TARGET_CQ_POLL_END", TRACE_BDEV_CQ_POLL_END,
			OWNER_BDEV, OBJECT_BDEV_IO, 0,
			{
			}
		},
	};

	spdk_trace_register_owner(OWNER_BDEV, 'b');
	spdk_trace_register_object(OBJECT_BDEV_IO, 'i');
	spdk_trace_register_description_ext(opts, SPDK_COUNTOF(opts));
}
