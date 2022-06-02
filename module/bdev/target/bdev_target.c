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

#include "spdk/bdev_module.h"
#include "spdk/log.h"

struct target_ns_entry {
	struct spdk_nvme_ctrlr	*ctrlr;
	struct spdk_nvme_ns	*ns;
	struct spdk_nvme_qpair	*qpair;
};

struct target_disk {
	struct spdk_bdev		disk;
	// act as circular buffer
	void				*malloc_buf;
	// real disk
	struct target_ns_entry ns_entry;

	TAILQ_ENTRY(target_disk)	link;
};

struct target_metadata {
	uint32_t seq_num;
	// offset in the circular buffer, to provide a same memory view for all
	// data nodes
	uint32_t buffer_offset;
	uint32_t checksum;
};

struct target_task {
	int				num_outstanding;
	enum spdk_bdev_io_status	status;
	TAILQ_ENTRY(target_task)	tailq;
};

struct target_channel {
	struct spdk_io_channel		*accel_channel;
	struct spdk_poller		*completion_poller;
	struct spdk_poller* background_poller;
	TAILQ_HEAD(, target_task)	completed_tasks;
};

static void
target_done(void *ref, int status)
{
	struct target_task *task = (struct target_task *)ref;

	if (status != 0) {
		if (status == -ENOMEM) {
			task->status = SPDK_BDEV_IO_STATUS_NOMEM;
		} else {
			task->status = SPDK_BDEV_IO_STATUS_FAILED;
		}
	}

	if (--task->num_outstanding == 0) {
		SPDK_DEBUGLOG(bdev_target, "task complete with status %d\n", task->status);
		spdk_bdev_io_complete(spdk_bdev_io_from_ctx(task), task->status);
	}
}

static void
target_complete_task(struct target_task *task, struct target_channel *mch,
		     enum spdk_bdev_io_status status)
{
	task->status = status;
	TAILQ_INSERT_TAIL(&mch->completed_tasks, task, tailq);
}

static TAILQ_HEAD(, target_disk) g_target_disks = TAILQ_HEAD_INITIALIZER(g_target_disks);

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
	free(target_disk);
}

static int
bdev_target_destruct(void *ctx)
{
	struct target_disk *target_disk = ctx;

	TAILQ_REMOVE(&g_target_disks, target_disk, link);
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
bdev_target_readv(struct target_disk *mdisk, struct spdk_io_channel *ch,
		  struct target_task *task,
		  struct iovec *iov, int iovcnt, size_t len, uint64_t offset)
{
	int64_t res = 0;
	void *src = mdisk->malloc_buf + offset;
	int i;

	if (bdev_target_check_iov_len(iov, iovcnt, len)) {
		spdk_bdev_io_complete(spdk_bdev_io_from_ctx(task),
				      SPDK_BDEV_IO_STATUS_FAILED);
		return;
	}

	SPDK_DEBUGLOG(bdev_target, "read %zu bytes from offset %#" PRIx64 ", iovcnt=%d\n",
		      len, offset, iovcnt);

	task->status = SPDK_BDEV_IO_STATUS_SUCCESS;
	task->num_outstanding = 0;

	for (i = 0; i < iovcnt; i++) {
		SPDK_DEBUGLOG(bdev_target, "submit read task\n");
		task->num_outstanding++;
		res = spdk_accel_submit_copy(ch, iov[i].iov_base,
					     src, iov[i].iov_len, 0, target_done, task);

		if (res != 0) {
			target_done(task, res);
			break;
		}

		src += iov[i].iov_len;
		len -= iov[i].iov_len;
	}
}

static void
bdev_target_writev(struct target_disk *mdisk, struct spdk_io_channel *ch,
		   struct target_task *task,
		   struct iovec *iov, int iovcnt, size_t len, uint64_t offset)
{
	int64_t res = 0;
	void *dst = mdisk->malloc_buf + offset;
	int i;

	if (bdev_target_check_iov_len(iov, iovcnt, len)) {
		spdk_bdev_io_complete(spdk_bdev_io_from_ctx(task),
				      SPDK_BDEV_IO_STATUS_FAILED);
		return;
	}

	SPDK_DEBUGLOG(bdev_target, "wrote %zu bytes to offset %#" PRIx64 ", iovcnt=%d\n",
		      len, offset, iovcnt);
	
	SPDK_DEBUGLOG(bdev_target, "write iov[0] = %s\n", (char*)iov[0].iov_base);

	task->status = SPDK_BDEV_IO_STATUS_SUCCESS;
	task->num_outstanding = 0;

	for (i = 0; i < iovcnt; i++) {
		SPDK_DEBUGLOG(bdev_target, "submit write task\n");
		task->num_outstanding++;
		res = spdk_accel_submit_copy(ch, dst, iov[i].iov_base,
					     iov[i].iov_len, 0, target_done, task);

		if (res != 0) {
			target_done(task, res);
			break;
		}

		dst += iov[i].iov_len;
	}
}

static int
bdev_target_unmap(struct target_disk *mdisk,
		  struct spdk_io_channel *ch,
		  struct target_task *task,
		  uint64_t offset,
		  uint64_t byte_count)
{
	task->status = SPDK_BDEV_IO_STATUS_SUCCESS;
	task->num_outstanding = 1;

	return spdk_accel_submit_fill(ch, mdisk->malloc_buf + offset, 0,
				      byte_count, 0, target_done, task);
}

static void _log_md(struct spdk_bdev_io* bdev_io) {
	SPDK_DEBUGLOG(bdev_target, "iov len = %d, iov[0].len = %ld\n", bdev_io->u.bdev.iovcnt,
	bdev_io->u.bdev.iovs[0].iov_len);

	SPDK_DEBUGLOG(bdev_target, "MD = %s\n", (char*)bdev_io->u.bdev.iovs[0].iov_base);
}

static int _bdev_target_submit_request(struct target_channel *mch, struct spdk_bdev_io *bdev_io)
{
	uint32_t block_size = bdev_io->bdev->blocklen;
	_log_md(bdev_io);
	struct target_metadata* md = (struct target_metadata*) bdev_io->u.bdev.iovs[0].iov_base;
	bdev_io->u.bdev.iovs[0].iov_base += TARGET_MD_LEN;

	if (bdev_io->u.bdev.iovs[0].iov_base == NULL) {
		SPDK_DEBUGLOG(bdev_target, "Received read req where iov_base is null\n");
		return 0;
	}

	switch (bdev_io->type) {
	case SPDK_BDEV_IO_TYPE_READ:
		if (bdev_io->u.bdev.iovs[0].iov_base == NULL) {
			// TODO: in which case will the code enter this branch?
			// in theory, I would read from disk instead.
			SPDK_DEBUGLOG(bdev_target, "Received read req where iov_base is null\n");
			assert(bdev_io->u.bdev.iovcnt == 1);
			bdev_io->u.bdev.iovs[0].iov_base =
				((struct target_disk *)bdev_io->bdev->ctxt)->malloc_buf +
				bdev_io->u.bdev.offset_blocks * block_size;
			bdev_io->u.bdev.iovs[0].iov_len = bdev_io->u.bdev.num_blocks * block_size;
			target_complete_task((struct target_task *)bdev_io->driver_ctx, mch,
					     SPDK_BDEV_IO_STATUS_SUCCESS);
			return 0;
		}

		bdev_target_readv((struct target_disk *)bdev_io->bdev->ctxt,
				  mch->accel_channel,
				  (struct target_task *)bdev_io->driver_ctx,
				  bdev_io->u.bdev.iovs,
				  bdev_io->u.bdev.iovcnt,
				  bdev_io->u.bdev.num_blocks * block_size,
				  bdev_io->u.bdev.offset_blocks * block_size);
		return 0;

	case SPDK_BDEV_IO_TYPE_WRITE:
		bdev_target_writev((struct target_disk *)bdev_io->bdev->ctxt,
				   mch->accel_channel,
				   (struct target_task *)bdev_io->driver_ctx,
				   bdev_io->u.bdev.iovs,
				   bdev_io->u.bdev.iovcnt,
				   bdev_io->u.bdev.num_blocks * block_size,
				   bdev_io->u.bdev.offset_blocks * block_size);
		return 0;

	case SPDK_BDEV_IO_TYPE_RESET:
		target_complete_task((struct target_task *)bdev_io->driver_ctx, mch,
				     SPDK_BDEV_IO_STATUS_SUCCESS);
		return 0;

	case SPDK_BDEV_IO_TYPE_FLUSH:
		target_complete_task((struct target_task *)bdev_io->driver_ctx, mch,
				     SPDK_BDEV_IO_STATUS_SUCCESS);
		return 0;

	case SPDK_BDEV_IO_TYPE_UNMAP:
		return bdev_target_unmap((struct target_disk *)bdev_io->bdev->ctxt,
					 mch->accel_channel,
					 (struct target_task *)bdev_io->driver_ctx,
					 bdev_io->u.bdev.offset_blocks * block_size,
					 bdev_io->u.bdev.num_blocks * block_size);

	case SPDK_BDEV_IO_TYPE_WRITE_ZEROES:
		/* bdev_target_unmap is implemented with a call to mem_cpy_fill which zeroes out all of the requested bytes. */
		return bdev_target_unmap((struct target_disk *)bdev_io->bdev->ctxt,
					 mch->accel_channel,
					 (struct target_task *)bdev_io->driver_ctx,
					 bdev_io->u.bdev.offset_blocks * block_size,
					 bdev_io->u.bdev.num_blocks * block_size);

	case SPDK_BDEV_IO_TYPE_ZCOPY:
		if (bdev_io->u.bdev.zcopy.start) {
			void *buf;
			size_t len;

			buf = ((struct target_disk *)bdev_io->bdev->ctxt)->malloc_buf +
			      bdev_io->u.bdev.offset_blocks * block_size;
			len = bdev_io->u.bdev.num_blocks * block_size;
			spdk_bdev_io_set_buf(bdev_io, buf, len);

		}
		target_complete_task((struct target_task *)bdev_io->driver_ctx, mch,
				     SPDK_BDEV_IO_STATUS_SUCCESS);
		return 0;
	case SPDK_BDEV_IO_TYPE_ABORT:
		target_complete_task((struct target_task *)bdev_io->driver_ctx, mch,
				     SPDK_BDEV_IO_STATUS_FAILED);
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
		target_complete_task((struct target_task *)bdev_io->driver_ctx, mch,
				     SPDK_BDEV_IO_STATUS_FAILED);
	}
}

static bool
bdev_target_io_type_supported(void *ctx, enum spdk_bdev_io_type io_type)
{
	switch (io_type) {
	case SPDK_BDEV_IO_TYPE_READ:
	case SPDK_BDEV_IO_TYPE_WRITE:
	case SPDK_BDEV_IO_TYPE_FLUSH:
	case SPDK_BDEV_IO_TYPE_RESET:
	case SPDK_BDEV_IO_TYPE_UNMAP:
	case SPDK_BDEV_IO_TYPE_WRITE_ZEROES:
	case SPDK_BDEV_IO_TYPE_ZCOPY:
	case SPDK_BDEV_IO_TYPE_ABORT:
		return true;

	default:
		return false;
	}
}

static struct spdk_io_channel *
bdev_target_get_io_channel(void *ctx)
{
	return spdk_get_io_channel(&g_target_disks);
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

static bool
probe_cb(void *cb_ctx, const struct spdk_nvme_transport_id *trid,
	 struct spdk_nvme_ctrlr_opts *opts)
{
	SPDK_DEBUGLOG(bdev_target, "Attaching to %s\n", trid->traddr);

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
	struct target_disk* disk = (struct target_disk*)cb_ctx;

	SPDK_DEBUGLOG(bdev_target, "Attached to %s\n", trid->traddr);
	disk->ns_entry.ctrlr = ctrlr;

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
		SPDK_DEBUGLOG(bdev_target, "  Namespace ID: %d size: %juGB\n", spdk_nvme_ns_get_id(ns),
	       spdk_nvme_ns_get_size(ns) / 1000000000);
		disk->ns_entry.ns = ns;
		num_ns++;
	}

	if (num_ns != 1) {
		SPDK_ERRLOG("Unexpected # of namespaces %d\n", num_ns);
	}
}

int
create_target_disk(struct spdk_bdev **bdev, const char *name, const struct spdk_uuid *uuid,
		   uint64_t num_blocks, uint32_t block_size, uint32_t optimal_io_boundary)
{
	struct target_disk	*mdisk;
	int rc;
	struct spdk_nvme_transport_id trid = {};

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
	mdisk->malloc_buf = spdk_zmalloc(num_blocks * block_size, 2 * 1024 * 1024, NULL,
					 SPDK_ENV_LCORE_ID_ANY, SPDK_MALLOC_DMA);
	if (!mdisk->malloc_buf) {
		SPDK_ERRLOG("malloc_buf spdk_zmalloc() failed\n");
		target_disk_free(mdisk);
		return -ENOMEM;
	}


	/*
	 * Attach a nvme controller locally
	 */
	spdk_nvme_trid_populate_transport(&trid, SPDK_NVME_TRANSPORT_PCIE);
	snprintf(trid.subnqn, sizeof(trid.subnqn), "%s", SPDK_NVMF_DISCOVERY_NQN);

	rc = spdk_nvme_probe(&trid, mdisk, probe_cb, attach_cb, NULL);
	if (rc != 0) {
		SPDK_ERRLOG("spdk_nvme_probe() failed");
		rc = 1;
		return rc;
	}

	mdisk->ns_entry.qpair = spdk_nvme_ctrlr_alloc_io_qpair(mdisk->ns_entry.ctrlr, NULL, 0);
	if (mdisk->ns_entry.qpair == NULL) {
		SPDK_ERRLOG("ERROR: spdk_nvme_ctrlr_alloc_io_qpair() failed");
	}

	if (name) {
		mdisk->disk.name = strdup(name);
	} else {
		/* Auto-generate a name */
		mdisk->disk.name = spdk_sprintf_alloc("Malloc%d", target_disk_count);
		target_disk_count++;
	}
	if (!mdisk->disk.name) {
		target_disk_free(mdisk);
		return -ENOMEM;
	}
	mdisk->disk.product_name = "Target disk";

	mdisk->disk.write_cache = 0;
	mdisk->disk.blocklen = block_size;
	mdisk->disk.md_len = TARGET_MD_LEN;
	mdisk->disk.md_interleave = true;
	mdisk->disk.dif_type = SPDK_DIF_DISABLE;
	mdisk->disk.blockcnt = num_blocks;
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

	rc = spdk_bdev_register(&mdisk->disk);
	if (rc) {
		target_disk_free(mdisk);
		return rc;
	}

	*bdev = &(mdisk->disk);

	TAILQ_INSERT_TAIL(&g_target_disks, mdisk, link);

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

static int
target_completion_poller(void *ctx)
{
	struct target_channel *ch = ctx;
	struct target_task *task;
	TAILQ_HEAD(, target_task) completed_tasks;
	uint32_t num_completions = 0;

	TAILQ_INIT(&completed_tasks);
	TAILQ_SWAP(&completed_tasks, &ch->completed_tasks, target_task, tailq);

	while (!TAILQ_EMPTY(&completed_tasks)) {
		task = TAILQ_FIRST(&completed_tasks);
		TAILQ_REMOVE(&completed_tasks, task, tailq);
		spdk_bdev_io_complete(spdk_bdev_io_from_ctx(task), task->status);
		num_completions++;
	}

	return num_completions > 0 ? SPDK_POLLER_BUSY : SPDK_POLLER_IDLE;
}

static int
target_background_poller(void *ctx)
{
	struct target_channel *ch = ctx;

}

static int
target_create_channel_cb(void *io_device, void *ctx)
{
	struct target_channel *ch = ctx;

	ch->accel_channel = spdk_accel_engine_get_io_channel();
	if (!ch->accel_channel) {
		SPDK_ERRLOG("Failed to get accel engine's IO channel\n");
		return -ENOMEM;
	}

	ch->completion_poller = SPDK_POLLER_REGISTER(target_completion_poller, ch, 0);
	if (!ch->completion_poller) {
		SPDK_ERRLOG("Failed to register target completion poller\n");
		spdk_put_io_channel(ch->accel_channel);
		return -ENOMEM;
	}

	ch->background_poller = SPDK_POLLER_REGISTER(target_background_poller, ch, 0);
	if (!ch->background_poller) {
		SPDK_ERRLOG("Failed to register target background poller\n");
		spdk_put_io_channel(ch->accel_channel);
		return -ENOMEM;
	}

	TAILQ_INIT(&ch->completed_tasks);

	return 0;
}

static void
target_destroy_channel_cb(void *io_device, void *ctx)
{
	struct target_channel *ch = ctx;

	assert(TAILQ_EMPTY(&ch->completed_tasks));

	spdk_put_io_channel(ch->accel_channel);
	spdk_poller_unregister(&ch->completion_poller);
	spdk_poller_unregister(&ch->background_poller);
}

static int bdev_target_initialize(void)
{
	/* This needs to be reset for each reinitialization of submodules.
	 * Otherwise after enough devices or reinitializations the value gets too high.
	 * TODO: Make malloc bdev name mandatory and remove this counter. */
	target_disk_count = 0;

	spdk_io_device_register(&g_target_disks, target_create_channel_cb,
				target_destroy_channel_cb, sizeof(struct target_channel),
				"bdev_target");

	return 0;
}

static void
bdev_target_deinitialize(void)
{
	spdk_io_device_unregister(&g_target_disks, NULL);
}

SPDK_LOG_REGISTER_COMPONENT(bdev_target)
