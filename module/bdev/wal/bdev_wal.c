/*-
 *   BSD LICENSE
 *
 *   Copyright (c) Intel Corporation.
 *   All rights reserved.
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

#include "bdev_wal.h"
#include "spdk/env.h"
#include "spdk/thread.h"
#include "spdk/log.h"
#include "spdk/string.h"
#include "spdk/util.h"
#include "spdk/json.h"
#include "spdk/string.h"

static bool g_shutdown_started = false;

/* wal bdev config as read from config file */
struct wal_config	g_wal_config = {
	.wal_bdev_config_head = TAILQ_HEAD_INITIALIZER(g_wal_config.wal_bdev_config_head),
};

/*
 * List of wal bdev in configured list, these wal bdevs are registered with
 * bdev layer
 */
struct wal_configured_tailq	g_wal_bdev_configured_list = TAILQ_HEAD_INITIALIZER(
			g_wal_bdev_configured_list);

/* List of wal bdev in configuring list */
struct wal_configuring_tailq	g_wal_bdev_configuring_list = TAILQ_HEAD_INITIALIZER(
			g_wal_bdev_configuring_list);

/* List of all wal bdevs */
struct wal_all_tailq		g_wal_bdev_list = TAILQ_HEAD_INITIALIZER(g_wal_bdev_list);

/* List of all wal bdevs that are offline */
struct wal_offline_tailq	g_wal_bdev_offline_list = TAILQ_HEAD_INITIALIZER(
			g_wal_bdev_offline_list);

static TAILQ_HEAD(, wal_bdev_module) g_wal_modules = TAILQ_HEAD_INITIALIZER(g_wal_modules);

/* Function declarations */
static void	wal_bdev_examine(struct spdk_bdev *bdev);
static int	wal_bdev_start(struct wal_bdev *bdev);
static void	wal_bdev_stop(struct wal_bdev *bdev);
static int	wal_bdev_init(void);
static void	wal_bdev_deconfigure(struct wal_bdev *wal_bdev,
				      wal_bdev_destruct_cb cb_fn, void *cb_arg);
static void	wal_bdev_event_base_bdev(enum spdk_bdev_event_type type, struct spdk_bdev *bdev,
		void *event_ctx);

/*
 * brief:
 * wal_bdev_create_cb function is a cb function for wal bdev which creates the
 * hierarchy from wal bdev to base bdev io channels. It will be called per core
 * params:
 * io_device - pointer to wal bdev io device represented by wal_bdev
 * ctx_buf - pointer to context buffer for wal bdev io channel
 * returns:
 * 0 - success
 * non zero - failure
 */
static int
wal_bdev_create_cb(void *io_device, void *ctx_buf)
{
	struct wal_bdev            *wal_bdev = io_device;
	struct wal_bdev_io_channel *wal_ch = ctx_buf;
	uint8_t i;

	SPDK_DEBUGLOG(bdev_wal, "wal_bdev_create_cb, %p\n", wal_ch);

	assert(wal_bdev != NULL);
	assert(wal_bdev->state == WAL_BDEV_STATE_ONLINE);

	wal_ch->log_channel = spdk_bdev_get_io_channel(wal_bdev->log_bdev_info.desc);
	if (!wal_ch->log_channel) {
		SPDK_ERRLOG("Unable to create io channel for log bdev\n");
		return -ENOMEM;
	}
	
	wal_ch->core_channel = spdk_bdev_get_io_channel(wal_bdev->core_bdev_info.desc);
	if (!wal_ch->core_channel) {
		spdk_put_io_channel(wal_ch->log_channel);
		SPDK_ERRLOG("Unable to create io channel for core bdev\n");
		return -ENOMEM;
	}

	return 0;
}

/*
 * brief:
 * wal_bdev_destroy_cb function is a cb function for wal bdev which deletes the
 * hierarchy from wal bdev to base bdev io channels. It will be called per core
 * params:
 * io_device - pointer to wal bdev io device represented by wal_bdev
 * ctx_buf - pointer to context buffer for wal bdev io channel
 * returns:
 * none
 */
static void
wal_bdev_destroy_cb(void *io_device, void *ctx_buf)
{
	struct wal_bdev_io_channel *wal_ch = ctx_buf;
	uint8_t i;

	SPDK_DEBUGLOG(bdev_wal, "wal_bdev_destroy_cb\n");

	assert(wal_ch != NULL);
	assert(wal_ch->log_channel);
	assert(wal_ch->core_channel);
	spdk_put_io_channel(wal_ch->log_channel);
	spdk_put_io_channel(wal_ch->core_channel);
	wal_ch->log_channel = NULL;
	wal_ch->core_channel = NULL;
}

/*
 * brief:
 * wal_bdev_cleanup is used to cleanup and free wal_bdev related data
 * structures.
 * params:
 * wal_bdev - pointer to wal_bdev
 * returns:
 * none
 */
static void
wal_bdev_cleanup(struct wal_bdev *wal_bdev)
{
	SPDK_DEBUGLOG(bdev_wal, "wal_bdev_cleanup, %p name %s, state %u, config %p\n",
		      wal_bdev,
		      wal_bdev->bdev.name, wal_bdev->state, wal_bdev->config);
	if (wal_bdev->state == WAL_BDEV_STATE_CONFIGURING) {
		TAILQ_REMOVE(&g_wal_bdev_configuring_list, wal_bdev, state_link);
	} else if (wal_bdev->state == WAL_BDEV_STATE_OFFLINE) {
		TAILQ_REMOVE(&g_wal_bdev_offline_list, wal_bdev, state_link);
	} else {
		assert(0);
	}
	TAILQ_REMOVE(&g_wal_bdev_list, wal_bdev, global_link);
	free(wal_bdev->bdev.name);
	free(wal_bdev->base_bdev_info);
	if (wal_bdev->config) {
		wal_bdev->config->wal_bdev = NULL;
	}
	free(wal_bdev);
}

/*
 * brief:
 * wrapper for the bdev close operation
 * params:
 * base_info - wal base bdev info
 * returns:
 */
static void
_wal_bdev_free_base_bdev_resource(void *ctx)
{
	struct spdk_bdev_desc *desc = ctx;

	spdk_bdev_close(desc);
}


/*
 * brief:
 * free resource of base bdev for wal bdev
 * params:
 * wal_bdev - pointer to wal bdev
 * base_info - wal base bdev info
 * returns:
 * 0 - success
 * non zero - failure
 */
static void
wal_bdev_free_base_bdev_resource(struct wal_bdev *wal_bdev,
				  struct wal_base_bdev_info *base_info)
{
	spdk_bdev_module_release_bdev(base_info->bdev);
	if (base_info->thread && base_info->thread != spdk_get_thread()) {
		spdk_thread_send_msg(base_info->thread, _wal_bdev_free_base_bdev_resource, base_info->desc);
	} else {
		spdk_bdev_close(base_info->desc);
	}
	base_info->desc = NULL;
	base_info->bdev = NULL;

	assert(wal_bdev->num_base_bdevs_discovered);
	wal_bdev->num_base_bdevs_discovered--;
}

/*
 * brief:
 * wal_bdev_destruct is the destruct function table pointer for wal bdev
 * params:
 * ctxt - pointer to wal_bdev
 * returns:
 * 0 - success
 * non zero - failure
 */
static int
wal_bdev_destruct(void *ctxt)
{
	struct wal_bdev *wal_bdev = ctxt;
	struct wal_base_bdev_info *base_info;

	SPDK_DEBUGLOG(bdev_wal, "wal_bdev_destruct\n");

	wal_bdev->destruct_called = true;
	WAL_FOR_EACH_BASE_BDEV(wal_bdev, base_info) {
		/*
		 * Close all base bdev descriptors for which call has come from below
		 * layers.  Also close the descriptors if we have started shutdown.
		 */
		if (g_shutdown_started ||
		    ((base_info->remove_scheduled == true) &&
		     (base_info->bdev != NULL))) {
			wal_bdev_free_base_bdev_resource(wal_bdev, base_info);
		}
	}

	if (g_shutdown_started) {
		TAILQ_REMOVE(&g_wal_bdev_configured_list, wal_bdev, state_link);
		wal_bdev_stop(wal_bdev);
		wal_bdev->state = WAL_BDEV_STATE_OFFLINE;
		TAILQ_INSERT_TAIL(&g_wal_bdev_offline_list, wal_bdev, state_link);
	}

	spdk_io_device_unregister(wal_bdev, NULL);

	if (wal_bdev->num_base_bdevs_discovered == 0) {
		/* Free wal_bdev when there are no base bdevs left */
		SPDK_DEBUGLOG(bdev_wal, "wal bdev base bdevs is 0, going to free all in destruct\n");
		wal_bdev_cleanup(wal_bdev);
	}

	return 0;
}

void
wal_bdev_io_complete(struct wal_bdev_io *wal_io, enum spdk_bdev_io_status status)
{
	struct spdk_bdev_io *bdev_io = spdk_bdev_io_from_ctx(wal_io);

	spdk_bdev_io_complete(bdev_io, status);
}

/*
 * brief:
 * wal_bdev_io_complete_part - signal the completion of a part of the expected
 * base bdev IOs and complete the wal_io if this is the final expected IO.
 * The caller should first set wal_io->base_bdev_io_remaining. This function
 * will decrement this counter by the value of the 'completed' parameter and
 * complete the wal_io if the counter reaches 0. The caller is free to
 * interpret the 'base_bdev_io_remaining' and 'completed' values as needed,
 * it can represent e.g. blocks or IOs.
 * params:
 * wal_io - pointer to wal_bdev_io
 * completed - the part of the wal_io that has been completed
 * status - status of the base IO
 * returns:
 * true - if the wal_io is completed
 * false - otherwise
 */
bool
wal_bdev_io_complete_part(struct wal_bdev_io *wal_io, uint64_t completed,
			   enum spdk_bdev_io_status status)
{
	assert(wal_io->base_bdev_io_remaining >= completed);
	wal_io->base_bdev_io_remaining -= completed;

	if (status != SPDK_BDEV_IO_STATUS_SUCCESS) {
		wal_io->base_bdev_io_status = status;
	}

	if (wal_io->base_bdev_io_remaining == 0) {
		wal_bdev_io_complete(wal_io, wal_io->base_bdev_io_status);
		return true;
	} else {
		return false;
	}
}

/*
 * brief:
 * wal_bdev_queue_io_wait function processes the IO which failed to submit.
 * It will try to queue the IOs after storing the context to bdev wait queue logic.
 * params:
 * wal_io - pointer to wal_bdev_io
 * bdev - the block device that the IO is submitted to
 * ch - io channel
 * cb_fn - callback when the spdk_bdev_io for bdev becomes available
 * returns:
 * none
 */
void
wal_bdev_queue_io_wait(struct wal_bdev_io *wal_io, struct spdk_bdev *bdev,
			struct spdk_io_channel *ch, spdk_bdev_io_wait_cb cb_fn)
{
	wal_io->waitq_entry.bdev = bdev;
	wal_io->waitq_entry.cb_fn = cb_fn;
	wal_io->waitq_entry.cb_arg = wal_io;
	spdk_bdev_queue_io_wait(bdev, ch, &wal_io->waitq_entry);
}

// TODO -
static void
wal_base_bdev_rw_complete(struct spdk_bdev_io *bdev_io, bool success, void *cb_arg)
{
	struct wal_bdev_io *wal_io = cb_arg;

	spdk_bdev_free_io(bdev_io);

	wal_bdev_io_complete_part(wal_io, 1, success ?
				   SPDK_BDEV_IO_STATUS_SUCCESS :
				   SPDK_BDEV_IO_STATUS_FAILED);
}

static void
wal_bdev_submit_read_request(struct wal_bdev_io *wal_io);

static void
_wal_bdev_submit_read_request(void *_wal_io)
{
	struct wal_bdev_io *wal_io = _wal_io;

	wal_bdev_submit_read_request(wal_io);
}

/*
 * brief:
 * wal_bdev_submit_read_request function submits read requests
 * to the first disk; it will submit as many as possible unless a read fails with -ENOMEM, in
 * which case it will queue it for later submission
 * params:
 * wal_io
 * returns:
 * none
 */
static void
wal_bdev_submit_read_request(struct wal_bdev_io *wal_io)
{
	struct spdk_bdev_io		*bdev_io = spdk_bdev_io_from_ctx(wal_io);
	struct wal_bdev		*wal_bdev;
	int				ret;
	struct wal_base_bdev_info	*base_info;
	struct spdk_io_channel		*base_ch;

	wal_bdev = wal_io->wal_bdev;

	if (wal_io->base_bdev_io_remaining == 0) {
		wal_io->base_bdev_io_remaining = 1;
	}

	base_info = &wal_bdev->base_bdev_info[0];
	base_ch = wal_io->wal_ch->base_channel[0];

	ret = spdk_bdev_readv_blocks(base_info->desc, base_ch,
					bdev_io->u.bdev.iovs, bdev_io->u.bdev.iovcnt,
					bdev_io->u.bdev.offset_blocks, bdev_io->u.bdev.num_blocks, wal_base_bdev_rw_complete,
					wal_io);

	if (ret != 0) {
			if (ret == -ENOMEM) {
			wal_bdev_queue_io_wait(wal_io, base_info->bdev, base_ch,
						_wal_bdev_submit_read_request);
			return;
		} else {
			SPDK_ERRLOG("bdev io submit error not due to ENOMEM, it should not happen\n");
			assert(false);
			wal_bdev_io_complete(wal_io, SPDK_BDEV_IO_STATUS_FAILED);
			return;
		}
	}
}

static void
wal_bdev_submit_write_request(struct wal_bdev_io *wal_io);

static void
_wal_bdev_submit_write_request(void *_wal_io)
{
	struct wal_bdev_io *wal_io = _wal_io;

	wal_bdev_submit_write_request(wal_io);
}

/*
 * brief:
 * wal_bdev_submit_write_request function submits write requests
 * to member disks; it will submit as many as possible unless a write fails with -ENOMEM, in
 * which case it will queue it for later submission
 * params:
 * wal_io
 * returns:
 * none
 */
static void
wal_bdev_submit_write_request(struct wal_bdev_io *wal_io)
{
	struct spdk_bdev_io		*bdev_io = spdk_bdev_io_from_ctx(wal_io);
	struct wal_bdev		*wal_bdev;
	int				ret;
	uint8_t				i;
	struct wal_base_bdev_info	*base_info;
	struct spdk_io_channel		*base_ch;

	wal_bdev = wal_io->wal_bdev;

	if (wal_io->base_bdev_io_remaining == 0) {
		wal_io->base_bdev_io_remaining = wal_bdev->num_base_bdevs;
	}

	while (wal_io->base_bdev_io_submitted < wal_bdev->num_base_bdevs) {
		i = wal_io->base_bdev_io_submitted;
		base_info = &wal_bdev->base_bdev_info[i];
		base_ch = wal_io->wal_ch->base_channel[i];

		ret = spdk_bdev_writev_blocks(base_info->desc, base_ch,
						bdev_io->u.bdev.iovs, bdev_io->u.bdev.iovcnt,
						bdev_io->u.bdev.offset_blocks, bdev_io->u.bdev.num_blocks, wal_base_bdev_rw_complete,
						wal_io);

		if (ret == 0) {
			wal_io->base_bdev_io_submitted++;
		} else if (ret == -ENOMEM) {
			wal_bdev_queue_io_wait(wal_io, base_info->bdev, base_ch,
						_wal_bdev_submit_write_request);
			return;
		} else {
			SPDK_ERRLOG("bdev io submit error not due to ENOMEM, it should not happen\n");
			assert(false);
			wal_bdev_io_complete(wal_io, SPDK_BDEV_IO_STATUS_FAILED);
			return;
		}
	}
}

// TODO - end

static void
wal_base_bdev_reset_complete(struct spdk_bdev_io *bdev_io, bool success, void *cb_arg)
{
	struct wal_bdev_io *wal_io = cb_arg;

	spdk_bdev_free_io(bdev_io);

	wal_bdev_io_complete_part(wal_io, 1, success ?
				   SPDK_BDEV_IO_STATUS_SUCCESS :
				   SPDK_BDEV_IO_STATUS_FAILED);
}

static void
wal_bdev_submit_reset_request(struct wal_bdev_io *wal_io);

static void
_wal_bdev_submit_reset_request(void *_wal_io)
{
	struct wal_bdev_io *wal_io = _wal_io;

	wal_bdev_submit_reset_request(wal_io);
}

/*
 * brief:
 * wal_bdev_submit_reset_request function submits reset requests
 * to member disks; it will submit as many as possible unless a reset fails with -ENOMEM, in
 * which case it will queue it for later submission
 * params:
 * wal_io
 * returns:
 * none
 */
static void
wal_bdev_submit_reset_request(struct wal_bdev_io *wal_io)
{
	struct wal_bdev		*wal_bdev;
	int				ret;
	uint8_t				i;
	struct wal_base_bdev_info	*base_info;
	struct spdk_io_channel		*base_ch;

	wal_bdev = wal_io->wal_bdev;

	if (wal_io->base_bdev_io_remaining == 0) {
		wal_io->base_bdev_io_remaining = wal_bdev->num_base_bdevs;
	}

	while (wal_io->base_bdev_io_submitted < wal_bdev->num_base_bdevs) {
		i = wal_io->base_bdev_io_submitted;
		base_info = &wal_bdev->base_bdev_info[i];
		base_ch = wal_io->wal_ch->base_channel[i];
		ret = spdk_bdev_reset(base_info->desc, base_ch,
				      wal_base_bdev_reset_complete, wal_io);
		if (ret == 0) {
			wal_io->base_bdev_io_submitted++;
		} else if (ret == -ENOMEM) {
			wal_bdev_queue_io_wait(wal_io, base_info->bdev, base_ch,
						_wal_bdev_submit_reset_request);
			return;
		} else {
			SPDK_ERRLOG("bdev io submit error not due to ENOMEM, it should not happen\n");
			assert(false);
			wal_bdev_io_complete(wal_io, SPDK_BDEV_IO_STATUS_FAILED);
			return;
		}
	}
}

// TODO - start
static void
wal_base_bdev_null_payload_complete(struct spdk_bdev_io *bdev_io, bool success, void *cb_arg)
{
	struct wal_bdev_io *wal_io = cb_arg;

	spdk_bdev_free_io(bdev_io);

	wal_bdev_io_complete_part(wal_io, 1, success ?
				   SPDK_BDEV_IO_STATUS_SUCCESS :
				   SPDK_BDEV_IO_STATUS_FAILED);
}

static void
wal_bdev_submit_null_payload_request(struct wal_bdev_io *wal_io);

static void
_wal_bdev_submit_null_payload_request(void *_wal_io)
{
	struct wal_bdev_io *wal_io = _wal_io;

	wal_bdev_submit_null_payload_request(wal_io);
}

/*
 * brief:
 * wal_bdev_submit_reset_request function submits reset requests
 * to member disks; it will submit as many as possible unless a reset fails with -ENOMEM, in
 * which case it will queue it for later submission
 * params:
 * wal_io
 * returns:
 * none
 */
static void
wal_bdev_submit_null_payload_request(struct wal_bdev_io *wal_io)
{
	struct spdk_bdev_io		*bdev_io = spdk_bdev_io_from_ctx(wal_io);
	struct wal_bdev		*wal_bdev;
	int				ret;
	uint8_t				i;
	struct wal_base_bdev_info	*base_info;
	struct spdk_io_channel		*base_ch;

	wal_bdev = wal_io->wal_bdev;

	if (wal_io->base_bdev_io_remaining == 0) {
		wal_io->base_bdev_io_remaining = wal_bdev->num_base_bdevs;
	}

	while (wal_io->base_bdev_io_submitted < wal_bdev->num_base_bdevs) {
		i = wal_io->base_bdev_io_submitted;
		base_info = &wal_bdev->base_bdev_info[i];
		base_ch = wal_io->wal_ch->base_channel[i];

		switch (bdev_io->type) {
		case SPDK_BDEV_IO_TYPE_UNMAP:
			ret = spdk_bdev_unmap_blocks(base_info->desc, base_ch,
						     bdev_io->u.bdev.offset_blocks, bdev_io->u.bdev.num_blocks,
						     wal_base_bdev_null_payload_complete, wal_io);
			break;

		case SPDK_BDEV_IO_TYPE_FLUSH:
			ret = spdk_bdev_flush_blocks(base_info->desc, base_ch,
						     bdev_io->u.bdev.offset_blocks, bdev_io->u.bdev.num_blocks,
						     wal_base_bdev_null_payload_complete, wal_io);
			break;

		default:
			SPDK_ERRLOG("submit request, invalid io type with null payload %u\n", bdev_io->type);
			assert(false);
			ret = -EIO;
		}
		
		if (ret == 0) {
			wal_io->base_bdev_io_submitted++;
		} else if (ret == -ENOMEM) {
			wal_bdev_queue_io_wait(wal_io, base_info->bdev, base_ch,
						_wal_bdev_submit_null_payload_request);
			return;
		} else {
			SPDK_ERRLOG("bdev io submit error not due to ENOMEM, it should not happen\n");
			assert(false);
			wal_bdev_io_complete(wal_io, SPDK_BDEV_IO_STATUS_FAILED);
			return;
		}
	}
}
// TODO - end

/*
 * brief:
 * Callback function to spdk_bdev_io_get_buf.
 * params:
 * ch - pointer to wal bdev io channel
 * bdev_io - pointer to parent bdev_io on wal bdev device
 * success - True if buffer is allocated or false otherwise.
 * returns:
 * none
 */
static void
wal_bdev_get_buf_cb(struct spdk_io_channel *ch, struct spdk_bdev_io *bdev_io,
		     bool success)
{
	struct wal_bdev_io *wal_io = (struct wal_bdev_io *)bdev_io->driver_ctx;

	if (!success) {
		wal_bdev_io_complete(wal_io, SPDK_BDEV_IO_STATUS_FAILED);
		return;
	}

	wal_bdev_submit_read_request(wal_io);
}

/*
 * brief:
 * wal_bdev_submit_request function is the submit_request function pointer of
 * wal bdev function table. This is used to submit the io on wal_bdev to below
 * layers.
 * params:
 * ch - pointer to wal bdev io channel
 * bdev_io - pointer to parent bdev_io on wal bdev device
 * returns:
 * none
 */
static void
wal_bdev_submit_request(struct spdk_io_channel *ch, struct spdk_bdev_io *bdev_io)
{
	struct wal_bdev_io *wal_io = (struct wal_bdev_io *)bdev_io->driver_ctx;

	wal_io->wal_bdev = bdev_io->bdev->ctxt;
	wal_io->wal_ch = spdk_io_channel_get_ctx(ch);
	wal_io->base_bdev_io_remaining = 0;
	wal_io->base_bdev_io_submitted = 0;
	wal_io->base_bdev_io_status = SPDK_BDEV_IO_STATUS_SUCCESS;

	switch (bdev_io->type) {
	case SPDK_BDEV_IO_TYPE_READ:
		spdk_bdev_io_get_buf(bdev_io, wal_bdev_get_buf_cb,
				     bdev_io->u.bdev.num_blocks * bdev_io->bdev->blocklen);
		break;
	case SPDK_BDEV_IO_TYPE_WRITE:
		wal_bdev_submit_write_request(wal_io);
		break;

	case SPDK_BDEV_IO_TYPE_RESET:
		wal_bdev_submit_reset_request(wal_io);
		break;

	case SPDK_BDEV_IO_TYPE_FLUSH:
	case SPDK_BDEV_IO_TYPE_UNMAP:
		wal_bdev_submit_null_payload_request(wal_io);
		break;

	default:
		SPDK_ERRLOG("submit request, invalid io type %u\n", bdev_io->type);
		wal_bdev_io_complete(wal_io, SPDK_BDEV_IO_STATUS_FAILED);
		break;
	}
}

/*
 * brief:
 * _wal_bdev_io_type_supported checks whether io_type is supported in
 * all base bdev modules of wal bdev module. If anyone among the base_bdevs
 * doesn't support, the wal device doesn't supports.
 *
 * params:
 * wal_bdev - pointer to wal bdev context
 * io_type - io type
 * returns:
 * true - io_type is supported
 * false - io_type is not supported
 */
inline static bool
_wal_bdev_io_type_supported(struct wal_bdev *wal_bdev, enum spdk_bdev_io_type io_type)
{
	struct wal_base_bdev_info *base_info;

	WAL_FOR_EACH_BASE_BDEV(wal_bdev, base_info) {
		if (base_info->bdev == NULL) {
			assert(false);
			continue;
		}

		if (spdk_bdev_io_type_supported(base_info->bdev, io_type) == false) {
			return false;
		}
	}

	return true;
}

/*
 * brief:
 * wal_bdev_io_type_supported is the io_supported function for bdev function
 * table which returns whether the particular io type is supported or not by
 * wal bdev module
 * params:
 * ctx - pointer to wal bdev context
 * type - io type
 * returns:
 * true - io_type is supported
 * false - io_type is not supported
 */
static bool
wal_bdev_io_type_supported(void *ctx, enum spdk_bdev_io_type io_type)
{
	switch (io_type) {
	case SPDK_BDEV_IO_TYPE_READ:
	case SPDK_BDEV_IO_TYPE_WRITE:
		return true;

	case SPDK_BDEV_IO_TYPE_FLUSH:
	case SPDK_BDEV_IO_TYPE_RESET:
	case SPDK_BDEV_IO_TYPE_UNMAP:
		return _wal_bdev_io_type_supported(ctx, io_type);

	default:
		return false;
	}

	return false;
}

/*
 * brief:
 * wal_bdev_get_io_channel is the get_io_channel function table pointer for
 * wal bdev. This is used to return the io channel for this wal bdev
 * params:
 * ctxt - pointer to wal_bdev
 * returns:
 * pointer to io channel for wal bdev
 */
static struct spdk_io_channel *
wal_bdev_get_io_channel(void *ctxt)
{
	struct wal_bdev *wal_bdev = ctxt;

	return spdk_get_io_channel(wal_bdev);
}

/*
 * brief:
 * wal_bdev_dump_info_json is the function table pointer for wal bdev
 * params:
 * ctx - pointer to wal_bdev
 * w - pointer to json context
 * returns:
 * 0 - success
 * non zero - failure
 */
static int
wal_bdev_dump_info_json(void *ctx, struct spdk_json_write_ctx *w)
{
	struct wal_bdev *wal_bdev = ctx;
	struct wal_base_bdev_info *base_info;

	SPDK_DEBUGLOG(bdev_wal, "wal_bdev_dump_config_json\n");
	assert(wal_bdev != NULL);

	/* Dump the wal bdev configuration related information */
	spdk_json_write_named_object_begin(w, "wal");
	spdk_json_write_named_uint32(w, "state", wal_bdev->state);
	spdk_json_write_named_uint32(w, "destruct_called", wal_bdev->destruct_called);
	spdk_json_write_named_uint32(w, "num_base_bdevs", wal_bdev->num_base_bdevs);
	spdk_json_write_named_uint32(w, "num_base_bdevs_discovered", wal_bdev->num_base_bdevs_discovered);
	spdk_json_write_name(w, "base_bdevs_list");
	spdk_json_write_array_begin(w);
	WAL_FOR_EACH_BASE_BDEV(wal_bdev, base_info) {
		if (base_info->bdev) {
			spdk_json_write_string(w, base_info->bdev->name);
		} else {
			spdk_json_write_null(w);
		}
	}
	spdk_json_write_array_end(w);
	spdk_json_write_object_end(w);

	return 0;
}

/*
 * brief:
 * wal_bdev_write_config_json is the function table pointer for wal bdev
 * params:
 * bdev - pointer to spdk_bdev
 * w - pointer to json context
 * returns:
 * none
 */
static void
wal_bdev_write_config_json(struct spdk_bdev *bdev, struct spdk_json_write_ctx *w)
{
	struct wal_bdev *wal_bdev = bdev->ctxt;
	struct wal_base_bdev_info *base_info;

	spdk_json_write_object_begin(w);

	spdk_json_write_named_string(w, "method", "bdev_wal_create");

	spdk_json_write_named_object_begin(w, "params");
	spdk_json_write_named_string(w, "name", bdev->name);

	spdk_json_write_named_array_begin(w, "base_bdevs");
	WAL_FOR_EACH_BASE_BDEV(wal_bdev, base_info) {
		if (base_info->bdev) {
			spdk_json_write_string(w, base_info->bdev->name);
		}
	}
	spdk_json_write_array_end(w);
	spdk_json_write_object_end(w);

	spdk_json_write_object_end(w);
}

/* g_wal_bdev_fn_table is the function table for wal bdev */
static const struct spdk_bdev_fn_table g_wal_bdev_fn_table = {
	.destruct		= wal_bdev_destruct,
	.submit_request		= wal_bdev_submit_request,
	.io_type_supported	= wal_bdev_io_type_supported,
	.get_io_channel		= wal_bdev_get_io_channel,
	.dump_info_json		= wal_bdev_dump_info_json,
	.write_config_json	= wal_bdev_write_config_json,
};

/*
 * brief:
 * wal_bdev_config_cleanup function is used to free memory for one wal_bdev in configuration
 * params:
 * wal_cfg - pointer to wal_bdev_config structure
 * returns:
 * none
 */
void
wal_bdev_config_cleanup(struct wal_bdev_config *wal_cfg)
{
	uint8_t i;

	TAILQ_REMOVE(&g_wal_config.wal_bdev_config_head, wal_cfg, link);
	g_wal_config.total_wal_bdev--;

	if (wal_cfg->base_bdev) {
		for (i = 0; i < wal_cfg->num_base_bdevs; i++) {
			free(wal_cfg->base_bdev[i].name);
		}
		free(wal_cfg->base_bdev);
	}
	free(wal_cfg->name);
	free(wal_cfg);
}

/*
 * brief:
 * wal_bdev_free is the wal bdev function table function pointer. This is
 * called on bdev free path
 * params:
 * none
 * returns:
 * none
 */
static void
wal_bdev_free(void)
{
	struct wal_bdev_config *wal_cfg, *tmp;

	SPDK_DEBUGLOG(bdev_wal, "wal_bdev_free\n");
	TAILQ_FOREACH_SAFE(wal_cfg, &g_wal_config.wal_bdev_config_head, link, tmp) {
		wal_bdev_config_cleanup(wal_cfg);
	}
}

/* brief
 * wal_bdev_config_find_by_name is a helper function to find wal bdev config
 * by name as key.
 *
 * params:
 * wal_name - name for wal bdev.
 */
struct wal_bdev_config *
wal_bdev_config_find_by_name(const char *wal_name)
{
	struct wal_bdev_config *wal_cfg;

	TAILQ_FOREACH(wal_cfg, &g_wal_config.wal_bdev_config_head, link) {
		if (!strcmp(wal_cfg->name, wal_name)) {
			return wal_cfg;
		}
	}

	return wal_cfg;
}

/*
 * brief
 * wal_bdev_config_add function adds config for newly created wal bdev.
 *
 * params:
 * wal_name - name for wal bdev.
 * num_base_bdevs - number of base bdevs.
 * _wal_cfg - Pointer to newly added configuration
 */
int
wal_bdev_config_add(const char *wal_name, uint8_t num_base_bdevs,
		     struct wal_bdev_config **_wal_cfg)
{
	struct wal_bdev_config *wal_cfg;

	wal_cfg = wal_bdev_config_find_by_name(wal_name);
	if (wal_cfg != NULL) {
		SPDK_ERRLOG("Duplicate wal bdev name found in config file %s\n",
			    wal_name);
		return -EEXIST;
	}

	if (num_base_bdevs == 0) {
		SPDK_ERRLOG("Invalid base device count %u\n", num_base_bdevs);
		return -EINVAL;
	}

	wal_cfg = calloc(1, sizeof(*wal_cfg));
	if (wal_cfg == NULL) {
		SPDK_ERRLOG("unable to allocate memory\n");
		return -ENOMEM;
	}

	wal_cfg->name = strdup(wal_name);
	if (!wal_cfg->name) {
		free(wal_cfg);
		SPDK_ERRLOG("unable to allocate memory\n");
		return -ENOMEM;
	}
	wal_cfg->num_base_bdevs = num_base_bdevs;

	wal_cfg->base_bdev = calloc(num_base_bdevs, sizeof(*wal_cfg->base_bdev));
	if (wal_cfg->base_bdev == NULL) {
		free(wal_cfg->name);
		free(wal_cfg);
		SPDK_ERRLOG("unable to allocate memory\n");
		return -ENOMEM;
	}

	TAILQ_INSERT_TAIL(&g_wal_config.wal_bdev_config_head, wal_cfg, link);
	g_wal_config.total_wal_bdev++;

	*_wal_cfg = wal_cfg;
	return 0;
}

/*
 * brief:
 * wal_bdev_config_add_base_bdev function add base bdev to wal bdev config.
 *
 * params:
 * wal_cfg - pointer to wal bdev configuration
 * base_bdev_name - name of base bdev
 * slot - Position to add base bdev
 */
int
wal_bdev_config_add_base_bdev(struct wal_bdev_config *wal_cfg, const char *base_bdev_name,
			       uint8_t slot)
{
	uint8_t i;
	struct wal_bdev_config *tmp;

	if (slot >= wal_cfg->num_base_bdevs) {
		return -EINVAL;
	}

	TAILQ_FOREACH(tmp, &g_wal_config.wal_bdev_config_head, link) {
		for (i = 0; i < tmp->num_base_bdevs; i++) {
			if (tmp->base_bdev[i].name != NULL) {
				if (!strcmp(tmp->base_bdev[i].name, base_bdev_name)) {
					SPDK_ERRLOG("duplicate base bdev name %s mentioned\n",
						    base_bdev_name);
					return -EEXIST;
				}
			}
		}
	}

	wal_cfg->base_bdev[slot].name = strdup(base_bdev_name);
	if (wal_cfg->base_bdev[slot].name == NULL) {
		SPDK_ERRLOG("unable to allocate memory\n");
		return -ENOMEM;
	}

	return 0;
}

/*
 * brief:
 * wal_bdev_fini_start is called when bdev layer is starting the
 * shutdown process
 * params:
 * none
 * returns:
 * none
 */
static void
wal_bdev_fini_start(void)
{
	SPDK_DEBUGLOG(bdev_wal, "wal_bdev_fini_start\n");
	g_shutdown_started = true;
}

/*
 * brief:
 * wal_bdev_exit is called on wal bdev module exit time by bdev layer
 * params:
 * none
 * returns:
 * none
 */
static void
wal_bdev_exit(void)
{
	SPDK_DEBUGLOG(bdev_wal, "wal_bdev_exit\n");
	wal_bdev_free();
}

/*
 * brief:
 * wal_bdev_get_ctx_size is used to return the context size of bdev_io for wal
 * module
 * params:
 * none
 * returns:
 * size of spdk_bdev_io context for wal
 */
static int
wal_bdev_get_ctx_size(void)
{
	SPDK_DEBUGLOG(bdev_wal, "wal_bdev_get_ctx_size\n");
	return sizeof(struct wal_bdev_io);
}

/*
 * brief:
 * wal_bdev_can_claim_bdev is the function to check if this base_bdev can be
 * claimed by wal bdev or not.
 * params:
 * bdev_name - represents base bdev name
 * _wal_cfg - pointer to wal bdev config parsed from config file
 * base_bdev_slot - if bdev can be claimed, it represents the base_bdev correct
 * slot. This field is only valid if return value of this function is true
 * returns:
 * true - if bdev can be claimed
 * false - if bdev can't be claimed
 */
static bool
wal_bdev_can_claim_bdev(const char *bdev_name, struct wal_bdev_config **_wal_cfg,
			 uint8_t *base_bdev_slot)
{
	struct wal_bdev_config *wal_cfg;
	uint8_t i;

	TAILQ_FOREACH(wal_cfg, &g_wal_config.wal_bdev_config_head, link) {
		for (i = 0; i < wal_cfg->num_base_bdevs; i++) {
			/*
			 * Check if the base bdev name is part of wal bdev configuration.
			 * If match is found then return true and the slot information where
			 * this base bdev should be inserted in wal bdev
			 */
			if (!strcmp(bdev_name, wal_cfg->base_bdev[i].name)) {
				*_wal_cfg = wal_cfg;
				*base_bdev_slot = i;
				return true;
			}
		}
	}

	return false;
}


static struct spdk_bdev_module g_wal_if = {
	.name = "wal",
	.module_init = wal_bdev_init,
	.fini_start = wal_bdev_fini_start,
	.module_fini = wal_bdev_exit,
	.get_ctx_size = wal_bdev_get_ctx_size,
	.examine_config = wal_bdev_examine,
	.async_init = false,
	.async_fini = false,
};
SPDK_BDEV_MODULE_REGISTER(wal, &g_wal_if)

/*
 * brief:
 * wal_bdev_init is the initialization function for wal bdev module
 * params:
 * none
 * returns:
 * 0 - success
 * non zero - failure
 */
static int
wal_bdev_init(void)
{
	return 0;
}

/*
 * brief:
 * wal_bdev_create allocates wal bdev based on passed configuration
 * params:
 * wal_cfg - configuration of wal bdev
 * returns:
 * 0 - success
 * non zero - failure
 */
int
wal_bdev_create(struct wal_bdev_config *wal_cfg)
{
	struct wal_bdev *wal_bdev;
	struct spdk_bdev *wal_bdev_gen;

	wal_bdev = calloc(1, sizeof(*wal_bdev));
	if (!wal_bdev) {
		SPDK_ERRLOG("Unable to allocate memory for wal bdev\n");
		return -ENOMEM;
	}

	wal_bdev->num_base_bdevs = wal_cfg->num_base_bdevs;
	wal_bdev->base_bdev_info = calloc(wal_bdev->num_base_bdevs,
					   sizeof(struct wal_base_bdev_info));
	if (!wal_bdev->base_bdev_info) {
		SPDK_ERRLOG("Unable able to allocate base bdev info\n");
		free(wal_bdev);
		return -ENOMEM;
	}

	wal_bdev->state = WAL_BDEV_STATE_CONFIGURING;
	wal_bdev->config = wal_cfg;

	wal_bdev_gen = &wal_bdev->bdev;

	wal_bdev_gen->name = strdup(wal_cfg->name);
	if (!wal_bdev_gen->name) {
		SPDK_ERRLOG("Unable to allocate name for wal\n");
		free(wal_bdev->base_bdev_info);
		free(wal_bdev);
		return -ENOMEM;
	}

	wal_bdev_gen->product_name = "WAL Volume";
	wal_bdev_gen->ctxt = wal_bdev;
	wal_bdev_gen->fn_table = &g_wal_bdev_fn_table;
	wal_bdev_gen->module = &g_wal_if;
	wal_bdev_gen->write_cache = 0;

	TAILQ_INSERT_TAIL(&g_wal_bdev_configuring_list, wal_bdev, state_link);
	TAILQ_INSERT_TAIL(&g_wal_bdev_list, wal_bdev, global_link);

	wal_cfg->wal_bdev = wal_bdev;

	return 0;
}

/*
 * brief
 * wal_bdev_alloc_base_bdev_resource allocates resource of base bdev.
 * params:
 * wal_bdev - pointer to wal bdev
 * bdev_name - base bdev name
 * base_bdev_slot - position to add base bdev
 * returns:
 * 0 - success
 * non zero - failure
 */
static int
wal_bdev_alloc_base_bdev_resource(struct wal_bdev *wal_bdev, const char *bdev_name,
				   uint8_t base_bdev_slot)
{
	struct spdk_bdev_desc *desc;
	struct spdk_bdev *bdev;
	int rc;

	rc = spdk_bdev_open_ext(bdev_name, true, wal_bdev_event_base_bdev, NULL, &desc);
	if (rc != 0) {
		if (rc != -ENODEV) {
			SPDK_ERRLOG("Unable to create desc on bdev '%s'\n", bdev_name);
		}
		return rc;
	}

	bdev = spdk_bdev_desc_get_bdev(desc);

	rc = spdk_bdev_module_claim_bdev(bdev, NULL, &g_wal_if);
	if (rc != 0) {
		SPDK_ERRLOG("Unable to claim this bdev as it is already claimed\n");
		spdk_bdev_close(desc);
		return rc;
	}

	SPDK_DEBUGLOG(bdev_wal, "bdev %s is claimed\n", bdev_name);

	assert(wal_bdev->state != WAL_BDEV_STATE_ONLINE);
	assert(base_bdev_slot < wal_bdev->num_base_bdevs);

	wal_bdev->base_bdev_info[base_bdev_slot].thread = spdk_get_thread();
	wal_bdev->base_bdev_info[base_bdev_slot].bdev = bdev;
	wal_bdev->base_bdev_info[base_bdev_slot].desc = desc;
	wal_bdev->num_base_bdevs_discovered++;
	assert(wal_bdev->num_base_bdevs_discovered <= wal_bdev->num_base_bdevs);

	return 0;
}

/*
 * brief:
 * If wal bdev config is complete, then only register the wal bdev to
 * bdev layer and remove this wal bdev from configuring list and
 * insert the wal bdev to configured list
 * params:
 * wal_bdev - pointer to wal bdev
 * returns:
 * 0 - success
 * non zero - failure
 */
static int
wal_bdev_configure(struct wal_bdev *wal_bdev)
{
	uint32_t blocklen = 0;
	struct spdk_bdev *wal_bdev_gen;
	struct wal_base_bdev_info *base_info;
	int rc = 0;

	assert(wal_bdev->state == WAL_BDEV_STATE_CONFIGURING);
	assert(wal_bdev->num_base_bdevs_discovered == wal_bdev->num_base_bdevs);

	WAL_FOR_EACH_BASE_BDEV(wal_bdev, base_info) {
		/* Check blocklen for all base bdevs that it should be same */
		if (blocklen == 0) {
			blocklen = base_info->bdev->blocklen;
		} else if (blocklen != base_info->bdev->blocklen) {
			/*
			 * Assumption is that all the base bdevs for any wal bdev should
			 * have same blocklen
			 */
			SPDK_ERRLOG("Blocklen of various bdevs not matching\n");
			return -EINVAL;
		}
	}
	assert(blocklen > 0);

	wal_bdev->blocklen_shift = spdk_u32log2(blocklen);

	wal_bdev_gen = &wal_bdev->bdev;
	wal_bdev_gen->blocklen = blocklen;

	rc = wal_bdev_start(wal_bdev);
	if (rc != 0) {
		SPDK_ERRLOG("wal module startup callback failed\n");
		return rc;
	}
	wal_bdev->state = WAL_BDEV_STATE_ONLINE;
	SPDK_DEBUGLOG(bdev_wal, "io device register %p\n", wal_bdev);
	SPDK_DEBUGLOG(bdev_wal, "blockcnt %" PRIu64 ", blocklen %u\n",
		      wal_bdev_gen->blockcnt, wal_bdev_gen->blocklen);
	spdk_io_device_register(wal_bdev, wal_bdev_create_cb, wal_bdev_destroy_cb,
				sizeof(struct wal_bdev_io_channel),
				wal_bdev->bdev.name);
	rc = spdk_bdev_register(wal_bdev_gen);
	if (rc != 0) {
		SPDK_ERRLOG("Unable to register wal bdev and stay at configuring state\n");
		wal_bdev_stop(wal_bdev);
		spdk_io_device_unregister(wal_bdev, NULL);
		wal_bdev->state = WAL_BDEV_STATE_CONFIGURING;
		return rc;
	}
	SPDK_DEBUGLOG(bdev_wal, "wal bdev generic %p\n", wal_bdev_gen);
	TAILQ_REMOVE(&g_wal_bdev_configuring_list, wal_bdev, state_link);
	TAILQ_INSERT_TAIL(&g_wal_bdev_configured_list, wal_bdev, state_link);
	SPDK_DEBUGLOG(bdev_wal, "wal bdev is created with name %s, wal_bdev %p\n",
		      wal_bdev_gen->name, wal_bdev);

	return 0;
}

/*
 * brief:
 * If wal bdev is online and registered, change the bdev state to
 * configuring and unregister this wal device. Queue this wal device
 * in configuring list
 * params:
 * wal_bdev - pointer to wal bdev
 * cb_fn - callback function
 * cb_arg - argument to callback function
 * returns:
 * none
 */
static void
wal_bdev_deconfigure(struct wal_bdev *wal_bdev, wal_bdev_destruct_cb cb_fn,
		      void *cb_arg)
{
	if (wal_bdev->state != WAL_BDEV_STATE_ONLINE) {
		if (cb_fn) {
			cb_fn(cb_arg, 0);
		}
		return;
	}

	assert(wal_bdev->num_base_bdevs == wal_bdev->num_base_bdevs_discovered);
	TAILQ_REMOVE(&g_wal_bdev_configured_list, wal_bdev, state_link);
	wal_bdev_stop(wal_bdev);
	wal_bdev->state = WAL_BDEV_STATE_OFFLINE;
	assert(wal_bdev->num_base_bdevs_discovered);
	TAILQ_INSERT_TAIL(&g_wal_bdev_offline_list, wal_bdev, state_link);
	SPDK_DEBUGLOG(bdev_wal, "wal bdev state changing from online to offline\n");

	spdk_bdev_unregister(&wal_bdev->bdev, cb_fn, cb_arg);
}

/*
 * brief:
 * wal_bdev_find_by_base_bdev function finds the wal bdev which has
 *  claimed the base bdev.
 * params:
 * base_bdev - pointer to base bdev pointer
 * _wal_bdev - Reference to pointer to wal bdev
 * _base_info - Reference to the wal base bdev info.
 * returns:
 * true - if the wal bdev is found.
 * false - if the wal bdev is not found.
 */
static bool
wal_bdev_find_by_base_bdev(struct spdk_bdev *base_bdev, struct wal_bdev **_wal_bdev,
			    struct wal_base_bdev_info **_base_info)
{
	struct wal_bdev *wal_bdev;
	struct wal_base_bdev_info *base_info;

	TAILQ_FOREACH(wal_bdev, &g_wal_bdev_list, global_link) {
		WAL_FOR_EACH_BASE_BDEV(wal_bdev, base_info) {
			if (base_info->bdev == base_bdev) {
				*_wal_bdev = wal_bdev;
				*_base_info = base_info;
				return true;
			}
		}
	}

	return false;
}

/*
 * brief:
 * wal_bdev_remove_base_bdev function is called by below layers when base_bdev
 * is removed. This function checks if this base bdev is part of any wal bdev
 * or not. If yes, it takes necessary action on that particular wal bdev.
 * params:
 * base_bdev - pointer to base bdev pointer which got removed
 * returns:
 * none
 */
static void
wal_bdev_remove_base_bdev(struct spdk_bdev *base_bdev)
{
	struct wal_bdev	*wal_bdev = NULL;
	struct wal_base_bdev_info *base_info;

	SPDK_DEBUGLOG(bdev_wal, "wal_bdev_remove_base_bdev\n");

	/* Find the wal_bdev which has claimed this base_bdev */
	if (!wal_bdev_find_by_base_bdev(base_bdev, &wal_bdev, &base_info)) {
		SPDK_ERRLOG("bdev to remove '%s' not found\n", base_bdev->name);
		return;
	}

	assert(base_info->desc);
	base_info->remove_scheduled = true;

	if (wal_bdev->destruct_called == true ||
	    wal_bdev->state == WAL_BDEV_STATE_CONFIGURING) {
		/*
		 * As wal bdev is not registered yet or already unregistered,
		 * so cleanup should be done here itself.
		 */
		wal_bdev_free_base_bdev_resource(wal_bdev, base_info);
		if (wal_bdev->num_base_bdevs_discovered == 0) {
			/* There is no base bdev for this wal, so free the wal device. */
			wal_bdev_cleanup(wal_bdev);
			return;
		}
	}

	wal_bdev_deconfigure(wal_bdev, NULL, NULL);
}

/*
 * brief:
 * wal_bdev_event_base_bdev function is called by below layers when base_bdev
 * triggers asynchronous event.
 * params:
 * type - event details.
 * bdev - bdev that triggered event.
 * event_ctx - context for event.
 * returns:
 * none
 */
static void
wal_bdev_event_base_bdev(enum spdk_bdev_event_type type, struct spdk_bdev *bdev,
			  void *event_ctx)
{
	switch (type) {
	case SPDK_BDEV_EVENT_REMOVE:
		wal_bdev_remove_base_bdev(bdev);
		break;
	default:
		SPDK_NOTICELOG("Unsupported bdev event: type %d\n", type);
		break;
	}
}

/*
 * brief:
 * Remove base bdevs from the wal bdev one by one.  Skip any base bdev which
 *  doesn't exist.
 * params:
 * wal_cfg - pointer to wal bdev config.
 * cb_fn - callback function
 * cb_ctx - argument to callback function
 */
void
wal_bdev_remove_base_devices(struct wal_bdev_config *wal_cfg,
			      wal_bdev_destruct_cb cb_fn, void *cb_arg)
{
	struct wal_bdev		*wal_bdev;
	struct wal_base_bdev_info	*base_info;

	SPDK_DEBUGLOG(bdev_wal, "wal_bdev_remove_base_devices\n");

	wal_bdev = wal_cfg->wal_bdev;
	if (wal_bdev == NULL) {
		SPDK_DEBUGLOG(bdev_wal, "wal bdev %s doesn't exist now\n", wal_cfg->name);
		if (cb_fn) {
			cb_fn(cb_arg, 0);
		}
		return;
	}

	if (wal_bdev->destroy_started) {
		SPDK_DEBUGLOG(bdev_wal, "destroying wal bdev %s is already started\n",
			      wal_cfg->name);
		if (cb_fn) {
			cb_fn(cb_arg, -EALREADY);
		}
		return;
	}

	wal_bdev->destroy_started = true;

	WAL_FOR_EACH_BASE_BDEV(wal_bdev, base_info) {
		if (base_info->bdev == NULL) {
			continue;
		}

		assert(base_info->desc);
		base_info->remove_scheduled = true;

		if (wal_bdev->destruct_called == true ||
		    wal_bdev->state == WAL_BDEV_STATE_CONFIGURING) {
			/*
			 * As wal bdev is not registered yet or already unregistered,
			 * so cleanup should be done here itself.
			 */
			wal_bdev_free_base_bdev_resource(wal_bdev, base_info);
			if (wal_bdev->num_base_bdevs_discovered == 0) {
				/* There is no base bdev for this wal, so free the wal device. */
				wal_bdev_cleanup(wal_bdev);
				if (cb_fn) {
					cb_fn(cb_arg, 0);
				}
				return;
			}
		}
	}

	wal_bdev_deconfigure(wal_bdev, cb_fn, cb_arg);
}

/*
 * brief:
 * wal_bdev_add_base_device function is the actual function which either adds
 * the nvme base device to existing wal bdev or create a new wal bdev. It also claims
 * the base device and keep the open descriptor.
 * params:
 * wal_cfg - pointer to wal bdev config
 * bdev - pointer to base bdev
 * base_bdev_slot - position to add base bdev
 * returns:
 * 0 - success
 * non zero - failure
 */
static int
wal_bdev_add_base_device(struct wal_bdev_config *wal_cfg, const char *bdev_name,
			  uint8_t base_bdev_slot)
{
	struct wal_bdev	*wal_bdev;
	int			rc;

	wal_bdev = wal_cfg->wal_bdev;
	if (!wal_bdev) {
		SPDK_ERRLOG("WAL bdev '%s' is not created yet\n", wal_cfg->name);
		return -ENODEV;
	}

	rc = wal_bdev_alloc_base_bdev_resource(wal_bdev, bdev_name, base_bdev_slot);
	if (rc != 0) {
		if (rc != -ENODEV) {
			SPDK_ERRLOG("Failed to allocate resource for bdev '%s'\n", bdev_name);
		}
		return rc;
	}

	assert(wal_bdev->num_base_bdevs_discovered <= wal_bdev->num_base_bdevs);

	if (wal_bdev->num_base_bdevs_discovered == wal_bdev->num_base_bdevs) {
		rc = wal_bdev_configure(wal_bdev);
		if (rc != 0) {
			SPDK_ERRLOG("Failed to configure wal bdev\n");
			return rc;
		}
	}

	return 0;
}

/*
 * brief:
 * Add base bdevs to the wal bdev one by one.  Skip any base bdev which doesn't
 *  exist or fails to add. If all base bdevs are successfully added, the wal bdev
 *  moves to the configured state and becomes available. Otherwise, the wal bdev
 *  stays at the configuring state with added base bdevs.
 * params:
 * wal_cfg - pointer to wal bdev config
 * returns:
 * 0 - The wal bdev moves to the configured state or stays at the configuring
 *     state with added base bdevs due to any nonexistent base bdev.
 * non zero - Failed to add any base bdev and stays at the configuring state with
 *            added base bdevs.
 */
int
wal_bdev_add_base_devices(struct wal_bdev_config *wal_cfg)
{
	uint8_t	i;
	int	rc = 0, _rc;

	for (i = 0; i < wal_cfg->num_base_bdevs; i++) {
		_rc = wal_bdev_add_base_device(wal_cfg, wal_cfg->base_bdev[i].name, i);
		if (_rc == -ENODEV) {
			SPDK_DEBUGLOG(bdev_wal, "base bdev %s doesn't exist now\n",
				      wal_cfg->base_bdev[i].name);
		} else if (_rc != 0) {
			SPDK_ERRLOG("Failed to add base bdev %s to WAL bdev %s: %s\n",
				    wal_cfg->base_bdev[i].name, wal_cfg->name,
				    spdk_strerror(-_rc));
			if (rc == 0) {
				rc = _rc;
			}
		}
	}

	return rc;
}

static int
wal_bdev_start(struct wal_bdev *wal_bdev)
{
	uint64_t min_blockcnt = UINT64_MAX;
	struct wal_base_bdev_info *base_info;

	WAL_FOR_EACH_BASE_BDEV(wal_bdev, base_info) {
		/* Calculate minimum block count from all base bdevs */
		min_blockcnt = spdk_min(min_blockcnt, base_info->bdev->blockcnt);
	}

	/*
	 * Take the minimum block count based approach where total block count
	 * of wal bdev is the number of base bdev times the minimum block count
	 * of any base bdev.
	 */
	SPDK_DEBUGLOG(bdev_wal0, "min blockcount %" PRIu64 ",  numbasedev %u, strip size shift %u\n",
		      min_blockcnt, wal_bdev->num_base_bdevs, wal_bdev->strip_size_shift);
	wal_bdev->bdev.blockcnt = min_blockcnt;

	return 0;
}

static void
wal_bdev_stop(struct wal_bdev *wal_bdev)
{
	// flush?
}

/*
 * brief:
 * wal_bdev_examine function is the examine function call by the below layers
 * like bdev_nvme layer. This function will check if this base bdev can be
 * claimed by this wal bdev or not.
 * params:
 * bdev - pointer to base bdev
 * returns:
 * none
 */
static void
wal_bdev_examine(struct spdk_bdev *bdev)
{
	struct wal_bdev_config	*wal_cfg;
	uint8_t			base_bdev_slot;

	if (wal_bdev_can_claim_bdev(bdev->name, &wal_cfg, &base_bdev_slot)) {
		wal_bdev_add_base_device(wal_cfg, bdev->name, base_bdev_slot);
	} else {
		SPDK_DEBUGLOG(bdev_wal, "bdev %s can't be claimed\n",
			      bdev->name);
	}

	spdk_bdev_module_examine_done(&g_wal_if);
}

/* Log component for bdev wal bdev module */
SPDK_LOG_REGISTER_COMPONENT(bdev_wal)
