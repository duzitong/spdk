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

#include "bdev_replica.h"
#include "spdk/env.h"
#include "spdk/thread.h"
#include "spdk/log.h"
#include "spdk/string.h"
#include "spdk/util.h"
#include "spdk/json.h"
#include "spdk/string.h"

static bool g_shutdown_started = false;

/* replica bdev config as read from config file */
struct replica_config	g_replica_config = {
	.replica_bdev_config_head = TAILQ_HEAD_INITIALIZER(g_replica_config.replica_bdev_config_head),
};

/*
 * List of replica bdev in configured list, these replica bdevs are registered with
 * bdev layer
 */
struct replica_configured_tailq	g_replica_bdev_configured_list = TAILQ_HEAD_INITIALIZER(
			g_replica_bdev_configured_list);

/* List of replica bdev in configuring list */
struct replica_configuring_tailq	g_replica_bdev_configuring_list = TAILQ_HEAD_INITIALIZER(
			g_replica_bdev_configuring_list);

/* List of all replica bdevs */
struct replica_all_tailq		g_replica_bdev_list = TAILQ_HEAD_INITIALIZER(g_replica_bdev_list);

/* List of all replica bdevs that are offline */
struct replica_offline_tailq	g_replica_bdev_offline_list = TAILQ_HEAD_INITIALIZER(
			g_replica_bdev_offline_list);

static TAILQ_HEAD(, replica_bdev_module) g_replica_modules = TAILQ_HEAD_INITIALIZER(g_replica_modules);

/* Function declarations */
static void	replica_bdev_examine(struct spdk_bdev *bdev);
static int	replica_bdev_start(struct replica_bdev *bdev);
static void	replica_bdev_stop(struct replica_bdev *bdev);
static int	replica_bdev_init(void);
static void	replica_bdev_deconfigure(struct replica_bdev *replica_bdev,
				      replica_bdev_destruct_cb cb_fn, void *cb_arg);
static void	replica_bdev_event_base_bdev(enum spdk_bdev_event_type type, struct spdk_bdev *bdev,
		void *event_ctx);

/*
 * brief:
 * replica_bdev_create_cb function is a cb function for replica bdev which creates the
 * hierarchy from replica bdev to base bdev io channels. It will be called per core
 * params:
 * io_device - pointer to replica bdev io device represented by replica_bdev
 * ctx_buf - pointer to context buffer for replica bdev io channel
 * returns:
 * 0 - success
 * non zero - failure
 */
static int
replica_bdev_create_cb(void *io_device, void *ctx_buf)
{
	struct replica_bdev            *replica_bdev = io_device;
	struct replica_bdev_io_channel *replica_ch = ctx_buf;
	uint8_t i;

	SPDK_DEBUGLOG(bdev_replica, "replica_bdev_create_cb, %p\n", replica_ch);

	assert(replica_bdev != NULL);
	assert(replica_bdev->state == REPLICA_BDEV_STATE_ONLINE);

	replica_ch->num_channels = replica_bdev->num_base_bdevs;

	replica_ch->base_channel = calloc(replica_ch->num_channels,
				       sizeof(struct spdk_io_channel *));
	if (!replica_ch->base_channel) {
		SPDK_ERRLOG("Unable to allocate base bdevs io channel\n");
		return -ENOMEM;
	}
	for (i = 0; i < replica_ch->num_channels; i++) {
		/*
		 * Get the spdk_io_channel for all the base bdevs. This is used during
		 * split logic to send the respective child bdev ios to respective base
		 * bdev io channel.
		 */
		replica_ch->base_channel[i] = spdk_bdev_get_io_channel(
						   replica_bdev->base_bdev_info[i].desc);
		if (!replica_ch->base_channel[i]) {
			uint8_t j;

			for (j = 0; j < i; j++) {
				spdk_put_io_channel(replica_ch->base_channel[j]);
			}
			free(replica_ch->base_channel);
			replica_ch->base_channel = NULL;
			SPDK_ERRLOG("Unable to create io channel for base bdev\n");
			return -ENOMEM;
		}
	}

	return 0;
}

/*
 * brief:
 * replica_bdev_destroy_cb function is a cb function for replica bdev which deletes the
 * hierarchy from replica bdev to base bdev io channels. It will be called per core
 * params:
 * io_device - pointer to replica bdev io device represented by replica_bdev
 * ctx_buf - pointer to context buffer for replica bdev io channel
 * returns:
 * none
 */
static void
replica_bdev_destroy_cb(void *io_device, void *ctx_buf)
{
	struct replica_bdev_io_channel *replica_ch = ctx_buf;
	uint8_t i;

	SPDK_DEBUGLOG(bdev_replica, "replica_bdev_destroy_cb\n");

	assert(replica_ch != NULL);
	assert(replica_ch->base_channel);
	for (i = 0; i < replica_ch->num_channels; i++) {
		/* Free base bdev channels */
		assert(replica_ch->base_channel[i] != NULL);
		spdk_put_io_channel(replica_ch->base_channel[i]);
	}
	free(replica_ch->base_channel);
	replica_ch->base_channel = NULL;
}

/*
 * brief:
 * replica_bdev_cleanup is used to cleanup and free replica_bdev related data
 * structures.
 * params:
 * replica_bdev - pointer to replica_bdev
 * returns:
 * none
 */
static void
replica_bdev_cleanup(struct replica_bdev *replica_bdev)
{
	SPDK_DEBUGLOG(bdev_replica, "replica_bdev_cleanup, %p name %s, state %u, config %p\n",
		      replica_bdev,
		      replica_bdev->bdev.name, replica_bdev->state, replica_bdev->config);
	if (replica_bdev->state == REPLICA_BDEV_STATE_CONFIGURING) {
		TAILQ_REMOVE(&g_replica_bdev_configuring_list, replica_bdev, state_link);
	} else if (replica_bdev->state == REPLICA_BDEV_STATE_OFFLINE) {
		TAILQ_REMOVE(&g_replica_bdev_offline_list, replica_bdev, state_link);
	} else {
		assert(0);
	}
	TAILQ_REMOVE(&g_replica_bdev_list, replica_bdev, global_link);
	free(replica_bdev->bdev.name);
	free(replica_bdev->base_bdev_info);
	if (replica_bdev->config) {
		replica_bdev->config->replica_bdev = NULL;
	}
	free(replica_bdev);
}

/*
 * brief:
 * wrapper for the bdev close operation
 * params:
 * base_info - replica base bdev info
 * returns:
 */
static void
_replica_bdev_free_base_bdev_resource(void *ctx)
{
	struct spdk_bdev_desc *desc = ctx;

	spdk_bdev_close(desc);
}


/*
 * brief:
 * free resource of base bdev for replica bdev
 * params:
 * replica_bdev - pointer to replica bdev
 * base_info - replica base bdev info
 * returns:
 * 0 - success
 * non zero - failure
 */
static void
replica_bdev_free_base_bdev_resource(struct replica_bdev *replica_bdev,
				  struct replica_base_bdev_info *base_info)
{
	spdk_bdev_module_release_bdev(base_info->bdev);
	if (base_info->thread && base_info->thread != spdk_get_thread()) {
		spdk_thread_send_msg(base_info->thread, _replica_bdev_free_base_bdev_resource, base_info->desc);
	} else {
		spdk_bdev_close(base_info->desc);
	}
	base_info->desc = NULL;
	base_info->bdev = NULL;

	assert(replica_bdev->num_base_bdevs_discovered);
	replica_bdev->num_base_bdevs_discovered--;
}

/*
 * brief:
 * replica_bdev_destruct is the destruct function table pointer for replica bdev
 * params:
 * ctxt - pointer to replica_bdev
 * returns:
 * 0 - success
 * non zero - failure
 */
static int
replica_bdev_destruct(void *ctxt)
{
	struct replica_bdev *replica_bdev = ctxt;
	struct replica_base_bdev_info *base_info;

	SPDK_DEBUGLOG(bdev_replica, "replica_bdev_destruct\n");

	replica_bdev->destruct_called = true;
	REPLICA_FOR_EACH_BASE_BDEV(replica_bdev, base_info) {
		/*
		 * Close all base bdev descriptors for which call has come from below
		 * layers.  Also close the descriptors if we have started shutdown.
		 */
		if (g_shutdown_started ||
		    ((base_info->remove_scheduled == true) &&
		     (base_info->bdev != NULL))) {
			replica_bdev_free_base_bdev_resource(replica_bdev, base_info);
		}
	}

	if (g_shutdown_started) {
		TAILQ_REMOVE(&g_replica_bdev_configured_list, replica_bdev, state_link);
		replica_bdev_stop(replica_bdev);
		replica_bdev->state = REPLICA_BDEV_STATE_OFFLINE;
		TAILQ_INSERT_TAIL(&g_replica_bdev_offline_list, replica_bdev, state_link);
	}

	spdk_io_device_unregister(replica_bdev, NULL);

	if (replica_bdev->num_base_bdevs_discovered == 0) {
		/* Free replica_bdev when there are no base bdevs left */
		SPDK_DEBUGLOG(bdev_replica, "replica bdev base bdevs is 0, going to free all in destruct\n");
		replica_bdev_cleanup(replica_bdev);
	}

	return 0;
}

void
replica_bdev_io_complete(struct replica_bdev_io *replica_io, enum spdk_bdev_io_status status)
{
	struct spdk_bdev_io *bdev_io = spdk_bdev_io_from_ctx(replica_io);

	spdk_bdev_io_complete(bdev_io, status);
}

/*
 * brief:
 * replica_bdev_io_complete_part - signal the completion of a part of the expected
 * base bdev IOs and complete the replica_io if this is the final expected IO.
 * The caller should first set replica_io->base_bdev_io_remaining. This function
 * will decrement this counter by the value of the 'completed' parameter and
 * complete the replica_io if the counter reaches 0. The caller is free to
 * interpret the 'base_bdev_io_remaining' and 'completed' values as needed,
 * it can represent e.g. blocks or IOs.
 * params:
 * replica_io - pointer to replica_bdev_io
 * completed - the part of the replica_io that has been completed
 * status - status of the base IO
 * returns:
 * true - if the replica_io is completed
 * false - otherwise
 */
bool
replica_bdev_io_complete_part(struct replica_bdev_io *replica_io, uint64_t completed,
			   enum spdk_bdev_io_status status)
{
	assert(replica_io->base_bdev_io_remaining >= completed);
	replica_io->base_bdev_io_remaining -= completed;

	if (status != SPDK_BDEV_IO_STATUS_SUCCESS) {
		replica_io->base_bdev_io_status = status;
	}

	if (replica_io->base_bdev_io_remaining == 0) {
		replica_bdev_io_complete(replica_io, replica_io->base_bdev_io_status);
		return true;
	} else {
		return false;
	}
}

/*
 * brief:
 * replica_bdev_queue_io_wait function processes the IO which failed to submit.
 * It will try to queue the IOs after storing the context to bdev wait queue logic.
 * params:
 * replica_io - pointer to replica_bdev_io
 * bdev - the block device that the IO is submitted to
 * ch - io channel
 * cb_fn - callback when the spdk_bdev_io for bdev becomes available
 * returns:
 * none
 */
void
replica_bdev_queue_io_wait(struct replica_bdev_io *replica_io, struct spdk_bdev *bdev,
			struct spdk_io_channel *ch, spdk_bdev_io_wait_cb cb_fn)
{
	replica_io->waitq_entry.bdev = bdev;
	replica_io->waitq_entry.cb_fn = cb_fn;
	replica_io->waitq_entry.cb_arg = replica_io;
	spdk_bdev_queue_io_wait(bdev, ch, &replica_io->waitq_entry);
}

// TODO -
static void
replica_base_bdev_rw_complete(struct spdk_bdev_io *bdev_io, bool success, void *cb_arg)
{
	struct replica_bdev_io *replica_io = cb_arg;

	spdk_bdev_free_io(bdev_io);

	replica_bdev_io_complete_part(replica_io, 1, success ?
				   SPDK_BDEV_IO_STATUS_SUCCESS :
				   SPDK_BDEV_IO_STATUS_FAILED);
}

static void
replica_bdev_submit_read_request(struct replica_bdev_io *replica_io);

static void
_replica_bdev_submit_read_request(void *_replica_io)
{
	struct replica_bdev_io *replica_io = _replica_io;

	replica_bdev_submit_read_request(replica_io);
}

/*
 * brief:
 * replica_bdev_submit_read_request function submits read requests
 * to the first disk; it will submit as many as possible unless a read fails with -ENOMEM, in
 * which case it will queue it for later submission
 * params:
 * replica_io
 * returns:
 * none
 */
static void
replica_bdev_submit_read_request(struct replica_bdev_io *replica_io)
{
	struct spdk_bdev_io		*bdev_io = spdk_bdev_io_from_ctx(replica_io);
	struct replica_bdev		*replica_bdev;
	int				ret;
	uint8_t				i;
	struct replica_base_bdev_info	*base_info;
	struct spdk_io_channel		*base_ch;

	replica_bdev = replica_io->replica_bdev;

	if (replica_io->base_bdev_io_remaining == 0) {
		replica_io->base_bdev_io_remaining = 1;
	}

	base_info = &replica_bdev->base_bdev_info[0];
	base_ch = replica_io->replica_ch->base_channel[0];

	ret = spdk_bdev_readv_blocks(base_info->desc, base_ch,
					bdev_io->u.bdev.iovs, bdev_io->u.bdev.iovcnt,
					bdev_io->u.bdev.offset_blocks, bdev_io->u.bdev.num_blocks, replica_base_bdev_rw_complete,
					replica_io);

	if (ret != 0) {
			if (ret == -ENOMEM) {
			replica_bdev_queue_io_wait(replica_io, base_info->bdev, base_ch,
						_replica_bdev_submit_read_request);
			return;
		} else {
			SPDK_ERRLOG("bdev io submit error not due to ENOMEM, it should not happen\n");
			assert(false);
			replica_bdev_io_complete(replica_io, SPDK_BDEV_IO_STATUS_FAILED);
			return;
		}
	}
}

static void
replica_bdev_submit_write_request(struct replica_bdev_io *replica_io);

static void
_replica_bdev_submit_write_request(void *_replica_io)
{
	struct replica_bdev_io *replica_io = _replica_io;

	replica_bdev_submit_write_request(replica_io);
}

/*
 * brief:
 * replica_bdev_submit_write_request function submits write requests
 * to member disks; it will submit as many as possible unless a write fails with -ENOMEM, in
 * which case it will queue it for later submission
 * params:
 * replica_io
 * returns:
 * none
 */
static void
replica_bdev_submit_write_request(struct replica_bdev_io *replica_io)
{
	struct spdk_bdev_io		*bdev_io = spdk_bdev_io_from_ctx(replica_io);
	struct replica_bdev		*replica_bdev;
	int				ret;
	uint8_t				i;
	struct replica_base_bdev_info	*base_info;
	struct spdk_io_channel		*base_ch;

	replica_bdev = replica_io->replica_bdev;

	if (replica_io->base_bdev_io_remaining == 0) {
		replica_io->base_bdev_io_remaining = replica_bdev->num_base_bdevs;
	}

	while (replica_io->base_bdev_io_submitted < replica_bdev->num_base_bdevs) {
		i = replica_io->base_bdev_io_submitted;
		base_info = &replica_bdev->base_bdev_info[i];
		base_ch = replica_io->replica_ch->base_channel[i];

		ret = spdk_bdev_writev_blocks(base_info->desc, base_ch,
						bdev_io->u.bdev.iovs, bdev_io->u.bdev.iovcnt,
						bdev_io->u.bdev.offset_blocks, bdev_io->u.bdev.num_blocks, replica_base_bdev_rw_complete,
						replica_io);

		if (ret == 0) {
			replica_io->base_bdev_io_submitted++;
		} else if (ret == -ENOMEM) {
			replica_bdev_queue_io_wait(replica_io, base_info->bdev, base_ch,
						_replica_bdev_submit_write_request);
			return;
		} else {
			SPDK_ERRLOG("bdev io submit error not due to ENOMEM, it should not happen\n");
			assert(false);
			replica_bdev_io_complete(replica_io, SPDK_BDEV_IO_STATUS_FAILED);
			return;
		}
	}
}

// TODO - end

static void
replica_base_bdev_reset_complete(struct spdk_bdev_io *bdev_io, bool success, void *cb_arg)
{
	struct replica_bdev_io *replica_io = cb_arg;

	spdk_bdev_free_io(bdev_io);

	replica_bdev_io_complete_part(replica_io, 1, success ?
				   SPDK_BDEV_IO_STATUS_SUCCESS :
				   SPDK_BDEV_IO_STATUS_FAILED);
}

static void
replica_bdev_submit_reset_request(struct replica_bdev_io *replica_io);

static void
_replica_bdev_submit_reset_request(void *_replica_io)
{
	struct replica_bdev_io *replica_io = _replica_io;

	replica_bdev_submit_reset_request(replica_io);
}

/*
 * brief:
 * replica_bdev_submit_reset_request function submits reset requests
 * to member disks; it will submit as many as possible unless a reset fails with -ENOMEM, in
 * which case it will queue it for later submission
 * params:
 * replica_io
 * returns:
 * none
 */
static void
replica_bdev_submit_reset_request(struct replica_bdev_io *replica_io)
{
	struct replica_bdev		*replica_bdev;
	int				ret;
	uint8_t				i;
	struct replica_base_bdev_info	*base_info;
	struct spdk_io_channel		*base_ch;

	replica_bdev = replica_io->replica_bdev;

	if (replica_io->base_bdev_io_remaining == 0) {
		replica_io->base_bdev_io_remaining = replica_bdev->num_base_bdevs;
	}

	while (replica_io->base_bdev_io_submitted < replica_bdev->num_base_bdevs) {
		i = replica_io->base_bdev_io_submitted;
		base_info = &replica_bdev->base_bdev_info[i];
		base_ch = replica_io->replica_ch->base_channel[i];
		ret = spdk_bdev_reset(base_info->desc, base_ch,
				      replica_base_bdev_reset_complete, replica_io);
		if (ret == 0) {
			replica_io->base_bdev_io_submitted++;
		} else if (ret == -ENOMEM) {
			replica_bdev_queue_io_wait(replica_io, base_info->bdev, base_ch,
						_replica_bdev_submit_reset_request);
			return;
		} else {
			SPDK_ERRLOG("bdev io submit error not due to ENOMEM, it should not happen\n");
			assert(false);
			replica_bdev_io_complete(replica_io, SPDK_BDEV_IO_STATUS_FAILED);
			return;
		}
	}
}

// TODO - start
static void
replica_base_bdev_null_payload_complete(struct spdk_bdev_io *bdev_io, bool success, void *cb_arg)
{
	struct replica_bdev_io *replica_io = cb_arg;

	spdk_bdev_free_io(bdev_io);

	replica_bdev_io_complete_part(replica_io, 1, success ?
				   SPDK_BDEV_IO_STATUS_SUCCESS :
				   SPDK_BDEV_IO_STATUS_FAILED);
}

static void
replica_bdev_submit_null_payload_request(struct replica_bdev_io *replica_io);

static void
_replica_bdev_submit_null_payload_request(void *_replica_io)
{
	struct replica_bdev_io *replica_io = _replica_io;

	replica_bdev_submit_null_payload_request(replica_io);
}

/*
 * brief:
 * replica_bdev_submit_reset_request function submits reset requests
 * to member disks; it will submit as many as possible unless a reset fails with -ENOMEM, in
 * which case it will queue it for later submission
 * params:
 * replica_io
 * returns:
 * none
 */
static void
replica_bdev_submit_null_payload_request(struct replica_bdev_io *replica_io)
{
	struct spdk_bdev_io		*bdev_io = spdk_bdev_io_from_ctx(replica_io);
	struct replica_bdev		*replica_bdev;
	int				ret;
	uint8_t				i;
	struct replica_base_bdev_info	*base_info;
	struct spdk_io_channel		*base_ch;

	replica_bdev = replica_io->replica_bdev;

	if (replica_io->base_bdev_io_remaining == 0) {
		replica_io->base_bdev_io_remaining = replica_bdev->num_base_bdevs;
	}

	while (replica_io->base_bdev_io_submitted < replica_bdev->num_base_bdevs) {
		i = replica_io->base_bdev_io_submitted;
		base_info = &replica_bdev->base_bdev_info[i];
		base_ch = replica_io->replica_ch->base_channel[i];

		switch (bdev_io->type) {
		case SPDK_BDEV_IO_TYPE_UNMAP:
			ret = spdk_bdev_unmap_blocks(base_info->desc, base_ch,
						     bdev_io->u.bdev.offset_blocks, bdev_io->u.bdev.num_blocks,
						     replica_base_bdev_null_payload_complete, replica_io);
			break;

		case SPDK_BDEV_IO_TYPE_FLUSH:
			ret = spdk_bdev_flush_blocks(base_info->desc, base_ch,
						     bdev_io->u.bdev.offset_blocks, bdev_io->u.bdev.num_blocks,
						     replica_base_bdev_null_payload_complete, replica_io);
			break;

		default:
			SPDK_ERRLOG("submit request, invalid io type with null payload %u\n", bdev_io->type);
			assert(false);
			ret = -EIO;
		}
		
		if (ret == 0) {
			replica_io->base_bdev_io_submitted++;
		} else if (ret == -ENOMEM) {
			replica_bdev_queue_io_wait(replica_io, base_info->bdev, base_ch,
						_replica_bdev_submit_null_payload_request);
			return;
		} else {
			SPDK_ERRLOG("bdev io submit error not due to ENOMEM, it should not happen\n");
			assert(false);
			replica_bdev_io_complete(replica_io, SPDK_BDEV_IO_STATUS_FAILED);
			return;
		}
	}
}
// TODO - end

/*
 * brief:
 * Callback function to spdk_bdev_io_get_buf.
 * params:
 * ch - pointer to replica bdev io channel
 * bdev_io - pointer to parent bdev_io on replica bdev device
 * success - True if buffer is allocated or false otherwise.
 * returns:
 * none
 */
static void
replica_bdev_get_buf_cb(struct spdk_io_channel *ch, struct spdk_bdev_io *bdev_io,
		     bool success)
{
	struct replica_bdev_io *replica_io = (struct replica_bdev_io *)bdev_io->driver_ctx;

	if (!success) {
		replica_bdev_io_complete(replica_io, SPDK_BDEV_IO_STATUS_FAILED);
		return;
	}

	replica_bdev_submit_read_request(replica_io);
}

/*
 * brief:
 * replica_bdev_submit_request function is the submit_request function pointer of
 * replica bdev function table. This is used to submit the io on replica_bdev to below
 * layers.
 * params:
 * ch - pointer to replica bdev io channel
 * bdev_io - pointer to parent bdev_io on replica bdev device
 * returns:
 * none
 */
static void
replica_bdev_submit_request(struct spdk_io_channel *ch, struct spdk_bdev_io *bdev_io)
{
	struct replica_bdev_io *replica_io = (struct replica_bdev_io *)bdev_io->driver_ctx;

	replica_io->replica_bdev = bdev_io->bdev->ctxt;
	replica_io->replica_ch = spdk_io_channel_get_ctx(ch);
	replica_io->base_bdev_io_remaining = 0;
	replica_io->base_bdev_io_submitted = 0;
	replica_io->base_bdev_io_status = SPDK_BDEV_IO_STATUS_SUCCESS;

	switch (bdev_io->type) {
	case SPDK_BDEV_IO_TYPE_READ:
		spdk_bdev_io_get_buf(bdev_io, replica_bdev_get_buf_cb,
				     bdev_io->u.bdev.num_blocks * bdev_io->bdev->blocklen);
		break;
	case SPDK_BDEV_IO_TYPE_WRITE:
		replica_bdev_submit_write_request(replica_io);
		break;

	case SPDK_BDEV_IO_TYPE_RESET:
		replica_bdev_submit_reset_request(replica_io);
		break;

	case SPDK_BDEV_IO_TYPE_FLUSH:
	case SPDK_BDEV_IO_TYPE_UNMAP:
		replica_bdev_submit_null_payload_request(replica_io);
		break;

	default:
		SPDK_ERRLOG("submit request, invalid io type %u\n", bdev_io->type);
		replica_bdev_io_complete(replica_io, SPDK_BDEV_IO_STATUS_FAILED);
		break;
	}
}

/*
 * brief:
 * _replica_bdev_io_type_supported checks whether io_type is supported in
 * all base bdev modules of replica bdev module. If anyone among the base_bdevs
 * doesn't support, the replica device doesn't supports.
 *
 * params:
 * replica_bdev - pointer to replica bdev context
 * io_type - io type
 * returns:
 * true - io_type is supported
 * false - io_type is not supported
 */
inline static bool
_replica_bdev_io_type_supported(struct replica_bdev *replica_bdev, enum spdk_bdev_io_type io_type)
{
	struct replica_base_bdev_info *base_info;

	REPLICA_FOR_EACH_BASE_BDEV(replica_bdev, base_info) {
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
 * replica_bdev_io_type_supported is the io_supported function for bdev function
 * table which returns whether the particular io type is supported or not by
 * replica bdev module
 * params:
 * ctx - pointer to replica bdev context
 * type - io type
 * returns:
 * true - io_type is supported
 * false - io_type is not supported
 */
static bool
replica_bdev_io_type_supported(void *ctx, enum spdk_bdev_io_type io_type)
{
	switch (io_type) {
	case SPDK_BDEV_IO_TYPE_READ:
	case SPDK_BDEV_IO_TYPE_WRITE:
		return true;

	case SPDK_BDEV_IO_TYPE_FLUSH:
	case SPDK_BDEV_IO_TYPE_RESET:
	case SPDK_BDEV_IO_TYPE_UNMAP:
		return _replica_bdev_io_type_supported(ctx, io_type);

	default:
		return false;
	}

	return false;
}

/*
 * brief:
 * replica_bdev_get_io_channel is the get_io_channel function table pointer for
 * replica bdev. This is used to return the io channel for this replica bdev
 * params:
 * ctxt - pointer to replica_bdev
 * returns:
 * pointer to io channel for replica bdev
 */
static struct spdk_io_channel *
replica_bdev_get_io_channel(void *ctxt)
{
	struct replica_bdev *replica_bdev = ctxt;

	return spdk_get_io_channel(replica_bdev);
}

/*
 * brief:
 * replica_bdev_dump_info_json is the function table pointer for replica bdev
 * params:
 * ctx - pointer to replica_bdev
 * w - pointer to json context
 * returns:
 * 0 - success
 * non zero - failure
 */
static int
replica_bdev_dump_info_json(void *ctx, struct spdk_json_write_ctx *w)
{
	struct replica_bdev *replica_bdev = ctx;
	struct replica_base_bdev_info *base_info;

	SPDK_DEBUGLOG(bdev_replica, "replica_bdev_dump_config_json\n");
	assert(replica_bdev != NULL);

	/* Dump the replica bdev configuration related information */
	spdk_json_write_named_object_begin(w, "replica");
	spdk_json_write_named_uint32(w, "state", replica_bdev->state);
	spdk_json_write_named_uint32(w, "destruct_called", replica_bdev->destruct_called);
	spdk_json_write_named_uint32(w, "num_base_bdevs", replica_bdev->num_base_bdevs);
	spdk_json_write_named_uint32(w, "num_base_bdevs_discovered", replica_bdev->num_base_bdevs_discovered);
	spdk_json_write_name(w, "base_bdevs_list");
	spdk_json_write_array_begin(w);
	REPLICA_FOR_EACH_BASE_BDEV(replica_bdev, base_info) {
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
 * replica_bdev_write_config_json is the function table pointer for replica bdev
 * params:
 * bdev - pointer to spdk_bdev
 * w - pointer to json context
 * returns:
 * none
 */
static void
replica_bdev_write_config_json(struct spdk_bdev *bdev, struct spdk_json_write_ctx *w)
{
	struct replica_bdev *replica_bdev = bdev->ctxt;
	struct replica_base_bdev_info *base_info;

	spdk_json_write_object_begin(w);

	spdk_json_write_named_string(w, "method", "bdev_replica_create");

	spdk_json_write_named_object_begin(w, "params");
	spdk_json_write_named_string(w, "name", bdev->name);

	spdk_json_write_named_array_begin(w, "base_bdevs");
	REPLICA_FOR_EACH_BASE_BDEV(replica_bdev, base_info) {
		if (base_info->bdev) {
			spdk_json_write_string(w, base_info->bdev->name);
		}
	}
	spdk_json_write_array_end(w);
	spdk_json_write_object_end(w);

	spdk_json_write_object_end(w);
}

/* g_replica_bdev_fn_table is the function table for replica bdev */
static const struct spdk_bdev_fn_table g_replica_bdev_fn_table = {
	.destruct		= replica_bdev_destruct,
	.submit_request		= replica_bdev_submit_request,
	.io_type_supported	= replica_bdev_io_type_supported,
	.get_io_channel		= replica_bdev_get_io_channel,
	.dump_info_json		= replica_bdev_dump_info_json,
	.write_config_json	= replica_bdev_write_config_json,
};

/*
 * brief:
 * replica_bdev_config_cleanup function is used to free memory for one replica_bdev in configuration
 * params:
 * replica_cfg - pointer to replica_bdev_config structure
 * returns:
 * none
 */
void
replica_bdev_config_cleanup(struct replica_bdev_config *replica_cfg)
{
	uint8_t i;

	TAILQ_REMOVE(&g_replica_config.replica_bdev_config_head, replica_cfg, link);
	g_replica_config.total_replica_bdev--;

	if (replica_cfg->base_bdev) {
		for (i = 0; i < replica_cfg->num_base_bdevs; i++) {
			free(replica_cfg->base_bdev[i].name);
		}
		free(replica_cfg->base_bdev);
	}
	free(replica_cfg->name);
	free(replica_cfg);
}

/*
 * brief:
 * replica_bdev_free is the replica bdev function table function pointer. This is
 * called on bdev free path
 * params:
 * none
 * returns:
 * none
 */
static void
replica_bdev_free(void)
{
	struct replica_bdev_config *replica_cfg, *tmp;

	SPDK_DEBUGLOG(bdev_replica, "replica_bdev_free\n");
	TAILQ_FOREACH_SAFE(replica_cfg, &g_replica_config.replica_bdev_config_head, link, tmp) {
		replica_bdev_config_cleanup(replica_cfg);
	}
}

/* brief
 * replica_bdev_config_find_by_name is a helper function to find replica bdev config
 * by name as key.
 *
 * params:
 * replica_name - name for replica bdev.
 */
struct replica_bdev_config *
replica_bdev_config_find_by_name(const char *replica_name)
{
	struct replica_bdev_config *replica_cfg;

	TAILQ_FOREACH(replica_cfg, &g_replica_config.replica_bdev_config_head, link) {
		if (!strcmp(replica_cfg->name, replica_name)) {
			return replica_cfg;
		}
	}

	return replica_cfg;
}

/*
 * brief
 * replica_bdev_config_add function adds config for newly created replica bdev.
 *
 * params:
 * replica_name - name for replica bdev.
 * num_base_bdevs - number of base bdevs.
 * _replica_cfg - Pointer to newly added configuration
 */
int
replica_bdev_config_add(const char *replica_name, uint8_t num_base_bdevs,
		     struct replica_bdev_config **_replica_cfg)
{
	struct replica_bdev_config *replica_cfg;

	replica_cfg = replica_bdev_config_find_by_name(replica_name);
	if (replica_cfg != NULL) {
		SPDK_ERRLOG("Duplicate replica bdev name found in config file %s\n",
			    replica_name);
		return -EEXIST;
	}

	if (num_base_bdevs == 0) {
		SPDK_ERRLOG("Invalid base device count %u\n", num_base_bdevs);
		return -EINVAL;
	}

	replica_cfg = calloc(1, sizeof(*replica_cfg));
	if (replica_cfg == NULL) {
		SPDK_ERRLOG("unable to allocate memory\n");
		return -ENOMEM;
	}

	replica_cfg->name = strdup(replica_name);
	if (!replica_cfg->name) {
		free(replica_cfg);
		SPDK_ERRLOG("unable to allocate memory\n");
		return -ENOMEM;
	}
	replica_cfg->num_base_bdevs = num_base_bdevs;

	replica_cfg->base_bdev = calloc(num_base_bdevs, sizeof(*replica_cfg->base_bdev));
	if (replica_cfg->base_bdev == NULL) {
		free(replica_cfg->name);
		free(replica_cfg);
		SPDK_ERRLOG("unable to allocate memory\n");
		return -ENOMEM;
	}

	TAILQ_INSERT_TAIL(&g_replica_config.replica_bdev_config_head, replica_cfg, link);
	g_replica_config.total_replica_bdev++;

	*_replica_cfg = replica_cfg;
	return 0;
}

/*
 * brief:
 * replica_bdev_config_add_base_bdev function add base bdev to replica bdev config.
 *
 * params:
 * replica_cfg - pointer to replica bdev configuration
 * base_bdev_name - name of base bdev
 * slot - Position to add base bdev
 */
int
replica_bdev_config_add_base_bdev(struct replica_bdev_config *replica_cfg, const char *base_bdev_name,
			       uint8_t slot)
{
	uint8_t i;
	struct replica_bdev_config *tmp;

	if (slot >= replica_cfg->num_base_bdevs) {
		return -EINVAL;
	}

	TAILQ_FOREACH(tmp, &g_replica_config.replica_bdev_config_head, link) {
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

	replica_cfg->base_bdev[slot].name = strdup(base_bdev_name);
	if (replica_cfg->base_bdev[slot].name == NULL) {
		SPDK_ERRLOG("unable to allocate memory\n");
		return -ENOMEM;
	}

	return 0;
}

/*
 * brief:
 * replica_bdev_fini_start is called when bdev layer is starting the
 * shutdown process
 * params:
 * none
 * returns:
 * none
 */
static void
replica_bdev_fini_start(void)
{
	SPDK_DEBUGLOG(bdev_replica, "replica_bdev_fini_start\n");
	g_shutdown_started = true;
}

/*
 * brief:
 * replica_bdev_exit is called on replica bdev module exit time by bdev layer
 * params:
 * none
 * returns:
 * none
 */
static void
replica_bdev_exit(void)
{
	SPDK_DEBUGLOG(bdev_replica, "replica_bdev_exit\n");
	replica_bdev_free();
}

/*
 * brief:
 * replica_bdev_get_ctx_size is used to return the context size of bdev_io for replica
 * module
 * params:
 * none
 * returns:
 * size of spdk_bdev_io context for replica
 */
static int
replica_bdev_get_ctx_size(void)
{
	SPDK_DEBUGLOG(bdev_replica, "replica_bdev_get_ctx_size\n");
	return sizeof(struct replica_bdev_io);
}

/*
 * brief:
 * replica_bdev_can_claim_bdev is the function to check if this base_bdev can be
 * claimed by replica bdev or not.
 * params:
 * bdev_name - represents base bdev name
 * _replica_cfg - pointer to replica bdev config parsed from config file
 * base_bdev_slot - if bdev can be claimed, it represents the base_bdev correct
 * slot. This field is only valid if return value of this function is true
 * returns:
 * true - if bdev can be claimed
 * false - if bdev can't be claimed
 */
static bool
replica_bdev_can_claim_bdev(const char *bdev_name, struct replica_bdev_config **_replica_cfg,
			 uint8_t *base_bdev_slot)
{
	struct replica_bdev_config *replica_cfg;
	uint8_t i;

	TAILQ_FOREACH(replica_cfg, &g_replica_config.replica_bdev_config_head, link) {
		for (i = 0; i < replica_cfg->num_base_bdevs; i++) {
			/*
			 * Check if the base bdev name is part of replica bdev configuration.
			 * If match is found then return true and the slot information where
			 * this base bdev should be inserted in replica bdev
			 */
			if (!strcmp(bdev_name, replica_cfg->base_bdev[i].name)) {
				*_replica_cfg = replica_cfg;
				*base_bdev_slot = i;
				return true;
			}
		}
	}

	return false;
}


static struct spdk_bdev_module g_replica_if = {
	.name = "replica",
	.module_init = replica_bdev_init,
	.fini_start = replica_bdev_fini_start,
	.module_fini = replica_bdev_exit,
	.get_ctx_size = replica_bdev_get_ctx_size,
	.examine_config = replica_bdev_examine,
	.async_init = false,
	.async_fini = false,
};
SPDK_BDEV_MODULE_REGISTER(replica, &g_replica_if)

/*
 * brief:
 * replica_bdev_init is the initialization function for replica bdev module
 * params:
 * none
 * returns:
 * 0 - success
 * non zero - failure
 */
static int
replica_bdev_init(void)
{
	return 0;
}

/*
 * brief:
 * replica_bdev_create allocates replica bdev based on passed configuration
 * params:
 * replica_cfg - configuration of replica bdev
 * returns:
 * 0 - success
 * non zero - failure
 */
int
replica_bdev_create(struct replica_bdev_config *replica_cfg)
{
	struct replica_bdev *replica_bdev;
	struct spdk_bdev *replica_bdev_gen;

	replica_bdev = calloc(1, sizeof(*replica_bdev));
	if (!replica_bdev) {
		SPDK_ERRLOG("Unable to allocate memory for replica bdev\n");
		return -ENOMEM;
	}

	replica_bdev->num_base_bdevs = replica_cfg->num_base_bdevs;
	replica_bdev->base_bdev_info = calloc(replica_bdev->num_base_bdevs,
					   sizeof(struct replica_base_bdev_info));
	if (!replica_bdev->base_bdev_info) {
		SPDK_ERRLOG("Unable able to allocate base bdev info\n");
		free(replica_bdev);
		return -ENOMEM;
	}

	replica_bdev->state = REPLICA_BDEV_STATE_CONFIGURING;
	replica_bdev->config = replica_cfg;

	replica_bdev_gen = &replica_bdev->bdev;

	replica_bdev_gen->name = strdup(replica_cfg->name);
	if (!replica_bdev_gen->name) {
		SPDK_ERRLOG("Unable to allocate name for replica\n");
		free(replica_bdev->base_bdev_info);
		free(replica_bdev);
		return -ENOMEM;
	}

	replica_bdev_gen->product_name = "Replica Volume";
	replica_bdev_gen->ctxt = replica_bdev;
	replica_bdev_gen->fn_table = &g_replica_bdev_fn_table;
	replica_bdev_gen->module = &g_replica_if;
	replica_bdev_gen->write_cache = 0;

	TAILQ_INSERT_TAIL(&g_replica_bdev_configuring_list, replica_bdev, state_link);
	TAILQ_INSERT_TAIL(&g_replica_bdev_list, replica_bdev, global_link);

	replica_cfg->replica_bdev = replica_bdev;

	return 0;
}

/*
 * brief
 * replica_bdev_alloc_base_bdev_resource allocates resource of base bdev.
 * params:
 * replica_bdev - pointer to replica bdev
 * bdev_name - base bdev name
 * base_bdev_slot - position to add base bdev
 * returns:
 * 0 - success
 * non zero - failure
 */
static int
replica_bdev_alloc_base_bdev_resource(struct replica_bdev *replica_bdev, const char *bdev_name,
				   uint8_t base_bdev_slot)
{
	struct spdk_bdev_desc *desc;
	struct spdk_bdev *bdev;
	int rc;

	rc = spdk_bdev_open_ext(bdev_name, true, replica_bdev_event_base_bdev, NULL, &desc);
	if (rc != 0) {
		if (rc != -ENODEV) {
			SPDK_ERRLOG("Unable to create desc on bdev '%s'\n", bdev_name);
		}
		return rc;
	}

	bdev = spdk_bdev_desc_get_bdev(desc);

	rc = spdk_bdev_module_claim_bdev(bdev, NULL, &g_replica_if);
	if (rc != 0) {
		SPDK_ERRLOG("Unable to claim this bdev as it is already claimed\n");
		spdk_bdev_close(desc);
		return rc;
	}

	SPDK_DEBUGLOG(bdev_replica, "bdev %s is claimed\n", bdev_name);

	assert(replica_bdev->state != REPLICA_BDEV_STATE_ONLINE);
	assert(base_bdev_slot < replica_bdev->num_base_bdevs);

	replica_bdev->base_bdev_info[base_bdev_slot].thread = spdk_get_thread();
	replica_bdev->base_bdev_info[base_bdev_slot].bdev = bdev;
	replica_bdev->base_bdev_info[base_bdev_slot].desc = desc;
	replica_bdev->num_base_bdevs_discovered++;
	assert(replica_bdev->num_base_bdevs_discovered <= replica_bdev->num_base_bdevs);

	return 0;
}

/*
 * brief:
 * If replica bdev config is complete, then only register the replica bdev to
 * bdev layer and remove this replica bdev from configuring list and
 * insert the replica bdev to configured list
 * params:
 * replica_bdev - pointer to replica bdev
 * returns:
 * 0 - success
 * non zero - failure
 */
static int
replica_bdev_configure(struct replica_bdev *replica_bdev)
{
	uint32_t blocklen = 0;
	struct spdk_bdev *replica_bdev_gen;
	struct replica_base_bdev_info *base_info;
	int rc = 0;

	assert(replica_bdev->state == REPLICA_BDEV_STATE_CONFIGURING);
	assert(replica_bdev->num_base_bdevs_discovered == replica_bdev->num_base_bdevs);

	REPLICA_FOR_EACH_BASE_BDEV(replica_bdev, base_info) {
		/* Check blocklen for all base bdevs that it should be same */
		if (blocklen == 0) {
			blocklen = base_info->bdev->blocklen;
		} else if (blocklen != base_info->bdev->blocklen) {
			/*
			 * Assumption is that all the base bdevs for any replica bdev should
			 * have same blocklen
			 */
			SPDK_ERRLOG("Blocklen of various bdevs not matching\n");
			return -EINVAL;
		}
	}
	assert(blocklen > 0);

	replica_bdev->blocklen_shift = spdk_u32log2(blocklen);

	replica_bdev_gen = &replica_bdev->bdev;
	replica_bdev_gen->blocklen = blocklen;

	rc = replica_bdev_start(replica_bdev);
	if (rc != 0) {
		SPDK_ERRLOG("replica module startup callback failed\n");
		return rc;
	}
	replica_bdev->state = REPLICA_BDEV_STATE_ONLINE;
	SPDK_DEBUGLOG(bdev_replica, "io device register %p\n", replica_bdev);
	SPDK_DEBUGLOG(bdev_replica, "blockcnt %" PRIu64 ", blocklen %u\n",
		      replica_bdev_gen->blockcnt, replica_bdev_gen->blocklen);
	spdk_io_device_register(replica_bdev, replica_bdev_create_cb, replica_bdev_destroy_cb,
				sizeof(struct replica_bdev_io_channel),
				replica_bdev->bdev.name);
	rc = spdk_bdev_register(replica_bdev_gen);
	if (rc != 0) {
		SPDK_ERRLOG("Unable to register replica bdev and stay at configuring state\n");
		replica_bdev_stop(replica_bdev);
		spdk_io_device_unregister(replica_bdev, NULL);
		replica_bdev->state = REPLICA_BDEV_STATE_CONFIGURING;
		return rc;
	}
	SPDK_DEBUGLOG(bdev_replica, "replica bdev generic %p\n", replica_bdev_gen);
	TAILQ_REMOVE(&g_replica_bdev_configuring_list, replica_bdev, state_link);
	TAILQ_INSERT_TAIL(&g_replica_bdev_configured_list, replica_bdev, state_link);
	SPDK_DEBUGLOG(bdev_replica, "replica bdev is created with name %s, replica_bdev %p\n",
		      replica_bdev_gen->name, replica_bdev);

	return 0;
}

/*
 * brief:
 * If replica bdev is online and registered, change the bdev state to
 * configuring and unregister this replica device. Queue this replica device
 * in configuring list
 * params:
 * replica_bdev - pointer to replica bdev
 * cb_fn - callback function
 * cb_arg - argument to callback function
 * returns:
 * none
 */
static void
replica_bdev_deconfigure(struct replica_bdev *replica_bdev, replica_bdev_destruct_cb cb_fn,
		      void *cb_arg)
{
	if (replica_bdev->state != REPLICA_BDEV_STATE_ONLINE) {
		if (cb_fn) {
			cb_fn(cb_arg, 0);
		}
		return;
	}

	assert(replica_bdev->num_base_bdevs == replica_bdev->num_base_bdevs_discovered);
	TAILQ_REMOVE(&g_replica_bdev_configured_list, replica_bdev, state_link);
	replica_bdev_stop(replica_bdev);
	replica_bdev->state = REPLICA_BDEV_STATE_OFFLINE;
	assert(replica_bdev->num_base_bdevs_discovered);
	TAILQ_INSERT_TAIL(&g_replica_bdev_offline_list, replica_bdev, state_link);
	SPDK_DEBUGLOG(bdev_replica, "replica bdev state changing from online to offline\n");

	spdk_bdev_unregister(&replica_bdev->bdev, cb_fn, cb_arg);
}

/*
 * brief:
 * replica_bdev_find_by_base_bdev function finds the replica bdev which has
 *  claimed the base bdev.
 * params:
 * base_bdev - pointer to base bdev pointer
 * _replica_bdev - Reference to pointer to replica bdev
 * _base_info - Reference to the replica base bdev info.
 * returns:
 * true - if the replica bdev is found.
 * false - if the replica bdev is not found.
 */
static bool
replica_bdev_find_by_base_bdev(struct spdk_bdev *base_bdev, struct replica_bdev **_replica_bdev,
			    struct replica_base_bdev_info **_base_info)
{
	struct replica_bdev *replica_bdev;
	struct replica_base_bdev_info *base_info;

	TAILQ_FOREACH(replica_bdev, &g_replica_bdev_list, global_link) {
		REPLICA_FOR_EACH_BASE_BDEV(replica_bdev, base_info) {
			if (base_info->bdev == base_bdev) {
				*_replica_bdev = replica_bdev;
				*_base_info = base_info;
				return true;
			}
		}
	}

	return false;
}

/*
 * brief:
 * replica_bdev_remove_base_bdev function is called by below layers when base_bdev
 * is removed. This function checks if this base bdev is part of any replica bdev
 * or not. If yes, it takes necessary action on that particular replica bdev.
 * params:
 * base_bdev - pointer to base bdev pointer which got removed
 * returns:
 * none
 */
static void
replica_bdev_remove_base_bdev(struct spdk_bdev *base_bdev)
{
	struct replica_bdev	*replica_bdev = NULL;
	struct replica_base_bdev_info *base_info;

	SPDK_DEBUGLOG(bdev_replica, "replica_bdev_remove_base_bdev\n");

	/* Find the replica_bdev which has claimed this base_bdev */
	if (!replica_bdev_find_by_base_bdev(base_bdev, &replica_bdev, &base_info)) {
		SPDK_ERRLOG("bdev to remove '%s' not found\n", base_bdev->name);
		return;
	}

	assert(base_info->desc);
	base_info->remove_scheduled = true;

	if (replica_bdev->destruct_called == true ||
	    replica_bdev->state == REPLICA_BDEV_STATE_CONFIGURING) {
		/*
		 * As replica bdev is not registered yet or already unregistered,
		 * so cleanup should be done here itself.
		 */
		replica_bdev_free_base_bdev_resource(replica_bdev, base_info);
		if (replica_bdev->num_base_bdevs_discovered == 0) {
			/* There is no base bdev for this replica, so free the replica device. */
			replica_bdev_cleanup(replica_bdev);
			return;
		}
	}

	replica_bdev_deconfigure(replica_bdev, NULL, NULL);
}

/*
 * brief:
 * replica_bdev_event_base_bdev function is called by below layers when base_bdev
 * triggers asynchronous event.
 * params:
 * type - event details.
 * bdev - bdev that triggered event.
 * event_ctx - context for event.
 * returns:
 * none
 */
static void
replica_bdev_event_base_bdev(enum spdk_bdev_event_type type, struct spdk_bdev *bdev,
			  void *event_ctx)
{
	switch (type) {
	case SPDK_BDEV_EVENT_REMOVE:
		replica_bdev_remove_base_bdev(bdev);
		break;
	default:
		SPDK_NOTICELOG("Unsupported bdev event: type %d\n", type);
		break;
	}
}

/*
 * brief:
 * Remove base bdevs from the replica bdev one by one.  Skip any base bdev which
 *  doesn't exist.
 * params:
 * replica_cfg - pointer to replica bdev config.
 * cb_fn - callback function
 * cb_ctx - argument to callback function
 */
void
replica_bdev_remove_base_devices(struct replica_bdev_config *replica_cfg,
			      replica_bdev_destruct_cb cb_fn, void *cb_arg)
{
	struct replica_bdev		*replica_bdev;
	struct replica_base_bdev_info	*base_info;

	SPDK_DEBUGLOG(bdev_replica, "replica_bdev_remove_base_devices\n");

	replica_bdev = replica_cfg->replica_bdev;
	if (replica_bdev == NULL) {
		SPDK_DEBUGLOG(bdev_replica, "replica bdev %s doesn't exist now\n", replica_cfg->name);
		if (cb_fn) {
			cb_fn(cb_arg, 0);
		}
		return;
	}

	if (replica_bdev->destroy_started) {
		SPDK_DEBUGLOG(bdev_replica, "destroying replica bdev %s is already started\n",
			      replica_cfg->name);
		if (cb_fn) {
			cb_fn(cb_arg, -EALREADY);
		}
		return;
	}

	replica_bdev->destroy_started = true;

	REPLICA_FOR_EACH_BASE_BDEV(replica_bdev, base_info) {
		if (base_info->bdev == NULL) {
			continue;
		}

		assert(base_info->desc);
		base_info->remove_scheduled = true;

		if (replica_bdev->destruct_called == true ||
		    replica_bdev->state == REPLICA_BDEV_STATE_CONFIGURING) {
			/*
			 * As replica bdev is not registered yet or already unregistered,
			 * so cleanup should be done here itself.
			 */
			replica_bdev_free_base_bdev_resource(replica_bdev, base_info);
			if (replica_bdev->num_base_bdevs_discovered == 0) {
				/* There is no base bdev for this replica, so free the replica device. */
				replica_bdev_cleanup(replica_bdev);
				if (cb_fn) {
					cb_fn(cb_arg, 0);
				}
				return;
			}
		}
	}

	replica_bdev_deconfigure(replica_bdev, cb_fn, cb_arg);
}

/*
 * brief:
 * replica_bdev_add_base_device function is the actual function which either adds
 * the nvme base device to existing replica bdev or create a new replica bdev. It also claims
 * the base device and keep the open descriptor.
 * params:
 * replica_cfg - pointer to replica bdev config
 * bdev - pointer to base bdev
 * base_bdev_slot - position to add base bdev
 * returns:
 * 0 - success
 * non zero - failure
 */
static int
replica_bdev_add_base_device(struct replica_bdev_config *replica_cfg, const char *bdev_name,
			  uint8_t base_bdev_slot)
{
	struct replica_bdev	*replica_bdev;
	int			rc;

	replica_bdev = replica_cfg->replica_bdev;
	if (!replica_bdev) {
		SPDK_ERRLOG("Replica bdev '%s' is not created yet\n", replica_cfg->name);
		return -ENODEV;
	}

	rc = replica_bdev_alloc_base_bdev_resource(replica_bdev, bdev_name, base_bdev_slot);
	if (rc != 0) {
		if (rc != -ENODEV) {
			SPDK_ERRLOG("Failed to allocate resource for bdev '%s'\n", bdev_name);
		}
		return rc;
	}

	assert(replica_bdev->num_base_bdevs_discovered <= replica_bdev->num_base_bdevs);

	if (replica_bdev->num_base_bdevs_discovered == replica_bdev->num_base_bdevs) {
		rc = replica_bdev_configure(replica_bdev);
		if (rc != 0) {
			SPDK_ERRLOG("Failed to configure replica bdev\n");
			return rc;
		}
	}

	return 0;
}

/*
 * brief:
 * Add base bdevs to the replica bdev one by one.  Skip any base bdev which doesn't
 *  exist or fails to add. If all base bdevs are successfully added, the replica bdev
 *  moves to the configured state and becomes available. Otherwise, the replica bdev
 *  stays at the configuring state with added base bdevs.
 * params:
 * replica_cfg - pointer to replica bdev config
 * returns:
 * 0 - The replica bdev moves to the configured state or stays at the configuring
 *     state with added base bdevs due to any nonexistent base bdev.
 * non zero - Failed to add any base bdev and stays at the configuring state with
 *            added base bdevs.
 */
int
replica_bdev_add_base_devices(struct replica_bdev_config *replica_cfg)
{
	uint8_t	i;
	int	rc = 0, _rc;

	for (i = 0; i < replica_cfg->num_base_bdevs; i++) {
		_rc = replica_bdev_add_base_device(replica_cfg, replica_cfg->base_bdev[i].name, i);
		if (_rc == -ENODEV) {
			SPDK_DEBUGLOG(bdev_replica, "base bdev %s doesn't exist now\n",
				      replica_cfg->base_bdev[i].name);
		} else if (_rc != 0) {
			SPDK_ERRLOG("Failed to add base bdev %s to REPLICA bdev %s: %s\n",
				    replica_cfg->base_bdev[i].name, replica_cfg->name,
				    spdk_strerror(-_rc));
			if (rc == 0) {
				rc = _rc;
			}
		}
	}

	return rc;
}

static int
replica_bdev_start(struct replica_bdev *replica_bdev)
{
	uint64_t min_blockcnt = UINT64_MAX;
	struct replica_base_bdev_info *base_info;

	REPLICA_FOR_EACH_BASE_BDEV(replica_bdev, base_info) {
		/* Calculate minimum block count from all base bdevs */
		min_blockcnt = spdk_min(min_blockcnt, base_info->bdev->blockcnt);
	}

	/*
	 * Take the minimum block count based approach where total block count
	 * of replica bdev is the number of base bdev times the minimum block count
	 * of any base bdev.
	 */
	SPDK_DEBUGLOG(bdev_replica0, "min blockcount %" PRIu64 ",  numbasedev %u, strip size shift %u\n",
		      min_blockcnt, replica_bdev->num_base_bdevs, replica_bdev->strip_size_shift);
	replica_bdev->bdev.blockcnt = min_blockcnt;

	return 0;
}

static void
replica_bdev_stop(struct replica_bdev *replica_bdev)
{
	// flush?
}

/*
 * brief:
 * replica_bdev_examine function is the examine function call by the below layers
 * like bdev_nvme layer. This function will check if this base bdev can be
 * claimed by this replica bdev or not.
 * params:
 * bdev - pointer to base bdev
 * returns:
 * none
 */
static void
replica_bdev_examine(struct spdk_bdev *bdev)
{
	struct replica_bdev_config	*replica_cfg;
	uint8_t			base_bdev_slot;

	if (replica_bdev_can_claim_bdev(bdev->name, &replica_cfg, &base_bdev_slot)) {
		replica_bdev_add_base_device(replica_cfg, bdev->name, base_bdev_slot);
	} else {
		SPDK_DEBUGLOG(bdev_replica, "bdev %s can't be claimed\n",
			      bdev->name);
	}

	spdk_bdev_module_examine_done(&g_replica_if);
}

/* Log component for bdev replica bdev module */
SPDK_LOG_REGISTER_COMPONENT(bdev_replica)
