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
#include "spdk/likely.h"
#include "spdk/log.h"
#include "spdk/string.h"
#include "spdk/util.h"
#include "spdk/json.h"
#include "spdk/trace.h"

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
static void	wal_bdev_event_base_bdev(enum spdk_bdev_event_type type, struct spdk_bdev *bdev,
		void *event_ctx);
static bool wal_bdev_is_valid_entry(struct wal_bdev *bdev, struct bstat *bstat);
int wal_log_bdev_writev_blocks_with_md(struct spdk_bdev_desc *desc, struct spdk_io_channel *ch,
										struct iovec *iovs, int iovcnt, void *md_buf,
										uint64_t offset_blocks, uint64_t num_blocks,
										struct wal_bdev_io *wal_io);
static int wal_bdev_submit_pending_writes(void *ctx);
static int wal_bdev_mover(void *ctx);
static void wal_bdev_mover_read_data(struct spdk_bdev_io *bdev_io, bool success, void *ctx);
static void wal_bdev_mover_write_data(struct spdk_bdev_io *bdev_io, bool success, void *ctx);
static void wal_bdev_mover_update_head(struct spdk_bdev_io *bdev_io, bool success, void *ctx);
static void wal_bdev_mover_clean(struct spdk_bdev_io *bdev_io, bool success, void *ctx);
static void wal_bdev_mover_free(struct wal_mover_context *ctx);
static void wal_bdev_mover_reset(struct wal_bdev *bdev);
static int wal_bdev_cleaner(void *ctx);
static int wal_bdev_stat_report(void *ctx);

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

	SPDK_DEBUGLOG(bdev_wal, "wal_bdev_create_cb, %p\n", wal_ch);

	assert(wal_bdev != NULL);
	assert(wal_bdev->state == WAL_BDEV_STATE_ONLINE);

	wal_ch->wal_bdev = wal_bdev;

	pthread_mutex_lock(&wal_bdev->mutex);
	if (wal_bdev->ch_count == 0) {
		wal_bdev->log_channel = spdk_bdev_get_io_channel(wal_bdev->log_bdev_info.desc);
		if (!wal_bdev->log_channel) {
			SPDK_ERRLOG("Unable to create io channel for log bdev\n");
			return -ENOMEM;
		}
		
		wal_bdev->core_channel = spdk_bdev_get_io_channel(wal_bdev->core_bdev_info.desc);
		if (!wal_bdev->core_channel) {
			spdk_put_io_channel(wal_bdev->log_channel);
			SPDK_ERRLOG("Unable to create io channel for core bdev\n");
			return -ENOMEM;
		}

		
		wal_bdev->pending_writes_poller = SPDK_POLLER_REGISTER(wal_bdev_submit_pending_writes, wal_bdev, 0);
		wal_bdev->mover_poller = SPDK_POLLER_REGISTER(wal_bdev_mover, wal_bdev, 0);
		wal_bdev->cleaner_poller = SPDK_POLLER_REGISTER(wal_bdev_cleaner, wal_bdev, 10);
		wal_bdev->stat_poller = SPDK_POLLER_REGISTER(wal_bdev_stat_report, wal_bdev, 30*1000*1000);

		wal_bdev->open_thread = spdk_get_thread();
	}
	wal_bdev->ch_count++;
	pthread_mutex_unlock(&wal_bdev->mutex);

	return 0;
}

static void
_wal_bdev_destroy_cb(void *arg)
{
	struct wal_bdev	*wal_bdev = arg;

	spdk_put_io_channel(wal_bdev->log_channel);
	spdk_put_io_channel(wal_bdev->core_channel);
	wal_bdev->log_channel = NULL;
	wal_bdev->core_channel = NULL;

	spdk_poller_unregister(&wal_bdev->pending_writes_poller);
	spdk_poller_unregister(&wal_bdev->mover_poller);
	spdk_poller_unregister(&wal_bdev->cleaner_poller);
	spdk_poller_unregister(&wal_bdev->stat_poller);
	
	wal_bdev->open_thread = NULL;
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
	struct wal_bdev            *wal_bdev = wal_ch->wal_bdev;

	SPDK_DEBUGLOG(bdev_wal, "wal_bdev_destroy_cb\n");

	assert(wal_ch != NULL);

	pthread_mutex_lock(&wal_bdev->mutex);
	wal_bdev->ch_count--;
	if (wal_bdev->ch_count == 0) {
		if (wal_bdev->open_thread != spdk_get_thread()) {
			spdk_thread_send_msg(wal_bdev->open_thread,
					     _wal_bdev_destroy_cb, wal_bdev);
		} else {
			_wal_bdev_destroy_cb(wal_bdev);
		}
	}
	pthread_mutex_unlock(&wal_bdev->mutex);
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

	SPDK_DEBUGLOG(bdev_wal, "wal_bdev_destruct\n");

	wal_bdev->destruct_called = true;
	if (g_shutdown_started ||
		((wal_bdev->log_bdev_info.remove_scheduled == true) &&
			(wal_bdev->log_bdev_info.bdev != NULL))) {
		wal_bdev_free_base_bdev_resource(wal_bdev, &wal_bdev->log_bdev_info);
	}

	if (g_shutdown_started ||
		((wal_bdev->core_bdev_info.remove_scheduled == true) &&
			(wal_bdev->core_bdev_info.bdev != NULL))) {
		wal_bdev_free_base_bdev_resource(wal_bdev, &wal_bdev->core_bdev_info);
	}

	if (g_shutdown_started) {
		TAILQ_REMOVE(&g_wal_bdev_configured_list, wal_bdev, state_link);
		wal_bdev_stop(wal_bdev);
		wal_bdev->state = WAL_BDEV_STATE_OFFLINE;
		TAILQ_INSERT_TAIL(&g_wal_bdev_offline_list, wal_bdev, state_link);
	}

	spdk_io_device_unregister(wal_bdev, NULL);

	wal_bdev_cleanup(wal_bdev);

	return 0;
}

void _wal_bdev_io_complete(void *arg);

void
_wal_bdev_io_complete(void *arg)
{
	struct wal_bdev_io *wal_io = (struct wal_bdev_io *)arg;

	spdk_bdev_io_complete(wal_io->orig_io, wal_io->status);
}

void
wal_bdev_io_complete(struct wal_bdev_io *wal_io, enum spdk_bdev_io_status status)
{	
	wal_io->status = status;
	if (wal_io->orig_thread != spdk_get_thread()) {
		spdk_thread_send_msg(wal_io->orig_thread, _wal_bdev_io_complete, wal_io);
	} else {
		_wal_bdev_io_complete(wal_io);
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

static bool wal_bdev_is_valid_entry(struct wal_bdev *bdev, struct bstat *bstat)
{
    if (bstat->type == LOCATION_BDEV) {
        if (bstat->round == bdev->tail_round) {
            return true;
        }

        if (bstat->round < bdev->tail_round - 1) {
            return false;
        }

        if (bstat->l.bdevOffset >= bdev->log_tail) {
            return true;
        }
        return false;
    }

    return true;
}

// TODO -
static void
wal_base_bdev_read_complete(struct spdk_bdev_io *bdev_io, bool success, void *cb_arg)
{
	struct wal_bdev_io *wal_io = cb_arg;

	spdk_bdev_free_io(bdev_io);

	wal_bdev_io_complete(wal_io, success ?
				   SPDK_BDEV_IO_STATUS_SUCCESS :
				   SPDK_BDEV_IO_STATUS_FAILED);
}

static void
wal_base_bdev_read_complete_part(struct spdk_bdev_io *bdev_io, bool success, void *cb_arg)
{
	struct wal_bdev_io *wal_io = cb_arg;
	struct spdk_bdev_io *orig_io = wal_io->orig_io;
	int i;
	void *copy = wal_io->read_buf;

	spdk_bdev_free_io(bdev_io);

	wal_io->remaining_base_bdev_io--;
	wal_io->status = success && wal_io->status >= 0
					? SPDK_BDEV_IO_STATUS_SUCCESS
					: SPDK_BDEV_IO_STATUS_FAILED;

	if (!success) {
		SPDK_ERRLOG("Error reading data from base bdev.\n");
	}

	if (wal_io->remaining_base_bdev_io == 0) {
		if (wal_io->status == SPDK_BDEV_IO_STATUS_SUCCESS) {
			for (i = 0; i < orig_io->u.bdev.iovcnt; i++) {
				memcpy(orig_io->u.bdev.iovs[i].iov_base, copy, (size_t)orig_io->u.bdev.iovs[i].iov_len);
				copy += orig_io->u.bdev.iovs[i].iov_len;
			}
			// TODO: add to index
		}

		spdk_free(wal_io->read_buf);

		wal_bdev_io_complete(wal_io, wal_io->status);
	}
}

static void
wal_base_bdev_write_complete(struct spdk_bdev_io *bdev_io, bool success, void *cb_arg)
{
	struct wal_bdev_io *wal_io = cb_arg;
	uint64_t begin, end;
	begin = wal_io->metadata->core_offset;
	end = wal_io->metadata->core_offset + wal_io->metadata->core_length - 1;

	spdk_trace_record_tsc(spdk_get_ticks(), TRACE_WAL_BSTAT_CREATE_START, 0, 0, (uintptr_t)wal_io);
	struct bstat *bstat = bstatBdevCreate(begin, end, wal_io->metadata->round,
										bdev_io->u.bdev.offset_blocks+1, wal_io->wal_bdev->bstat_pool);
	spdk_trace_record_tsc(spdk_get_ticks(), TRACE_WAL_BSTAT_CREATE_END, 0, 0, (uintptr_t)wal_io);
	
	
	spdk_trace_record_tsc(spdk_get_ticks(), TRACE_WAL_BSL_INSERT_START, 0, 0, (uintptr_t)wal_io);
	bslInsert(wal_io->wal_bdev->bsl, begin, end,
				bstat, wal_io->wal_bdev->bslfn);
	spdk_trace_record_tsc(spdk_get_ticks(), TRACE_WAL_BSL_INSERT_END, 0, 0, (uintptr_t)wal_io);

	spdk_bdev_free_io(bdev_io);

	spdk_free(wal_io->metadata);

	wal_bdev_io_complete(wal_io, success ?
				   SPDK_BDEV_IO_STATUS_SUCCESS :
				   SPDK_BDEV_IO_STATUS_FAILED);
}

int
wal_log_bdev_writev_blocks_with_md(struct spdk_bdev_desc *desc, struct spdk_io_channel *ch,
				struct iovec *iovs, int iovcnt, void *md_buf,
				uint64_t offset_blocks, uint64_t num_blocks,
				struct wal_bdev_io *wal_io)
{
	return spdk_bdev_writev_blocks_with_md(desc, ch,
					iovs, iovcnt, md_buf,
					offset_blocks, num_blocks, wal_base_bdev_write_complete, wal_io);
}

static void
wal_bdev_submit_read_request(struct wal_bdev_io *wal_io);

static void
_wal_bdev_submit_read_request(void *_wal_io)
{
	struct wal_bdev_io *wal_io = _wal_io;

	wal_bdev_submit_read_request(wal_io);
}

static void
wal_bdev_read_request_error(int ret, struct wal_bdev_io *wal_io, 
                            struct wal_base_bdev_info    *base_info,
                            struct spdk_io_channel        *base_ch)
{
    if (ret == -ENOMEM) {
        wal_bdev_queue_io_wait(wal_io, base_info->bdev, base_ch,
                    _wal_bdev_submit_read_request);
        return;
    } else {
        SPDK_ERRLOG("bdev io submit error due to %d, it should not happen\n", ret);
        assert(false);
		if (wal_io->read_buf) {
			spdk_free(wal_io->read_buf);
		}
        wal_bdev_io_complete(wal_io, SPDK_BDEV_IO_STATUS_FAILED);
        return;
    }
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
	struct bskiplistNode        *bn;
    uint64_t    read_begin, read_end, read_cur, tmp;

	wal_bdev = wal_io->wal_bdev;
	read_begin = bdev_io->u.bdev.offset_blocks;
    read_end = bdev_io->u.bdev.offset_blocks + bdev_io->u.bdev.num_blocks - 1;

    bn = bslFirstNodeAfterBegin(wal_bdev->bsl, read_begin);

	if (bn && bn->begin <= read_begin && bn->end >= read_end 
        && wal_bdev_is_valid_entry(wal_bdev, bn->ele)) {
        // one entry in log bdev
		ret = spdk_bdev_readv_blocks(wal_bdev->log_bdev_info.desc, wal_bdev->log_channel,
                        bdev_io->u.bdev.iovs, bdev_io->u.bdev.iovcnt,
                        bn->ele->l.bdevOffset + read_begin - bn->ele->begin, bdev_io->u.bdev.num_blocks << wal_bdev->blocklen_shift,
                        wal_base_bdev_read_complete, wal_io);
        
        if (ret != 0) {
			SPDK_NOTICELOG("read from begin: %ld, end: %ld\n.", read_begin, read_end);
			SPDK_NOTICELOG("bn begin: %ld, end: %ld\n.", bn->begin, bn->end);
			SPDK_NOTICELOG("bn ele begin: %ld, end: %ld\n.", bn->ele->begin, bn->ele->end);
			SPDK_NOTICELOG("read from log bdev offset: %ld, delta: %ld\n.", bn->ele->l.bdevOffset, read_begin - bn->ele->begin);
			wal_bdev_read_request_error(ret, wal_io, &wal_bdev->log_bdev_info, wal_bdev->log_channel);
            return;
        }
        return;
    }

    if (!bn || bn->begin > read_end) {
        // not found in index
        ret = spdk_bdev_readv_blocks(wal_bdev->core_bdev_info.desc, wal_bdev->core_channel,
                        bdev_io->u.bdev.iovs, bdev_io->u.bdev.iovcnt,
                        bdev_io->u.bdev.offset_blocks, bdev_io->u.bdev.num_blocks, wal_base_bdev_read_complete,
                        wal_io);

        if (ret != 0) {
			wal_bdev_read_request_error(ret, wal_io, &wal_bdev->core_bdev_info, wal_bdev->core_channel);
            return;
        }
        return;
    }

	// merge from log & core
	wal_io->read_buf = spdk_zmalloc(bdev_io->u.bdev.num_blocks * wal_bdev->bdev.blocklen, 0, NULL, SPDK_ENV_LCORE_ID_ANY, SPDK_MALLOC_DMA);
	wal_io->remaining_base_bdev_io = 0;
	read_cur = read_begin;

	while (read_cur <= read_end) {
		while (bn && !wal_bdev_is_valid_entry(wal_bdev, bn->ele)) {
			bn = bn->level[0].forward;
		}

		if (!bn || read_cur < bn->begin) {
			if (!bn) {
				tmp = read_end;
			} else {
				tmp = bn->begin - 1 > read_end ? read_end : bn->begin - 1;
			}

			wal_io->remaining_base_bdev_io++;
			ret = spdk_bdev_read_blocks(wal_bdev->core_bdev_info.desc, wal_bdev->core_channel,
							wal_io->read_buf + (read_cur - read_begin) * wal_bdev->bdev.blocklen,
							read_cur, tmp - read_cur + 1, wal_base_bdev_read_complete_part,
							wal_io);

			if (ret != 0) {
				wal_bdev_read_request_error(ret, wal_io, &wal_bdev->core_bdev_info, wal_bdev->core_channel);
				return;
			}
			read_cur = tmp + 1;
			continue;
		}

		if (bn && read_cur >= bn->begin) {
			tmp = bn->end > read_end ? read_end : bn->end;
			
			wal_io->remaining_base_bdev_io++;
			ret = spdk_bdev_read_blocks(wal_bdev->log_bdev_info.desc, wal_bdev->log_channel,
							wal_io->read_buf + (read_cur - read_begin) * wal_bdev->bdev.blocklen,
							bn->ele->l.bdevOffset + read_cur - bn->ele->begin, tmp - read_cur + 1, wal_base_bdev_read_complete_part,
							wal_io);

			if (ret != 0) {
				wal_bdev_read_request_error(ret, wal_io, &wal_bdev->log_bdev_info, wal_bdev->log_channel);
				return;
			}
			read_cur = tmp + 1;
			bn = bn->level[0].forward;
			continue;
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
	struct wal_base_bdev_info	*base_info;
	struct spdk_io_channel		*base_ch;
	struct wal_metadata			*metadata;
	uint64_t	log_offset, log_blocks, next_tail;

	wal_bdev = wal_io->wal_bdev;

	base_info = &wal_bdev->log_bdev_info;
	base_ch = wal_bdev->log_channel;

	if (spdk_unlikely(bdev_io->u.bdev.num_blocks >= wal_bdev->log_max)) {
		SPDK_ERRLOG("request block %ld exceeds the max blocks %ld of log device\n", bdev_io->u.bdev.num_blocks, wal_bdev->log_max);
		wal_bdev_io_complete(wal_io, SPDK_BDEV_IO_STATUS_FAILED);
	}

	// use 1 block in log device for metadata
	metadata = (struct wal_metadata *) spdk_zmalloc(wal_bdev->log_bdev_info.bdev->blocklen, 0, NULL, SPDK_ENV_LCORE_ID_ANY, SPDK_MALLOC_DMA);

	if (spdk_unlikely(!metadata)) {
		SPDK_ERRLOG("wal bdev cannot alloc metadata.\n");
		wal_bdev_io_complete(wal_io, SPDK_BDEV_IO_STATUS_NOMEM);
	}

	metadata->version = METADATA_VERSION;
	metadata->seq = ++wal_bdev->seq;
	metadata->core_offset = bdev_io->u.bdev.offset_blocks;
	metadata->core_length =  bdev_io->u.bdev.num_blocks;

	wal_io->metadata = metadata;

	log_blocks = (bdev_io->u.bdev.num_blocks << wal_bdev->blocklen_shift) + 1;
	next_tail = wal_bdev->log_tail + log_blocks;
	if (next_tail >= wal_bdev->log_max) {
		if (spdk_unlikely(wal_bdev->tail_round > wal_bdev->head_round || log_blocks > wal_bdev->log_head)) {
			goto write_no_space;
		} else {
			log_offset = 0;
			wal_bdev->log_tail = log_blocks;
			wal_bdev->tail_round++;
		}
	} else if (wal_bdev->tail_round > wal_bdev->head_round && next_tail > wal_bdev->log_head) {
		goto write_no_space;
	} else {
		log_offset = wal_bdev->log_tail;
		wal_bdev->log_tail += log_blocks;
	}
	metadata->next_offset = wal_bdev->log_tail;
	metadata->length = log_blocks - 1;
	metadata->round = wal_bdev->tail_round;

	ret = wal_log_bdev_writev_blocks_with_md(base_info->desc, base_ch,
					bdev_io->u.bdev.iovs, bdev_io->u.bdev.iovcnt, metadata,
					log_offset, metadata->length, wal_io);

	if (spdk_likely(ret == 0)) {
		
	} else if (ret == -ENOMEM) {
		wal_bdev_queue_io_wait(wal_io, base_info->bdev, base_ch,
					_wal_bdev_submit_write_request);
	} else {
		SPDK_ERRLOG("bdev io submit error due to %d, it should not happen\n", ret);
		wal_bdev_io_complete(wal_io, SPDK_BDEV_IO_STATUS_FAILED);
	}
	return;
write_no_space:
	SPDK_DEBUGLOG(bdev_wal, "queue bdev io submit due to no enough space left on log device.\n");
	TAILQ_INSERT_TAIL(&wal_bdev->pending_writes, wal_io, tailq);
	return;
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
 * _wal_bdev_submit_request function is single thread entry point of IO.
 * params:
 * args - wal io
 * returns:
 * none
 */
static void
_wal_bdev_submit_request(void *arg)
{
	struct spdk_bdev_io *bdev_io = arg;
	struct wal_bdev_io *wal_io = (struct wal_bdev_io *)bdev_io->driver_ctx;

	switch (bdev_io->type) {
	case SPDK_BDEV_IO_TYPE_READ:
		spdk_bdev_io_get_buf(bdev_io, wal_bdev_get_buf_cb,
				     bdev_io->u.bdev.num_blocks * bdev_io->bdev->blocklen);
		break;
	case SPDK_BDEV_IO_TYPE_WRITE:
		wal_bdev_submit_write_request(wal_io);
		break;

	case SPDK_BDEV_IO_TYPE_RESET:
	case SPDK_BDEV_IO_TYPE_FLUSH:
	case SPDK_BDEV_IO_TYPE_UNMAP:
	default:
		SPDK_ERRLOG("submit request, invalid io type %u\n", bdev_io->type);
		wal_bdev_io_complete(wal_io, SPDK_BDEV_IO_STATUS_FAILED);
		break;
	}
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

	wal_io->wal_ch = spdk_io_channel_get_ctx(ch);
	wal_io->wal_bdev = wal_io->wal_ch->wal_bdev;
	wal_io->orig_io = bdev_io;
	wal_io->orig_thread = spdk_get_thread();

	/* Send this request to the open_thread if that's not what we're on. */
	if (wal_io->orig_thread != wal_io->wal_bdev->open_thread) {
		spdk_thread_send_msg(wal_io->wal_bdev->open_thread, _wal_bdev_submit_request, bdev_io);
	} else {
		_wal_bdev_submit_request(bdev_io);
	}
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

	SPDK_DEBUGLOG(bdev_wal, "wal_bdev_dump_config_json\n");
	assert(wal_bdev != NULL);

	/* Dump the wal bdev configuration related information */
	spdk_json_write_named_object_begin(w, "wal");
	spdk_json_write_named_uint32(w, "state", wal_bdev->state);
	spdk_json_write_named_uint32(w, "destruct_called", wal_bdev->destruct_called);

	if (wal_bdev->log_bdev_info.bdev) {
		spdk_json_write_named_string(w, "log_bdev", wal_bdev->log_bdev_info.bdev->name);
	} else {
		spdk_json_write_named_null(w, "log_bdev");
	}

	if (wal_bdev->core_bdev_info.bdev) {
		spdk_json_write_named_string(w, "core_bdev", wal_bdev->core_bdev_info.bdev->name);
	} else {
		spdk_json_write_named_null(w, "core_bdev");
	}

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

	spdk_json_write_object_begin(w);

	spdk_json_write_named_string(w, "method", "bdev_wal_create");

	spdk_json_write_named_object_begin(w, "params");
	spdk_json_write_named_string(w, "name", bdev->name);

	if (wal_bdev->log_bdev_info.bdev) {
		spdk_json_write_named_string(w, "log_bdev", wal_bdev->log_bdev_info.bdev->name);
	} else {
		spdk_json_write_named_null(w, "log_bdev");
	}

	if (wal_bdev->core_bdev_info.bdev) {
		spdk_json_write_named_string(w, "core_bdev", wal_bdev->core_bdev_info.bdev->name);
	} else {
		spdk_json_write_named_null(w, "core_bdev");
	}
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
	TAILQ_REMOVE(&g_wal_config.wal_bdev_config_head, wal_cfg, link);
	g_wal_config.total_wal_bdev--;

	free(wal_cfg->log_bdev.name);
	free(wal_cfg->core_bdev.name);
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
 * _wal_cfg - Pointer to newly added configuration
 */
int
wal_bdev_config_add(const char *wal_name, const char *log_bdev_name, const char *core_bdev_name, 
					struct wal_bdev_config **_wal_cfg)
{
	struct wal_bdev_config *wal_cfg;

	wal_cfg = wal_bdev_config_find_by_name(wal_name);
	if (wal_cfg != NULL) {
		SPDK_ERRLOG("Duplicate wal bdev name found in config file %s\n",
			    wal_name);
		return -EEXIST;
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

	wal_cfg->log_bdev.name = strdup(log_bdev_name);
	if (!wal_cfg->log_bdev.name) {
		free(wal_cfg->name);
		free(wal_cfg);
		SPDK_ERRLOG("unable to allocate memory\n");
		return -ENOMEM;
	}

	wal_cfg->core_bdev.name = strdup(core_bdev_name);
	if (!wal_cfg->core_bdev.name) {
		free(wal_cfg->log_bdev.name);
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
 * is_log - if bdev can be claimed, it represents the bdev is log or core
 * slot. This field is only valid if return value of this function is true
 * returns:
 * true - if bdev can be claimed
 * false - if bdev can't be claimed
 */
static bool
wal_bdev_can_claim_bdev(const char *bdev_name, struct wal_bdev_config **_wal_cfg,
			 bool *is_log)
{
	struct wal_bdev_config *wal_cfg;

	TAILQ_FOREACH(wal_cfg, &g_wal_config.wal_bdev_config_head, link) {
		if (!strcmp(bdev_name, wal_cfg->log_bdev.name)) {
			*_wal_cfg = wal_cfg;
			*is_log = true;
			return true;
		}

		if (!strcmp(bdev_name, wal_cfg->core_bdev.name)) {
			*_wal_cfg = wal_cfg;
			*is_log = false;
			return true;
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

	wal_bdev->state = WAL_BDEV_STATE_CONFIGURING;
	wal_bdev->config = wal_cfg;

	wal_bdev_gen = &wal_bdev->bdev;

	wal_bdev_gen->name = strdup(wal_cfg->name);
	if (!wal_bdev_gen->name) {
		SPDK_ERRLOG("Unable to allocate name for wal\n");
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
	struct spdk_bdev *wal_bdev_gen;
	int rc = 0;

	SPDK_NOTICELOG("Configure wal bdev %s.\n", wal_bdev->bdev.name);

	assert(wal_bdev->state == WAL_BDEV_STATE_CONFIGURING);

	wal_bdev->blocklen_shift = spdk_u32log2(wal_bdev->core_bdev_info.bdev->blocklen) - spdk_u32log2(wal_bdev->log_bdev_info.bdev->blocklen);
	wal_bdev_gen = &wal_bdev->bdev;

	wal_bdev->state = WAL_BDEV_STATE_ONLINE;
	
	pthread_mutex_init(&wal_bdev->mutex, NULL);

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
		// TODO
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

	if (wal_bdev->log_bdev_info.bdev != NULL) {
		assert(wal_bdev->log_bdev_info.desc);
		wal_bdev->log_bdev_info.remove_scheduled = true;
		if (wal_bdev->destruct_called == true ||
		    wal_bdev->state == WAL_BDEV_STATE_CONFIGURING) {
			/*
			 * As wal bdev is not registered yet or already unregistered,
			 * so cleanup should be done here itself.
			 */
			wal_bdev_free_base_bdev_resource(wal_bdev, &wal_bdev->log_bdev_info);
		}
	}

	if (wal_bdev->core_bdev_info.bdev != NULL) {
		assert(wal_bdev->core_bdev_info.desc);
		wal_bdev->core_bdev_info.remove_scheduled = true;
		if (wal_bdev->destruct_called == true ||
		    wal_bdev->state == WAL_BDEV_STATE_CONFIGURING) {
			/*
			 * As wal bdev is not registered yet or already unregistered,
			 * so cleanup should be done here itself.
			 */
			wal_bdev_free_base_bdev_resource(wal_bdev, &wal_bdev->core_bdev_info);
		}
	}

	wal_bdev_cleanup(wal_bdev);
	if (cb_fn) {
		cb_fn(cb_arg, 0);
	}
	return;
}

/*
 * brief:
 * wal_bdev_add_log_device function is the actual function which either adds
 * the nvme base device to existing wal bdev. It also claims
 * the base device and keep the open descriptor.
 * params:
 * wal_cfg - pointer to wal bdev config
 * returns:
 * 0 - success
 * non zero - failure
 */
static int
wal_bdev_add_log_device(struct wal_bdev_config *wal_cfg)
{
	struct wal_bdev	*wal_bdev;
	struct spdk_bdev *bdev;
	struct spdk_bdev_desc *desc;
	int			rc;

	wal_bdev = wal_cfg->wal_bdev;
	if (!wal_bdev) {
		SPDK_ERRLOG("WAL bdev '%s' is not created yet\n", wal_cfg->name);
		return -ENODEV;
	}

	rc = spdk_bdev_open_ext(wal_cfg->log_bdev.name, true, wal_bdev_event_base_bdev, NULL, &desc);
	if (rc != 0) {
		if (rc != -ENODEV) {
			SPDK_ERRLOG("Unable to create desc on bdev '%s'\n", wal_cfg->log_bdev.name);
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

	SPDK_DEBUGLOG(bdev_wal, "bdev %s is claimed\n", wal_cfg->log_bdev.name);
	wal_bdev->log_bdev_info.thread = spdk_get_thread();
	wal_bdev->log_bdev_info.bdev = bdev;
	wal_bdev->log_bdev_info.desc = desc;

	return 0;
}

/*
 * brief:
 * wal_bdev_add_log_device function is the actual function which either adds
 * the nvme base device to existing wal bdev. It also claims
 * the base device and keep the open descriptor.
 * params:
 * wal_cfg - pointer to wal bdev config
 * returns:
 * 0 - success
 * non zero - failure
 */
static int
wal_bdev_add_core_device(struct wal_bdev_config *wal_cfg)
{
	struct wal_bdev	*wal_bdev;
	struct spdk_bdev *bdev;
	struct spdk_bdev_desc *desc;
	int			rc;

	wal_bdev = wal_cfg->wal_bdev;
	if (!wal_bdev) {
		SPDK_ERRLOG("WAL bdev '%s' is not created yet\n", wal_cfg->name);
		return -ENODEV;
	}

	rc = spdk_bdev_open_ext(wal_cfg->core_bdev.name, true, wal_bdev_event_base_bdev, NULL, &desc);
	if (rc != 0) {
		if (rc != -ENODEV) {
			SPDK_ERRLOG("Unable to create desc on bdev '%s'\n", wal_cfg->core_bdev.name);
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

	SPDK_DEBUGLOG(bdev_wal, "bdev %s is claimed\n", wal_cfg->core_bdev.name);
	wal_bdev->core_bdev_info.thread = spdk_get_thread();
	wal_bdev->core_bdev_info.bdev = bdev;
	wal_bdev->core_bdev_info.desc = desc;

	wal_bdev->bdev.blocklen = bdev->blocklen;
	wal_bdev->bdev.blockcnt = bdev->blockcnt;

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
	struct wal_bdev	*wal_bdev;
	int			rc;

	wal_bdev = wal_cfg->wal_bdev;
	if (!wal_bdev) {
		SPDK_ERRLOG("WAL bdev '%s' is not created yet\n", wal_cfg->name);
		return -ENODEV;
	}

	rc = wal_bdev_add_log_device(wal_cfg);
	if (rc) {
		SPDK_ERRLOG("Failed to add log device '%s' to WAL bdev '%s'.\n", wal_cfg->log_bdev.name, wal_cfg->name);
		return rc;
	}

	rc = wal_bdev_add_core_device(wal_cfg);
	if (rc) {
		SPDK_ERRLOG("Failed to add core device '%s' to WAL bdev '%s'.\n", wal_cfg->core_bdev.name, wal_cfg->name);
		return rc;
	}

	rc = wal_bdev_start(wal_bdev);
	if (rc) {
		SPDK_ERRLOG("Failed to start WAL bdev '%s'.\n", wal_cfg->name);
		return rc;
	}

	rc = wal_bdev_configure(wal_bdev);
	if (rc != 0) {
		SPDK_ERRLOG("Failed to configure WAL bdev\n");
		return rc;
	}
	
	return 0;
}

static int
wal_bdev_start(struct wal_bdev *wal_bdev)
{
	uint64_t mempool_size;
	int i;

	wal_bdev->log_max = wal_bdev->log_bdev_info.bdev->blockcnt - 2;  // last block used to track log head

	mempool_size = wal_bdev->log_bdev_info.bdev->blockcnt < wal_bdev->core_bdev_info.bdev->blockcnt 
				? wal_bdev->log_bdev_info.bdev->blockcnt
				: wal_bdev->core_bdev_info.bdev->blockcnt;
	mempool_size = spdk_align64pow2(mempool_size);

	wal_bdev->bstat_pool = spdk_mempool_create("WAL_BSTAT_POOL", mempool_size, sizeof(bstat), SPDK_MEMPOOL_DEFAULT_CACHE_SIZE, SPDK_ENV_SOCKET_ID_ANY);
	wal_bdev->bsl_node_pool = spdk_mempool_create("WAL_BSL_NODE_POOL", mempool_size, sizeof(bskiplistNode), SPDK_MEMPOOL_DEFAULT_CACHE_SIZE, SPDK_ENV_SOCKET_ID_ANY);

	wal_bdev->bsl = bslCreate(wal_bdev->bsl_node_pool, wal_bdev->bstat_pool);
	wal_bdev->bslfn = bslfnCreate(wal_bdev->bsl_node_pool, wal_bdev->bstat_pool);
	TAILQ_INIT(&wal_bdev->pending_writes);
	for (i = 0; i < MAX_OUTSTANDING_MOVES; i++) {
		wal_bdev->mover_context[i].state = MOVER_IDLE;
	}
	// TODO: recover
	wal_bdev->log_head = wal_bdev->move_head = 0;
	wal_bdev->log_tail = 0;
	wal_bdev->head_round = wal_bdev->move_round = 0;
	wal_bdev->tail_round = 0;

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
	bool			is_log;

	if (wal_bdev_can_claim_bdev(bdev->name, &wal_cfg, &is_log)) {
		if (is_log) {
			wal_bdev_add_log_device(wal_cfg);
		} else {
			wal_bdev_add_core_device(wal_cfg);
		}
	} else {
		SPDK_DEBUGLOG(bdev_wal, "bdev %s can't be claimed\n",
			      bdev->name);
	}

	spdk_bdev_module_examine_done(&g_wal_if);
}

static int
wal_bdev_submit_pending_writes(void *ctx)
{
	struct wal_bdev *wal_bdev = ctx;
	struct wal_bdev_io	*wal_io;
	struct wal_base_bdev_info	*base_info;
	struct spdk_io_channel		*base_ch;
	int		ret;
	uint64_t log_blocks, next_tail, log_offset;

	while (!TAILQ_EMPTY(&wal_bdev->pending_writes)) {
		wal_io = TAILQ_FIRST(&wal_bdev->pending_writes);

		base_info = &wal_bdev->log_bdev_info;
		base_ch = wal_bdev->log_channel;

		log_blocks = (wal_io->metadata->core_length << wal_bdev->blocklen_shift) + 1;
		next_tail = wal_bdev->log_tail + log_blocks;
		if (next_tail >= wal_bdev->log_max) {
			if (spdk_unlikely(wal_bdev->tail_round > wal_bdev->head_round || log_blocks > wal_bdev->log_head)) {
				return SPDK_POLLER_BUSY;
			} else {
				log_offset = 0;
				wal_bdev->log_tail = log_blocks;
				wal_bdev->tail_round++;
			}
		} else if (wal_bdev->tail_round > wal_bdev->head_round && next_tail > wal_bdev->log_head) {
			return SPDK_POLLER_BUSY;
		} else {
			log_offset = wal_bdev->log_tail;
			wal_bdev->log_tail += log_blocks;
		}
		wal_io->metadata->next_offset = wal_bdev->log_tail;
		wal_io->metadata->length = log_blocks - 1;
		wal_io->metadata->round = wal_bdev->tail_round;

		ret = wal_log_bdev_writev_blocks_with_md(base_info->desc, base_ch,
						wal_io->orig_io->u.bdev.iovs, wal_io->orig_io->u.bdev.iovcnt, wal_io->metadata,
						log_offset, wal_io->metadata->length, wal_io);

		if (spdk_likely(ret == 0)) {
			TAILQ_REMOVE(&wal_bdev->pending_writes, wal_io, tailq);
		} else if (ret == -ENOMEM) {	
			// retry next time
		} else {
			SPDK_ERRLOG("pending write submit error due to %d, it should not happen\n", ret);
			wal_bdev_io_complete(wal_io, SPDK_BDEV_IO_STATUS_FAILED);
		}
		return SPDK_POLLER_BUSY;
	}

	return SPDK_POLLER_IDLE;
}


static int
wal_bdev_mover(void *ctx)
{
	struct wal_bdev *bdev = ctx;
	int ret, i;

	if (bdev->move_head == bdev->log_tail && bdev->move_round == bdev->tail_round) {
		return SPDK_POLLER_IDLE;
	}

	spdk_trace_record_tsc(spdk_get_ticks(), TRACE_WAL_MOVE_CALLED, 0, 0, (uintptr_t)NULL);

	for (i = 0; i < MAX_OUTSTANDING_MOVES; i++) {
		if (bdev->mover_context[i].state == MOVER_READING_MD) {
			spdk_trace_record_tsc(spdk_get_ticks(), TRACE_WAL_MOVE_MD_LOCKED, 0, 0, (uintptr_t)NULL);
			return SPDK_POLLER_BUSY;
		}
	}

	for (i = 0; i < MAX_OUTSTANDING_MOVES; i++) {
		if (bdev->mover_context[i].state == MOVER_IDLE) {
			break;
		}
	}

	if (i == MAX_OUTSTANDING_MOVES) {
		SPDK_DEBUGLOG(bdev_wal, "All movers are used.\n");
		spdk_trace_record_tsc(spdk_get_ticks(), TRACE_WAL_MOVE_NO_WORKER, 0, 0, (uintptr_t)NULL);
		return SPDK_POLLER_BUSY;
	}

	spdk_trace_record_tsc(spdk_get_ticks(), TRACE_WAL_MOVE_READ_MD, 0, 0, (uintptr_t)&bdev->mover_context[i], i, bdev->move_head);
	bdev->mover_context[i].state = MOVER_READING_MD;
	bdev->mover_context[i].id = i;
	bdev->mover_context[i].bdev = bdev;
	bdev->mover_context[i].metadata = (struct wal_metadata *) spdk_zmalloc(bdev->log_bdev_info.bdev->blocklen, 0, 
														NULL, SPDK_ENV_LCORE_ID_ANY, SPDK_MALLOC_DMA);
	if (!bdev->mover_context[i].metadata) {
		SPDK_DEBUGLOG(bdev_wal, "No mem for metadata.\n");
		wal_bdev_mover_free(&bdev->mover_context[i]);
		return SPDK_POLLER_BUSY;
	}

	ret = spdk_bdev_read_blocks(bdev->log_bdev_info.desc, bdev->log_channel, bdev->mover_context[i].metadata, bdev->move_head, 1, 
									wal_bdev_mover_read_data, &bdev->mover_context[i]);
	if (ret) {
		SPDK_ERRLOG("Failed to read metadata during move: %d.\n", ret);
		wal_bdev_mover_free(&bdev->mover_context[i]);
	}

	return SPDK_POLLER_BUSY;
}

static void
wal_bdev_mover_read_data(struct spdk_bdev_io *bdev_io, bool success, void *ctx)
{
	struct wal_mover_context *mover_ctx = ctx;
	struct wal_bdev *bdev = mover_ctx->bdev;
	struct wal_metadata *metadata = mover_ctx->metadata;
	int ret, i;
	uint64_t log_position, data_begin, date_end, other_begin, other_end;

	spdk_bdev_free_io(bdev_io);

	if (spdk_unlikely(bdev->destruct_called || bdev->destroy_started)) {
		wal_bdev_mover_free(mover_ctx);
		return;
	}

	if (spdk_unlikely(!success)) {
		SPDK_ERRLOG("Failed to read metadata during move.\n");
		wal_bdev_mover_reset(bdev);
		wal_bdev_mover_free(mover_ctx);
		return;
	}

	assert(metadata);
	if (spdk_unlikely(metadata->version != METADATA_VERSION)) {
		SPDK_DEBUGLOG(bdev_wal, "Go back to block '0' during move.\n");
		bdev->move_head = 0;
		bdev->move_round++;
		wal_bdev_mover_free(mover_ctx);
		return;
	}

	data_begin = metadata->core_offset;
	date_end = metadata->core_offset + metadata->core_length - 1;
	for (i = 0; i < MAX_OUTSTANDING_MOVES; i++) {
		if ( i != mover_ctx->id
			&& bdev->mover_context[i].state != MOVER_IDLE 
			&& bdev->mover_context[i].metadata) {
			other_begin = bdev->mover_context[i].metadata->core_offset;
			other_end = bdev->mover_context[i].metadata->core_offset + bdev->mover_context[i].metadata->core_length - 1;
			if (other_end >= data_begin && other_begin <= date_end) {
				wal_bdev_mover_free(mover_ctx);
				return;
			}
		}
	}

	// Can be moved in parallel
	mover_ctx->data = spdk_zmalloc(bdev->log_bdev_info.bdev->blocklen * metadata->length, 0, 
								NULL, SPDK_ENV_LCORE_ID_ANY, SPDK_MALLOC_DMA);
	
	if (!mover_ctx->data) {
		SPDK_DEBUGLOG(bdev_wal, "No mem for data.\n");
		wal_bdev_mover_free(mover_ctx);
		return;
	}

	log_position = bdev->move_head + 1;
	bdev->move_head = metadata->next_offset;
	mover_ctx->state = MOVER_READING_DATA;
	spdk_trace_record_tsc(spdk_get_ticks(), TRACE_WAL_MOVE_READ_DATA, 0, 0, (uintptr_t)mover_ctx, mover_ctx->id, log_position, metadata->length, metadata->round, metadata->next_offset);
	
	ret = spdk_bdev_read_blocks(bdev->log_bdev_info.desc, bdev->log_channel, mover_ctx->data, log_position, metadata->length, 
									wal_bdev_mover_write_data, mover_ctx);
	if (ret) {
		SPDK_ERRLOG("Failed to read data during move: %d.\n", ret);
		wal_bdev_mover_free(ctx);
	}
}

static void 
wal_bdev_mover_write_data(struct spdk_bdev_io *bdev_io, bool success, void *ctx)
{
	struct wal_mover_context *mover_ctx = ctx;
	struct wal_bdev *bdev = mover_ctx->bdev;
	struct wal_metadata *metadata = mover_ctx->metadata;
	int ret;

	spdk_bdev_free_io(bdev_io);

	if (spdk_unlikely(bdev->destruct_called || bdev->destroy_started)) {
		wal_bdev_mover_free(mover_ctx);
		return;
	}

	if (spdk_unlikely(!success)) {
		SPDK_ERRLOG("Failed to read data during move.\n");
		wal_bdev_mover_reset(bdev);
		wal_bdev_mover_free(mover_ctx);
		return;
	}

	spdk_trace_record_tsc(spdk_get_ticks(), TRACE_WAL_MOVE_WRITE_DATA, 0, 0, (uintptr_t)mover_ctx, mover_ctx->id, metadata->core_offset, metadata->core_length, metadata->round);
	mover_ctx->state = MOVER_WRITING_DATA;

	ret = spdk_bdev_write_blocks(bdev->core_bdev_info.desc, bdev->core_channel, mover_ctx->data,
									metadata->core_offset, metadata->core_length,
									wal_bdev_mover_update_head, mover_ctx);
	if (ret) {
		SPDK_ERRLOG("Failed to write data during move: %d.\n", ret);
		wal_bdev_mover_free(mover_ctx);
	}
}

static void 
wal_bdev_mover_update_head(struct spdk_bdev_io *bdev_io, bool success, void *ctx)
{
	struct wal_mover_context *mover_ctx = ctx;
	struct wal_bdev *bdev = mover_ctx->bdev;
	struct wal_metadata *metadata = mover_ctx->metadata;
	int ret, i, j;
	struct wal_log_info *info;

	spdk_bdev_free_io(bdev_io);

	if (spdk_unlikely(bdev->destruct_called || bdev->destroy_started)) {
		return;
	}

	if (spdk_unlikely(!success)) {
		SPDK_ERRLOG("Failed to write data during move.\n");
		wal_bdev_mover_reset(bdev);
		wal_bdev_mover_free(mover_ctx);
		return;
	}

	spdk_trace_record_tsc(spdk_get_ticks(), TRACE_WAL_MOVE_UPDATE_HEAD, 0, 0, (uintptr_t)mover_ctx, mover_ctx->id, metadata->next_offset, metadata->round);
	mover_ctx->state = MOVER_UPDATING_HEAD;
	for (i = 0; i < MAX_OUTSTANDING_MOVES; i++) {
		if (i != mover_ctx->id
			&& bdev->mover_context[i].state > MOVER_READING_MD	// Reading md is run in serial. So, if the worker is still reading md, then it must be a later entry.
			&& bdev->mover_context[i].state < MOVER_PERSIST_HEAD // Wait only for on air works
			&& bdev->mover_context[i].metadata
			&& (bdev->mover_context[i].metadata->round < mover_ctx->metadata->round
				|| (bdev->mover_context[i].metadata->next_offset < mover_ctx->metadata->next_offset 
					&& bdev->mover_context[i].metadata->round == mover_ctx->metadata->round))) {
				SPDK_DEBUGLOG(bdev_wal, "%ld(%ld) waiting %ld(%ld) to update head.\n",  metadata->next_offset, metadata->round, 
								bdev->mover_context[i].metadata->next_offset, bdev->mover_context[i].metadata->round);
				spdk_trace_record_tsc(spdk_get_ticks(), TRACE_WAL_MOVE_WAIT_OTHERS, 0, 0, (uintptr_t)mover_ctx, 
										metadata->next_offset, metadata->round, bdev->mover_context[i].id,
										bdev->mover_context[i].metadata->next_offset, bdev->mover_context[i].metadata->round);
				return;
			}
	}

	info = spdk_zmalloc(bdev->log_bdev_info.bdev->blocklen, 0, 
							NULL, SPDK_ENV_LCORE_ID_ANY, SPDK_MALLOC_DMA);

	memset(bdev->sorted_context, 0, MAX_OUTSTANDING_MOVES * sizeof(bdev->sorted_context[0]));
	for (i = 0; i < MAX_OUTSTANDING_MOVES; i++) {
		if (bdev->mover_context[i].state != MOVER_IDLE && bdev->mover_context[i].state != MOVER_PERSIST_HEAD) {
			assert(bdev->sorted_context[MAX_OUTSTANDING_MOVES - 1] == NULL);
			for (j = MAX_OUTSTANDING_MOVES - 2; j >= 0 ; j--) {
				if (bdev->sorted_context[j] && bdev->sorted_context[j]->metadata) {
					if (bdev->sorted_context[j]->metadata->round > bdev->mover_context[i].metadata->round
						|| bdev->sorted_context[j]->metadata->next_offset > bdev->mover_context[i].metadata->next_offset) {
						bdev->sorted_context[j+1] = bdev->sorted_context[j];
					} else {
						bdev->sorted_context[j] = &bdev->mover_context[i];
						break;
					}
				}
			}
		}
	}

	for (i = 0; i < MAX_OUTSTANDING_MOVES; i++) {
		if (bdev->sorted_context[i] && bdev->sorted_context[i]->state == MOVER_UPDATING_HEAD) {
			break;
		}
	}
	assert(bdev->sorted_context[i] == mover_ctx);
	for (; i < MAX_OUTSTANDING_MOVES; i++) {
		if (bdev->sorted_context[i] && bdev->sorted_context[i]->state == MOVER_UPDATING_HEAD) {
			info->head = bdev->sorted_context[i]->metadata->next_offset;
			info->round = bdev->sorted_context[i]->metadata->round;
		} else {
			break;
		}
	}

	mover_ctx->info = info;
	mover_ctx->state = MOVER_PERSIST_HEAD;

	ret = spdk_bdev_write_blocks(bdev->log_bdev_info.desc, bdev->log_channel, info,
									bdev->log_max, 1,
									wal_bdev_mover_clean, mover_ctx);
	if (ret) {
		SPDK_ERRLOG("Failed to update head to log bdev during move: %d.\n", ret);
		wal_bdev_mover_free(mover_ctx);
	}
}

static void
wal_bdev_mover_clean(struct spdk_bdev_io *bdev_io, bool success, void *ctx)
{
	struct wal_mover_context *mover_ctx = ctx;
	struct wal_bdev *bdev = mover_ctx->bdev;

	spdk_bdev_free_io(bdev_io);
	
	if (spdk_unlikely(bdev->destruct_called || bdev->destroy_started)) {
		return;
	}

	if (spdk_unlikely(!success)) {
		SPDK_ERRLOG("Failed to update head to log bdev during move.\n");
		wal_bdev_mover_reset(bdev);
		wal_bdev_mover_free(ctx);
		return;
	}

	bdev->log_head = mover_ctx->info->head;
	bdev->head_round = mover_ctx->info->round;
	spdk_trace_record_tsc(spdk_get_ticks(), TRACE_WAL_MOVE_UPDATE_HEAD_END, 0, 0, (uintptr_t)mover_ctx, mover_ctx->id, bdev->log_head, bdev->head_round);
	
	wal_bdev_mover_free(mover_ctx);
}

static void
wal_bdev_mover_free(struct wal_mover_context *ctx)
{
	if (ctx->info) {
		spdk_free(ctx->info);
		ctx->info = NULL;
	}
	if (ctx->data) {
		spdk_free(ctx->data);
		ctx->data = NULL;
	}
	if (ctx->metadata) {
		spdk_free(ctx->metadata);
		ctx->metadata = NULL;
	}

	ctx->state = MOVER_IDLE;
}

static void
wal_bdev_mover_reset(struct wal_bdev *bdev)
{
	bdev->move_head = bdev->log_head;
	bdev->move_round = bdev->head_round;
}

static int
wal_bdev_cleaner(void *ctx)
{
	struct wal_bdev *wal_bdev = ctx;
	bskiplistNode *update[BSKIPLIST_MAXLEVEL], *x, *tmp;
	int i, j, count = 0, total;
	
    long rand = random();

    for (i = 0; i < 3; i++) {
        rand <<= 4;
        rand += random();
    }
    rand %= wal_bdev->bdev.blockcnt;

    x = wal_bdev->bsl->header;
    for (i = wal_bdev->bsl->level-1; i >= 0; i--) {
        while (x->level[i].forward &&
                (x->level[i].forward->end < rand))
        {
            x = x->level[i].forward;
        }
        update[i] = x;
    }

	// try removal for level times.
	for (i = 0; i < wal_bdev->bsl->level; i++) {
		tmp = x->level[0].forward;
		if (tmp) {
			if (wal_bdev_is_valid_entry(wal_bdev, tmp->ele)) {
				for (j = 0; j < tmp->height; j++) {
					update[j] = tmp;
				}
			} else {
				for (j = 0; j < tmp->height; j++) {
					update[j]->level[j].forward = tmp->level[j].forward;
					tmp->level[j].forward = NULL;
				}
				wal_bdev->bslfn->tail->level[0].forward = tmp;
				wal_bdev->bslfn->tail = tmp;
				count++;
			}
		} else {
			break;
		}
	}

	total = bslfnFree(wal_bdev->bslfn, 10);

	if (total) {
		return SPDK_POLLER_BUSY;
	}

	return SPDK_POLLER_IDLE;
}

static int
wal_bdev_stat_report(void *ctx)
{
	struct wal_bdev *bdev = ctx;

	// bslPrint(bdev->bsl, 1);
	SPDK_NOTICELOG("WAL bdev head: %ld(%ld), tail: %ld(%ld).\n", bdev->log_head, bdev->head_round, bdev->log_tail, bdev->tail_round);

	return SPDK_POLLER_BUSY;
}

/* Log component for bdev wal bdev module */
SPDK_LOG_REGISTER_COMPONENT(bdev_wal)

SPDK_TRACE_REGISTER_FN(wal_trace, "wal", TRACE_GROUP_WAL)
{
	struct spdk_trace_tpoint_opts opts[] = {
		{
			"WAL_BSTAT_CREATE_START", TRACE_WAL_BSTAT_CREATE_START,
			OWNER_WAL, OBJECT_WAL_IO, 1,
			{}
		},
		{
			"WAL_BSTAT_CREATE_END", TRACE_WAL_BSTAT_CREATE_END,
			OWNER_WAL, OBJECT_WAL_IO, 0,
			{}
		},
		{
			"WAL_BSL_INSERT_START", TRACE_WAL_BSL_INSERT_START,
			OWNER_WAL, OBJECT_WAL_IO, 1,
			{}
		},
		{
			"WAL_BSL_INSERT_END", TRACE_WAL_BSL_INSERT_END,
			OWNER_WAL, OBJECT_WAL_IO, 0,
			{}
		},
		{
			"WAL_BSL_RAND_START", TRACE_WAL_BSL_RAND_START,
			OWNER_WAL, OBJECT_WAL_IO, 1,
			{}
		},
		{
			"WAL_BSL_RAND_END", TRACE_WAL_BSL_RAND_END,
			OWNER_WAL, OBJECT_WAL_IO, 0,
			{}
		},
		{
			"WAL_MOVE_READ_MD", TRACE_WAL_MOVE_READ_MD,
			OWNER_WAL, OBJECT_WAL_IO, 1,
			{
				{ "id", SPDK_TRACE_ARG_TYPE_INT, 8 },
				{ "offset", SPDK_TRACE_ARG_TYPE_INT, 8 },
			}
		},
		{
			"WAL_MOVE_READ_DATA", TRACE_WAL_MOVE_READ_DATA,
			OWNER_WAL, OBJECT_WAL_IO, 0,
			{
				{ "id", SPDK_TRACE_ARG_TYPE_INT, 8 },
				{ "offset", SPDK_TRACE_ARG_TYPE_INT, 8 },
				{ "length", SPDK_TRACE_ARG_TYPE_INT, 8 },
				{ "round", SPDK_TRACE_ARG_TYPE_INT, 8 },
				{ "next", SPDK_TRACE_ARG_TYPE_INT, 8 },
			}
		},
		{
			"WAL_MOVE_WRITE_DATA", TRACE_WAL_MOVE_WRITE_DATA,
			OWNER_WAL, OBJECT_WAL_IO, 0,
			{
				{ "id", SPDK_TRACE_ARG_TYPE_INT, 8 },
				{ "offset", SPDK_TRACE_ARG_TYPE_INT, 8 },
				{ "length", SPDK_TRACE_ARG_TYPE_INT, 8 },
				{ "round", SPDK_TRACE_ARG_TYPE_INT, 8 },
			}
		},
		{
			"WAL_MOVE_UPDATE_HEAD", TRACE_WAL_MOVE_UPDATE_HEAD,
			OWNER_WAL, OBJECT_WAL_IO, 0,
			{
				{ "id", SPDK_TRACE_ARG_TYPE_INT, 8 },
				{ "head", SPDK_TRACE_ARG_TYPE_INT, 8 },
				{ "round", SPDK_TRACE_ARG_TYPE_INT, 8 },
			}
		},
		{
			"WAL_MOVE_WAIT_OTHERS", TRACE_WAL_MOVE_WAIT_OTHERS,
			OWNER_WAL, OBJECT_WAL_IO, 0,
			{
				{ "head", SPDK_TRACE_ARG_TYPE_INT, 8 },
				{ "round", SPDK_TRACE_ARG_TYPE_INT, 8 },
				{ "o_id", SPDK_TRACE_ARG_TYPE_INT, 8 },
				{ "o_head", SPDK_TRACE_ARG_TYPE_INT, 8 },
				{ "o_round", SPDK_TRACE_ARG_TYPE_INT, 8 },
			}
		},
		{
			"WAL_MOVE_UPDATE_HEAD_END", TRACE_WAL_MOVE_UPDATE_HEAD_END,
			OWNER_WAL, OBJECT_WAL_IO, 0,
			{
				{ "id", SPDK_TRACE_ARG_TYPE_INT, 8 },
				{ "head", SPDK_TRACE_ARG_TYPE_INT, 8 },
				{ "round", SPDK_TRACE_ARG_TYPE_INT, 8 },
			}
		},
		{
			"WAL_MOVE_CALLED", TRACE_WAL_MOVE_CALLED,
			OWNER_WAL, OBJECT_WAL_IO, 1,
			{}
		},
		{
			"WAL_MOVE_MD_LOCKED", TRACE_WAL_MOVE_MD_LOCKED,
			OWNER_WAL, OBJECT_WAL_IO, 1,
			{}
		},
		{
			"WAL_MOVE_NO_WORKER", TRACE_WAL_MOVE_NO_WORKER,
			OWNER_WAL, OBJECT_WAL_IO, 1,
			{}
		},
	};

	spdk_trace_register_owner(OWNER_WAL, 'b');
	spdk_trace_register_object(OBJECT_WAL_IO, 'i');
	spdk_trace_register_description_ext(opts, SPDK_COUNTOF(opts));
}
