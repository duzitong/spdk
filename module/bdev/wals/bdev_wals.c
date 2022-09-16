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

#include "bdev_wals.h"
#include "spdk/env.h"
#include "spdk/thread.h"
#include "spdk/likely.h"
#include "spdk/log.h"
#include "spdk/string.h"
#include "spdk/util.h"
#include "spdk/json.h"
#include "spdk/trace.h"

static bool g_shutdown_started = false;

/* wals bdev config as read from config file */
struct wals_config	g_wals_config = {
	.wals_bdev_config_head = TAILQ_HEAD_INITIALIZER(g_wals_config.wals_bdev_config_head),
};

/*
 * List of wals bdev in configured list, these wals bdevs are registered with
 * bdev layer
 */
struct wals_configured_tailq	g_wals_bdev_configured_list = TAILQ_HEAD_INITIALIZER(
			g_wals_bdev_configured_list);

/* List of wals bdev in configuring list */
struct wals_configuring_tailq	g_wals_bdev_configuring_list = TAILQ_HEAD_INITIALIZER(
			g_wals_bdev_configuring_list);

/* List of all wals bdevs */
struct wals_all_tailq		g_wals_bdev_list = TAILQ_HEAD_INITIALIZER(g_wals_bdev_list);

/* List of all wals bdevs that are offline */
struct wals_offline_tailq	g_wals_bdev_offline_list = TAILQ_HEAD_INITIALIZER(
			g_wals_bdev_offline_list);

static TAILQ_HEAD(, wals_target_module) g_wals_target_modules = TAILQ_HEAD_INITIALIZER(g_wals_target_modules);

static struct wals_target_module *wals_bdev_target_module_find(char *name)
{
	struct wals_target_module *target_module;

	TAILQ_FOREACH(target_module, &g_wals_target_modules, link) {
		if (strcmp(target_module->name, name) == 0) {
			return target_module;
		}
	}

	return NULL;
}

void wals_bdev_target_module_list_add(struct wals_target_module *target_module)
{
	if (wals_bdev_target_module_find(target_module->name) != NULL) {
		SPDK_ERRLOG("target module '%s' already registered.\n", target_module->name);
		assert(false);
	} else {
		TAILQ_INSERT_TAIL(&g_wals_target_modules, target_module, link);
	}
}

/* Function declarations */
static void	wals_bdev_examine(struct spdk_bdev *bdev);
static int	wals_bdev_start(struct wals_bdev *bdev);
static void	wals_bdev_stop(struct wals_bdev *bdev);
static int	wals_bdev_init(void);
static bool wals_bdev_is_valid_entry(struct wals_log_position after, struct bstat *bstat);
static int wals_bdev_submit_pending_writes(void *ctx);
static int wals_bdev_log_head_update(void *ctx);
static int wals_bdev_cleaner(void *ctx);
static int wals_bdev_stat_report(void *ctx);

/*
 * brief:
 * wals_bdev_create_cb function is a cb function for wals bdev which creates the
 * hierarchy from wals bdev to base bdev io channels. It will be called per core
 * params:
 * io_device - pointer to wals bdev io device represented by wals_bdev
 * ctx_buf - pointer to context buffer for wals bdev io channel
 * returns:
 * 0 - success
 * non zero - failure
 */
static int
wals_bdev_create_cb(void *io_device, void *ctx_buf)
{
	struct wals_bdev            *wals_bdev = io_device;
	struct wals_bdev_io_channel *wals_ch = ctx_buf;
	uint32_t 					lcore;
	struct spdk_cpuset			*set = spdk_thread_get_cpumask(spdk_get_thread());

	SPDK_DEBUGLOG(bdev_wals, "wals_bdev_create_cb, %p\n", wals_ch);

	assert(wals_bdev != NULL);
	assert(wals_bdev->state == WALS_BDEV_STATE_ONLINE);

	wals_ch->wals_bdev = wals_bdev;

	pthread_mutex_lock(&wals_bdev->mutex);
	SPDK_NOTICELOG("Core mask of current thread: 0x%s\n", spdk_cpuset_fmt(set));
	for (lcore = 0; lcore < SPDK_CPUSET_SIZE; lcore++) {
		if (spdk_cpuset_get_cpu(set, lcore)) {
			break;
		}
	}

	if (lcore % 2 == 0) {
		if (!wals_bdev->write_thread_set) {
			// TODO: call module to register write pollers
			wals_bdev->pending_writes_poller = SPDK_POLLER_REGISTER(wals_bdev_submit_pending_writes, wals_bdev, 0);

			wals_bdev->write_thread = spdk_get_thread();
			if (!wals_bdev->read_thread_set) {
				wals_bdev->read_thread = wals_bdev->write_thread;
			}
			wals_bdev->write_thread_set = true;
		}
	} else {
		if (!wals_bdev->read_thread_set) {
			// TODO: call module to register read pollers
			wals_bdev->log_head_update_poller = SPDK_POLLER_REGISTER(wals_bdev_log_head_update, wals_bdev, 5);
			wals_bdev->cleaner_poller = SPDK_POLLER_REGISTER(wals_bdev_cleaner, wals_bdev, 10);
			wals_bdev->stat_poller = SPDK_POLLER_REGISTER(wals_bdev_stat_report, wals_bdev, 30*1000*1000);

			wals_bdev->read_thread = spdk_get_thread();
			wals_bdev->read_thread_set = true;
		}
	}

	wals_bdev->ch_count++;
	pthread_mutex_unlock(&wals_bdev->mutex);

	return 0;
}

static void
wals_bdev_unregister_write_pollers(void *arg)
{
	struct wals_bdev	*wals_bdev = arg;

	// TODO: call module to unregister
	spdk_poller_unregister(&wals_bdev->pending_writes_poller);
	
	wals_bdev->write_thread = NULL;
	wals_bdev->write_thread_set = false;
}

static void
wals_bdev_unregister_read_pollers(void *arg)
{
	struct wals_bdev	*wals_bdev = arg;

	// TODO: call module to unregister
	spdk_poller_unregister(&wals_bdev->cleaner_poller);
	spdk_poller_unregister(&wals_bdev->stat_poller);
	
	wals_bdev->read_thread = NULL;
	wals_bdev->read_thread_set = false;
}

/*
 * brief:
 * wals_bdev_destroy_cb function is a cb function for wals bdev which deletes the
 * hierarchy from wals bdev to base bdev io channels. It will be called per core
 * params:
 * io_device - pointer to wals bdev io device represented by wals_bdev
 * ctx_buf - pointer to context buffer for wals bdev io channel
 * returns:
 * none
 */
static void
wals_bdev_destroy_cb(void *io_device, void *ctx_buf)
{
	struct wals_bdev_io_channel *wals_ch = ctx_buf;
	struct wals_bdev            *wals_bdev = wals_ch->wals_bdev;

	SPDK_DEBUGLOG(bdev_wals, "wals_bdev_destroy_cb\n");

	assert(wals_ch != NULL);

	pthread_mutex_lock(&wals_bdev->mutex);
	wals_bdev->ch_count--;
	if (wals_bdev->ch_count == 0) {
		if (wals_bdev->write_thread != spdk_get_thread()) {
			spdk_thread_send_msg(wals_bdev->write_thread,
					     wals_bdev_unregister_write_pollers, wals_bdev);
		} else {
			wals_bdev_unregister_write_pollers(wals_bdev);
		}

		if (wals_bdev->read_thread != spdk_get_thread()) {
			spdk_thread_send_msg(wals_bdev->read_thread,
					     wals_bdev_unregister_read_pollers, wals_bdev);
		} else {
			wals_bdev_unregister_read_pollers(wals_bdev);
		}
	}
	pthread_mutex_unlock(&wals_bdev->mutex);
}

/*
 * brief:
 * wals_bdev_cleanup is used to cleanup and free wals_bdev related data
 * structures.
 * params:
 * wals_bdev - pointer to wals_bdev
 * returns:
 * none
 */
static void
wals_bdev_cleanup(struct wals_bdev *wals_bdev)
{
	SPDK_DEBUGLOG(bdev_wals, "wals_bdev_cleanup, %p name %s, state %u, config %p\n",
		      wals_bdev,
		      wals_bdev->bdev.name, wals_bdev->state, wals_bdev->config);
	if (wals_bdev->state == WALS_BDEV_STATE_CONFIGURING) {
		TAILQ_REMOVE(&g_wals_bdev_configuring_list, wals_bdev, state_link);
	} else if (wals_bdev->state == WALS_BDEV_STATE_OFFLINE) {
		TAILQ_REMOVE(&g_wals_bdev_offline_list, wals_bdev, state_link);
	} else {
		assert(0);
	}
	TAILQ_REMOVE(&g_wals_bdev_list, wals_bdev, global_link);
	free(wals_bdev->bdev.name);
	if (wals_bdev->config) {
		wals_bdev->config->wals_bdev = NULL;
	}
	free(wals_bdev);
}

/*
 * brief:
 * wals_bdev_destruct is the destruct function table pointer for wals bdev
 * params:
 * ctxt - pointer to wals_bdev
 * returns:
 * 0 - success
 * non zero - failure
 */
static int
wals_bdev_destruct(void *ctxt)
{
	struct wals_bdev *wals_bdev = ctxt;

	SPDK_DEBUGLOG(bdev_wals, "wals_bdev_destruct\n");

	wals_bdev->destruct_called = true;

	if (g_shutdown_started) {
		TAILQ_REMOVE(&g_wals_bdev_configured_list, wals_bdev, state_link);
		wals_bdev_stop(wals_bdev);
		wals_bdev->state = WALS_BDEV_STATE_OFFLINE;
		TAILQ_INSERT_TAIL(&g_wals_bdev_offline_list, wals_bdev, state_link);
	}

	spdk_io_device_unregister(wals_bdev, NULL);

	wals_bdev_cleanup(wals_bdev);

	return 0;
}

void _wals_bdev_io_complete(void *arg);

void
_wals_bdev_io_complete(void *arg)
{
	struct wals_bdev_io *wals_io = (struct wals_bdev_io *)arg;

	spdk_bdev_io_complete(wals_io->orig_io, wals_io->status);
}

void
wals_bdev_io_complete(struct wals_bdev_io *wals_io, enum spdk_bdev_io_status status)
{	
	wals_io->status = status;
	if (wals_io->orig_thread != spdk_get_thread()) {
		spdk_thread_send_msg(wals_io->orig_thread, _wals_bdev_io_complete, wals_io);
	} else {
		_wals_bdev_io_complete(wals_io);
	}
}

static wals_log_position
wals_bdev_get_targets_log_head_min2(struct wals_slice *slice)
{
	int i, min = 0, min_2 = 0;
	wals_log_position head[4];

	for (i = 0; i < NUM_TARGETS; i++) {
		head[i] = slice->targets[i]->head;
	}

	for (i = 1; i < NUM_TARGETS; i++) {
		if (head[i].round > head[min].round) {
			continue;
		}

		if (head[i].round < head[min].round) {
			min = i;
			continue;
		}

		if (head[i].offset < head[min].offset) {
			min = i;
		}
	}

	if (min == 0) {
		min_2 = 1;
	}

	for (i = 1; i < NUM_TARGETS; i++) {
		if (i == min) {
			continue;
		}
		
		if (head[i].round > head[min_2].round) {
			continue;
		}

		if (head[i].round < head[min_2].round) {
			min_2 = i;
			continue;
		}

		if (head[i].offset < head[min_2].offset) {
			min_2 = i;
		}
	}

	return head[min_2];
}

static bool
wals_bdev_is_valid_entry(struct wals_log_position after, struct bstat *bstat)
{
    if (bstat->type == LOCATION_BDEV) {
		if (bstat->round > after.round) {
			return true;
		}

		if (bstat->round < after.round) {
			return false;
		}

		if (bstat->l.bdevOffset >= after.offset) {
			return true;
		}

        return false;
    }

    return true;
}

static struct wals_read_after*
wals_bdev_insert_read_after(struct wals_log_position pos, struct wals_slice *slice) {
	struct wals_read_after *i, *read_after = calloc(1, sizeof(*read_after));
	read_after->pos = pos;

	if (LIST_EMPTY(&slice->outstanding_read_afters)) {
		LIST_INSERT_HEAD(&slice->outstanding_read_afters, read_after, entries);

		return read_after;
	}

	i = LIST_FIRST(&slice->outstanding_read_afters);
	while (i != NULL) {
		if (i->pos.round > pos.round) {
			break;
		}

		if (i->pos.round < pos.round) {
			i = LIST_NEXT(i, entries);
			continue;
		}

		if (i->pos.offset > pos.offset) {
			break;
		}

		i = LIST_NEXT(i, entries);
	}

	LIST_INSERT_BEFORE(i, read_after, entries);

	return read_after;
}

void
wals_target_read_complete(struct wals_bdev_io *wals_io, bool success)
{
	struct spdk_bdev_io *orig_io = wals_io->orig_io;
	int i;
	void *copy = wals_io->read_buf;
	struct wals_slice *slice;

	wals_io->remaining_read_requests--;
	wals_io->status = success && wals_io->status >= 0
					? SPDK_BDEV_IO_STATUS_SUCCESS
					: SPDK_BDEV_IO_STATUS_FAILED;

	if (!success) {
		SPDK_ERRLOG("Error reading data from target.\n");
	}

	if (wals_io->remaining_read_requests == 0) {
		if (wals_io->status == SPDK_BDEV_IO_STATUS_SUCCESS) {
			for (i = 0; i < orig_io->u.bdev.iovcnt; i++) {
				memcpy(orig_io->u.bdev.iovs[i].iov_base, copy, (size_t)orig_io->u.bdev.iovs[i].iov_len);
				copy += orig_io->u.bdev.iovs[i].iov_len;
			}
			// TODO: add to index
		}

		slice = &wals_io->wals_bdev->slices[wals_io->slice_index];

		if (wals_io->read_after == LIST_FIRST(&slice->outstanding_read_afters)) {
			wals_io->wals_bdev->slices[wals_io->slice_index].head = wals_io->read_after->pos;
		}

		LIST_REMOVE(wals_io->read_after, entries);
		free(wals_io->read_after);

		spdk_free(wals_io->read_buf);

		wals_bdev_io_complete(wals_io, wals_io->status);
	}
}

/*
 * brief:
 * wals_bdev_submit_read_request function submits read requests
 * to the first disk; it will submit as many as possible unless a read fails with -ENOMEM, in
 * which case it will queue it for later submission
 * params:
 * wals_io
 * returns:
 * none
 */
static void
wals_bdev_submit_read_request(struct wals_bdev_io *wals_io)
{
	struct spdk_bdev_io		*bdev_io = spdk_bdev_io_from_ctx(wals_io);
	struct wals_bdev		*wals_bdev = wals_io->wals_bdev;
	struct wals_slice		*slice;
	int						ret, target_index;
	wals_log_position		valid_pos;
	struct bskiplistNode	*bn;
    uint64_t    			read_begin, read_end, read_cur, tmp;

	SPDK_NOTICELOG("submit read: %ld+%ld\n", bdev_io->u.bdev.offset_blocks, bdev_io->u.bdev.num_blocks);

	wals_io->slice_index = bdev_io->u.bdev.offset_blocks / wals_bdev->slice_blockcnt;
	slice = &wals_bdev->slices[wals_io->slice_index];
	wals_io->read_buf = spdk_zmalloc(bdev_io->u.bdev.num_blocks * wals_bdev->bdev.blocklen, 0, NULL, SPDK_ENV_LCORE_ID_ANY, SPDK_MALLOC_DMA);

	valid_pos = wals_bdev_get_targets_log_head_min2(slice);
	SPDK_NOTICELOG("valid pos: %ld(%ld)\n", valid_pos.offset, valid_pos.round);
	wals_io->read_after = wals_bdev_insert_read_after(valid_pos, slice);

	read_begin = bdev_io->u.bdev.offset_blocks;
    read_end = bdev_io->u.bdev.offset_blocks + bdev_io->u.bdev.num_blocks - 1;

	bn = bslFirstNodeAfterBegin(wals_bdev->bsl, read_begin);
	while (bn && !wals_bdev_is_valid_entry(valid_pos, bn->ele)) {
		bn = bn->level[0].forward;
	}

	if (bn && bn->begin <= read_begin && bn->end >= read_end) {
		SPDK_NOTICELOG("one entry in log\n");
		// One entry in someone's log
		wals_io->remaining_read_requests = 1;
		// TODO: pick the best choice
		target_index = 0;
		ret = wals_bdev->module->submit_log_read_request(slice->targets[target_index], wals_io->read_buf, 
														bn->ele->l.bdevOffset + read_begin - bn->ele->begin,
														bdev_io->u.bdev.num_blocks, wals_io);
		if (spdk_unlikely(ret != 0)) {
			SPDK_ERRLOG("submit log read request failed to target %d in slice %ld\n", target_index, wals_io->slice_index);
			wals_target_read_complete(wals_io, SPDK_BDEV_IO_STATUS_FAILED);
		}
		return;
	}

	if (!bn || bn->begin > read_end) {
		SPDK_NOTICELOG("one entry in core\n");
		// not found in index
		wals_io->remaining_read_requests = 1;
		// TODO: round-robin?
		target_index = 0;
		ret = wals_bdev->module->submit_core_read_request(slice->targets[target_index], wals_io->read_buf, 
														bdev_io->u.bdev.offset_blocks, bdev_io->u.bdev.num_blocks, wals_io);
		if (spdk_unlikely(ret != 0)) {
			SPDK_ERRLOG("submit core read request failed to target %d in slice %ld\n", target_index, wals_io->slice_index);
			wals_bdev_io_complete(wals_io, SPDK_BDEV_IO_STATUS_FAILED);
		}
		return;
	}

	SPDK_NOTICELOG("merged read\n");
	/*
	 * Completion in read submit request leads to complete the whole read request every time.
	 * Add the initial remaining read requests by 1 to avoid this.
	 * The remaining read request will not be incremented on the last submission so that the whole request could be completed.
	 */
	wals_io->remaining_read_requests = 1;
	read_cur = read_begin;
	// TODO: round-robin?
	target_index = 0;

	while (read_cur <= read_end) {
		while (bn && !wals_bdev_is_valid_entry(valid_pos, bn->ele)) {
			bn = bn->level[0].forward;
		}

		if (!bn || read_cur < bn->begin) {
			if (!bn) {
				tmp = read_end;
			} else {
				tmp = bn->begin - 1 > read_end ? read_end : bn->begin - 1;
			}

			if (tmp != read_end) {
				wals_io->remaining_read_requests++;
			}
			ret = wals_bdev->module->submit_core_read_request(slice->targets[target_index], wals_io->read_buf + (read_cur - read_begin) * wals_bdev->bdev.blocklen, 
															read_cur, tmp - read_cur + 1, wals_io);
			if (spdk_unlikely(ret != 0)) {
				SPDK_ERRLOG("submit core read request failed to target %d in slice %ld\n", target_index, wals_io->slice_index);
				wals_bdev_io_complete(wals_io, SPDK_BDEV_IO_STATUS_FAILED);
			}
			read_cur = tmp + 1;
			continue;
		}

		if (bn && read_cur >= bn->begin) {
			tmp = bn->end > read_end ? read_end : bn->end;

			if (tmp != read_end) {
				wals_io->remaining_read_requests++;
			}
			ret = wals_bdev->module->submit_log_read_request(slice->targets[target_index], wals_io->read_buf + (read_cur - read_begin) * wals_bdev->bdev.blocklen, 
															bn->ele->l.bdevOffset + read_cur - bn->ele->begin, tmp - read_cur + 1, wals_io);
			if (spdk_unlikely(ret != 0)) {
				SPDK_ERRLOG("submit log read request failed to target %d in slice %ld\n", target_index, wals_io->slice_index);
				wals_bdev_io_complete(wals_io, SPDK_BDEV_IO_STATUS_FAILED);
			}
			read_cur = tmp + 1;
			bn = bn->level[0].forward;
			continue;
		}
	}
}

static void
wals_bdev_insert_read_index(void *arg)
{
	struct wals_index_msg *msg = arg;
	struct bstat *bstat = bstatBdevCreate(msg->begin, msg->end, msg->round, msg->offset, msg->wals_bdev->bstat_pool);
	
	bslInsert(msg->wals_bdev->bsl, msg->begin, msg->end, bstat, msg->wals_bdev->bslfn);
}

static void
wals_bdev_write_complete_quorum(void *arg)
{
	struct wals_bdev_io *wals_io = arg;
	struct wals_bdev	*wals_bdev = wals_io->wals_bdev;
	struct wals_metadata *metadata = wals_io->metadata;
	struct wals_index_msg *msg = spdk_mempool_get(wals_bdev->index_msg_pool);
	
	msg->begin = metadata->core_offset;
	msg->end = metadata->core_offset + metadata->length - 1;
	msg->offset = metadata->next_offset - metadata->length;
	msg->round = metadata->round;
	msg->wals_bdev = wals_bdev;
	spdk_thread_send_msg(wals_bdev->read_thread, wals_bdev_insert_read_index, msg);

	wals_io->orig_io->free_deferred = true;
	spdk_bdev_io_complete(wals_io->orig_io, SPDK_BDEV_IO_STATUS_SUCCESS);
}

static void
wals_bdev_write_complete_all(void* arg)
{
	struct wals_bdev_io *wals_io = arg;
	wals_io->orig_io->free_deferred = false;
	if (wals_io->orig_io->free_called) {
		spdk_bdev_free_io(wals_io->orig_io);
	}
	wals_io->orig_io->free_called = false;
}

void
wals_target_write_complete(struct wals_bdev_io *wals_io, bool success)
{
	wals_io->targets_completed++;

	if (spdk_unlikely(!success)) {
		wals_io->targets_failed++;

		if (wals_io->targets_failed > NUM_TARGETS - QUORUM_TARGETS) {
			SPDK_ERRLOG("Write failure to quorum targets on slice %ld.\n", wals_io->slice_index);
			wals_bdev_io_complete(wals_io, SPDK_BDEV_IO_STATUS_FAILED);
		}
	}

	if (wals_io->targets_completed - wals_io->targets_failed == QUORUM_TARGETS) {
		if (wals_io->orig_thread != spdk_get_thread()) {
			spdk_thread_send_msg(wals_io->orig_thread, wals_bdev_write_complete_quorum, wals_io);
		} else {
			wals_bdev_write_complete_quorum(wals_io);
		}
	}

	if (wals_io->targets_completed == NUM_TARGETS) {
		if (wals_io->orig_thread != spdk_get_thread()) {
			spdk_thread_send_msg(wals_io->orig_thread, wals_bdev_write_complete_all, wals_io);
		} else {
			wals_bdev_write_complete_all(wals_io);
		}
	}
}

/*
 * Updates the tail
 * return true if there's enough space. Otherwise, return false.
 */
static bool
wals_bdev_update_tail(uint64_t size_to_put, wals_log_position tail, uint64_t max,
					wals_log_position head, wals_log_position *new)
{
	uint64_t next = tail.offset + size_to_put;
	*new = tail;
	if (next > max) {
		if (tail.round > head.round || size_to_put > head.offset) {
			return false;
		} else {
			new->offset = size_to_put;
			new->round = tail.round + 1;
		}
	} else if (tail.round > head.round && next > head.offset) {
		return false;
	} else {
		new->offset = next;
	}

	return true;
}

static void
_wals_bdev_submit_write_request(struct wals_bdev_io *wals_io, wals_log_position slice_tail, wals_log_position buffer_tail)
{
	struct spdk_bdev_io		*bdev_io = spdk_bdev_io_from_ctx(wals_io);
	struct wals_bdev		*wals_bdev = wals_io->wals_bdev;
	struct wals_slice		*slice = wals_bdev->slices[wals_io->slice_index];
	
	int						ret, i;
	struct wals_metadata	*metadata;
	void					*ptr, *data;
	struct iovec			*iovs;

	ptr = wals_bdev->buffer + wals_bdev->buffer_tail.offset * wals_bdev->buffer_blocklen;
	memset(ptr, 0, wals_bdev->buffer_blocklen);
	metadata = (struct wals_metadata *) ptr;
	metadata->version = METADATA_VERSION; // TODO: add CRC
	metadata->seq = ++slice->seq;
	metadata->core_offset = bdev_io->u.bdev.offset_blocks;
	metadata->next_offset = slice_tail.offset;
	metadata->length = bdev_io->u.bdev.num_blocks;
	metadata->round = slice->tail.round;

	wals_io->metadata = metadata;

	// memcpy data
	data = wals_bdev->buffer + (wals_bdev->buffer_tail.offset + METADATA_BLOCKS) * wals_bdev->buffer_blocklen;
	iovs = bdev_io->u.bdev.iovs;

	for (i = 0; i < bdev_io->u.bdev.iovcnt; i++) {
		memcpy(data, iovs[i].iov_base, iovs[i].iov_len);
		data += iovs[i].iov_len;
	}

	// call module to submit to all targets
	wals_io->targets_failed = 0;
	wals_io->targets_completed = 0;
	for (i = 0; i < NUM_TARGETS; i++) {
		ret = wals_bdev->module->submit_log_write_request(slice->targets[i], ptr, slice->tail.offset, bdev_io->u.bdev.num_blocks + METADATA_BLOCKS, wals_io);
		if (spdk_unlikely(ret != 0)) {
			wals_io->targets_failed++;
			SPDK_ERRLOG("io submit error due to %d for target %d on slice %ld.\n", ret, i, wals_io->slice_index);
		}
	}
	
	// update tails
	slice->tail = slice_tail;
	wals_bdev->buffer_tail = buffer_tail;
	SPDK_NOTICELOG("slice tail updated: %ld(%ld)\n", slice->tail.offset, slice->tail.round);
	SPDK_NOTICELOG("buffer tail updated: %ld(%ld)\n", wals_bdev->buffer_tail.offset, wals_bdev->buffer_tail.round);

	if (spdk_unlikely(wals_io->targets_failed > NUM_TARGETS - QUORUM_TARGETS)) {
		SPDK_ERRLOG("IO submit failure to quorum targets on slice %ld.\n", wals_io->slice_index);
		wals_bdev_io_complete(wals_io, SPDK_BDEV_IO_STATUS_FAILED);
	}
}

/*
 * brief:
 * wals_bdev_submit_write_request function submits write requests
 * to member disks; it will submit as many as possible unless a write fails with -ENOMEM, in
 * which case it will queue it for later submission
 * params:
 * wals_io
 * returns:
 * none
 */
static void
wals_bdev_submit_write_request(void *arg)
{
	struct wals_bdev_io 	*wals_io = arg;
	struct spdk_bdev_io		*bdev_io = spdk_bdev_io_from_ctx(wals_io);
	struct wals_bdev		*wals_bdev = wals_io->wals_bdev;

	struct wals_slice		*slice;
	wals_log_position		slice_tail, buffer_tail;

	SPDK_NOTICELOG("submit write: %ld+%ld\n", bdev_io->u.bdev.offset_blocks, bdev_io->u.bdev.num_blocks);

	wals_io->slice_index = bdev_io->u.bdev.offset_blocks / wals_bdev->slice_blockcnt;
	slice = &wals_bdev->slices[wals_io->slice_index];

	// check slice space
	if (!wals_bdev_update_tail(bdev_io->u.bdev.num_blocks + METADATA_BLOCKS,
								slice->tail, slice->log_blockcnt, slice->head, &slice_tail)) {
		SPDK_DEBUGLOG(bdev_wals, "queue bdev io submit due to no enough space left on slice log.\n");
		TAILQ_INSERT_TAIL(&slice->pending_writes, wals_io, tailq);
	}

	// check buffer space
	if (!wals_bdev_update_tail(bdev_io->u.bdev.num_blocks + METADATA_BLOCKS, 
								wals_bdev->buffer_tail, wals_bdev->buffer_blockcnt, wals_bdev->buffer_head, &buffer_tail)) {
		SPDK_DEBUGLOG(bdev_wals, "queue bdev io submit due to no enough space left on buffer.\n");
		TAILQ_INSERT_TAIL(&slice->pending_writes, wals_io, tailq);
	}

	_wals_bdev_submit_write_request(wals_io, slice_tail, buffer_tail);
}

// TODO - end

/*
 * brief:
 * Callback function to spdk_bdev_io_get_buf.
 * params:
 * ch - pointer to wals bdev io channel
 * bdev_io - pointer to parent bdev_io on wals bdev device
 * success - True if buffer is allocated or false otherwise.
 * returns:
 * none
 */
static void
wals_bdev_get_buf_cb(struct spdk_io_channel *ch, struct spdk_bdev_io *bdev_io,
		     bool success)
{
	struct wals_bdev_io *wals_io = (struct wals_bdev_io *)bdev_io->driver_ctx;

	if (!success) {
		wals_bdev_io_complete(wals_io, SPDK_BDEV_IO_STATUS_FAILED);
		return;
	}

	wals_bdev_submit_read_request(wals_io);
}

static void
wals_bdev_io_get_buf(void *arg)
{
	struct spdk_bdev_io *bdev_io = arg;
	spdk_bdev_io_get_buf(bdev_io, wals_bdev_get_buf_cb,
				     bdev_io->u.bdev.num_blocks * bdev_io->bdev->blocklen);
}

/*
 * brief:
 * wals_bdev_submit_request function is the submit_request function pointer of
 * wals bdev function table. This is used to submit the io on wals_bdev to below
 * layers.
 * params:
 * ch - pointer to wals bdev io channel
 * bdev_io - pointer to parent bdev_io on wals bdev device
 * returns:
 * none
 */
static void
wals_bdev_submit_request(struct spdk_io_channel *ch, struct spdk_bdev_io *bdev_io)
{
	struct wals_bdev_io *wals_io = (struct wals_bdev_io *)bdev_io->driver_ctx;

	wals_io->wals_ch = spdk_io_channel_get_ctx(ch);
	wals_io->wals_bdev = wals_io->wals_ch->wals_bdev;
	wals_io->orig_io = bdev_io;
	wals_io->orig_thread = spdk_get_thread();
	wals_io->status = SPDK_BDEV_IO_STATUS_SUCCESS;

	switch (bdev_io->type) {
	case SPDK_BDEV_IO_TYPE_READ:
		if (wals_io->orig_thread != wals_io->wals_bdev->read_thread) {
			spdk_thread_send_msg(wals_io->wals_bdev->read_thread, wals_bdev_io_get_buf, bdev_io);
		} else {
			wals_bdev_io_get_buf(bdev_io);
		}
		break;
	case SPDK_BDEV_IO_TYPE_WRITE:
		if (wals_io->orig_thread != wals_io->wals_bdev->write_thread) {
			spdk_thread_send_msg(wals_io->wals_bdev->write_thread, wals_bdev_submit_write_request, wals_io);
		} else {
			wals_bdev_submit_write_request(wals_io);
		}
		break;
	case SPDK_BDEV_IO_TYPE_RESET:
	case SPDK_BDEV_IO_TYPE_FLUSH:
	case SPDK_BDEV_IO_TYPE_UNMAP:
	default:
		SPDK_ERRLOG("submit request, invalid io type %u\n", bdev_io->type);
		wals_bdev_io_complete(wals_io, SPDK_BDEV_IO_STATUS_FAILED);
		break;
	}
}

/*
 * brief:
 * wals_bdev_io_type_supported is the io_supported function for bdev function
 * table which returns whether the particular io type is supported or not by
 * wals bdev module
 * params:
 * ctx - pointer to wals bdev context
 * type - io type
 * returns:
 * true - io_type is supported
 * false - io_type is not supported
 */
static bool
wals_bdev_io_type_supported(void *ctx, enum spdk_bdev_io_type io_type)
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
 * wals_bdev_get_io_channel is the get_io_channel function table pointer for
 * wals bdev. This is used to return the io channel for this wals bdev
 * params:
 * ctxt - pointer to wals_bdev
 * returns:
 * pointer to io channel for wals bdev
 */
static struct spdk_io_channel *
wals_bdev_get_io_channel(void *ctxt)
{
	struct wals_bdev *wals_bdev = ctxt;

	return spdk_get_io_channel(wals_bdev);
}

/*
 * brief:
 * wals_bdev_dump_info_json is the function table pointer for wals bdev
 * params:
 * ctx - pointer to wals_bdev
 * w - pointer to json context
 * returns:
 * 0 - success
 * non zero - failure
 */
static int
wals_bdev_dump_info_json(void *ctx, struct spdk_json_write_ctx *w)
{
	struct wals_bdev *wals_bdev = ctx;

	SPDK_DEBUGLOG(bdev_wals, "wals_bdev_dump_config_json\n");
	assert(wals_bdev != NULL);

	/* Dump the wals bdev configuration related information */
	spdk_json_write_named_object_begin(w, "wals");
	spdk_json_write_named_uint32(w, "state", wals_bdev->state);
	spdk_json_write_named_uint32(w, "destruct_called", wals_bdev->destruct_called);

	// TODO: add dump info
	spdk_json_write_object_end(w);

	return 0;
}

/*
 * brief:
 * wals_bdev_write_config_json is the function table pointer for wals bdev
 * params:
 * bdev - pointer to spdk_bdev
 * w - pointer to json context
 * returns:
 * none
 */
static void
wals_bdev_write_config_json(struct spdk_bdev *bdev, struct spdk_json_write_ctx *w)
{
	struct wals_bdev *wals_bdev = bdev->ctxt;

	spdk_json_write_object_begin(w);

	spdk_json_write_named_string(w, "method", "bdev_wals_create");

	spdk_json_write_named_object_begin(w, "params");
	spdk_json_write_named_string(w, "name", bdev->name);

	// TODO: add config info
	spdk_json_write_object_end(w);

	spdk_json_write_object_end(w);
}

/* g_wals_bdev_fn_table is the function table for wals bdev */
static const struct spdk_bdev_fn_table g_wals_bdev_fn_table = {
	.destruct		= wals_bdev_destruct,
	.submit_request		= wals_bdev_submit_request,
	.io_type_supported	= wals_bdev_io_type_supported,
	.get_io_channel		= wals_bdev_get_io_channel,
	.dump_info_json		= wals_bdev_dump_info_json,
	.write_config_json	= wals_bdev_write_config_json,
};

/*
 * brief:
 * wals_bdev_config_cleanup function is used to free memory for one wals_bdev in configuration
 * params:
 * wals_cfg - pointer to wals_bdev_config structure
 * returns:
 * none
 */
void
wals_bdev_config_cleanup(struct wals_bdev_config *wals_cfg)
{
	TAILQ_REMOVE(&g_wals_config.wals_bdev_config_head, wals_cfg, link);
	g_wals_config.total_wals_bdev--;

	// TODO: free
	free(wals_cfg->name);
	free(wals_cfg);
}

/*
 * brief:
 * wals_bdev_free is the wals bdev function table function pointer. This is
 * called on bdev free path
 * params:
 * none
 * returns:
 * none
 */
static void
wals_bdev_free(void)
{
	struct wals_bdev_config *wals_cfg, *tmp;

	SPDK_DEBUGLOG(bdev_wals, "wals_bdev_free\n");
	TAILQ_FOREACH_SAFE(wals_cfg, &g_wals_config.wals_bdev_config_head, link, tmp) {
		wals_bdev_config_cleanup(wals_cfg);
	}
}

/* brief
 * wals_bdev_config_find_by_name is a helper function to find wals bdev config
 * by name as key.
 *
 * params:
 * wals_name - name for wals bdev.
 */
struct wals_bdev_config *
wals_bdev_config_find_by_name(const char *wals_name)
{
	struct wals_bdev_config *wals_cfg;

	TAILQ_FOREACH(wals_cfg, &g_wals_config.wals_bdev_config_head, link) {
		if (!strcmp(wals_cfg->name, wals_name)) {
			return wals_cfg;
		}
	}

	return wals_cfg;
}

/*
 * brief
 * wals_bdev_config_add function adds config for newly created wals bdev.
 *
 * params:
 * wals_name - name for wals bdev.
 * _wals_cfg - Pointer to newly added configuration
 */
int
wals_bdev_config_add(const char *wals_name, const char *module_name,
			struct rpc_bdev_wals_slice *slices, uint64_t slicecnt,
			uint64_t blocklen, uint64_t slice_blockcnt, uint64_t buffer_blockcnt,
			struct wals_bdev_config **_wals_cfg)
{
	struct wals_bdev_config *wals_cfg;
	uint64_t	i, j;

	wals_cfg = wals_bdev_config_find_by_name(wals_name);
	if (wals_cfg != NULL) {
		SPDK_ERRLOG("Duplicate wals bdev name found in config file %s\n",
			    wals_name);
		return -EEXIST;
	}

	wals_cfg = calloc(1, sizeof(*wals_cfg));
	if (wals_cfg == NULL) {
		SPDK_ERRLOG("unable to allocate memory\n");
		return -ENOMEM;
	}

	wals_cfg->name = strdup(wals_name);
	if (!wals_cfg->name) {
		free(wals_cfg);
		SPDK_ERRLOG("unable to allocate memory\n");
		return -ENOMEM;
	}

	wals_cfg->module_name = strdup(module_name);
	if (!wals_cfg->module_name) {
		free(wals_cfg->name);
		free(wals_cfg);
		SPDK_ERRLOG("unable to allocate memory\n");
		return -ENOMEM;
	}
	
	wals_cfg->blocklen = blocklen;
	wals_cfg->slice_blockcnt = slice_blockcnt;
	wals_cfg->buffer_blockcnt = buffer_blockcnt;

	wals_cfg->slicecnt = slicecnt;
	wals_cfg->slices = calloc(slicecnt, sizeof(struct wals_slice_config));
	if (!wals_cfg->slices) {
		free(wals_cfg->module_name);
		free(wals_cfg->name);
		free(wals_cfg);
		SPDK_ERRLOG("unable to allocate memory\n");
		return -ENOMEM;
	}

	for (i = 0; i < slicecnt; i++) {
		for (j = 0; j < NUM_TARGETS; j++) {
			wals_cfg->slices[i].targets[j].target_log_info.nqn = strdup(slices[i].targets[j].log.nqn);
			wals_cfg->slices[i].targets[j].target_log_info.address = strdup(slices[i].targets[j].log.address);
			wals_cfg->slices[i].targets[j].target_log_info.port = slices[i].targets[j].log.port;
			wals_cfg->slices[i].targets[j].target_core_info.nqn = strdup(slices[i].targets[j].core.nqn);
			wals_cfg->slices[i].targets[j].target_core_info.address = strdup(slices[i].targets[j].core.address);
			wals_cfg->slices[i].targets[j].target_core_info.port = slices[i].targets[j].core.port;
		}
	}

	TAILQ_INSERT_TAIL(&g_wals_config.wals_bdev_config_head, wals_cfg, link);
	g_wals_config.total_wals_bdev++;

	*_wals_cfg = wals_cfg;
	return 0;
}

/*
 * brief:
 * wals_bdev_fini_start is called when bdev layer is starting the
 * shutdown process
 * params:
 * none
 * returns:
 * none
 */
static void
wals_bdev_fini_start(void)
{
	SPDK_DEBUGLOG(bdev_wals, "wals_bdev_fini_start\n");
	g_shutdown_started = true;
}

/*
 * brief:
 * wals_bdev_exit is called on wals bdev module exit time by bdev layer
 * params:
 * none
 * returns:
 * none
 */
static void
wals_bdev_exit(void)
{
	SPDK_DEBUGLOG(bdev_wals, "wals_bdev_exit\n");
	wals_bdev_free();
}

/*
 * brief:
 * wals_bdev_get_ctx_size is used to return the context size of bdev_io for wals
 * module
 * params:
 * none
 * returns:
 * size of spdk_bdev_io context for wals
 */
static int
wals_bdev_get_ctx_size(void)
{
	SPDK_DEBUGLOG(bdev_wals, "wals_bdev_get_ctx_size\n");
	return sizeof(struct wals_bdev_io);
}


static struct spdk_bdev_module g_wals_if = {
	.name = "wals",
	.module_init = wals_bdev_init,
	.fini_start = wals_bdev_fini_start,
	.module_fini = wals_bdev_exit,
	.get_ctx_size = wals_bdev_get_ctx_size,
	.examine_config = wals_bdev_examine,
	.async_init = false,
	.async_fini = false,
};
SPDK_BDEV_MODULE_REGISTER(wals, &g_wals_if)

/*
 * brief:
 * wals_bdev_init is the initialization function for wals bdev module
 * params:
 * none
 * returns:
 * 0 - success
 * non zero - failure
 */
static int
wals_bdev_init(void)
{
	return 0;
}

/*
 * brief:
 * wals_bdev_create allocates wals bdev based on passed configuration
 * params:
 * wals_cfg - configuration of wals bdev
 * returns:
 * 0 - success
 * non zero - failure
 */
int
wals_bdev_create(struct wals_bdev_config *wals_cfg)
{
	struct wals_bdev *wals_bdev;
	struct spdk_bdev *wals_bdev_gen;

	wals_bdev = calloc(1, sizeof(*wals_bdev));
	if (!wals_bdev) {
		SPDK_ERRLOG("Unable to allocate memory for wals bdev\n");
		return -ENOMEM;
	}

	wals_bdev->slicecnt = wals_cfg->slicecnt;
	wals_bdev->slices = calloc(wals_bdev->slicecnt, sizeof(struct wals_slice));

	wals_bdev->state = WALS_BDEV_STATE_CONFIGURING;
	wals_bdev->config = wals_cfg;

	wals_bdev_gen = &wals_bdev->bdev;

	wals_bdev_gen->name = strdup(wals_cfg->name);
	if (!wals_bdev_gen->name) {
		SPDK_ERRLOG("Unable to allocate name for wals\n");
		free(wals_bdev);
		return -ENOMEM;
	}

	wals_bdev_gen->product_name = "WALS Volume";
	wals_bdev_gen->ctxt = wals_bdev;
	wals_bdev_gen->fn_table = &g_wals_bdev_fn_table;
	wals_bdev_gen->module = &g_wals_if;
	wals_bdev_gen->write_cache = 0;

	TAILQ_INSERT_TAIL(&g_wals_bdev_configuring_list, wals_bdev, state_link);
	TAILQ_INSERT_TAIL(&g_wals_bdev_list, wals_bdev, global_link);

	wals_cfg->wals_bdev = wals_bdev;

	return 0;
}

/*
 * brief:
 * If wals bdev config is complete, then only register the wals bdev to
 * bdev layer and remove this wals bdev from configuring list and
 * insert the wals bdev to configured list
 * params:
 * wals_bdev - pointer to wals bdev
 * returns:
 * 0 - success
 * non zero - failure
 */
static int
wals_bdev_configure(struct wals_bdev *wals_bdev)
{
	struct spdk_bdev *wals_bdev_gen;
	int rc = 0;

	SPDK_NOTICELOG("Configure wals bdev %s.\n", wals_bdev->bdev.name);

	assert(wals_bdev->state == WALS_BDEV_STATE_CONFIGURING);

	wals_bdev->blocklen_shift = 0; // TODO: set when log and core have different blocklen
	wals_bdev_gen = &wals_bdev->bdev;

	wals_bdev->state = WALS_BDEV_STATE_ONLINE;
	
	pthread_mutex_init(&wals_bdev->mutex, NULL);

	SPDK_DEBUGLOG(bdev_wals, "io device register %p\n", wals_bdev);
	SPDK_DEBUGLOG(bdev_wals, "blockcnt %" PRIu64 ", blocklen %u\n",
		      wals_bdev_gen->blockcnt, wals_bdev_gen->blocklen);
	spdk_io_device_register(wals_bdev, wals_bdev_create_cb, wals_bdev_destroy_cb,
				sizeof(struct wals_bdev_io_channel),
				wals_bdev->bdev.name);
	rc = spdk_bdev_register(wals_bdev_gen);
	if (rc != 0) {
		SPDK_ERRLOG("Unable to register wals bdev and stay at configuring state\n");
		wals_bdev_stop(wals_bdev);
		spdk_io_device_unregister(wals_bdev, NULL);
		wals_bdev->state = WALS_BDEV_STATE_CONFIGURING;
		return rc;
	}
	SPDK_DEBUGLOG(bdev_wals, "wals bdev generic %p\n", wals_bdev_gen);
	TAILQ_REMOVE(&g_wals_bdev_configuring_list, wals_bdev, state_link);
	TAILQ_INSERT_TAIL(&g_wals_bdev_configured_list, wals_bdev, state_link);
	SPDK_DEBUGLOG(bdev_wals, "wals bdev is created with name %s, wals_bdev %p\n",
		      wals_bdev_gen->name, wals_bdev);

	return 0;
}

/*
 * brief:
 * Remove base bdevs from the wals bdev one by one.  Skip any base bdev which
 *  doesn't exist.
 * params:
 * wals_cfg - pointer to wals bdev config.
 * cb_fn - callback function
 * cb_ctx - argument to callback function
 */
void
wals_bdev_remove_base_devices(struct wals_bdev_config *wals_cfg,
			      wals_bdev_destruct_cb cb_fn, void *cb_arg)
{
	struct wals_bdev		*wals_bdev;

	SPDK_DEBUGLOG(bdev_wals, "wals_bdev_remove_base_devices\n");

	wals_bdev = wals_cfg->wals_bdev;
	if (wals_bdev == NULL) {
		SPDK_DEBUGLOG(bdev_wals, "wals bdev %s doesn't exist now\n", wals_cfg->name);
		if (cb_fn) {
			cb_fn(cb_arg, 0);
		}
		return;
	}

	if (wals_bdev->destroy_started) {
		SPDK_DEBUGLOG(bdev_wals, "destroying wals bdev %s is already started\n",
			      wals_cfg->name);
		if (cb_fn) {
			cb_fn(cb_arg, -EALREADY);
		}
		return;
	}

	wals_bdev->destroy_started = true;

	// TODO: call module to remove

	wals_bdev_cleanup(wals_bdev);
	if (cb_fn) {
		cb_fn(cb_arg, 0);
	}
	return;
}

int
wals_bdev_start_all(struct wals_bdev_config *wals_cfg)
{
	struct wals_bdev	*wals_bdev;
	uint64_t	i, j;
	int			rc;

	wals_bdev = wals_cfg->wals_bdev;
	if (!wals_bdev) {
		SPDK_ERRLOG("WALS bdev '%s' is not created yet\n", wals_cfg->name);
		return -ENODEV;
	}

	wals_bdev->module = wals_bdev_target_module_find(wals_cfg->module_name);
	if (wals_bdev->module == NULL) {
		SPDK_ERRLOG("WALS target module '%s' not found", wals_cfg->module_name);
		return -EINVAL;
	}

	wals_bdev->bdev.blocklen = wals_cfg->blocklen;
	wals_bdev->bdev.blockcnt = wals_cfg->slice_blockcnt * wals_cfg->slicecnt;
	wals_bdev->bdev.optimal_io_boundary = wals_cfg->slice_blockcnt;
	wals_bdev->bdev.split_on_optimal_io_boundary = true;
	wals_bdev->slice_blockcnt = wals_cfg->slice_blockcnt;
	wals_bdev->buffer_blocklen = wals_cfg->blocklen;
	wals_bdev->buffer_blockcnt = wals_cfg->buffer_blockcnt;

	for (i = 0; i < wals_cfg->slicecnt; i++) {
		wals_bdev->slices[i].log_blockcnt = UINT64_MAX;
		for (j = 0; j < NUM_TARGETS; j++) {
			wals_bdev->slices[i].targets[j] = wals_bdev->module->start(&wals_cfg->slices[i].targets[j], wals_bdev, &wals_bdev->slices[i]);
			if (wals_bdev->slices[i].targets[j] == NULL) {
				SPDK_ERRLOG("Failed to start target '%ld' in slice '%ld'.", j, i);
				return -EFAULT;
			}
			if (wals_bdev->slices[i].targets[j]->log_blockcnt < wals_bdev->slices[i].log_blockcnt) {
				wals_bdev->slices[i].log_blockcnt = wals_bdev->slices[i].targets[j]->log_blockcnt;
			}
		}
	}

	rc = wals_bdev_start(wals_bdev);
	if (rc) {
		SPDK_ERRLOG("Failed to start WALS bdev '%s'.\n", wals_cfg->name);
		return rc;
	}

	rc = wals_bdev_configure(wals_bdev);
	if (rc != 0) {
		SPDK_ERRLOG("Failed to configure WALS bdev\n");
		return rc;
	}
	
	return 0;
}

static int
wals_bdev_start(struct wals_bdev *wals_bdev)
{
	uint64_t mempool_size;
	uint64_t i;

	mempool_size = 1024;
	mempool_size = spdk_align64pow2(mempool_size);

	wals_bdev->bstat_pool = spdk_mempool_create("WALS_BSTAT_POOL", mempool_size, sizeof(bstat), SPDK_MEMPOOL_DEFAULT_CACHE_SIZE, SPDK_ENV_SOCKET_ID_ANY);
	wals_bdev->bsl_node_pool = spdk_mempool_create("WALS_BSL_NODE_POOL", mempool_size, sizeof(bskiplistNode), SPDK_MEMPOOL_DEFAULT_CACHE_SIZE, SPDK_ENV_SOCKET_ID_ANY);
	wals_bdev->index_msg_pool = spdk_mempool_create("WALS_INDEX_MSG_POOL", 256, sizeof(struct wals_index_msg), SPDK_MEMPOOL_DEFAULT_CACHE_SIZE, SPDK_ENV_SOCKET_ID_ANY);

	wals_bdev->bsl = bslCreate(wals_bdev->bsl_node_pool, wals_bdev->bstat_pool);
	wals_bdev->bslfn = bslfnCreate(wals_bdev->bsl_node_pool, wals_bdev->bstat_pool);

	for (i = 0; i < wals_bdev->slicecnt; i++) {
		TAILQ_INIT(&wals_bdev->slices[i].pending_writes);
		LIST_INIT(&wals_bdev->slices[i].outstanding_read_afters);
	}

	wals_bdev->buffer = spdk_zmalloc(wals_bdev->buffer_blockcnt * wals_bdev->buffer_blocklen, 2 * 1024 * 1024, NULL,
					 SPDK_ENV_LCORE_ID_ANY, SPDK_MALLOC_DMA);
	// TODO: recover

	return 0;
}

static void
wals_bdev_stop(struct wals_bdev *wals_bdev)
{
	// TODO: call target module stop
	// flush?
}

/*
 * brief:
 * wals_bdev_examine function is the examine function call by the below layers
 * like bdev_nvme layer. This function will check if this base bdev can be
 * claimed by this wals bdev or not.
 * params:
 * bdev - pointer to base bdev
 * returns:
 * none
 */
static void
wals_bdev_examine(struct spdk_bdev *bdev)
{
	struct wals_bdev_config	*wals_cfg;
	
	// TODO: let target module to examine

	spdk_bdev_module_examine_done(&g_wals_if);
}

static int
wals_bdev_submit_pending_writes(void *ctx)
{
	struct wals_bdev *wals_bdev = ctx;
	struct wals_bdev_io	*wals_io;
	struct spdk_bdev_io *bdev_io;
	struct wals_slice *slice;
	struct wals_log_position buffer_tail, slice_tail;
	uint64_t i, cnt;

	for (i = 0; i < wals_bdev->slicecnt; i++) {
		slice = &wals_bdev->slices[i];
		if (!TAILQ_EMPTY(&slice->pending_writes)) {
			wals_io = TAILQ_FIRST(&slice->pending_writes);
			bdev_io = spdk_bdev_io_from_ctx(wals_io);
			// check slice space
			if (!wals_bdev_update_tail(bdev_io->u.bdev.num_blocks + METADATA_BLOCKS,
										slice->tail, slice->log_blockcnt, slice->head, &slice_tail)) {
				SPDK_DEBUGLOG(bdev_wals, "waiting bdev io submit due to no enough space left on slice log.\n");
			}

			// check buffer space
			if (!wals_bdev_update_tail(bdev_io->u.bdev.num_blocks + METADATA_BLOCKS, 
										wals_bdev->buffer_tail, wals_bdev->buffer_blockcnt, wals_bdev->buffer_head, &buffer_tail)) {
				SPDK_DEBUGLOG(bdev_wals, "waiting bdev io submit due to no enough space left on buffer.\n");
			}
			_wals_bdev_submit_write_request(wals_io, slice_tail, buffer_tail);
			cnt++;
		}
	}

	return cnt > 0 ? SPDK_POLLER_BUSY : SPDK_POLLER_IDLE;
}

static int 
wals_bdev_log_head_update(void *ctx)
{
	struct wals_bdev *wals_bdev = ctx;
	uint64_t i, cnt = 0;
	struct wals_slice *slice;

	for (i = 0; i < wals_bdev->slicecnt; i++) {
		slice = &wals_bdev->slices[i];
		if (LIST_EMPTY(&slice->outstanding_read_afters)) {
			slice->head = wals_bdev_get_targets_log_head_min2(slice);
			cnt++;
		}
	}

	return cnt > 0 ? SPDK_POLLER_BUSY : SPDK_POLLER_IDLE;
}

static int
wals_bdev_cleaner(void *ctx)
{
	struct wals_bdev *wals_bdev = ctx;
	int i, idx, j, count = 0, total;
	bskiplistNode *update[BSKIPLIST_MAXLEVEL], *x, *tmp;
	long rand = random();

	// clean the index
	for (i = 0; i < 3; i++) {
        rand <<= 4;
        rand += random();
    }
    rand %= wals_bdev->bdev.blockcnt;

	x = wals_bdev->bsl->header;
    for (i = wals_bdev->bsl->level-1; i >= 0; i--) {
        while (x->level[i].forward &&
                (x->level[i].forward->end < rand))
        {
            x = x->level[i].forward;
        }
        update[i] = x;
    }

	// try removal for level times.
	for (i = 0; i < wals_bdev->bsl->level; i++) {
		tmp = x->level[0].forward;
		if (tmp) {
			if (wals_bdev_is_valid_entry(
				wals_bdev_get_targets_log_head_min2(&wals_bdev->slices[tmp->ele->begin / wals_bdev->slice_blockcnt]),
				tmp->ele)) {
				for (j = 0; j < tmp->height; j++) {
					update[j] = tmp;
				}
			} else {
				for (j = 0; j < tmp->height; j++) {
					update[j]->level[j].forward = tmp->level[j].forward;
					tmp->level[j].forward = NULL;
				}
				wals_bdev->bslfn->tail->level[0].forward = tmp;
				wals_bdev->bslfn->tail = tmp;
				count++;
			}
		} else {
			break;
		}
	}

	total = bslfnFree(wals_bdev->bslfn, 10);

	if (total) {
		return SPDK_POLLER_BUSY;
	}

	return SPDK_POLLER_IDLE;
}

static int
wals_bdev_stat_report(void *ctx)
{
	struct wals_bdev *bdev = ctx;

	return SPDK_POLLER_BUSY;
}

/* Log component for bdev wals bdev module */
SPDK_LOG_REGISTER_COMPONENT(bdev_wals)

SPDK_TRACE_REGISTER_FN(wals_trace, "wals", TRACE_GROUP_WALS)
{
	struct spdk_trace_tpoint_opts opts[] = {
		{
			"WALS_BSTAT_CREATE_START", TRACE_WALS_BSTAT_CREATE_START,
			OWNER_WALS, OBJECT_WALS_IO, 1,
			{}
		},
		{
			"WALS_BSTAT_CREATE_END", TRACE_WALS_BSTAT_CREATE_END,
			OWNER_WALS, OBJECT_WALS_IO, 0,
			{}
		},
		{
			"WALS_BSL_INSERT_START", TRACE_WALS_BSL_INSERT_START,
			OWNER_WALS, OBJECT_WALS_IO, 1,
			{}
		},
		{
			"WALS_BSL_INSERT_END", TRACE_WALS_BSL_INSERT_END,
			OWNER_WALS, OBJECT_WALS_IO, 0,
			{}
		},
		{
			"WALS_BSL_RAND_START", TRACE_WALS_BSL_RAND_START,
			OWNER_WALS, OBJECT_WALS_IO, 1,
			{}
		},
		{
			"WALS_BSL_RAND_END", TRACE_WALS_BSL_RAND_END,
			OWNER_WALS, OBJECT_WALS_IO, 0,
			{}
		},
	};

	spdk_trace_register_owner(OWNER_WALS, 'b');
	spdk_trace_register_object(OBJECT_WALS_IO, 'i');
	spdk_trace_register_description_ext(opts, SPDK_COUNTOF(opts));
}
