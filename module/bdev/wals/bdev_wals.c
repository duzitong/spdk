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

#include <math.h>
#include "bdev_wals.h"
#include "spdk/crc32.h"
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
static void wals_bdev_submit_read_request(struct wals_bdev_io *wals_io);
static void	wals_bdev_examine(struct spdk_bdev *bdev);
static int	wals_bdev_start(struct wals_bdev *bdev);
static void	wals_bdev_stop(struct wals_bdev *bdev);
static int	wals_bdev_init(void);
static bool wals_bdev_is_valid_entry(struct wals_log_position after, struct bstat *bstat);
static int wals_bdev_log_head_update(void *ctx);
static int wals_bdev_cleaner(void *ctx);
static int wals_bdev_stat_report(void *ctx);

static int
wals_bdev_foreach_target_call(struct wals_bdev *wals_bdev, wals_target_fn fn, char *name)
{
	uint64_t i, j;
	int rc;
	
	for (i = 0; i < wals_bdev->slicecnt; i++) {
		for (j = 0; j < NUM_TARGETS; j++) {
			rc = fn(wals_bdev->slices[i].targets[j], wals_bdev);
			if (rc) {
				SPDK_ERRLOG("Failed to call %s target '%ld' in slice '%ld'.", name, j, i);
				return rc;
			}
		}
	}

	return 0;
}

static void wals_log_skip_list_node(struct bskiplistNode* node) {
	char buf[128];
	bslPrintNode(buf, 128, node);
	SPDK_INFOLOG(bdev_wals, "%s\n", buf);
}

static struct wals_lp_firo*
wals_bdev_firo_alloc(const char *name, uint32_t pool_size)
{
	struct wals_lp_firo *firo = calloc(1, sizeof(struct wals_lp_firo));

	TAILQ_INIT(&firo->head);
	firo->entry_pool = spdk_mempool_create(name, pool_size, sizeof(struct wals_lp_firo_entry), 0, SPDK_ENV_SOCKET_ID_ANY);

	return firo;
}

static void
wals_bdev_firo_free(struct wals_lp_firo* firo)
{
	spdk_mempool_free(firo->entry_pool);
}

static struct wals_lp_firo_entry*
wals_bdev_firo_insert(struct wals_lp_firo* firo, wals_log_position lp)
{
	struct wals_lp_firo_entry *entry = spdk_mempool_get(firo->entry_pool);
	entry->pos = lp;
	entry->removed = false;
	TAILQ_INSERT_TAIL(&firo->head, entry, link);

	return entry;
}

static void
wals_bdev_firo_remove(struct wals_lp_firo* firo, struct wals_lp_firo_entry* entry, struct wals_log_position *ret)
{
	struct wals_lp_firo_entry *e = TAILQ_FIRST(&firo->head);

	entry->removed = true;
	while (e && e->removed) {
		*ret = e->pos;
		TAILQ_REMOVE(&firo->head, e, link);
		spdk_mempool_put(firo->entry_pool, e);
		e = TAILQ_FIRST(&firo->head);
	}
}

static bool
wals_bdev_firo_empty(struct wals_lp_firo* firo)
{
	return TAILQ_EMPTY(&firo->head);
}

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

	SPDK_NOTICELOG("wals_bdev_create_cb, %p\n", wals_ch);

	assert(wals_bdev != NULL);
	assert(wals_bdev->state == WALS_BDEV_STATE_ONLINE);

	wals_ch->wals_bdev = wals_bdev;

	pthread_mutex_lock(&wals_bdev->mutex);
	SPDK_NOTICELOG("Core mask of current thread: 0x%s\n", spdk_cpuset_fmt(set));

	/*
	 * When bdev is created, gpt will exaime this bdev, it issues a request and then close the bdev.
	 * Then, destroy_cb will be called. write thread will be set to NULL.
	 * test/bdevio does following steps:
	 *   1. create test bdev on core 0. At this moment, wals bdev needs to serve read request for gpt exaimine.
	 *   2. issue requests on its io_thread, typically, it's core 2. wals_bdev_create_cb will be called only on this core.
	 * Then, if the write thread is reset to NULL, write requests cannot be handled.
	 * So, we do a hack here to remain write thread when wals bdev destroy_cb is called very soon.
	 */
	if (wals_bdev->ch_count == 0) {
		wals_bdev->ch_create_tsc = spdk_get_ticks();	
	}

	for (lcore = 0; lcore < SPDK_CPUSET_SIZE; lcore++) {
		if (spdk_cpuset_get_cpu(set, lcore)) {
			break;
		}
	}

	if (lcore == wals_bdev->write_lcore && wals_bdev->write_thread == NULL) {
		SPDK_NOTICELOG("register write pollers\n");

		wals_bdev->write_thread = spdk_get_thread();

		wals_bdev_foreach_target_call(wals_bdev, wals_bdev->module->register_write_pollers, "register_write_pollers");

		/*
		 * When bdev is created, gpt will exaime this bdev, it issues some read requests.
		 * At this moment, if there's no read_thread to handle the request, bdev cannot be started.
		 * Thus, add a hack here for this.
		 * Afterwards, read thread will be set to the thread on read core.
		 */
		if (!wals_bdev->read_thread) {
			wals_bdev->read_thread = wals_bdev->write_thread;
		}
	}
	if (lcore == wals_bdev->read_lcore && (wals_bdev->read_thread == NULL || wals_bdev->read_thread == wals_bdev->write_thread)) {
		SPDK_NOTICELOG("register read pollers\n");

		wals_bdev->log_head_update_poller = SPDK_POLLER_REGISTER(wals_bdev_log_head_update, wals_bdev, 5);
		wals_bdev->cleaner_poller = SPDK_POLLER_REGISTER(wals_bdev_cleaner, wals_bdev, 1);
		wals_bdev->stat_poller = SPDK_POLLER_REGISTER(wals_bdev_stat_report, wals_bdev, 30*1000*1000);

		wals_bdev_foreach_target_call(wals_bdev, wals_bdev->module->register_read_pollers, "register_read_pollers");

		wals_bdev->read_thread = spdk_get_thread();
	}

	wals_bdev->ch_count++;
	pthread_mutex_unlock(&wals_bdev->mutex);

	return 0;
}

static void
wals_bdev_unregister_write_pollers(void *arg)
{
	struct wals_bdev	*wals_bdev = arg;
	SPDK_NOTICELOG("Unregister write pollers\n");

	wals_bdev_foreach_target_call(wals_bdev, wals_bdev->module->unregister_write_pollers, "unregister_write_pollers");
	
	wals_bdev->write_thread = NULL;
}

static void
wals_bdev_unregister_read_pollers(void *arg)
{
	struct wals_bdev	*wals_bdev = arg;
	SPDK_NOTICELOG("Unregister read pollers\n");

	wals_bdev_foreach_target_call(wals_bdev, wals_bdev->module->unregister_read_pollers, "unregister_read_pollers");

	spdk_poller_unregister(&wals_bdev->log_head_update_poller);
	spdk_poller_unregister(&wals_bdev->cleaner_poller);
	spdk_poller_unregister(&wals_bdev->stat_poller);
	
	wals_bdev->read_thread = NULL;
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

	SPDK_NOTICELOG("wals_bdev_destroy_cb\n");

	assert(wals_ch != NULL);

	pthread_mutex_lock(&wals_bdev->mutex);
	wals_bdev->ch_count--;
	SPDK_NOTICELOG("Ticks from creation: %ld\n", spdk_get_ticks() - wals_bdev->ch_create_tsc);
	if ((spdk_get_ticks() - wals_bdev->ch_create_tsc > spdk_get_ticks_hz() / 1000) // > 1ms, it's a good MAGIC time threshold.
		&& wals_bdev->ch_count == 0) {
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

wals_crc
wals_bdev_calc_crc(void *data, size_t len)
{
	return spdk_crc32c_update(data, len, MAGIC_INIT_CRC);
}

static wals_log_position
wals_bdev_get_targets_log_head_min(struct wals_slice *slice)
{
	int i, min = 0;
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

	return head[min];
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

void
wals_target_read_complete(struct wals_bdev_io *wals_io, bool success)
{
	struct spdk_bdev_io *orig_io = wals_io->orig_io;
	int i;
	void *copy = dma_page_get_buf(wals_io->dma_page);
	struct wals_slice *slice;
	wals_log_position temp;
	
	spdk_trace_record_tsc(spdk_get_ticks(), TRACE_WALS_S_COMP_R_T, 0, 0, (uintptr_t)wals_io);

	wals_io->remaining_read_requests--;
	wals_io->status = success && wals_io->status >= 0
					? SPDK_BDEV_IO_STATUS_SUCCESS
					: SPDK_BDEV_IO_STATUS_FAILED;

	if (!success) {
		SPDK_ERRLOG("Error reading data from target %d.\n", wals_io->target_index);
	}

	if (wals_io->remaining_read_requests == 0) {

		spdk_trace_record_tsc(spdk_get_ticks(), TRACE_WALS_S_COMP_R_A, 0, 0, (uintptr_t)wals_io);

		if (wals_io->status == SPDK_BDEV_IO_STATUS_SUCCESS) {
			for (i = 0; i < orig_io->u.bdev.iovcnt; i++) {
				memcpy(orig_io->u.bdev.iovs[i].iov_base, copy, (size_t)orig_io->u.bdev.iovs[i].iov_len);
				copy += orig_io->u.bdev.iovs[i].iov_len;
			}
			// TODO: add to index
		}

		slice = &wals_io->wals_bdev->slices[wals_io->slice_index];

		temp = slice->head;
		wals_bdev_firo_remove(slice->read_firo, wals_io->firo_entry, &temp);
		slice->head = temp;

		dma_heap_put_page(wals_io->wals_bdev->read_heap, wals_io->dma_page);

		if (spdk_likely(wals_io->status == SPDK_BDEV_IO_STATUS_SUCCESS)) {
			wals_bdev_io_complete(wals_io, SPDK_BDEV_IO_STATUS_SUCCESS);
		} else {
			// TODO: the retry logic may read failed_target_id, which leads to corrupted data.
			wals_io->targets_failed++;
			wals_io->target_index = (wals_io->target_index + 1) % QUORUM_TARGETS;
			if (wals_io->targets_failed < QUORUM_TARGETS) {
				wals_io->status = SPDK_BDEV_IO_STATUS_SUCCESS;
				wals_bdev_submit_read_request(wals_io);
			} else {
				SPDK_ERRLOG("read request failed on all targets.\n");
				wals_bdev_io_complete(wals_io, SPDK_BDEV_IO_STATUS_FAILED);
			}
		}

		spdk_trace_record_tsc(spdk_get_ticks(), TRACE_WALS_F_COMP_R_A, 0, 0, (uintptr_t)wals_io);
	}
	
	spdk_trace_record_tsc(spdk_get_ticks(), TRACE_WALS_F_COMP_R_T, 0, 0, (uintptr_t)wals_io);
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
	int						ret;
	wals_log_position		valid_pos;
	struct bskiplistNode	*bn;
    uint64_t    			read_begin, read_end, read_cur, tmp;
	void					*buf;
	struct wals_checksum_offset	checksum_offset;

	spdk_trace_record_tsc(spdk_get_ticks(), TRACE_WALS_S_SUB_R, 0, 0, (uintptr_t)wals_io, spdk_thread_get_id(spdk_get_thread()));

	SPDK_INFOLOG(bdev_wals, "submit read: %ld+%ld\n", bdev_io->u.bdev.offset_blocks, bdev_io->u.bdev.num_blocks);

	wals_io->slice_index = bdev_io->u.bdev.offset_blocks / wals_bdev->slice_blockcnt;
	slice = &wals_bdev->slices[wals_io->slice_index];
	
	wals_io->dma_page = dma_heap_get_page(wals_bdev->read_heap, bdev_io->u.bdev.num_blocks * wals_bdev->blocklen);
	if (!wals_io->dma_page) {
		SPDK_NOTICELOG("No sufficient read buffer, size: %ld", bdev_io->u.bdev.num_blocks * wals_bdev->blocklen);
		wals_bdev_io_complete(wals_io, SPDK_BDEV_IO_STATUS_NOMEM);
		return;
	}
	buf = dma_page_get_buf(wals_io->dma_page);

	valid_pos = wals_bdev_get_targets_log_head_min(slice);
	SPDK_INFOLOG(bdev_wals, "valid pos: (%ld, %ld)\n", valid_pos.offset, valid_pos.round);
	wals_io->firo_entry = wals_bdev_firo_insert(slice->read_firo, valid_pos);

	read_begin = bdev_io->u.bdev.offset_blocks;
    read_end = bdev_io->u.bdev.offset_blocks + bdev_io->u.bdev.num_blocks - 1;

	bn = bslFirstNodeAfterBegin(wals_bdev->bsl, read_begin);
	wals_log_skip_list_node(bn);
	/*
	 * Completion in read submit request leads to complete the whole read request every time.
	 * Add the initial remaining read requests by 1 to avoid this.
	 * The remaining read request will not be incremented on the last submission so that the whole request could be completed.
	 */
	wals_io->remaining_read_requests = 1;
	read_cur = read_begin;

	while (read_cur <= read_end) {
		while (bn && !wals_bdev_is_valid_entry(valid_pos, bn->ele)) {
			bn = bn->level[0].forward;
		}
		wals_log_skip_list_node(bn);

		if (!bn || read_cur < bn->begin) {
			if (!bn) {
				tmp = read_end;
			} else {
				tmp = bn->begin - 1 > read_end ? read_end : bn->begin - 1;
			}

			if (tmp != read_end) {
				wals_io->remaining_read_requests++;
			}
			/*
			 * TODO: Data on target may corrupt.
			 * Either submit reads to all targets or try next target on failure returned.
			 */

			spdk_trace_record_tsc(spdk_get_ticks(), TRACE_WALS_S_SUB_R_T, 0, 0, (uintptr_t)wals_io, wals_io->target_index, 1);

			SPDK_INFOLOG(bdev_wals, "Read from disk: [%ld, %ld]\n", read_cur, tmp);

			ret = wals_bdev->module->submit_core_read_request(slice->targets[wals_io->target_index], buf + (read_cur - read_begin) * wals_bdev->bdev.blocklen, 
															read_cur, tmp - read_cur + 1, wals_io);
			
			spdk_trace_record_tsc(spdk_get_ticks(), TRACE_WALS_F_SUB_R_T, 0, 0, (uintptr_t)wals_io, wals_io->target_index, 1);

			if (spdk_unlikely(ret != 0)) {
				SPDK_ERRLOG("submit core read request failed to target %d in slice %ld\n", wals_io->target_index, wals_io->slice_index);
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

			if (spdk_unlikely(bn->ele->failed)) {
				SPDK_ERRLOG("read [%ld, %ld] hit failed blocks [%ld, %ld] in slice %ld\n", read_cur, tmp, bn->begin, bn->end, wals_io->slice_index);
				wals_bdev_io_complete(wals_io, SPDK_BDEV_IO_STATUS_FAILED);
				break;
			}

			checksum_offset.block_offset = bn->ele->mdOffset;
			checksum_offset.byte_offset = offsetof(struct wals_metadata, data_checksum) + (read_cur - bn->ele->begin) * sizeof(wals_crc);

			spdk_trace_record_tsc(spdk_get_ticks(), TRACE_WALS_S_SUB_R_T, 0, 0, (uintptr_t)wals_io, wals_io->target_index, 0);

			for (int i = 0; i < NUM_TARGETS; i++) {
				if (bn->ele->failed_target_id != slice->targets[i]->id) {
					SPDK_INFOLOG(bdev_wals, "Read from log: [%ld, %ld]\n", read_cur, tmp);
					ret = wals_bdev->module->submit_log_read_request(slice->targets[i],
						buf + (read_cur - read_begin) * wals_bdev->bdev.blocklen, 
						bn->ele->l.bdevOffset + read_cur - bn->ele->begin,
						tmp - read_cur + 1,
						checksum_offset,
						wals_io);
					break;
				}
			}
			
			spdk_trace_record_tsc(spdk_get_ticks(), TRACE_WALS_F_SUB_R_T, 0, 0, (uintptr_t)wals_io, wals_io->target_index, 0);

			if (spdk_unlikely(ret != 0)) {
				SPDK_ERRLOG("submit log read request failed to target %d in slice %ld\n", wals_io->target_index, wals_io->slice_index);
				wals_bdev_io_complete(wals_io, SPDK_BDEV_IO_STATUS_FAILED);
				break;
			}
			read_cur = tmp + 1;
			bn = bn->level[0].forward;
			continue;
		}
	}

	spdk_trace_record_tsc(spdk_get_ticks(), TRACE_WALS_F_SUB_R, 0, 0, (uintptr_t)wals_io);
}

static void
wals_bdev_insert_read_index(void *arg)
{
	struct wals_index_msg *msg = arg;
	struct wals_bdev *wals_bdev = msg->wals_bdev;
	struct bstat *bstat;
	int count = 0;
	
	spdk_trace_record_tsc(spdk_get_ticks(), TRACE_WALS_S_INSERT_INDEX, 0, 0, (uintptr_t)wals_bdev);

	while (spdk_mempool_count(wals_bdev->bsl_node_pool) <= 1 || spdk_mempool_count(wals_bdev->bstat_pool) == 0) {
		wals_bdev_cleaner(wals_bdev);
		count++;
		if (count % 100000000 == 0) {
			SPDK_NOTICELOG("cleaning index during insert is potentially blocked.\n");
		}
	}

	bstat = bstatBdevCreate(msg->begin, msg->end, msg->round, msg->offset, wals_bdev->bstat_pool);
	bstat->failed = msg->failed;
	bstat->mdOffset = msg->md_offset;
	bstat->failed_target_id = msg->failed_target_id;
	
	bslInsert(wals_bdev->bsl, msg->begin, msg->end, bstat, wals_bdev->bslfn);
	spdk_mempool_put(wals_bdev->index_msg_pool, msg);
	SPDK_DEBUGLOG(bdev_wals, "(%ld) msg returned\n", spdk_thread_get_id(spdk_get_thread()));

	spdk_trace_record_tsc(spdk_get_ticks(), TRACE_WALS_F_INSERT_INDEX, 0, 0, (uintptr_t)wals_bdev);
}

static void
wals_bdev_write_complete_deferred_success(void *arg)
{
	struct wals_bdev_io *wals_io = arg;
	
	spdk_trace_record_tsc(spdk_get_ticks(), TRACE_WALS_S_COMP_IO, 0, 0, (uintptr_t)wals_io, 1, 1);

	spdk_bdev_io_complete(wals_io->orig_io, SPDK_BDEV_IO_STATUS_SUCCESS);

	spdk_trace_record_tsc(spdk_get_ticks(), TRACE_WALS_F_COMP_IO, 0, 0, (uintptr_t)wals_io, 1, 1);
}

static void
wals_bdev_write_complete_deferred_failure(void *arg)
{
	struct wals_bdev_io *wals_io = arg;

	spdk_trace_record_tsc(spdk_get_ticks(), TRACE_WALS_S_COMP_IO, 0, 0, (uintptr_t)wals_io, 0, 1);

	spdk_bdev_io_complete(wals_io->orig_io, SPDK_BDEV_IO_STATUS_FAILED);

	spdk_trace_record_tsc(spdk_get_ticks(), TRACE_WALS_F_COMP_IO, 0, 0, (uintptr_t)wals_io, 0, 1);
}

static void
wals_bdev_write_complete_quorum(struct wals_bdev_io *wals_io)
{
	struct wals_bdev	*wals_bdev = wals_io->wals_bdev;
	struct wals_metadata *metadata = wals_io->metadata;
	struct wals_index_msg *msg = spdk_mempool_get(wals_bdev->index_msg_pool);
	int rc, count = 0;
	
	spdk_trace_record_tsc(spdk_get_ticks(), TRACE_WALS_S_COMP_W_Q, 0, 0, (uintptr_t)wals_io);

	spdk_trace_record_tsc(spdk_get_ticks(), TRACE_WALS_S_COMP_W_WM, 0, 0, (uintptr_t)wals_io);

	while (!msg) {
		msg = spdk_mempool_get(wals_bdev->index_msg_pool);
		count++;
		if (count % 100000000 == 0) {
			SPDK_NOTICELOG("waiting msg %p, %ld+%ld\n", wals_io, metadata->core_offset, metadata->length);
			SPDK_NOTICELOG("read thread last tsc: %ld, now: %ld\n", spdk_thread_get_last_tsc(wals_bdev->read_thread), spdk_get_ticks());
		}
	}

	spdk_trace_record_tsc(spdk_get_ticks(), TRACE_WALS_F_COMP_W_WM, 0, 0, (uintptr_t)wals_io);

	SPDK_DEBUGLOG(bdev_wals, "(%ld)msg got for io %p\n", spdk_thread_get_id(spdk_get_thread()), wals_io);
	
	msg->begin = metadata->core_offset;
	msg->end = metadata->core_offset + metadata->length - 1;
	msg->offset = metadata->next_offset - metadata->length;
	msg->md_offset = msg->offset - metadata->md_blocknum;
	msg->round = metadata->round;
	msg->failed = false;
	msg->failed_target_id = wals_io->failed_target_id;
	msg->wals_bdev = wals_bdev;
	SPDK_DEBUGLOG(bdev_wals, "msg begin: %ld, end: %ld\n", msg->begin, msg->end);
	
	do {
		rc = spdk_thread_send_msg(wals_bdev->read_thread, wals_bdev_insert_read_index, msg);
	} while (rc != 0);

	if (wals_io->orig_thread != spdk_get_thread()) {
		spdk_thread_send_msg(wals_io->orig_thread, wals_bdev_write_complete_deferred_success, wals_io);
	} else {
		wals_bdev_write_complete_deferred_success(wals_io);
	}

	spdk_trace_record_tsc(spdk_get_ticks(), TRACE_WALS_F_COMP_W_Q, 0, 0, (uintptr_t)wals_io);
}

static void
wals_bdev_write_complete_failed(struct wals_bdev_io *wals_io)
{
	struct wals_bdev	*wals_bdev = wals_io->wals_bdev;
	struct wals_metadata *metadata = wals_io->metadata;
	struct wals_index_msg *msg = spdk_mempool_get(wals_bdev->index_msg_pool);
	int rc, count = 0;
	
	spdk_trace_record_tsc(spdk_get_ticks(), TRACE_WALS_S_COMP_W_Q, 0, 0, (uintptr_t)wals_io);

	spdk_trace_record_tsc(spdk_get_ticks(), TRACE_WALS_S_COMP_W_WM, 0, 0, (uintptr_t)wals_io);

	while (!msg) {
		msg = spdk_mempool_get(wals_bdev->index_msg_pool);
		count++;
		if (count % 100000000 == 0) {
			SPDK_NOTICELOG("waiting msg %p, %ld+%ld\n", wals_io, metadata->core_offset, metadata->length);
			SPDK_NOTICELOG("read thread last tsc: %ld, now: %ld\n", spdk_thread_get_last_tsc(wals_bdev->read_thread), spdk_get_ticks());
		}
	}

	spdk_trace_record_tsc(spdk_get_ticks(), TRACE_WALS_F_COMP_W_WM, 0, 0, (uintptr_t)wals_io);

	SPDK_DEBUGLOG(bdev_wals, "(%ld)msg got for io %p\n", spdk_thread_get_id(spdk_get_thread()), wals_io);
	
	msg->begin = metadata->core_offset;
	msg->end = metadata->core_offset + metadata->length - 1;
	msg->offset = metadata->next_offset - metadata->length;
	msg->round = metadata->round;
	msg->failed = true;
	msg->wals_bdev = wals_bdev;
	SPDK_DEBUGLOG(bdev_wals, "msg begin: %ld, end: %ld\n", msg->begin, msg->end);
	
	do {
		rc = spdk_thread_send_msg(wals_bdev->read_thread, wals_bdev_insert_read_index, msg);
	} while (rc != 0);

	if (wals_io->orig_thread != spdk_get_thread()) {
		spdk_thread_send_msg(wals_io->orig_thread, wals_bdev_write_complete_deferred_failure, wals_io);
	} else {
		wals_bdev_write_complete_deferred_failure(wals_io);
	}

	spdk_trace_record_tsc(spdk_get_ticks(), TRACE_WALS_F_COMP_W_Q, 0, 0, (uintptr_t)wals_io);
}

static void
wals_bdev_write_free_io(void *arg)
{
	struct wals_bdev_io *wals_io = arg;
	wals_io->orig_io->can_free = true;
	spdk_bdev_free_io(wals_io->orig_io);
}

static void
wals_bdev_write_complete_all(struct wals_bdev_io *wals_io)
{
	spdk_trace_record_tsc(spdk_get_ticks(), TRACE_WALS_S_COMP_W_A, 0, 0, (uintptr_t)wals_io);

	struct wals_slice *slice = &wals_io->wals_bdev->slices[wals_io->slice_index];
	wals_bdev_firo_remove(slice->write_firo, wals_io->firo_entry, &slice->committed_tail);

	dma_heap_put_page(wals_io->wals_bdev->write_heap, wals_io->dma_page);

	if (wals_io->orig_thread != spdk_get_thread()) {
		spdk_thread_send_msg(wals_io->orig_thread, wals_bdev_write_free_io, wals_io);
	} else {
		wals_bdev_write_free_io(wals_io);
	}

	spdk_trace_record_tsc(spdk_get_ticks(), TRACE_WALS_F_COMP_W_A, 0, 0, (uintptr_t)wals_io);
}

void
wals_target_write_complete(struct wals_bdev_io *wals_io, bool success, int target_id)
{
	spdk_trace_record_tsc(spdk_get_ticks(), TRACE_WALS_S_COMP_W_T, 0, 0, (uintptr_t)wals_io);

	wals_io->targets_completed++;
	wals_io->orig_io->can_free = false;

	// if some targets already fail, then failed_target_id must have a valid value.
	if (wals_io->targets_failed == 0) {
		if (success) {
			// if all four targets are successful, then failed_target_id will be 0 eventually.
			// but we don't revert the failed_target_id which was sent to read thread.
			wals_io->failed_target_id -= target_id;
		}
		else {
			// only track first failure.
			wals_io->failed_target_id = target_id;
		}
	}

	if (((struct wals_metadata*) wals_io->dma_page->buf)->version != METADATA_VERSION) {
        SPDK_ERRLOG("%p %d %ld\n", wals_io->dma_page, success, wals_io->dma_page->data_size);
    }

	if (spdk_unlikely(!success)) {
		wals_io->targets_failed++;

		if (!wals_io->io_completed && wals_io->targets_failed >= NUM_TARGETS - QUORUM_TARGETS + 1) {
			wals_bdev_write_complete_failed(wals_io);
			wals_io->io_completed = true;
		}

		if (wals_io->targets_completed == NUM_TARGETS) {
			wals_bdev_write_complete_all(wals_io);
		}

		return;
	}

	if (!wals_io->io_completed && wals_io->targets_completed - wals_io->targets_failed >= QUORUM_TARGETS) {
		wals_bdev_write_complete_quorum(wals_io);
		wals_io->io_completed = true;
	}

	if (wals_io->targets_completed == NUM_TARGETS) {
		if (wals_io->targets_failed == 0 && wals_io->failed_target_id != 0) {
			SPDK_ERRLOG("Failed target id %d when all four targets are successful\n", wals_io->failed_target_id);
		}
		wals_bdev_write_complete_all(wals_io);
	}

	spdk_trace_record_tsc(spdk_get_ticks(), TRACE_WALS_F_COMP_W_T, 0, 0, (uintptr_t)wals_io);
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
_wals_bdev_submit_write_request(struct wals_bdev_io *wals_io, wals_log_position slice_tail)
{
	struct spdk_bdev_io		*bdev_io = spdk_bdev_io_from_ctx(wals_io);
	struct wals_bdev		*wals_bdev = wals_io->wals_bdev;
	struct wals_slice		*slice = &wals_bdev->slices[wals_io->slice_index];
	
	int						ret, i;
	struct wals_metadata	*metadata;
	void					*ptr, *data;
	wals_crc				*checksum;
	struct iovec			*iovs;
	size_t					md_size = offsetof(struct wals_metadata, md_checksum);

	spdk_trace_record_tsc(spdk_get_ticks(), TRACE_WALS_S_SUB_W_I, 0, 0, (uintptr_t)wals_io);

	ptr = dma_page_get_buf(wals_io->dma_page);
	metadata = (struct wals_metadata *) ptr;
	metadata->version = METADATA_VERSION; // TODO: add metadata CRC
	metadata->seq = ++slice->seq;
	metadata->core_offset = bdev_io->u.bdev.offset_blocks;
	metadata->next_offset = slice_tail.offset;
	metadata->length = bdev_io->u.bdev.num_blocks;
	metadata->round = slice_tail.round;
	metadata->md_blocknum = wals_io->total_num_blocks - bdev_io->u.bdev.num_blocks;

	spdk_trace_record_tsc(spdk_get_ticks(), TRACE_WALS_S_CALC_CRC, 0, 0, (uintptr_t)wals_io, 1);
	metadata->md_checksum = wals_bdev_calc_crc(metadata, md_size);
	spdk_trace_record_tsc(spdk_get_ticks(), TRACE_WALS_F_CALC_CRC, 0, 0, (uintptr_t)wals_io);

	wals_io->metadata = metadata;

	// memcpy data
	data = ptr + (wals_io->total_num_blocks - bdev_io->u.bdev.num_blocks) * wals_bdev->blocklen;
	iovs = bdev_io->u.bdev.iovs;

	for (i = 0; i < bdev_io->u.bdev.iovcnt; i++) {
		memcpy(data, iovs[i].iov_base, iovs[i].iov_len);
		data += iovs[i].iov_len;
	}
	
	data = ptr + (wals_io->total_num_blocks - bdev_io->u.bdev.num_blocks) * wals_bdev->blocklen;
	checksum = metadata->data_checksum;

	spdk_trace_record_tsc(spdk_get_ticks(), TRACE_WALS_S_CALC_CRC, 0, 0, (uintptr_t)wals_io, bdev_io->u.bdev.num_blocks);
	for (i = 0; i < bdev_io->u.bdev.num_blocks; i++) {
		*checksum = wals_bdev_calc_crc(data, wals_bdev->blocklen);
		checksum++;
		data += wals_bdev->blocklen;
	}
	spdk_trace_record_tsc(spdk_get_ticks(), TRACE_WALS_F_CALC_CRC, 0, 0, (uintptr_t)wals_io);

	wals_io->firo_entry = wals_bdev_firo_insert(slice->write_firo, slice_tail);

	// must happen before sending write IOs to avoid sync return.
	for (i = 0; i < NUM_TARGETS; i++) {
		wals_io->failed_target_id += slice->targets[i]->id;
	}

	// call module to submit to all targets
	for (i = 0; i < NUM_TARGETS; i++) {
		spdk_trace_record_tsc(spdk_get_ticks(), TRACE_WALS_S_SUB_W_T, 0, 0, (uintptr_t)wals_io, i, wals_io->total_num_blocks);

		ret = wals_bdev->module->submit_log_write_request(slice->targets[i], ptr,
														slice_tail.offset - wals_io->total_num_blocks,
														wals_io->total_num_blocks,
														wals_io);

		spdk_trace_record_tsc(spdk_get_ticks(), TRACE_WALS_F_SUB_W_T, 0, 0, (uintptr_t)wals_io, i);

		if (spdk_unlikely(ret != 0)) {
			wals_io->targets_failed++;
			SPDK_ERRLOG("io submit error due to %d for target %d on slice %ld.\n", ret, i, wals_io->slice_index);
		}
	}

	slice->tail = slice_tail;
	SPDK_DEBUGLOG(bdev_wals, "slice tail updated: %ld(%ld)\n", slice->tail.offset, slice->tail.round);

	if (spdk_unlikely(wals_io->targets_failed == NUM_TARGETS)) {
		SPDK_ERRLOG("All targets failed on slice %ld, no complete callback will be triggered.\n", wals_io->slice_index);
		wals_bdev_io_complete(wals_io, SPDK_BDEV_IO_STATUS_FAILED);
	}

	// from now on, every call to spdk_bdev_free_io will not do anything,
	// until all targets return.
	wals_io->orig_io->can_free = false;

	spdk_trace_record_tsc(spdk_get_ticks(), TRACE_WALS_F_SUB_W_I, 0, 0, (uintptr_t)wals_io);
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
	wals_log_position		slice_tail;

	double					md_size = offsetof(struct wals_metadata, data_checksum);

	spdk_trace_record_tsc(spdk_get_ticks(), TRACE_WALS_S_SUB_W, 0, 0, (uintptr_t)wals_io, spdk_thread_get_id(spdk_get_thread()));

	SPDK_DEBUGLOG(bdev_wals, "submit write: %ld+%ld\n", bdev_io->u.bdev.offset_blocks, bdev_io->u.bdev.num_blocks);

	wals_io->slice_index = bdev_io->u.bdev.offset_blocks / wals_bdev->slice_blockcnt;
	slice = &wals_bdev->slices[wals_io->slice_index];

	md_size += bdev_io->u.bdev.num_blocks * sizeof(wals_crc);
	wals_io->total_num_blocks = ceil(md_size / wals_bdev->blocklen) + bdev_io->u.bdev.num_blocks;

	// check slice space
	if (!wals_bdev_update_tail(wals_io->total_num_blocks,
								slice->tail, slice->log_blockcnt, slice->head, &slice_tail)) {
		// SPDK_NOTICELOG("queue bdev io submit due to no enough space left on slice log. head: (%ld,%ld) tail: (%ld,%ld)\n", slice->head.offset, slice->head.round, slice->tail.offset, slice->tail.round);
		spdk_thread_send_msg(spdk_get_thread(), wals_bdev_submit_write_request, wals_io);
		
		spdk_trace_record_tsc(spdk_get_ticks(), TRACE_WALS_F_SUB_W, 0, 0, (uintptr_t)wals_io, 1, 0);

		return;
	}

	// check buffer space
	wals_io->dma_page = dma_heap_get_page(wals_bdev->write_heap, bdev_io->u.bdev.num_blocks * wals_bdev->blocklen);
	if (!wals_io->dma_page) {
		SPDK_NOTICELOG("queue bdev io submit due to no enough space left on buffer.\n");
		spdk_thread_send_msg(spdk_get_thread(), wals_bdev_submit_write_request, wals_io);

		spdk_trace_record_tsc(spdk_get_ticks(), TRACE_WALS_F_SUB_W, 0, 0, (uintptr_t)wals_io, 0, 1);

		return;
	}

	_wals_bdev_submit_write_request(wals_io, slice_tail);

	spdk_trace_record_tsc(spdk_get_ticks(), TRACE_WALS_F_SUB_W, 0, 0, (uintptr_t)wals_io, 0, 0);
}

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

static void wals_bdev_diagnose_unregister_write_pollers(struct wals_bdev* wals_bdev) {
	wals_bdev_foreach_target_call(wals_bdev,
		wals_bdev->module->diagnose_unregister_write_pollers,
		"diagnose_unregister_write_pollers");
}

static void wals_bdev_diagnose_unregister_read_pollers(struct wals_bdev* wals_bdev) {
	spdk_poller_unregister(&wals_bdev->log_head_update_poller);
	spdk_poller_unregister(&wals_bdev->cleaner_poller);
	spdk_poller_unregister(&wals_bdev->stat_poller);

	wals_bdev_foreach_target_call(wals_bdev,
		wals_bdev->module->diagnose_unregister_read_pollers,
		"diagnose_unregister_read_pollers");
}

static void wals_bdev_enter_diagnostic_mode(struct wals_bdev* wals_bdev) {
	wals_bdev->in_diagnostic_mode = true;
	if (spdk_get_thread() == wals_bdev->write_thread) {
		wals_bdev_diagnose_unregister_write_pollers(wals_bdev);
	}
	else {
		spdk_thread_send_msg(wals_bdev->write_thread,
			wals_bdev_diagnose_unregister_write_pollers,
			wals_bdev);
	}

	if (spdk_get_thread() == wals_bdev->read_thread) {
		wals_bdev_diagnose_unregister_read_pollers(wals_bdev);
	}
	else {
		spdk_thread_send_msg(wals_bdev->read_thread,
			wals_bdev_diagnose_unregister_read_pollers,
			wals_bdev);
	}

	spdk_log_set_flag("bdev_wals");
	// Debug logs should not be seen unless compiled with DEBUG macro.
	spdk_log_set_level(SPDK_LOG_DEBUG);
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

	spdk_trace_record_tsc(spdk_get_ticks(), TRACE_WALS_S_SUB_IO, 0, 0, (uintptr_t)wals_io, spdk_thread_get_id(spdk_get_thread()));

	wals_io->wals_ch = spdk_io_channel_get_ctx(ch);
	wals_io->wals_bdev = wals_io->wals_ch->wals_bdev;
	wals_io->orig_io = bdev_io;
	wals_io->orig_thread = spdk_get_thread();
	wals_io->status = SPDK_BDEV_IO_STATUS_SUCCESS;
	wals_io->targets_failed = 0;
	wals_io->targets_completed = 0;
	wals_io->target_index = 0;  // TODO: round-robin?
	wals_io->io_completed = false;
	wals_io->failed_target_id = 0;

	/*
	 * Write requests are sent to write_thread, read requests are sent to read_thread.
	 * All read/write related logics must be done on its corresponding thread.
	 * The only thing should be processed on orig_thread is completing the io request.
	 */

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
	case SPDK_BDEV_IO_TYPE_FLUSH:
		// use it as internal diagnostic
		SPDK_NOTICELOG("Enter diagnostic mode.\n");
		wals_bdev_enter_diagnostic_mode(wals_io->wals_bdev);
		break;
	case SPDK_BDEV_IO_TYPE_RESET:
	case SPDK_BDEV_IO_TYPE_UNMAP:
	default:
		SPDK_ERRLOG("submit request, invalid io type %u\n", bdev_io->type);
		wals_bdev_io_complete(wals_io, SPDK_BDEV_IO_STATUS_FAILED);
		break;
	}

	spdk_trace_record_tsc(spdk_get_ticks(), TRACE_WALS_F_SUB_IO, 0, 0, (uintptr_t)wals_io);
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
	case SPDK_BDEV_IO_TYPE_FLUSH:
		return true;

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
			uint32_t write_lcore, uint32_t read_lcore,
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

	wals_cfg->write_lcore = write_lcore;
	wals_cfg->read_lcore = read_lcore;

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

	wals_bdev->write_lcore = wals_cfg->write_lcore;
	wals_bdev->read_lcore = wals_cfg->read_lcore;

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
	wals_bdev->blocklen = wals_cfg->blocklen;
	wals_bdev->blocklen_shift = spdk_align64pow2(wals_bdev->blocklen);
	wals_bdev->buffer_blockcnt = wals_cfg->buffer_blockcnt;

	rc = wals_bdev_start(wals_bdev);
	if (rc) {
		SPDK_ERRLOG("Failed to start WALS bdev '%s'.\n", wals_cfg->name);
		return rc;
	}

	for (i = 0; i < wals_cfg->slicecnt; i++) {
		wals_bdev->slices[i].log_blockcnt = UINT64_MAX;
		for (j = 0; j < NUM_TARGETS; j++) {
			wals_bdev->slices[i].targets[j] = wals_bdev->module->start(&wals_cfg->slices[i].targets[j], wals_bdev, &wals_bdev->slices[i]);
			if (wals_bdev->slices[i].targets[j] == NULL) {
				SPDK_ERRLOG("Failed to start target '%ld' in slice '%ld'.", j + 1, i);
				return -EFAULT;
			}

			// TODO: target allocation should be done by MDS
			wals_bdev->slices[i].targets[j]->id = j;
			if (wals_bdev->slices[i].targets[j]->log_blockcnt < wals_bdev->slices[i].log_blockcnt) {
				wals_bdev->slices[i].log_blockcnt = wals_bdev->slices[i].targets[j]->log_blockcnt;
			}
		}
		SPDK_NOTICELOG("log_blockcnt = %ld\n", wals_bdev->slices[i].log_blockcnt);
	}

	// TODO: recover

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
	char pool_name[30];

	mempool_size = wals_bdev->bdev.blockcnt;
	mempool_size = spdk_align64pow2(mempool_size);

	wals_bdev->bstat_pool = spdk_mempool_create("WALS_BSTAT_POOL", mempool_size, sizeof(bstat), 0, SPDK_ENV_SOCKET_ID_ANY);
	wals_bdev->bsl_node_pool = spdk_mempool_create("WALS_BSL_NODE_POOL", mempool_size, sizeof(bskiplistNode), 0, SPDK_ENV_SOCKET_ID_ANY);
	wals_bdev->index_msg_pool = spdk_mempool_create("WALS_INDEX_MSG_POOL", 128, sizeof(struct wals_index_msg), 0, SPDK_ENV_SOCKET_ID_ANY);

	wals_bdev->bsl = bslCreate(wals_bdev->bsl_node_pool, wals_bdev->bstat_pool);
	wals_bdev->bslfn = bslfnCreate(wals_bdev->bsl_node_pool, wals_bdev->bstat_pool);

	for (i = 0; i < wals_bdev->slicecnt; i++) {
		snprintf(pool_name, sizeof(pool_name), "WALS_WRITE_FIRO_%ld", i);
		wals_bdev->slices[i].write_firo = wals_bdev_firo_alloc(pool_name, 256);
		snprintf(pool_name, sizeof(pool_name), "WALS_READ_FIRO_%ld", i);
		wals_bdev->slices[i].read_firo = wals_bdev_firo_alloc(pool_name, 256);
	}

	wals_bdev->write_heap = dma_heap_alloc(wals_bdev->buffer_blockcnt * wals_bdev->blocklen, offsetof(struct wals_metadata, data_checksum), sizeof(wals_crc), wals_bdev->blocklen_shift);
	wals_bdev->read_heap = dma_heap_alloc(wals_bdev->buffer_blockcnt * wals_bdev->blocklen, 0, 0, wals_bdev->blocklen_shift);

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

// log head update should only update to the minimum of outstanding read PMEM IOs.
// In current implementation, only update when there are no outstanding read PMEM IOs.
static int 
wals_bdev_log_head_update(void *ctx)
{
	struct wals_bdev *wals_bdev = ctx;
	uint64_t i, cnt = 0;
	struct wals_slice *slice;

	spdk_trace_record_tsc(spdk_get_ticks(), TRACE_WALS_S_UPDATE_HEAD, 0, 0, (uintptr_t)wals_bdev);

	for (i = 0; i < wals_bdev->slicecnt; i++) {
		slice = &wals_bdev->slices[i];
		if (wals_bdev_firo_empty(slice->read_firo)) {
			slice->head = wals_bdev_get_targets_log_head_min(slice);
			cnt++;
		}
	}

	spdk_trace_record_tsc(spdk_get_ticks(), TRACE_WALS_F_UPDATE_HEAD, 0, 0, (uintptr_t)wals_bdev);

	return cnt > 0 ? SPDK_POLLER_BUSY : SPDK_POLLER_IDLE;
}

static int
wals_bdev_cleaner(void *ctx)
{
	struct wals_bdev *wals_bdev = ctx;
	int i, j, total;
	bskiplistNode *update[BSKIPLIST_MAXLEVEL], *x;
	long rand = random();

	spdk_trace_record_tsc(spdk_get_ticks(), TRACE_WALS_S_CLEAN_INDEX, 0, 0, (uintptr_t)wals_bdev);

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

	x = x->level[0].forward;
	if (x && !wals_bdev_is_valid_entry(
		// BUG: should be slice->head?
		wals_bdev_get_targets_log_head_min(&wals_bdev->slices[x->ele->begin / wals_bdev->slice_blockcnt]),
		x->ele)) {
		for (j = 0; j < x->height; j++) {
			update[j]->level[j].forward = x->level[j].forward;
			x->level[j].forward = NULL;
		}
		wals_bdev->bslfn->tail->level[0].forward = x;
		wals_bdev->bslfn->tail = x;
	}

	total = bslfnFree(wals_bdev->bslfn, 10);

	spdk_trace_record_tsc(spdk_get_ticks(), TRACE_WALS_F_CLEAN_INDEX, 0, 0, (uintptr_t)wals_bdev, total);

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
			"WALS_S_SUB_IO", TRACE_WALS_S_SUB_IO,
			OWNER_WALS, OBJECT_WALS_IO, 1,
			{
				{ "thread", SPDK_TRACE_ARG_TYPE_INT, 8 },
			}
		},
		{
			"WALS_F_SUB_IO", TRACE_WALS_F_SUB_IO,
			OWNER_WALS, OBJECT_WALS_IO, 0,
			{}
		},
		{
			"WALS_S_COMP_IO", TRACE_WALS_S_COMP_IO,
			OWNER_WALS, OBJECT_WALS_IO, 0,
			{
				{ "success", SPDK_TRACE_ARG_TYPE_INT, 8 },
				{ "deferred", SPDK_TRACE_ARG_TYPE_INT, 8 },
			}
		},
		{
			"WALS_F_COMP_IO", TRACE_WALS_F_COMP_IO,
			OWNER_WALS, OBJECT_WALS_IO, 0,
			{
				{ "success", SPDK_TRACE_ARG_TYPE_INT, 8 },
				{ "deferred", SPDK_TRACE_ARG_TYPE_INT, 8 },
			}
		},
		{
			"WALS_S_SUB_W", TRACE_WALS_S_SUB_W,
			OWNER_WALS, OBJECT_WALS_IO, 0,
			{
				{ "thread", SPDK_TRACE_ARG_TYPE_INT, 8 },
			}
		},
		{
			"WALS_F_SUB_W", TRACE_WALS_F_SUB_W,
			OWNER_WALS, OBJECT_WALS_IO, 0,
			{
				{ "sfull", SPDK_TRACE_ARG_TYPE_INT, 8 },
				{ "bfull", SPDK_TRACE_ARG_TYPE_INT, 8 },
			}
		},
		{
			"WALS_S_SUB_W_I", TRACE_WALS_S_SUB_W_I,
			OWNER_WALS, OBJECT_WALS_IO, 0,
			{}
		},
		{
			"WALS_F_SUB_W_I", TRACE_WALS_F_SUB_W_I,
			OWNER_WALS, OBJECT_WALS_IO, 0,
			{}
		},
		{
			"WALS_S_SUB_W_T", TRACE_WALS_S_SUB_W_T,
			OWNER_WALS, OBJECT_WALS_IO, 0,
			{
				{ "target", SPDK_TRACE_ARG_TYPE_INT, 8 },
				{ "total", SPDK_TRACE_ARG_TYPE_INT, 8 },
			}
		},
		{
			"WALS_F_SUB_W_T", TRACE_WALS_F_SUB_W_T,
			OWNER_WALS, OBJECT_WALS_IO, 0,
			{
				{ "target", SPDK_TRACE_ARG_TYPE_INT, 8 },
			}
		},
		{
			"WALS_S_COMP_W_T", TRACE_WALS_S_COMP_W_T,
			OWNER_WALS, OBJECT_WALS_IO, 0,
			{}
		},
		{
			"WALS_F_COMP_W_T", TRACE_WALS_F_COMP_W_T,
			OWNER_WALS, OBJECT_WALS_IO, 0,
			{}
		},
		{
			"WALS_S_COMP_W_Q", TRACE_WALS_S_COMP_W_Q,
			OWNER_WALS, OBJECT_WALS_IO, 0,
			{}
		},
		{
			"WALS_F_COMP_W_Q", TRACE_WALS_F_COMP_W_Q,
			OWNER_WALS, OBJECT_WALS_IO, 0,
			{}
		},
		{
			"WALS_S_COMP_W_A", TRACE_WALS_S_COMP_W_A,
			OWNER_WALS, OBJECT_WALS_IO, 0,
			{}
		},
		{
			"WALS_F_COMP_W_A", TRACE_WALS_F_COMP_W_A,
			OWNER_WALS, OBJECT_WALS_IO, 0,
			{}
		},
		{
			"WALS_S_COMP_W_F", TRACE_WALS_S_COMP_W_F,
			OWNER_WALS, OBJECT_WALS_IO, 0,
			{}
		},
		{
			"WALS_F_COMP_W_F", TRACE_WALS_F_COMP_W_F,
			OWNER_WALS, OBJECT_WALS_IO, 0,
			{}
		},
		{
			"WALS_S_COMP_W_WM", TRACE_WALS_S_COMP_W_WM,
			OWNER_WALS, OBJECT_WALS_IO, 0,
			{}
		},
		{
			"WALS_F_COMP_W_WM", TRACE_WALS_F_COMP_W_WM,
			OWNER_WALS, OBJECT_WALS_IO, 0,
			{}
		},
		{
			"WALS_S_SUB_R", TRACE_WALS_S_SUB_R,
			OWNER_WALS, OBJECT_WALS_IO, 0,
			{
				{ "thread", SPDK_TRACE_ARG_TYPE_INT, 8 },
			}
		},
		{
			"WALS_F_SUB_R", TRACE_WALS_F_SUB_R,
			OWNER_WALS, OBJECT_WALS_IO, 0,
			{}
		},
		{
			"WALS_S_SUB_R_T", TRACE_WALS_S_SUB_R_T,
			OWNER_WALS, OBJECT_WALS_IO, 0,
			{}
		},
		{
			"WALS_F_SUB_R_T", TRACE_WALS_F_SUB_R_T,
			OWNER_WALS, OBJECT_WALS_IO, 0,
			{}
		},
		{
			"WALS_S_COMP_R_T", TRACE_WALS_S_COMP_R_T,
			OWNER_WALS, OBJECT_WALS_IO, 0,
			{}
		},
		{
			"WALS_F_COMP_R_T", TRACE_WALS_F_COMP_R_T,
			OWNER_WALS, OBJECT_WALS_IO, 0,
			{}
		},
		{
			"WALS_S_COMP_R_A", TRACE_WALS_S_COMP_R_A,
			OWNER_WALS, OBJECT_WALS_IO, 0,
			{}
		},
		{
			"WALS_F_COMP_R_A", TRACE_WALS_F_COMP_R_A,
			OWNER_WALS, OBJECT_WALS_IO, 0,
			{}
		},
		{
			"WALS_S_INSERT_INDEX", TRACE_WALS_S_INSERT_INDEX,
			OWNER_WALS, OBJECT_WALS_BDEV, 1,
			{}
		},
		{
			"WALS_F_INSERT_INDEX", TRACE_WALS_F_INSERT_INDEX,
			OWNER_WALS, OBJECT_WALS_BDEV, 0,
			{}
		},
		{
			"WALS_S_CLEAN_INDEX", TRACE_WALS_S_CLEAN_INDEX,
			OWNER_WALS, OBJECT_WALS_BDEV, 1,
			{}
		},
		{
			"WALS_F_CLEAN_INDEX", TRACE_WALS_F_CLEAN_INDEX,
			OWNER_WALS, OBJECT_WALS_BDEV, 0,
			{
				{ "cleaned", SPDK_TRACE_ARG_TYPE_INT, 8 },
			}
		},
		{
			"WALS_S_UPDATE_HEAD", TRACE_WALS_S_UPDATE_HEAD,
			OWNER_WALS, OBJECT_WALS_BDEV, 1,
			{}
		},
		{
			"WALS_F_UPDATE_HEAD", TRACE_WALS_F_UPDATE_HEAD,
			OWNER_WALS, OBJECT_WALS_BDEV, 0,
			{}
		},
		{
			"WALS_S_RDMA_CQ", TRACE_WALS_S_RDMA_CQ,
			OWNER_WALS, OBJECT_WALS_BDEV, 1,
			{}
		},
		{
			"WALS_F_RDMA_CQ", TRACE_WALS_F_RDMA_CQ,
			OWNER_WALS, OBJECT_WALS_BDEV, 0,
			{}
		},
		{
			"WALS_S_CALC_CRC", TRACE_WALS_S_CALC_CRC,
			OWNER_WALS, OBJECT_WALS_IO, 0,
			{
				{ "blocks", SPDK_TRACE_ARG_TYPE_INT, 8 }
			}
		},
		{
			"WALS_F_CALC_CRC", TRACE_WALS_F_CALC_CRC,
			OWNER_WALS, OBJECT_WALS_IO, 0,
			{}
		},
	};

	spdk_trace_register_owner(OWNER_WALS, 'w');
	spdk_trace_register_object(OBJECT_WALS_IO, 'i');
	spdk_trace_register_object(OBJECT_WALS_BDEV, 'b');
	spdk_trace_register_description_ext(opts, SPDK_COUNTOF(opts));
}
