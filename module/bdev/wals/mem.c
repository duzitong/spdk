#include "bdev_wals.h"

#include "spdk/env.h"
#include "spdk/thread.h"
#include "spdk/string.h"
#include "spdk/util.h"

#include "spdk/log.h"

#define LOG_BUFFER_SIZE     131072

struct wals_mem_target {
    void                *log_buf;

    void                *core_buf;

    uint64_t            blocklen;

    struct wals_slice   *slice;
};

static struct wals_target* 
mem_start(struct wals_target_config *config, struct wals_bdev *wals_bdev, struct wals_slice *slice)
{
    struct wals_target *target = calloc(1, sizeof(struct wals_target));
    struct wals_mem_target *mem_target = calloc(1, sizeof(struct wals_mem_target));

    mem_target->slice = slice;
    mem_target->blocklen = wals_bdev->bdev.blocklen;
    mem_target->log_buf = spdk_zmalloc(LOG_BUFFER_SIZE * mem_target->blocklen, 2 * 1024 * 1024, NULL,
					 SPDK_ENV_LCORE_ID_ANY, SPDK_MALLOC_DMA);
    mem_target->core_buf = spdk_zmalloc(wals_bdev->slice_blockcnt * mem_target->blocklen, 2 * 1024 * 1024, NULL,
					 SPDK_ENV_LCORE_ID_ANY, SPDK_MALLOC_DMA);
    
    target->log_blockcnt = LOG_BUFFER_SIZE;
    target->private_info = mem_target;

    return target;
}

static void
mem_stop(struct wals_target *target, struct wals_bdev *wals_bdev)
{

}

static int
mem_submit_log_read_request(struct wals_target* target, void *data, uint64_t offset, uint64_t cnt, struct wals_bdev_io *wals_io)
{
    return 0;
}

static int
mem_submit_core_read_request(struct wals_target* target, void *data, uint64_t offset, uint64_t cnt, struct wals_bdev_io *wals_io)
{
    return 0;
}

static int
mem_submit_log_write_request(struct wals_target* target, void *data, uint64_t offset, uint64_t cnt, struct wals_bdev_io *wals_io)
{
    struct wals_mem_target *mem_target = target->private_info;
    memcpy(mem_target->log_buf + offset * mem_target->blocklen, data, cnt * mem_target->blocklen);

    struct wals_metadata *metadata = data;
    memcpy(mem_target->core_buf + metadata->core_offset * mem_target->blocklen, data + METADATA_BLOCKS * mem_target->blocklen, metadata->length * mem_target->blocklen);

    target->log_head_offset = mem_target->slice->log_tail_offset;
    target->log_head_round = mem_target->slice->log_tail_round;
    wals_target_write_complete(wals_io, true);
    return 0;
}


static struct wals_target_module g_mem_module = {
	.name = "mem",
	.start = mem_start,
	.stop = mem_stop,
    .submit_log_read_request = mem_submit_log_read_request,
	.submit_core_read_request = mem_submit_core_read_request,
	.submit_log_write_request = mem_submit_log_write_request,
};
TARGET_MODULE_REGISTER(&g_mem_module)

SPDK_LOG_REGISTER_COMPONENT(bdev_wals_mem)
