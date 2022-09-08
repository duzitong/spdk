#include "bdev_wals.h"

#include "spdk/env.h"
#include "spdk/thread.h"
#include "spdk/string.h"
#include "spdk/util.h"

#include "spdk/log.h"

#define LOG_BUFFER_SIZE     131072

struct wals_mem_target {
    void *log_buf;

    void *core_buf;
}

static struct wals_target* 
mem_start(struct wals_target_config *config, struct wals_bdev *wals_bdev)
{
    struct wals_target *target = calloc(1, sizeof(struct wals_target));
    struct wals_mem_target *mem_target = calloc(1, sizeof(struct wals_mem_target));

    mem_target->log_buf = spdk_zmalloc(LOG_BUFFER_SIZE * wals_bdev->bdev.blocklen, 2 * 1024 * 1024, NULL,
					 SPDK_ENV_LCORE_ID_ANY, SPDK_MALLOC_DMA);
    mem_target->core_buf = spdk_zmalloc(wals_bdev->slice_blockcnt * wals_bdev->bdev.blocklen, 2 * 1024 * 1024, NULL,
					 SPDK_ENV_LCORE_ID_ANY, SPDK_MALLOC_DMA);
    
    target->log_blockcnt = LOG_BUFFER_SIZE;
    target->private_info = mem_target;

    return target;
}

static void
mem_stop(struct wals_target *target, struct wals_bdev wals_bdev)
{

}

static int
mem_submit_log_read_request(struct wals_target* target, struct wals_bdev_io *wals_io)
{
    return 0;
}

static int
mem_submit_core_read_request(struct wals_target* target, struct wals_bdev_io *wals_io)
{
    return 0;
}

static int
mem_submit_log_write_request(struct wals_target* target, struct wals_bdev_io *wals_io)
{
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
