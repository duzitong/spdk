#include "bdev_wals.h"

#include "spdk/env.h"
#include "spdk/thread.h"
#include "spdk/string.h"
#include "spdk/util.h"

#include "spdk/log.h"

static struct wals_target* 
mem_start(struct wals_target_config *config)
{
    return calloc(1, sizeof(struct wals_target));
}

static void
mem_stop(struct wals_target *target)
{

}

static int
mem_submit_log_read_request(struct wals_target* target, struct wals_bdev_io *wals_io)
{

}

static int
mem_submit_core_read_request(struct wals_target* target, struct wals_bdev_io *wals_io)
{

}

static int
mem_submit_log_write_request(struct wals_target* target, struct wals_bdev_io *wals_io)
{

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
