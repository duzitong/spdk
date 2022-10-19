#include "bdev_wals.h"

#include "spdk/env.h"
#include "spdk/thread.h"
#include "spdk/string.h"
#include "spdk/util.h"
#include "spdk/likely.h"

#include "spdk/log.h"

#define LOG_BUFFER_SIZE     131072


static struct wals_target* 
null_start(struct wals_target_config *config, struct wals_bdev *wals_bdev, struct wals_slice *slice)
{
    struct wals_target *target = calloc(1, sizeof(struct wals_target));
    
    target->log_blockcnt = LOG_BUFFER_SIZE;

    return target;
}

static void
null_stop(struct wals_target *target, struct wals_bdev *wals_bdev)
{

}

static int
null_submit_log_read_request(struct wals_target* target, void *data, uint64_t offset, uint64_t cnt, struct wals_bdev_io *wals_io)
{
    SPDK_DEBUGLOG(bdev_wals_null, "log read: %ld+%ld\n", offset, cnt);

    wals_target_read_complete(wals_io, true);
    return 0;
}

static int
null_submit_core_read_request(struct wals_target* target, void *data, uint64_t offset, uint64_t cnt, struct wals_bdev_io *wals_io)
{
    SPDK_DEBUGLOG(bdev_wals_null, "core read: %ld+%ld\n", offset, cnt);

    wals_target_read_complete(wals_io, true);
    return 0;
}

static int
null_submit_log_write_request(struct wals_target* target, void *data, uint64_t offset, uint64_t cnt, struct wals_bdev_io *wals_io)
{
    SPDK_DEBUGLOG(bdev_wals_null, "log write: %ld+%ld\n", offset, cnt);

    if (spdk_unlikely(offset + cnt > LOG_BUFFER_SIZE)) {
        SPDK_ERRLOG("ERROR!!! %ld, %ld\n", offset, cnt);
    }

    if (offset < target->head.offset) {
        target->head.round++;
    }
    target->head.offset = offset+cnt;
    SPDK_DEBUGLOG(bdev_wals_null, "target head updated: %ld(%ld)\n", target->head.offset, target->head.round);
    wals_target_write_complete(wals_io, true);
    return 0;
}

static int
null_register_write_pollers(struct wals_target *target, struct wals_bdev *wals_bdev)
{
    return 0;
}

static int
null_unregister_write_pollers(struct wals_target *target, struct wals_bdev *wals_bdev)
{
    return 0;
}

static int
null_register_read_pollers(struct wals_target *target, struct wals_bdev *wals_bdev)
{
    return 0;
}

static int
null_unregister_read_pollers(struct wals_target *target, struct wals_bdev *wals_bdev)
{
    return 0;
}


static struct wals_target_module g_null_module = {
	.name = "null",
	.start = null_start,
	.stop = null_stop,
    .submit_log_read_request = null_submit_log_read_request,
	.submit_core_read_request = null_submit_core_read_request,
	.submit_log_write_request = null_submit_log_write_request,
    .register_write_pollers = null_register_write_pollers,
    .unregister_write_pollers = null_unregister_write_pollers,
    .register_read_pollers = null_register_read_pollers,
    .unregister_read_pollers = null_unregister_read_pollers,
};
TARGET_MODULE_REGISTER(&g_null_module)

SPDK_LOG_REGISTER_COMPONENT(bdev_wals_null)
