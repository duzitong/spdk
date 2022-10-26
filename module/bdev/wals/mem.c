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
mem_submit_log_read_request(struct wals_target* target, void *data, uint64_t offset, uint64_t cnt, struct wals_checksum_offset checksum_offset, struct wals_bdev_io *wals_io)
{
    SPDK_DEBUGLOG(bdev_wals_mem, "log read: %ld+%ld\n", offset, cnt);
    struct wals_mem_target *mem_target = target->private_info;
    uint64_t i;
    wals_crc *checksum = (wals_crc *) mem_target->log_buf + checksum_offset.block_offset * mem_target->blocklen + checksum_offset.byte_offset;
    void *buf = mem_target->log_buf + offset * mem_target->blocklen;
    wals_crc calc_checksum;

    memcpy(data, buf, cnt * mem_target->blocklen);

    for (i = 0; i < cnt; i++) {
        calc_checksum = wals_bdev_calc_crc(buf, mem_target->blocklen);
        if (calc_checksum != *checksum) {
            wals_target_read_complete(wals_io, false);
            return 0;
        }
        buf += mem_target->blocklen;
        checksum++;
    }

    wals_target_read_complete(wals_io, true);
    return 0;
}

static int
mem_submit_core_read_request(struct wals_target* target, void *data, uint64_t offset, uint64_t cnt, struct wals_bdev_io *wals_io)
{
    SPDK_DEBUGLOG(bdev_wals_mem, "core read: %ld+%ld\n", offset, cnt);
    struct wals_mem_target *mem_target = target->private_info;
    memcpy(data, mem_target->core_buf + offset * mem_target->blocklen, cnt * mem_target->blocklen);

    wals_target_read_complete(wals_io, true);
    return 0;
}

static int
mem_submit_log_write_request(struct wals_target* target, void *data, uint64_t offset, uint64_t cnt, struct wals_bdev_io *wals_io)
{
    SPDK_DEBUGLOG(bdev_wals_mem, "log write: %ld+%ld\n", offset, cnt);
    struct wals_mem_target *mem_target = target->private_info;
    memcpy(mem_target->log_buf + offset * mem_target->blocklen, data, cnt * mem_target->blocklen);

    struct wals_metadata *metadata = data;
    SPDK_DEBUGLOG(bdev_wals_mem, "core write: %ld+%ld\n", metadata->core_offset, metadata->length);
    memcpy(mem_target->core_buf + metadata->core_offset * mem_target->blocklen, data + metadata->md_blocknum * mem_target->blocklen, metadata->length * mem_target->blocklen);

    if (offset < target->head.offset) {
        target->head.round++;
    }
    target->head.offset = offset+cnt;
    SPDK_DEBUGLOG(bdev_wals_mem, "target head updated: %ld(%ld)\n", target->head.offset, target->head.round);
    wals_target_write_complete(wals_io, true);
    return 0;
}

static int
mem_register_write_pollers(struct wals_target *target, struct wals_bdev *wals_bdev)
{
    return 0;
}

static int
mem_unregister_write_pollers(struct wals_target *target, struct wals_bdev *wals_bdev)
{
    return 0;
}

static int
mem_register_read_pollers(struct wals_target *target, struct wals_bdev *wals_bdev)
{
    return 0;
}

static int
mem_unregister_read_pollers(struct wals_target *target, struct wals_bdev *wals_bdev)
{
    return 0;
}


static struct wals_target_module g_mem_module = {
	.name = "mem",
	.start = mem_start,
	.stop = mem_stop,
    .submit_log_read_request = mem_submit_log_read_request,
	.submit_core_read_request = mem_submit_core_read_request,
	.submit_log_write_request = mem_submit_log_write_request,
    .register_write_pollers = mem_register_write_pollers,
    .unregister_write_pollers = mem_unregister_write_pollers,
    .register_read_pollers = mem_register_read_pollers,
    .unregister_read_pollers = mem_unregister_read_pollers,
};
TARGET_MODULE_REGISTER(&g_mem_module)

SPDK_LOG_REGISTER_COMPONENT(bdev_wals_mem)
