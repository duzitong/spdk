#include "bdev_wals.h"

#include "spdk/env.h"
#include "spdk/thread.h"
#include "spdk/string.h"
#include "spdk/util.h"
#include "spdk/nvme.h"
#include "spdk/rdma_connection.h"
#include "spdk/memory.h"

#include "spdk/log.h"
#include <infiniband/verbs.h>
#include <rdma/rdma_cma.h>

#define WC_BATCH_SIZE 32
// Should not exceed log_blockcnt
#define PENDING_IO_MAX_CNT 131072
#define MAX_SLICES 256
// currently a fake one, to make sure that it fully RDMA reads the struct
#define DESTAGE_INFO_CHECKSUM 666
// 10ms should be enough for 512-byte payload.
#define TIMEOUT_MS_PER_BLOCK 10

// must be greater or equal to NUM_TARGETS
#define NUM_NODES 4

enum nvmf_cli_status {
    NVMF_CLI_UNINITIALIZED,
    NVMF_CLI_INITIALIZED,
    NVMF_CLI_DISCONNECTED,
};

enum wals_io_type {
    WALS_RDMA_WRITE,
    WALS_RDMA_READ,
    WALS_NVMF_READ,
};

struct pending_io_context {
    struct wals_bdev_io* io;
    int target_id;
    uint64_t timeout_ticks;
    struct pending_io_queue* io_queue;
};

struct pending_io_queue {
    int head;
    int tail;
    int node_id;
    enum wals_io_type io_type;
    struct spdk_poller* poller;
    
    struct pending_io_context pending_ios[PENDING_IO_MAX_CNT];
};

static inline bool is_io_queue_full(struct pending_io_queue* q) {
    return (q->head + PENDING_IO_MAX_CNT - q->tail - 1) % PENDING_IO_MAX_CNT == 0;
}

static inline uint64_t calc_timeout_ticks(struct wals_bdev_io* io) {
    return spdk_get_ticks() + TIMEOUT_MS_PER_BLOCK * io->orig_io->u.bdev.num_blocks * spdk_get_ticks_hz() / 1000;
}

// TODO: separate module?
struct nvmf_cli_connection {
    struct addrinfo* server_addr;
    struct spdk_nvme_transport_id transport_id;
    enum nvmf_cli_status status;
    // when multiple slices connected to it, record the last one's tail (in blocks).
    // not trying to reuse a slice if it is deleted.
    uint64_t last_block_offset;
    int reset_failed_cnt;

    struct spdk_nvme_ctrlr* ctrlr;
    struct spdk_nvme_ns* ns;
    struct spdk_nvme_qpair* qp;
};

struct cli_rdma_context {
    uint64_t io_fail_cnt;
    struct wals_target* wals_target;
    TAILQ_HEAD(, wals_cli_slice) slices;
};

// actually should be num of data servers
struct rdma_connection* g_rdma_conns[NUM_NODES];
struct nvmf_cli_connection g_nvmf_cli_conns[NUM_NODES];
struct pending_io_queue g_pending_write_io_queue[NUM_NODES];
struct pending_io_queue g_pending_read_io_queue[NUM_NODES];
struct pending_io_queue g_pending_nvmf_read_io_queue[NUM_NODES];

struct spdk_poller* g_nvmf_connection_poller;
struct spdk_poller* g_nvmf_reconnection_poller;
struct spdk_poller* g_rdma_cq_poller;
struct spdk_poller* g_nvmf_cq_poller;
struct spdk_poller* g_destage_info_poller;
// TODO: nvmf timeout? not sure whether nvmf returns in-order or not.
struct spdk_poller* g_log_read_timeout_poller;
struct spdk_poller* g_log_write_timeout_poller;

static int g_num_slices = 0;

// TODO: rewrite it to supp
TAILQ_HEAD(, wals_cli_slice) g_slices;
struct destage_info* g_destage_tail;
struct destage_info* g_commit_tail;

struct wals_cli_slice {
    int id;
    struct wals_bdev* wals_bdev;
    // can use CONTAINEROF?
    struct wals_target* wals_target;
    struct wals_slice* wals_slice;
    struct rdma_connection* rdma_conn;
    struct nvmf_cli_connection* nvmf_conn;
    // if connected to the same ssd, then they need to share the same qp.
    uint64_t nvmf_block_offset;
    uint64_t prev_seq;
    uint64_t prev_offset;

	/* link next for rdma connections */
	TAILQ_ENTRY(wals_cli_slice)	tailq_rdma;

	/* link next for all slices connections */
	TAILQ_ENTRY(wals_cli_slice)	tailq_all_slices;
};

// static int rdma_cli_connection_poller(void* ctx);
static int nvmf_cli_connection_poller(void* ctx);
static int nvmf_cli_reconnection_poller(void* ctx);
static int rdma_cq_poller(void* ctx);
static int nvmf_cq_poller(void* ctx);
static int slice_destage_info_poller(void* ctx);
static int pending_io_timeout_poller(void* ctx);

static void target_context_created_cb(void* ctx, void* arg) {
    struct cli_rdma_context* context = ctx;
    struct wals_cli_slice* cli_slice = arg;
    TAILQ_INIT(&context->slices);
    TAILQ_INSERT_TAIL(&context->slices, cli_slice, tailq_rdma);
}

static void target_connected_cb(struct rdma_connection* rdma_conn) {
    struct cli_rdma_context* context = rdma_conn->rdma_context;

    rdma_connection_register(rdma_conn, g_destage_tail, VALUE_2MB);
    rdma_connection_register(rdma_conn, g_commit_tail, VALUE_2MB);

    struct wals_cli_slice* cli_slice = TAILQ_FIRST(&context->slices);
    rdma_connection_register(rdma_conn, cli_slice->wals_bdev->write_heap->buf, cli_slice->wals_bdev->write_heap->buf_size);
    rdma_connection_register(rdma_conn, cli_slice->wals_bdev->read_heap->buf, cli_slice->wals_bdev->read_heap->buf_size);
}

static void target_disconnect_cb(struct rdma_connection* rdma_conn) {
    struct cli_rdma_context* context = rdma_conn->rdma_context;

    struct wals_cli_slice* cli_slice;
    TAILQ_FOREACH(cli_slice, &context->slices, tailq_rdma) {
        SPDK_NOTICELOG("Slice (%d, %d) disconnected. Set the head to MAX\n",
            cli_slice->id,
            cli_slice->wals_target->node_id);
        cli_slice->wals_target->head.round = UINT64_MAX;
        cli_slice->wals_target->head.offset = UINT64_MAX;
    }
}

static bool
nvmf_cli_probe_cb(void *cb_ctx, const struct spdk_nvme_transport_id *trid,
	 struct spdk_nvme_ctrlr_opts *opts)
{
	SPDK_NOTICELOG("Attaching to %s:%s\n", trid->traddr, trid->trsvcid);

	return true;
}

static void
nvmf_cli_attach_cb(void *cb_ctx, const struct spdk_nvme_transport_id *trid,
	  struct spdk_nvme_ctrlr *ctrlr, const struct spdk_nvme_ctrlr_opts *opts)
{
	int nsid;
	struct spdk_nvme_ns *ns;
	struct wals_cli_slice* slice = (struct wals_cli_slice*)cb_ctx;

	SPDK_DEBUGLOG(bdev_wals_cli, "Attached to %s\n", trid->traddr);
	SPDK_NOTICELOG("IO queue = %d, IO request = %d\n",
		opts->io_queue_size,
		opts->io_queue_requests);
	slice->nvmf_conn->ctrlr = ctrlr;

	/*
	 * Each controller has one or more namespaces.  An NVMe namespace is basically
	 *  equivalent to a SCSI LUN.  The controller's IDENTIFY data tells us how
	 *  many namespaces exist on the controller.  For Intel(R) P3X00 controllers,
	 *  it will just be one namespace.
	 *
	 * Note that in NVMe, namespace IDs start at 1, not 0.
	 */
	int num_ns = 0;
	for (nsid = spdk_nvme_ctrlr_get_first_active_ns(ctrlr); nsid != 0;
	     nsid = spdk_nvme_ctrlr_get_next_active_ns(ctrlr, nsid)) {
		ns = spdk_nvme_ctrlr_get_ns(ctrlr, nsid);
		if (ns == NULL) {
			continue;
		}
		SPDK_NOTICELOG("Namespace ID: %d size: %juGB\n", spdk_nvme_ns_get_id(ns),
	       spdk_nvme_ns_get_size(ns) / 1000000000);
		slice->nvmf_conn->ns = ns;

		if (spdk_nvme_ns_get_sector_size(ns) != spdk_nvme_ns_get_extended_sector_size(ns)) {
			SPDK_NOTICELOG("disk sector size mismatch with extended sector size\n");
		}

        if (slice->wals_bdev->bdev.blocklen % spdk_nvme_ns_get_sector_size(ns) != 0) {
            // TODO: should fail
            SPDK_ERRLOG("Backend ssd should provide sector size of multiples of %d\n",
                spdk_nvme_ns_get_sector_size(ns));
        }

		num_ns++;
	}

	if (num_ns != 1) {
		SPDK_ERRLOG("Unexpected # of namespaces %d\n", num_ns);
	}

}

static void
nvmf_cli_remove_cb(void *cb_ctx, struct spdk_nvme_ctrlr *ctrlr)
{
    // TODO
    SPDK_NOTICELOG("removing nvmf controller\n");

}

static struct wals_target*
cli_start(struct wals_target_config *config, struct wals_bdev *wals_bdev, struct wals_slice *slice)
{
    // init g_destage_tail and g_commit_tail
    // TODO: should use atomic
    if (g_destage_tail == NULL) {
        g_destage_tail = spdk_zmalloc(VALUE_2MB, VALUE_2MB, NULL,
            SPDK_ENV_LCORE_ID_ANY, SPDK_MALLOC_DMA);
    }

    if (g_commit_tail == NULL) {
        g_commit_tail = spdk_zmalloc(VALUE_2MB, VALUE_2MB, NULL,
            SPDK_ENV_LCORE_ID_ANY, SPDK_MALLOC_DMA);
    }

    if (g_num_slices == 0) {
        // BUG: if the first slice registration failed, it will be called twice
        TAILQ_INIT(&g_slices);

        for (int i = 0; i < MAX_SLICES; i++) {
            g_destage_tail[i].checksum = DESTAGE_INFO_CHECKSUM;
            g_commit_tail[i].checksum = DESTAGE_INFO_CHECKSUM;
        }
    }
    if (wals_bdev->blocklen != wals_bdev->bdev.blocklen) {
        SPDK_ERRLOG("Only support buffer blocklen == bdev blocklen\n");
        return NULL;
    }

    int rc;
    // TODO: error case should recycle resources
    struct wals_target *target = calloc(1, sizeof(struct wals_target));
    struct wals_cli_slice *cli_slice = calloc(1, sizeof(struct wals_cli_slice));

    cli_slice->wals_bdev = wals_bdev;
    cli_slice->wals_target = target;
    cli_slice->wals_slice = slice;

	struct sockaddr_in addr;
	struct addrinfo hints = {};
	struct addrinfo* addr_res = NULL;
	hints.ai_family = AF_INET;
	hints.ai_flags = AI_PASSIVE;
    char port_buf[32];
    snprintf(port_buf, 32, "%d", config->target_log_info.port);

	getaddrinfo(config->target_log_info.address, port_buf, &hints, &addr_res);
	memcpy(&addr, addr_res->ai_addr, sizeof(addr));

    struct cli_rdma_context* rdma_context;

    // 1. use or create rdma connection
    // TODO: rdma conn api should support get-or-create ?
    for (int i = 0; i < NUM_NODES; i++) {
        if (g_rdma_conns[i] != NULL) {
            // compare ip and port
            struct sockaddr_in* cur_addr = g_rdma_conns[i]->server_addr->ai_addr;
            if (cur_addr->sin_addr.s_addr == addr.sin_addr.s_addr
                && cur_addr->sin_port == addr.sin_port) {
                SPDK_NOTICELOG("Already created rdma connection.\n");
                // TODO: MDS should provide node id.
                target->node_id = i;
                cli_slice->rdma_conn = g_rdma_conns[i];
                rdma_context = cli_slice->rdma_conn->rdma_context;
                TAILQ_INSERT_TAIL(&rdma_context->slices, cli_slice, tailq_rdma);
                break;
            }
        }
    }

    if (cli_slice->rdma_conn == NULL) {
        // no existing connection. need to build one 
        for (int i = 0; i < NUM_NODES; i++) {
            if (g_rdma_conns[i] == NULL) {
                // this slot is empty. use it
                SPDK_NOTICELOG("Using empty RDMA connection slot (aka. node id) %d\n", i);
                target->node_id = i;
                g_rdma_conns[i] = rdma_connection_alloc(false,
                    config->target_log_info.address,
                    port_buf,
                    sizeof(struct cli_rdma_context),
                    NULL,
                    0,
                    0,
                    target_context_created_cb,
                    cli_slice,
                    target_connected_cb,
                    target_disconnect_cb,
                    true);

                cli_slice->rdma_conn = g_rdma_conns[i];
                rdma_context = g_rdma_conns[i]->rdma_context;
                // while (!rdma_connection_is_connected(g_rdma_conns[i])) {
                //     rdma_connection_connect(g_rdma_conns[i]);
                // }

                break;
            }
        }

        if (cli_slice->rdma_conn == NULL) {
            // should not happen
            SPDK_ERRLOG("No empty rdma conn slot!\n");
            return NULL;
        }
    }

    int i;
    // 2. use or create nvmf connection
    for (i = 0; i < NUM_NODES; i++) {
        if (g_nvmf_cli_conns[i].status != NVMF_CLI_UNINITIALIZED) {
            // compare ip and port
            if (strcmp(g_nvmf_cli_conns[i].transport_id.traddr, config->target_core_info.address) == 0
                && atoi(g_nvmf_cli_conns[i].transport_id.trsvcid) == config->target_core_info.port) {
                SPDK_NOTICELOG("Already created nvmf connection.\n");
                cli_slice->nvmf_conn = &g_nvmf_cli_conns[i];
                break;
            }
        }
    }

    if (cli_slice->nvmf_conn == NULL) {
        for (i = 0; i < NUM_NODES; i++) {
            if (g_nvmf_cli_conns[i].status == NVMF_CLI_UNINITIALIZED) {
                SPDK_NOTICELOG("Using empty NVMf connection slot %d\n", i);
                cli_slice->nvmf_conn = &g_nvmf_cli_conns[i];
                spdk_nvme_trid_populate_transport(&g_nvmf_cli_conns[i].transport_id,
                    SPDK_NVME_TRANSPORT_RDMA);
                
                strcpy(g_nvmf_cli_conns[i].transport_id.traddr, config->target_core_info.address);
                snprintf(g_nvmf_cli_conns[i].transport_id.trsvcid, SPDK_NVMF_TRSVCID_MAX_LEN + 1, "%d", config->target_core_info.port);
                strcpy(g_nvmf_cli_conns[i].transport_id.subnqn, config->target_core_info.nqn);
                // TODO: should be in config
                g_nvmf_cli_conns[i].transport_id.adrfam = SPDK_NVMF_ADRFAM_IPV4;

                rc = spdk_nvme_probe(&g_nvmf_cli_conns[i].transport_id,
                    cli_slice,
                    nvmf_cli_probe_cb,
                    nvmf_cli_attach_cb,
                    nvmf_cli_remove_cb);

                if (rc != 0) {
                    SPDK_ERRLOG("failed to attach to nvmf\n");
                    return NULL;
                }

                cli_slice->nvmf_conn->qp = spdk_nvme_ctrlr_alloc_io_qpair(cli_slice->nvmf_conn->ctrlr,
                    NULL,
                    0);
                
                if (cli_slice->nvmf_conn->qp == NULL) {
                    SPDK_ERRLOG("failed to alloc io qp\n");
                    return NULL;
                }

                g_nvmf_cli_conns[i].status = NVMF_CLI_INITIALIZED;

                break;
            }
        }
    }

    // Should not happen
    if (i != target->node_id) {
        SPDK_ERRLOG("NVME Node ID %d mismatch with RDMA Node ID %d\n", i, target->node_id);
    }

    uint64_t multiplier = cli_slice->wals_bdev->bdev.blocklen / spdk_nvme_ns_get_sector_size(cli_slice->nvmf_conn->ns);
    SPDK_NOTICELOG("A block in buffer means %ld blocks in ssd\n", multiplier);

    uint64_t slices_in_ssd = multiplier * cli_slice->wals_bdev->slice_blockcnt;
    cli_slice->nvmf_block_offset = cli_slice->nvmf_conn->last_block_offset;
    cli_slice->nvmf_conn->last_block_offset += slices_in_ssd;

    if (cli_slice->nvmf_conn->last_block_offset > spdk_nvme_ns_get_num_sectors(cli_slice->nvmf_conn->ns)) {
        SPDK_ERRLOG("SSD capacity exceeded: requires %ld but only has %ld sectors\n",
            cli_slice->nvmf_conn->last_block_offset,
            spdk_nvme_ns_get_num_sectors(cli_slice->nvmf_conn->ns));
        return NULL;
    }

    target->log_blockcnt = cli_slice->rdma_conn->handshake_buf[1].block_cnt;
    target->private_info = cli_slice;

    cli_slice->id = g_num_slices;
    g_num_slices++;

    TAILQ_INSERT_TAIL(&g_slices, cli_slice, tailq_all_slices);

    return target;
}

static void
cli_stop(struct wals_target *target, struct wals_bdev *wals_bdev)
{
    // TODO

}

// static bool is_io_within_memory_region(void* data, uint64_t cnt, uint64_t blocklen, struct ibv_mr* mr) {
//     return (data >= mr->addr
//         && data < mr->addr + mr->length
//         && data + cnt * blocklen >= mr->addr
//         && data + cnt * blocklen < mr->addr + mr->length);
// }

static int
cli_submit_log_read_request(struct wals_target* target, void *data, uint64_t offset, uint64_t cnt, struct wals_checksum_offset checksum_offset, struct wals_bdev_io *wals_io)
{
    // SPDK_NOTICELOG("Reading from log %ld %ld\n", offset, cnt);
    // BUG: the reconnection poller must be in the same thread as the IO thread, 
    // otherwise it may secretly try to reconnect and free the RDMA resources, causing 
    // segfault.
    // Workaround: make free RDMA resources async; do reconnection and switch it in async
    int rc;
    struct wals_cli_slice* slice = target->private_info;
    struct pending_io_queue* io_queue = &g_pending_read_io_queue[target->node_id];
    pthread_rwlock_rdlock(&slice->rdma_conn->lock);

    if (!rdma_connection_is_connected(slice->rdma_conn)) {
        wals_target_read_complete(wals_io, false);
        goto end;
    }

    if (is_io_queue_full(io_queue)) {
        SPDK_ERRLOG("Should not happen: rdma read io queue is full\n");
        wals_target_read_complete(wals_io, false);
        goto end;
    }

    if (offset + cnt > slice->rdma_conn->handshake_buf[1].block_cnt) {
        SPDK_ERRLOG("READ out of remote PMEM range: %ld %ld\n", offset, cnt);
        wals_target_read_complete(wals_io, false);
        goto end;
    }

    struct ibv_send_wr wr, *bad_wr = NULL;
    struct ibv_sge sge;
    memset(&wr, 0, sizeof(wr));
    wr.wr_id = (uint64_t)&io_queue->pending_ios[io_queue->tail];
    wr.next = NULL;
    // TODO: inline?
    wr.send_flags = IBV_SEND_SIGNALED;
    wr.opcode = IBV_WR_RDMA_READ;
    wr.num_sge = 1;
    wr.sg_list = &sge;
    wr.wr.rdma.remote_addr = (uint64_t)slice->rdma_conn->handshake_buf[1].base_addr + offset * slice->wals_bdev->blocklen;
    wr.wr.rdma.rkey = slice->rdma_conn->handshake_buf[1].rkey;

    rdma_connection_construct_sge(slice->rdma_conn,
        &sge,
        data,
        cnt * slice->wals_bdev->blocklen);

    // SPDK_NOTICELOG("READ %p %p %p %p %ld %ld %ld %ld\n", wals_io, data, data + sge.length, wr.wr.rdma.remote_addr,
    //     cnt, offset, slice->wals_bdev->blocklen, sge.lkey);

    io_queue->pending_ios[io_queue->tail] =
    (struct pending_io_context) {
        .io = wals_io,
        .target_id = target->target_id,
        .timeout_ticks = calc_timeout_ticks(wals_io),
        .io_queue = io_queue
    };

    io_queue->tail = (io_queue->tail + 1) % PENDING_IO_MAX_CNT;
    
    rc = ibv_post_send(slice->rdma_conn->cm_id->qp, &wr, &bad_wr);
    if (rc != 0) {
        SPDK_ERRLOG("RDMA read failed with errno = %d\n", rc);
        SPDK_NOTICELOG("Local: %p %d; Remote: %p %d; Len = %d\n",
            (void*)sge.addr, sge.lkey, (void*)wr.wr.rdma.remote_addr, wr.wr.rdma.rkey,
            sge.length);
        wals_target_read_complete(wals_io, false);
        io_queue->tail = (io_queue->tail + PENDING_IO_MAX_CNT - 1) % PENDING_IO_MAX_CNT;
        goto end;
    }

end:
    pthread_rwlock_unlock(&slice->rdma_conn->lock);
    return 0;
}

static void nvmf_read_done(void *ref, const struct spdk_nvme_cpl *cpl) {
	struct pending_io_context *io_context = ref;
    struct pending_io_queue* io_queue = io_context->io_queue;

    if (io_context->io == io_queue->pending_ios[io_queue->head].io) {
        // it is the latest one
        if (spdk_nvme_cpl_is_success(cpl)) {
            wals_target_read_complete(io_context->io, true);
        }
        else {
            SPDK_ERRLOG("NVMf client failed to read from remote SSD\n");
            wals_target_read_complete(io_context->io, false);
        }
        io_queue->head = (io_queue->head + 1) % PENDING_IO_MAX_CNT;
    }
    else {
        SPDK_NOTICELOG("Ignoring io (%p, %d, %d) due to already timeout\n",
            io_context->io, io_queue->node_id, io_context->target_id);
    }
}

static void rdma_operation_done(void* arg, bool success) {
    struct pending_io_context* io_context = arg;
    struct pending_io_queue* io_queue = io_context->io_queue;

    if (io_context->io == io_queue->pending_ios[io_queue->head].io) {
        switch (io_queue->io_type) {
            case WALS_RDMA_READ:
                wals_target_read_complete(io_context->io, success);
                break;
            case WALS_RDMA_WRITE:
                wals_target_write_complete(io_context->io, success, io_context->target_id);
                break;
            default:
                SPDK_ERRLOG("Code bug. IO type %d should be handled by other function\n", io_queue->io_type);
                break;
        }
        io_queue->head = (io_queue->head + 1) % PENDING_IO_MAX_CNT;
    }
    else {
        SPDK_NOTICELOG("Ignoring io (%p, %d, %d) due to already timeout\n",
            io_context->io, io_queue->node_id, io_context->target_id);
    }
}

static void rdma_operation_success(void* arg) {
    rdma_operation_done(arg, true);
}

static void rdma_operation_failure(void* arg) {
    rdma_operation_done(arg, false);
}

static int
cli_submit_core_read_request(struct wals_target* target, void *data, uint64_t offset, uint64_t cnt, struct wals_bdev_io *wals_io)
{
    // SPDK_NOTICELOG("Reading from disk %ld %ld\n", offset, cnt);
    int rc;
    struct wals_cli_slice* slice = target->private_info;
    uint64_t multiplier = slice->wals_bdev->bdev.blocklen / spdk_nvme_ns_get_sector_size(slice->nvmf_conn->ns);
    struct pending_io_queue* io_queue = &g_pending_nvmf_read_io_queue[target->node_id];

    if (is_io_queue_full(io_queue)) {
        SPDK_ERRLOG("Should not happen: nvmf read io queue is full\n");
        wals_target_read_complete(wals_io, false);
        return 0;
    }

    if (g_nvmf_cli_conns[target->node_id].status != NVMF_CLI_INITIALIZED) {
        wals_target_read_complete(wals_io, false);
        return 0;
    }

    io_queue->pending_ios[io_queue->tail] =
    (struct pending_io_context) {
        .io = wals_io,
        .timeout_ticks = calc_timeout_ticks(wals_io),
        .io_queue = io_queue
    };

    io_queue->tail = (io_queue->tail + 1) % PENDING_IO_MAX_CNT;

    rc = spdk_nvme_ns_cmd_read(slice->nvmf_conn->ns,
        slice->nvmf_conn->qp,
        data,
        slice->nvmf_block_offset + offset * multiplier,
        cnt * multiplier,
        nvmf_read_done,
        &io_queue->pending_ios[io_queue->tail],
        0);

    if (rc != 0) {
        SPDK_ERRLOG("NVMf read io failed with rc = %d\n", rc);
        wals_target_read_complete(wals_io, false);
        io_queue->tail = (io_queue->tail + PENDING_IO_MAX_CNT - 1) % PENDING_IO_MAX_CNT;
        return 0;
    }

    return 0;
}

static int
cli_submit_log_write_request(struct wals_target* target, void *data, uint64_t offset, uint64_t cnt, struct wals_bdev_io *wals_io)
{
    // SPDK_NOTICELOG("Writing to log %ld %ld\n", offset, cnt);
    int rc;
    struct wals_cli_slice* slice = target->private_info;
    struct pending_io_queue* io_queue = &g_pending_write_io_queue[target->node_id];
    pthread_rwlock_rdlock(&slice->rdma_conn->lock);

    if (!rdma_connection_is_connected(slice->rdma_conn)) {
        wals_target_write_complete(wals_io, false, target->target_id);
        goto end;
    }

    if (is_io_queue_full(io_queue)) {
        SPDK_ERRLOG("Should not happen: rdma write io queue is full\n");
        wals_target_write_complete(wals_io, false, target->target_id);
        goto end;
    }

    if (offset + cnt > slice->rdma_conn->handshake_buf[1].block_cnt) {
        SPDK_ERRLOG("Write out of remote PMEM range: %ld %ld\n", offset, cnt);
        wals_target_write_complete(wals_io, false, target->target_id);
        goto end;
    }

    struct wals_metadata* metadata = data;
    if (metadata->seq != slice->prev_seq + 1) {
        SPDK_NOTICELOG("Seq jumped from %ld to %ld. It is expected when the data node reconnects.\n",
            slice->prev_seq,
            metadata->seq);
    }
    if (offset != slice->prev_offset) {
        if (offset != 0) {
            SPDK_NOTICELOG("Offset jumped from %ld to %ld.\n", slice->prev_offset, offset);
        }
    }
    slice->prev_seq = metadata->seq;
    slice->prev_offset = offset + cnt;
    
    // SPDK_NOTICELOG("Sending md %ld %ld %ld %ld %ld %ld to slice %d\n",
    //     metadata->version,
    //     metadata->seq,
    //     metadata->next_offset,
    //     metadata->round,
    //     metadata->length,
    //     metadata->core_offset,
    //     slice->id);

	struct ibv_send_wr wr, *bad_wr = NULL;
	struct ibv_sge sge;
	memset(&wr, 0, sizeof(wr));
	wr.wr_id = (uint64_t)&io_queue->pending_ios[io_queue->tail];
	wr.next = NULL;
	wr.send_flags = IBV_SEND_SIGNALED;
	wr.opcode = IBV_WR_RDMA_WRITE;
	wr.num_sge = 1;
	wr.sg_list = &sge;
	wr.wr.rdma.remote_addr = (uint64_t)slice->rdma_conn->handshake_buf[1].base_addr + offset * wals_io->wals_bdev->blocklen;
	wr.wr.rdma.rkey = slice->rdma_conn->handshake_buf[1].rkey;

    rdma_connection_construct_sge(slice->rdma_conn,
        &sge,
        data,
        cnt * slice->wals_bdev->blocklen);

    io_queue->pending_ios[io_queue->tail] =
    (struct pending_io_context) {
        .io = wals_io,
        .target_id = target->target_id,
        .timeout_ticks = calc_timeout_ticks(wals_io),
        .io_queue = io_queue
    };

    io_queue->tail = (io_queue->tail + 1) % PENDING_IO_MAX_CNT;

	rc = ibv_post_send(slice->rdma_conn->cm_id->qp, &wr, &bad_wr);
	if (rc != 0) {
		SPDK_ERRLOG("RDMA write failed with errno = %d\n", rc);
		SPDK_NOTICELOG("Local: %p %d; Remote: %p %d; Len = %d\n",
			(void*)sge.addr, sge.lkey, (void*)wr.wr.rdma.remote_addr, wr.wr.rdma.rkey,
			sge.length);
		wals_target_write_complete(wals_io, false, target->target_id);
        io_queue->tail = (io_queue->tail + PENDING_IO_MAX_CNT - 1) % PENDING_IO_MAX_CNT;
        goto end;
	}

end:
    pthread_rwlock_unlock(&slice->rdma_conn->lock);
    return 0;
}

static int
cli_register_write_pollers(struct wals_target *target, struct wals_bdev *wals_bdev)
{
    // NOTE: may need different pollers for each qp in the future
    if (g_rdma_cq_poller == NULL) {
        g_rdma_cq_poller = SPDK_POLLER_REGISTER(rdma_cq_poller, wals_bdev, 0);
    }

    for (int node_id = 0; node_id < NUM_NODES; node_id++) {
        if (g_pending_write_io_queue[node_id].poller == NULL) {
            g_pending_write_io_queue[node_id].node_id = node_id;
            g_pending_write_io_queue[node_id].io_type = WALS_RDMA_WRITE;
            g_pending_write_io_queue[node_id].poller = SPDK_POLLER_REGISTER(
                pending_io_timeout_poller,
                &g_pending_write_io_queue[node_id],
                1000);
        }
    }

    return 0;
}

static int
cli_unregister_write_pollers(struct wals_target *target, struct wals_bdev *wals_bdev)
{
    if (g_rdma_cq_poller != NULL) {
        spdk_poller_unregister(&g_rdma_cq_poller);
    }

    for (int node_id = 0; node_id < NUM_NODES; node_id++) {
        if (g_pending_write_io_queue[node_id].poller != NULL) {
            spdk_poller_unregister(&g_pending_write_io_queue[node_id].poller);
        }
    }

    // TODO: make it in rdma_connection.c
    for (int node_id = 0; node_id < NUM_NODES; node_id++) {
        spdk_poller_unregister(&g_rdma_conns[node_id]->connect_poller);
        spdk_poller_unregister(&g_rdma_conns[node_id]->reconnect_poller);
    }
    return 0;
}

static int
cli_register_read_pollers(struct wals_target *target, struct wals_bdev *wals_bdev)
{
    if (g_nvmf_cq_poller == NULL) {
        g_nvmf_cq_poller = SPDK_POLLER_REGISTER(nvmf_cq_poller, wals_bdev, 0);
    }
    if (g_destage_info_poller == NULL) {
        g_destage_info_poller = SPDK_POLLER_REGISTER(slice_destage_info_poller, NULL, 2000);
    }

    if (g_nvmf_connection_poller == NULL) {
        g_nvmf_connection_poller = SPDK_POLLER_REGISTER(nvmf_cli_connection_poller, wals_bdev, 5 * 1000);
    }

    if (g_nvmf_reconnection_poller == NULL) {
        g_nvmf_reconnection_poller = SPDK_POLLER_REGISTER(nvmf_cli_reconnection_poller, wals_bdev, 5 * 1000 * 1000);
    }

    for (int node_id = 0; node_id < NUM_NODES; node_id++) {
        if (g_pending_read_io_queue[node_id].poller == NULL) {
            g_pending_read_io_queue[node_id].node_id = node_id;
            g_pending_read_io_queue[node_id].io_type = WALS_RDMA_READ;
            g_pending_read_io_queue[node_id].poller = SPDK_POLLER_REGISTER(
                pending_io_timeout_poller,
                &g_pending_read_io_queue[node_id],
                1000);
        }
        if (g_pending_nvmf_read_io_queue[node_id].poller == NULL) {
            g_pending_nvmf_read_io_queue[node_id].node_id = node_id;
            g_pending_nvmf_read_io_queue[node_id].io_type = WALS_NVMF_READ;
            g_pending_nvmf_read_io_queue[node_id].poller = SPDK_POLLER_REGISTER(
                pending_io_timeout_poller,
                &g_pending_nvmf_read_io_queue[node_id],
                1000);
        }
    }
    return 0;
}

static int
cli_unregister_read_pollers(struct wals_target *target, struct wals_bdev *wals_bdev)
{
    if (g_nvmf_cq_poller != NULL) {
        spdk_poller_unregister(&g_nvmf_cq_poller);
    }
    if (g_destage_info_poller != NULL) {
        spdk_poller_unregister(&g_destage_info_poller);
    }
    if (g_nvmf_connection_poller != NULL) {
        spdk_poller_unregister(&g_nvmf_connection_poller);
    }
    if (g_nvmf_reconnection_poller != NULL) {
        spdk_poller_unregister(&g_nvmf_reconnection_poller);
    }

    for (int node_id = 0; node_id < NUM_NODES; node_id++) {
        if (g_pending_read_io_queue[node_id].poller != NULL) {
            spdk_poller_unregister(&g_pending_read_io_queue[node_id].poller);
        }
        if (g_pending_nvmf_read_io_queue[node_id].poller != NULL) {
            spdk_poller_unregister(&g_pending_nvmf_read_io_queue[node_id].poller);
        }
    }
    return 0;
}

static int
cli_diagnose_unregister_read_pollers(struct wals_target *target, struct wals_bdev *wals_bdev) {
    spdk_poller_unregister(&g_destage_info_poller);
    return 0;
}

static int
cli_diagnose_unregister_write_pollers(struct wals_target *target, struct wals_bdev *wals_bdev) {
    return 0;
}

// WRITE THREAD
// static int
// rdma_cli_connection_poller(void* ctx) {
//     for (int i = 0; i < NUM_NODES; i++) {
//         if (g_rdma_conns[i] != NULL) {
//             rdma_connection_connect(g_rdma_conns[i]);
//         }
//     }
// }

// READ THREAD
// same logic as the poller in examples/nvme/reconnect.c
// assume that the namespace and io qpair don't need recreation.
static int 
nvmf_cli_connection_poller(void* ctx) {
    // TODO: do I need pthread_setcancelstate?
    int rc;
    enum spdk_thread_poller_rc poller_rc = SPDK_POLLER_IDLE;
    for (int i = 0; i < NUM_NODES; i++) {
        if (g_nvmf_cli_conns[i].status == NVMF_CLI_INITIALIZED) {
            rc = spdk_nvme_ctrlr_process_admin_completions(g_nvmf_cli_conns[i].ctrlr);
            if (rc == -ENXIO) {
                g_nvmf_cli_conns[i].status = NVMF_CLI_DISCONNECTED;
            }
            poller_rc = SPDK_POLLER_BUSY;
        }
    }
    return poller_rc;
}

// READ THREAD
static int 
nvmf_cli_reconnection_poller(void* ctx) {
    int rc;
    enum spdk_thread_poller_rc poller_rc = SPDK_POLLER_IDLE;
    for (int i = 0; i < NUM_NODES; i++) {
        if (g_nvmf_cli_conns[i].status == NVMF_CLI_DISCONNECTED) {
            // assume that the transport id doesn't change
            rc = spdk_nvme_ctrlr_reset(g_nvmf_cli_conns[i].ctrlr);
            if (rc != 0) {
                g_nvmf_cli_conns[i].reset_failed_cnt++;
                // SPDK_NOTICELOG("Cannot reset nvmf ctrlr %d: %d\n", i, rc);
            }
            else {
                SPDK_NOTICELOG("Nvmf ctrlr %d reset successfully\n", i);
                int nsid = spdk_nvme_ctrlr_get_first_active_ns(g_nvmf_cli_conns[i].ctrlr);
                g_nvmf_cli_conns[i].ns = spdk_nvme_ctrlr_get_ns(g_nvmf_cli_conns[i].ctrlr, nsid);
                rc = spdk_nvme_ctrlr_reconnect_io_qpair(g_nvmf_cli_conns[i].qp);
                if (rc != 0) {
                    SPDK_ERRLOG("qp should not fail to reconnect when ctrlr is good: %d\n", rc);
                }
                g_nvmf_cli_conns[i].reset_failed_cnt = 0;
                g_nvmf_cli_conns[i].status = NVMF_CLI_INITIALIZED;
            }
            poller_rc = SPDK_POLLER_BUSY;
        }

    }
    return poller_rc;

}

// READ THREAD
static int slice_destage_info_poller(void* ctx) {
    // the first struct is the destage tail of the target node that the CLI should 
    // RDMA read.
    // the second struct is the commit tail of the slice that the CLI should RDMA write.
    struct wals_cli_slice* cli_slice;
    TAILQ_FOREACH(cli_slice, &g_slices, tailq_all_slices) {
        if (!rdma_connection_is_connected(cli_slice->rdma_conn)) {
            continue;
        }

        struct rdma_handshake* remote_handshake = &cli_slice->rdma_conn->handshake_buf[1];
        struct cli_rdma_context* rdma_context = cli_slice->rdma_conn->rdma_context;
        // 1. read destage tail
        if (g_destage_tail[cli_slice->id].checksum == DESTAGE_INFO_CHECKSUM) {
            struct ibv_send_wr read_wr, *bad_read_wr = NULL;
            struct ibv_sge read_sge;
            memset(&read_wr, 0, sizeof(read_wr));

            read_wr.wr_id = 0;
            read_wr.opcode = IBV_WR_RDMA_READ;
            read_wr.sg_list = &read_sge;
            read_wr.num_sge = 1;
            read_wr.send_flags = IBV_SEND_SIGNALED;
            read_wr.wr.rdma.remote_addr = (uint64_t)remote_handshake->base_addr + remote_handshake->block_cnt * remote_handshake->block_size;
            read_wr.wr.rdma.rkey = remote_handshake->rkey;

            pthread_rwlock_rdlock(&cli_slice->rdma_conn->lock);
            rdma_connection_construct_sge(cli_slice->rdma_conn,
                &read_sge,
                &g_destage_tail[cli_slice->id],
                sizeof(struct destage_info));

            int rc = ibv_post_send(cli_slice->rdma_conn->cm_id->qp, &read_wr, &bad_read_wr);
            if (rc != 0) {
                SPDK_ERRLOG("post send read failed for slice %d: error %d\n",
                    cli_slice->id,
                    rc);
            }
            pthread_rwlock_unlock(&cli_slice->rdma_conn->lock);
        } 
        else if (g_destage_tail[cli_slice->id].checksum == 0) {
            // RDMA read is successful
            // update and set the checksum back
            // SPDK_NOTICELOG("Read destage tail %ld %ld\n", g_destage_tail[cli_slice->id].offset,
            //     g_destage_tail[cli_slice->id].round);
            cli_slice->wals_target->head.offset = g_destage_tail[cli_slice->id].offset;
            cli_slice->wals_target->head.round = g_destage_tail[cli_slice->id].round;
            g_destage_tail[cli_slice->id].checksum = DESTAGE_INFO_CHECKSUM;
        }
        else {
            SPDK_ERRLOG("Should not happen\n");
        }

        // 2. write commit tail
        g_commit_tail[cli_slice->id].offset = cli_slice->wals_slice->committed_tail.offset;
        g_commit_tail[cli_slice->id].round = cli_slice->wals_slice->committed_tail.round;
        struct ibv_send_wr write_wr, *bad_write_wr = NULL;
        struct ibv_sge write_sge;
        memset(&write_wr, 0, sizeof(write_wr));

        write_wr.wr_id = 0;
        write_wr.opcode = IBV_WR_RDMA_WRITE;
        write_wr.sg_list = &write_sge;
        write_wr.num_sge = 1;
        write_wr.send_flags = IBV_SEND_SIGNALED;
        write_wr.wr.rdma.remote_addr =
            (uint64_t)remote_handshake->base_addr + \
            remote_handshake->block_cnt * remote_handshake->block_size + \
            sizeof(struct destage_info);
        write_wr.wr.rdma.rkey = remote_handshake->rkey;

        pthread_rwlock_rdlock(&cli_slice->rdma_conn->lock);
        rdma_connection_construct_sge(cli_slice->rdma_conn,
            &write_sge,
            &g_commit_tail[cli_slice->id],
            sizeof(struct destage_info));

        int rc = ibv_post_send(cli_slice->rdma_conn->cm_id->qp, &write_wr, &bad_write_wr);
        if (rc != 0) {
            SPDK_ERRLOG("post send write failed for slice %d: error %d\n",
                cli_slice->id,
                rc);
        }
        pthread_rwlock_unlock(&cli_slice->rdma_conn->lock);
    }
    return SPDK_POLLER_BUSY;
}

// WRITE THREAD
static int
rdma_cq_poller(void* ctx) {
    struct ibv_wc wc_buf[WC_BATCH_SIZE];
    struct wals_bdev* wals_bdev = ctx;
    enum spdk_thread_poller_rc poller_rc = SPDK_POLLER_IDLE;
    spdk_trace_record_tsc(spdk_get_ticks(), TRACE_WALS_S_RDMA_CQ, 0, 0, (uintptr_t)ctx);
    for (int node_id = 0; node_id < NUM_NODES; node_id++) {
        struct cli_rdma_context* rdma_context = g_rdma_conns[node_id]->rdma_context;
        if (rdma_connection_is_connected(g_rdma_conns[node_id])) {
            if (pthread_rwlock_rdlock(&g_rdma_conns[node_id]->lock) != 0) {
                SPDK_ERRLOG("Failed to acquire read lock\n");
            }
            int cnt = ibv_poll_cq(g_rdma_conns[node_id]->cq, WC_BATCH_SIZE, wc_buf);
            if (pthread_rwlock_unlock(&g_rdma_conns[node_id]->lock) != 0) {
                SPDK_ERRLOG("Failed to release read lock\n");
            }
            if (cnt < 0) {
                // hopefully the reconnection poller will spot the error and try to reconnect
				SPDK_ERRLOG("ibv_poll_cq failed\n");
            }
            else if (cnt > 0) {
                poller_rc = SPDK_POLLER_BUSY;
                for (int j = 0; j < cnt; j++) {
                    bool success = true;
					struct pending_io_context* io_context = (void*)wc_buf[j].wr_id;
                    if (wc_buf[j].wr_id == 0) {
                        // special case: the wr is for reading destage tail or writing commit tail.
                        // do nothing
                        continue;
                    }

					if (wc_buf[j].status != IBV_WC_SUCCESS) {
                        SPDK_NOTICELOG("IO (%p, %p, %d, %d) RDMA op %d failed with status %d\n",
                            io_context,
                            io_context->io,
                            node_id,
                            io_context->target_id,
                            wc_buf[j].opcode,
                            wc_buf[j].status);
                        
                        rdma_context->io_fail_cnt++;
                        success = false;
					}

                    spdk_msg_fn cb_fn = success ? rdma_operation_success : rdma_operation_failure;

                    // if (wc_buf[j].opcode == IBV_WC_RDMA_READ) {
                    if (io_context->io_queue->io_type == WALS_RDMA_READ) {
                        // if (io_context->io_queue->io_type == WALS_RDMA_WRITE) {
                        //     SPDK_ERRLOG("Send write IO to read thread\n");
                        // }
                        spdk_thread_send_msg(
                            wals_bdev->read_thread,
                            cb_fn,
                            io_context);
                    }
                    // else if (wc_buf[j].opcode == IBV_WC_RDMA_WRITE) {
                    else if (io_context->io_queue->io_type == WALS_RDMA_WRITE) {
                        // we are already in write thread.
                        cb_fn(io_context);
                    }
                    else {
                        SPDK_NOTICELOG("Unsupported IO Type %d\n", io_context->io_queue->io_type);
                    }
                }
            }
        }
    }

    spdk_trace_record_tsc(spdk_get_ticks(), TRACE_WALS_F_RDMA_CQ, 0, 0, (uintptr_t)ctx);
    return poller_rc;
}

// READ THREAD
static int
nvmf_cq_poller(void* ctx) {
    int rc;
    enum spdk_thread_poller_rc poller_rc = SPDK_POLLER_IDLE;
    for (int i = 0; i < NUM_NODES; i++) {
        if (g_nvmf_cli_conns[i].status == NVMF_CLI_INITIALIZED) {
            rc = spdk_nvme_qpair_process_completions(g_nvmf_cli_conns[i].qp, 0);
            if (rc < 0) {
                // NXIO is expected when the connection is down.
                if (rc != -ENXIO) {
                    SPDK_ERRLOG("NVMf request failed for conn %d: %d\n", i, rc);
                }
            }
            else if (rc > 0) {
                poller_rc = SPDK_POLLER_BUSY;
            }
        }
    }
    return poller_rc;
}

// READ/WRITE THREAD
static int 
pending_io_timeout_poller(void* ctx) {
    enum spdk_thread_poller_rc poller_rc = SPDK_POLLER_IDLE;
    struct pending_io_queue* io_queue = ctx;

    uint64_t current_ticks = spdk_get_ticks();
    int j;
    for (j = io_queue->head;
        j != io_queue->tail;
        j = (j + 1) % PENDING_IO_MAX_CNT) {
        struct pending_io_context* io_context = &io_queue->pending_ios[j];
        struct wals_bdev_io* io = io_context->io;
        uint64_t timeout_ticks = io_context->timeout_ticks;

        if (timeout_ticks < current_ticks) {
            // timeout.
            poller_rc = SPDK_POLLER_BUSY;
            SPDK_NOTICELOG("IO (%p, %p, %p, %d, %d, %ld, %d, %d, %d) timeout\n",
                io,
                io->orig_io,
                io_queue,
                io_queue->node_id,
                io_queue->io_type,
                io->orig_io->u.bdev.num_blocks,
                io_queue->head,
                io_queue->tail,
                j);
            switch (io_queue->io_type) {
                case WALS_RDMA_WRITE:
                    wals_target_write_complete(io, false, io_context->target_id);
                    break;
                case WALS_RDMA_READ:
                case WALS_NVMF_READ:
                    wals_target_read_complete(io, false);
                    break;
            }
        }
        else {
            break;
        }
    }
    io_queue->head = j;

    return poller_rc;
}

static struct wals_target_module g_rdma_cli_module = {
	.name = "cli",
	.start = cli_start,
	.stop = cli_stop,
    .submit_log_read_request = cli_submit_log_read_request,
	.submit_core_read_request = cli_submit_core_read_request,
	.submit_log_write_request = cli_submit_log_write_request,
    .register_write_pollers = cli_register_write_pollers,
    .unregister_write_pollers = cli_unregister_write_pollers,
    .register_read_pollers = cli_register_read_pollers,
    .unregister_read_pollers = cli_unregister_read_pollers,
    .diagnose_unregister_read_pollers = cli_diagnose_unregister_read_pollers,
    .diagnose_unregister_write_pollers = cli_diagnose_unregister_write_pollers,
};

TARGET_MODULE_REGISTER(&g_rdma_cli_module)

SPDK_LOG_REGISTER_COMPONENT(bdev_wals_cli)
