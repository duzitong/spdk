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
#define TIMEOUT_MS 10

enum nvmf_cli_status {
    NVMF_CLI_UNINITIALIZED,
    NVMF_CLI_INITIALIZED,
    NVMF_CLI_ERROR,
};

struct pending_io_context {
    struct wals_bdev_io* io;
    uint64_t ticks;
};

struct pending_io_queue {
    int head;
    int tail;
    struct pending_io_context pending_ios[PENDING_IO_MAX_CNT];
};

static inline bool is_io_queue_full(struct pending_io_queue* q) {
    return (q->head + PENDING_IO_MAX_CNT - q->tail - 1) % PENDING_IO_MAX_CNT == 0;
}

// TODO: separate module?
struct nvmf_cli_connection {
    struct addrinfo* server_addr;
    struct spdk_nvme_transport_id transport_id;
    enum nvmf_cli_status status;
    // when multiple slices connected to it, record the last one's tail (in blocks).
    // not trying to reuse a slice if it is deleted.
    uint64_t last_block_offset;

    struct spdk_nvme_ctrlr* ctrlr;
    struct spdk_nvme_ns* ns;
    struct spdk_nvme_qpair* qp;
};

struct slice_rdma_context {
    struct pending_io_queue pending_read_io_queue;
    struct pending_io_queue pending_write_io_queue;
    struct pending_io_queue pending_nvmf_read_io_queue;
    uint64_t io_fail_cnt;
    uint64_t reconnect_cnt;
    TAILQ_HEAD(, wals_cli_slice) slices;
};


// actually should be num of servers
struct rdma_connection* g_rdma_conns[NUM_TARGETS];
struct nvmf_cli_connection g_nvmf_cli_conns[NUM_TARGETS];

struct spdk_poller* g_rdma_connection_poller;
struct spdk_poller* g_nvmf_connection_poller;
struct spdk_poller* g_rdma_cq_poller;
struct spdk_poller* g_nvmf_cq_poller;
struct spdk_poller* g_destage_info_poller;
// TODO: nvmf timeout? not sure whether nvmf returns in-order or not.
struct spdk_poller* g_log_read_timeout_poller;
struct spdk_poller* g_log_write_timeout_poller;


int g_num_slices = 0;

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

static int rdma_cli_connection_poller(void* ctx);
static int nvmf_cli_connection_poller(void* ctx);
static int rdma_cq_poller(void* ctx);
static int nvmf_cq_poller(void* ctx);
static int slice_destage_info_poller(void* ctx);

void target_connected_cb(void *cb_ctx, struct rdma_connection* rdma_conn) {
    struct slice_rdma_context* context = cb_ctx;

    rdma_connection_register(rdma_conn, g_destage_tail, VALUE_2MB);
    rdma_connection_register(rdma_conn, g_commit_tail, VALUE_2MB);

    struct wals_cli_slice* cli_slice = TAILQ_FIRST(&context->slices);
    rdma_connection_register(rdma_conn, cli_slice->wals_bdev->write_heap->buf, cli_slice->wals_bdev->write_heap->buf_size);
    rdma_connection_register(rdma_conn, cli_slice->wals_bdev->read_heap->buf, cli_slice->wals_bdev->read_heap->buf_size);
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

    struct slice_rdma_context* rdma_context;

    // 1. use or create rdma connection
    // TODO: rdma conn api should support get-or-create ?
    for (int i = 0; i < NUM_TARGETS; i++) {
        if (g_rdma_conns[i] != NULL) {
            // compare ip and port
            struct sockaddr_in* cur_addr = g_rdma_conns[i]->server_addr->ai_addr;
            if (cur_addr->sin_addr.s_addr == addr.sin_addr.s_addr
                && cur_addr->sin_port == addr.sin_port) {
                SPDK_NOTICELOG("Already created rdma connection.\n");
                cli_slice->rdma_conn = g_rdma_conns[i];
                rdma_context = cli_slice->rdma_conn->rdma_context;
                TAILQ_INSERT_TAIL(&rdma_context->slices, cli_slice, tailq_rdma);
                break;
            }
        }
    }

    if (cli_slice->rdma_conn == NULL) {
        // no existing connection. need to build one 
        for (int i = 0; i < NUM_TARGETS; i++) {
            if (g_rdma_conns[i] == NULL) {
                // this slot is empty. use it
                SPDK_NOTICELOG("Using empty RDMA connection slot %d\n", i);
                g_rdma_conns[i] = rdma_connection_alloc(false,
                    config->target_log_info.address,
                    port_buf,
                    sizeof(struct slice_rdma_context),
                    NULL,
                    0,
                    0,
                    target_connected_cb);
                cli_slice->rdma_conn = g_rdma_conns[i];
                rdma_context = g_rdma_conns[i]->rdma_context;
                TAILQ_INIT(&rdma_context->slices);
                TAILQ_INSERT_TAIL(&rdma_context->slices, cli_slice, tailq_rdma);

                // only need one poller for connecting the qps
                if (g_rdma_connection_poller == NULL) {
                    g_rdma_connection_poller = SPDK_POLLER_REGISTER(rdma_cli_connection_poller, wals_bdev, 5 * 1000);
                }

                while (!rdma_connection_is_connected(g_rdma_conns[i])) {
                    rdma_connection_connect(g_rdma_conns[i]);
                }

                break;
            }
        }

        if (cli_slice->rdma_conn == NULL) {
            // should not happen
            SPDK_ERRLOG("No empty rdma conn slot!\n");
            return NULL;
        }
    }

    // 2. use or create nvmf connection
    for (int i = 0; i < NUM_TARGETS; i++) {
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
        for (int i = 0; i < NUM_TARGETS; i++) {
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

                if (g_nvmf_connection_poller == NULL) {
                    g_nvmf_connection_poller = SPDK_POLLER_REGISTER(nvmf_cli_connection_poller, wals_bdev, 5 * 1000);
                }

                break;
            }
        }
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
    if (!rdma_connection_is_connected(slice->rdma_conn)) {
        wals_target_read_complete(wals_io, false);
        return 0;
    }

    // if (!is_io_within_memory_region(data, cnt, slice->wals_bdev->blocklen, slice->rdma_conn->mr_read)) {
    //     SPDK_ERRLOG("IO out of MR range\n");
    //     wals_target_read_complete(wals_io, false);
    //     return 0;
    // }

    if (offset + cnt > slice->rdma_conn->handshake_buf[1].block_cnt) {
        SPDK_ERRLOG("READ out of remote PMEM range: %ld %ld\n", offset, cnt);
        wals_target_read_complete(wals_io, false);
        return 0;
    }

    struct ibv_send_wr wr, *bad_wr = NULL;
    struct ibv_sge sge;
    memset(&wr, 0, sizeof(wr));
    wr.wr_id = (uint64_t)wals_io;
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

    SPDK_NOTICELOG("READ %p %p %p %p %ld %ld %ld %ld\n", wals_io, data, data + sge.length, wr.wr.rdma.remote_addr,
        cnt, offset, slice->wals_bdev->blocklen, sge.lkey);
    
    rc = ibv_post_send(slice->rdma_conn->cm_id->qp, &wr, &bad_wr);
    if (rc != 0) {
        SPDK_ERRLOG("RDMA read failed with errno = %d\n", rc);
        SPDK_NOTICELOG("Local: %p %d; Remote: %p %d; Len = %d\n",
            (void*)sge.addr, sge.lkey, (void*)wr.wr.rdma.remote_addr, wr.wr.rdma.rkey,
            sge.length);
        wals_target_read_complete(wals_io, false);
    }

    return 0;
}

static void cli_read_done(void *ref, const struct spdk_nvme_cpl *cpl) {
	struct wals_bdev_io *io = ref;
	if (spdk_nvme_cpl_is_success(cpl)) {
		SPDK_NOTICELOG("read successful\n");
        wals_target_read_complete(io, true);
	}
	else {
		// TODO: if the read failed?
		SPDK_ERRLOG("nvmf CLI failed to read from remote SSD\n");
        wals_target_read_complete(io, false);
	}
}

static int
cli_submit_core_read_request(struct wals_target* target, void *data, uint64_t offset, uint64_t cnt, struct wals_bdev_io *wals_io)
{
    SPDK_NOTICELOG("Reading from disk %ld %ld\n", offset, cnt);
    int rc;
    struct wals_cli_slice* slice = target->private_info;
    uint64_t multiplier = slice->wals_bdev->bdev.blocklen / spdk_nvme_ns_get_sector_size(slice->nvmf_conn->ns);

    rc = spdk_nvme_ns_cmd_read(slice->nvmf_conn->ns,
        slice->nvmf_conn->qp,
        data,
        slice->nvmf_block_offset + offset * multiplier,
        cnt * multiplier,
        cli_read_done,
        wals_io,
        0);

    return 0;
}

static int
cli_submit_log_write_request(struct wals_target* target, void *data, uint64_t offset, uint64_t cnt, struct wals_bdev_io *wals_io)
{
    // SPDK_NOTICELOG("Writing to log %ld %ld\n", offset, cnt);
    int rc;
    struct wals_cli_slice* slice = target->private_info;

    if (!rdma_connection_is_connected(slice->rdma_conn)) {
        wals_target_write_complete(wals_io, false);
        return 0;
    }

    // if (!is_io_within_memory_region(data, cnt, slice->wals_bdev->blocklen, slice->rdma_conn->mr_write)) {
    //     SPDK_ERRLOG("IO out of MR range\n");
    //     wals_target_write_complete(wals_io, false);
    //     return 0;
    // }

    if (offset + cnt > slice->rdma_conn->handshake_buf[1].block_cnt) {
        SPDK_ERRLOG("Write out of remote PMEM range: %ld %ld\n", offset, cnt);
        wals_target_write_complete(wals_io, false);
        return 0;
    }

    struct wals_metadata* metadata = data;
    if (metadata->seq != slice->prev_seq + 1) {
        SPDK_ERRLOG("Seq jumped from %ld to %ld\n", slice->prev_seq, metadata->seq);
    }
    if (offset != slice->prev_offset) {
        if (offset != 0) {
            SPDK_ERRLOG("Offset jumped from %ld to %ld\n", slice->prev_offset, offset);
        }
    }
    slice->prev_seq++;
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
	wr.wr_id = (uint64_t)wals_io;
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

	rc = ibv_post_send(slice->rdma_conn->cm_id->qp, &wr, &bad_wr);
	if (rc != 0) {
		SPDK_ERRLOG("RDMA write failed with errno = %d\n", rc);
		SPDK_NOTICELOG("Local: %p %d; Remote: %p %d; Len = %d\n",
			(void*)sge.addr, sge.lkey, (void*)wr.wr.rdma.remote_addr, wr.wr.rdma.rkey,
			sge.length);
		wals_target_write_complete(wals_io, false);
	}

    struct slice_rdma_context* rdma_context = slice->rdma_conn->rdma_context;

    if (is_io_queue_full(&rdma_context->pending_write_io_queue)) {
        SPDK_ERRLOG("Should not happen: io queue is full\n");
        return 0;
    }

    rdma_context->pending_write_io_queue.pending_ios[rdma_context->pending_write_io_queue.tail] =
    (struct pending_io_context) {
        .io = wals_io,
        .ticks = spdk_get_ticks()
    };

    rdma_context->pending_write_io_queue.tail = (
        rdma_context->pending_write_io_queue.tail + 1
    ) % PENDING_IO_MAX_CNT;

    return 0;
}

static int
cli_register_write_pollers(struct wals_target *target, struct wals_bdev *wals_bdev)
{
    // NOTE: may need different pollers for each qp in the future
    if (g_rdma_cq_poller == NULL) {
        g_rdma_cq_poller = SPDK_POLLER_REGISTER(rdma_cq_poller, wals_bdev, 0);
    }
    if (g_destage_info_poller == NULL) {
        g_destage_info_poller = SPDK_POLLER_REGISTER(slice_destage_info_poller, NULL, 2000);
    }
    return 0;
}

static int
cli_unregister_write_pollers(struct wals_target *target, struct wals_bdev *wals_bdev)
{
    if (g_destage_info_poller != NULL) {
        spdk_poller_unregister(&g_destage_info_poller);
    }
    if (g_rdma_cq_poller != NULL) {
        spdk_poller_unregister(&g_rdma_cq_poller);
    }
    return 0;
}

static int
cli_register_read_pollers(struct wals_target *target, struct wals_bdev *wals_bdev)
{
    if (g_nvmf_cq_poller == NULL) {
        g_nvmf_cq_poller = SPDK_POLLER_REGISTER(nvmf_cq_poller, wals_bdev, 0);
    }
    return 0;
}

static int
cli_unregister_read_pollers(struct wals_target *target, struct wals_bdev *wals_bdev)
{
    if (g_nvmf_cq_poller != NULL) {
        spdk_poller_unregister(&g_nvmf_cq_poller);
    }
    return 0;
}

static int
rdma_cli_connection_poller(void* ctx) {
    for (int i = 0; i < NUM_TARGETS; i++) {
        if (g_rdma_conns[i] != NULL) {
            rdma_connection_connect(g_rdma_conns[i]);
        }
    }
}

// same logic as the poller in examples/nvme/reconnect.c
// assume that the namespace and io qpair don't need recreation.
static int 
nvmf_cli_connection_poller(void* ctx) {
    // TODO: do I need pthread_setcancelstate?
    int rc;
    for (int i = 0; i < NUM_TARGETS; i++) {
        if (g_nvmf_cli_conns[i].status != NVMF_CLI_INITIALIZED) {
            continue;
        }

        rc = spdk_nvme_ctrlr_process_admin_completions(g_nvmf_cli_conns[i].ctrlr);
        if (rc == -ENXIO) {
            // nvmf already has poller to reset the ctrlr.
            // assume that the transport id doesn't change
            rc = spdk_nvme_ctrlr_reset(g_nvmf_cli_conns[i].ctrlr);
            if (rc != 0) {
                SPDK_WARNLOG("Cannot reset nvmf ctrlr %d: %d\n", i, rc);
            }
            else {
                SPDK_NOTICELOG("Nvmf ctrlr %d reset successfully\n", i);
            }
        }
    }
    return SPDK_POLLER_BUSY;
}

static int slice_destage_info_poller(void* ctx) {
    SPDK_NOTICELOG("In destage info poller\n");
    // the first struct is the destage tail of the target node that the CLI should 
    // RDMA read.
    // the second struct is the commit tail of the slice that the CLI should RDMA write.
    struct wals_cli_slice* cli_slice;
    TAILQ_FOREACH(cli_slice, &g_slices, tailq_all_slices) {
        if (!rdma_connection_is_connected(cli_slice->rdma_conn)) {
            continue;
        }

        struct rdma_handshake* remote_handshake = &cli_slice->rdma_conn->handshake_buf[1];
        struct slice_rdma_context* rdma_context = cli_slice->rdma_conn->rdma_context;
        // 1. read destage tail
        if (g_destage_tail[cli_slice->id].checksum == DESTAGE_INFO_CHECKSUM) {
            struct ibv_send_wr read_wr, *bad_read_wr = NULL;
            struct ibv_sge read_sge;
            memset(&read_wr, 0, sizeof(read_wr));

            read_wr.wr_id = 0;
            read_wr.opcode = IBV_WR_RDMA_READ;
            read_wr.sg_list = &read_sge;
            read_wr.num_sge = 1;
            read_wr.send_flags = 0;
            read_wr.wr.rdma.remote_addr = (uint64_t)remote_handshake->base_addr + remote_handshake->block_cnt * remote_handshake->block_size;
            read_wr.wr.rdma.rkey = remote_handshake->rkey;

            rdma_connection_construct_sge(cli_slice->rdma_conn,
                &read_sge,
                &g_destage_tail[cli_slice->id],
                sizeof(struct destage_info));

            int rc = ibv_post_send(cli_slice->rdma_conn->cm_id->qp, &read_wr, &bad_read_wr);
            if (rc != 0) {
                SPDK_ERRLOG("post send read failed for slice %d: error %d\n",
                    cli_slice->id,
                    rc);
                continue;
            }
        } 
        else if (g_destage_tail[cli_slice->id].checksum == 0) {
            // RDMA read is successful
            // update and set the checksum back
            SPDK_NOTICELOG("Read destage tail %ld %ld\n", g_destage_tail[cli_slice->id].offset,
                g_destage_tail[cli_slice->id].round);
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
        write_wr.send_flags = 0;
        write_wr.wr.rdma.remote_addr =
            (uint64_t)remote_handshake->base_addr + \
            remote_handshake->block_cnt * remote_handshake->block_size + \
            sizeof(struct destage_info);
        write_wr.wr.rdma.rkey = remote_handshake->rkey;

        rdma_connection_construct_sge(cli_slice->rdma_conn,
            &write_sge,
            &g_commit_tail[cli_slice->id],
            sizeof(struct destage_info));

        int rc = ibv_post_send(cli_slice->rdma_conn->cm_id->qp, &write_wr, &bad_write_wr);
        if (rc != 0) {
            SPDK_ERRLOG("post send write failed for slice %d: error %d\n",
                cli_slice->id,
                rc);
            continue;
        }
    }
    return SPDK_POLLER_BUSY;
}

static int
rdma_cq_poller(void* ctx) {
    struct ibv_wc wc_buf[WC_BATCH_SIZE];
    spdk_trace_record_tsc(spdk_get_ticks(), TRACE_WALS_S_RDMA_CQ, 0, 0, (uintptr_t)ctx);
    for (int i = 0; i < NUM_TARGETS; i++) {
        struct slice_rdma_context* rdma_context = g_rdma_conns[i]->rdma_context;
        if (rdma_connection_is_connected(g_rdma_conns[i])) {
            int cnt = ibv_poll_cq(g_rdma_conns[i]->cq, WC_BATCH_SIZE, wc_buf);
            if (cnt < 0) {
                // hopefully the reconnection poller will spot the error and try to reconnect
				SPDK_ERRLOG("ibv_poll_cq failed\n");
            }
            else {
                for (int j = 0; j < cnt; j++) {
                    bool success = true;
					void* io = (void*)wc_buf[j].wr_id;
					if (wc_buf[j].status != IBV_WC_SUCCESS) {
                        // TODO: should set the qp state to error, or reconnect?
                        if (rdma_context->io_fail_cnt % 10000 == 0) {
                            SPDK_ERRLOG("IO %p RDMA op %d failed with status %d\n",
                                io,
                                wc_buf[j].opcode,
                                wc_buf[j].status);
                        }
                        
                        rdma_context->io_fail_cnt++;
                        success = false;
					}

                    if (wc_buf[j].wr_id == 0) {
                        // special case: the wr is for reading destage tail or writing commit tail.
                        // do nothing
                        continue;
                    }


                    if (wc_buf[j].opcode == IBV_WC_RDMA_READ) {
                        wals_target_read_complete(io, success);
                    }
                    else {
                        if (io == rdma_context->pending_write_io_queue.pending_ios[rdma_context->pending_write_io_queue.head].io) {
                            // SPDK_NOTICELOG("Write IO %p ok\n", io);
                            wals_target_write_complete(io, success);
                            rdma_context->pending_write_io_queue.head = (
                                rdma_context->pending_write_io_queue.head + 1
                            ) % PENDING_IO_MAX_CNT;
                        }
                        else {
                            SPDK_NOTICELOG("Ignoring io %p due to already timeout\n", io);
                        }
                    }
                }
            }
        }

        uint64_t current_ticks = spdk_get_ticks();
        uint64_t timeout_ticks = TIMEOUT_MS * spdk_get_ticks_hz() / 1000;
        int j;
        for (j = rdma_context->pending_write_io_queue.head;
            j != rdma_context->pending_write_io_queue.tail;
            j = (j + 1) % PENDING_IO_MAX_CNT) {

            if (
                rdma_context->pending_write_io_queue.pending_ios[j].ticks
                + timeout_ticks
                < current_ticks
            ) {
                // timeout.
                struct wals_bdev_io* timeout_io = rdma_context->pending_write_io_queue.pending_ios[j].io;
                SPDK_NOTICELOG("IO %p timeout\n", timeout_io);
                wals_target_write_complete(timeout_io, false);
            }
            else {
                break;
            }
        }
        rdma_context->pending_write_io_queue.head = j;
    }
    spdk_trace_record_tsc(spdk_get_ticks(), TRACE_WALS_F_RDMA_CQ, 0, 0, (uintptr_t)ctx);
    return SPDK_POLLER_IDLE;
}

static int
nvmf_cq_poller(void* ctx) {
    for (int i = 0; i < NUM_TARGETS; i++) {
        if (g_nvmf_cli_conns[i].status == NVMF_CLI_INITIALIZED) {
            spdk_nvme_qpair_process_completions(g_nvmf_cli_conns[i].qp, 0);
        }
    }
    return SPDK_POLLER_IDLE;
}

static int 
pending_io_timeout_poller(void* ctx) {
    return SPDK_POLLER_IDLE;
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
};

TARGET_MODULE_REGISTER(&g_rdma_cli_module)

SPDK_LOG_REGISTER_COMPONENT(bdev_wals_cli)
