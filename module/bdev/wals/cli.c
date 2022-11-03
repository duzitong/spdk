#include "bdev_wals.h"

#include "spdk/env.h"
#include "spdk/thread.h"
#include "spdk/string.h"
#include "spdk/util.h"
#include "spdk/nvme.h"
#include "spdk/rdma.h"

#include "spdk/log.h"
#include <infiniband/verbs.h>
#include <rdma/rdma_cma.h>

#define WC_BATCH_SIZE 32
// Should not exceed log_blockcnt
#define PENDING_IO_MAX_CNT 131072
#define MAX_SLICES 256
// currently a fake one, to make sure that it fully RDMA reads the struct
#define DESTAGE_INFO_CHECKSUM 666

enum rdma_cli_status {
    RDMA_CLI_UNINITIALIZED,
	RDMA_CLI_CONNECTING,
	RDMA_CLI_ESTABLISHED,
	RDMA_CLI_CONNECTED,
	RDMA_CLI_INITIALIZED,
	RDMA_CLI_ERROR,
	RDMA_CLI_ADDR_RESOLVING,
	RDMA_CLI_ROUTE_RESOLVING,
};

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

// TODO: refactor it so that fields for each slice is not in it
struct rdma_cli_connection {
    int id;
    struct rdma_event_channel* channel;
    struct rdma_cm_id* cm_id;
    struct ibv_cq* cq;
    struct ibv_mr* mr_read;
    struct ibv_mr* mr_write;
    struct ibv_mr* mr_handshake;
    struct ibv_mr* mr_destage_tail;
    struct ibv_mr* mr_commit_tail;
    struct addrinfo* server_addr;
    struct ibv_wc wc_buf[WC_BATCH_SIZE];
    struct pending_io_queue pending_read_io_queue;
    struct pending_io_queue pending_write_io_queue;
    struct pending_io_queue pending_nvmf_read_io_queue;

    volatile enum rdma_cli_status status;
    uint64_t reject_cnt;
    uint64_t io_fail_cnt;
    uint64_t reconnect_cnt;
    size_t blockcnt_per_slice;
    TAILQ_HEAD(, wals_cli_slice) slices;
};

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

// actually should be num of servers
struct rdma_cli_connection g_rdma_cli_conns[NUM_TARGETS];
struct nvmf_cli_connection g_nvmf_cli_conns[NUM_TARGETS];

struct rdma_handshake g_rdma_handshakes[NUM_TARGETS + 1];

struct spdk_poller* g_rdma_connection_poller;
struct spdk_poller* g_nvmf_connection_poller;
struct spdk_poller* g_rdma_cq_poller;
struct spdk_poller* g_nvmf_cq_poller;
struct spdk_poller* g_destage_info_poller;

int g_num_slices = 0;

// TODO: rewrite it to supp
TAILQ_HEAD(, wals_cli_slice) g_slices;
struct destage_info g_destage_tail[MAX_SLICES];
struct destage_info g_commit_tail[MAX_SLICES];

struct wals_cli_slice {
    int id;
    struct wals_bdev* wals_bdev;
    // can use CONTAINEROF?
    struct wals_target* wals_target;
    struct wals_slice* wals_slice;
    struct rdma_cli_connection* rdma_conn;
    struct nvmf_cli_connection* nvmf_conn;
    // if connected to the same ssd, then they need to share the same qp.
    uint64_t nvmf_block_offset;

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

    // 1. use or create rdma connection
    for (int i = 0; i < NUM_TARGETS; i++) {
        if (g_rdma_cli_conns[i].status != RDMA_CLI_UNINITIALIZED) {
            // compare ip and port
            struct sockaddr_in* cur_addr = g_rdma_cli_conns[i].server_addr->ai_addr;
            if (cur_addr->sin_addr.s_addr == addr.sin_addr.s_addr
                && cur_addr->sin_port == addr.sin_port) {
                SPDK_NOTICELOG("Already created rdma connection.\n");
                cli_slice->rdma_conn = &g_rdma_cli_conns[i];
                TAILQ_INSERT_TAIL(&g_rdma_cli_conns[i].slices, cli_slice, tailq_rdma);
                break;
            }
        }
    }

    if (cli_slice->rdma_conn == NULL) {
        // no existing connection. need to build one 
        for (int i = 0; i < NUM_TARGETS; i++) {
            if (g_rdma_cli_conns[i].status == RDMA_CLI_UNINITIALIZED) {
                // this slot is empty. use it
                SPDK_NOTICELOG("Using empty RDMA connection slot %d\n", i);
                cli_slice->rdma_conn = &g_rdma_cli_conns[i];
                TAILQ_INIT(&g_rdma_cli_conns[i].slices);
                TAILQ_INSERT_TAIL(&g_rdma_cli_conns[i].slices, cli_slice, tailq_rdma);

                g_rdma_cli_conns[i].id = i;
                g_rdma_cli_conns[i].server_addr = addr_res;
                struct rdma_event_channel* channel = rdma_create_event_channel();
                g_rdma_cli_conns[i].channel = channel;

                int flags = fcntl(channel->fd, F_GETFL);
                int rc = fcntl(channel->fd, F_SETFL, flags | O_NONBLOCK);
                if (rc != 0) {
                    SPDK_ERRLOG("fcntl failed\n");
                    return NULL;
                }
                g_rdma_cli_conns[i].status = RDMA_CLI_INITIALIZED;

                // only need one poller for connecting the qps
                if (g_rdma_connection_poller == NULL) {
                    g_rdma_connection_poller = SPDK_POLLER_REGISTER(rdma_cli_connection_poller, wals_bdev, 5 * 1000);
                }


                while (g_rdma_cli_conns[i].status != RDMA_CLI_CONNECTED) {
                    rdma_cli_connection_poller(wals_bdev);
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

    // TODO: should not set log_blockcnt right now
    target->log_blockcnt = cli_slice->rdma_conn->blockcnt_per_slice;
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

static bool is_io_within_memory_region(void* data, uint64_t cnt, uint64_t blocklen, struct ibv_mr* mr) {
    return (data >= mr->addr
        && data < mr->addr + mr->length
        && data + cnt * blocklen >= mr->addr
        && data + cnt * blocklen < mr->addr + mr->length);
}

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
    // if (slice->rdma_conn->status != RDMA_CLI_CONNECTED) {
    //     if (wals_io->metadata->seq <= 10) {
    //         // HACK: the bdev sends some unimportant IOs after the initialization,
    //         // but the RDMA handshake may be pending.
    //         SPDK_NOTICELOG("Completing IO without actually sending it\n");
    //         wals_target_read_complete(wals_io, true);
    //     }
    //     else {
    //         wals_target_read_complete(wals_io, false);
    //     }
    //     return 0;
    // }

    if (!is_io_within_memory_region(data, cnt, slice->wals_bdev->blocklen, slice->rdma_conn->mr_read)) {
        SPDK_ERRLOG("IO out of MR range\n");
        wals_target_read_complete(wals_io, false);
        return 0;
    }

    if (offset + cnt > slice->rdma_conn->blockcnt_per_slice) {
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
    wr.wr.rdma.remote_addr = (uint64_t)g_rdma_handshakes[slice->rdma_conn->id].base_addr + offset * slice->wals_bdev->blocklen;
    wr.wr.rdma.rkey = g_rdma_handshakes[slice->rdma_conn->id].rkey;

    sge.addr = (uint64_t)data;
    sge.length = cnt * slice->wals_bdev->blocklen;
    sge.lkey = slice->rdma_conn->mr_read->lkey;
    // SPDK_NOTICELOG("READ %p %p %p %p %ld %ld %ld %ld\n", wals_io, data, data + sge.length, wr.wr.rdma.remote_addr,
    //     cnt, offset, slice->wals_bdev->blocklen, sge.lkey);

    
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
		// SPDK_NOTICELOG("read successful\n");
        wals_target_read_complete(io, true);
	}
	else {
		// TODO: if the read failed?
		SPDK_ERRLOG("nvmf client failed to read from remote SSD\n");
        wals_target_read_complete(io, false);
	}
}

static int
cli_submit_core_read_request(struct wals_target* target, void *data, uint64_t offset, uint64_t cnt, struct wals_bdev_io *wals_io)
{
    // SPDK_NOTICELOG("Reading from disk %ld %ld\n", offset, cnt);
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

    if (!is_io_within_memory_region(data, cnt, slice->wals_bdev->blocklen, slice->rdma_conn->mr_write)) {
        SPDK_ERRLOG("IO out of MR range\n");
        wals_target_write_complete(wals_io, false);
        return 0;
    }

    if (offset + cnt > slice->rdma_conn->blockcnt_per_slice) {
        SPDK_ERRLOG("Write out of remote PMEM range: %ld %ld\n", offset, cnt);
        wals_target_write_complete(wals_io, false);
        return 0;
    }

    // struct wals_metadata* metadata = data;
    
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
	wr.wr.rdma.remote_addr = (uint64_t)g_rdma_handshakes[slice->rdma_conn->id].base_addr + offset * wals_io->wals_bdev->blocklen;
	wr.wr.rdma.rkey = g_rdma_handshakes[slice->rdma_conn->id].rkey;

	sge.addr = (uint64_t)data;
	sge.length = cnt * slice->wals_bdev->blocklen;
	sge.lkey = slice->rdma_conn->mr_write->lkey;

	rc = ibv_post_send(slice->rdma_conn->cm_id->qp, &wr, &bad_wr);
	if (rc != 0) {
		SPDK_ERRLOG("RDMA write failed with errno = %d\n", rc);
		SPDK_NOTICELOG("Local: %p %d; Remote: %p %d; Len = %d\n",
			(void*)sge.addr, sge.lkey, (void*)wr.wr.rdma.remote_addr, wr.wr.rdma.rkey,
			sge.length);
		wals_target_write_complete(wals_io, false);
	}
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
        g_destage_info_poller = SPDK_POLLER_REGISTER(slice_destage_info_poller, NULL, 200);
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

static int 
rdma_cli_connection_poller(void* ctx) {
    struct wals_bdev* wals_bdev = ctx;
    for (int i = 0; i < NUM_TARGETS; i++) {
        switch (g_rdma_cli_conns[i].status) {
            case RDMA_CLI_UNINITIALIZED:
            {
                break;
            }
            case RDMA_CLI_INITIALIZED:
            {
                int rc;
                struct rdma_cm_id* cm_id = NULL;
                rc = rdma_create_id(g_rdma_cli_conns[i].channel, &cm_id, NULL, RDMA_PS_TCP);
                if (rc != 0) {
                    SPDK_ERRLOG("rdma_create_id failed\n");
                    g_rdma_cli_conns[i].status = RDMA_CLI_ERROR;
                    break;
                }
                g_rdma_cli_conns[i].cm_id = cm_id;

                rc = rdma_resolve_addr(cm_id, NULL, g_rdma_cli_conns[i].server_addr->ai_addr, 1000);
                if (rc != 0) {
                    SPDK_ERRLOG("rdma_resolve_addr failed\n");
                    g_rdma_cli_conns[i].status = RDMA_CLI_ERROR;
                    break;
                }
                g_rdma_cli_conns[i].status = RDMA_CLI_ADDR_RESOLVING;
                break;
            }
            case RDMA_CLI_ADDR_RESOLVING:
            case RDMA_CLI_ROUTE_RESOLVING:
            {
                int rc;
                struct rdma_cm_event* event;
                rc = rdma_get_cm_event(g_rdma_cli_conns[i].channel, &event);
                if (rc != 0) {
                    if (errno == EAGAIN) {
                        // wait for server to accept. do nothing
                    }
                    else {
                        SPDK_ERRLOG("Unexpected CM error %d\n", errno);
                    }
                    break;
                }
                enum rdma_cm_event_type expected_event_type;
                if (g_rdma_cli_conns[i].status == RDMA_CLI_ADDR_RESOLVING) {
                    expected_event_type = RDMA_CM_EVENT_ADDR_RESOLVED;
                }
                else if (g_rdma_cli_conns[i].status == RDMA_CLI_ROUTE_RESOLVING) {
                    expected_event_type = RDMA_CM_EVENT_ROUTE_RESOLVED;
                }
                else {
                    // should not happen
                    SPDK_ERRLOG("ERROR\n");
                    g_rdma_cli_conns[i].status = RDMA_CLI_ERROR;
                    break;
                }

                // suppose addr and resolving never fails
                if (event->event != expected_event_type) {
                    SPDK_ERRLOG("unexpected event type %d (expect %d)\n",
                        event->event,
                        expected_event_type);
                    g_rdma_cli_conns[i].status = RDMA_CLI_ERROR;
                    break;
                }
                if (g_rdma_cli_conns[i].cm_id != event->id) {
                    SPDK_ERRLOG("CM id mismatch\n");
                    g_rdma_cli_conns[i].status = RDMA_CLI_ERROR;
                    break;
                }
                rc = rdma_ack_cm_event(event);
                if (rc != 0) {
                    SPDK_ERRLOG("ack cm event failed\n");
                    g_rdma_cli_conns[i].status = RDMA_CLI_ERROR;
                    break;
                }

                if (g_rdma_cli_conns[i].status == RDMA_CLI_ADDR_RESOLVING) {
                    rc = rdma_resolve_route(g_rdma_cli_conns[i].cm_id, 1000);
                    if (rc != 0) {
                        SPDK_ERRLOG("resolve route failed\n");
                        g_rdma_cli_conns[i].status = RDMA_CLI_ERROR;
                        break;
                    }
                    g_rdma_cli_conns[i].status = RDMA_CLI_ROUTE_RESOLVING;
                }
                else if (g_rdma_cli_conns[i].status == RDMA_CLI_ROUTE_RESOLVING) {
                    struct ibv_context* ibv_context = g_rdma_cli_conns[i].cm_id->verbs;
                    struct ibv_device_attr device_attr = {};
                    ibv_query_device(ibv_context, &device_attr);
                    // SPDK_NOTICELOG("max wr sge = %d, max wr num = %d, max cqe = %d, max qp = %d\n",
                    //     device_attr.max_sge, device_attr.max_qp_wr, device_attr.max_cqe, device_attr.max_qp);
                    struct ibv_cq* ibv_cq = ibv_create_cq(ibv_context, 256, NULL, NULL, 0);
                    if (ibv_cq == NULL) {
                        SPDK_ERRLOG("Failed to create cq\n");
                        g_rdma_cli_conns[i].status = RDMA_CLI_ERROR;
                        break;
                    }
                    g_rdma_cli_conns[i].cq = ibv_cq;
                    // no SRQ here - only one qp
                    // TODO: fine-tune these params; 
                    struct ibv_qp_init_attr init_attr = {
                        .send_cq = ibv_cq,
                        .recv_cq = ibv_cq,
                        .qp_type = IBV_QPT_RC,
                        .cap = {
                            .max_send_sge = device_attr.max_sge,
                            .max_send_wr = 256,
                            .max_recv_sge = device_attr.max_sge,
                            .max_recv_wr = 256,
                        }
                    };

                    rc = rdma_create_qp(g_rdma_cli_conns[i].cm_id, NULL, &init_attr);
                    if (rc != 0) {
                        SPDK_ERRLOG("rdma_create_qp fails %d\n", rc);
                        g_rdma_cli_conns[i].status = RDMA_CLI_ERROR;
                        break;
                    }

                    struct ibv_mr* mr_read = ibv_reg_mr(g_rdma_cli_conns[i].cm_id->qp->pd,
                        wals_bdev->read_heap->buf,
                        wals_bdev->read_heap->buf_size,
                        IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ);

                    if (mr_read == NULL) {
                        SPDK_ERRLOG("failed to reg mr read\n");
                        g_rdma_cli_conns[i].status = RDMA_CLI_ERROR;
                        break;
                    }
                    g_rdma_cli_conns[i].mr_read = mr_read;

                    // SPDK_NOTICELOG("Registering read buf %p to %p\n", wals_bdev->read_heap->buf, wals_bdev->read_heap->buf + wals_bdev->read_heap->buf_size);

                    struct ibv_mr* mr_write = ibv_reg_mr(g_rdma_cli_conns[i].cm_id->qp->pd,
                        wals_bdev->write_heap->buf,
                        wals_bdev->write_heap->buf_size,
                        IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ);

                    if (mr_write == NULL) {
                        SPDK_ERRLOG("failed to reg mr write\n");
                        g_rdma_cli_conns[i].status = RDMA_CLI_ERROR;
                        break;
                    }
                    g_rdma_cli_conns[i].mr_write = mr_write;

                    // SPDK_NOTICELOG("Registering write buf %p to %p\n", wals_bdev->write_heap->buf, wals_bdev->write_heap->buf + wals_bdev->write_heap->buf_size);
                    
                    struct ibv_mr* mr_handshake = ibv_reg_mr(g_rdma_cli_conns[i].cm_id->qp->pd,
                        g_rdma_handshakes,
                        sizeof(g_rdma_handshakes),
                        IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ);

                    if (mr_handshake == NULL) {
                        SPDK_ERRLOG("failed to reg mr handshake\n");
                        g_rdma_cli_conns[i].status = RDMA_CLI_ERROR;
                        break;
                    }
                    g_rdma_cli_conns[i].mr_handshake = mr_handshake;

                    struct ibv_mr* mr_destage_tail = ibv_reg_mr(g_rdma_cli_conns[i].cm_id->qp->pd,
                        g_destage_tail,
                        sizeof(g_destage_tail),
                        IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ);

                    if (mr_destage_tail == NULL) {
                        SPDK_ERRLOG("failed to reg mr destage tail\n");
                        g_rdma_cli_conns[i].status = RDMA_CLI_ERROR;
                        break;
                    }
                    g_rdma_cli_conns[i].mr_destage_tail = mr_destage_tail;

                    struct ibv_mr* mr_commit_tail = ibv_reg_mr(g_rdma_cli_conns[i].cm_id->qp->pd,
                        g_commit_tail,
                        sizeof(g_commit_tail),
                        IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ);

                    if (mr_commit_tail == NULL) {
                        SPDK_ERRLOG("failed to reg mr commit tail\n");
                        g_rdma_cli_conns[i].status = RDMA_CLI_ERROR;
                        break;
                    }
                    g_rdma_cli_conns[i].mr_commit_tail = mr_commit_tail;

                    struct ibv_recv_wr wr, *bad_wr = NULL;
                    struct ibv_sge sge;

                    struct rdma_handshake* handshake = &g_rdma_handshakes[NUM_TARGETS];
                    struct rdma_handshake* remote_handshake = &g_rdma_handshakes[i];
                    handshake->base_addr = &g_rdma_handshakes[0];
                    handshake->rkey = mr_handshake->rkey;
                    handshake->reconnect_cnt = g_rdma_cli_conns[i].reconnect_cnt;

                    // TODO: support multiple slices in each RDMA connection
                    struct wals_cli_slice* cli_slice = TAILQ_FIRST(&g_rdma_cli_conns[i].slices);
                    handshake->destage_tail.offset = cli_slice->wals_slice->head.offset;
                    handshake->destage_tail.round = cli_slice->wals_slice->head.round;

                    wr.wr_id = (uintptr_t)i;
                    wr.next = NULL;
                    wr.sg_list = &sge;
                    wr.num_sge = 1;

                    sge.addr = (uint64_t)remote_handshake;
                    sge.length = sizeof(struct rdma_handshake);
                    sge.lkey = mr_handshake->lkey;

                    rc = ibv_post_recv(g_rdma_cli_conns[i].cm_id->qp, &wr, &bad_wr);
                    if (rc != 0) {
                        SPDK_ERRLOG("post recv failed\n");
                        g_rdma_cli_conns[i].status = RDMA_CLI_ERROR;
                        break;
                    }

                    struct rdma_conn_param conn_param = {};

                    conn_param.responder_resources = 16;
                    conn_param.initiator_depth = 16;
                    conn_param.retry_count = 7;
                    conn_param.rnr_retry_count = 7;

                    rc = rdma_connect(g_rdma_cli_conns[i].cm_id, &conn_param);
                    if (rc != 0) {
                        SPDK_ERRLOG("rdma_connect failed\n");
                        g_rdma_cli_conns[i].status = RDMA_CLI_ERROR;
                        break;
                    }
                    g_rdma_cli_conns[i].status = RDMA_CLI_CONNECTING;
                }
                break;
            }
            case RDMA_CLI_CONNECTED:
            {
                struct rdma_cm_event* cm_event;
                int rc = rdma_get_cm_event(g_rdma_cli_conns[i].channel, &cm_event);
                if (rc != 0) {
                    if (errno == EAGAIN) {
                        // wait for server to accept. do nothing
                    }
                    else {
                        SPDK_ERRLOG("Unexpected CM error %d\n", errno);
                    }
                    break;
                }
                rc = rdma_ack_cm_event(cm_event);
                if (rc != 0) {
                    SPDK_ERRLOG("failed to ack event\n");
                    g_rdma_cli_conns[i].status = RDMA_CLI_ERROR;
                    break;
                }

                if (cm_event->event == RDMA_CM_EVENT_DISCONNECTED) {
                    SPDK_NOTICELOG("Received disconnect event\n");
                    rdma_destroy_qp(g_rdma_cli_conns[i].cm_id);
                    if (g_rdma_cli_conns[i].cm_id->qp != NULL) {
                        SPDK_NOTICELOG("cannot free qp\n");
                        g_rdma_cli_conns[i].status = RDMA_CLI_ERROR;
                        break;
                    }

                    rc = rdma_destroy_id(g_rdma_cli_conns[i].cm_id);
                    if (rc != 0) {
                        SPDK_ERRLOG("cannot destroy id\n");
                        g_rdma_cli_conns[i].status = RDMA_CLI_ERROR;
                        break;
                    }
                    g_rdma_cli_conns[i].cm_id = NULL;

                    rc = ibv_destroy_cq(g_rdma_cli_conns[i].cq);
                    if (rc != 0) {
                        SPDK_ERRLOG("destroy cq failed\n");
                        g_rdma_cli_conns[i].status = RDMA_CLI_ERROR;
                        break;
                    }
                    g_rdma_cli_conns[i].cq = NULL;

                    rc = ibv_dereg_mr(g_rdma_cli_conns[i].mr_read);
                    if (rc != 0) {
                        SPDK_ERRLOG("failed to dereg mr\n");
                        g_rdma_cli_conns[i].status = RDMA_CLI_ERROR;
                        break;
                    }
                    g_rdma_cli_conns[i].mr_read = NULL;
                    rc = ibv_dereg_mr(g_rdma_cli_conns[i].mr_write);
                    if (rc != 0) {
                        SPDK_ERRLOG("failed to dereg mr\n");
                        g_rdma_cli_conns[i].status = RDMA_CLI_ERROR;
                        break;
                    }
                    g_rdma_cli_conns[i].mr_write = NULL;
                    rc = ibv_dereg_mr(g_rdma_cli_conns[i].mr_handshake);
                    if (rc != 0) {
                        SPDK_ERRLOG("failed to dereg mr\n");
                        g_rdma_cli_conns[i].status = RDMA_CLI_ERROR;
                        break;
                    }
                    g_rdma_cli_conns[i].mr_handshake = NULL;
                    rc = ibv_dereg_mr(g_rdma_cli_conns[i].mr_destage_tail);
                    if (rc != 0) {
                        SPDK_ERRLOG("failed to dereg mr\n");
                        g_rdma_cli_conns[i].status = RDMA_CLI_ERROR;
                        break;
                    }
                    g_rdma_cli_conns[i].mr_destage_tail = NULL;

                    rc = ibv_dereg_mr(g_rdma_cli_conns[i].mr_commit_tail);
                    if (rc != 0) {
                        SPDK_ERRLOG("failed to dereg mr\n");
                        g_rdma_cli_conns[i].status = RDMA_CLI_ERROR;
                        break;
                    }
                    g_rdma_cli_conns[i].mr_commit_tail = NULL;

                    g_rdma_cli_conns[i].status = RDMA_CLI_INITIALIZED;
                }
                else {
                    SPDK_ERRLOG("Should not receive event %d when connected\n", cm_event->event);
                }
                break;
            }
            case RDMA_CLI_CONNECTING:
            {
                struct rdma_handshake* handshake = &g_rdma_handshakes[NUM_TARGETS];
                int rc;
                struct rdma_cm_event* connect_event;
                rc = rdma_get_cm_event(g_rdma_cli_conns[i].channel, &connect_event);
                if (rc != 0) {
                    if (errno == EAGAIN) {
                        // wait for server to accept. do nothing
                    }
                    else {
                        SPDK_ERRLOG("Unexpected CM error %d\n", errno);
                    }
                    break;
                }
                rc = rdma_ack_cm_event(connect_event);
                if (rc != 0) {
                    SPDK_ERRLOG("failed to ack event\n");
                    g_rdma_cli_conns[i].status = RDMA_CLI_ERROR;
                    break;
                }

                if (connect_event->event == RDMA_CM_EVENT_REJECTED) {
                    g_rdma_cli_conns[i].reject_cnt++;
                    if (g_rdma_cli_conns[i].reject_cnt % 1000 == 1) {
                        SPDK_NOTICELOG("Rejected %ld. Try again...\n", g_rdma_cli_conns[i].reject_cnt);
                    }
                    // remote not ready yet
                    // destroy all rdma resources and try again
                    rdma_destroy_qp(g_rdma_cli_conns[i].cm_id);
                    if (g_rdma_cli_conns[i].cm_id->qp != NULL) {
                        SPDK_NOTICELOG("cannot free qp\n");
                        g_rdma_cli_conns[i].status = RDMA_CLI_ERROR;
                        break;
                    }

                    rc = rdma_destroy_id(g_rdma_cli_conns[i].cm_id);
                    if (rc != 0) {
                        SPDK_ERRLOG("cannot destroy id\n");
                        g_rdma_cli_conns[i].status = RDMA_CLI_ERROR;
                        break;
                    }
                    g_rdma_cli_conns[i].cm_id = NULL;

                    rc = ibv_destroy_cq(g_rdma_cli_conns[i].cq);
                    if (rc != 0) {
                        SPDK_ERRLOG("destroy cq failed\n");
                        g_rdma_cli_conns[i].status = RDMA_CLI_ERROR;
                        break;
                    }
                    g_rdma_cli_conns[i].cq = NULL;

                    rc = ibv_dereg_mr(g_rdma_cli_conns[i].mr_read);
                    if (rc != 0) {
                        SPDK_ERRLOG("failed to dereg mr\n");
                        g_rdma_cli_conns[i].status = RDMA_CLI_ERROR;
                        break;
                    }
                    g_rdma_cli_conns[i].mr_read = NULL;
                    rc = ibv_dereg_mr(g_rdma_cli_conns[i].mr_write);
                    if (rc != 0) {
                        SPDK_ERRLOG("failed to dereg mr\n");
                        g_rdma_cli_conns[i].status = RDMA_CLI_ERROR;
                        break;
                    }
                    g_rdma_cli_conns[i].mr_write = NULL;
                    rc = ibv_dereg_mr(g_rdma_cli_conns[i].mr_handshake);
                    if (rc != 0) {
                        SPDK_ERRLOG("failed to dereg mr\n");
                        g_rdma_cli_conns[i].status = RDMA_CLI_ERROR;
                        break;
                    }
                    g_rdma_cli_conns[i].mr_handshake = NULL;
                    rc = ibv_dereg_mr(g_rdma_cli_conns[i].mr_destage_tail);
                    if (rc != 0) {
                        SPDK_ERRLOG("failed to dereg mr\n");
                        g_rdma_cli_conns[i].status = RDMA_CLI_ERROR;
                        break;
                    }
                    g_rdma_cli_conns[i].mr_destage_tail = NULL;

                    rc = ibv_dereg_mr(g_rdma_cli_conns[i].mr_commit_tail);
                    if (rc != 0) {
                        SPDK_ERRLOG("failed to dereg mr\n");
                        g_rdma_cli_conns[i].status = RDMA_CLI_ERROR;
                        break;
                    }
                    g_rdma_cli_conns[i].mr_commit_tail = NULL;


                    g_rdma_cli_conns[i].status = RDMA_CLI_INITIALIZED;
                    g_rdma_cli_conns[i].reconnect_cnt++;
                    break;
                }
                else if (connect_event->event != RDMA_CM_EVENT_ESTABLISHED) {
                    SPDK_ERRLOG("invalid event type %d\n", connect_event->event);
                    break;
                }

                g_rdma_cli_conns[i].status = RDMA_CLI_ESTABLISHED;
                break;
            }
            case RDMA_CLI_ESTABLISHED:
            {
                struct ibv_wc wc;
                struct rdma_handshake* handshake = &g_rdma_handshakes[NUM_TARGETS];
                struct rdma_handshake* remote_handshake = &g_rdma_handshakes[i];
                int cnt = ibv_poll_cq(g_rdma_cli_conns[i].cq, 1, &wc);
                if (cnt < 0) {
                    SPDK_ERRLOG("ibv_poll_cq failed\n");
                    break;
                }

                if (cnt == 0) {
                    break;
                }

                if (wc.status != IBV_WC_SUCCESS) {
                    SPDK_ERRLOG("WC bad status %d\n", wc.status);
                    break;
                }

                if (wc.wr_id < NUM_TARGETS) {
                    // recv complete
                    SPDK_NOTICELOG("received remote addr %p rkey %d block_cnt %ld block_size %ld\n",
                        remote_handshake->base_addr,
                        remote_handshake->rkey,
                        remote_handshake->block_cnt,
                        remote_handshake->block_size);

                    if (remote_handshake->block_size != wals_bdev->bdev.blocklen) {
                        // TODO: this is fatal
                        SPDK_ERRLOG("Bdev blocklen %d mismatch log blocklen %ld\n",
                            wals_bdev->bdev.blocklen,
                            remote_handshake->block_size);
                    }

                    g_rdma_cli_conns[i].blockcnt_per_slice = remote_handshake->block_cnt;
                    
                    SPDK_NOTICELOG("connected. posting send...\n");
                    handshake->block_cnt = remote_handshake->block_cnt;
                    handshake->block_size = remote_handshake->block_size;
                    SPDK_NOTICELOG("sending local addr %p rkey %d block_cnt %ld block_size %ld\n",
                        handshake->base_addr,
                        handshake->rkey,
                        handshake->block_cnt,
                        handshake->block_size);

                    struct ibv_send_wr send_wr, *bad_send_wr = NULL;
                    struct ibv_sge send_sge;
                    memset(&send_wr, 0, sizeof(send_wr));

                    send_wr.wr_id = NUM_TARGETS + i;
                    send_wr.opcode = IBV_WR_SEND;
                    send_wr.sg_list = &send_sge;
                    send_wr.num_sge = 1;
                    send_wr.send_flags = IBV_SEND_SIGNALED;

                    send_sge.addr = (uint64_t)handshake;
                    send_sge.length = sizeof(struct rdma_handshake);
                    send_sge.lkey = g_rdma_cli_conns[i].mr_handshake->lkey;

                    int rc = ibv_post_send(g_rdma_cli_conns[i].cm_id->qp, &send_wr, &bad_send_wr);
                    if (rc != 0) {
                        SPDK_ERRLOG("post send failed\n");
                        break;
                    }
                }
                else if (wc.wr_id < 2 * NUM_TARGETS) {
                    // send cpl
                    SPDK_NOTICELOG("send req complete\n");
                    SPDK_NOTICELOG("rdma handshake complete\n");
                    g_rdma_cli_conns[i].status = RDMA_CLI_CONNECTED;
                }
                else {
                    SPDK_ERRLOG("Should not complete wrid = %ld\n", wc.wr_id);
                }
                break;
            }
            case RDMA_CLI_ERROR:
            {
                SPDK_NOTICELOG("In error state. Cannot recover by now\n");
                break;
            }
        }
	}

	return SPDK_POLLER_BUSY;
}

static int slice_destage_info_poller(void* ctx) {
    // the first struct is the destage tail of the target node that the client should 
    // RDMA read.
    // the second struct is the commit tail of the slice that the client should RDMA write.
    struct wals_cli_slice* cli_slice;
    TAILQ_FOREACH(cli_slice, &g_slices, tailq_all_slices) {
        // 1. read destage tail
        if (g_destage_tail[cli_slice->id].checksum == DESTAGE_INFO_CHECKSUM) {
            if (cli_slice->rdma_conn->status == RDMA_CLI_CONNECTED) {
                struct ibv_send_wr read_wr, *bad_read_wr = NULL;
                struct ibv_sge read_sge;
                memset(&read_wr, 0, sizeof(read_wr));

                read_wr.wr_id = 0;
                read_wr.opcode = IBV_WR_RDMA_READ;
                read_wr.sg_list = &read_sge;
                read_wr.num_sge = 1;
                read_wr.send_flags = 0;
                read_wr.wr.rdma.remote_addr = (uint64_t)g_rdma_handshakes[cli_slice->rdma_conn->id].base_addr + cli_slice->rdma_conn->blockcnt_per_slice * cli_slice->wals_bdev->blocklen;
                read_wr.wr.rdma.rkey = g_rdma_handshakes[cli_slice->rdma_conn->id].rkey;

                read_sge.addr = (uint64_t)&g_destage_tail[cli_slice->id];
                read_sge.length = sizeof(struct destage_info);
                read_sge.lkey = cli_slice->rdma_conn->mr_destage_tail->lkey;

                int rc = ibv_post_send(cli_slice->rdma_conn->cm_id->qp, &read_wr, &bad_read_wr);
                if (rc != 0) {
                    SPDK_ERRLOG("post send failed\n");
                    continue;
                }
            }
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
        if (cli_slice->rdma_conn->status == RDMA_CLI_CONNECTED) {
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
                (uint64_t)g_rdma_handshakes[cli_slice->rdma_conn->id].base_addr + \
                cli_slice->rdma_conn->blockcnt_per_slice * cli_slice->wals_bdev->blocklen + \
                sizeof(struct destage_info);
            write_wr.wr.rdma.rkey = g_rdma_handshakes[cli_slice->rdma_conn->id].rkey;

            write_sge.addr = (uint64_t)&g_commit_tail[cli_slice->id];
            write_sge.length = sizeof(struct destage_info);
            write_sge.lkey = cli_slice->rdma_conn->mr_commit_tail->lkey;

            int rc = ibv_post_send(cli_slice->rdma_conn->cm_id->qp, &write_wr, &bad_write_wr);
            if (rc != 0) {
                SPDK_ERRLOG("post send failed\n");
                continue;
            }
        }
    }
    return SPDK_POLLER_BUSY;
}

static int
rdma_cq_poller(void* ctx) {
    spdk_trace_record_tsc(spdk_get_ticks(), TRACE_WALS_S_RDMA_CQ, 0, 0, (uintptr_t)ctx);
    for (int i = 0; i < NUM_TARGETS; i++) {
        if (g_rdma_cli_conns[i].status == RDMA_CLI_CONNECTED) {
            int cnt = ibv_poll_cq(g_rdma_cli_conns[i].cq, WC_BATCH_SIZE, &g_rdma_cli_conns[i].wc_buf[0]);
            if (cnt < 0) {
                // hopefully the reconnection poller will spot the error and try to reconnect
				SPDK_ERRLOG("ibv_poll_cq failed\n");
            }
            else {
                for (int j = 0; j < cnt; j++) {
                    bool success = true;
					void* io = (void*)g_rdma_cli_conns[i].wc_buf[j].wr_id;
					if (g_rdma_cli_conns[i].wc_buf[j].status != IBV_WC_SUCCESS) {
                        // TODO: should set the qp state to error, or reconnect?
                        if (g_rdma_cli_conns[i].io_fail_cnt % 10000 == 0) {
                            SPDK_ERRLOG("IO %p RDMA op %d failed with status %d\n",
                                io,
                                g_rdma_cli_conns[i].wc_buf[j].opcode,
                                g_rdma_cli_conns[i].wc_buf[j].status);
                        }
                        
                        g_rdma_cli_conns[i].io_fail_cnt++;
                        success = false;
					}

                    if (g_rdma_cli_conns[i].wc_buf[j].wr_id == 0) {
                        // special case: the wr is for reading destage tail or writing commit tail.
                        // do nothing
                        continue;
                    }


                    if (g_rdma_cli_conns[i].wc_buf[j].opcode == IBV_WC_RDMA_READ) {
                        wals_target_read_complete(io, success);
                    }
                    else {
                        wals_target_write_complete(io, success);
                    }
                }
            }
        }
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
