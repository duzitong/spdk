#include "bdev_wals.h"

#include "spdk/env.h"
#include "spdk/thread.h"
#include "spdk/string.h"
#include "spdk/util.h"
#include "spdk/nvme.h"

#include "spdk/log.h"
#include <infiniband/verbs.h>
#include <rdma/rdma_cma.h>

#define WC_BATCH_SIZE 32
// TODO: make it configurable
#define LOG_BLOCKCNT 131072

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

struct rdma_cli_connection {
    struct rdma_event_channel* channel;
    struct rdma_cm_id* cm_id;
    struct ibv_cq* cq;
    struct ibv_mr* mr;
    struct addrinfo* server_addr;
    struct ibv_wc wc_buf[WC_BATCH_SIZE];
    enum rdma_cli_status status;
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
};

// actually should be num of servers
struct rdma_cli_connection g_rdma_cli_conns[NUM_TARGETS];
struct nvmf_cli_connection g_nvmf_cli_conns[NUM_TARGETS];

struct spdk_poller* connection_poller;

struct wals_cli_slice {
    struct rdma_cli_connection* rdma_conn;
    struct nvmf_cli_connection* nvmf_conn;
    void* buf;
    uint64_t blocklen;
    // if connected to the same ssd, then they need to share the same qp.
    uint64_t nvmf_block_offset;

	/* link next for pending writes */
	TAILQ_ENTRY(wals_cli_slice)	tailq;
};

static bool
nvmf_cli_probe_cb(void *cb_ctx, const struct spdk_nvme_transport_id *trid,
	 struct spdk_nvme_ctrlr_opts *opts)
{
	printf("Attaching to %s\n", trid->traddr);

	return true;
}

static void
nvmf_cli_attach_cb(void *cb_ctx, const struct spdk_nvme_transport_id *trid,
	  struct spdk_nvme_ctrlr *ctrlr, const struct spdk_nvme_ctrlr_opts *opts)
{

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
    int rc;
    // TODO: error case should recycle resources
    struct wals_target *target = calloc(1, sizeof(struct wals_target));
    struct wals_cli_slice *cli_slice = calloc(1, sizeof(struct wals_cli_slice));

    cli_slice->blocklen = wals_bdev->bdev.blocklen;
    cli_slice->buf = spdk_zmalloc(LOG_BLOCKCNT * cli_slice->blocklen, 2 * 1024 * 1024, NULL,
					 SPDK_ENV_LCORE_ID_ANY, SPDK_MALLOC_DMA);

    if (cli_slice->buf == NULL) {
        SPDK_ERRLOG("Cannot allocate buffer for slice\n");
        return NULL;
    }

	struct sockaddr_in addr;
	struct addrinfo hints = {};
	struct addrinfo* addr_res = NULL;
	hints.ai_family = AF_INET;
	hints.ai_flags = AI_PASSIVE;

	getaddrinfo(config->target_log_info.address, config->target_log_info.port, &hints, &addr_res);
	memcpy(&addr, addr_res->ai_addr, sizeof(addr));

    for (int i = 0; i < NUM_TARGETS; i++) {
        if (g_rdma_cli_conns[i].status != RDMA_CLI_UNINITIALIZED) {
            // compare ip and port
            struct sockaddr_in* cur_addr = g_rdma_cli_conns[i].server_addr->ai_addr;
            if (cur_addr->sin_addr.s_addr == addr.sin_addr.s_addr
                && cur_addr->sin_port == addr.sin_port) {
                SPDK_NOTICELOG("Already created rdma connection.\n");
                cli_slice->rdma_conn = &g_rdma_cli_conns[i];
                TAILQ_INSERT_TAIL(&g_rdma_cli_conns[i].slices, cli_slice, tailq);
                break;
            }
        }
    }

    if (cli_slice->rdma_conn == NULL) {
        // no existing connection. need to build one 
        for (int i = 0; i < NUM_TARGETS; i++) {
            if (g_rdma_cli_conns[i].status == RDMA_CLI_UNINITIALIZED) {
                // this slot is empty. use it
                cli_slice->rdma_conn = &g_rdma_cli_conns[i];
                TAILQ_INIT(&g_rdma_cli_conns[i].slices);
                TAILQ_INSERT_TAIL(&g_rdma_cli_conns[i].slices, cli_slice, tailq);

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
                if (connection_poller == NULL) {
                    connection_poller = SPDK_POLLER_REGISTER(rdma_cli_connection_poller, NULL, 5 * 1000);
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

    for (int i = 0; i < NUM_TARGETS; i++) {
        if (g_nvmf_cli_conns[i].status != NVMF_CLI_UNINITIALIZED) {
            // compare ip and port
            struct sockaddr_in* cur_addr = g_nvmf_cli_conns[i].server_addr->ai_addr;
            if (strcmp(g_nvmf_cli_conns[i].transport_id.traddr, config->target_core_info.address) == 0
                && strcmp(g_nvmf_cli_conns[i].transport_id.trsvcid, config->target_core_info.port) == 0) {
                SPDK_NOTICELOG("Already created nvmf connection.\n");
                cli_slice->nvmf_conn = &g_nvmf_cli_conns[i];
                // TODO: change offset
                break;

            }
        }
    }

    if (cli_slice->nvmf_conn == NULL) {
        for (int i = 0; i < NUM_TARGETS; i++) {
            if (g_nvmf_cli_conns[i].status == NVMF_CLI_UNINITIALIZED) {
                spdk_nvme_trid_populate_transport(&g_nvmf_cli_conns[i].transport_id,
                    SPDK_NVME_TRANSPORT_RDMA);
                
                strcpy(g_nvmf_cli_conns[i].transport_id.traddr, config->target_core_info.address);
                strcpy(g_nvmf_cli_conns[i].transport_id.trsvcid, config->target_core_info.port);
                strcpy(g_nvmf_cli_conns[i].transport_id.subnqn, config->target_core_info.nqn);
                // TODO: should be in config
                g_nvmf_cli_conns[i].transport_id.adrfam = SPDK_NVMF_ADRFAM_IPV4;

                rc = spdk_nvme_probe(&g_nvmf_cli_conns[i].transport_id,
                    &g_nvmf_cli_conns[i],
                    nvmf_cli_probe_cb,
                    nvmf_cli_attach_cb,
                    nvmf_cli_remove_cb);

                if (rc != 0) {
                    
                }

            }
        }
        

    }

    target->log_blockcnt = LOG_BLOCKCNT;
    target->private_info = cli_slice;

    return target;
}

static void
cli_stop(struct wals_target *target, struct wals_bdev *wals_bdev)
{
    // TODO

}

static int
cli_submit_log_read_request(struct wals_target* target, void *data, uint64_t offset, uint64_t cnt, struct wals_bdev_io *wals_io)
{
    // TODO
    return 0;
}

static int
cli_submit_core_read_request(struct wals_target* target, void *data, uint64_t offset, uint64_t cnt, struct wals_bdev_io *wals_io)
{
    // TODO
    return 0;
}

static int
cli_submit_log_write_request(struct wals_target* target, void *data, uint64_t offset, uint64_t cnt, struct wals_bdev_io *wals_io)
{
    // TODO
    return 0;
}

static int 
rdma_cli_connection_poller(void* ctx) {
    for (int i = 0; i < NUM_TARGETS; i++) {
        switch (g_rdma_cli_conns[i].status) {
            case RDMA_CLI_INITIALIZED:
            {
                int rc;
                struct rdma_cm_id* cm_id = NULL;
                rc = rdma_create_id(g_rdma_cli_conns[i].channel, &cm_id, NULL, RDMA_PS_TCP);
                if (rc != 0) {
                    SPDK_ERRLOG("rdma_create_id failed\n");
                    g_rdma_cli_conns[i].status = RDMA_CLI_ERROR;
                    return SPDK_POLLER_BUSY;
                }
                g_rdma_cli_conns[i].cm_id = cm_id;

                rc = rdma_resolve_addr(cm_id, NULL, g_rdma_cli_conns[i].server_addr->ai_addr, 1000);
                if (rc != 0) {
                    SPDK_ERRLOG("rdma_resolve_addr failed\n");
                    g_rdma_cli_conns[i].status = RDMA_CLI_ERROR;
                    return SPDK_POLLER_BUSY;
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
                    return SPDK_POLLER_IDLE;
                }
                enum rdma_cm_event_type expected_event_type;
                if (g_rdma_cli_conns[i].status == RDMA_CLI_ADDR_RESOLVING) {
                    expected_event_type = RDMA_CM_EVENT_ADDR_RESOLVED;
                }
                else if (g_rdma_cli_conns[i].status == RDMA_CLI_ROUTE_RESOLVING) {
                    expected_event_type = RDMA_CM_EVENT_ROUTE_RESOLVED;
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
                    SPDK_NOTICELOG("max wr sge = %d, max wr num = %d, max cqe = %d, max qp = %d\n",
                        device_attr.max_sge, device_attr.max_qp_wr, device_attr.max_cqe, device_attr.max_qp);
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

                    struct wals_cli_slice* slice;

                    TAILQ_FOREACH(slice, &g_rdma_cli_conns[i].slices, tailq) {
                        
                    }


                    struct ibv_mr* ibv_mr = ibv_reg_mr(g_rdma_cli_conns[i].cm_id->qp->pd,
                        g_rdma_cli_conns[i].malloc_buf,
                        g_rdma_cli_conns[i].disk.blockcnt * g_rdma_cli_conns[i].disk.blocklen + 2 * sizeof(struct rdma_handshake),
                        IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ);

                    if (ibv_mr == NULL) {
                        SPDK_ERRLOG("failed to reg mr\n");
                        g_rdma_cli_conns[i].status = RDMA_CLI_ERROR;
                        break;
                    }
                    g_rdma_cli_conns[i].mr = ibv_mr;
                    
                    struct ibv_recv_wr wr, *bad_wr = NULL;
                    struct ibv_sge sge;

                    struct rdma_handshake* handshake = g_rdma_cli_conns[i].malloc_buf + g_rdma_cli_conns[i].disk.blockcnt * g_rdma_cli_conns[i].disk.blocklen;
                    struct rdma_handshake* remote_handshake = handshake + 1;
                    handshake->base_addr = g_rdma_cli_conns[i].malloc_buf;
                    handshake->rkey = ibv_mr->rkey;
                    handshake->block_cnt = g_rdma_cli_conns[i].disk.blockcnt;
                    handshake->block_size = g_rdma_cli_conns[i].disk.blocklen;

                    wr.wr_id = (uintptr_t)1;
                    wr.next = NULL;
                    wr.sg_list = &sge;
                    wr.num_sge = 1;

                    sge.addr = (uint64_t)remote_handshake;
                    sge.length = sizeof(struct rdma_handshake);
                    sge.lkey = ibv_mr->lkey;
                    g_rdma_cli_conns[i].remote_handshake = remote_handshake;

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
                    return SPDK_POLLER_IDLE;
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
                        return SPDK_POLLER_BUSY;
                    }

                    rc = rdma_destroy_id(g_rdma_cli_conns[i].cm_id);
                    if (rc != 0) {
                        SPDK_ERRLOG("cannot destroy id\n");
                        g_rdma_cli_conns[i].status = RDMA_CLI_ERROR;
                        return SPDK_POLLER_BUSY;
                    }
                    g_rdma_cli_conns[i].cm_id = NULL;

                    rc = ibv_destroy_cq(g_rdma_cli_conns[i].cq);
                    if (rc != 0) {
                        SPDK_ERRLOG("destroy cq failed\n");
                        g_rdma_cli_conns[i].status = RDMA_CLI_ERROR;
                        return SPDK_POLLER_BUSY;
                    }
                    g_rdma_cli_conns[i].cq = NULL;

                    rc = ibv_dereg_mr(g_rdma_cli_conns[i].mr);
                    if (rc != 0) {
                        SPDK_ERRLOG("failed to dereg mr\n");
                        g_rdma_cli_conns[i].status = RDMA_CLI_ERROR;
                        return SPDK_POLLER_BUSY;
                    }
                    g_rdma_cli_conns[i].mr = NULL;

                    g_rdma_cli_conns[i].status = RDMA_CLI_INITIALIZED;
                }
                else {
                    SPDK_ERRLOG("Should not receive event %d when connected\n", cm_event->event);
                }
                break;
            }
            case RDMA_CLI_CONNECTING:
            {
                struct rdma_handshake* handshake = g_rdma_cli_conns[i].remote_handshake - 1;
                int rc;
                struct rdma_cm_event* connect_event;
                struct ibv_send_wr send_wr, *bad_send_wr = NULL;
                struct ibv_sge send_sge;
                rc = rdma_get_cm_event(g_rdma_cli_conns[i].channel, &connect_event);
                if (rc != 0) {
                    if (errno == EAGAIN) {
                        // wait for server to accept. do nothing
                    }
                    else {
                        SPDK_ERRLOG("Unexpected CM error %d\n", errno);
                    }
                    return SPDK_POLLER_IDLE;
                }
                rc = rdma_ack_cm_event(connect_event);
                if (rc != 0) {
                    SPDK_ERRLOG("failed to ack event\n");
                    g_rdma_cli_conns[i].status = RDMA_CLI_ERROR;
                    break;
                }

                if (connect_event->event == RDMA_CM_EVENT_REJECTED) {
                    SPDK_NOTICELOG("Rejected. Try again...\n");
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

                    rc = ibv_dereg_mr(g_rdma_cli_conns[i].mr);
                    if (rc != 0) {
                        SPDK_ERRLOG("failed to dereg mr\n");
                        g_rdma_cli_conns[i].status = RDMA_CLI_ERROR;
                        break;
                    }
                    g_rdma_cli_conns[i].mr = NULL;

                    g_rdma_cli_conns[i].status = RDMA_CLI_INITIALIZED;
                    break;
                }
                else if (connect_event->event != RDMA_CM_EVENT_ESTABLISHED) {
                    SPDK_ERRLOG("invalid event type %d\n", connect_event->event);
                    break;
                }

                SPDK_NOTICELOG("connected. posting send...\n");
                SPDK_NOTICELOG("sending local addr %p rkey %d block_cnt %ld block_size %ld\n",
                    handshake->base_addr,
                    handshake->rkey,
                    handshake->block_cnt,
                    handshake->block_size);

                memset(&send_wr, 0, sizeof(send_wr));

                send_wr.wr_id = 2;
                send_wr.opcode = IBV_WR_SEND;
                send_wr.sg_list = &send_sge;
                send_wr.num_sge = 1;
                send_wr.send_flags = IBV_SEND_SIGNALED;

                send_sge.addr = (uint64_t)handshake;
                send_sge.length = sizeof(struct rdma_handshake);
                send_sge.lkey = g_rdma_cli_conns[i].mr->lkey;

                rc = ibv_post_send(g_rdma_cli_conns[i].cm_id->qp, &send_wr, &bad_send_wr);
                if (rc != 0) {
                    SPDK_ERRLOG("post send failed\n");
                    break;
                }
                g_rdma_cli_conns[i].status = RDMA_CLI_ESTABLISHED;
                break;
            }
            case RDMA_CLI_ESTABLISHED:
            {
                struct ibv_wc wc;
                struct rdma_handshake* handshake = g_rdma_cli_conns[i].remote_handshake - 1;
                int cnt = ibv_poll_cq(g_rdma_cli_conns[i].cq, 1, &wc);
                if (cnt < 0) {
                    SPDK_ERRLOG("ibv_poll_cq failed\n");
                    return SPDK_POLLER_IDLE;
                }

                if (cnt == 0) {
                    return SPDK_POLLER_IDLE;
                }

                if (wc.status != IBV_WC_SUCCESS) {
                    SPDK_ERRLOG("WC bad status %d\n", wc.status);
                    return SPDK_POLLER_IDLE;
                }

                if (wc.wr_id == 1) {
                    // recv complete
                    SPDK_NOTICELOG("received remote addr %p rkey %d block_cnt %ld block_size %ld\n",
                        g_rdma_cli_conns[i].remote_handshake->base_addr,
                        g_rdma_cli_conns[i].remote_handshake->rkey,
                        g_rdma_cli_conns[i].remote_handshake->block_cnt,
                        g_rdma_cli_conns[i].remote_handshake->block_size);
                    
                    if (g_rdma_cli_conns[i].remote_handshake->block_cnt != handshake->block_cnt ||
                        g_rdma_cli_conns[i].remote_handshake->block_size != handshake->block_size) {
                        SPDK_ERRLOG("buffer config handshake mismatch\n");
                        return -EINVAL;
                    }
                    SPDK_NOTICELOG("rdma handshake complete\n");
                    g_rdma_cli_conns[i].status = RDMA_CLI_CONNECTED;
                }
                else if (wc.wr_id == 2) {
                    SPDK_NOTICELOG("send req complete\n");
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

static struct wals_target_module g_rdma_cli_module = {
	.name = "rdma_cli",
	.start = cli_start,
	.stop = cli_stop,
    .submit_log_read_request = cli_submit_log_read_request,
	.submit_core_read_request = cli_submit_core_read_request,
	.submit_log_write_request = cli_submit_log_write_request,
};

TARGET_MODULE_REGISTER(&g_rdma_cli_module)

SPDK_LOG_REGISTER_COMPONENT(bdev_wals_rdma_cli)