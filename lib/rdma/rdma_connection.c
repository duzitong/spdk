/*   SPDX-License-Identifier: BSD-3-Clause
 *   Copyright (c) Intel Corporation.
 *   All rights reserved.
 */

#include "pthread.h"
#include "spdk/stdinc.h"

#include "spdk/rdma_connection.h"
#include "spdk/env.h"
#include "spdk/memory.h"

#include "spdk/likely.h"
#include "spdk/util.h"

struct rdma_connection* rdma_connection_alloc(
    bool is_server,
    const char* ip,
    const char* port,
    int context_length,
    void* base_addr,
    uint64_t block_size,
    uint64_t block_cnt,
	rdma_connection_connected_cb connected_cb)
{
    struct rdma_connection* rdma_conn = calloc(1, sizeof(struct rdma_connection));
	pthread_rwlock_init(&rdma_conn->lock, NULL);
    void* rdma_context = calloc(1, context_length);
    void* handshake_buffer = spdk_zmalloc(VALUE_2MB, VALUE_2MB, NULL,
                    SPDK_ENV_LCORE_ID_ANY, SPDK_MALLOC_DMA);
    rdma_conn->handshake_buf = handshake_buffer;
    rdma_conn->handshake_buf->base_addr = base_addr;
    rdma_conn->handshake_buf->block_cnt = block_cnt;
    rdma_conn->handshake_buf->block_size = block_size;
    rdma_conn->is_server = is_server;
    rdma_conn->rdma_context = rdma_context;
    rdma_conn->rdma_context_length = context_length;
	rdma_conn->connected_cb = connected_cb;
	rdma_conn->mem_map = spdk_mem_map_alloc(0, NULL, NULL);

	struct rdma_event_channel* channel = rdma_create_event_channel();
	rdma_conn->channel = channel;

	// need to make sure that the fd is set to non-blocking before 
	// entering the poller.
	int flags = fcntl(rdma_conn->channel->fd, F_GETFL);
	int rc = fcntl(rdma_conn->channel->fd, F_SETFL, flags | O_NONBLOCK);
	if (rc != 0) {
		SPDK_ERRLOG("fcntl failed\n");
		return NULL;
	}

	rdma_conn->status = is_server ? RDMA_SERVER_INITIALIZED : RDMA_CLI_INITIALIZED;

	struct addrinfo hints = {};
	struct addrinfo* addr_res = NULL;
	hints.ai_family = AF_INET;
	hints.ai_flags = AI_PASSIVE;
	rc = getaddrinfo(ip, port, &hints, &addr_res);
	if (rc != 0) {
		SPDK_ERRLOG("getaddrinfo failed\n");
		return NULL;
	}
	rdma_conn->server_addr = addr_res;
	SPDK_NOTICELOG("Alloc rdma conn %p: (%s:%s, is_server=%d)\n", rdma_conn, ip, port, is_server);

    return rdma_conn;
}

// Must not hold any lock when entering the function!
int rdma_connection_connect(struct rdma_connection* rdma_conn) {
	int rc = 0;

	rc = pthread_rwlock_trywrlock(&rdma_conn->lock);
	
	if (rc != 0) {
		if (rc != EBUSY) {
			SPDK_NOTICELOG("Unexpected error %d when trying to write-lock\n", rc);
		}
		return 0;
	}

	switch (rdma_conn->status) {
		case RDMA_CLI_INITIALIZED:
		case RDMA_SERVER_INITIALIZED:
		{
			struct rdma_cm_id* cm_id = NULL;
			rc = rdma_create_id(rdma_conn->channel, &cm_id, NULL, RDMA_PS_TCP);
			if (rc != 0) {
				SPDK_ERRLOG("rdma_create_id failed\n");
				goto end;
			}
			if (rdma_conn->is_server) {
				rdma_conn->parent_cm_id = cm_id;
				struct sockaddr_in addr;
				memcpy(&addr, rdma_conn->server_addr->ai_addr, sizeof(addr));
				rc = rdma_bind_addr(cm_id, (struct sockaddr*)&addr);
				if (rc != 0) {
					SPDK_ERRLOG("rdma bind addr failed\n");
					rdma_conn->status = RDMA_SERVER_ERROR;
					goto end;
				}
				rc = rdma_listen(cm_id, 3);
				if (rc != 0) {
					SPDK_ERRLOG("rdma listen failed\n");
					rdma_conn->status = RDMA_SERVER_ERROR;
					goto end;
				}

				SPDK_NOTICELOG("RDMA conn %p listening on port %d\n", rdma_conn, ntohs(addr.sin_port));
				rdma_conn->status = RDMA_SERVER_LISTENING;
			}
			else {
				rdma_conn->cm_id = cm_id;
                rc = rdma_resolve_addr(cm_id, NULL, rdma_conn->server_addr->ai_addr, 1000);
                if (rc != 0) {
                    SPDK_ERRLOG("rdma_resolve_addr failed\n");
                    rdma_conn->status = RDMA_CLI_ERROR;
                    goto end;
                }
                rdma_conn->status = RDMA_CLI_ADDR_RESOLVING;
			}
			break;
		}
		case RDMA_SERVER_LISTENING:
		{
			struct rdma_cm_event* connect_event;
			rc = rdma_get_cm_event(rdma_conn->channel, &connect_event);
			if (rc != 0) {
				if (errno == EAGAIN) {
					// waiting for connection
				}
				else {
					SPDK_ERRLOG("Unexpected CM error %d\n", errno);
				}
				goto end;
			}

			if (connect_event->event != RDMA_CM_EVENT_CONNECT_REQUEST) {
				// TODO: reconnection
				SPDK_ERRLOG("invalid event type %d\n", connect_event->event);
				goto end;
			}

			SPDK_NOTICELOG("RDMA conn %p received conn request\n", rdma_conn);

			struct ibv_context* ibv_context = connect_event->id->verbs;
			assert(ibv_context != NULL);
			struct ibv_device_attr device_attr = {};
			ibv_query_device(ibv_context, &device_attr);

			struct ibv_cq* ibv_cq = ibv_create_cq(ibv_context, 4096, NULL, NULL, 0);
			assert(ibv_cq != NULL);
			rdma_conn->cq = ibv_cq;

			// no SRQ here - only one qp
			// TODO: fine-tune these params; 
			struct ibv_qp_init_attr init_attr = {
				.send_cq = ibv_cq,
				.recv_cq = ibv_cq,
				.qp_type = IBV_QPT_RC,
				.cap = {
					.max_recv_sge = device_attr.max_sge,
					.max_send_sge = device_attr.max_sge,
					.max_send_wr = 256,
					.max_recv_wr = 256,
				}
			};

			rc = rdma_create_qp(connect_event->id, NULL, &init_attr);
			// SPDK_NOTICELOG("rdma_create_qp returns %d\n", rc);

			// the original cm id becomes useless from here.
			struct rdma_cm_id* child_cm_id = connect_event->id;
			rc = rdma_ack_cm_event(connect_event);
			rdma_conn->cm_id = child_cm_id;
			SPDK_NOTICELOG("RDMA conn %p acked conn request\n", rdma_conn);

            rdma_connection_register(
				rdma_conn,
				rdma_conn->handshake_buf,
				VALUE_2MB);
			
			// only data node provides base address
			if (rdma_conn->handshake_buf->base_addr != NULL) {
				rdma_connection_register(
					rdma_conn,
					rdma_conn->handshake_buf->base_addr,
					rdma_conn->handshake_buf->block_cnt * rdma_conn->handshake_buf->block_size + VALUE_2MB);
				
				rdma_conn->handshake_buf->rkey = rdma_connection_get_rkey(
					rdma_conn,
					rdma_conn->handshake_buf->base_addr,
					rdma_conn->handshake_buf->block_cnt * rdma_conn->handshake_buf->block_size + VALUE_2MB);
			}

			struct ibv_recv_wr wr, *bad_wr = NULL;
			struct ibv_sge sge;

			struct rdma_handshake* remote_handshake = rdma_conn->handshake_buf + 1;

			wr.wr_id = 1;
			wr.next = NULL;
			wr.sg_list = &sge;
			wr.num_sge = 1;
            rdma_connection_construct_sge(rdma_conn, &sge, remote_handshake, sizeof(struct rdma_handshake));

			rc = ibv_post_recv(child_cm_id->qp, &wr, &bad_wr);
			if (rc != 0) {
				SPDK_ERRLOG("post recv failed\n");
				goto end;
			}

			struct rdma_conn_param conn_param = {};

			conn_param.responder_resources = 16;
			conn_param.initiator_depth = 16;
			conn_param.retry_count = 7;
			conn_param.rnr_retry_count = 7;
			rc = rdma_accept(child_cm_id, &conn_param);

			if (rc != 0) {
				SPDK_ERRLOG("accept err\n");
				goto end;
			}
			rdma_conn->status = RDMA_SERVER_ACCEPTED;
			goto end;
		}
		case RDMA_SERVER_ACCEPTED:
		{
			struct rdma_cm_event* established_event;
			rc = rdma_get_cm_event(rdma_conn->channel, &established_event);
			if (rc != 0) {
				if (errno == EAGAIN) {
					// waiting for establish event
                    goto end;
				}
				else {
					SPDK_ERRLOG("Unexpected CM error %d\n", errno);
                    goto end;
				}
			}

			if (established_event->event != RDMA_CM_EVENT_ESTABLISHED) {
				SPDK_ERRLOG("incorrect established event %d\n", established_event->event);
				goto end;
			}

			SPDK_NOTICELOG("RDMA conn %p established. sending handshake ...\n", rdma_conn);
			
			struct ibv_send_wr send_wr, *bad_send_wr = NULL;
			memset(&send_wr, 0, sizeof(send_wr));

			struct ibv_sge send_sge;
			
			send_wr.wr_id = 2;
			send_wr.opcode = IBV_WR_SEND;
			send_wr.sg_list = &send_sge;
			send_wr.num_sge = 1;
			send_wr.send_flags = IBV_SEND_SIGNALED;

			rdma_connection_construct_sge(
				rdma_conn,
				&send_sge,
				rdma_conn->handshake_buf,
				sizeof(struct rdma_handshake));

			rc = ibv_post_send(rdma_conn->cm_id->qp, &send_wr, &bad_send_wr);
			if (rc != 0) {
				SPDK_ERRLOG("post send failed\n");
				goto end;
			}
			SPDK_NOTICELOG("RDMA conn %p sent local addr %p rkey %d length %ld\n",
				rdma_conn,
				rdma_conn->handshake_buf->base_addr,
				rdma_conn->handshake_buf->rkey,
				rdma_conn->handshake_buf->block_cnt * rdma_conn->handshake_buf->block_size);

			rdma_conn->status = RDMA_SERVER_ESTABLISHED;
			goto end;
		}
		case RDMA_SERVER_ESTABLISHED:
		{
			struct ibv_wc wc;
			int ret = ibv_poll_cq(rdma_conn->cq, 1, &wc);
			if (ret < 0) {
				SPDK_ERRLOG("ibv_poll_cq failed\n");
				goto end;
			}

			if (ret == 0) {
				goto end;
			}

			if (wc.status != IBV_WC_SUCCESS) {
				SPDK_ERRLOG("WC bad status %d\n", wc.status);
				goto end;
			}

			if (wc.wr_id == 1) {
				// recv complete
				struct rdma_handshake* remote_handshake = rdma_conn->handshake_buf + 1;
				SPDK_NOTICELOG("RDMA conn %p received remote addr %p rkey %d\n",
					rdma_conn,
					remote_handshake->base_addr,
					remote_handshake->rkey);

				rdma_conn->handshake_received = true;

				// if (remote_handshake->reconnect_cnt > 0) {
				// 	// TODO
				// 	SPDK_NOTICELOG("The node is reconnected\n");
				// 	if (remote_handshake->destage_tail.offset != 0 || remote_handshake->destage_tail.round != 0) {
				// 		pdisk->destage_tail->offset = remote_handshake->destage_tail.offset;
				// 		pdisk->destage_tail->round = remote_handshake->destage_tail.round;
				// 		pdisk->recover_tail.offset = remote_handshake->destage_tail.offset;
				// 		pdisk->recover_tail.round = remote_handshake->destage_tail.round;
				// 	}
				// }

			}
			else if (wc.wr_id == 2) {
				SPDK_NOTICELOG("send req complete\n");
				rdma_conn->handshake_sent = true;
			}
			else {
				SPDK_ERRLOG("Unexpected wr id %ld during handshake\n", wc.wr_id);
			}

			if (rdma_conn->handshake_received && rdma_conn->handshake_sent) {
				SPDK_NOTICELOG("rdma handshake complete\n");
				if (rdma_conn->connected_cb) {
					rdma_conn->connected_cb(rdma_conn->rdma_context, rdma_conn);
				}
				rdma_conn->status = RDMA_SERVER_CONNECTED;
			}

			goto end;
		}
		case RDMA_SERVER_CONNECTED:
		{
			struct rdma_cm_event* event;
			rc = rdma_get_cm_event(rdma_conn->channel, &event);

			if (rc != 0) {
				if (errno == EAGAIN) {
					// wait for server to accept. do nothing
				}
				else {
					SPDK_ERRLOG("Unexpected CM error %d\n", errno);
				}
				goto end;
			}

			rc = rdma_ack_cm_event(event);
			if (rc != 0) {
				SPDK_ERRLOG("failed to ack event\n");
				rdma_conn->status = RDMA_SERVER_ERROR;
				goto end;
			}

			if (event->event == RDMA_CM_EVENT_DISCONNECTED) {
				SPDK_NOTICELOG("Received disconnect event\n");
				rdma_conn->status = RDMA_SERVER_DISCONNECTED;
			}
			else {
				SPDK_ERRLOG("Should not receive event %d when connected\n", event->event);
			}
			goto end;
		}
		case RDMA_SERVER_ERROR:
		{
			rdma_connection_free(rdma_conn);
			rdma_conn->status = RDMA_SERVER_INITIALIZED;
			goto end;
		}
		case RDMA_SERVER_DISCONNECTED:
		{
			rdma_connection_free(rdma_conn);
			rdma_conn->status = RDMA_SERVER_INITIALIZED;
			goto end;
		}
		case RDMA_CLI_ADDR_RESOLVING:
		case RDMA_CLI_ROUTE_RESOLVING:
		{
			int rc;
			struct rdma_cm_event* event;
			rc = rdma_get_cm_event(rdma_conn->channel, &event);
			if (rc != 0) {
				if (errno == EAGAIN) {
					// wait for server to accept. do nothing
				}
				else {
					SPDK_ERRLOG("Unexpected CM error %d\n", errno);
				}
				goto end;
			}
			enum rdma_cm_event_type expected_event_type;
			if (rdma_conn->status == RDMA_CLI_ADDR_RESOLVING) {
				expected_event_type = RDMA_CM_EVENT_ADDR_RESOLVED;
			}
			else if (rdma_conn->status == RDMA_CLI_ROUTE_RESOLVING) {
				expected_event_type = RDMA_CM_EVENT_ROUTE_RESOLVED;
			}
			else {
				// should not happen
				SPDK_ERRLOG("ERROR\n");
				rdma_conn->status = RDMA_CLI_ERROR;
				goto end;
			}

			// suppose addr and resolving never fails
			if (event->event != expected_event_type) {
				SPDK_ERRLOG("unexpected event type %d (expect %d)\n",
					event->event,
					expected_event_type);
				rdma_conn->status = RDMA_CLI_ERROR;
				goto end;
			}
			if (rdma_conn->cm_id != event->id) {
				SPDK_ERRLOG("CM id mismatch\n");
				rdma_conn->status = RDMA_CLI_ERROR;
				goto end;
			}
			rc = rdma_ack_cm_event(event);
			if (rc != 0) {
				SPDK_ERRLOG("ack cm event failed\n");
				rdma_conn->status = RDMA_CLI_ERROR;
				goto end;
			}

			if (rdma_conn->status == RDMA_CLI_ADDR_RESOLVING) {
				rc = rdma_resolve_route(rdma_conn->cm_id, 1000);
				if (rc != 0) {
					SPDK_ERRLOG("resolve route failed\n");
					rdma_conn->status = RDMA_CLI_ERROR;
					goto end;
				}
				rdma_conn->status = RDMA_CLI_ROUTE_RESOLVING;
			}
			else if (rdma_conn->status == RDMA_CLI_ROUTE_RESOLVING) {
				struct ibv_context* ibv_context = rdma_conn->cm_id->verbs;
				struct ibv_device_attr device_attr = {};
				ibv_query_device(ibv_context, &device_attr);
				// SPDK_NOTICELOG("max wr sge = %d, max wr num = %d, max cqe = %d, max qp = %d\n",
				//     device_attr.max_sge, device_attr.max_qp_wr, device_attr.max_cqe, device_attr.max_qp);
				struct ibv_cq* ibv_cq = ibv_create_cq(ibv_context, 256, NULL, NULL, 0);
				if (ibv_cq == NULL) {
					SPDK_ERRLOG("Failed to create cq\n");
					rdma_conn->status = RDMA_CLI_ERROR;
					goto end;
				}
				rdma_conn->cq = ibv_cq;
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

				rc = rdma_create_qp(rdma_conn->cm_id, NULL, &init_attr);
				if (rc != 0) {
					SPDK_ERRLOG("rdma_create_qp fails %d\n", rc);
					rdma_conn->status = RDMA_CLI_ERROR;
					goto end;
				}

				struct ibv_recv_wr wr, *bad_wr = NULL;
				struct ibv_sge sge;

				struct rdma_handshake* remote_handshake = rdma_conn->handshake_buf + 1;

				rdma_connection_register(
					rdma_conn,
					rdma_conn->handshake_buf,
					VALUE_2MB);
				
				// only data node provides base address
				if (rdma_conn->handshake_buf->base_addr != NULL) {
					rdma_connection_register(
						rdma_conn,
						rdma_conn->handshake_buf->base_addr,
						rdma_conn->handshake_buf->block_cnt * rdma_conn->handshake_buf->block_size + VALUE_2MB);
					
					rdma_conn->handshake_buf->rkey = rdma_connection_get_rkey(
						rdma_conn,
						rdma_conn->handshake_buf->base_addr,
						rdma_conn->handshake_buf->block_cnt * rdma_conn->handshake_buf->block_size + VALUE_2MB);
				}

				wr.wr_id = (uintptr_t)2;
				wr.next = NULL;
				wr.sg_list = &sge;
				wr.num_sge = 1;
				rdma_connection_construct_sge(rdma_conn, &sge, remote_handshake, sizeof(struct rdma_handshake));

				rc = ibv_post_recv(rdma_conn->cm_id->qp, &wr, &bad_wr);
				if (rc != 0) {
					SPDK_ERRLOG("post recv failed\n");
					rdma_conn->status = RDMA_CLI_ERROR;
					goto end;
				}

				struct rdma_conn_param conn_param = {};

				conn_param.responder_resources = 16;
				conn_param.initiator_depth = 16;
				conn_param.retry_count = 7;
				conn_param.rnr_retry_count = 7;

				rc = rdma_connect(rdma_conn->cm_id, &conn_param);
				if (rc != 0) {
					SPDK_ERRLOG("rdma_connect failed\n");
					rdma_conn->status = RDMA_CLI_ERROR;
					goto end;
				}
				rdma_conn->status = RDMA_CLI_CONNECTING;
			}
			goto end;
		}
		case RDMA_CLI_CONNECTING:
		{
			int rc;
			struct rdma_cm_event* connect_event;
			rc = rdma_get_cm_event(rdma_conn->channel, &connect_event);
			if (rc != 0) {
				if (errno == EAGAIN) {
					// wait for server to accept. do nothing
				}
				else {
					SPDK_ERRLOG("Unexpected CM error %d\n", errno);
				}
				goto end;
			}
			rc = rdma_ack_cm_event(connect_event);
			if (rc != 0) {
				SPDK_ERRLOG("failed to ack event\n");
				rdma_conn->status = RDMA_CLI_ERROR;
				goto end;
			}

			if (connect_event->event == RDMA_CM_EVENT_REJECTED) {
				// remote not ready yet
				// destroy all rdma resources and try again
				rdma_conn->reject_cnt++;
				if (rdma_conn->reject_cnt % 1000 == 1) {
					SPDK_NOTICELOG("RDMA conn %p Rejected %d. Try again...\n", rdma_conn, rdma_conn->reject_cnt);
				}
				rdma_connection_free(rdma_conn);

				rdma_conn->status = RDMA_CLI_INITIALIZED;
				goto end;
			}
			else if (connect_event->event != RDMA_CM_EVENT_ESTABLISHED) {
				SPDK_ERRLOG("invalid event type %d\n", connect_event->event);
				goto end;
			}

			SPDK_NOTICELOG("connected. posting send...\n");
			SPDK_NOTICELOG("sending local addr %p rkey %d block_cnt %ld block_size %ld\n",
				rdma_conn->handshake_buf->base_addr,
				rdma_conn->handshake_buf->rkey,
				rdma_conn->handshake_buf->block_cnt,
				rdma_conn->handshake_buf->block_size);

			struct ibv_send_wr send_wr, *bad_send_wr = NULL;
			struct ibv_sge send_sge;
			memset(&send_wr, 0, sizeof(send_wr));

			send_wr.wr_id = 1;
			send_wr.opcode = IBV_WR_SEND;
			send_wr.sg_list = &send_sge;
			send_wr.num_sge = 1;
			send_wr.send_flags = IBV_SEND_SIGNALED;
			rdma_connection_construct_sge(rdma_conn, &send_sge, rdma_conn->handshake_buf, sizeof(struct rdma_handshake));

			rc = ibv_post_send(rdma_conn->cm_id->qp, &send_wr, &bad_send_wr);
			if (rc != 0) {
				SPDK_ERRLOG("post send failed\n");
				goto end;
			}
			rdma_conn->status = RDMA_CLI_ESTABLISHED;
			goto end;
		}
		case RDMA_CLI_ESTABLISHED:
		{
			struct ibv_wc wc;
			struct rdma_handshake* remote_handshake = rdma_conn->handshake_buf + 1;
			int cnt = ibv_poll_cq(rdma_conn->cq, 1, &wc);
			if (cnt < 0) {
				SPDK_ERRLOG("ibv_poll_cq failed\n");
				goto end;
			}

			if (cnt == 0) {
				goto end;
			}

			if (wc.status != IBV_WC_SUCCESS) {
				SPDK_ERRLOG("WC bad status %d\n", wc.status);
				goto end;
			}

			if (wc.wr_id == 2) {
				// recv complete
				SPDK_NOTICELOG("received remote addr %p rkey %d block_cnt %ld block_size %ld\n",
					remote_handshake->base_addr,
					remote_handshake->rkey,
					remote_handshake->block_cnt,
					remote_handshake->block_size);

				rdma_conn->handshake_received = true;
			}
			else if (wc.wr_id == 1) {
				// send cpl
				SPDK_NOTICELOG("send req complete\n");
				rdma_conn->handshake_sent = true;
			}
			else {
				SPDK_ERRLOG("Should not complete wrid = %ld\n", wc.wr_id);
			}

			if (rdma_conn->handshake_received && rdma_conn->handshake_sent) {
				SPDK_NOTICELOG("rdma handshake complete\n");
				if (rdma_conn->connected_cb) {
					rdma_conn->connected_cb(rdma_conn->rdma_context, rdma_conn);
				}

				rdma_conn->status = RDMA_CLI_CONNECTED;
			}
			goto end;
		}
		case RDMA_CLI_CONNECTED:
		{
			struct rdma_cm_event* cm_event;
			int rc = rdma_get_cm_event(rdma_conn->channel, &cm_event);
			if (rc != 0) {
				if (errno == EAGAIN) {
					// wait for server to accept. do nothing
				}
				else {
					SPDK_ERRLOG("Unexpected CM error %d\n", errno);
				}
				goto end;
			}
			rc = rdma_ack_cm_event(cm_event);
			if (rc != 0) {
				SPDK_ERRLOG("failed to ack event\n");
				rdma_conn->status = RDMA_CLI_ERROR;
				goto end;
			}

			if (cm_event->event == RDMA_CM_EVENT_DISCONNECTED) {
				SPDK_NOTICELOG("Received disconnect event\n");
				rdma_conn->status = RDMA_CLI_DISCONNECTED;
			}
			else {
				SPDK_ERRLOG("Should not receive event %d when connected\n", cm_event->event);
			}
			goto end;
		}
		case RDMA_CLI_ERROR:
		{
			rdma_connection_free(rdma_conn);
			rdma_conn->status = RDMA_CLI_INITIALIZED;
			goto end;
		}
		case RDMA_CLI_DISCONNECTED:
		{
			rdma_connection_free(rdma_conn);
			rdma_conn->status = RDMA_CLI_INITIALIZED;
			goto end;
		}
	}

end:
	if (pthread_rwlock_unlock(&rdma_conn->lock) != 0) {
		SPDK_ERRLOG("Failed to release rwlock\n");
	}
	return rc;
}

// Must hold write lock before entering the function.
void rdma_connection_free(struct rdma_connection* rdma_conn) {
	int rc;
	if (rdma_conn->is_server) {
		rdma_destroy_qp(rdma_conn->cm_id);
		if (rdma_conn->cm_id->qp != NULL) {
			SPDK_NOTICELOG("cannot free qp\n");
			rdma_conn->status = RDMA_SERVER_ERROR;
			return;
		}

		rc = rdma_destroy_id(rdma_conn->parent_cm_id);
		if (rc != 0) {
			SPDK_ERRLOG("cannot destroy id\n");
			rdma_conn->status = RDMA_SERVER_ERROR;
			return;
		}
		rdma_conn->parent_cm_id = NULL;

		rc = ibv_destroy_cq(rdma_conn->cq);
		if (rc != 0) {
			SPDK_ERRLOG("destroy cq failed\n");
			rdma_conn->status = RDMA_SERVER_ERROR;
			return;
		}
		rdma_conn->cq = NULL;
	}
	else {
		rdma_destroy_qp(rdma_conn->cm_id);
		if (rdma_conn->cm_id->qp != NULL) {
			SPDK_NOTICELOG("cannot free qp\n");
			rdma_conn->status = RDMA_CLI_ERROR;
			return;
		}

		rc = rdma_destroy_id(rdma_conn->cm_id);
		if (rc != 0) {
			SPDK_ERRLOG("cannot destroy id\n");
			rdma_conn->status = RDMA_CLI_ERROR;
			return;
		}
		rdma_conn->cm_id = NULL;

		rc = ibv_destroy_cq(rdma_conn->cq);
		if (rc != 0) {
			SPDK_ERRLOG("destroy cq failed\n");
			rdma_conn->status = RDMA_CLI_ERROR;
			return;
		}
		rdma_conn->cq = NULL;
	}

	for (int i = 0; i < rdma_conn->mr_cnt; i++) {
		spdk_mem_map_clear_translation(rdma_conn->mem_map,
			(uint64_t)rdma_conn->mr_arr[i]->addr,
			rdma_conn->mr_arr[i]->length);
		ibv_dereg_mr(rdma_conn->mr_arr[i]);
	}
	rdma_conn->mr_cnt = 0;
}

int rdma_connection_register(struct rdma_connection* rdma_conn, void* addr, uint32_t len) {
	struct ibv_mr* mr = ibv_reg_mr(rdma_conn->cm_id->pd,
		addr,
		len,
		IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ);
	
	if (mr == NULL) {
		SPDK_ERRLOG("Failed to register MR for addr %p len %d\n", addr, len);
		return -1;
	}

	if (rdma_conn->mr_cnt == RDMA_MAX_MR) {
		SPDK_ERRLOG("Failed to register new MR because the limit has been exceeded\n");
		return -1;
	}

	rdma_conn->mr_arr[rdma_conn->mr_cnt] = mr;
	rdma_conn->mr_cnt++;
	int rc = spdk_mem_map_set_translation(
		rdma_conn->mem_map,
		(uint64_t)addr,
		len,
		(uint64_t)mr);
	
	if (rc != 0) {
		SPDK_ERRLOG("Failed to set translation %p %d: %d\n", addr, len, rc);
	}

	return rc;
}

void rdma_connection_construct_sge(struct rdma_connection* rdma_conn, struct ibv_sge* sge, void* addr, uint32_t len) {
	sge->addr = (uint64_t)addr;
	sge->length = len;
	sge->lkey = rdma_connection_get_lkey(rdma_conn, addr, len);
}

static struct ibv_mr* _rdma_connection_get_mr(struct rdma_connection* rdma_conn, void* addr, uint32_t len) {
	uint64_t size;
	struct ibv_mr* mr = (struct ibv_mr*)spdk_mem_map_translate(rdma_conn->mem_map,
		(uint64_t)addr,
		&size);
	
	if (mr == NULL) {
		SPDK_ERRLOG("Cannot find translation for addr %p len %d\n",
			addr,
			len);
		return NULL;
	}

	// if (size < len) {
	// 	SPDK_ERRLOG("Not enough length for translating %p (%ld > %d)\n",
	// 		addr,
	// 		size,
	// 		len);
	// 	return NULL;
	// }
	return mr;
}

uint32_t rdma_connection_get_lkey(struct rdma_connection* rdma_conn, void* addr, uint32_t len) {
	struct ibv_mr* mr = _rdma_connection_get_mr(rdma_conn, addr, len);
	if (mr == NULL) {
		return 0;
	}

	return mr->lkey;
}

uint32_t rdma_connection_get_rkey(struct rdma_connection* rdma_conn, void* addr, uint32_t len) {
	struct ibv_mr* mr = _rdma_connection_get_mr(rdma_conn, addr, len);
	if (mr == NULL) {
		return 0;
	}

	return mr->rkey;
}

// no need to hold read lock, as the status will always change in the last phase.
bool rdma_connection_is_connected(struct rdma_connection* rdma_conn) {
	return rdma_conn != NULL && (rdma_conn->status == RDMA_SERVER_CONNECTED || rdma_conn->status == RDMA_CLI_CONNECTED);
}