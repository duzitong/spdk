/*-
 *   BSD LICENSE
 *
 *   Copyright (c) Intel Corporation.
 *   All rights reserved.
 *
 *   Redistribution and use in source and binary forms, with or without
 *   modification, are permitted provided that the following conditions
 *   are met:
 *
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in
 *       the documentation and/or other materials provided with the
 *       distribution.
 *     * Neither the name of Intel Corporation nor the names of its
 *       contributors may be used to endorse or promote products derived
 *       from this software without specific prior written permission.
 *
 *   THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 *   "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 *   LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 *   A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 *   OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 *   SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 *   LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 *   DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 *   THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 *   (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 *   OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

#include "spdk/stdinc.h"

#include "spdk/nvme.h"
#include "spdk/vmd.h"
#include "spdk/nvme_zns.h"
#include "spdk/env.h"
#include "spdk/string.h"
#include "spdk/log.h"
#include "spdk/assert.h"
#include <errno.h>

#include <infiniband/verbs.h>
#include <rdma/rdma_cma.h>

#define BUFFER_SIZE 512 * 1024 * 1024
void* circular_buffer;

static struct rdma_handshake {
	void* base_addr;
	uint32_t rkey;
};

// should be enough for IPv4
char addr_buf[16];
char port_buf[8];
char cpu_buf[8] = "0x40";

struct rdma_handshake *local_handshake, *remote_handshake;

int main(int argc, char **argv)
{
	int rc;
	struct spdk_env_opts opts;

	spdk_env_opts_init(&opts);

	int op;

	while ((op = getopt(argc, argv, "a:p:m:")) != -1) {
		switch (op) {
			case 'a':
				memcpy(addr_buf, optarg, strlen(optarg));
				break;
			case 'p':
				memcpy(port_buf, optarg, strlen(optarg));
				break;
			case 'm':
				memcpy(cpu_buf, optarg, strlen(optarg));
				break;
		}
	}

	if (!strlen(addr_buf) || !strlen(port_buf)) {
		printf("not enough arguments.\n");
		return 1;
	}

	opts.name = "persist_client";
	opts.core_mask = cpu_buf;
	if (spdk_env_init(&opts) < 0) {
		fprintf(stderr, "Unable to initialize SPDK env\n");
		return 1;
	}

	circular_buffer = spdk_zmalloc(BUFFER_SIZE, 2 * 1024 * 1024, NULL,
					 SPDK_ENV_SOCKET_ID_ANY, SPDK_MALLOC_DMA);
	local_handshake = spdk_zmalloc(sizeof(*local_handshake), 0, NULL,
					 SPDK_ENV_SOCKET_ID_ANY, SPDK_MALLOC_DMA);
	remote_handshake = spdk_zmalloc(sizeof(*remote_handshake), 0, NULL,
					 SPDK_ENV_SOCKET_ID_ANY, SPDK_MALLOC_DMA);
	
	// int n = 0;
	// struct ibv_device** ibv_list = ibv_get_device_list(&n);

	// if (n == 0) {
	// 	SPDK_ERRLOG("cannot find ib devices\n");
	// 	return -EINVAL;
	// }
	struct rdma_event_channel* rdma_channel = rdma_create_event_channel();

	struct rdma_cm_id* cm_id = NULL;
	
	rdma_create_id(rdma_channel, &cm_id, NULL, RDMA_PS_TCP);
	struct sockaddr_in addr;
	struct addrinfo hints = {};
	struct addrinfo* addr_res = NULL;
	hints.ai_family = AF_INET;
	hints.ai_flags = AI_PASSIVE;

	getaddrinfo(addr_buf, port_buf, &hints, &addr_res);
	memcpy(&addr, addr_res->ai_addr, sizeof(addr));

	rdma_resolve_addr(cm_id, NULL, addr_res->ai_addr, 1000);
	struct rdma_cm_event* resolve_addr_event, *resolve_route_event, *connect_event;
	rdma_get_cm_event(rdma_channel, &resolve_addr_event);
	if (resolve_addr_event->event != RDMA_CM_EVENT_ADDR_RESOLVED) {
		SPDK_ERRLOG("invalid event type\n");
		return -EINVAL;
	}
	assert(cm_id == resolve_addr_event->id);
	rdma_ack_cm_event(resolve_addr_event);

	rdma_resolve_route(cm_id, 1000);
	rdma_get_cm_event(rdma_channel, &resolve_route_event);
	if (resolve_route_event->event != RDMA_CM_EVENT_ROUTE_RESOLVED) {
		SPDK_ERRLOG("invalid event type\n");
		return -EINVAL;
	}
	assert(cm_id == resolve_route_event->id);
	rdma_ack_cm_event(resolve_route_event);

	struct ibv_context* ibv_context = cm_id->verbs;
	struct ibv_device_attr device_attr = {};
	ibv_query_device(ibv_context, &device_attr);
	SPDK_DEBUGLOG(bdev_target, "max wr sge = %d, max wr num = %d, max cqe = %d\n",
		device_attr.max_sge, device_attr.max_qp_wr, device_attr.max_cqe);
	struct ibv_pd* ibv_pd = ibv_alloc_pd(ibv_context);
	struct ibv_mr* data_mr = ibv_reg_mr(ibv_pd,
		circular_buffer,
		BUFFER_SIZE,
		IBV_ACCESS_LOCAL_WRITE);
	struct ibv_mr* local_handshake_mr = ibv_reg_mr(ibv_pd,
		local_handshake,
		sizeof(struct rdma_handshake),
		IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE);
	struct ibv_mr* remote_handshake_mr = ibv_reg_mr(ibv_pd,
		remote_handshake,
		sizeof(struct rdma_handshake),
		IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE);

	struct ibv_cq* ibv_cq = ibv_create_cq(ibv_context, 256, NULL, NULL, 0);

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

	int x = rdma_create_qp(cm_id, ibv_pd, &init_attr);
	SPDK_NOTICELOG("rdma_create_qp returns %d\n", x);

	struct ibv_recv_wr wr, *bad_wr = NULL;
	struct ibv_sge sge, send_sge;

	wr.wr_id = 1;
	wr.next = NULL;
	wr.sg_list = &sge;
	wr.num_sge = 1;

	sge.addr = (uint64_t)remote_handshake;
	sge.length = sizeof(struct rdma_handshake);
	sge.lkey = remote_handshake_mr->lkey;

	ibv_post_recv(cm_id->qp, &wr, &bad_wr);

	struct rdma_conn_param conn_param = {};

	conn_param.responder_resources = 16;
	conn_param.initiator_depth = 16;
	conn_param.retry_count = 7;
	conn_param.rnr_retry_count = 7;

	rdma_connect(cm_id, &conn_param);
	rdma_get_cm_event(rdma_channel, &connect_event);
	if (connect_event->event != RDMA_CM_EVENT_ESTABLISHED) {
		SPDK_ERRLOG("invalid event type %d\n", connect_event->event);
		return -EINVAL;
	}

	assert(connect_event->id == cm_id);

	SPDK_NOTICELOG("connected. posting send...\n");

	struct ibv_send_wr send_wr, *bad_send_wr = NULL;
	memset(&send_wr, 0, sizeof(send_wr));

	send_wr.wr_id = 2;
	send_wr.opcode = IBV_WR_SEND;
	send_wr.sg_list = &send_sge;
	send_wr.num_sge = 1;
	send_wr.send_flags = IBV_SEND_SIGNALED;

	send_sge.addr = (uint64_t)local_handshake;
	send_sge.length = sizeof(struct rdma_handshake);
	send_sge.lkey = local_handshake_mr->lkey;

	// TODO: use separate buffer
	local_handshake->base_addr = circular_buffer;
	local_handshake->rkey = data_mr->rkey;

	SPDK_NOTICELOG("sending local addr %p rkey %d\n", local_handshake->base_addr, local_handshake->rkey);
	ibv_post_send(cm_id->qp, &send_wr, &bad_send_wr);
	struct ibv_wc wc;
	bool handshake_send_cpl = false;
	bool handshake_recv_cpl = false;
	while (!handshake_send_cpl || !handshake_recv_cpl)
	{
		int ret = ibv_poll_cq(ibv_cq, 1, &wc);
		if (ret < 0) {
			SPDK_ERRLOG("ibv_poll_cq failed\n");
			return -EINVAL;
		}

		if (ret == 0) {
			continue;
		}

		if (wc.wr_id == 1) {
			// recv complete
			SPDK_NOTICELOG("received remote addr %p rkey %d\n", remote_handshake->base_addr, remote_handshake->rkey);
			handshake_recv_cpl = true;
		}
		else if (wc.wr_id == 2) {
			SPDK_NOTICELOG("send req complete\n");
			handshake_send_cpl = true;
		}
	}

	SPDK_NOTICELOG("rdma handshake complete\n");

	printf("press anything to quit...\n");
	char buf[128];
	scanf("%s", buf);
	return 0;
}
