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
#include "spdk/rdma.h"
#include <errno.h>

#include <infiniband/verbs.h>
#include <rdma/rdma_cma.h>

#define ADDR_BUF_SIZE 16
#define PORT_BUF_SIZE 8
#define CPU_BUF_SIZE 8

void* circular_buffer;
void* handshake_buffer;

// should be enough for IPv4
char addr_buf[ADDR_BUF_SIZE];
char port_buf[PORT_BUF_SIZE];
char cpu_buf[CPU_BUF_SIZE] = "0x40";

void check_strcpy(char* src, size_t buf_size) {
	size_t src_len = strlen(src);

	// buffer size need to be larger because of null ending
	if (src_len >= buf_size) {
		SPDK_ERRLOG("buffer not long enough for %s\n", src);
		exit(-1);
	}
}

void strcpy_s(void* dst, char* src, size_t dst_size) {
	check_strcpy(src, dst_size);
	strcpy(dst, src);
}

int main(int argc, char **argv)
{
	int rc;
	struct spdk_env_opts opts;

	spdk_env_opts_init(&opts);

	int op;

	while ((op = getopt(argc, argv, "a:p:m:")) != -1) {
		switch (op) {
			case 'a':
				strcpy_s(addr_buf, optarg, ADDR_BUF_SIZE);
				break;
			case 'p':
				strcpy_s(port_buf, optarg, PORT_BUF_SIZE);
				break;
			case 'm':
				strcpy_s(cpu_buf, optarg, CPU_BUF_SIZE);
				break;
		}
	}

	if (!strlen(addr_buf) || !strlen(port_buf)) {
		printf("not enough arguments.\n");
		return 1;
	}

	opts.name = "persist";
	opts.core_mask = cpu_buf;
	if (spdk_env_init(&opts) < 0) {
		fprintf(stderr, "Unable to initialize SPDK env\n");
		return 1;
	}

	// circular_buffer = spdk_zmalloc(BUFFER_SIZE + 2 * sizeof(struct rdma_handshake), 2 * 1024 * 1024, NULL,
	// 				 SPDK_ENV_LCORE_ID_ANY, SPDK_MALLOC_DMA);

	handshake_buffer = spdk_zmalloc(2 * sizeof(struct rdma_handshake), 2 * 1024 * 1024, NULL,
					 SPDK_ENV_LCORE_ID_ANY, SPDK_MALLOC_DMA);
	
	// int n = 0;
	// struct ibv_device** ibv_list = ibv_get_device_list(&n);

	// if (n == 0) {
	// 	SPDK_ERRLOG("cannot find ib devices\n");
	// 	return -EINVAL;
	// }

	struct rdma_event_channel* rdma_channel = rdma_create_event_channel();

	struct rdma_cm_id* cm_id = NULL;
	rc = rdma_create_id(rdma_channel, &cm_id, NULL, RDMA_PS_TCP);
	if (rc != 0) {
		printf("rdma_create_id failed\n");
		return 1;
	}

	struct sockaddr_in addr;
	struct addrinfo hints = {};
	struct addrinfo* addr_res = NULL;
	hints.ai_family = AF_INET;
	hints.ai_flags = AI_PASSIVE;
	rc = getaddrinfo(addr_buf, port_buf, &hints, &addr_res);
	if (rc != 0) {
		printf("getaddrinfo failed\n");
		return 1;
	}
	memcpy(&addr, addr_res->ai_addr, sizeof(addr));
	rc = rdma_bind_addr(cm_id, (struct sockaddr*)&addr);
	if (rc != 0) {
		printf("rdma bind addr failed\n");
		return 1;
	}
	rc = rdma_listen(cm_id, 3);
	if (rc != 0) {
		printf("rdma listen failed\n");
		return 1;
	}

	printf("listening on port %d\n", ntohs(addr.sin_port));

	struct rdma_cm_event* connect_event, *established_event;
	rdma_get_cm_event(rdma_channel, &connect_event);

	if (connect_event->event != RDMA_CM_EVENT_CONNECT_REQUEST) {
		SPDK_ERRLOG("invalid event type\n");
		return 1;
	}

	printf("received conn request\n");
	struct ibv_context* ibv_context = connect_event->id->verbs;
	struct ibv_device_attr device_attr = {};
	ibv_query_device(ibv_context, &device_attr);
	struct ibv_pd* ibv_pd = ibv_alloc_pd(ibv_context);
	struct ibv_mr* ibv_mr_handshake = ibv_reg_mr(ibv_pd,
		handshake_buffer,
		2 * sizeof(struct rdma_handshake),
		IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE);
	
	struct ibv_mr* ibv_mr_circular;

	struct ibv_cq* ibv_cq = ibv_create_cq(ibv_context, 4096, NULL, NULL, 0);
	assert(ibv_cq != NULL);
	assert(ibv_mr_handshake != NULL);
	assert(ibv_context != NULL);
	assert(ibv_pd != NULL);

	// no SRQ here - only one qp
	// TODO: fine-tune these params; 
	struct ibv_qp_init_attr init_attr = {
		.send_cq = ibv_cq,
		.recv_cq = ibv_cq,
		.qp_type = IBV_QPT_RC,
		.cap = {
			.max_recv_sge = 1,
			.max_send_sge = 1,
			.max_send_wr = 1,
			.max_recv_wr = 1,
		}
	};

	int x = rdma_create_qp(connect_event->id, ibv_pd, &init_attr);
	printf("rdma_create_qp returns %d\n", x);
	int err = errno;
	printf("err = %d\n", err);
	// the original cm id becomes useless from here.
	struct rdma_cm_id* child_cm_id = connect_event->id;
	rc = rdma_ack_cm_event(connect_event);
	assert(rc == 0);
	printf("acked conn request\n");

	struct ibv_recv_wr wr, *bad_wr = NULL;
	struct ibv_sge sge, send_sge;

	struct rdma_handshake* handshake = handshake_buffer;

	wr.wr_id = 1;
	wr.next = NULL;
	wr.sg_list = &sge;
	wr.num_sge = 1;

	sge.addr = (uint64_t)(handshake + 1);
	sge.length = sizeof(struct rdma_handshake);
	sge.lkey = ibv_mr_handshake->lkey;
	rc = ibv_post_recv(child_cm_id->qp, &wr, &bad_wr);
	if (rc != 0) {
		SPDK_ERRLOG("post recv failed\n");
		return -1;
	}


	struct rdma_conn_param conn_param = {};

	conn_param.responder_resources = device_attr.max_qp_rd_atom;
	conn_param.initiator_depth = device_attr.max_qp_init_rd_atom;
	conn_param.retry_count = 7;
	conn_param.rnr_retry_count = 7;
	rc = rdma_accept(child_cm_id, &conn_param);

	if (rc != 0) {
		printf("accept err = %d\n", err);
	}
	assert(rc == 0);

	rdma_get_cm_event(rdma_channel, &established_event);
	if (established_event->event != RDMA_CM_EVENT_ESTABLISHED) {
		SPDK_ERRLOG("incorrect established event\n");
		return 1;
	}

	printf("connected. posting send...\n");

	struct ibv_send_wr send_wr, *bad_send_wr = NULL;
	memset(&send_wr, 0, sizeof(send_wr));

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

		if (wc.status != IBV_WC_SUCCESS) {
			printf("WC bad status %d\n", wc.status);
			return 1;
		}

		if (wc.wr_id == 1) {
			// recv complete
			struct rdma_handshake* remote_handshake = handshake + 1;
			printf("received remote addr %p rkey %d length %ld (%ld MB)\n",
				remote_handshake->base_addr,
				remote_handshake->rkey,
				remote_handshake->length,
				remote_handshake->length / 1048576);
			handshake_recv_cpl = true;

			circular_buffer = spdk_zmalloc(remote_handshake->length,
				2 * 1024 * 1024,
				NULL,
				SPDK_ENV_LCORE_ID_ANY,
				SPDK_MALLOC_DMA);

			ibv_mr_circular = ibv_reg_mr(
				ibv_pd,
				circular_buffer,
				remote_handshake->length,
				IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_LOCAL_WRITE);
			
			handshake->base_addr = circular_buffer;
			handshake->rkey = ibv_mr_circular->rkey;
			handshake->length = remote_handshake->length;

			send_wr.wr_id = 2;
			send_wr.opcode = IBV_WR_SEND;
			send_wr.sg_list = &send_sge;
			send_wr.num_sge = 1;
			send_wr.send_flags = IBV_SEND_SIGNALED;

			send_sge.addr = (uint64_t)handshake;
			send_sge.length = sizeof(struct rdma_handshake);
			send_sge.lkey = ibv_mr_handshake->lkey;
			
			rc = ibv_post_send(child_cm_id->qp, &send_wr, &bad_send_wr);
			if (rc != 0) {
				printf("post send failed\n");
				return 1;
			}
			printf("sent local addr %p rkey %d length %ld\n",
				handshake->base_addr,
				handshake->rkey,
				handshake->length);
		}
		else if (wc.wr_id == 2) {
			printf("send req complete\n");
			handshake_send_cpl = true;
		}
	}

	printf("rdma handshake complete\n");
	printf("press anything to quit...\n");
	char buf[128];
	scanf("%s", buf);
	return 0;
}
