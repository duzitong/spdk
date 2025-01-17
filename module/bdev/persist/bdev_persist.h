/*-
 *   BSD LICENSE
 *
 *   Copyright (c) Intel Corporation.
 *   All rights reserved.
 *   Copyright (c) 2021 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
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

#ifndef SPDK_BDEV_PERSIST_H
#define SPDK_BDEV_PERSIST_H

#include "spdk/stdinc.h"

#include "spdk/nvme.h"
#include "spdk/bdev.h"
#include "spdk/trace.h"
#include "spdk_internal/trace_defs.h"
#include "../wals/bdev_wals.h"

#define PERSIST_BLOCK_SIZE 512
#define PERSIST_WC_BATCH_SIZE 32
#define PERSIST_METADATA_VERSION		10086	// TODO: merge with wal
// #define PERSIST_MD_LEN 8
#define RPC_MAX_PEERS 3

#define TRACE_BDEV_WRITE_MEMCPY_START	SPDK_TPOINT_ID(TRACE_GROUP_BDEV, 0x20)
#define TRACE_BDEV_WRITE_MEMCPY_END		SPDK_TPOINT_ID(TRACE_GROUP_BDEV, 0x21)
#define TRACE_BDEV_RDMA_POST_SEND_WRITE_START		SPDK_TPOINT_ID(TRACE_GROUP_BDEV, 0x22)
#define TRACE_BDEV_RDMA_POST_SEND_WRITE_END		SPDK_TPOINT_ID(TRACE_GROUP_BDEV, 0x23)
#define TRACE_BDEV_CQ_POLL		SPDK_TPOINT_ID(TRACE_GROUP_BDEV, 0x24)

typedef void (*spdk_delete_persist_complete)(void *cb_arg, int bdeverrno);

struct rpc_persist_peer_info {
	char* remote_ip;
	char* remote_port;
	char* local_port;
};

struct rpc_persist_peers {
	size_t num_peers;
	struct rpc_persist_peer_info peers[RPC_MAX_PEERS];
};

struct rpc_construct_persist {
	char *name;
	char *uuid;
	char *ip;
	char *port;
	bool attach_disk;
	struct rpc_persist_peers peers;
};

int create_persist_disk(struct spdk_bdev **bdev,
    const char *name,
    const char* ip,
    const char* port,
    const struct spdk_uuid *uuid,
    bool attach_disk,
    struct rpc_persist_peer_info* peer_info_array,
    size_t num_peers);

void delete_persist_disk(const char *name, spdk_delete_persist_complete cb_fn, void *cb_arg);

#endif /* SPDK_BDEV_PERSIST_H */
