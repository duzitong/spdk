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

#ifndef SPDK_BDEV_TARGET_H
#define SPDK_BDEV_TARGET_H

#include "spdk/stdinc.h"

#include "spdk/nvme.h"
#include "spdk/bdev.h"
#include "spdk/trace.h"
#include "spdk_internal/trace_defs.h"

#define TARGET_BLOCK_SIZE 512
#define TARGET_WC_BATCH_SIZE 32
// #define TARGET_MD_LEN 8

#define TRACE_BDEV_WRITE_MEMCPY_START	SPDK_TPOINT_ID(TRACE_GROUP_BDEV, 0x20)
#define TRACE_BDEV_WRITE_MEMCPY_END		SPDK_TPOINT_ID(TRACE_GROUP_BDEV, 0x21)
#define TRACE_BDEV_RDMA_POST_SEND_WRITE_START		SPDK_TPOINT_ID(TRACE_GROUP_BDEV, 0x22)
#define TRACE_BDEV_RDMA_POST_SEND_WRITE_END		SPDK_TPOINT_ID(TRACE_GROUP_BDEV, 0x23)
#define TRACE_BDEV_CQ_POLL		SPDK_TPOINT_ID(TRACE_GROUP_BDEV, 0x24)

typedef void (*spdk_delete_target_complete)(void *cb_arg, int bdeverrno);

int create_target_disk(struct spdk_bdev **bdev, const char *name, const char* ip, const char* port, const struct spdk_uuid *uuid,
		        uint64_t num_blocks, uint32_t block_size, uint32_t optimal_io_boundary, bool has_md);

void delete_target_disk(const char *name, spdk_delete_target_complete cb_fn, void *cb_arg);

#endif /* SPDK_BDEV_TARGET_H */
