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

#include "bdev_target.h"
#include "spdk/rpc.h"
#include "spdk/util.h"
#include "spdk/uuid.h"
#include "spdk/string.h"
#include "spdk/log.h"

struct rpc_construct_target {
	char *name;
	char *uuid;
	char *ip;
	char *port;
	uint64_t num_blocks;
	uint32_t block_size;
	uint32_t optimal_io_boundary;
};

static void
free_rpc_construct_target(struct rpc_construct_target *r)
{
	free(r->name);
	free(r->uuid);
	free(r->ip);
	free(r->port);
}

static const struct spdk_json_object_decoder rpc_construct_target_decoders[] = {
	{"name", offsetof(struct rpc_construct_target, name), spdk_json_decode_string, true},
	{"uuid", offsetof(struct rpc_construct_target, uuid), spdk_json_decode_string, true},
	{"ip", offsetof(struct rpc_construct_target, ip), spdk_json_decode_string, true},
	{"port", offsetof(struct rpc_construct_target, port), spdk_json_decode_string, true},
	{"num_blocks", offsetof(struct rpc_construct_target, num_blocks), spdk_json_decode_uint64},
	{"block_size", offsetof(struct rpc_construct_target, block_size), spdk_json_decode_uint32},
	{"optimal_io_boundary", offsetof(struct rpc_construct_target, optimal_io_boundary), spdk_json_decode_uint32, true},
};

static void
rpc_bdev_target_create(struct spdk_jsonrpc_request *request,
		       const struct spdk_json_val *params)
{
	struct rpc_construct_target req = {NULL};
	struct spdk_json_write_ctx *w;
	struct spdk_uuid *uuid = NULL;
	struct spdk_uuid decoded_uuid;
	struct spdk_bdev *bdev;
	int rc = 0;

	if (spdk_json_decode_object(params, rpc_construct_target_decoders,
				    SPDK_COUNTOF(rpc_construct_target_decoders),
				    &req)) {
		SPDK_DEBUGLOG(bdev_target, "spdk_json_decode_object failed\n");
		spdk_jsonrpc_send_error_response(request, SPDK_JSONRPC_ERROR_INTERNAL_ERROR,
						 "spdk_json_decode_object failed");
		goto cleanup;
	}

	if (req.num_blocks == 0) {
		spdk_jsonrpc_send_error_response(request, -EINVAL,
						 "Disk num_blocks must be greater than 0");
		goto cleanup;
	}

	if (req.block_size != TARGET_BLOCK_SIZE) {
		spdk_jsonrpc_send_error_response(request, -EINVAL, "Block size must equal to TARGET_BLOCK_SIZE");
		goto cleanup;
	}

	if (req.uuid) {
		if (spdk_uuid_parse(&decoded_uuid, req.uuid)) {
			spdk_jsonrpc_send_error_response(request, -EINVAL,
							 "Failed to parse bdev UUID");
			goto cleanup;
		}
		uuid = &decoded_uuid;
	}

	rc = create_target_disk(&bdev, req.name, req.ip, req.port, uuid, req.num_blocks, req.block_size,
				req.optimal_io_boundary);
	if (rc) {
		spdk_jsonrpc_send_error_response(request, rc, spdk_strerror(-rc));
		goto cleanup;
	}

	free_rpc_construct_target(&req);

	w = spdk_jsonrpc_begin_result(request);
	spdk_json_write_string(w, spdk_bdev_get_name(bdev));
	spdk_jsonrpc_end_result(request, w);
	return;

cleanup:
	free_rpc_construct_target(&req);
}
SPDK_RPC_REGISTER("bdev_target_create", rpc_bdev_target_create, SPDK_RPC_RUNTIME)
SPDK_RPC_REGISTER_ALIAS_DEPRECATED(bdev_target_create, construct_target_bdev)

struct rpc_delete_target {
	char *name;
};

static void
free_rpc_delete_target(struct rpc_delete_target *r)
{
	free(r->name);
}

static const struct spdk_json_object_decoder rpc_delete_target_decoders[] = {
	{"name", offsetof(struct rpc_delete_target, name), spdk_json_decode_string},
};

static void
rpc_bdev_target_delete_cb(void *cb_arg, int bdeverrno)
{
	struct spdk_jsonrpc_request *request = cb_arg;

	if (bdeverrno == 0) {
		spdk_jsonrpc_send_bool_response(request, true);
	} else {
		spdk_jsonrpc_send_error_response(request, bdeverrno, spdk_strerror(-bdeverrno));
	}
}

static void
rpc_bdev_target_delete(struct spdk_jsonrpc_request *request,
		       const struct spdk_json_val *params)
{
	struct rpc_delete_target req = {NULL};

	if (spdk_json_decode_object(params, rpc_delete_target_decoders,
				    SPDK_COUNTOF(rpc_delete_target_decoders),
				    &req)) {
		SPDK_DEBUGLOG(bdev_target, "spdk_json_decode_object failed\n");
		spdk_jsonrpc_send_error_response(request, SPDK_JSONRPC_ERROR_INTERNAL_ERROR,
						 "spdk_json_decode_object failed");
		goto cleanup;
	}

	delete_target_disk(req.name, rpc_bdev_target_delete_cb, request);

cleanup:
	free_rpc_delete_target(&req);
}
SPDK_RPC_REGISTER("bdev_target_delete", rpc_bdev_target_delete, SPDK_RPC_RUNTIME)
SPDK_RPC_REGISTER_ALIAS_DEPRECATED(bdev_target_delete, delete_target_bdev)