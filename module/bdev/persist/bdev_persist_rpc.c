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

#include "bdev_persist.h"
#include "spdk/rpc.h"
#include "spdk/util.h"
#include "spdk/uuid.h"
#include "spdk/string.h"
#include "spdk/log.h"


static const struct spdk_json_object_decoder rpc_bdev_peer_decoders[] = {
	{"remote_ip", offsetof(struct rpc_persist_peer_info, remote_ip), spdk_json_decode_string},
	{"remote_port", offsetof(struct rpc_persist_peer_info, remote_port), spdk_json_decode_string},
	{"local_port", offsetof(struct rpc_persist_peer_info, local_port), spdk_json_decode_string},
};

static int decode_peer(const struct spdk_json_val* val, void* out) {
	return spdk_json_decode_object(val, rpc_bdev_peer_decoders, SPDK_COUNTOF(rpc_bdev_peer_decoders), out);
}

static int decode_peers(const struct spdk_json_val* val, void* out) {
	struct rpc_persist_peers* peers = out;
	return spdk_json_decode_array(val, decode_peer, peers->peers, RPC_MAX_PEERS, &peers->num_peers, sizeof(struct rpc_persist_peer_info));
}

static void
free_rpc_construct_persist(struct rpc_construct_persist *r)
{
	free(r->name);
	free(r->uuid);
	free(r->ip);
	free(r->port);
}

static const struct spdk_json_object_decoder rpc_construct_persist_decoders[] = {
	{"name", offsetof(struct rpc_construct_persist, name), spdk_json_decode_string, true},
	{"uuid", offsetof(struct rpc_construct_persist, uuid), spdk_json_decode_string, true},
	{"ip", offsetof(struct rpc_construct_persist, ip), spdk_json_decode_string, true},
	{"port", offsetof(struct rpc_construct_persist, port), spdk_json_decode_string, true},
	{"attach_disk", offsetof(struct rpc_construct_persist, attach_disk), spdk_json_decode_bool, true},
	{"peers", offsetof(struct rpc_construct_persist, peers), decode_peers, true},
};

static void
rpc_bdev_persist_create(struct spdk_jsonrpc_request *request,
		       const struct spdk_json_val *params)
{
	struct rpc_construct_persist req = {NULL};
	struct spdk_json_write_ctx *w;
	struct spdk_uuid *uuid = NULL;
	struct spdk_uuid decoded_uuid;
	struct spdk_bdev *bdev;
	int rc = 0;

	if (spdk_json_decode_object(params, rpc_construct_persist_decoders,
				    SPDK_COUNTOF(rpc_construct_persist_decoders),
				    &req)) {
		SPDK_DEBUGLOG(bdev_persist, "spdk_json_decode_object failed\n");
		spdk_jsonrpc_send_error_response(request, SPDK_JSONRPC_ERROR_INTERNAL_ERROR,
						 "spdk_json_decode_object failed");
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

	rc = create_persist_disk(&bdev, req.name, req.ip, req.port, uuid, req.attach_disk, req.peers.peers, req.peers.num_peers);
	if (rc) {
		spdk_jsonrpc_send_error_response(request, rc, spdk_strerror(-rc));
		goto cleanup;
	}

	free_rpc_construct_persist(&req);

	w = spdk_jsonrpc_begin_result(request);
	spdk_json_write_string(w, spdk_bdev_get_name(bdev));
	spdk_jsonrpc_end_result(request, w);
	return;

cleanup:
	free_rpc_construct_persist(&req);
}
SPDK_RPC_REGISTER("bdev_persist_create", rpc_bdev_persist_create, SPDK_RPC_RUNTIME)
SPDK_RPC_REGISTER_ALIAS_DEPRECATED(bdev_persist_create, construct_persist_bdev)

struct rpc_delete_persist {
	char *name;
};

static void
free_rpc_delete_persist(struct rpc_delete_persist *r)
{
	free(r->name);
}

static const struct spdk_json_object_decoder rpc_delete_persist_decoders[] = {
	{"name", offsetof(struct rpc_delete_persist, name), spdk_json_decode_string},
};

static void
rpc_bdev_persist_delete_cb(void *cb_arg, int bdeverrno)
{
	struct spdk_jsonrpc_request *request = cb_arg;

	if (bdeverrno == 0) {
		spdk_jsonrpc_send_bool_response(request, true);
	} else {
		spdk_jsonrpc_send_error_response(request, bdeverrno, spdk_strerror(-bdeverrno));
	}
}

static void
rpc_bdev_persist_delete(struct spdk_jsonrpc_request *request,
		       const struct spdk_json_val *params)
{
	struct rpc_delete_persist req = {NULL};

	if (spdk_json_decode_object(params, rpc_delete_persist_decoders,
				    SPDK_COUNTOF(rpc_delete_persist_decoders),
				    &req)) {
		SPDK_DEBUGLOG(bdev_persist, "spdk_json_decode_object failed\n");
		spdk_jsonrpc_send_error_response(request, SPDK_JSONRPC_ERROR_INTERNAL_ERROR,
						 "spdk_json_decode_object failed");
		goto cleanup;
	}

	delete_persist_disk(req.name, rpc_bdev_persist_delete_cb, request);

cleanup:
	free_rpc_delete_persist(&req);
}
SPDK_RPC_REGISTER("bdev_persist_delete", rpc_bdev_persist_delete, SPDK_RPC_RUNTIME)
SPDK_RPC_REGISTER_ALIAS_DEPRECATED(bdev_persist_delete, delete_persist_bdev)
