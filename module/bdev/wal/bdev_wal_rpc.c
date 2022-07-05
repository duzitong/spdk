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

#include "spdk/rpc.h"
#include "spdk/bdev.h"
#include "bdev_wal.h"
#include "spdk/util.h"
#include "spdk/string.h"
#include "spdk/log.h"
#include "spdk/env.h"

/*
 * Input structure for bdev_wal_get_bdevs RPC
 */
struct rpc_bdev_wal_get_bdevs {
	/* category - all or online or configuring or offline */
	char *category;
};

/*
 * brief:
 * free_rpc_bdev_wal_get_bdevs function frees RPC bdev_wal_get_bdevs related parameters
 * params:
 * req - pointer to RPC request
 * returns:
 * none
 */
static void
free_rpc_bdev_wal_get_bdevs(struct rpc_bdev_wal_get_bdevs *req)
{
	free(req->category);
}

/*
 * Decoder object for RPC get_wals
 */
static const struct spdk_json_object_decoder rpc_bdev_wal_get_bdevs_decoders[] = {
	{"category", offsetof(struct rpc_bdev_wal_get_bdevs, category), spdk_json_decode_string},
};

/*
 * brief:
 * rpc_bdev_wal_get_bdevs function is the RPC for rpc_bdev_wal_get_bdevs. This is used to list
 * all the wal bdev names based on the input category requested. Category should be
 * one of "all", "online", "configuring" or "offline". "all" means all the wals
 * whether they are online or configuring or offline. "online" is the wal bdev which
 * is registered with bdev layer. "configuring" is the wal bdev which does not have
 * full configuration discovered yet. "offline" is the wal bdev which is not
 * registered with bdev as of now and it has encountered any error or user has
 * requested to offline the wal.
 * params:
 * request - pointer to json rpc request
 * params - pointer to request parameters
 * returns:
 * none
 */
static void
rpc_bdev_wal_get_bdevs(struct spdk_jsonrpc_request *request,
			const struct spdk_json_val *params)
{
	struct rpc_bdev_wal_get_bdevs   req = {};
	struct spdk_json_write_ctx  *w;
	struct wal_bdev            *wal_bdev;

	if (spdk_json_decode_object(params, rpc_bdev_wal_get_bdevs_decoders,
				    SPDK_COUNTOF(rpc_bdev_wal_get_bdevs_decoders),
				    &req)) {
		spdk_jsonrpc_send_error_response(request, SPDK_JSONRPC_ERROR_INTERNAL_ERROR,
						 "spdk_json_decode_object failed");
		goto cleanup;
	}

	if (!(strcmp(req.category, "all") == 0 ||
	      strcmp(req.category, "online") == 0 ||
	      strcmp(req.category, "configuring") == 0 ||
	      strcmp(req.category, "offline") == 0)) {
		spdk_jsonrpc_send_error_response(request, -EINVAL, spdk_strerror(EINVAL));
		goto cleanup;
	}

	w = spdk_jsonrpc_begin_result(request);
	spdk_json_write_array_begin(w);

	/* Get wal bdev list based on the category requested */
	if (strcmp(req.category, "all") == 0) {
		TAILQ_FOREACH(wal_bdev, &g_wal_bdev_list, global_link) {
			spdk_json_write_string(w, wal_bdev->bdev.name);
		}
	} else if (strcmp(req.category, "online") == 0) {
		TAILQ_FOREACH(wal_bdev, &g_wal_bdev_configured_list, state_link) {
			spdk_json_write_string(w, wal_bdev->bdev.name);
		}
	} else if (strcmp(req.category, "configuring") == 0) {
		TAILQ_FOREACH(wal_bdev, &g_wal_bdev_configuring_list, state_link) {
			spdk_json_write_string(w, wal_bdev->bdev.name);
		}
	} else {
		TAILQ_FOREACH(wal_bdev, &g_wal_bdev_offline_list, state_link) {
			spdk_json_write_string(w, wal_bdev->bdev.name);
		}
	}
	spdk_json_write_array_end(w);
	spdk_jsonrpc_end_result(request, w);

cleanup:
	free_rpc_bdev_wal_get_bdevs(&req);
}
SPDK_RPC_REGISTER("bdev_wal_get_bdevs", rpc_bdev_wal_get_bdevs, SPDK_RPC_RUNTIME)
SPDK_RPC_REGISTER_ALIAS_DEPRECATED(bdev_wal_get_bdevs, get_wal_bdevs)

/*
 * Input structure for RPC rpc_bdev_wal_create
 */
struct rpc_bdev_wal_create {
	/* wal bdev name */
	char             *name;

	/* log bdev name */
	char             *log_bdev;

	/* core bdev name */
	char             *core_bdev;
};

/*
 * brief:
 * free_rpc_bdev_wal_create function is to free RPC bdev_wal_create related parameters
 * params:
 * req - pointer to RPC request
 * returns:
 * none
 */
static void
free_rpc_bdev_wal_create(struct rpc_bdev_wal_create *req)
{
	free(req->name);
	free(req->log_bdev);
	free(req->core_bdev);
}

/*
 * Decoder object for RPC bdev_wal_create
 */
static const struct spdk_json_object_decoder rpc_bdev_wal_create_decoders[] = {
	{"name", offsetof(struct rpc_bdev_wal_create, name), spdk_json_decode_string},
	{"log_bdev", offsetof(struct rpc_bdev_wal_create, log_bdev), spdk_json_decode_string},
	{"core_bdev", offsetof(struct rpc_bdev_wal_create, core_bdev), spdk_json_decode_string},
};

/*
 * brief:
 * rpc_bdev_wal_create function is the RPC for creating wal bdevs. It takes
 * input as wal bdev name and list of base bdev names.
 * params:
 * request - pointer to json rpc request
 * params - pointer to request parameters
 * returns:
 * none
 */
static void
rpc_bdev_wal_create(struct spdk_jsonrpc_request *request,
		     const struct spdk_json_val *params)
{
	struct rpc_bdev_wal_create	req = {};
	struct wal_bdev_config		*wal_cfg;
	int				rc;

	if (spdk_json_decode_object(params, rpc_bdev_wal_create_decoders,
				    SPDK_COUNTOF(rpc_bdev_wal_create_decoders),
				    &req)) {
		spdk_jsonrpc_send_error_response(request, SPDK_JSONRPC_ERROR_INTERNAL_ERROR,
						 "spdk_json_decode_object failed");
		goto cleanup;
	}

	rc = wal_bdev_config_add(req.name, req.log_bdev, req.core_bdev, &wal_cfg);
	if (rc != 0) {
		spdk_jsonrpc_send_error_response_fmt(request, rc,
						     "Failed to add wal bdev config %s: %s",
						     req.name, spdk_strerror(-rc));
		goto cleanup;
	}

	rc = wal_bdev_create(wal_cfg);
	if (rc != 0) {
		wal_bdev_config_cleanup(wal_cfg);
		spdk_jsonrpc_send_error_response_fmt(request, rc,
						     "Failed to create wal bdev %s: %s",
						     req.name, spdk_strerror(-rc));
		goto cleanup;
	}

	rc = wal_bdev_add_base_devices(wal_cfg);
	if (rc != 0) {
		spdk_jsonrpc_send_error_response_fmt(request, rc,
						     "Failed to add any base bdev to wal bdev %s: %s",
						     req.name, spdk_strerror(-rc));
		goto cleanup;
	}

	spdk_jsonrpc_send_bool_response(request, true);

cleanup:
	free_rpc_bdev_wal_create(&req);
}
SPDK_RPC_REGISTER("bdev_wal_create", rpc_bdev_wal_create, SPDK_RPC_RUNTIME)
SPDK_RPC_REGISTER_ALIAS_DEPRECATED(bdev_wal_create, construct_wal_bdev)

/*
 * Input structure for RPC deleting a wal bdev
 */
struct rpc_bdev_wal_delete {
	/* wal bdev name */
	char *name;
};

/*
 * brief:
 * free_rpc_bdev_wal_delete function is used to free RPC bdev_wal_delete related parameters
 * params:
 * req - pointer to RPC request
 * params:
 * none
 */
static void
free_rpc_bdev_wal_delete(struct rpc_bdev_wal_delete *req)
{
	free(req->name);
}

/*
 * Decoder object for RPC wal_bdev_delete
 */
static const struct spdk_json_object_decoder rpc_bdev_wal_delete_decoders[] = {
	{"name", offsetof(struct rpc_bdev_wal_delete, name), spdk_json_decode_string},
};

struct rpc_bdev_wal_delete_ctx {
	struct rpc_bdev_wal_delete req;
	struct wal_bdev_config *wal_cfg;
	struct spdk_jsonrpc_request *request;
};

/*
 * brief:
 * params:
 * cb_arg - pointer to the callback context.
 * rc - return code of the deletion of the wal bdev.
 * returns:
 * none
 */
static void
bdev_wal_delete_done(void *cb_arg, int rc)
{
	struct rpc_bdev_wal_delete_ctx *ctx = cb_arg;
	struct wal_bdev_config *wal_cfg;
	struct spdk_jsonrpc_request *request = ctx->request;

	if (rc != 0) {
		SPDK_ERRLOG("Failed to delete wal bdev %s (%d): %s\n",
			    ctx->req.name, rc, spdk_strerror(-rc));
		spdk_jsonrpc_send_error_response(request, SPDK_JSONRPC_ERROR_INTERNAL_ERROR,
						 spdk_strerror(-rc));
		goto exit;
	}

	wal_cfg = ctx->wal_cfg;
	assert(wal_cfg->wal_bdev == NULL);

	wal_bdev_config_cleanup(wal_cfg);

	spdk_jsonrpc_send_bool_response(request, true);
exit:
	free_rpc_bdev_wal_delete(&ctx->req);
	free(ctx);
}

/*
 * brief:
 * rpc_bdev_wal_delete function is the RPC for deleting a wal bdev. It takes wal
 * name as input and delete that wal bdev including freeing the base bdev
 * resources.
 * params:
 * request - pointer to json rpc request
 * params - pointer to request parameters
 * returns:
 * none
 */
static void
rpc_bdev_wal_delete(struct spdk_jsonrpc_request *request,
		     const struct spdk_json_val *params)
{
	struct rpc_bdev_wal_delete_ctx *ctx;

	ctx = calloc(1, sizeof(*ctx));
	if (!ctx) {
		spdk_jsonrpc_send_error_response(request, -ENOMEM, spdk_strerror(ENOMEM));
		return;
	}

	if (spdk_json_decode_object(params, rpc_bdev_wal_delete_decoders,
				    SPDK_COUNTOF(rpc_bdev_wal_delete_decoders),
				    &ctx->req)) {
		spdk_jsonrpc_send_error_response(request, SPDK_JSONRPC_ERROR_INTERNAL_ERROR,
						 "spdk_json_decode_object failed");
		goto cleanup;
	}

	ctx->wal_cfg = wal_bdev_config_find_by_name(ctx->req.name);
	if (ctx->wal_cfg == NULL) {
		spdk_jsonrpc_send_error_response_fmt(request, ENODEV,
						     "wal bdev %s is not found in config",
						     ctx->req.name);
		goto cleanup;
	}

	ctx->request = request;

	/* Remove all the base bdevs from this wal bdev before deleting the wal bdev */
	wal_bdev_remove_base_devices(ctx->wal_cfg, bdev_wal_delete_done, ctx);

	return;

cleanup:
	free_rpc_bdev_wal_delete(&ctx->req);
	free(ctx);
}
SPDK_RPC_REGISTER("bdev_wal_delete", rpc_bdev_wal_delete, SPDK_RPC_RUNTIME)
SPDK_RPC_REGISTER_ALIAS_DEPRECATED(bdev_wal_delete, destroy_wal_bdev)
