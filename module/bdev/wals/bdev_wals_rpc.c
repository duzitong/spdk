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
#include "bdev_wals.h"
#include "spdk/util.h"
#include "spdk/string.h"
#include "spdk/log.h"
#include "spdk/env.h"

#define RPC_MAX_SLICES 256 // TODO: Add rpc to add slices in future

/*
 * Input structure for bdev_wals_get_bdevs RPC
 */
struct rpc_bdev_wals_get_bdevs {
	/* category - all or online or configuring or offline */
	char *category;
};

/*
 * brief:
 * free_rpc_bdev_wals_get_bdevs function frees RPC bdev_wals_get_bdevs related parameters
 * params:
 * req - pointer to RPC request
 * returns:
 * none
 */
static void
free_rpc_bdev_wals_get_bdevs(struct rpc_bdev_wals_get_bdevs *req)
{
	free(req->category);
}

/*
 * Decoder object for RPC get_walss
 */
static const struct spdk_json_object_decoder rpc_bdev_wals_get_bdevs_decoders[] = {
	{"category", offsetof(struct rpc_bdev_wals_get_bdevs, category), spdk_json_decode_string},
};

/*
 * brief:
 * rpc_bdev_wals_get_bdevs function is the RPC for rpc_bdev_wals_get_bdevs. This is used to list
 * all the wals bdev names based on the input category requested. Category should be
 * one of "all", "online", "configuring" or "offline". "all" means all the walss
 * whether they are online or configuring or offline. "online" is the wals bdev which
 * is registered with bdev layer. "configuring" is the wals bdev which does not have
 * full configuration discovered yet. "offline" is the wals bdev which is not
 * registered with bdev as of now and it has encountered any error or user has
 * requested to offline the wals.
 * params:
 * request - pointer to json rpc request
 * params - pointer to request parameters
 * returns:
 * none
 */
static void
rpc_bdev_wals_get_bdevs(struct spdk_jsonrpc_request *request,
			const struct spdk_json_val *params)
{
	struct rpc_bdev_wals_get_bdevs   req = {};
	struct spdk_json_write_ctx  *w;
	struct wals_bdev            *wals_bdev;

	if (spdk_json_decode_object(params, rpc_bdev_wals_get_bdevs_decoders,
				    SPDK_COUNTOF(rpc_bdev_wals_get_bdevs_decoders),
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

	/* Get wals bdev list based on the category requested */
	if (strcmp(req.category, "all") == 0) {
		TAILQ_FOREACH(wals_bdev, &g_wals_bdev_list, global_link) {
			spdk_json_write_string(w, wals_bdev->bdev.name);
		}
	} else if (strcmp(req.category, "online") == 0) {
		TAILQ_FOREACH(wals_bdev, &g_wals_bdev_configured_list, state_link) {
			spdk_json_write_string(w, wals_bdev->bdev.name);
		}
	} else if (strcmp(req.category, "configuring") == 0) {
		TAILQ_FOREACH(wals_bdev, &g_wals_bdev_configuring_list, state_link) {
			spdk_json_write_string(w, wals_bdev->bdev.name);
		}
	} else {
		TAILQ_FOREACH(wals_bdev, &g_wals_bdev_offline_list, state_link) {
			spdk_json_write_string(w, wals_bdev->bdev.name);
		}
	}
	spdk_json_write_array_end(w);
	spdk_jsonrpc_end_result(request, w);

cleanup:
	free_rpc_bdev_wals_get_bdevs(&req);
}
SPDK_RPC_REGISTER("bdev_wals_get_bdevs", rpc_bdev_wals_get_bdevs, SPDK_RPC_RUNTIME)
SPDK_RPC_REGISTER_ALIAS_DEPRECATED(bdev_wals_get_bdevs, get_wals_bdevs)

/*
 * Slices for creation
 */

struct rpc_bdev_wals_target_info {
	char		*nqn;

	char		*address;

	uint16_t	port;
};

static const struct spdk_json_object_decoder rpc_bdev_wals_target_info_decoders[] = {
	{"nqn", offsetof(struct rpc_bdev_wals_target_info, nqn), spdk_json_decode_string},
	{"address", offsetof(struct rpc_bdev_wals_target_info, address), spdk_json_decode_string},
	{"port", offsetof(struct rpc_bdev_wals_target_info, port), spdk_json_decode_uint16},
};

static int
decode_target_info(const struct spdk_json_val *val, void *out)
{
	return spdk_json_decode_object(val, rpc_bdev_wals_target_info_decoders, SPDK_COUNTOF(rpc_bdev_wals_target_info_decoders), out);
}

struct rpc_bdev_wals_target {
	struct rpc_bdev_wals_target_info	log;
	
	struct rpc_bdev_wals_target_info	core;
};

static const struct spdk_json_object_decoder rpc_bdev_wals_target_decoders[] = {
	{"log", offsetof(struct rpc_bdev_wals_target, log), decode_target_info},
	{"core", offsetof(struct rpc_bdev_wals_target, core), decode_target_info},
};

static int
decode_target(const struct spdk_json_val *val, void *out)
{
	return spdk_json_decode_object(val, rpc_bdev_wals_target_decoders, SPDK_COUNTOF(rpc_bdev_wals_target_decoders), out);
}

struct rpc_bdev_wals_slice {
	size_t						 	num_targets;

	struct rpc_bdev_wals_target		targets[NUM_TARGETS];
};

static int
decode_slice(const struct spdk_json_val *val, void *out)
{
	struct rpc_bdev_wals_slice *slice = out;
	return spdk_json_decode_array(val, decode_target, slice->targets,
				      NUM_TARGETS, &slice->num_targets, sizeof(struct rpc_bdev_wals_target))
			&& slice->num_targets == NUM_TARGETS;
}

struct rpc_bdev_wals_create_slices {
	size_t           			num_slices;

	struct rpc_bdev_wals_slice	slices[RPC_MAX_SLICES];
};

static int
decode_slices(const struct spdk_json_val *val, void *out)
{
	struct rpc_bdev_wals_create_slices *slices = out;
	return spdk_json_decode_array(val, decode_slice, slices->slices,
				      RPC_MAX_SLICES, &slices->num_slices, sizeof(struct rpc_bdev_wals_slice));
}

/*
 * Input structure for RPC rpc_bdev_wals_create
 */
struct rpc_bdev_wals_create {
	/* wals bdev name */
	char             *name;

	/* wals target module name */
	char             *module;

	struct rpc_bdev_wals_create_slices	slices;
};

static const struct spdk_json_object_decoder rpc_bdev_wals_create_decoders[] = {
	{"name", offsetof(struct rpc_bdev_wals_create, name), spdk_json_decode_string},
	{"module", offsetof(struct rpc_bdev_wals_create, module), spdk_json_decode_string},
	{"slices", offsetof(struct rpc_bdev_wals_create, slices), decode_slices},
};

/*
 * brief:
 * free_rpc_bdev_wals_create function is to free RPC bdev_wals_create related parameters
 * params:
 * req - pointer to RPC request
 * returns:
 * none
 */
static void
free_rpc_bdev_wals_create(struct rpc_bdev_wals_create *req)
{
	int i, j;

	free(req->name);
	free(req->module);
	for (i = 0; i < req->slices.num_slices; i++) {
		for (j = 0; j < req->slices.slices->num_targets; j++) {
			free(req->slices.slices[i].targets[j].log.nqn);
			free(req->slices.slices[i].targets[j].log.address);
			free(req->slices.slices[i].targets[j].core.nqn);
			free(req->slices.slices[i].targets[j].core.address);
		}
	}
}

/*
 * brief:
 * rpc_bdev_wals_create function is the RPC for creating wals bdevs. It takes
 * input as wals bdev name and list of base bdev names.
 * params:
 * request - pointer to json rpc request
 * params - pointer to request parameters
 * returns:
 * none
 */
static void
rpc_bdev_wals_create(struct spdk_jsonrpc_request *request,
		     const struct spdk_json_val *params)
{
	struct rpc_bdev_wals_create	req = {};
	struct wals_bdev_config		*wals_cfg;
	struct spdk_json_write_ctx *w;
	int				rc;

	if (spdk_json_decode_object(params, rpc_bdev_wals_create_decoders,
				    SPDK_COUNTOF(rpc_bdev_wals_create_decoders),
				    &req)) {
		spdk_jsonrpc_send_error_response(request, SPDK_JSONRPC_ERROR_INTERNAL_ERROR,
						 "spdk_json_decode_object failed");
		goto cleanup;
	}

	// rc = wals_bdev_config_add(req.name, req.log_bdev, req.core_bdev, &wals_cfg);
	// if (rc != 0) {
	// 	spdk_jsonrpc_send_error_response_fmt(request, rc,
	// 					     "Failed to add wals bdev config %s: %s",
	// 					     req.name, spdk_strerror(-rc));
	// 	goto cleanup;
	// }

	// rc = wals_bdev_create(wals_cfg);
	// if (rc != 0) {
	// 	wals_bdev_config_cleanup(wals_cfg);
	// 	spdk_jsonrpc_send_error_response_fmt(request, rc,
	// 					     "Failed to create wals bdev %s: %s",
	// 					     req.name, spdk_strerror(-rc));
	// 	goto cleanup;
	// }

	// rc = wals_bdev_add_base_devices(wals_cfg);
	// if (rc != 0) {
	// 	spdk_jsonrpc_send_error_response_fmt(request, rc,
	// 					     "Failed to add any base bdev to wals bdev %s: %s",
	// 					     req.name, spdk_strerror(-rc));
	// 	goto cleanup;
	// }

	w = spdk_jsonrpc_begin_result(request);
	spdk_json_write_string(w, req.name);
	spdk_jsonrpc_end_result(request, w);

cleanup:
	free_rpc_bdev_wals_create(&req);
}
SPDK_RPC_REGISTER("bdev_wals_create", rpc_bdev_wals_create, SPDK_RPC_RUNTIME)
SPDK_RPC_REGISTER_ALIAS_DEPRECATED(bdev_wals_create, construct_wals_bdev)

/*
 * Input structure for RPC deleting a wals bdev
 */
struct rpc_bdev_wals_delete {
	/* wals bdev name */
	char *name;
};

/*
 * brief:
 * free_rpc_bdev_wals_delete function is used to free RPC bdev_wals_delete related parameters
 * params:
 * req - pointer to RPC request
 * params:
 * none
 */
static void
free_rpc_bdev_wals_delete(struct rpc_bdev_wals_delete *req)
{
	free(req->name);
}

/*
 * Decoder object for RPC wals_bdev_delete
 */
static const struct spdk_json_object_decoder rpc_bdev_wals_delete_decoders[] = {
	{"name", offsetof(struct rpc_bdev_wals_delete, name), spdk_json_decode_string},
};

struct rpc_bdev_wals_delete_ctx {
	struct rpc_bdev_wals_delete req;
	struct wals_bdev_config *wals_cfg;
	struct spdk_jsonrpc_request *request;
};

/*
 * brief:
 * params:
 * cb_arg - pointer to the callback context.
 * rc - return code of the deletion of the wals bdev.
 * returns:
 * none
 */
static void
bdev_wals_delete_done(void *cb_arg, int rc)
{
	struct rpc_bdev_wals_delete_ctx *ctx = cb_arg;
	struct wals_bdev_config *wals_cfg;
	struct spdk_jsonrpc_request *request = ctx->request;

	if (rc != 0) {
		SPDK_ERRLOG("Failed to delete wals bdev %s (%d): %s\n",
			    ctx->req.name, rc, spdk_strerror(-rc));
		spdk_jsonrpc_send_error_response(request, SPDK_JSONRPC_ERROR_INTERNAL_ERROR,
						 spdk_strerror(-rc));
		goto exit;
	}

	wals_cfg = ctx->wals_cfg;
	assert(wals_cfg->wals_bdev == NULL);

	wals_bdev_config_cleanup(wals_cfg);

	spdk_jsonrpc_send_bool_response(request, true);
exit:
	free_rpc_bdev_wals_delete(&ctx->req);
	free(ctx);
}

/*
 * brief:
 * rpc_bdev_wals_delete function is the RPC for deleting a wals bdev. It takes wals
 * name as input and delete that wals bdev including freeing the base bdev
 * resources.
 * params:
 * request - pointer to json rpc request
 * params - pointer to request parameters
 * returns:
 * none
 */
static void
rpc_bdev_wals_delete(struct spdk_jsonrpc_request *request,
		     const struct spdk_json_val *params)
{
	struct rpc_bdev_wals_delete_ctx *ctx;

	ctx = calloc(1, sizeof(*ctx));
	if (!ctx) {
		spdk_jsonrpc_send_error_response(request, -ENOMEM, spdk_strerror(ENOMEM));
		return;
	}

	if (spdk_json_decode_object(params, rpc_bdev_wals_delete_decoders,
				    SPDK_COUNTOF(rpc_bdev_wals_delete_decoders),
				    &ctx->req)) {
		spdk_jsonrpc_send_error_response(request, SPDK_JSONRPC_ERROR_INTERNAL_ERROR,
						 "spdk_json_decode_object failed");
		goto cleanup;
	}

	ctx->wals_cfg = wals_bdev_config_find_by_name(ctx->req.name);
	if (ctx->wals_cfg == NULL) {
		spdk_jsonrpc_send_error_response_fmt(request, ENODEV,
						     "wals bdev %s is not found in config",
						     ctx->req.name);
		goto cleanup;
	}

	ctx->request = request;

	/* Remove all the base bdevs from this wals bdev before deleting the wals bdev */
	wals_bdev_remove_base_devices(ctx->wals_cfg, bdev_wals_delete_done, ctx);

	return;

cleanup:
	free_rpc_bdev_wals_delete(&ctx->req);
	free(ctx);
}
SPDK_RPC_REGISTER("bdev_wals_delete", rpc_bdev_wals_delete, SPDK_RPC_RUNTIME)
SPDK_RPC_REGISTER_ALIAS_DEPRECATED(bdev_wals_delete, destroy_wals_bdev)
