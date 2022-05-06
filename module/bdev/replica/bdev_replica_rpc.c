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
#include "bdev_replica.h"
#include "spdk/util.h"
#include "spdk/string.h"
#include "spdk/log.h"
#include "spdk/env.h"

/*
 * Input structure for bdev_replica_get_bdevs RPC
 */
struct rpc_bdev_replica_get_bdevs {
	/* category - all or online or configuring or offline */
	char *category;
};

/*
 * brief:
 * free_rpc_bdev_replica_get_bdevs function frees RPC bdev_replica_get_bdevs related parameters
 * params:
 * req - pointer to RPC request
 * returns:
 * none
 */
static void
free_rpc_bdev_replica_get_bdevs(struct rpc_bdev_replica_get_bdevs *req)
{
	free(req->category);
}

/*
 * Decoder object for RPC get_replicas
 */
static const struct spdk_json_object_decoder rpc_bdev_replica_get_bdevs_decoders[] = {
	{"category", offsetof(struct rpc_bdev_replica_get_bdevs, category), spdk_json_decode_string},
};

/*
 * brief:
 * rpc_bdev_replica_get_bdevs function is the RPC for rpc_bdev_replica_get_bdevs. This is used to list
 * all the replica bdev names based on the input category requested. Category should be
 * one of "all", "online", "configuring" or "offline". "all" means all the replicas
 * whether they are online or configuring or offline. "online" is the replica bdev which
 * is registered with bdev layer. "configuring" is the replica bdev which does not have
 * full configuration discovered yet. "offline" is the replica bdev which is not
 * registered with bdev as of now and it has encountered any error or user has
 * requested to offline the replica.
 * params:
 * request - pointer to json rpc request
 * params - pointer to request parameters
 * returns:
 * none
 */
static void
rpc_bdev_replica_get_bdevs(struct spdk_jsonrpc_request *request,
			const struct spdk_json_val *params)
{
	struct rpc_bdev_replica_get_bdevs   req = {};
	struct spdk_json_write_ctx  *w;
	struct replica_bdev            *replica_bdev;

	if (spdk_json_decode_object(params, rpc_bdev_replica_get_bdevs_decoders,
				    SPDK_COUNTOF(rpc_bdev_replica_get_bdevs_decoders),
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

	/* Get replica bdev list based on the category requested */
	if (strcmp(req.category, "all") == 0) {
		TAILQ_FOREACH(replica_bdev, &g_replica_bdev_list, global_link) {
			spdk_json_write_string(w, replica_bdev->bdev.name);
		}
	} else if (strcmp(req.category, "online") == 0) {
		TAILQ_FOREACH(replica_bdev, &g_replica_bdev_configured_list, state_link) {
			spdk_json_write_string(w, replica_bdev->bdev.name);
		}
	} else if (strcmp(req.category, "configuring") == 0) {
		TAILQ_FOREACH(replica_bdev, &g_replica_bdev_configuring_list, state_link) {
			spdk_json_write_string(w, replica_bdev->bdev.name);
		}
	} else {
		TAILQ_FOREACH(replica_bdev, &g_replica_bdev_offline_list, state_link) {
			spdk_json_write_string(w, replica_bdev->bdev.name);
		}
	}
	spdk_json_write_array_end(w);
	spdk_jsonrpc_end_result(request, w);

cleanup:
	free_rpc_bdev_replica_get_bdevs(&req);
}
SPDK_RPC_REGISTER("bdev_replica_get_bdevs", rpc_bdev_replica_get_bdevs, SPDK_RPC_RUNTIME)

/*
 * Base bdevs in RPC bdev_replica_initiator_create
 */
struct rpc_bdev_replica_initiator_create_base_bdevs {
	/* Number of base bdevs */
	size_t           num_base_bdevs;

	/* List of base bdevs names */
	char             *base_bdevs[MAX_BASE_BDEVS];
};

/*
 * Input structure for RPC rpc_bdev_replica_initiator_create
 */
struct rpc_bdev_replica_initiator_create {
	/* Replica bdev name */
	char                                 				*name;

	/* Base bdevs information */
	struct rpc_bdev_replica_initiator_create_base_bdevs base_bdevs;
};

/*
 * brief:
 * free_rpc_bdev_replica_initiator_create function is to free RPC bdev_replica_initiator_create related parameters
 * params:
 * req - pointer to RPC request
 * returns:
 * none
 */
static void
free_rpc_bdev_replica_initiator_create(struct rpc_bdev_replica_initiator_create *req)
{
	size_t i;

	free(req->name);
	for (i = 0; i < req->base_bdevs.num_base_bdevs; i++) {
		free(req->base_bdevs.base_bdevs[i]);
	}
}

/*
 * Decoder function for RPC bdev_replica_initiator_create to decode base bdevs list
 */
static int
decode_base_bdevs(const struct spdk_json_val *val, void *out)
{
	struct rpc_bdev_replica_initiator_create_base_bdevs *base_bdevs = out;
	return spdk_json_decode_array(val, spdk_json_decode_string, base_bdevs->base_bdevs,
				      MAX_BASE_BDEVS, &base_bdevs->num_base_bdevs, sizeof(char *));
}

/*
 * Decoder object for RPC bdev_replica_initiator_create
 */
static const struct spdk_json_object_decoder rpc_bdev_replica_initiator_create_decoders[] = {
	{"name", offsetof(struct rpc_bdev_replica_initiator_create, name), spdk_json_decode_string},
	{"base_bdevs", offsetof(struct rpc_bdev_replica_initiator_create, base_bdevs), decode_base_bdevs},
};

/*
 * brief:
 * rpc_bdev_replica_initiator_create function is the RPC for creating Replica bdevs. It takes
 * input as replica bdev name and list of base bdev names.
 * params:
 * request - pointer to json rpc request
 * params - pointer to request parameters
 * returns:
 * none
 */
static void
rpc_bdev_replica_initiator_create(struct spdk_jsonrpc_request *request,
		     const struct spdk_json_val *params)
{
	struct rpc_bdev_replica_initiator_create	req = {};
	struct replica_bdev_config		*replica_cfg;
	int				rc;
	size_t				i;

	if (spdk_json_decode_object(params, rpc_bdev_replica_initiator_create_decoders,
				    SPDK_COUNTOF(rpc_bdev_replica_initiator_create_decoders),
				    &req)) {
		spdk_jsonrpc_send_error_response(request, SPDK_JSONRPC_ERROR_INTERNAL_ERROR,
						 "spdk_json_decode_object failed");
		goto cleanup;
	}

	rc = replica_bdev_config_add(req.name, req.base_bdevs.num_base_bdevs, REPLICA_BDEV_TYPE_INITIATOR
				  &replica_cfg);
	if (rc != 0) {
		spdk_jsonrpc_send_error_response_fmt(request, rc,
						     "Failed to add Replica bdev config %s: %s",
						     req.name, spdk_strerror(-rc));
		goto cleanup;
	}

	for (i = 0; i < req.base_bdevs.num_base_bdevs; i++) {
		rc = replica_bdev_config_add_base_bdev(replica_cfg, req.base_bdevs.base_bdevs[i], i);
		if (rc != 0) {
			replica_bdev_config_cleanup(replica_cfg);
			spdk_jsonrpc_send_error_response_fmt(request, rc,
							     "Failed to add base bdev %s to Replica bdev config %s: %s",
							     req.base_bdevs.base_bdevs[i], req.name,
							     spdk_strerror(-rc));
			goto cleanup;
		}
	}

	rc = replica_bdev_create(replica_cfg);
	if (rc != 0) {
		replica_bdev_config_cleanup(replica_cfg);
		spdk_jsonrpc_send_error_response_fmt(request, rc,
						     "Failed to create Replica bdev %s: %s",
						     req.name, spdk_strerror(-rc));
		goto cleanup;
	}

	rc = replica_bdev_add_base_devices(replica_cfg);
	if (rc != 0) {
		spdk_jsonrpc_send_error_response_fmt(request, rc,
						     "Failed to add any base bdev to Replica bdev %s: %s",
						     req.name, spdk_strerror(-rc));
		goto cleanup;
	}

	spdk_jsonrpc_send_bool_response(request, true);

cleanup:
	free_rpc_bdev_replica_initiator_create(&req);
}
SPDK_RPC_REGISTER("bdev_replica_initiator_create", rpc_bdev_replica_initiator_create, SPDK_RPC_RUNTIME)


/*
 * Input structure for RPC rpc_bdev_replica_initiator_create
 */
struct rpc_bdev_replica_target_create {
	/* Replica bdev name */
	char	*name;

	char	*log_bdev_name;

	char	*base_bdev_name;		
};

/*
 * brief:
 * free_rpc_bdev_replica_target_create function is to free RPC bdev_replica_target_create related parameters
 * params:
 * req - pointer to RPC request
 * returns:
 * none
 */
static void
free_rpc_bdev_replica_target_create(struct rpc_bdev_replica_target_create *req)
{
	free(req->name);
	free(req->log_bdev_name);
	free(req->base_bdev_name);
}

/*
 * Decoder object for RPC bdev_replica_target_create
 */
static const struct spdk_json_object_decoder rpc_bdev_replica_target_create_decoders[] = {
	{"name", offsetof(struct rpc_bdev_replica_target_create, name), spdk_json_decode_string},
	{"log_bdev_name", offsetof(struct rpc_bdev_replica_target_create, log_bdev_name), spdk_json_decode_string},
	{"base_bdev_name", offsetof(struct rpc_bdev_replica_target_create, base_bdev_name), spdk_json_decode_string},
};

/*
 * brief:
 * rpc_bdev_replica_target_create function is the RPC for creating Replica target bdevs. It takes
 * input as replica bdev name and list of base bdev names.
 * params:
 * request - pointer to json rpc request
 * params - pointer to request parameters
 * returns:
 * none
 */
static void
rpc_bdev_replica_target_create(struct spdk_jsonrpc_request *request,
		     const struct spdk_json_val *params)
{
	struct rpc_bdev_replica_target_create	req = {};
	struct replica_bdev_config		*replica_cfg;
	int				rc;
	size_t				i;

	if (spdk_json_decode_object(params, rpc_bdev_replica_target_create_decoders,
				    SPDK_COUNTOF(rpc_bdev_replica_target_create_decoders),
				    &req)) {
		spdk_jsonrpc_send_error_response(request, SPDK_JSONRPC_ERROR_INTERNAL_ERROR,
						 "spdk_json_decode_object failed");
		goto cleanup;
	}

	rc = replica_bdev_config_add(req.name, 2, REPLICA_BDEV_TYPE_TARGET, &replica_cfg);
	if (rc != 0) {
		spdk_jsonrpc_send_error_response_fmt(request, rc,
						     "Failed to add Replica bdev config %s: %s",
						     req.name, spdk_strerror(-rc));
		goto cleanup;
	}

	rc = replica_bdev_config_add_base_bdev(replica_cfg, req.log_bdev_name, 0);
	if (rc != 0) {
		replica_bdev_config_cleanup(replica_cfg);
		spdk_jsonrpc_send_error_response_fmt(request, rc,
								"Failed to add log bdev %s to Replica bdev config %s: %s",
								req.log_bdev_name, req.name,
								spdk_strerror(-rc));
		goto cleanup;
	}

	rc = replica_bdev_config_add_base_bdev(replica_cfg, req.base_bdev_name, 1);
	if (rc != 0) {
		replica_bdev_config_cleanup(replica_cfg);
		spdk_jsonrpc_send_error_response_fmt(request, rc,
								"Failed to add base bdev %s to Replica bdev config %s: %s",
								req.base_bdev_name, req.name,
								spdk_strerror(-rc));
		goto cleanup;
	}
	

	rc = replica_bdev_create(replica_cfg);
	if (rc != 0) {
		replica_bdev_config_cleanup(replica_cfg);
		spdk_jsonrpc_send_error_response_fmt(request, rc,
						     "Failed to create Replica bdev %s: %s",
						     req.name, spdk_strerror(-rc));
		goto cleanup;
	}

	rc = replica_bdev_add_base_devices(replica_cfg);
	if (rc != 0) {
		spdk_jsonrpc_send_error_response_fmt(request, rc,
						     "Failed to add any base bdev to Replica bdev %s: %s",
						     req.name, spdk_strerror(-rc));
		goto cleanup;
	}

	spdk_jsonrpc_send_bool_response(request, true);

cleanup:
	free_rpc_bdev_replica_target_create(&req);
}
SPDK_RPC_REGISTER("bdev_replica_target_create", rpc_bdev_replica_target_create, SPDK_RPC_RUNTIME)

/*
 * Input structure for RPC deleting a replica bdev
 */
struct rpc_bdev_replica_delete {
	/* replica bdev name */
	char *name;
};

/*
 * brief:
 * free_rpc_bdev_replica_delete function is used to free RPC bdev_replica_delete related parameters
 * params:
 * req - pointer to RPC request
 * params:
 * none
 */
static void
free_rpc_bdev_replica_delete(struct rpc_bdev_replica_delete *req)
{
	free(req->name);
}

/*
 * Decoder object for RPC replica_bdev_delete
 */
static const struct spdk_json_object_decoder rpc_bdev_replica_delete_decoders[] = {
	{"name", offsetof(struct rpc_bdev_replica_delete, name), spdk_json_decode_string},
};

struct rpc_bdev_replica_delete_ctx {
	struct rpc_bdev_replica_delete req;
	struct replica_bdev_config *replica_cfg;
	struct spdk_jsonrpc_request *request;
};

/*
 * brief:
 * params:
 * cb_arg - pointer to the callback context.
 * rc - return code of the deletion of the replica bdev.
 * returns:
 * none
 */
static void
bdev_replica_delete_done(void *cb_arg, int rc)
{
	struct rpc_bdev_replica_delete_ctx *ctx = cb_arg;
	struct replica_bdev_config *replica_cfg;
	struct spdk_jsonrpc_request *request = ctx->request;

	if (rc != 0) {
		SPDK_ERRLOG("Failed to delete replica bdev %s (%d): %s\n",
			    ctx->req.name, rc, spdk_strerror(-rc));
		spdk_jsonrpc_send_error_response(request, SPDK_JSONRPC_ERROR_INTERNAL_ERROR,
						 spdk_strerror(-rc));
		goto exit;
	}

	replica_cfg = ctx->replica_cfg;
	assert(replica_cfg->replica_bdev == NULL);

	replica_bdev_config_cleanup(replica_cfg);

	spdk_jsonrpc_send_bool_response(request, true);
exit:
	free_rpc_bdev_replica_delete(&ctx->req);
	free(ctx);
}

/*
 * brief:
 * rpc_bdev_replica_delete function is the RPC for deleting a replica bdev. It takes replica
 * name as input and delete that replica bdev including freeing the base bdev
 * resources.
 * params:
 * request - pointer to json rpc request
 * params - pointer to request parameters
 * returns:
 * none
 */
static void
rpc_bdev_replica_delete(struct spdk_jsonrpc_request *request,
		     const struct spdk_json_val *params)
{
	struct rpc_bdev_replica_delete_ctx *ctx;

	ctx = calloc(1, sizeof(*ctx));
	if (!ctx) {
		spdk_jsonrpc_send_error_response(request, -ENOMEM, spdk_strerror(ENOMEM));
		return;
	}

	if (spdk_json_decode_object(params, rpc_bdev_replica_delete_decoders,
				    SPDK_COUNTOF(rpc_bdev_replica_delete_decoders),
				    &ctx->req)) {
		spdk_jsonrpc_send_error_response(request, SPDK_JSONRPC_ERROR_INTERNAL_ERROR,
						 "spdk_json_decode_object failed");
		goto cleanup;
	}

	ctx->replica_cfg = replica_bdev_config_find_by_name(ctx->req.name);
	if (ctx->replica_cfg == NULL) {
		spdk_jsonrpc_send_error_response_fmt(request, ENODEV,
						     "replica bdev %s is not found in config",
						     ctx->req.name);
		goto cleanup;
	}

	ctx->request = request;

	/* Remove all the base bdevs from this replica bdev before deleting the replica bdev */
	replica_bdev_remove_base_devices(ctx->replica_cfg, bdev_replica_delete_done, ctx);

	return;

cleanup:
	free_rpc_bdev_replica_delete(&ctx->req);
	free(ctx);
}
SPDK_RPC_REGISTER("bdev_replica_delete", rpc_bdev_replica_delete, SPDK_RPC_RUNTIME)
