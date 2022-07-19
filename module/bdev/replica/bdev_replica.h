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

#ifndef SPDK_BDEV_REPLICA_INTERNAL_H
#define SPDK_BDEV_REPLICA_INTERNAL_H

#include "spdk/bdev_module.h"

/*
 * Raid state describes the state of the replica. This replica bdev can be either in
 * configured list or configuring list
 */
enum replica_bdev_state {
	/* replica bdev is ready and is seen by upper layers */
	REPLICA_BDEV_STATE_ONLINE,

	/*
	 * replica bdev is configuring, not all underlying bdevs are present.
	 * And can't be seen by upper layers.
	 */
	REPLICA_BDEV_STATE_CONFIGURING,

	/*
	 * In offline state, replica bdev layer will complete all incoming commands without
	 * submitting to underlying base nvme bdevs
	 */
	REPLICA_BDEV_STATE_OFFLINE,

	/* replica bdev max, new states should be added before this */
	REPLICA_BDEV_MAX
};

/*
 * replica_base_bdev_info contains information for the base bdevs which are part of some
 * replica. This structure contains the per base bdev information. Whatever is
 * required per base device for replica bdev will be kept here
 */
struct replica_base_bdev_info {
	/* pointer to base spdk bdev */
	struct spdk_bdev	*bdev;

	/* pointer to base bdev descriptor opened by replica bdev */
	struct spdk_bdev_desc	*desc;

	/*
	 * When underlying base device calls the hot plug function on drive removal,
	 * this flag will be set and later after doing some processing, base device
	 * descriptor will be closed
	 */
	bool			remove_scheduled;

	/* thread where base device is opened */
	struct spdk_thread	*thread;
};

/*
 * replica_bdev_io is the context part of bdev_io. It contains the information
 * related to bdev_io for a replica bdev
 */
struct replica_bdev_io {
	/* The replica bdev associated with this IO */
	struct replica_bdev *replica_bdev;

	/* WaitQ entry, used only in waitq logic */
	struct spdk_bdev_io_wait_entry	waitq_entry;

	/* Context of the original channel for this IO */
	struct replica_bdev_io_channel	*replica_ch;

	/* Used for tracking progress on io requests sent to member disks. */
	uint64_t			base_bdev_io_remaining;
	uint8_t				base_bdev_io_submitted;
	uint8_t				base_bdev_io_status;
};

/*
 * replica_bdev is the single entity structure which contains SPDK block device
 * and the information related to any replica bdev either configured or
 * in configuring list. io device is created on this.
 */
struct replica_bdev {
	/* replica bdev device, this will get registered in bdev layer */
	struct spdk_bdev		bdev;

	/* link of replica bdev to link it to configured, configuring or offline list */
	TAILQ_ENTRY(replica_bdev)		state_link;

	/* link of replica bdev to link it to global replica bdev list */
	TAILQ_ENTRY(replica_bdev)		global_link;

	/* pointer to config file entry */
	struct replica_bdev_config		*config;

	/* array of base bdev info */
	struct replica_base_bdev_info	*base_bdev_info;

	/* use md or not */
	bool				md;

	/* block length bit shift for optimized calculation */
	uint32_t			blocklen_shift;

	/* state of replica bdev */
	enum replica_bdev_state		state;

	/* number of base bdevs comprising replica bdev  */
	uint8_t				num_base_bdevs;

	/* number of base bdevs discovered */
	uint8_t				num_base_bdevs_discovered;

	/* Set to true if destruct is called for this replica bdev */
	bool				destruct_called;

	/* Set to true if destroy of this replica bdev is started. */
	bool				destroy_started;
};

#define REPLICA_FOR_EACH_BASE_BDEV(r, i) \
	for (i = r->base_bdev_info; i < r->base_bdev_info + r->num_base_bdevs; i++)

/*
 * replica_base_bdev_config is the per base bdev data structure which contains
 * information w.r.t to per base bdev during parsing config
 */
struct replica_base_bdev_config {
	/* base bdev name from config file */
	char				*name;
};

/*
 * replica_bdev_config contains the replica bdev config related information after
 * parsing the config file
 */
struct replica_bdev_config {
	/* base bdev config per underlying bdev */
	struct replica_base_bdev_config	*base_bdev;

	/* Points to already created replica bdev  */
	struct replica_bdev		*replica_bdev;

	char				*name;

	/* number of base bdevs */
	uint8_t				num_base_bdevs;

	bool				md;

	TAILQ_ENTRY(replica_bdev_config)	link;
};

/*
 * replica_config is the top level structure representing the replica bdev config as read
 * from config file for all replicas
 */
struct replica_config {
	/* replica bdev  context from config file */
	TAILQ_HEAD(, replica_bdev_config) replica_bdev_config_head;

	/* total replica bdev  from config file */
	uint8_t total_replica_bdev;
};

/*
 * replica_bdev_io_channel is the context of spdk_io_channel for replica bdev device. It
 * contains the relationship of replica bdev io channel with base bdev io channels.
 */
struct replica_bdev_io_channel {
	/* Array of IO channels of base bdevs */
	struct spdk_io_channel	**base_channel;

	/* Number of IO channels */
	uint8_t			num_channels;
};

/* TAIL heads for various replica bdev lists */
TAILQ_HEAD(replica_configured_tailq, replica_bdev);
TAILQ_HEAD(replica_configuring_tailq, replica_bdev);
TAILQ_HEAD(replica_all_tailq, replica_bdev);
TAILQ_HEAD(replica_offline_tailq, replica_bdev);

extern struct replica_configured_tailq	g_replica_bdev_configured_list;
extern struct replica_configuring_tailq	g_replica_bdev_configuring_list;
extern struct replica_all_tailq		g_replica_bdev_list;
extern struct replica_offline_tailq	g_replica_bdev_offline_list;
extern struct replica_config		g_replica_config;

typedef void (*replica_bdev_destruct_cb)(void *cb_ctx, int rc);

int replica_bdev_create(struct replica_bdev_config *replica_cfg);
int replica_bdev_add_base_devices(struct replica_bdev_config *replica_cfg);
void replica_bdev_remove_base_devices(struct replica_bdev_config *replica_cfg,
				   replica_bdev_destruct_cb cb_fn, void *cb_ctx);
int replica_bdev_config_add(const char *replica_name, uint8_t num_base_bdevs, bool md,
			 struct replica_bdev_config **_replica_cfg);
int replica_bdev_config_add_base_bdev(struct replica_bdev_config *replica_cfg,
				   const char *base_bdev_name, uint8_t slot);
void replica_bdev_config_cleanup(struct replica_bdev_config *replica_cfg);
struct replica_bdev_config *replica_bdev_config_find_by_name(const char *replica_name);

/*
 * REPLICA module descriptor
 */
struct replica_bdev_module {
	/* Minimum required number of base bdevs. Must be > 0. */
	uint8_t base_bdevs_min;

	/*
	 * Maximum number of base bdevs that can be removed without failing
	 * the array.
	 */
	uint8_t base_bdevs_max_degraded;

	/*
	 * Called when the replica is starting, right before changing the state to
	 * online and registering the bdev. Parameters of the bdev like blockcnt
	 * should be set here.
	 *
	 * Non-zero return value will abort the startup process.
	 */
	int (*start)(struct replica_bdev *replica_bdev);

	/*
	 * Called when the replica is stopping, right before changing the state to
	 * offline and unregistering the bdev. Optional.
	 */
	void (*stop)(struct replica_bdev *replica_bdev);

	/* Handler for R/W requests */
	void (*submit_rw_request)(struct replica_bdev_io *replica_io);

	/* Handler for requests without payload (flush, unmap). Optional. */
	void (*submit_null_payload_request)(struct replica_bdev_io *replica_io);

	TAILQ_ENTRY(replica_bdev_module) link;
};

bool
replica_bdev_io_complete_part(struct replica_bdev_io *replica_io, uint64_t completed,
			   enum spdk_bdev_io_status status);
void
replica_bdev_queue_io_wait(struct replica_bdev_io *replica_io, struct spdk_bdev *bdev,
			struct spdk_io_channel *ch, spdk_bdev_io_wait_cb cb_fn);
void
replica_bdev_io_complete(struct replica_bdev_io *replica_io, enum spdk_bdev_io_status status);

#endif /* SPDK_BDEV_REPLICA_INTERNAL_H */
