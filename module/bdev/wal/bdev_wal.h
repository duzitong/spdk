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

#ifndef SPDK_BDEV_WAL_INTERNAL_H
#define SPDK_BDEV_WAL_INTERNAL_H

#include "spdk/bdev_module.h"
#include "spdk/env.h"
#include "spdk/thread.h"
#include "spdk/trace.h"
#include "spdk_internal/trace_defs.h"
#include "bsl.h"

#define METADATA_VERSION	10086	// XD

#define TRACE_BDEV_BSTAT_CREATE_START	SPDK_TPOINT_ID(TRACE_GROUP_BDEV, 0x10)
#define TRACE_BDEV_BSTAT_CREATE_END		SPDK_TPOINT_ID(TRACE_GROUP_BDEV, 0x11)
#define TRACE_BDEV_BSL_INSERT_START		SPDK_TPOINT_ID(TRACE_GROUP_BDEV, 0x12)
#define TRACE_BDEV_BSL_INSERT_END		SPDK_TPOINT_ID(TRACE_GROUP_BDEV, 0x13)

/*
 * WAL state describes the state of the wal bdev. This wal bdev can be either in
 * configured list or configuring list
 */
enum wal_bdev_state {
	/* wal bdev is ready and is seen by upper layers */
	WAL_BDEV_STATE_ONLINE,

	/*
	 * wal bdev is configuring, not all underlying bdevs are present.
	 * And can't be seen by upper layers.
	 */
	WAL_BDEV_STATE_CONFIGURING,

	/*
	 * In offline state, wal bdev layer will complete all incoming commands without
	 * submitting to underlying base nvme bdevs
	 */
	WAL_BDEV_STATE_OFFLINE,

	/* wal bdev max, new states should be added before this */
	WAL_BDEV_MAX
};

/*
 * wal_base_bdev_info contains information for the base bdevs which are part of some
 * wal. This structure contains the per base bdev information. Whatever is
 * required per base device for wal bdev will be kept here
 */
struct wal_base_bdev_info {
	/* pointer to base spdk bdev */
	struct spdk_bdev	*bdev;

	/* pointer to base bdev descriptor opened by wal bdev */
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
 * wal_bdev_io is the context part of bdev_io. It contains the information
 * related to bdev_io for a wal bdev
 */
struct wal_bdev_io {
	/* The wal bdev associated with this IO */
	struct wal_bdev *wal_bdev;

	/* WaitQ entry, used only in waitq logic */
	struct spdk_bdev_io_wait_entry	waitq_entry;

	/* Context of the original channel for this IO */
	struct wal_bdev_io_channel	*wal_ch;

	/* the original IO */
	struct spdk_bdev_io	*orig_io;

	/* the original thread */
	struct spdk_bdev_io	*orig_thread;

	/* save for completion on orig thread */
	enum spdk_bdev_io_status status;

	struct wal_metadata	*metadata;

	uint16_t	remaining_base_bdev_io;

	void	*read_buf;
};

/*
 * wal_bdev is the single entity structure which contains SPDK block device
 * and the information related to any wal bdev either configured or
 * in configuring list. io device is created on this.
 */
struct wal_bdev {
	/* wal bdev device, this will get registered in bdev layer */
	struct spdk_bdev		bdev;

	/* link of wal bdev to link it to configured, configuring or offline list */
	TAILQ_ENTRY(wal_bdev)		state_link;

	/* link of wal bdev to link it to global wal bdev list */
	TAILQ_ENTRY(wal_bdev)		global_link;

	/* pointer to config file entry */
	struct wal_bdev_config		*config;

	/* bdev info of log bdev */
	struct wal_base_bdev_info	log_bdev_info;

	/* bdev info of core bdev */
	struct wal_base_bdev_info	core_bdev_info;

	/* block length bit shift for optimized calculation */
	uint32_t			blocklen_shift;

	/* state of wal bdev */
	enum wal_bdev_state		state;

	/* Set to true if destruct is called for this wal bdev */
	bool				destruct_called;

	/* Set to true if destroy of this wal bdev is started. */
	bool				destroy_started;

	/* open thread */
	struct spdk_thread		*open_thread;

	/* mutex to set thread and pollers */
	pthread_mutex_t			mutex;

	bool					open_thread_set;

	/* bsl node mempool */
	struct spdk_mempool		*bsl_node_pool;

	/* bstat mempool */
	struct spdk_mempool		*bstat_pool;

	/* sequence id */
	uint64_t	seq;

	/* head offset of logs */
	uint64_t	log_head;
	
	/* current round of log head */
	uint64_t	head_round;

	/* tail offset of logs */
	uint64_t	log_tail;

	/* current round of log tail */
	uint64_t	tail_round;

	/* max blocks of logs */
	uint64_t	log_max;

	/* skip list index */
	struct bskiplist 	*bsl;

	/* nodes to free */
	struct bskiplistFreeNodes *bslfn;

	/* indicate whether there's ongoing moving task */
	bool		moving;
};

struct wal_metadata {
	uint64_t	version;
	
	uint64_t	seq;

	uint64_t	next_offset;

	uint64_t	length;

	uint64_t	core_offset;

	uint64_t	core_length;

	uint64_t	round;
};

/* info stored in the last block of log bdev */
struct wal_log_info {
	uint64_t	head;
};

struct wal_mover_context {
	struct wal_bdev				*bdev;

	struct wal_bdev_io_channel	*ch;

	struct wal_metadata 		*metadata;

	void 						*data;

	struct wal_log_info			*info;
};

/*
 * wal_base_bdev_config is the per base bdev data structure which contains
 * information w.r.t to per base bdev during parsing config
 */
struct wal_base_bdev_config {
	/* base bdev name from config file */
	char				*name;
};

/*
 * wal_bdev_config contains the wal bdev config related information after
 * parsing the config file
 */
struct wal_bdev_config {
	/* base bdev of log bdev */
	struct wal_base_bdev_config	log_bdev;

	/* base bdev of core bdev */
	struct wal_base_bdev_config	core_bdev;

	/* Points to already created wal bdev  */
	struct wal_bdev		*wal_bdev;

	char				*name;

	TAILQ_ENTRY(wal_bdev_config)	link;
};

/*
 * wal_config is the top level structure representing the wal bdev config as read
 * from config file for all wals
 */
struct wal_config {
	/* wal bdev  context from config file */
	TAILQ_HEAD(, wal_bdev_config) wal_bdev_config_head;

	/* total wal bdev  from config file */
	uint8_t total_wal_bdev;
};

/*
 * wal_bdev_io_channel is the context of spdk_io_channel for wal bdev device. It
 * contains the relationship of wal bdev io channel with base bdev io channels.
 */
struct wal_bdev_io_channel {
	/* wal bdev */
	struct wal_bdev			*wal_bdev;

	/* IO channel of log bdev */
	struct spdk_io_channel	*log_channel;

	/* IO channel of core bdev */
	struct spdk_io_channel  *core_channel;

	/* poller to move data from log bdev to core bdev */
	struct spdk_poller		*mover_poller;

	/* poller to clean index */
	struct spdk_poller		*cleaner_poller;

	/* poller to report stat */
	struct spdk_poller		*stat_poller;
};

/* TAIL heads for various wal bdev lists */
TAILQ_HEAD(wal_configured_tailq, wal_bdev);
TAILQ_HEAD(wal_configuring_tailq, wal_bdev);
TAILQ_HEAD(wal_all_tailq, wal_bdev);
TAILQ_HEAD(wal_offline_tailq, wal_bdev);

extern struct wal_configured_tailq	g_wal_bdev_configured_list;
extern struct wal_configuring_tailq	g_wal_bdev_configuring_list;
extern struct wal_all_tailq		g_wal_bdev_list;
extern struct wal_offline_tailq	g_wal_bdev_offline_list;
extern struct wal_config		g_wal_config;

typedef void (*wal_bdev_destruct_cb)(void *cb_ctx, int rc);

int wal_bdev_create(struct wal_bdev_config *wal_cfg);
int wal_bdev_add_base_devices(struct wal_bdev_config *wal_cfg);
void wal_bdev_remove_base_devices(struct wal_bdev_config *wal_cfg,
				   wal_bdev_destruct_cb cb_fn, void *cb_ctx);
int wal_bdev_config_add(const char *wal_name, const char *log_bdev_name, const char *core_bdev_name,
			 struct wal_bdev_config **_wal_cfg);
void wal_bdev_config_cleanup(struct wal_bdev_config *wal_cfg);
struct wal_bdev_config *wal_bdev_config_find_by_name(const char *wal_name);

/*
 * WAL module descriptor
 */
struct wal_bdev_module {
	/* Minimum required number of base bdevs. Must be > 0. */
	uint8_t base_bdevs_min;

	/*
	 * Maximum number of base bdevs that can be removed without failing
	 * the array.
	 */
	uint8_t base_bdevs_max_degraded;

	/*
	 * Called when the wal is starting, right before changing the state to
	 * online and registering the bdev. Parameters of the bdev like blockcnt
	 * should be set here.
	 *
	 * Non-zero return value will abort the startup process.
	 */
	int (*start)(struct wal_bdev *wal_bdev);

	/*
	 * Called when the wal is stopping, right before changing the state to
	 * offline and unregistering the bdev. Optional.
	 */
	void (*stop)(struct wal_bdev *wal_bdev);

	/* Handler for R/W requests */
	void (*submit_rw_request)(struct wal_bdev_io *wal_io);

	/* Handler for requests without payload (flush, unmap). Optional. */
	void (*submit_null_payload_request)(struct wal_bdev_io *wal_io);

	TAILQ_ENTRY(wal_bdev_module) link;
};

bool
wal_bdev_io_complete_part(struct wal_bdev_io *wal_io, uint64_t completed,
			   enum spdk_bdev_io_status status);
void
wal_bdev_queue_io_wait(struct wal_bdev_io *wal_io, struct spdk_bdev *bdev,
			struct spdk_io_channel *ch, spdk_bdev_io_wait_cb cb_fn);
void
wal_bdev_io_complete(struct wal_bdev_io *wal_io, enum spdk_bdev_io_status status);

#endif /* SPDK_BDEV_WAL_INTERNAL_H */
