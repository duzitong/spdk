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

/*_
 * WALS consists functionalities of WAL, Replica, and even sort of remote connection.
 * 'S' stands for the advanced version of WAL.
 * It can also stand for 'sliced' version of WAL.
 */

#ifndef SPDK_BDEV_WALS_INTERNAL_H
#define SPDK_BDEV_WALS_INTERNAL_H

#include "spdk/bdev_module.h"
#include "spdk/env.h"
#include "spdk/thread.h"
#include "spdk/trace.h"
#include "spdk_internal/trace_defs.h"
#include "../wal/bsl.h"

#define METADATA_VERSION		10086	// XD
#define NUM_TARGETS				4
#define QUORUM_TARGETS			3

#define OWNER_WALS				0x9
#define OBJECT_WALS_IO			0x9
#define TRACE_GROUP_WALS		0xE

#define TRACE_WALS_BSTAT_CREATE_START	SPDK_TPOINT_ID(TRACE_GROUP_WALS, 0x10)
#define TRACE_WALS_BSTAT_CREATE_END		SPDK_TPOINT_ID(TRACE_GROUP_WALS, 0x11)
#define TRACE_WALS_BSL_INSERT_START		SPDK_TPOINT_ID(TRACE_GROUP_WALS, 0x12)
#define TRACE_WALS_BSL_INSERT_END		SPDK_TPOINT_ID(TRACE_GROUP_WALS, 0x13)
#define TRACE_WALS_BSL_RAND_START		SPDK_TPOINT_ID(TRACE_GROUP_WALS, 0x14)
#define TRACE_WALS_BSL_RAND_END			SPDK_TPOINT_ID(TRACE_GROUP_WALS, 0x15)
#define TRACE_WALS_MOVE_READ_MD			SPDK_TPOINT_ID(TRACE_GROUP_WALS, 0x21)
#define TRACE_WALS_MOVE_READ_DATA		SPDK_TPOINT_ID(TRACE_GROUP_WALS, 0x22)
#define TRACE_WALS_MOVE_WRITE_DATA		SPDK_TPOINT_ID(TRACE_GROUP_WALS, 0x23)
#define TRACE_WALS_MOVE_UPDATE_HEAD		SPDK_TPOINT_ID(TRACE_GROUP_WALS, 0x24)
#define TRACE_WALS_MOVE_WAIT_OTHERS		SPDK_TPOINT_ID(TRACE_GROUP_WALS, 0x25)
#define TRACE_WALS_MOVE_UPDATE_HEAD_END	SPDK_TPOINT_ID(TRACE_GROUP_WALS, 0x26)
#define TRACE_WALS_MOVE_CALLED			SPDK_TPOINT_ID(TRACE_GROUP_WALS, 0x30)
#define TRACE_WALS_MOVE_MD_LOCKED		SPDK_TPOINT_ID(TRACE_GROUP_WALS, 0x31)
#define TRACE_WALS_MOVE_NO_WORKER		SPDK_TPOINT_ID(TRACE_GROUP_WALS, 0x32)

/*
 * WALS state describes the state of the wals bdev. This wals bdev can be either in
 * configured list or configuring list
 */
enum wals_bdev_state {
	/* wals bdev is ready and is seen by upper layers */
	WALS_BDEV_STATE_ONLINE,

	/*
	 * wals bdev is configuring, not all underlying bdevs are present.
	 * And can't be seen by upper layers.
	 */
	WALS_BDEV_STATE_CONFIGURING,

	/*
	 * In offline state, wals bdev layer will complete all incoming commands without
	 * submitting to underlying base nvme bdevs
	 */
	WALS_BDEV_STATE_OFFLINE,

	/* wals bdev max, new states should be added before this */
	WALS_BDEV_MAX
};

/*
 * WALS target module descriptor
 */
struct wals_target_module {
	/* name of the module */
	char	*name;

	/*
	 * Called when the wals is starting, right before changing the state to
	 * online and registering the bdev. Parameters of the bdev like blockcnt
	 * should be set here.
	 *
	 * Non-zero return value will abort the startup process.
	 */
	struct wals_target* (*start)(struct wals_target_config *config);

	/*
	 * Called when the wals is stopping, right before changing the state to
	 * offline and unregistering the bdev. Optional.
	 */
	void (*stop)(struct wals_target *target);

	/* Handler for log read requests */
	void (*submit_log_read_request)(struct wals_target* target, struct wals_bdev_io *wals_io);

	/* Handler for core read requests */
	void (*submit_core_read_request)(struct wals_target* target, struct wals_bdev_io *wals_io);

	/* Handler for log write requests */
	void (*submit_log_write_request)(struct wals_target* target, struct wals_bdev_io *wals_io);

	TAILQ_ENTRY(wals_slice_module) link;
};

struct wals_metadata {
	uint64_t	version;
	
	uint64_t	seq;

	uint64_t	next_offset;

	/* log blockcnt */
	uint64_t	length;

	uint64_t	core_offset;

	/* core blockcnt */
	uint64_t	core_length;

	uint64_t	round;
};

struct wals_target_info {
	char		*nqn;

	char		*address;

	uint16_t	port;
};

struct wals_target_config {
	struct wals_target_info 	target_log_info;

	struct wals_target_info 	target_core_info;
};

struct wals_target {
	volatile uint64_t				log_head_offset;

	volatile uint64_t				log_head_round;

	void							*private_info;
};

struct wals_slice {
	uint64_t	seq;

	uint64_t	log_tail_offset;

	uint64_t	log_tail_round;

	struct wals_target	target[NUM_TARGETS];
};

/*
 * wals_bdev_io is the context part of bdev_io. It contains the information
 * related to bdev_io for a wals bdev
 */
struct wals_bdev_io {
	/* The wals bdev associated with this IO */
	struct wals_bdev *wals_bdev;

	/* WaitQ entry, used only in waitq logic */
	struct spdk_bdev_io_wait_entry	waitq_entry;

	/* Context of the original channel for this IO */
	struct wals_bdev_io_channel	*wals_ch;

	/* the original IO */
	struct spdk_bdev_io	*orig_io;

	/* the original thread */
	struct spdk_thread	*orig_thread;

	/* save for completion on orig thread */
	enum spdk_bdev_io_status status;

	struct wals_metadata	*metadata;
	
	uint16_t	remaining_base_bdev_io;

	void	*read_buf;

	/* link next for pending writes */
	TAILQ_ENTRY(wals_bdev_io)	tailq;
};

/*
 * wals_bdev is the single entity structure which contains SPDK block device
 * and the information related to any wals bdev either configured or
 * in configuring list. io device is created on this.
 */
struct wals_bdev {
	/* wals bdev device, this will get registered in bdev layer */
	struct spdk_bdev		bdev;

	/* link of wals bdev to link it to configured, configuring or offline list */
	TAILQ_ENTRY(wals_bdev)		state_link;

	/* link of wals bdev to link it to global wals bdev list */
	TAILQ_ENTRY(wals_bdev)		global_link;

	/* pointer to config file entry */
	struct wals_bdev_config		*config;

	/* block length bit shift for optimized calculation */
	uint32_t			blocklen_shift;

	/* state of wals bdev */
	enum wals_bdev_state		state;

	/* Set to true if destruct is called for this wals bdev */
	bool				destruct_called;

	/* Set to true if destroy of this wals bdev is started. */
	bool				destroy_started;

	/* count of open channels */
	uint32_t			ch_count;

	/* write thread */
	struct spdk_thread		*write_thread;

	/* read thread */
	struct spdk_thread		*read_thread;

	/* mutex to set thread and pollers */
	pthread_mutex_t			mutex;

	/* poller to complete pending writes */
	struct spdk_poller		*pending_writes_poller;

	/* poller to clean index */
	struct spdk_poller		*cleaner_poller;

	/* poller to report stat */
	struct spdk_poller		*stat_poller;

	/* number of slices */
	uint64_t			slicecnt;

	/* number of blocks per slice */
	uint64_t			slice_blockcnt;

	struct wals_slice	*slices;

	/* buffer block length */
	uint64_t			buffer_blocklen;

	/* number of blocks of the buffer */
	uint64_t			buffer_blockcnt;

	uint64_t			buffer_tail;

	uint64_t			buffer_head;

	/* bsl node mempool */
	struct spdk_mempool		*bsl_node_pool;

	/* bstat mempool */
	struct spdk_mempool		*bstat_pool;

	/* skip list index */
	struct bskiplist 	*bsl;

	/* nodes to free */
	struct bskiplistFreeNodes *bslfn;

	struct wals_target_module	*module;

	/* pending writes due to no enough space on log device or (unlikely) buffer */
	TAILQ_HEAD(, wals_bdev_io)	pending_writes;
};

struct wals_slice_config {
	struct wals_target_config	targets[NUM_TARGETS];
};

/*
 * wals_bdev_config contains the wals bdev config related information after
 * parsing the config file
 */
struct wals_bdev_config {
	char						*name;

	struct wals_slice_config 	*slices;

	uint64_t					slicecnt;

	char						*module_name;

	/* Points to already created wals bdev  */
	struct wals_bdev			*wals_bdev;

	TAILQ_ENTRY(wals_bdev_config)	link;
};

/*
 * wals_config is the top level structure representing the wals bdev config as read
 * from config file for all walss
 */
struct wals_config {
	/* wals bdev  context from config file */
	TAILQ_HEAD(, wals_bdev_config) wals_bdev_config_head;

	/* total wals bdev  from config file */
	uint8_t total_wals_bdev;
};

/*
 * wals_bdev_io_channel is the context of spdk_io_channel for wals bdev device. It
 * contains the relationship of wals bdev io channel with base bdev io channels.
 */
struct wals_bdev_io_channel {
	/* wals bdev */
	struct wals_bdev			*wals_bdev;
};

/* TAIL heads for various wals bdev lists */
TAILQ_HEAD(wals_configured_tailq, wals_bdev);
TAILQ_HEAD(wals_configuring_tailq, wals_bdev);
TAILQ_HEAD(wals_all_tailq, wals_bdev);
TAILQ_HEAD(wals_offline_tailq, wals_bdev);

extern struct wals_configured_tailq	g_wals_bdev_configured_list;
extern struct wals_configuring_tailq	g_wals_bdev_configuring_list;
extern struct wals_all_tailq		g_wals_bdev_list;
extern struct wals_offline_tailq	g_wals_bdev_offline_list;
extern struct wals_config		g_wals_config;

typedef void (*wals_bdev_destruct_cb)(void *cb_ctx, int rc);

int wals_bdev_config_add(const char *wals_name, uint64_t slicecnt, const char *module_name,
			 struct wals_bdev_config **_wals_cfg);
int wals_bdev_create(struct wals_bdev_config *wals_cfg);
int wals_bdev_add_targets(struct wals_bdev_config *wals_cfg, uint64_t slicenum, struct wals_target_config *targets);
void wals_bdev_remove_base_devices(struct wals_bdev_config *wals_cfg,
				   wals_bdev_destruct_cb cb_fn, void *cb_ctx);
void wals_bdev_config_cleanup(struct wals_bdev_config *wals_cfg);
struct wals_bdev_config *wals_bdev_config_find_by_name(const char *wals_name);

bool
wals_bdev_io_complete_part(struct wals_bdev_io *wals_io, uint64_t completed,
			   enum spdk_bdev_io_status status);
void
wals_bdev_queue_io_wait(struct wals_bdev_io *wals_io, struct spdk_bdev *bdev,
			struct spdk_io_channel *ch, spdk_bdev_io_wait_cb cb_fn);
void
wals_bdev_io_complete(struct wals_bdev_io *wals_io, enum spdk_bdev_io_status status);

#endif /* SPDK_BDEV_WALS_INTERNAL_H */
