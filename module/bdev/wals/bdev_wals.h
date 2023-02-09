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
#include "dma_heap.h"

#define METADATA_VERSION		10086	// XD
#define MAGIC_INIT_CRC			0x88610086

#define NUM_TARGETS				4
#define QUORUM_TARGETS			3

#define OWNER_WALS				0x9
#define OBJECT_WALS_IO			0x9
#define OBJECT_WALS_BDEV		0xA
#define TRACE_GROUP_WALS		0xE

/*
 * _S: start
 * _F: finish
 * _SUB: submit
 * _COMP: complete
 * _W: write
 * _R: read
 */

#define TRACE_WALS_S_SUB_IO				SPDK_TPOINT_ID(TRACE_GROUP_WALS, 0x00)
#define TRACE_WALS_F_SUB_IO				SPDK_TPOINT_ID(TRACE_GROUP_WALS, 0x01)
#define TRACE_WALS_S_COMP_IO			SPDK_TPOINT_ID(TRACE_GROUP_WALS, 0x02)
#define TRACE_WALS_F_COMP_IO			SPDK_TPOINT_ID(TRACE_GROUP_WALS, 0x03)
#define TRACE_WALS_S_INSERT_INDEX		SPDK_TPOINT_ID(TRACE_GROUP_WALS, 0x04)	// insert index
#define TRACE_WALS_F_INSERT_INDEX		SPDK_TPOINT_ID(TRACE_GROUP_WALS, 0x05)
#define TRACE_WALS_S_CLEAN_INDEX		SPDK_TPOINT_ID(TRACE_GROUP_WALS, 0x06)	// clean index
#define TRACE_WALS_F_CLEAN_INDEX		SPDK_TPOINT_ID(TRACE_GROUP_WALS, 0x07)
#define TRACE_WALS_S_UPDATE_HEAD		SPDK_TPOINT_ID(TRACE_GROUP_WALS, 0x08)	// update log head
#define TRACE_WALS_F_UPDATE_HEAD		SPDK_TPOINT_ID(TRACE_GROUP_WALS, 0x09)
#define TRACE_WALS_S_CALC_CRC			SPDK_TPOINT_ID(TRACE_GROUP_WALS, 0x0A)	// calculate crc
#define TRACE_WALS_F_CALC_CRC			SPDK_TPOINT_ID(TRACE_GROUP_WALS, 0x0B)

#define TRACE_WALS_S_SUB_W				SPDK_TPOINT_ID(TRACE_GROUP_WALS, 0x10)
#define TRACE_WALS_F_SUB_W				SPDK_TPOINT_ID(TRACE_GROUP_WALS, 0x11)
#define TRACE_WALS_S_SUB_W_I			SPDK_TPOINT_ID(TRACE_GROUP_WALS, 0x12) 	// internal
#define TRACE_WALS_F_SUB_W_I			SPDK_TPOINT_ID(TRACE_GROUP_WALS, 0x13)
#define TRACE_WALS_S_SUB_W_T			SPDK_TPOINT_ID(TRACE_GROUP_WALS, 0x14)	// target
#define TRACE_WALS_F_SUB_W_T			SPDK_TPOINT_ID(TRACE_GROUP_WALS, 0x15)
#define TRACE_WALS_S_COMP_W_T			SPDK_TPOINT_ID(TRACE_GROUP_WALS, 0x16)
#define TRACE_WALS_F_COMP_W_T			SPDK_TPOINT_ID(TRACE_GROUP_WALS, 0x17)
#define TRACE_WALS_S_COMP_W_Q			SPDK_TPOINT_ID(TRACE_GROUP_WALS, 0x18) 	// quorum
#define TRACE_WALS_F_COMP_W_Q			SPDK_TPOINT_ID(TRACE_GROUP_WALS, 0x19)
#define TRACE_WALS_S_COMP_W_A			SPDK_TPOINT_ID(TRACE_GROUP_WALS, 0x1A)	// all
#define TRACE_WALS_F_COMP_W_A			SPDK_TPOINT_ID(TRACE_GROUP_WALS, 0x1B)
#define TRACE_WALS_S_COMP_W_F			SPDK_TPOINT_ID(TRACE_GROUP_WALS, 0x1C)	// failed
#define TRACE_WALS_F_COMP_W_F			SPDK_TPOINT_ID(TRACE_GROUP_WALS, 0x1D)
#define TRACE_WALS_S_COMP_W_WM			SPDK_TPOINT_ID(TRACE_GROUP_WALS, 0x1E)	// wait msg
#define TRACE_WALS_F_COMP_W_WM			SPDK_TPOINT_ID(TRACE_GROUP_WALS, 0x1F)

#define TRACE_WALS_S_SUB_R				SPDK_TPOINT_ID(TRACE_GROUP_WALS, 0x20)
#define TRACE_WALS_F_SUB_R				SPDK_TPOINT_ID(TRACE_GROUP_WALS, 0x21)
#define TRACE_WALS_S_SUB_R_T			SPDK_TPOINT_ID(TRACE_GROUP_WALS, 0x22)	// target
#define TRACE_WALS_F_SUB_R_T			SPDK_TPOINT_ID(TRACE_GROUP_WALS, 0x23)
#define TRACE_WALS_S_COMP_R_T			SPDK_TPOINT_ID(TRACE_GROUP_WALS, 0x24)
#define TRACE_WALS_F_COMP_R_T			SPDK_TPOINT_ID(TRACE_GROUP_WALS, 0x25)
#define TRACE_WALS_S_COMP_R_A			SPDK_TPOINT_ID(TRACE_GROUP_WALS, 0x26)	// all
#define TRACE_WALS_F_COMP_R_A			SPDK_TPOINT_ID(TRACE_GROUP_WALS, 0x27)

#define TRACE_WALS_S_RDMA_CQ            SPDK_TPOINT_ID(TRACE_GROUP_WALS, 0x36)  // rdma poller
#define TRACE_WALS_F_RDMA_CQ            SPDK_TPOINT_ID(TRACE_GROUP_WALS, 0x37)  // rdma poller


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

typedef uint32_t wals_crc;

struct wals_metadata {
	uint64_t	version;
	
	uint64_t	seq;

	uint64_t	next_offset;

	uint64_t	length;

	uint64_t	core_offset;

	uint64_t	round;

	uint64_t	md_blocknum;

	// new fields add before checksums
	wals_crc	md_checksum;

	wals_crc	data_checksum[0];
};

struct wals_checksum_offset {
	uint64_t	block_offset;

	uint64_t	byte_offset;
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

typedef struct wals_log_position {
	uint64_t	offset;
	uint64_t	round;
} wals_log_position;

struct wals_lp_firo_entry {
	wals_log_position 				pos;

	bool							removed;

	TAILQ_ENTRY(wals_lp_firo_entry)	link;
};

struct wals_lp_firo {	
	TAILQ_HEAD(, wals_lp_firo_entry)	head;

	struct spdk_mempool					*entry_pool;
};

struct wals_target {
	int id;

	volatile uint64_t			log_blockcnt;

	// BUG: need to be atomicly updated
	volatile wals_log_position	head;

	void						*private_info;
};

struct wals_slice {
	uint64_t					seq;

	uint64_t					log_blockcnt;

	// updated by write thread to point to the tail offset of 
	// PMEM buffer.
	wals_log_position			tail;

	// BUG: need to be atomicly updated (RCU)
	// min(outstanding read requests offset, min(targets.offset))
	// [head, tail] is the current PMEM buffer that is readable.
	// head should only advance when there are no read PMEM IOs.
	// 
	// It should be the minimum of destage tail of all four data nodes, 
	// because it can be useful for a data node to recover from other nodes.
	volatile wals_log_position	head;

	wals_log_position			committed_tail;

	struct wals_target			*targets[NUM_TARGETS];

	struct wals_lp_firo			*write_firo;

	// outstanding read requests offset
	struct wals_lp_firo			*read_firo;
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
	volatile enum spdk_bdev_io_status status;

	struct wals_metadata	*metadata;
	
	uint16_t	remaining_read_requests;

	struct wals_lp_firo_entry	*firo_entry;

	struct dma_page	*dma_page;

	uint64_t	slice_index;

	uint64_t	total_num_blocks;

	/* write targets failed */
	/* read  targets failed */
	int		targets_failed;

	/* write targets completed */
	int		targets_completed;

	/* track read target */
	int		target_index;

	bool	io_completed;
};

typedef int (*wals_target_fn)(struct wals_target* target, struct wals_bdev *wals_bdev);

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
	struct wals_target* (*start)(struct wals_target_config *config, struct wals_bdev *wals_bdev, struct wals_slice *slice);

	/*
	 * Called when the wals is stopping, right before changing the state to
	 * offline and unregistering the bdev. Optional.
	 */
	void (*stop)(struct wals_target *target, struct wals_bdev *wals_bdev);

	/* Handler for log read requests */
	int (*submit_log_read_request)(struct wals_target *target, void *data, uint64_t offset, uint64_t cnt, struct wals_checksum_offset checksum_offset, struct wals_bdev_io *wals_io);

	/* Handler for core read requests */
	int (*submit_core_read_request)(struct wals_target *target, void *data, uint64_t offset, uint64_t cnt, struct wals_bdev_io *wals_io);

	/* Handler for log write requests */
	int (*submit_log_write_request)(struct wals_target *target, void *data, uint64_t offset, uint64_t cnt, struct wals_bdev_io *wals_io);

	/* register write pollers */
	int (*register_write_pollers)(struct wals_target *target, struct wals_bdev *wals_bdev);

	/* unregister write pollers */
	int (*unregister_write_pollers)(struct wals_target *target, struct wals_bdev *wals_bdev);

	/* register read pollers */
	int (*register_read_pollers)(struct wals_target *target, struct wals_bdev *wals_bdev);

	/* register read pollers */
	int (*unregister_read_pollers)(struct wals_target *target, struct wals_bdev *wals_bdev);

	TAILQ_ENTRY(wals_target_module) link;
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

	/* state of wals bdev */
	enum wals_bdev_state		state;

	/* Set to true if destruct is called for this wals bdev */
	bool				destruct_called;

	/* Set to true if destroy of this wals bdev is started. */
	bool				destroy_started;

	/* count of open channels */
	uint32_t			ch_count;

	uint32_t				write_lcore;

	/* write thread */
	struct spdk_thread		*write_thread;

	uint32_t				read_lcore;

	/* read thread */
	struct spdk_thread		*read_thread;

	/* mutex to set thread and pollers */
	pthread_mutex_t			mutex;

	/* poller to update log head */
	struct spdk_poller		*log_head_update_poller;

	/* poller to clean index */
	struct spdk_poller		*cleaner_poller;

	/* poller to report stat */
	struct spdk_poller		*stat_poller;

	/* number of slices */
	uint64_t			slicecnt;

	/* number of blocks per slice */
	uint64_t			slice_blockcnt;

	struct wals_slice	*slices;

	/* block length */
	uint64_t			blocklen;

	/* blocklen shift for fast calculation */
	int					blocklen_shift;

	/* number of blocks of the buffer */
	uint64_t			buffer_blockcnt;

	struct dma_heap		*write_heap;

	/* buffer for reads, with size of write buffer for now */
	struct dma_heap		*read_heap;

	/* bsl node mempool */
	struct spdk_mempool		*bsl_node_pool;

	/* bstat mempool */
	struct spdk_mempool		*bstat_pool;

	/* index message pool */
	struct spdk_mempool		*index_msg_pool;

	/* skip list index */
	struct bskiplist 	*bsl;

	/* nodes to free */
	struct bskiplistFreeNodes *bslfn;

	struct wals_target_module	*module;

	uint64_t		ch_create_tsc;
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

	uint64_t					blocklen;

	uint64_t					slice_blockcnt;

	uint64_t					buffer_blockcnt;

	uint32_t					write_lcore;

	uint32_t					read_lcore;

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

struct wals_index_msg {
	uint64_t			begin;

	uint64_t			end;

	uint64_t			round;

	uint64_t			offset;

	uint64_t			md_offset;

	bool				failed;

	struct wals_bdev	*wals_bdev;
};

/* structs for rpc*/
struct rpc_bdev_wals_target_info {
	char		*nqn;

	char		*address;

	uint16_t	port;
};

struct rpc_bdev_wals_target {
	struct rpc_bdev_wals_target_info	log;
	
	struct rpc_bdev_wals_target_info	core;
};

struct rpc_bdev_wals_slice {
	size_t						 	num_targets;

	struct rpc_bdev_wals_target		targets[NUM_TARGETS];
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

int wals_bdev_config_add(const char *wals_name, const char *module_name,
			struct rpc_bdev_wals_slice *slices, uint64_t slicecnt,
			uint64_t blocklen, uint64_t blockcnt, uint64_t buffer_blockcnt,
			uint32_t write_lcore, uint32_t read_lcore,
			struct wals_bdev_config **_wals_cfg);
int wals_bdev_create(struct wals_bdev_config *wals_cfg);
int wals_bdev_start_all(struct wals_bdev_config *wal_cfg);
int wals_bdev_add_targets(struct wals_bdev_config *wals_cfg, uint64_t slicenum, struct wals_target_config *targets);
void wals_bdev_remove_base_devices(struct wals_bdev_config *wals_cfg,
				   wals_bdev_destruct_cb cb_fn, void *cb_ctx);
void wals_bdev_config_cleanup(struct wals_bdev_config *wals_cfg);
struct wals_bdev_config *wals_bdev_config_find_by_name(const char *wals_name);

void wals_bdev_target_module_list_add(struct wals_target_module *target_module);

#define __TARGET_MODULE_REGISTER(line) __TARGET_MODULE_REGISTER_(line)
#define __TARGET_MODULE_REGISTER_(line) target_module_register_##line

#define TARGET_MODULE_REGISTER(_module)					\
__attribute__((constructor)) static void				\
__TARGET_MODULE_REGISTER(__LINE__)(void)					\
{									\
    wals_bdev_target_module_list_add(_module);					\
}

wals_crc
wals_bdev_calc_crc(void *data, size_t len);
void
wals_target_read_complete(struct wals_bdev_io *wals_io, bool success);
void
wals_target_write_complete(struct wals_bdev_io *wals_io, bool success);
void
wals_bdev_io_complete(struct wals_bdev_io *wals_io, enum spdk_bdev_io_status status);

#endif /* SPDK_BDEV_WALS_INTERNAL_H */
