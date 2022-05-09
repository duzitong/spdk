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

#include "bdev_replica.h"

#include "spdk/env.h"
#include "spdk/thread.h"
#include "spdk/string.h"
#include "spdk/util.h"

#include "spdk/log.h"

#define MAX_QUORUM_LOSS 1

struct initiator_info {
	/* Owning replica bdev */
	struct replica_bdev 		*replica_bdev

    /* Seq number for current io. First 32 bits for topology ver., second for message seq. */
    uint64_t                    seq;

    /* Completed seq number of base bdevs */
	uint64_t                    completed_seq[MAX_BASE_BDEVS];

    /* Cleaner poller
	 * Cleaner publish the min completed_seq of all target nodes to all targets.
	 * Initiator and target nodes will remove replica logs before this seq number.
	 * Cleaner also needs to recycle data cache that one of the target nodes have moved onto its SSD.
	 * When recycling data cache, only data in replica log is recycled.
	 * Replica log entry is kept to track which target to read data from its SSD. 
	 */
    struct spdk_poller          *cleaner_poller;

    /* TailQ to maintain logs */
    TAILQ_HEAD(, replica_log)   replica_logs;

    /* Array index for replica logs 
	 * TODO: change to other data structure like b-tree.
	 */
    TAILQ_ENTRY(replica_log)    *index;
};

static void
initiator_base_bdev_write_complete(struct spdk_bdev_io *bdev_io, bool success, void *cb_arg)
{
	struct replica_bdev_io *replica_io = cb_arg;

	spdk_bdev_free_io(bdev_io);

	initiator_write_complete_part(replica_io, 1, success ?
				   SPDK_BDEV_IO_STATUS_SUCCESS :
				   SPDK_BDEV_IO_STATUS_FAILED);
}

/*
 * brief:
 * initiator_write_complete_part - signal the completion of a part of the expected
 * base bdev IOs and complete the replica_io if this is the final expected IO.
 * The caller should first set replica_io->base_bdev_io_remaining. This function
 * will decrement this counter by the value of the 'completed' parameter and
 * complete the replica_io if the counter reaches 0. The caller is free to
 * interpret the 'base_bdev_io_remaining' and 'completed' values as needed,
 * it can represent e.g. blocks or IOs.
 * params:
 * replica_io - pointer to replica_bdev_io
 * completed - the part of the replica_io that has been completed
 * status - status of the base IO
 * returns:
 * true - if the replica_io is completed
 * false - otherwise
 */
bool
initiator_write_complete_part(struct replica_bdev_io *replica_io, uint64_t completed,
			   enum spdk_bdev_io_status status)
{
	assert(replica_io->base_bdev_io_remaining >= completed);
	replica_io->base_bdev_io_remaining -= completed;

	if (status != SPDK_BDEV_IO_STATUS_SUCCESS) {
		replica_io->base_bdev_io_status = status;
	}

	if (replica_io->base_bdev_io_remaining <= MAX_QUORUM_LOSS) {
		/*
		 * TODO: Update mem cache
		 */
		
		replica_bdev_io_complete(replica_io, replica_io->base_bdev_io_status);
		return true;
	} else {
		return false;
	}
}

static void
initiator_submit_write_request(struct replica_bdev_io *replica_io);

static void
_initiator_submit_write_request(void *_replica_io)
{
	struct replica_bdev_io *replica_io = _replica_io;

	initiator_submit_write_request(replica_io);
}

/*
 * brief:
 * initiator_submit_write_request function submits write requests to member disks;
 * it will submit as many as possible unless a write fails with -ENOMEM, in
 * which case it will queue it for later submission
 * params:
 * replica_io
 * returns:
 * none
 */
static void
initiator_submit_write_request(struct replica_bdev_io *replica_io)
{
	struct spdk_bdev_io		*bdev_io = spdk_bdev_io_from_ctx(replica_io);
	struct replica_bdev		*replica_bdev;
	int				ret;
	uint8_t				i;
	struct replica_base_bdev_info	*base_info;
	struct spdk_io_channel		*base_ch;

	replica_bdev = replica_io->replica_bdev;

	if (replica_io->base_bdev_io_remaining == 0) {
		replica_io->base_bdev_io_remaining = replica_bdev->num_base_bdevs;
	}

	// TODO: construct metadata

	while (replica_io->base_bdev_io_submitted < replica_bdev->num_base_bdevs) {
		i = replica_io->base_bdev_io_submitted;
		base_info = &replica_bdev->base_bdev_info[i];
		base_ch = replica_io->replica_ch->base_channel[i];

		// TODO: send with metadata
		ret = spdk_bdev_writev_blocks(base_info->desc, base_ch,
						bdev_io->u.bdev.iovs, bdev_io->u.bdev.iovcnt,
						bdev_io->u.bdev.offset_blocks, bdev_io->u.bdev.num_blocks, initiator_base_bdev_write_complete,
						replica_io);

		if (ret == 0) {
			replica_io->base_bdev_io_submitted++;
		} else if (ret == -ENOMEM) {
			replica_bdev_queue_io_wait(replica_io, base_info->bdev, base_ch,
						_initiator_submit_write_request);
			return;
		} else {
			SPDK_ERRLOG("bdev io submit error not due to ENOMEM, it should not happen\n");
			assert(false);
			replica_bdev_io_complete(replica_io, SPDK_BDEV_IO_STATUS_FAILED);
			return;
		}
	}
}

static inline uint8_t
raid5_stripe_data_chunks_num(const struct raid_bdev *raid_bdev)
{
	return raid_bdev->num_base_bdevs - raid_bdev->module->base_bdevs_max_degraded;
}

static void
raid5_submit_rw_request(struct raid_bdev_io *raid_io)
{
	raid_bdev_io_complete(raid_io, SPDK_BDEV_IO_STATUS_FAILED);
}

static int
initiator_start(struct replica_bdev *replica_bdev)
{
	uint64_t min_blockcnt = UINT64_MAX;
	struct replica_base_bdev_info *base_info;
	struct initiator_info *initiator_info;

	initiator_info = calloc(1, sizeof(*initiator_info));
	if (!initiator_info) {
		SPDK_ERRLOG("Failed to allocate initiator_info\n");
		return -ENOMEM;
	}
	initiator_info->replica_bdev = replica_bdev;

	REPLICA_FOR_EACH_BASE_BDEV(replica_bdev, base_info) {
		min_blockcnt = spdk_min(min_blockcnt, base_info->bdev->blockcnt);
	}

	replica_bdev->bdev.blockcnt = min_blockcnt;

	replica_bdev->module_private = initiator_info;

	return 0;
}

static void
initiator_stop(struct replica_bdev *replica_bdev)
{
	struct initiator_info *initiator_info = replica_bdev->module_private;

	free(initiator_info);
}

static struct replica_bdev_module g_initiator_module = {
	.start = initiator_start,
	.stop = initiator_stop,
	.submit_read_request = ,
    .submit_write_request = initiator_submit_write_request,
};
REPLICA_INITIATOR_MODULE_REGISTER(&g_initiator_module)

SPDK_LOG_REGISTER_COMPONENT(bdev_replica_initiator)
