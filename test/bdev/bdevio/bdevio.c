/*   SPDX-License-Identifier: BSD-3-Clause
 *   Copyright (c) Intel Corporation.
 *   All rights reserved.
 *   Copyright (c) 2022 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 */

#include "spdk/stdinc.h"

#include "spdk/bdev.h"
#include "spdk/accel_engine.h"
#include "spdk/diagnostic.h"
#include "spdk/env.h"
#include "spdk/log.h"
#include "spdk/thread.h"
#include "spdk/event.h"
#include "spdk/rpc.h"
#include "spdk/util.h"
#include "spdk/string.h"
#include "spdk/memory.h"

#include "bdev_internal.h"
#include "CUnit/Basic.h"

#define BUFFER_IOVS		1024
#define BUFFER_SIZE		260 * 1024
#define BDEV_TASK_ARRAY_SIZE	2048
#define MAX_QD 32
#define DEFAULT_BLOCK_SIZE 512
#define DEFAULT_BLOCK_CNT 2097152
#define LAST_WRITTEN_ID_RECALL_CNT 10

pthread_mutex_t g_test_mutex;
pthread_cond_t g_test_cond;

static struct spdk_thread *g_thread_init;
static struct spdk_thread *g_thread_ut;
static struct spdk_thread *g_thread_io;
static bool g_wait_for_tests = false;
static int g_num_failures = 0;
static bool g_shutdown = false;
static bool g_node_failure = false;
static int g_long_running_seconds = 0;
static unsigned int g_seed = 0;
// set by the initialization of each loop (ut thread).
// only IO thread is allowed to modify it within the loop
static int g_remaining_io = 0;
// 512B, 4K, 8K, 16K, 32K, 64K, 1M, 2M
static const int g_block_cnt_arr[] = {1, 8, 16, 32, 64, 128, 1024 * 2, 1024 * 4};
static int g_write_id = 1;
static uint64_t g_last_written_id_pos[DEFAULT_BLOCK_CNT];
static uint64_t g_last_written_id[DEFAULT_BLOCK_CNT][LAST_WRITTEN_ID_RECALL_CNT];

enum io_failure_reason {
	IO_FAILURE_NO_FAILURE,
	IO_FAILURE_BLOCK_INCONSISTENT,
	IO_FAILURE_OUTDATED,
	IO_FAILURE_UNWRITTEN,
	IO_FAILURE_UNEXPECTED_READ_FAIL,
};

struct io_target {
	struct spdk_bdev	*bdev;
	struct spdk_bdev_desc	*bdev_desc;
	struct spdk_io_channel	*ch;
	struct io_target	*next;
};

static struct io_test_unit {
	uint64_t write_id;
	bool is_write;
	int offset;
	int block_cnt;
	void* buf;
	void* md_buf;
	// only success or not
	bool result;
};

static struct io_test_batch {
	int qd;
	struct io_test_unit* elements;
};

struct bdevio_request {
	char *buf;
	char *fused_buf;
	int data_len;
	uint64_t offset;
	struct iovec iov[BUFFER_IOVS];
	int iovcnt;
	struct iovec fused_iov[BUFFER_IOVS];
	int fused_iovcnt;
	struct io_target *target;
	struct io_test_unit* io;
};

struct io_target *g_io_targets = NULL;
struct io_target *g_current_io_target = NULL;
static void rpc_perform_tests_cb(unsigned num_failures, struct spdk_jsonrpc_request *request);

static void
initialize_buffer(char **buf, int pattern, int size)
{
	*buf = spdk_zmalloc(size, 0x1000, NULL, SPDK_ENV_LCORE_ID_ANY, SPDK_MALLOC_DMA);
	memset(*buf, pattern, size);
}

static struct io_test_unit* io_test_unit_alloc(struct io_test_unit* ptr,
	void* buf,
	void* md_buf,
	bool is_write,
	uint64_t write_id,
	int block_cnt,
	int offset
) {
	if (ptr == NULL) {
		ptr = calloc(1, sizeof(struct io_test_unit));
	}
	ptr->is_write = is_write;
	ptr->write_id = write_id;
	ptr->buf = buf;
	ptr->md_buf = md_buf;
	ptr->result = false;
	ptr->block_cnt = block_cnt;
	ptr->offset = offset;

	// write_id is 0 for read requests
	for (unsigned j = 0; j < ptr->block_cnt * DEFAULT_BLOCK_SIZE / sizeof(uint64_t); j++) {
		((uint64_t*)ptr->buf)[j] = ptr->write_id;
	}

	return ptr;
}

static struct io_test_batch*
io_test_batch_alloc(int qd, void* buf, void* md_buf, double write_prob) {
	struct io_test_unit* elements = calloc(qd, sizeof(struct io_test_unit));
	for (int i = 0; i < qd; i++) {
		bool is_write = ((double)rand() / RAND_MAX) < write_prob;
		int block_cnt = g_block_cnt_arr[rand() % SPDK_COUNTOF(g_block_cnt_arr)];
		io_test_unit_alloc(&elements[i],
			buf,
			md_buf,
			is_write,
			is_write ? g_write_id : 0,
			block_cnt,
			rand() % (DEFAULT_BLOCK_CNT - block_cnt));

		if (is_write) {
			g_write_id++;
		}

		buf += elements[i].block_cnt * DEFAULT_BLOCK_SIZE;
	}

	struct io_test_batch* batch = calloc(1, sizeof(struct io_test_batch));
	batch->qd = qd;
	batch->elements = elements;
	return batch;
}

static void io_test_batch_free(struct io_test_batch* batch) {
	// for (int i = 0; i < batch->qd; i++) {
	// 	spdk_free(batch->elements[i].buf);
	// }
	free(batch->elements);
	free(batch);
}

static void
execute_spdk_function(spdk_msg_fn fn, void *arg)
{
	pthread_mutex_lock(&g_test_mutex);
	spdk_thread_send_msg(g_thread_io, fn, arg);
	pthread_cond_wait(&g_test_cond, &g_test_mutex);
	pthread_mutex_unlock(&g_test_mutex);
}

static void
execute_spdk_function_many(spdk_msg_fn fn, void *arg, bool is_final)
{
	if (is_final) {
		pthread_mutex_lock(&g_test_mutex);
		spdk_thread_send_msg(g_thread_io, fn, arg);
		pthread_cond_wait(&g_test_cond, &g_test_mutex);
		pthread_mutex_unlock(&g_test_mutex);
	}
	else {
		spdk_thread_send_msg(g_thread_io, fn, arg);
	}
}

static void
wake_ut_thread(void)
{
	pthread_mutex_lock(&g_test_mutex);
	pthread_cond_signal(&g_test_cond);
	pthread_mutex_unlock(&g_test_mutex);
}

static void
__get_io_channel(void *arg)
{
	struct io_target *target = arg;

	target->ch = spdk_bdev_get_io_channel(target->bdev_desc);
	assert(target->ch);
	wake_ut_thread();
}

static void
bdevio_construct_target_open_cb(enum spdk_bdev_event_type type, struct spdk_bdev *bdev,
				void *event_ctx)
{
}

static int
bdevio_construct_target(struct spdk_bdev *bdev)
{
	struct io_target *target;
	int rc;
	uint64_t num_blocks = spdk_bdev_get_num_blocks(bdev);
	uint32_t block_size = spdk_bdev_get_block_size(bdev);

	target = malloc(sizeof(struct io_target));
	if (target == NULL) {
		return -ENOMEM;
	}

	rc = spdk_bdev_open_ext(spdk_bdev_get_name(bdev), true, bdevio_construct_target_open_cb, NULL,
				&target->bdev_desc);
	if (rc != 0) {
		free(target);
		SPDK_ERRLOG("Could not open leaf bdev %s, error=%d\n", spdk_bdev_get_name(bdev), rc);
		return rc;
	}

	printf("  %s: %" PRIu64 " blocks of %" PRIu32 " bytes (%" PRIu64 " MiB)\n",
	       spdk_bdev_get_name(bdev),
	       num_blocks, block_size,
	       (num_blocks * block_size + 1024 * 1024 - 1) / (1024 * 1024));

	target->bdev = bdev;
	target->next = g_io_targets;
	execute_spdk_function(__get_io_channel, target);
	g_io_targets = target;

	return 0;
}

static int
bdevio_construct_targets(void)
{
	struct spdk_bdev *bdev;
	int rc;

	printf("I/O targets:\n");

	bdev = spdk_bdev_first_leaf();
	while (bdev != NULL) {
		rc = bdevio_construct_target(bdev);
		if (rc < 0) {
			SPDK_ERRLOG("Could not construct bdev %s, error=%d\n", spdk_bdev_get_name(bdev), rc);
			return rc;
		}
		bdev = spdk_bdev_next_leaf(bdev);
	}

	if (g_io_targets == NULL) {
		SPDK_ERRLOG("No bdevs to perform tests on\n");
		return -1;
	}

	return 0;
}

static void
__put_io_channel(void *arg)
{
	struct io_target *target = arg;

	spdk_put_io_channel(target->ch);
	wake_ut_thread();
}

static void
bdevio_cleanup_targets(void)
{
	struct io_target *target;

	target = g_io_targets;
	while (target != NULL) {
		execute_spdk_function(__put_io_channel, target);
		spdk_bdev_close(target->bdev_desc);
		g_io_targets = target->next;
		free(target);
		target = g_io_targets;
	}
}

static bool g_completion_success;

static void
quick_test_complete(struct spdk_bdev_io *bdev_io, bool success, void *arg)
{
	g_completion_success = success;
	spdk_bdev_free_io(bdev_io);
	wake_ut_thread();
}

static void
quick_test_complete_many(struct spdk_bdev_io *bdev_io, bool success, void *arg)
{
	struct io_test_unit* io = arg;
	io->result = success;
	// printf("IO %d %d %d state %d\n", io->write_id, io->offset, io->block_cnt, success);
	g_remaining_io--;
	spdk_bdev_free_io(bdev_io);
	if (g_remaining_io == 0) {
		wake_ut_thread();
	}
}

static uint64_t
bdev_bytes_to_blocks(struct spdk_bdev *bdev, uint64_t bytes)
{
	uint32_t block_size = spdk_bdev_get_block_size(bdev);

	CU_ASSERT(bytes % block_size == 0);
	return bytes / block_size;
}

static void
__blockdev_write(void *arg)
{
	struct bdevio_request *req = arg;
	struct io_target *target = req->target;
	int rc;

	if (req->iovcnt) {
		rc = spdk_bdev_writev(target->bdev_desc, target->ch, req->iov, req->iovcnt, req->offset,
				      req->data_len, quick_test_complete, NULL);
	} else {
		rc = spdk_bdev_write(target->bdev_desc, target->ch, req->buf, req->offset,
				     req->data_len, quick_test_complete, NULL);
	}

	if (rc) {
		g_completion_success = false;
		wake_ut_thread();
	}
}

static void
__blockdev_write_many(void *arg)
{
	struct io_test_unit *io = arg;
	int rc;

	// printf("Submit IO %p %p %d %d %d\n", io, io->buf, io->write_id, io->offset, io->block_cnt);
	rc = spdk_bdev_write_blocks(g_current_io_target->bdev_desc, g_current_io_target->ch, io->buf, io->offset,
					io->block_cnt, quick_test_complete_many, io);

	if (rc) {
		printf("Call bdev write failed with %d\n", rc);
		g_remaining_io--;
		if (g_remaining_io == 0) {
			wake_ut_thread();
		}
	}
}

static void
__blockdev_write_zeroes(void *arg)
{
	struct bdevio_request *req = arg;
	struct io_target *target = req->target;
	int rc;

	rc = spdk_bdev_write_zeroes(target->bdev_desc, target->ch, req->offset,
				    req->data_len, quick_test_complete, NULL);
	if (rc) {
		g_completion_success = false;
		wake_ut_thread();
	}
}

static void
__blockdev_compare_and_write(void *arg)
{
	struct bdevio_request *req = arg;
	struct io_target *target = req->target;
	struct spdk_bdev *bdev = target->bdev;
	int rc;

	rc = spdk_bdev_comparev_and_writev_blocks(target->bdev_desc, target->ch, req->iov, req->iovcnt,
			req->fused_iov, req->fused_iovcnt, bdev_bytes_to_blocks(bdev, req->offset),
			bdev_bytes_to_blocks(bdev, req->data_len), quick_test_complete, NULL);

	if (rc) {
		g_completion_success = false;
		wake_ut_thread();
	}
}

static void
sgl_chop_buffer(struct bdevio_request *req, int iov_len)
{
	int data_len = req->data_len;
	char *buf = req->buf;

	req->iovcnt = 0;
	if (!iov_len) {
		return;
	}

	for (; data_len > 0 && req->iovcnt < BUFFER_IOVS; req->iovcnt++) {
		if (data_len < iov_len) {
			iov_len = data_len;
		}

		req->iov[req->iovcnt].iov_base = buf;
		req->iov[req->iovcnt].iov_len = iov_len;

		buf += iov_len;
		data_len -= iov_len;
	}

	CU_ASSERT_EQUAL_FATAL(data_len, 0);
}

static void
sgl_chop_fused_buffer(struct bdevio_request *req, int iov_len)
{
	int data_len = req->data_len;
	char *buf = req->fused_buf;

	req->fused_iovcnt = 0;
	if (!iov_len) {
		return;
	}

	for (; data_len > 0 && req->fused_iovcnt < BUFFER_IOVS; req->fused_iovcnt++) {
		if (data_len < iov_len) {
			iov_len = data_len;
		}

		req->fused_iov[req->fused_iovcnt].iov_base = buf;
		req->fused_iov[req->fused_iovcnt].iov_len = iov_len;

		buf += iov_len;
		data_len -= iov_len;
	}

	CU_ASSERT_EQUAL_FATAL(data_len, 0);
}

static void
blockdev_write(struct io_target *target, char *tx_buf,
	       uint64_t offset, int data_len, int iov_len)
{
	struct bdevio_request req;

	req.target = target;
	req.buf = tx_buf;
	req.data_len = data_len;
	req.offset = offset;
	sgl_chop_buffer(&req, iov_len);

	g_completion_success = false;

	execute_spdk_function(__blockdev_write, &req);
}

static void
blockdev_write_many(bool is_final, struct io_test_unit* io)
{
	execute_spdk_function_many(__blockdev_write_many, io, is_final);
}

static void
_blockdev_compare_and_write(struct io_target *target, char *cmp_buf, char *write_buf,
			    uint64_t offset, int data_len, int iov_len)
{
	struct bdevio_request req;

	req.target = target;
	req.buf = cmp_buf;
	req.fused_buf = write_buf;
	req.data_len = data_len;
	req.offset = offset;
	sgl_chop_buffer(&req, iov_len);
	sgl_chop_fused_buffer(&req, iov_len);

	g_completion_success = false;

	execute_spdk_function(__blockdev_compare_and_write, &req);
}

static void
blockdev_write_zeroes(struct io_target *target, char *tx_buf,
		      uint64_t offset, int data_len)
{
	struct bdevio_request req;

	req.target = target;
	req.buf = tx_buf;
	req.data_len = data_len;
	req.offset = offset;

	g_completion_success = false;

	execute_spdk_function(__blockdev_write_zeroes, &req);
}

static void
__blockdev_read(void *arg)
{
	struct bdevio_request *req = arg;
	struct io_target *target = req->target;
	int rc;

	if (req->iovcnt) {
		rc = spdk_bdev_readv(target->bdev_desc, target->ch, req->iov, req->iovcnt, req->offset,
				     req->data_len, quick_test_complete, NULL);
	} else {
		rc = spdk_bdev_read(target->bdev_desc, target->ch, req->buf, req->offset,
				    req->data_len, quick_test_complete, NULL);
	}

	if (rc) {
		g_completion_success = false;
		wake_ut_thread();
	}
}

static void
__blockdev_read_many(void *arg)
{
	struct io_test_unit *io = arg;
	int rc;

	// printf("Submit IO %p %d %d %d\n", io, io->write_id, io->offset, io->block_cnt);
	if (io->md_buf) {
		// printf("Read with md\n");
		rc = spdk_bdev_read_blocks_with_md(g_current_io_target->bdev_desc, g_current_io_target->ch, io->buf, io->md_buf, io->offset,
					io->block_cnt, quick_test_complete_many, io);
	}
	else {
		rc = spdk_bdev_read_blocks(g_current_io_target->bdev_desc, g_current_io_target->ch, io->buf, io->offset,
					io->block_cnt, quick_test_complete_many, io);
	}

	if (rc) {
		printf("Call bdev read failed with %d\n", rc);
		g_remaining_io--;
		if (g_remaining_io == 0) {
			wake_ut_thread();
		}
	}
}

static void
blockdev_read(struct io_target *target, char *rx_buf,
	      uint64_t offset, int data_len, int iov_len)
{
	struct bdevio_request req;

	req.target = target;
	req.buf = rx_buf;
	req.data_len = data_len;
	req.offset = offset;
	req.iovcnt = 0;
	sgl_chop_buffer(&req, iov_len);

	g_completion_success = false;

	execute_spdk_function(__blockdev_read, &req);
}

static void
blockdev_read_many(bool is_final, struct io_test_unit* io)
{
	execute_spdk_function_many(__blockdev_read_many, io, is_final);
}

static int
blockdev_write_read_data_match(char *rx_buf, char *tx_buf, int data_length)
{
	int rc;
	rc = memcmp(rx_buf, tx_buf, data_length);

	spdk_free(rx_buf);
	spdk_free(tx_buf);

	return rc;
}

static void
blockdev_write_read(uint32_t data_length, uint32_t iov_len, int pattern, uint64_t offset,
		    int expected_rc, bool write_zeroes)
{
	struct io_target *target;
	char	*tx_buf = NULL;
	char	*rx_buf = NULL;
	int	rc;

	target = g_current_io_target;

	if (!write_zeroes) {
		initialize_buffer(&tx_buf, pattern, data_length);
		initialize_buffer(&rx_buf, 0, data_length);

		blockdev_write(target, tx_buf, offset, data_length, iov_len);
	} else {
		initialize_buffer(&tx_buf, 0, data_length);
		initialize_buffer(&rx_buf, pattern, data_length);

		blockdev_write_zeroes(target, tx_buf, offset, data_length);
	}


	if (expected_rc == 0) {
		CU_ASSERT_EQUAL(g_completion_success, true);
	} else {
		CU_ASSERT_EQUAL(g_completion_success, false);
	}
	blockdev_read(target, rx_buf, offset, data_length, iov_len);

	if (expected_rc == 0) {
		CU_ASSERT_EQUAL(g_completion_success, true);
	} else {
		CU_ASSERT_EQUAL(g_completion_success, false);
	}

	if (g_completion_success) {
		rc = blockdev_write_read_data_match(rx_buf, tx_buf, data_length);
		/* Assert the write by comparing it with values read
		 * from each blockdev */
		CU_ASSERT_EQUAL(rc, 0);
	}
}

static void
blockdev_compare_and_write(uint32_t data_length, uint32_t iov_len, uint64_t offset)
{
	struct io_target *target;
	char	*tx_buf = NULL;
	char	*write_buf = NULL;
	char	*rx_buf = NULL;
	int	rc;

	target = g_current_io_target;

	initialize_buffer(&tx_buf, 0xAA, data_length);
	initialize_buffer(&rx_buf, 0, data_length);
	initialize_buffer(&write_buf, 0xBB, data_length);

	blockdev_write(target, tx_buf, offset, data_length, iov_len);
	CU_ASSERT_EQUAL(g_completion_success, true);

	_blockdev_compare_and_write(target, tx_buf, write_buf, offset, data_length, iov_len);
	CU_ASSERT_EQUAL(g_completion_success, true);

	_blockdev_compare_and_write(target, tx_buf, write_buf, offset, data_length, iov_len);
	CU_ASSERT_EQUAL(g_completion_success, false);

	blockdev_read(target, rx_buf, offset, data_length, iov_len);
	CU_ASSERT_EQUAL(g_completion_success, true);
	rc = blockdev_write_read_data_match(rx_buf, write_buf, data_length);
	/* Assert the write by comparing it with values read
	 * from each blockdev */
	CU_ASSERT_EQUAL(rc, 0);
}

static void
blockdev_write_read_block(void)
{
	uint32_t data_length;
	uint64_t offset;
	int pattern;
	int expected_rc;
	struct io_target *target = g_current_io_target;
	struct spdk_bdev *bdev = target->bdev;

	/* Data size = 1 block */
	data_length = spdk_bdev_get_block_size(bdev);
	CU_ASSERT_TRUE(data_length < BUFFER_SIZE);
	offset = 0;
	pattern = 0xA3;
	/* Params are valid, hence the expected return value
	 * of write and read for all blockdevs is 0. */
	expected_rc = 0;

	blockdev_write_read(data_length, 0, pattern, offset, expected_rc, 0);
}

static void
blockdev_write_zeroes_read_block(void)
{
	uint32_t data_length;
	uint64_t offset;
	int pattern;
	int expected_rc;
	struct io_target *target = g_current_io_target;
	struct spdk_bdev *bdev = target->bdev;

	/* Data size = 1 block */
	data_length = spdk_bdev_get_block_size(bdev);
	offset = 0;
	pattern = 0xA3;
	/* Params are valid, hence the expected return value
	 * of write_zeroes and read for all blockdevs is 0. */
	expected_rc = 0;

	blockdev_write_read(data_length, 0, pattern, offset, expected_rc, 1);
}

/*
 * This i/o will not have to split at the bdev layer.
 */
static void
blockdev_write_zeroes_read_no_split(void)
{
	uint32_t data_length;
	uint64_t offset;
	int pattern;
	int expected_rc;
	struct io_target *target = g_current_io_target;
	struct spdk_bdev *bdev = target->bdev;

	/* Data size = block size aligned ZERO_BUFFER_SIZE */
	data_length = ZERO_BUFFER_SIZE; /* from bdev_internal.h */
	data_length -= ZERO_BUFFER_SIZE % spdk_bdev_get_block_size(bdev);
	offset = 0;
	pattern = 0xA3;
	/* Params are valid, hence the expected return value
	 * of write_zeroes and read for all blockdevs is 0. */
	expected_rc = 0;

	blockdev_write_read(data_length, 0, pattern, offset, expected_rc, 1);
}

/*
 * This i/o will have to split at the bdev layer if
 * write-zeroes is not supported by the bdev.
 */
static void
blockdev_write_zeroes_read_split(void)
{
	uint32_t data_length;
	uint64_t offset;
	int pattern;
	int expected_rc;
	struct io_target *target = g_current_io_target;
	struct spdk_bdev *bdev = target->bdev;

	/* Data size = block size aligned 3 * ZERO_BUFFER_SIZE */
	data_length = 3 * ZERO_BUFFER_SIZE; /* from bdev_internal.h */
	data_length -= data_length % spdk_bdev_get_block_size(bdev);
	offset = 0;
	pattern = 0xA3;
	/* Params are valid, hence the expected return value
	 * of write_zeroes and read for all blockdevs is 0. */
	expected_rc = 0;

	blockdev_write_read(data_length, 0, pattern, offset, expected_rc, 1);
}

/*
 * This i/o will have to split at the bdev layer if
 * write-zeroes is not supported by the bdev. It also
 * tests a write size that is not an even multiple of
 * the bdev layer zero buffer size.
 */
static void
blockdev_write_zeroes_read_split_partial(void)
{
	uint32_t data_length;
	uint64_t offset;
	int pattern;
	int expected_rc;
	struct io_target *target = g_current_io_target;
	struct spdk_bdev *bdev = target->bdev;
	uint32_t block_size = spdk_bdev_get_block_size(bdev);

	/* Data size = block size aligned 7 * ZERO_BUFFER_SIZE / 2 */
	data_length = ZERO_BUFFER_SIZE * 7 / 2;
	data_length -= data_length % block_size;
	offset = 0;
	pattern = 0xA3;
	/* Params are valid, hence the expected return value
	 * of write_zeroes and read for all blockdevs is 0. */
	expected_rc = 0;

	blockdev_write_read(data_length, 0, pattern, offset, expected_rc, 1);
}

static void
blockdev_writev_readv_block(void)
{
	uint32_t data_length, iov_len;
	uint64_t offset;
	int pattern;
	int expected_rc;
	struct io_target *target = g_current_io_target;
	struct spdk_bdev *bdev = target->bdev;

	/* Data size = 1 block */
	data_length = spdk_bdev_get_block_size(bdev);
	iov_len = data_length;
	CU_ASSERT_TRUE(data_length < BUFFER_SIZE);
	offset = 0;
	pattern = 0xA3;
	/* Params are valid, hence the expected return value
	 * of write and read for all blockdevs is 0. */
	expected_rc = 0;

	blockdev_write_read(data_length, iov_len, pattern, offset, expected_rc, 0);
}

static void
blockdev_comparev_and_writev(void)
{
	uint32_t data_length, iov_len;
	uint64_t offset;
	struct io_target *target = g_current_io_target;
	struct spdk_bdev *bdev = target->bdev;

	/* Data size = acwu size */
	data_length = spdk_bdev_get_block_size(bdev) * spdk_bdev_get_acwu(bdev);
	iov_len = data_length;
	CU_ASSERT_TRUE(data_length < BUFFER_SIZE);
	offset = 0;

	blockdev_compare_and_write(data_length, iov_len, offset);
}

static void
blockdev_writev_readv_30x1block(void)
{
	uint32_t data_length, iov_len;
	uint64_t offset;
	int pattern;
	int expected_rc;
	struct io_target *target = g_current_io_target;
	struct spdk_bdev *bdev = target->bdev;
	uint32_t block_size = spdk_bdev_get_block_size(bdev);

	/* Data size = 30 * block size */
	data_length = block_size * 30;
	iov_len = block_size;
	CU_ASSERT_TRUE(data_length < BUFFER_SIZE);
	offset = 0;
	pattern = 0xA3;
	/* Params are valid, hence the expected return value
	 * of write and read for all blockdevs is 0. */
	expected_rc = 0;

	blockdev_write_read(data_length, iov_len, pattern, offset, expected_rc, 0);
}

static void
blockdev_write_read_8blocks(void)
{
	uint32_t data_length;
	uint64_t offset;
	int pattern;
	int expected_rc;
	struct io_target *target = g_current_io_target;
	struct spdk_bdev *bdev = target->bdev;

	/* Data size = 8 * block size */
	data_length = spdk_bdev_get_block_size(bdev) * 8;
	CU_ASSERT_TRUE(data_length < BUFFER_SIZE);
	offset = data_length;
	pattern = 0xA3;
	/* Params are valid, hence the expected return value
	 * of write and read for all blockdevs is 0. */
	expected_rc = 0;

	blockdev_write_read(data_length, 0, pattern, offset, expected_rc, 0);
}

static void
blockdev_writev_readv_8blocks(void)
{
	uint32_t data_length, iov_len;
	uint64_t offset;
	int pattern;
	int expected_rc;
	struct io_target *target = g_current_io_target;
	struct spdk_bdev *bdev = target->bdev;

	/* Data size = 8 * block size */
	data_length = spdk_bdev_get_block_size(bdev) * 8;
	iov_len = data_length;
	CU_ASSERT_TRUE(data_length < BUFFER_SIZE);
	offset = data_length;
	pattern = 0xA3;
	/* Params are valid, hence the expected return value
	 * of write and read for all blockdevs is 0. */
	expected_rc = 0;

	blockdev_write_read(data_length, iov_len, pattern, offset, expected_rc, 0);
}

static void
blockdev_write_read_size_gt_128k(void)
{
	uint32_t data_length;
	uint64_t offset;
	int pattern;
	int expected_rc;
	struct io_target *target = g_current_io_target;
	struct spdk_bdev *bdev = target->bdev;
	uint32_t block_size = spdk_bdev_get_block_size(bdev);

	/* Data size = block size aligned 128K + 1 block */
	data_length = 128 * 1024;
	data_length -= data_length % block_size;
	data_length += block_size;
	CU_ASSERT_TRUE(data_length < BUFFER_SIZE);
	offset = block_size * 2;
	pattern = 0xA3;
	/* Params are valid, hence the expected return value
	 * of write and read for all blockdevs is 0. */
	expected_rc = 0;

	blockdev_write_read(data_length, 0, pattern, offset, expected_rc, 0);
}

static void
blockdev_writev_readv_size_gt_128k(void)
{
	uint32_t data_length, iov_len;
	uint64_t offset;
	int pattern;
	int expected_rc;
	struct io_target *target = g_current_io_target;
	struct spdk_bdev *bdev = target->bdev;
	uint32_t block_size = spdk_bdev_get_block_size(bdev);

	/* Data size = block size aligned 128K + 1 block */
	data_length = 128 * 1024;
	data_length -= data_length % block_size;
	data_length += block_size;
	iov_len = data_length;
	CU_ASSERT_TRUE(data_length < BUFFER_SIZE);
	offset = block_size * 2;
	pattern = 0xA3;
	/* Params are valid, hence the expected return value
	 * of write and read for all blockdevs is 0. */
	expected_rc = 0;

	blockdev_write_read(data_length, iov_len, pattern, offset, expected_rc, 0);
}

static void
blockdev_writev_readv_size_gt_128k_two_iov(void)
{
	uint32_t data_length, iov_len;
	uint64_t offset;
	int pattern;
	int expected_rc;
	struct io_target *target = g_current_io_target;
	struct spdk_bdev *bdev = target->bdev;
	uint32_t block_size = spdk_bdev_get_block_size(bdev);

	/* Data size = block size aligned 128K + 1 block */
	data_length = 128 * 1024;
	data_length -= data_length % block_size;
	iov_len = data_length;
	data_length += block_size;
	CU_ASSERT_TRUE(data_length < BUFFER_SIZE);
	offset = block_size * 2;
	pattern = 0xA3;
	/* Params are valid, hence the expected return value
	 * of write and read for all blockdevs is 0. */
	expected_rc = 0;

	blockdev_write_read(data_length, iov_len, pattern, offset, expected_rc, 0);
}

static void
blockdev_write_read_invalid_size(void)
{
	uint32_t data_length;
	uint64_t offset;
	int pattern;
	int expected_rc;
	struct io_target *target = g_current_io_target;
	struct spdk_bdev *bdev = target->bdev;
	uint32_t block_size = spdk_bdev_get_block_size(bdev);

	/* Data size is not a multiple of the block size */
	data_length = block_size - 1;
	CU_ASSERT_TRUE(data_length < BUFFER_SIZE);
	offset = block_size * 2;
	pattern = 0xA3;
	/* Params are invalid, hence the expected return value
	 * of write and read for all blockdevs is < 0 */
	expected_rc = -1;

	blockdev_write_read(data_length, 0, pattern, offset, expected_rc, 0);
}

static void
blockdev_write_read_offset_plus_nbytes_equals_bdev_size(void)
{
	struct io_target *target;
	struct spdk_bdev *bdev;
	char	*tx_buf = NULL;
	char	*rx_buf = NULL;
	uint64_t offset;
	uint32_t block_size;
	int rc;

	target = g_current_io_target;
	bdev = target->bdev;

	block_size = spdk_bdev_get_block_size(bdev);

	/* The start offset has been set to a marginal value
	 * such that offset + nbytes == Total size of
	 * blockdev. */
	offset = ((spdk_bdev_get_num_blocks(bdev) - 1) * block_size);

	initialize_buffer(&tx_buf, 0xA3, block_size);
	initialize_buffer(&rx_buf, 0, block_size);

	blockdev_write(target, tx_buf, offset, block_size, 0);
	CU_ASSERT_EQUAL(g_completion_success, true);

	blockdev_read(target, rx_buf, offset, block_size, 0);
	CU_ASSERT_EQUAL(g_completion_success, true);

	rc = blockdev_write_read_data_match(rx_buf, tx_buf, block_size);
	/* Assert the write by comparing it with values read
	 * from each blockdev */
	CU_ASSERT_EQUAL(rc, 0);
}

static void
blockdev_write_read_offset_plus_nbytes_gt_bdev_size(void)
{
	struct io_target *target = g_current_io_target;
	struct spdk_bdev *bdev = target->bdev;
	char	*tx_buf = NULL;
	char	*rx_buf = NULL;
	int	data_length;
	uint64_t offset;
	int pattern;
	uint32_t block_size = spdk_bdev_get_block_size(bdev);

	/* Tests the overflow condition of the blockdevs. */
	data_length = block_size * 2;
	CU_ASSERT_TRUE(data_length < BUFFER_SIZE);
	pattern = 0xA3;

	target = g_current_io_target;
	bdev = target->bdev;

	/* The start offset has been set to a valid value
	 * but offset + nbytes is greater than the Total size
	 * of the blockdev. The test should fail. */
	offset = (spdk_bdev_get_num_blocks(bdev) - 1) * block_size;

	initialize_buffer(&tx_buf, pattern, data_length);
	initialize_buffer(&rx_buf, 0, data_length);

	blockdev_write(target, tx_buf, offset, data_length, 0);
	CU_ASSERT_EQUAL(g_completion_success, false);

	blockdev_read(target, rx_buf, offset, data_length, 0);
	CU_ASSERT_EQUAL(g_completion_success, false);
}

static void
blockdev_write_read_max_offset(void)
{
	int	data_length;
	uint64_t offset;
	int pattern;
	int expected_rc;
	struct io_target *target = g_current_io_target;
	struct spdk_bdev *bdev = target->bdev;

	data_length = spdk_bdev_get_block_size(bdev);
	CU_ASSERT_TRUE(data_length < BUFFER_SIZE);
	/* The start offset has been set to UINT64_MAX such that
	 * adding nbytes wraps around and points to an invalid address. */
	offset = UINT64_MAX;
	pattern = 0xA3;
	/* Params are invalid, hence the expected return value
	 * of write and read for all blockdevs is < 0 */
	expected_rc = -1;

	blockdev_write_read(data_length, 0, pattern, offset, expected_rc, 0);
}

static void
blockdev_overlapped_write_read_2blocks(void)
{
	int	data_length;
	uint64_t offset;
	int pattern;
	int expected_rc;
	struct io_target *target = g_current_io_target;
	struct spdk_bdev *bdev = target->bdev;

	/* Data size = 2 blocks */
	data_length = spdk_bdev_get_block_size(bdev) * 2;
	CU_ASSERT_TRUE(data_length < BUFFER_SIZE);
	offset = 0;
	pattern = 0xA3;
	/* Params are valid, hence the expected return value
	 * of write and read for all blockdevs is 0. */
	expected_rc = 0;
	/* Assert the write by comparing it with values read
	 * from the same offset for each blockdev */
	blockdev_write_read(data_length, 0, pattern, offset, expected_rc, 0);

	/* Overwrite the pattern 0xbb of size 2*block size on an address offset
	 * overlapping with the address written above and assert the new value in
	 * the overlapped address range */
	/* Populate 2*block size with value 0xBB */
	pattern = 0xBB;
	/* Offset = 1 block; Overlap offset addresses and write value 0xbb */
	offset = spdk_bdev_get_block_size(bdev);
	/* Assert the write by comparing it with values read
	 * from the overlapped offset for each blockdev */
	blockdev_write_read(data_length, 0, pattern, offset, expected_rc, 0);
}

static void
__blockdev_reset(void *arg)
{
	struct bdevio_request *req = arg;
	struct io_target *target = req->target;
	int rc;

	rc = spdk_bdev_reset(target->bdev_desc, target->ch, quick_test_complete, NULL);
	if (rc < 0) {
		g_completion_success = false;
		wake_ut_thread();
	}
}

// Diagnostics Only!
static void
__blockdev_flush(void *arg)
{
	struct bdevio_request *req = arg;
	struct io_target *target = req->target;
	int rc;

	// offset and length not relevant for diagnostics 
	rc = spdk_bdev_flush_blocks(target->bdev_desc, target->ch, 0, 1, quick_test_complete, NULL);
	if (rc < 0) {
		printf("Call bdev flush failed with %d\n", rc);
		g_completion_success = false;
		wake_ut_thread();
	}
}

// Diagnostics Only!
static void
blockdev_flush(void)
{
	struct bdevio_request req;
	struct io_target *target;
	bool flush_supported;

	target = g_current_io_target;
	req.target = target;

	flush_supported = spdk_bdev_io_type_supported(target->bdev, SPDK_BDEV_IO_TYPE_FLUSH);
	CU_ASSERT_EQUAL(true, flush_supported);

	execute_spdk_function(__blockdev_flush, &req);
}

static void
blockdev_test_reset(void)
{
	struct bdevio_request req;
	struct io_target *target;
	bool reset_supported;

	target = g_current_io_target;
	req.target = target;

	reset_supported = spdk_bdev_io_type_supported(target->bdev, SPDK_BDEV_IO_TYPE_RESET);
	g_completion_success = false;

	execute_spdk_function(__blockdev_reset, &req);

	CU_ASSERT_EQUAL(g_completion_success, reset_supported);
}

struct bdevio_passthrough_request {
	struct spdk_nvme_cmd cmd;
	void *buf;
	uint32_t len;
	struct io_target *target;
	int sct;
	int sc;
	uint32_t cdw0;
};

static void
nvme_pt_test_complete(struct spdk_bdev_io *bdev_io, bool success, void *arg)
{
	struct bdevio_passthrough_request *pt_req = arg;

	spdk_bdev_io_get_nvme_status(bdev_io, &pt_req->cdw0, &pt_req->sct, &pt_req->sc);
	spdk_bdev_free_io(bdev_io);
	wake_ut_thread();
}

static void
__blockdev_nvme_passthru(void *arg)
{
	struct bdevio_passthrough_request *pt_req = arg;
	struct io_target *target = pt_req->target;
	int rc;

	rc = spdk_bdev_nvme_io_passthru(target->bdev_desc, target->ch,
					&pt_req->cmd, pt_req->buf, pt_req->len,
					nvme_pt_test_complete, pt_req);
	if (rc) {
		wake_ut_thread();
	}
}

static void
blockdev_test_nvme_passthru_rw(void)
{
	struct bdevio_passthrough_request pt_req;
	void *write_buf, *read_buf;
	struct io_target *target;

	target = g_current_io_target;

	if (!spdk_bdev_io_type_supported(target->bdev, SPDK_BDEV_IO_TYPE_NVME_IO)) {
		return;
	}

	memset(&pt_req, 0, sizeof(pt_req));
	pt_req.target = target;
	pt_req.cmd.opc = SPDK_NVME_OPC_WRITE;
	pt_req.cmd.nsid = 1;
	*(uint64_t *)&pt_req.cmd.cdw10 = 4;
	pt_req.cmd.cdw12 = 0;

	pt_req.len = spdk_bdev_get_block_size(target->bdev);
	write_buf = spdk_malloc(pt_req.len, 0, NULL, SPDK_ENV_LCORE_ID_ANY, SPDK_MALLOC_DMA);
	memset(write_buf, 0xA5, pt_req.len);
	pt_req.buf = write_buf;

	pt_req.sct = SPDK_NVME_SCT_VENDOR_SPECIFIC;
	pt_req.sc = SPDK_NVME_SC_INVALID_FIELD;
	execute_spdk_function(__blockdev_nvme_passthru, &pt_req);
	CU_ASSERT(pt_req.sct == SPDK_NVME_SCT_GENERIC);
	CU_ASSERT(pt_req.sc == SPDK_NVME_SC_SUCCESS);

	pt_req.cmd.opc = SPDK_NVME_OPC_READ;
	read_buf = spdk_zmalloc(pt_req.len, 0, NULL, SPDK_ENV_LCORE_ID_ANY, SPDK_MALLOC_DMA);
	pt_req.buf = read_buf;

	pt_req.sct = SPDK_NVME_SCT_VENDOR_SPECIFIC;
	pt_req.sc = SPDK_NVME_SC_INVALID_FIELD;
	execute_spdk_function(__blockdev_nvme_passthru, &pt_req);
	CU_ASSERT(pt_req.sct == SPDK_NVME_SCT_GENERIC);
	CU_ASSERT(pt_req.sc == SPDK_NVME_SC_SUCCESS);

	CU_ASSERT(!memcmp(read_buf, write_buf, pt_req.len));
	spdk_free(read_buf);
	spdk_free(write_buf);
}

static void
blockdev_test_nvme_passthru_vendor_specific(void)
{
	struct bdevio_passthrough_request pt_req;
	struct io_target *target;

	target = g_current_io_target;

	if (!spdk_bdev_io_type_supported(target->bdev, SPDK_BDEV_IO_TYPE_NVME_IO)) {
		return;
	}

	memset(&pt_req, 0, sizeof(pt_req));
	pt_req.target = target;
	pt_req.cmd.opc = 0x7F; /* choose known invalid opcode */
	pt_req.cmd.nsid = 1;

	pt_req.sct = SPDK_NVME_SCT_VENDOR_SPECIFIC;
	pt_req.sc = SPDK_NVME_SC_SUCCESS;
	pt_req.cdw0 = 0xbeef;
	execute_spdk_function(__blockdev_nvme_passthru, &pt_req);
	CU_ASSERT(pt_req.sct == SPDK_NVME_SCT_GENERIC);
	CU_ASSERT(pt_req.sc == SPDK_NVME_SC_INVALID_OPCODE);
	CU_ASSERT(pt_req.cdw0 == 0x0);
}

static void
__blockdev_nvme_admin_passthru(void *arg)
{
	struct bdevio_passthrough_request *pt_req = arg;
	struct io_target *target = pt_req->target;
	int rc;

	rc = spdk_bdev_nvme_admin_passthru(target->bdev_desc, target->ch,
					   &pt_req->cmd, pt_req->buf, pt_req->len,
					   nvme_pt_test_complete, pt_req);
	if (rc) {
		wake_ut_thread();
	}
}

static void
blockdev_test_nvme_admin_passthru(void)
{
	struct io_target *target;
	struct bdevio_passthrough_request pt_req;

	target = g_current_io_target;

	if (!spdk_bdev_io_type_supported(target->bdev, SPDK_BDEV_IO_TYPE_NVME_ADMIN)) {
		return;
	}

	memset(&pt_req, 0, sizeof(pt_req));
	pt_req.target = target;
	pt_req.cmd.opc = SPDK_NVME_OPC_IDENTIFY;
	pt_req.cmd.nsid = 0;
	*(uint64_t *)&pt_req.cmd.cdw10 = SPDK_NVME_IDENTIFY_CTRLR;

	pt_req.len = sizeof(struct spdk_nvme_ctrlr_data);
	pt_req.buf = spdk_malloc(pt_req.len, 0, NULL, SPDK_ENV_LCORE_ID_ANY, SPDK_MALLOC_DMA);

	pt_req.sct = SPDK_NVME_SCT_GENERIC;
	pt_req.sc = SPDK_NVME_SC_SUCCESS;
	execute_spdk_function(__blockdev_nvme_admin_passthru, &pt_req);
	CU_ASSERT(pt_req.sct == SPDK_NVME_SCT_GENERIC);
	CU_ASSERT(pt_req.sc == SPDK_NVME_SC_SUCCESS);
}

static bool has_overlap(int l1, int r1, int l2, int r2) {
	return l1 <= r2 || l2 <= r1;
}

static bool validate_block_is_consistent(uint64_t* ptr) {
	for (unsigned i = 1; i < DEFAULT_BLOCK_SIZE / sizeof(uint64_t); i++) {
		if (ptr[i] != ptr[i - 1]) {
			printf("Block inconsistent\n");
			for (unsigned j = 0; j < DEFAULT_BLOCK_CNT / sizeof(uint64_t); j++) {
				printf("%ld,", ptr[j]);
			}
			printf("\n");
			return false;
		}
	}
	return true;
}

static void 
blockdev_test_long_running(void)
{
	struct io_target* target = g_current_io_target;
	unsigned long total_io_cnt = 0;
	unsigned long total_write_io_cnt = 0;
	unsigned long total_failed_write_io_cnt = 0;
	unsigned long total_failed_read_io_cnt = 0;
	uint64_t bdev_block_cnt = spdk_bdev_get_num_blocks(target->bdev);
	uint64_t bdev_block_size = spdk_bdev_get_block_size(target->bdev);
	enum io_failure_reason failure_reason = IO_FAILURE_NO_FAILURE;
	void* buf = spdk_zmalloc(MAX_QD * DEFAULT_BLOCK_SIZE * g_block_cnt_arr[SPDK_COUNTOF(g_block_cnt_arr) - 1],
		0x1000,
		NULL,
		SPDK_ENV_LCORE_ID_ANY,
		SPDK_MALLOC_DMA);
	struct diagnostic_read_md* read_md = spdk_zmalloc(DEFAULT_BLOCK_SIZE,
		0x1000,
		NULL,
		SPDK_ENV_LCORE_ID_ANY,
		SPDK_MALLOC_DMA);

	if (bdev_block_size != DEFAULT_BLOCK_SIZE) {
		// TODO: support more block size
		printf("Long running task only support 512B\n");
		return;
	}

	if (bdev_block_cnt < DEFAULT_BLOCK_CNT) {
		printf("Long running target should have at least 1GB\n");
		return;
	}

	// One Int for each block to save space
	// against 512B * (1024 * 1024 * 2)
	bdev_block_cnt = DEFAULT_BLOCK_CNT;

	void* init_buf = NULL;
	int init_block_cnt = g_block_cnt_arr[SPDK_COUNTOF(g_block_cnt_arr) - 1];
	initialize_buffer(&init_buf, 0, init_block_cnt * DEFAULT_BLOCK_SIZE);

	for (int i = 0;
		i < DEFAULT_BLOCK_CNT / init_block_cnt;
		i++) {
		blockdev_write(target,
			init_buf,
			i * init_block_cnt * DEFAULT_BLOCK_SIZE,
			init_block_cnt * DEFAULT_BLOCK_SIZE,
			0);
	}

	uint64_t start_ticks = spdk_get_ticks();
	uint64_t end_ticks = start_ticks + g_long_running_seconds * spdk_get_ticks_hz();
	int q;
	uint64_t failed_offset = 0;

	srand(g_seed);
	bool ok = true;
	while (true) {
		if (spdk_get_ticks() >= end_ticks) {
			break;
		}

		if (!ok) {
			break;
		}

		if (g_remaining_io != 0) {
			printf("ERROR: test code bug\n");
			ok = false;
			goto end;
		}

		int qd = 1 + (rand() % MAX_QD);
		total_io_cnt += qd;
		g_remaining_io = qd;
		// no metadata for test IOs
		struct io_test_batch* batch = io_test_batch_alloc(qd, buf, NULL, 0.5);

		for (q = 0; q < qd; q++) {
			bool is_final = q == (qd - 1);
			if (batch->elements[q].is_write) {
				// printf("Submitting write %d %p\n", batch->elements[q].write_id, &batch->elements[q]);
				// TODO: need to consider write fail
				total_write_io_cnt++;
				blockdev_write_many(
					is_final,
					&batch->elements[q]);
			}
			else {
				blockdev_read_many(
					is_final,
					&batch->elements[q]);
			}
		}

		for (q = 0; q < qd; q++) {
			struct io_test_unit* io = &batch->elements[q];
			// no validation for the write io
			// write io only changes the g_last_written_id array
			if (!io->is_write) {
				if (io->result) {
					for (int i = 0; i < io->block_cnt; i++) {
						// 1. validate block integrity
						uint64_t* cur_buf = io->buf + i * DEFAULT_BLOCK_SIZE;

						if (!validate_block_is_consistent(cur_buf)) {
							ok = false;
							failure_reason = IO_FAILURE_BLOCK_INCONSISTENT;
							goto end;
						}

						// 2. should not read outdated data
						uint64_t cur_id = cur_buf[0];
						int recall_pos = (g_last_written_id_pos[io->offset + i] + LAST_WRITTEN_ID_RECALL_CNT - 1) % LAST_WRITTEN_ID_RECALL_CNT;
						uint64_t last_written_id = g_last_written_id[io->offset + i][recall_pos];
						if (cur_id < last_written_id) {
							ok = false;
							failed_offset = io->offset + i;
							printf("Outdated IO: %ld, %ld, %ld, %ld, %d, %d\n",
								cur_id,
								last_written_id,
								total_io_cnt,
								total_write_io_cnt,
								io->offset,
								io->block_cnt);
							
							printf("Last %d written ids: \n", LAST_WRITTEN_ID_RECALL_CNT);
							for (int k = 0; k < LAST_WRITTEN_ID_RECALL_CNT; k++) {
								printf("%ld, ", g_last_written_id[io->offset + i][k]);
							}
							printf("\n");

							failure_reason = IO_FAILURE_OUTDATED;
							goto end;
						}

						// 3. the id should be:
						// a) last written id of the previous batches
						// b) the id of any write io in the batch; if the write io fails,
						// then set it to be successful.
						if (cur_id != last_written_id) {
							bool found = false;
							for (int j = 0; j < qd; j++) {
								if (cur_id == batch->elements[j].write_id) {
									found = true;
									batch->elements[j].result = true;
									break;
								}
							}
							if (!found) {
								ok = false;
								failed_offset = io->offset + i;
								failure_reason = IO_FAILURE_UNWRITTEN;
								goto end;
							}

							// g_last_written_id[io->offset + i] = cur_id;
						}
					}
				}
				else {
					printf("Read IO should not fail in test env\n");
					// if failed, then the range must not be readable.
					// it can happen if:
					// 1. previous write fail. note that the write may be read by later
					// read requests successfully.
					// 2. the region is not readable before the batch. Since the read and 
					// write thread are separated, the read may still fail when a previous 
					// write in the batch succeeded.
					total_failed_read_io_cnt++;
					bool expected = false;
					for (int i = 0; i < io->block_cnt; i++) {
						if (g_last_written_id[io->offset + i] == 0) {
							expected = true;
							break;
						}
					}

					for (q = 0; q < qd; q++) {
						if (batch->elements[q].is_write
							&& !batch->elements[q].result
							&& has_overlap(io->offset,
								io->offset + io->block_cnt,
								batch->elements[q].offset,
								batch->elements[q].offset + batch->elements[q].block_cnt)) {
							expected = true;
							break;
						}
					}

					if (!expected) {
						ok = false;
						failure_reason = IO_FAILURE_UNEXPECTED_READ_FAIL;
						goto end;
					}
				}
			}
			else if (!io->result) {
				total_failed_write_io_cnt++;
			}
		}

		// update g_last_written_id for each write
		for (q = qd - 1; q >= 0; q--) {
			struct io_test_unit* io = &batch->elements[q];
			if (io->is_write) {
				if (io->result) {
					for (int i = 0; i < io->block_cnt; i++) {
						int new_pos = g_last_written_id_pos[io->offset + i];
						int recall_pos = (g_last_written_id_pos[io->offset + i] + LAST_WRITTEN_ID_RECALL_CNT - 1) % LAST_WRITTEN_ID_RECALL_CNT;
						g_last_written_id[io->offset + i][new_pos] = 
							spdk_max(g_last_written_id[io->offset + i][recall_pos], io->write_id);
						g_last_written_id_pos[io->offset + i] = (new_pos + 1) % LAST_WRITTEN_ID_RECALL_CNT;
					}
				}
				else {
					printf("Write IO %ld failed\n", io->write_id);
					// check if there is newer successful write io.
					// if there is not, then the block is not readable.
					for (int i = 0; i < io->block_cnt; i++) {
						int new_pos = g_last_written_id_pos[io->offset + i];
						int recall_pos = (g_last_written_id_pos[io->offset + i] + LAST_WRITTEN_ID_RECALL_CNT - 1) % LAST_WRITTEN_ID_RECALL_CNT;
						if (g_last_written_id[io->offset + i][recall_pos] < io->write_id) {
							g_last_written_id[io->offset + i][new_pos] = 0;
						}
						g_last_written_id_pos[io->offset + i] = (new_pos + 1) % LAST_WRITTEN_ID_RECALL_CNT;
					}
				}
			}
		}

		io_test_batch_free(batch);
	}


end:
	if (!ok) {
		printf("Failure reason is %d\n", failure_reason);
		printf("#IO: %ld, #Write IO: %ld, #Failed write IO: %ld, #Failed read IO: %ld\n", 
			total_io_cnt,
			total_write_io_cnt,
			total_failed_write_io_cnt,
			total_failed_read_io_cnt);
		
		printf("Entering diagnostic mode\n");
		blockdev_flush();

		for (int target = 0; target < 4; target++) {
			read_md->force_read_from_disk = false;
			read_md->target_id = target;
			struct io_test_unit* io_unit = io_test_unit_alloc(NULL,
				buf,
				read_md,
				false,
				0,
				1,
				failed_offset);
			g_remaining_io = 1;
			blockdev_read_many(true, io_unit);

			printf("Result from target %d: %ld\n", target, ((uint64_t*)buf)[0]);
			validate_block_is_consistent(buf);
		}
	}

	spdk_free(buf);
	CU_ASSERT(ok);
	return;
}

static void
__stop_init_thread(void *arg)
{
	unsigned num_failures = g_num_failures;
	struct spdk_jsonrpc_request *request = arg;

	g_num_failures = 0;

	bdevio_cleanup_targets();
	if (g_wait_for_tests && !g_shutdown) {
		/* Do not stop the app yet, wait for another RPC */
		rpc_perform_tests_cb(num_failures, request);
		return;
	}
	spdk_app_stop(num_failures);
}

static void
stop_init_thread(unsigned num_failures, struct spdk_jsonrpc_request *request)
{
	g_num_failures = num_failures;

	spdk_thread_send_msg(g_thread_init, __stop_init_thread, request);
}

static int
suite_init(void)
{
	if (g_current_io_target == NULL) {
		g_current_io_target = g_io_targets;
	}
	return 0;
}

static int
suite_fini(void)
{
	g_current_io_target = g_current_io_target->next;
	return 0;
}

#define SUITE_NAME_MAX 64

static int
__setup_ut_on_single_target(struct io_target *target)
{
	unsigned rc = 0;
	CU_pSuite suite = NULL;
	char name[SUITE_NAME_MAX];

	snprintf(name, sizeof(name), "bdevio tests on: %s", spdk_bdev_get_name(target->bdev));
	suite = CU_add_suite(name, suite_init, suite_fini);
	if (suite == NULL) {
		CU_cleanup_registry();
		rc = CU_get_error();
		return -rc;
	}

	if (
		CU_add_test(suite, "blockdev write read block",
			    blockdev_write_read_block) == NULL
		|| CU_add_test(suite, "blockdev write zeroes read block",
			       blockdev_write_zeroes_read_block) == NULL
		|| CU_add_test(suite, "blockdev write zeroes read no split",
			       blockdev_write_zeroes_read_no_split) == NULL
		|| CU_add_test(suite, "blockdev write zeroes read split",
			       blockdev_write_zeroes_read_split) == NULL
		|| CU_add_test(suite, "blockdev write zeroes read split partial",
			       blockdev_write_zeroes_read_split_partial) == NULL
		|| CU_add_test(suite, "blockdev reset",
			       blockdev_test_reset) == NULL
		|| CU_add_test(suite, "blockdev write read 8 blocks",
			       blockdev_write_read_8blocks) == NULL
		|| CU_add_test(suite, "blockdev write read size > 128k",
			       blockdev_write_read_size_gt_128k) == NULL
		|| CU_add_test(suite, "blockdev write read invalid size",
			       blockdev_write_read_invalid_size) == NULL
		|| CU_add_test(suite, "blockdev write read offset + nbytes == size of blockdev",
			       blockdev_write_read_offset_plus_nbytes_equals_bdev_size) == NULL
		|| CU_add_test(suite, "blockdev write read offset + nbytes > size of blockdev",
			       blockdev_write_read_offset_plus_nbytes_gt_bdev_size) == NULL
		|| CU_add_test(suite, "blockdev write read max offset",
			       blockdev_write_read_max_offset) == NULL
		|| CU_add_test(suite, "blockdev write read 2 blocks on overlapped address offset",
			       blockdev_overlapped_write_read_2blocks) == NULL
		|| CU_add_test(suite, "blockdev writev readv 8 blocks",
			       blockdev_writev_readv_8blocks) == NULL
		|| CU_add_test(suite, "blockdev writev readv 30 x 1block",
			       blockdev_writev_readv_30x1block) == NULL
		|| CU_add_test(suite, "blockdev writev readv block",
			       blockdev_writev_readv_block) == NULL
		|| CU_add_test(suite, "blockdev writev readv size > 128k",
			       blockdev_writev_readv_size_gt_128k) == NULL
		|| CU_add_test(suite, "blockdev writev readv size > 128k in two iovs",
			       blockdev_writev_readv_size_gt_128k_two_iov) == NULL
		|| CU_add_test(suite, "blockdev comparev and writev",
			       blockdev_comparev_and_writev) == NULL
		|| CU_add_test(suite, "blockdev nvme passthru rw",
			       blockdev_test_nvme_passthru_rw) == NULL
		|| CU_add_test(suite, "blockdev nvme passthru vendor specific",
			       blockdev_test_nvme_passthru_vendor_specific) == NULL
		|| CU_add_test(suite, "blockdev nvme admin passthru",
			       blockdev_test_nvme_admin_passthru) == NULL
	) {
		CU_cleanup_registry();
		rc = CU_get_error();
		return -rc;
	}

	if (g_long_running_seconds > 0) {
		// TODO: add long running tests
		if (CU_add_test(suite, "blockdev long running test", blockdev_test_long_running) == NULL) {
			CU_cleanup_registry();
			rc = CU_get_error();
			return -rc;
		}
	}

	return 0;
}

static void
__run_ut_thread(void *arg)
{
	struct spdk_jsonrpc_request *request = arg;
	int rc = 0;
	struct io_target *target;
	unsigned num_failures;

	if (CU_initialize_registry() != CUE_SUCCESS) {
		/* CUnit error, probably won't recover */
		rc = CU_get_error();
		stop_init_thread(-rc, request);
	}

	target = g_io_targets;
	while (target != NULL) {
		rc = __setup_ut_on_single_target(target);
		if (rc < 0) {
			/* CUnit error, probably won't recover */
			stop_init_thread(-rc, request);
		}
		target = target->next;
	}
	CU_basic_set_mode(CU_BRM_VERBOSE);
	CU_basic_run_tests();
	num_failures = CU_get_number_of_failures();
	CU_cleanup_registry();

	stop_init_thread(num_failures, request);
}

static void
__construct_targets(void *arg)
{
	if (bdevio_construct_targets() < 0) {
		spdk_app_stop(-1);
		return;
	}

	// wait for one node to go down
	if (g_node_failure) {
		sleep(30);
	}

	spdk_thread_send_msg(g_thread_ut, __run_ut_thread, NULL);
}

static void
test_main(void *arg1)
{
	struct spdk_cpuset tmpmask = {};
	uint32_t i;

	pthread_mutex_init(&g_test_mutex, NULL);
	pthread_cond_init(&g_test_cond, NULL);

	/* This test runs specifically on at least three cores.
	 * g_thread_init is the app_thread on main core from event framework.
	 * Next two are only for the tests and should always be on separate CPU cores. */
	if (spdk_env_get_core_count() < 3) {
		spdk_app_stop(-1);
		return;
	}

	SPDK_ENV_FOREACH_CORE(i) {
		if (i == spdk_env_get_current_core()) {
			g_thread_init = spdk_get_thread();
			continue;
		}
		spdk_cpuset_zero(&tmpmask);
		spdk_cpuset_set_cpu(&tmpmask, i, true);
		if (g_thread_ut == NULL) {
			g_thread_ut = spdk_thread_create("ut_thread", &tmpmask);
		} else if (g_thread_io == NULL) {
			g_thread_io = spdk_thread_create("io_thread", &tmpmask);
		}

	}

	if (g_wait_for_tests) {
		/* Do not perform any tests until RPC is received */
		return;
	}

	spdk_thread_send_msg(g_thread_init, __construct_targets, NULL);
}

static void
bdevio_usage(void)
{
	printf(" -w                        start bdevio app and wait for RPC to start the tests\n");
	printf(" -N                        wait for 30s to kill a node before starting the tests\n");
}

static int
bdevio_parse_arg(int ch, char *arg)
{
	switch (ch) {
	case 'w':
		g_wait_for_tests =  true;
		break;
	case 'N':
		g_node_failure = true;
		break;
	case 't':
		g_long_running_seconds = spdk_strtoll(optarg, 10);
		break;
	default:
		return -EINVAL;
	}
	return 0;
}

struct rpc_perform_tests {
	char *name;
};

static void
free_rpc_perform_tests(struct rpc_perform_tests *r)
{
	free(r->name);
}

static const struct spdk_json_object_decoder rpc_perform_tests_decoders[] = {
	{"name", offsetof(struct rpc_perform_tests, name), spdk_json_decode_string, true},
};

static void
rpc_perform_tests_cb(unsigned num_failures, struct spdk_jsonrpc_request *request)
{
	struct spdk_json_write_ctx *w;

	if (num_failures == 0) {
		w = spdk_jsonrpc_begin_result(request);
		spdk_json_write_uint32(w, num_failures);
		spdk_jsonrpc_end_result(request, w);
	} else {
		spdk_jsonrpc_send_error_response_fmt(request, SPDK_JSONRPC_ERROR_INTERNAL_ERROR,
						     "%d test cases failed", num_failures);
	}
}

static void
rpc_perform_tests(struct spdk_jsonrpc_request *request, const struct spdk_json_val *params)
{
	struct rpc_perform_tests req = {NULL};
	struct spdk_bdev *bdev;
	int rc;

	if (params && spdk_json_decode_object(params, rpc_perform_tests_decoders,
					      SPDK_COUNTOF(rpc_perform_tests_decoders),
					      &req)) {
		SPDK_ERRLOG("spdk_json_decode_object failed\n");
		spdk_jsonrpc_send_error_response(request, SPDK_JSONRPC_ERROR_INVALID_PARAMS, "Invalid parameters");
		goto invalid;
	}

	if (req.name) {
		bdev = spdk_bdev_get_by_name(req.name);
		if (bdev == NULL) {
			SPDK_ERRLOG("Bdev '%s' does not exist\n", req.name);
			spdk_jsonrpc_send_error_response_fmt(request, SPDK_JSONRPC_ERROR_INTERNAL_ERROR,
							     "Bdev '%s' does not exist: %s",
							     req.name, spdk_strerror(ENODEV));
			goto invalid;
		}
		rc = bdevio_construct_target(bdev);
		if (rc < 0) {
			SPDK_ERRLOG("Could not construct target for bdev '%s'\n", spdk_bdev_get_name(bdev));
			spdk_jsonrpc_send_error_response_fmt(request, SPDK_JSONRPC_ERROR_INTERNAL_ERROR,
							     "Could not construct target for bdev '%s': %s",
							     spdk_bdev_get_name(bdev), spdk_strerror(-rc));
			goto invalid;
		}
	} else {
		rc = bdevio_construct_targets();
		if (rc < 0) {
			SPDK_ERRLOG("Could not construct targets for all bdevs\n");
			spdk_jsonrpc_send_error_response_fmt(request, SPDK_JSONRPC_ERROR_INTERNAL_ERROR,
							     "Could not construct targets for all bdevs: %s",
							     spdk_strerror(-rc));
			goto invalid;
		}
	}
	free_rpc_perform_tests(&req);

	spdk_thread_send_msg(g_thread_ut, __run_ut_thread, request);

	return;

invalid:
	free_rpc_perform_tests(&req);
}
SPDK_RPC_REGISTER("perform_tests", rpc_perform_tests, SPDK_RPC_RUNTIME)

static void
spdk_bdevio_shutdown_cb(void)
{
	g_shutdown = true;
	spdk_thread_send_msg(g_thread_init, __stop_init_thread, NULL);
}

int
main(int argc, char **argv)
{
	int			rc;
	struct spdk_app_opts	opts = {};

	spdk_app_opts_init(&opts, sizeof(opts));
	opts.name = "bdevio";
	opts.reactor_mask = "0x7";
	opts.shutdown_cb = spdk_bdevio_shutdown_cb;

	if ((rc = spdk_app_parse_args(argc, argv, &opts, "wNt:", NULL,
				      bdevio_parse_arg, bdevio_usage)) !=
	    SPDK_APP_PARSE_ARGS_SUCCESS) {
		return rc;
	}

	rc = spdk_app_start(&opts, test_main, NULL);
	spdk_app_fini();

	return rc;
}
