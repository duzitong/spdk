/*   SPDX-License-Identifier: BSD-3-Clause
 *   Copyright (c) Intel Corporation.
 *   All rights reserved.
 */

#include "spdk/stdinc.h"

#include "spdk/rdma_atomic.h"
#include "spdk/bit_pool.h"
#include "spdk/env.h"

#include "spdk/likely.h"
#include "spdk/util.h"

struct rdma_atomic_rcu*
rdma_atomic_rcu_create(size_t size, bool is_sender) {
    struct rdma_atomic_rcu* rcu = calloc(1, sizeof(struct rdma_atomic_rcu));
    rcu->size = size;
    rcu->is_sender = is_sender;
}