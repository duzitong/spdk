#ifndef SPDK_RDMA_ATOMIC_H_
#define SPDK_RDMA_ATOMIC_H_

#include "spdk/stdinc.h"
#include "spdk/rdma_connection.h"

#ifdef __cplusplus
extern "C" {
#endif

struct spdk_poller* rcu_poller;

// struct for RCU (Read-Copy-Update) in remote environment.
struct rdma_atomic_rcu {
    // one act as sender, another act as receiver.
    bool is_sender;
    // the unit size of the element. Simply use sizeof(...) to get it.
    size_t size;
    // the circular buffer for RDMA send/receive. Once RDMA receive completes, 
    // the element needs to be copied to local_buf so that the buffer can be reused.
    void* rdma_buf;
    // the circular buffer for local atomic operations.
    void* local_buf;
    // The head is where the reader is reading from; the tail is where the writer is 
    // writing to. Head should never be equal to tail. If head is not one step behind 
    // tail, then advance head before read. Similarly, tail can never reach head.
    // Only one thread is allowed to call read/write.
    int head, tail;
    struct ibv_mr* mr;
};

struct rdma_atomic_rcu*
rdma_atomic_rcu_create(size_t size, bool is_sender);

void rdma_atomic_rcu_register(struct rdma_atomic_rcu* rcu, struct ibv_mr* mr);

void* rdma_atomic_rcu_read();

int rdma_atomic_rcu_write(void* ptr);

#ifdef __cplusplus
}
#endif

#endif /* SPDK_RDMA_ATOMIC_H_ */
