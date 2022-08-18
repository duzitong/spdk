#ifndef SPDK_RDMA_H_
#define SPDK_RDMA_H_

#include "spdk/stdinc.h"

#ifdef __cplusplus
extern "C" {
#endif

struct rdma_handshake {
	void* base_addr;
	uint32_t rkey;
	uint64_t block_size;
	uint64_t block_cnt;
};

#ifdef __cplusplus
}
#endif

#endif /* SPDK_RDMA_H_ */
