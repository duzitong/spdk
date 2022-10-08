#ifndef SPDK_RDMA_H_
#define SPDK_RDMA_H_

#include "spdk/stdinc.h"

#ifdef __cplusplus
extern "C" {
#endif

enum rdma_handshake_status {
	RDMA_HANDSHAKE_UNINITIALIZED,
	RDMA_HANDSHAKE_INITIALIZED,
};

struct rdma_handshake {
	// prevent data node from reading empty/corrupted data.
	enum rdma_handshake_status status;
	// enough for IPv4
	char address[16];
	// 6 is long enough
	char port[8];
	// for client, the address is the head of handshake buffer, which data node will read to setup connection between data nodes.
	// for server, the address is the head of its PMEM buffer.
	void* base_addr;
	uint32_t rkey;
	// client tells server about the block size and count
	uint64_t block_size;
	uint64_t block_cnt;
};

#ifdef __cplusplus
}
#endif

#endif /* SPDK_RDMA_H_ */
