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

struct destage_info {
	uint64_t offset;
	uint64_t round;
	uint32_t checksum;
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
	// the reconnect counter that client believes
	// if data node receives a number that is greater than itself, 
	// then it should do fully recovery
	// TODO: add more fields to support half-recovery
	uint64_t reconnect_cnt;
	struct destage_info destage_tail;
};

// bool destage_info_gt(struct destage_info* lhs, struct destage_info* rhs);
// bool destage_info_geq(struct destage_info* lhs, struct destage_info* rhs);
// bool destage_info_lt(struct destage_info* lhs, struct destage_info* rhs);
// bool destage_info_leq(struct destage_info* lhs, struct destage_info* rhs);

static inline bool destage_info_gt(struct destage_info* lhs, struct destage_info* rhs) {
	return (lhs->round > rhs->round) || (lhs->offset > rhs->offset && lhs->round == rhs->round);
}

static inline bool destage_info_geq(struct destage_info* lhs, struct destage_info* rhs) {
	return destage_info_gt(lhs, rhs) || (lhs->round == rhs->round && lhs->offset == rhs->offset);
}

static inline bool destage_info_lt(struct destage_info* lhs, struct destage_info* rhs) {
	return !destage_info_geq(lhs, rhs);
}

static inline bool destage_info_leq(struct destage_info* lhs, struct destage_info* rhs) {
	return !destage_info_gt(lhs, rhs);
}

#ifdef __cplusplus
}
#endif

#endif /* SPDK_RDMA_H_ */
