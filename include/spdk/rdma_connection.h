#ifndef SPDK_RDMA_CONNECTION_H_
#define SPDK_RDMA_CONNECTION_H_

#include "spdk/stdinc.h"
#include "spdk/log.h"

#include <infiniband/verbs.h>
#include <rdma/rdma_cma.h>

#ifdef __cplusplus
extern "C" {
#endif

#define RDMA_MAX_MR 64


enum rdma_handshake_status {
	RDMA_HANDSHAKE_UNINITIALIZED,
	RDMA_HANDSHAKE_INITIALIZED,
};

struct rdma_connection;

typedef void (*rdma_connection_connected_cb)(void *cb_ctx, struct rdma_connection* rdma_conn);

struct destage_info {
	uint64_t offset;
	uint64_t round;
	uint32_t checksum;
};

struct rdma_handshake {
	// prevent data node from reading empty/corrupted data.
	enum rdma_handshake_status status;
	// enough for IPv4
	// char address[16];
	// 6 is long enough
	// char port[8];
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

enum rdma_status {
	RDMA_SERVER_INITIALIZED,
	RDMA_SERVER_LISTENING,
	RDMA_SERVER_ACCEPTED,
	RDMA_SERVER_ESTABLISHED,
	RDMA_SERVER_CONNECTED,
	RDMA_SERVER_ERROR,
	RDMA_SERVER_DISCONNECTED,

	RDMA_CLI_INITIALIZED,
	RDMA_CLI_ADDR_RESOLVING,
	RDMA_CLI_ROUTE_RESOLVING,
	RDMA_CLI_CONNECTING,
	RDMA_CLI_ESTABLISHED,
	RDMA_CLI_CONNECTED,
	RDMA_CLI_ERROR,
	RDMA_CLI_DISCONNECTED,
};

struct rdma_connection {
	pthread_rwlock_t lock;
	// one node acts as recover server,
	// the other one acts as the client.
	// both should support reconnection.
	bool is_server;
	struct rdma_event_channel* channel;
	// if it is server, then it is local address
	// otherwise it is the server address
	struct addrinfo* server_addr;
	struct rdma_cm_id* cm_id;
	struct rdma_cm_id* parent_cm_id;
	int mr_cnt;
	struct ibv_mr* mr_arr[RDMA_MAX_MR];
	struct spdk_mem_map* mem_map;
	struct ibv_cq* cq;
	struct rdma_handshake* handshake_buf;
	volatile enum rdma_status status;
	int rdma_context_length;
	void* rdma_context;
	rdma_connection_connected_cb connected_cb;
	int reject_cnt;
	bool handshake_sent;
	bool handshake_received;
};


struct rdma_connection* rdma_connection_alloc(
	bool is_server,
	const char* ip,
	const char* port,
	int context_length,
	void* base_addr,
	uint64_t block_size,
	uint64_t block_cnt,
	rdma_connection_connected_cb connected_cb);

int rdma_connection_connect(struct rdma_connection* rdma_conn);
void rdma_connection_free(struct rdma_connection* rdma_conn);
int rdma_connection_register(struct rdma_connection* rdma_conn, void* addr, uint32_t len);
void rdma_connection_construct_sge(struct rdma_connection* rdma_conn, struct ibv_sge* sge, void* addr, uint32_t len);
uint32_t rdma_connection_get_lkey(struct rdma_connection* rdma_conn, void* addr, uint32_t len);
uint32_t rdma_connection_get_rkey(struct rdma_connection* rdma_conn, void* addr, uint32_t len);
bool rdma_connection_is_connected(struct rdma_connection* rdma_conn);

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

#endif /* SPDK_RDMA_CONNECTION_H_ */
