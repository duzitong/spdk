#ifndef SPDK_BSL_INTERNAL_H
#define SPDK_BSL_INTERNAL_H

#include "spdk/env.h"

#define BSKIPLIST_MAXLEVEL 28 /* make bskiplist node size to 256 */

#define BSKIPLIST_P 0.3      /* Skiplist P = 0.3 */
#define BSKIPLIST_TRACK_LEN 1000

typedef enum {
    LOCATION_BDEV,

    LOCATION_MEM
} locationType;

typedef enum {
    // NONE as first to avoid empty entries
    B_NODE_NONE,
    B_NODE_CREATE,
    B_NODE_INSERT,
    B_NODE_ADJUST_BEGIN,
    B_NODE_ADJUST_END,
    B_NODE_FREE,
} b_node_event_type;

typedef struct b_node_event_track {
    b_node_event_type event_type;
    long begin, end;
    long ele_begin, ele_end, round;
    long unsigned int mem_offset;
} b_node_event_track;

typedef struct b_node_event_track_list {
    int cur_idx;
    struct b_node_event_track event_track[BSKIPLIST_TRACK_LEN];
} b_node_event_track_list;

typedef struct bstat {
    // can be larger intervals than the one in bskiplistNode due to node split.
    long begin, end;
    locationType type;
    long unsigned int round;
    bool failed;
    union location {
        long unsigned int bdevOffset;
        void *memPointer;
    } l;  // data offset
    long unsigned int mdOffset;
    int failed_target_id;
} bstat;

typedef struct bskiplistNode {
    bstat *ele;
    // Blocks location in core device
    long begin, end;
    int height;
    struct bskiplistLevel {
        struct bskiplistNode *forward;
    } level[BSKIPLIST_MAXLEVEL];
} bskiplistNode;

typedef struct bskiplist {
    struct bskiplistNode *header;
    int level;
    struct spdk_mempool *node_pool;
    struct spdk_mempool *bstat_pool;
    bool enable_track;
} bskiplist;

typedef struct bskiplistFreeNodes {
    struct bskiplistNode *header, *tail;
    struct spdk_mempool *node_pool;
    struct spdk_mempool *bstat_pool;
} bskiplistFreeNodes;

bstat *bstatBdevCreate(long begin, long end, long round, long unsigned int bdevOffset, struct spdk_mempool *pool);
bstat *bstatMemCreate(long begin, long end, long round, void *memPointer, struct spdk_mempool *pool);
bskiplist *bslCreate(struct spdk_mempool *node_pool, struct spdk_mempool *bstat_pool);
void bsl_enable_track(bskiplist* bsl);
bskiplistFreeNodes *bslfnCreate(struct spdk_mempool *pool, struct spdk_mempool *bstat_pool, bskiplist* bsl);
void bslPrint(bskiplist *bsl, bool full);
void bslPrintNode(bskiplistNode *bn, char* buf, size_t size);
void bslfnPrint(bskiplistFreeNodes *bslfn);
int bslfnFree(bskiplistFreeNodes *bslfn, int max);
bskiplistNode *bslInsert(bskiplist *bsl, long begin, long end, bstat *ele, bskiplistFreeNodes *bslfn);
bskiplistNode *bslFirstNodeAfterBegin(bskiplist *bsl, long begin);
void bsl_record_event(bskiplistNode* node, b_node_event_type event_type);
void bsl_print_events(void);

#endif /* SPDK_BSL_INTERNAL_H */
