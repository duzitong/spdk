#ifndef SPDK_BSL_INTERNAL_H
#define SPDK_BSL_INTERNAL_H

#include "spdk/env.h"

#define BSKIPLIST_MAXLEVEL 28 /* make bskiplist node size to 256 */

#define BSKIPLIST_P 0.3      /* Skiplist P = 0.3 */

typedef enum {
    LOCATION_BDEV,

    LOCATION_MEM
} locationType;

typedef struct bstat {
    long begin, end;
    locationType type;
    long unsigned int round;
    bool failed;
    union location {
        long unsigned int bdevOffset;
        void *memPointer;
    } l;
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
} bskiplist;

typedef struct bskiplistFreeNodes {
    struct bskiplistNode *header, *tail;
    struct spdk_mempool *node_pool;
    struct spdk_mempool *bstat_pool;
} bskiplistFreeNodes;

bstat *bstatBdevCreate(long begin, long end, long round, long unsigned int bdevOffset, struct spdk_mempool *pool);
bstat *bstatMemCreate(long begin, long end, long round, void *memPointer, struct spdk_mempool *pool);
bskiplist *bslCreate(struct spdk_mempool *node_pool, struct spdk_mempool *bstat_pool);
bskiplistFreeNodes *bslfnCreate(struct spdk_mempool *pool, struct spdk_mempool *bstat_pool);
void bslPrint(bskiplist *bsl, char full);
void bslfnPrint(bskiplistFreeNodes *bslfn);
int bslfnFree(bskiplistFreeNodes *bslfn, int max);
bskiplistNode *bslInsert(bskiplist *bsl, long begin, long end, bstat *ele, bskiplistFreeNodes *bslfn);
bskiplistNode *bslFirstNodeAfterBegin(bskiplist *bsl, long begin);

#endif /* SPDK_BSL_INTERNAL_H */
