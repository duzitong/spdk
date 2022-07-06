#ifndef SPDK_BSL_INTERNAL_H
#define SPDK_BSL_INTERNAL_H

#include "spdk/stdinc.h"
#include "spdk/env.h"

#define BSKIPLIST_MAXLEVEL 32 /* Should be enough for 2^64 elements */

#define BSKIPLIST_P 0.3      /* Skiplist P = 0.3 */

typedef enum {
    BDEV,

    MEM
} locationType;

typedef struct bstat {
    long begin, end;
    locationType type;
    union {
        long bdevOffset;
        void *memPointer;
    } location;
} bstat;

typedef struct bskiplistNode {
    bstat *ele;
    // Blocks location in core device
    long begin, end;
    int height;
    struct bskiplistLevel {
        struct bskiplistNode *forward;
    } level[];
} bskiplistNode;

typedef struct bskiplist {
    struct bskiplistNode *header;
    int level;
} bskiplist;

typedef struct bskiplistFreeNodes {
    struct bskiplistNode *header, *tail;
} bskiplistFreeNodes;

bstat *bstatCreate(long begin, long end);
bskiplist *bslCreate(void);
bskiplistFreeNodes *bslfnCreate(void);
void bslPrint(bskiplist *bsl, char full);
void bslfnPrint(bskiplistFreeNodes *bslfn);
void bslfnFree(bskiplistFreeNodes *bslfn);
bskiplistNode *bslInsert(bskiplist *bsl, long begin, long end, bstat *ele, bskiplistFreeNodes *bslfn);
bskiplistNode *bslFirstNodeAfterBegin(bskiplist *bsl, long begin)

#endif /* SPDK_BSL_INTERNAL_H */
