#ifndef SPDK_BSL_INTERNAL_H
#define SPDK_BSL_INTERNAL_H

#define BSKIPLIST_MAXLEVEL 32 /* Should be enough for 2^64 elements */

#define BSKIPLIST_P 0.3      /* Skiplist P = 0.3 */

typedef enum {
    LOCATION_TYPE_BDEV,

    LOCATION_TYPE_MEM
} locationType;

typedef struct bstat {
    long begin, end;
    locationType type;
    u_int64_t round;
    uint64_t bdevOffset;
    void *memPointer;
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

bstat *bstatBdevCreate(long begin, long end, u_int64_t round, long bdevOffset);
bstat *bstatMemCreate(long begin, long end, u_int64_t round, void* data);
bskiplist *bslCreate(void);
bskiplistFreeNodes *bslfnCreate(void);
void bslPrint(bskiplist *bsl, char full);
void bslfnPrint(bskiplistFreeNodes *bslfn);
void bslfnFree(bskiplistFreeNodes *bslfn);
bskiplistNode *bslInsert(bskiplist *bsl, long begin, long end, bstat *ele, bskiplistFreeNodes *bslfn);
bskiplistNode *bslFirstNodeAfterBegin(bskiplist *bsl, long begin);

#endif /* SPDK_BSL_INTERNAL_H */
