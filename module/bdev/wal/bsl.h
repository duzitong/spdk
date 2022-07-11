#ifndef SPDK_BSL_INTERNAL_H
#define SPDK_BSL_INTERNAL_H

#define BSKIPLIST_MAXLEVEL 32 /* Should be enough for 2^64 elements */

#define BSKIPLIST_P 0.3      /* Skiplist P = 0.3 */

typedef enum {
    LOCATION_BDEV,

    LOCATION_MEM
} locationType;

typedef struct bstat {
    long begin, end;
    locationType type;
    long unsigned int round;
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
    } level[];
} bskiplistNode;

typedef struct bskiplist {
    struct bskiplistNode *header;
    int level;
} bskiplist;

typedef struct bskiplistFreeNodes {
    struct bskiplistNode *header, *tail;
} bskiplistFreeNodes;

bstat *bstatBdevCreate(long begin, long end, long round, long unsigned int bdevOffset);
bstat *bstatMemCreate(long begin, long end, long round, void *memPointer);
bskiplist *bslCreate(void);
bskiplistFreeNodes *bslfnCreate(void);
void bslPrint(bskiplist *bsl, char full);
void bslfnPrint(bskiplistFreeNodes *bslfn);
void bslfnFree(bskiplistFreeNodes *bslfn, int max);
bskiplistNode *bslInsert(bskiplist *bsl, long begin, long end, bstat *ele, bskiplistFreeNodes *bslfn);
bskiplistNode *bslFirstNodeAfterBegin(bskiplist *bsl, long begin);
bskiplistNode *bslGetRandomNode(bskiplist *bsl, unsigned long int mod);

#endif /* SPDK_BSL_INTERNAL_H */
