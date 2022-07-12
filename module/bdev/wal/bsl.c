#include "bsl.h"
#include "spdk/stdinc.h"
#include "spdk/env.h"

bstat *bstatClone(bstat *pb, struct spdk_mempool *pool);
int bslRandomLevel(void);
bskiplistNode *bslCreateNode(int level, long begin, long end, bstat *ele, struct spdk_mempool *pool);
void bslPrintNode(bskiplistNode *bsln);
void bslFreeNode(bskiplistNode *bsln);
void bslAdjustNodeBegin(bskiplistNode *bn, long end);
void bslAdjustNodeEnd(bskiplistNode *bn, long begin);


bstat *bstatBdevCreate(long begin, long end, long round, long unsigned int bdevOffset, struct spdk_mempool *pool) {
    bstat *pb = spdk_mempool_get(pool);
    pb->begin = begin;
    pb->end = end;
    pb->round = round;
    pb->type = LOCATION_BDEV;
    pb->l.bdevOffset = bdevOffset;
    return pb;
}

bstat *bstatMemCreate(long begin, long end, long round, void *memPointer, struct spdk_mempool *pool) {
    bstat *pb = spdk_mempool_get(pool);
    pb->begin = begin;
    pb->end = end;
    pb->round = round;
    pb->type = LOCATION_MEM;
    pb->l.memPointer = memPointer;
    return pb;
}

bstat *bstatClone(bstat *pb, struct spdk_mempool *pool) {
    bstat *clone = spdk_mempool_get(pool);
    clone->begin = pb->begin;
    clone->end = pb->end;
    clone->type = pb->type;
    clone->l = pb->l;
    return clone;
}

/* Returns a random level for the new skiplist node we are going to create.
 * The return value of this function is between 1 and BSKIPLIST_MAXLEVEL
 * (both inclusive), with a powerlaw-alike distribution where higher
 * levels are less likely to be returned. */
int bslRandomLevel(void) {
    static const int threshold = BSKIPLIST_P*RAND_MAX;
    int level = 1;
    while (random() < threshold)
        level += 1;
    return (level<BSKIPLIST_MAXLEVEL) ? level : BSKIPLIST_MAXLEVEL;
}

/* Create a skiplist node with the specified number of levels. */
bskiplistNode *bslCreateNode(int level, long begin, long end, bstat *ele, struct spdk_mempool *pool) {
    bskiplistNode *bn = spdk_mempool_get(pool);
    bn->begin = begin;
    bn->end = end;
    bn->ele = ele;
    bn->height = level;
    return bn;
}

/* Create a new skiplist. */
bskiplist *bslCreate(struct spdk_mempool *node_pool, struct spdk_mempool *bstat_pool) {
    int j;
    bskiplist *bsl;

    bsl = calloc(1, sizeof(*bsl));
    bsl->level = 1;
    bsl->node_pool = node_pool;
    bsl->header = bslCreateNode(BSKIPLIST_MAXLEVEL,-1, -1, NULL, node_pool);
    for (j = 0; j < BSKIPLIST_MAXLEVEL; j++) {
        bsl->header->level[j].forward = NULL;
    }
    return bsl;
}

bskiplistFreeNodes *bslfnCreate(struct spdk_mempool *pool) {
    bskiplistFreeNodes *bslfn;
    bslfn = calloc(1, sizeof(*bslfn));
    bslfn->pool = pool;
    bslfn->header = bslfn->tail = bslCreateNode(BSKIPLIST_MAXLEVEL,-1, -1, NULL, pool);
    bslfn->header->level[0].forward = NULL;
    return bslfn;
}

void bslPrint(bskiplist *bsl, char full) {
    bskiplistNode *x = bsl->header->level[0].forward;
    int i;
    
    if (full) {
        while (x) {
            printf("[%ld, %ld]->[%ld, %ld] ", x->begin, x->end, x->ele->begin, x->ele->end);
            x = x->level[0].forward;
        }
        printf("\n");
    }

    for (i = 0; i < bsl->level; i++) {
        printf("%2d: ", i+1);
        x = bsl->header;
        while (x) {
            printf("[%ld, %ld] ", x->begin, x->end);
            x = x->level[i].forward;
        }
        printf("\n");
    }
}

void bslPrintNode(bskiplistNode *bsln) {
    if (bsln) {
        printf("%02d: [%ld, %ld]\n", bsln->height, bsln->begin, bsln->end);
    } else {
        printf("NULL bsl node\n");
    }
}

void bslFreeNode(bskiplistNode *bsln) {
    if (bsln->ele->type == LOCATION_MEM)
        free(bsln->ele->l.memPointer);
    free(bsln->ele);
    free(bsln);
}

void bslfnPrint(bskiplistFreeNodes *bslfn) {
    bskiplistNode *x;

    printf("Free nodes: ");
    x = bslfn->header->level[0].forward;
    while (x) {
        printf("[%ld, %ld]", x->begin, x->end);
        x = x->level[0].forward;
    }
    printf("\n");
}

int bslfnFree(bskiplistFreeNodes *bslfn, int max) {
    bskiplistNode *x;
    int i = 0;

    x = bslfn->header->level[0].forward;
    while (x && i < max && x != bslfn->tail) {
        bslfn->header->level[0].forward = x->level[0].forward;
        bslFreeNode(x);
        x = bslfn->header->level[0].forward;
        i++;
    }
    return i;
}

void bslAdjustNodeBegin(bskiplistNode *bn, long end) {
    if (bn && bn->begin <= end) {
        bn->begin = end + 1;
    }
}

void bslAdjustNodeEnd(bskiplistNode *bn, long begin) {
    if (bn && bn->end >= begin) {
        bn->end = begin - 1;
    }
}

/* Insert a new node in the skiplist. */
bskiplistNode *bslInsert(bskiplist *bsl, long begin, long end, bstat *ele, bskiplistFreeNodes *bslfn) {
    bskiplistNode *updateb[BSKIPLIST_MAXLEVEL], *updatee[BSKIPLIST_MAXLEVEL], *x, *y;
    int i, level;

    x = bsl->header;
    for (i = bsl->level-1; i >= 0; i--) {
        while (x->level[i].forward &&
                (x->level[i].forward->begin < begin))
        {
            x = x->level[i].forward;
        }
        updateb[i] = x;
    }

    x = bsl->header;
    for (i = bsl->level-1; i >= 0; i--) {
        while (x->level[i].forward &&
                (x->level[i].forward->end <= end))
        {
            x = x->level[i].forward;
        }
        updatee[i] = x;
    }

    level = bslRandomLevel();
    if (level > bsl->level) {
        for (i = bsl->level; i < level; i++) {
            updateb[i] = bsl->header;
            updatee[i] = bsl->header;
        }
        bsl->level = level;
    }

    x = bslCreateNode(level, begin, end, ele, bsl->node_pool);

    if (updatee[0]->level[0].forward == updateb[0]) {
        // in updateb[0] scope, split to 3 nodes
        y = bslCreateNode(updateb[0]->height, end + 1, updateb[0]->end, bstatClone(updateb[0]->ele, bsl->bstat_pool), bsl->node_pool);
        updateb[0]->end = begin - 1;

        if (level < updateb[0]->height) {
            for (i = 0; i < level; i++) {
                y->level[i].forward = updateb[i]->level[i].forward;
                updateb[i]->level[i].forward = x;
                x->level[i].forward = y;
            }
            for (i = level; i < updateb[0]->height; i++) {
                y->level[i].forward = updateb[i]->level[i].forward;
                updateb[i]->level[i].forward = y;
            }
        } else {
            for (i = 0; i < updateb[0]->height; i++) {
                y->level[i].forward = updateb[i]->level[i].forward;
                updateb[i]->level[i].forward = x;
                x->level[i].forward = y;
            }
            for (i = updateb[0]->height; i < level; i++) {
                x->level[i].forward = updateb[i]->level[i].forward;
                updateb[i]->level[i].forward = x;
            }
        }
    } else if (updatee[0] == updateb[0]) {
        // joint scope, add node; adjust ub & ue.forward
        bslAdjustNodeEnd(updateb[0], begin);
        bslAdjustNodeBegin(updatee[0]->level[0].forward, end);

        for (i = 0; i < level; i++) {
            x->level[i].forward = updatee[i]->level[i].forward;
            updateb[i]->level[i].forward = x;
        }
    } else {
        // outer ub.forward scope; remove nodes [ub.forward, ue]; adjust ub & ue.forward
        bslfn->tail->level[0].forward = updateb[0]->level[0].forward;
        bslfn->tail = updatee[0];

        bslAdjustNodeEnd(updateb[0], begin);
        bslAdjustNodeBegin(updatee[0]->level[0].forward, end);

        // can be improved if there's a backward pointer
        y = updateb[0]->level[0].forward;
        while (y != updatee[0]->level[0].forward) {
            for (i = level; i < y->height; i++) {
                updateb[i]->level[i].forward = y->level[i].forward;
                y->level[i].forward = NULL;
            }
            y = y->level[0].forward;
        }

        for (i = 0; i < level; i++) {
            x->level[i].forward = updatee[i]->level[i].forward;
            updateb[i]->level[i].forward = x;
        }

        updatee[0]->level[0].forward = NULL;
    }

    return x;
}

bskiplistNode *bslFirstNodeAfterBegin(bskiplist *bsl, long begin) {
    bskiplistNode *x;
    int i;

    x = bsl->header;
    for (i = bsl->level-1; i >= 0; i--) {
        while (x->level[i].forward &&
                (x->level[i].forward->end < begin))
        {
            x = x->level[i].forward;
        }
    }

    return x->level[0].forward;
}
