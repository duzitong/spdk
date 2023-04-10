#include "bsl.h"
#include "spdk/stdinc.h"
#include "spdk/env.h"

bstat *bstatClone(bstat *pb, struct spdk_mempool *pool);
int bslRandomLevel(void);
bskiplistNode *bslCreateNode(int level, long begin, long end, bstat *ele, bskiplist *bsl);
void bslFreeNode(bskiplistNode *bsln, struct spdk_mempool *bstat_pool, struct spdk_mempool *node_pool);
void bslAdjustNodeBegin(bskiplistNode *bn, long end);
void bslAdjustNodeEnd(bskiplistNode *bn, long begin);


struct b_node_event_track_list g_track_list;

void bsl_enable_track(bskiplist* bsl) {
    bsl->enable_track = true;
}

void bsl_record_event(bskiplistNode* node, b_node_event_type event_type) {
    struct b_node_event_track* cur_event_track = &g_track_list.event_track[g_track_list.cur_idx];
    cur_event_track->begin = node->begin;
    cur_event_track->end = node->end;
    // ele is NULL for header node
    if (node->ele) {
        cur_event_track->ele_begin = node->ele->begin;
        cur_event_track->ele_end = node->ele->end;
        cur_event_track->round = node->ele->round;
        cur_event_track->mem_offset = node->ele->l.bdevOffset;
    }
    cur_event_track->event_type = event_type;
    g_track_list.cur_idx = (g_track_list.cur_idx + 1) % BSKIPLIST_TRACK_LEN;
}

void bsl_print_events(void) {
    printf("Last %d events: \n", BSKIPLIST_TRACK_LEN);
    int i = g_track_list.cur_idx;
    do {
        struct b_node_event_track* cur_event_track = &g_track_list.event_track[i];
        if (cur_event_track->event_type == B_NODE_NONE) {
            break;
        }

        printf("Type: %d, Node: [%ld, %ld], Ele: [%ld, %ld, %ld, %ld]\n",
            cur_event_track->event_type,
            cur_event_track->begin,
            cur_event_track->end,
            cur_event_track->ele_begin,
            cur_event_track->ele_end,
            cur_event_track->mem_offset,
            cur_event_track->round
            );
        
        i = (i + 1) % BSKIPLIST_TRACK_LEN;
    } while (i != g_track_list.cur_idx);
}

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
    clone->round = pb->round;
    clone->failed = pb->failed;
    clone->failed_target_id = pb->failed_target_id;
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
bskiplistNode *bslCreateNode(int level, long begin, long end, bstat *ele, bskiplist *bsl) {
    bskiplistNode *bn = spdk_mempool_get(bsl->node_pool);
    bn->begin = begin;
    bn->end = end;
    bn->ele = ele;
    bn->height = level;

    if (bsl->enable_track) {
        bsl_record_event(bn, B_NODE_CREATE);
    }
    return bn;
}

/* Create a new skiplist. */
bskiplist *bslCreate(struct spdk_mempool *node_pool, struct spdk_mempool *bstat_pool) {
    int j;
    bskiplist *bsl;

    bsl = calloc(1, sizeof(*bsl));
    bsl->level = 1;
    bsl->node_pool = node_pool;
    bsl->bstat_pool = bstat_pool;
    bsl->header = bslCreateNode(BSKIPLIST_MAXLEVEL,-1, -1, NULL, bsl);
    bsl->enable_track = false;
    for (j = 0; j < BSKIPLIST_MAXLEVEL; j++) {
        bsl->header->level[j].forward = NULL;
    }
    return bsl;
}

bskiplistFreeNodes *bslfnCreate(struct spdk_mempool *node_pool, struct spdk_mempool *bstat_pool, bskiplist* bsl) {
    bskiplistFreeNodes *bslfn;
    bslfn = calloc(1, sizeof(*bslfn));
    bslfn->node_pool = node_pool;
    bslfn->bstat_pool = bstat_pool;
    bslfn->header = bslfn->tail = bslCreateNode(BSKIPLIST_MAXLEVEL,-1, -1, NULL, bsl);
    bslfn->header->level[0].forward = NULL;
    return bslfn;
}

void bslPrint(bskiplist *bsl, bool full) {
    bskiplistNode *x = bsl->header->level[0].forward;
    int i;
    char buf[128];
    
    // if (full) {
    //     printf("Level 0: \n");
    //     while (x) {
    //         bslPrintNode(x, buf, 128);
    //         printf("%s\n", buf);
    //         x = x->level[0].forward;
    //     }
    //     printf("\n");
    // }

    for (i = 0; i < bsl->level; i++) {
        printf("Level %2d:\n", i);
        x = bsl->header;
        while (x) {
            bslPrintNode(x, buf, 128);
            printf("%s\n", buf);
            x = x->level[i].forward;
        }
        printf("\n\n");
    }
}

void bslPrintNode(bskiplistNode *bsln, char* buf, size_t size) {
    if (bsln) {
        if (bsln->ele) {
            snprintf(buf,
                size,
                "%02d: [%ld, %ld] -> [%ld, %ld, %ld, %ld]: [%d, %d]",
                bsln->height,
                bsln->begin,
                bsln->end,
                bsln->ele->begin,
                bsln->ele->end,
                bsln->ele->l.bdevOffset,
                bsln->ele->round,
                bsln->ele->failed,
                bsln->ele->failed_target_id);
        }
        else {
            snprintf(buf,
                size,
                "%02d: [%ld, %ld] -> NULL",
                bsln->height,
                bsln->begin,
                bsln->end);
        }
    } else {
        snprintf(buf, size, "NULL bsl node");
    }
}

void bslFreeNode(bskiplistNode *bsln, struct spdk_mempool *bstat_pool, struct spdk_mempool *node_pool) {
    if (bsln->ele->type == LOCATION_MEM)
        spdk_free(bsln->ele->l.memPointer);
    spdk_mempool_put(bstat_pool, bsln->ele);
    spdk_mempool_put(node_pool, bsln);
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
        bslFreeNode(x, bslfn->bstat_pool, bslfn->node_pool);
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

    x = bslCreateNode(level, begin, end, ele, bsl);

    if (updatee[0]->level[0].forward == updateb[0]) {
        // in updateb[0] scope, split to 3 nodes
        y = bslCreateNode(updateb[0]->height, end + 1, updateb[0]->end, bstatClone(updateb[0]->ele, bsl->bstat_pool), bsl);
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
        if (bsl->enable_track) {
            for (y = updateb[0]->level[0].forward; y != updatee[0]->level[0].forward; y = y->level[0].forward) {
                bsl_record_event(y, B_NODE_FREE);
            }
        }
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

        // for free nodes list
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
