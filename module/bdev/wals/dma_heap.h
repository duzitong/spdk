/*
 * A simple pre-allocated dma heap (not thread safe).
 * It's required to alloc different dma heap on different cores.
 * 
 * Memory is seperated into several buffer regions.
 * | page_512 | page_4k | page_8k | page_32k | page_64k | page_128k | page_512k | page_1024k |
 * 
 * Queues are used for each region to record the remaining buffers.
 */

#ifndef SPDK_DMA_HEAP_INTERNAL_H
#define SPDK_DMA_HEAP_INTERNAL_H

#include "spdk/env.h"

#define SIZE_SHIFT          3

#define SIZE_512            512
#define SIZE_512_SHIFT      9
#define SIZE_4K             4096
#define SIZE_4K_SHIFT       12
#define SIZE_8K             8192
#define SIZE_8K_SHIFT       13
#define SIZE_32K            32768
#define SIZE_32K_SHIFT      15
#define SIZE_64K            65536
#define SIZE_64K_SHIFT      16
#define SIZE_128K           131072
#define SIZE_128K_SHIFT     17
#define SIZE_1M             1048576
#define SIZE_1M_SHIFT       20
#define SIZE_4M             4194304
#define SIZE_4M_SHIFT       22

struct dma_page {
    void *buf;

    size_t data_size;

    size_t buf_size;

    TAILQ_ENTRY(dma_page) link;
};

struct dma_heap {
    void *buf;
    size_t buf_size;
    size_t data_size;
    size_t md_size;

    TAILQ_HEAD(, dma_page) page_512;
    TAILQ_HEAD(, dma_page) page_4k;
    TAILQ_HEAD(, dma_page) page_8k;
    TAILQ_HEAD(, dma_page) page_32k;
    TAILQ_HEAD(, dma_page) page_64k;
    TAILQ_HEAD(, dma_page) page_128k;
    TAILQ_HEAD(, dma_page) page_1m;
    TAILQ_HEAD(, dma_page) page_4m;
};

struct dma_heap* dma_heap_alloc(size_t data_size, size_t md_size, int checksum_size_512, int align_shift);
void* dma_heap_get_buf(struct dma_heap *heap);
size_t dma_heap_get_buf_size(struct dma_heap *heap);

struct dma_page* dma_heap_get_page(struct dma_heap *heap, size_t data_size);
void dma_heap_put_page(struct dma_heap *heap, struct dma_page *page);
void* dma_page_get_buf(struct dma_page *page);

#endif /* SPDK_DMA_HEAP_INTERNAL_H */