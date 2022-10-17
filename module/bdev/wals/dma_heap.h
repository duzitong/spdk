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
#define SIZE_4K             4096
#define SIZE_8K             8192
#define SIZE_32K            32768
#define SIZE_64K            65536
#define SIZE_128K           131072
#define SIZE_512K           524288
#define SIZE_1024K          1048576

struct dma_page {
    void *buf;

    size_t data_size;

    size_t md_size;

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
    TAILQ_HEAD(, dma_page) page_512k;
    TAILQ_HEAD(, dma_page) page_1024k;
};

struct dma_heap* dma_heap_alloc(size_t data_size, size_t md_size, size_t align);
void* dma_heap_get_buf(struct dma_heap *heap);
size_t dma_heap_get_buf_size(struct dma_heap *heap);

struct dma_page* dma_heap_get_page(struct dma_heap *heap, size_t data_size);
void dma_heap_put_page(struct dma_heap *heap, struct dma_page *page);
void* dma_page_get_buf(struct dma_page *page);

#endif /* SPDK_DMA_HEAP_INTERNAL_H */