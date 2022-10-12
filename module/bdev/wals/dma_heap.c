#include "dma_heap.h"

static size_t dma_heap_total_size(size_t data_size, size_t md_size)
{
    return ((data_size >> 2) << 2) + 
        ((data_size >> 2) / SIZE_512 
        + (data_size >> 2) / SIZE_4K 
        + (data_size >> 2) / SIZE_8K 
        + (data_size >> 2) / SIZE_64K) * md_size;
}

struct dma_heap* dma_heap_alloc(size_t data_size, size_t md_size, size_t align)
{
    struct dma_heap *heap = calloc(1, sizeof(*heap));
    struct dma_page *page;
    void *ptr;
    size_t total_size, i;

    total_size = dma_heap_total_size(data_size, md_size);
    heap->buf = spdk_dma_zmalloc(total_size, align, NULL);
    heap->buf_size = total_size;
    heap->data_size = (data_size >> 2) << 2;
    heap->md_size = md_size;

    TAILQ_INIT(&heap->buf_512);
    TAILQ_INIT(&heap->buf_4k);
    TAILQ_INIT(&heap->buf_8k);
    TAILQ_INIT(&heap->buf_64k);

    ptr = heap->buf;
    for (i = 0; i < (data_size >> 2) / SIZE_512; i++) {
        page = calloc(1, sizeof(*page));
        page->buf = ptr;
        page->data_size = SIZE_512;
        page->md_size = md_size;
        TAILQ_INSERT_TAIL(&heap->buf_512, page, link);
        ptr += SIZE_512 + md_size;
    }
    for (i = 0; i < (data_size >> 2) / SIZE_4K; i++) {
        page = calloc(1, sizeof(*page));
        page->buf = ptr;
        page->data_size = SIZE_4K;
        page->md_size = md_size;
        TAILQ_INSERT_TAIL(&heap->buf_4k, page, link);
        ptr+= SIZE_4K + md_size;
    }
    for (i = 0; i < (data_size >> 2) / SIZE_8K; i++) {
        page = calloc(1, sizeof(*page));
        page->buf = ptr;
        page->data_size = SIZE_8K;
        page->md_size = md_size;
        TAILQ_INSERT_TAIL(&heap->buf_8k, page, link);
        ptr+= SIZE_8K + md_size;
    }
    for (i = 0; i < (data_size >> 2) / SIZE_64K; i++) {
        page = calloc(1, sizeof(*page));
        page->buf = ptr;
        page->data_size = SIZE_64K;
        page->md_size = md_size;
        TAILQ_INSERT_TAIL(&heap->buf_64k, page, link);
        ptr+= SIZE_64K + md_size;
    }

    return heap;
}

void* dma_heap_get_buf(struct dma_heap *heap)
{
    return heap->buf;
}

size_t dma_heap_get_buf_size(struct dma_heap *heap)
{
    return heap->buf_size;
}

struct dma_page* dma_heap_get_page(struct dma_heap *heap, size_t size)
{
    struct dma_page *page;

    if (size <= SIZE_512) {
        if (TAILQ_EMPTY(&heap->buf_512)) {
            return NULL;
        }
        page = TAILQ_FIRST(&heap->buf_512);
        TAILQ_REMOVE(&heap->buf_512, page, link);
        return page;
    }

    if (size <= SIZE_4K) {
        if (TAILQ_EMPTY(&heap->buf_4k)) {
            return NULL;
        }
        page = TAILQ_FIRST(&heap->buf_4k);
        TAILQ_REMOVE(&heap->buf_4k, page, link);
        return page;
    }

    if (size <= SIZE_8K) {
        if (TAILQ_EMPTY(&heap->buf_8k)) {
            return NULL;
        }
        page = TAILQ_FIRST(&heap->buf_8k);
        TAILQ_REMOVE(&heap->buf_8k, page, link);
        return page;
    }

    if (size <= SIZE_64K) {
        if (TAILQ_EMPTY(&heap->buf_64k)) {
            return NULL;
        }
        page = TAILQ_FIRST(&heap->buf_64k);
        TAILQ_REMOVE(&heap->buf_64k, page, link);
        return page;
    }

    return NULL;
}

void dma_heap_put_page(struct dma_heap *heap, struct dma_page *page)
{
    switch (page->data_size) {
        case SIZE_512:
            TAILQ_INSERT_TAIL(&heap->buf_512, page, link);
            break;
        case SIZE_4K:
            TAILQ_INSERT_TAIL(&heap->buf_4k, page, link);
            break;
        case SIZE_8K:
            TAILQ_INSERT_TAIL(&heap->buf_8k, page, link);
            break;
        case SIZE_64K:
            TAILQ_INSERT_TAIL(&heap->buf_64k, page, link);
            break;
        default:
            // should not happen
            return;
    }
}

void* dma_page_get_buf(struct dma_page *page)
{
    return page->buf;
}
