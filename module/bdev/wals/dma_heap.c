#include "dma_heap.h"

static size_t dma_heap_total_size(size_t data_size, size_t md_size)
{
    return ((data_size >> SIZE_SHIFT) << SIZE_SHIFT) + 
        ((data_size >> SIZE_SHIFT) / SIZE_512 
        + (data_size >> SIZE_SHIFT) / SIZE_4K 
        + (data_size >> SIZE_SHIFT) / SIZE_8K 
        + (data_size >> SIZE_SHIFT) / SIZE_32K
        + (data_size >> SIZE_SHIFT) / SIZE_64K
        + (data_size >> SIZE_SHIFT) / SIZE_128K
        + (data_size >> SIZE_SHIFT) / SIZE_1M
        + (data_size >> SIZE_SHIFT) / SIZE_4M
        ) * md_size;
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
    heap->data_size = (data_size >> SIZE_SHIFT) << SIZE_SHIFT;
    heap->md_size = md_size;

    TAILQ_INIT(&heap->page_512);
    TAILQ_INIT(&heap->page_4k);
    TAILQ_INIT(&heap->page_8k);
    TAILQ_INIT(&heap->page_32k);
    TAILQ_INIT(&heap->page_64k);
    TAILQ_INIT(&heap->page_128k);
    TAILQ_INIT(&heap->page_1m);
    TAILQ_INIT(&heap->page_4m);

    ptr = heap->buf;
    for (i = 0; i < (data_size >> SIZE_SHIFT) / SIZE_512; i++) {
        page = calloc(1, sizeof(*page));
        page->buf = ptr;
        page->data_size = SIZE_512;
        page->md_size = md_size;
        TAILQ_INSERT_TAIL(&heap->page_512, page, link);
        ptr += SIZE_512 + md_size;
    }
    for (i = 0; i < (data_size >> SIZE_SHIFT) / SIZE_4K; i++) {
        page = calloc(1, sizeof(*page));
        page->buf = ptr;
        page->data_size = SIZE_4K;
        page->md_size = md_size;
        TAILQ_INSERT_TAIL(&heap->page_4k, page, link);
        ptr+= SIZE_4K + md_size;
    }
    for (i = 0; i < (data_size >> SIZE_SHIFT) / SIZE_8K; i++) {
        page = calloc(1, sizeof(*page));
        page->buf = ptr;
        page->data_size = SIZE_8K;
        page->md_size = md_size;
        TAILQ_INSERT_TAIL(&heap->page_8k, page, link);
        ptr+= SIZE_8K + md_size;
    }
    for (i = 0; i < (data_size >> SIZE_SHIFT) / SIZE_32K; i++) {
        page = calloc(1, sizeof(*page));
        page->buf = ptr;
        page->data_size = SIZE_32K;
        page->md_size = md_size;
        TAILQ_INSERT_TAIL(&heap->page_32k, page, link);
        ptr+= SIZE_32K + md_size;
    }
    for (i = 0; i < (data_size >> SIZE_SHIFT) / SIZE_64K; i++) {
        page = calloc(1, sizeof(*page));
        page->buf = ptr;
        page->data_size = SIZE_64K;
        page->md_size = md_size;
        TAILQ_INSERT_TAIL(&heap->page_64k, page, link);
        ptr+= SIZE_64K + md_size;
    }
    for (i = 0; i < (data_size >> SIZE_SHIFT) / SIZE_128K; i++) {
        page = calloc(1, sizeof(*page));
        page->buf = ptr;
        page->data_size = SIZE_128K;
        page->md_size = md_size;
        TAILQ_INSERT_TAIL(&heap->page_128k, page, link);
        ptr+= SIZE_128K + md_size;
    }
    for (i = 0; i < (data_size >> SIZE_SHIFT) / SIZE_1M; i++) {
        page = calloc(1, sizeof(*page));
        page->buf = ptr;
        page->data_size = SIZE_1M;
        page->md_size = md_size;
        TAILQ_INSERT_TAIL(&heap->page_1m, page, link);
        ptr+= SIZE_1M + md_size;
    }
    for (i = 0; i < (data_size >> SIZE_SHIFT) / SIZE_4M; i++) {
        page = calloc(1, sizeof(*page));
        page->buf = ptr;
        page->data_size = SIZE_4M;
        page->md_size = md_size;
        TAILQ_INSERT_TAIL(&heap->page_4m, page, link);
        ptr+= SIZE_4M + md_size;
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
        if (TAILQ_EMPTY(&heap->page_512)) {
            return NULL;
        }
        page = TAILQ_FIRST(&heap->page_512);
        TAILQ_REMOVE(&heap->page_512, page, link);
        return page;
    }

    if (size <= SIZE_4K) {
        if (TAILQ_EMPTY(&heap->page_4k)) {
            return NULL;
        }
        page = TAILQ_FIRST(&heap->page_4k);
        TAILQ_REMOVE(&heap->page_4k, page, link);
        return page;
    }

    if (size <= SIZE_8K) {
        if (TAILQ_EMPTY(&heap->page_8k)) {
            return NULL;
        }
        page = TAILQ_FIRST(&heap->page_8k);
        TAILQ_REMOVE(&heap->page_8k, page, link);
        return page;
    }

    if (size <= SIZE_32K) {
        if (TAILQ_EMPTY(&heap->page_32k)) {
            return NULL;
        }
        page = TAILQ_FIRST(&heap->page_32k);
        TAILQ_REMOVE(&heap->page_32k, page, link);
        return page;
    }

    if (size <= SIZE_64K) {
        if (TAILQ_EMPTY(&heap->page_64k)) {
            return NULL;
        }
        page = TAILQ_FIRST(&heap->page_64k);
        TAILQ_REMOVE(&heap->page_64k, page, link);
        return page;
    }

    if (size <= SIZE_128K) {
        if (TAILQ_EMPTY(&heap->page_128k)) {
            return NULL;
        }
        page = TAILQ_FIRST(&heap->page_128k);
        TAILQ_REMOVE(&heap->page_128k, page, link);
        return page;
    }
    
    if (size <= SIZE_1M) {
        if (TAILQ_EMPTY(&heap->page_1m)) {
            return NULL;
        }
        page = TAILQ_FIRST(&heap->page_1m);
        TAILQ_REMOVE(&heap->page_1m, page, link);
        return page;
    }  

    if (size <= SIZE_4M) {
        if (TAILQ_EMPTY(&heap->page_4m)) {
            return NULL;
        }
        page = TAILQ_FIRST(&heap->page_4m);
        TAILQ_REMOVE(&heap->page_4m, page, link);
        return page;
    }    

    return NULL;
}

void dma_heap_put_page(struct dma_heap *heap, struct dma_page *page)
{
    switch (page->data_size) {
        case SIZE_512:
            TAILQ_INSERT_TAIL(&heap->page_512, page, link);
            break;
        case SIZE_4K:
            TAILQ_INSERT_TAIL(&heap->page_4k, page, link);
            break;
        case SIZE_8K:
            TAILQ_INSERT_TAIL(&heap->page_8k, page, link);
            break;
        case SIZE_32K:
            TAILQ_INSERT_TAIL(&heap->page_32k, page, link);
            break;
        case SIZE_64K:
            TAILQ_INSERT_TAIL(&heap->page_64k, page, link);
            break;
        case SIZE_128K:
            TAILQ_INSERT_TAIL(&heap->page_128k, page, link);
            break;
        case SIZE_1M:
            TAILQ_INSERT_TAIL(&heap->page_1m, page, link);
            break;
        case SIZE_4M:
            TAILQ_INSERT_TAIL(&heap->page_4m, page, link);
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
