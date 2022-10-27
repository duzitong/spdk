#include "dma_heap.h"

#define BUFFER_ALIGN    2 * 1024 * 1024

static size_t
dma_heap_align(size_t size, int align_shift)
{
    return (size >> align_shift) << align_shift;
}

static size_t
dma_heap_total_size(size_t data_size, size_t md_size, int checksum_size_512, int align_shift)
{
    size_t total_size = data_size;

    total_size += (data_size >> SIZE_SHIFT) / SIZE_512 * dma_heap_align(checksum_size_512 + md_size, align_shift);
    total_size += (data_size >> SIZE_SHIFT) / SIZE_4K * dma_heap_align(checksum_size_512 * 8 + md_size, align_shift);
    total_size += (data_size >> SIZE_SHIFT) / SIZE_8K * dma_heap_align(checksum_size_512 * 16 + md_size, align_shift);
    total_size += (data_size >> SIZE_SHIFT) / SIZE_32K * dma_heap_align(checksum_size_512 * 64 + md_size, align_shift);
    total_size += (data_size >> SIZE_SHIFT) / SIZE_64K * dma_heap_align(checksum_size_512 * 128 + md_size, align_shift);
    total_size += (data_size >> SIZE_SHIFT) / SIZE_128K * dma_heap_align(checksum_size_512 * 256 + md_size, align_shift);
    total_size += (data_size >> SIZE_SHIFT) / SIZE_1M * dma_heap_align(checksum_size_512 * 2048 + md_size, align_shift);
    total_size += (data_size >> SIZE_SHIFT) / SIZE_4M * dma_heap_align(checksum_size_512 * 8192 + md_size, align_shift);
    return total_size;
}

struct dma_heap*
dma_heap_alloc(size_t data_size, size_t md_size, int checksum_size_512, int align_shift)
{
    struct dma_heap *heap = calloc(1, sizeof(*heap));
    struct dma_page *page;
    void *ptr;
    size_t total_size, i;

    data_size = (data_size >> SIZE_SHIFT) << SIZE_SHIFT; 
    total_size = dma_heap_total_size(data_size, md_size, checksum_size_512, align_shift);
    heap->buf = spdk_dma_zmalloc(total_size, BUFFER_ALIGN, NULL);
    heap->buf_size = total_size;
    heap->data_size = data_size;
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
        page->buf_size = page->data_size + dma_heap_align(checksum_size_512 + md_size, align_shift);
        TAILQ_INSERT_HEAD(&heap->page_512, page, link);
        ptr += page->buf_size;
    }
    for (i = 0; i < (data_size >> SIZE_SHIFT) / SIZE_4K; i++) {
        page = calloc(1, sizeof(*page));
        page->buf = ptr;
        page->data_size = SIZE_4K;
        page->buf_size = page->data_size + dma_heap_align(checksum_size_512 * 8 + md_size, align_shift);
        TAILQ_INSERT_HEAD(&heap->page_4k, page, link);
        ptr += page->buf_size;
    }
    for (i = 0; i < (data_size >> SIZE_SHIFT) / SIZE_8K; i++) {
        page = calloc(1, sizeof(*page));
        page->buf = ptr;
        page->data_size = SIZE_8K;
        page->buf_size = page->data_size + dma_heap_align(checksum_size_512 * 16 + md_size, align_shift);
        TAILQ_INSERT_HEAD(&heap->page_8k, page, link);
        ptr += page->buf_size;
    }
    for (i = 0; i < (data_size >> SIZE_SHIFT) / SIZE_32K; i++) {
        page = calloc(1, sizeof(*page));
        page->buf = ptr;
        page->data_size = SIZE_32K;
        page->buf_size = page->data_size + dma_heap_align(checksum_size_512 * 64 + md_size, align_shift);
        TAILQ_INSERT_HEAD(&heap->page_32k, page, link);
        ptr += page->buf_size;
    }
    for (i = 0; i < (data_size >> SIZE_SHIFT) / SIZE_64K; i++) {
        page = calloc(1, sizeof(*page));
        page->buf = ptr;
        page->data_size = SIZE_64K;
        page->buf_size = page->data_size + dma_heap_align(checksum_size_512 * 128 + md_size, align_shift);
        TAILQ_INSERT_HEAD(&heap->page_64k, page, link);
        ptr += page->buf_size;
    }
    for (i = 0; i < (data_size >> SIZE_SHIFT) / SIZE_128K; i++) {
        page = calloc(1, sizeof(*page));
        page->buf = ptr;
        page->data_size = SIZE_128K;
        page->buf_size = page->data_size + dma_heap_align(checksum_size_512 * 256 + md_size, align_shift);
        TAILQ_INSERT_HEAD(&heap->page_128k, page, link);
        ptr += page->buf_size;
    }
    for (i = 0; i < (data_size >> SIZE_SHIFT) / SIZE_1M; i++) {
        page = calloc(1, sizeof(*page));
        page->buf = ptr;
        page->data_size = SIZE_1M;
        page->buf_size = page->data_size + dma_heap_align(checksum_size_512 * 2048 + md_size, align_shift);
        TAILQ_INSERT_HEAD(&heap->page_1m, page, link);
        ptr += page->buf_size;
    }
    for (i = 0; i < (data_size >> SIZE_SHIFT) / SIZE_4M; i++) {
        page = calloc(1, sizeof(*page));
        page->buf = ptr;
        page->data_size = SIZE_4M;
        page->buf_size = page->data_size + dma_heap_align(checksum_size_512 * 8192 + md_size, align_shift);
        TAILQ_INSERT_HEAD(&heap->page_4m, page, link);
        ptr += page->buf_size;
    }

    return heap;
}

void*
dma_heap_get_buf(struct dma_heap *heap)
{
    return heap->buf;
}

size_t
dma_heap_get_buf_size(struct dma_heap *heap)
{
    return heap->buf_size;
}

struct dma_page*
dma_heap_get_page(struct dma_heap *heap, size_t size)
{
    struct dma_page *page;

    if (!(size >> SIZE_512_SHIFT)) {
        if (TAILQ_EMPTY(&heap->page_512)) {
            return NULL;
        }
        page = TAILQ_FIRST(&heap->page_512);
        TAILQ_REMOVE(&heap->page_512, page, link);
        return page;
    }

    if (!(size >> SIZE_4K_SHIFT)) {
        if (TAILQ_EMPTY(&heap->page_4k)) {
            return NULL;
        }
        page = TAILQ_FIRST(&heap->page_4k);
        TAILQ_REMOVE(&heap->page_4k, page, link);
        return page;
    }

    if (!(size >> SIZE_8K_SHIFT)) {
        if (TAILQ_EMPTY(&heap->page_8k)) {
            return NULL;
        }
        page = TAILQ_FIRST(&heap->page_8k);
        TAILQ_REMOVE(&heap->page_8k, page, link);
        return page;
    }

    if (!(size >> SIZE_32K_SHIFT)) {
        if (TAILQ_EMPTY(&heap->page_32k)) {
            return NULL;
        }
        page = TAILQ_FIRST(&heap->page_32k);
        TAILQ_REMOVE(&heap->page_32k, page, link);
        return page;
    }

    if (!(size >> SIZE_64K_SHIFT)) {
        if (TAILQ_EMPTY(&heap->page_64k)) {
            return NULL;
        }
        page = TAILQ_FIRST(&heap->page_64k);
        TAILQ_REMOVE(&heap->page_64k, page, link);
        return page;
    }

    if (!(size >> SIZE_128K_SHIFT)) {
        if (TAILQ_EMPTY(&heap->page_128k)) {
            return NULL;
        }
        page = TAILQ_FIRST(&heap->page_128k);
        TAILQ_REMOVE(&heap->page_128k, page, link);
        return page;
    }
    
    if (!(size >> SIZE_1M_SHIFT)) {
        if (TAILQ_EMPTY(&heap->page_1m)) {
            return NULL;
        }
        page = TAILQ_FIRST(&heap->page_1m);
        TAILQ_REMOVE(&heap->page_1m, page, link);
        return page;
    }  

    if (!(size >> SIZE_4M_SHIFT)) {
        if (TAILQ_EMPTY(&heap->page_4m)) {
            return NULL;
        }
        page = TAILQ_FIRST(&heap->page_4m);
        TAILQ_REMOVE(&heap->page_4m, page, link);
        return page;
    }    

    return NULL;
}

void
dma_heap_put_page(struct dma_heap *heap, struct dma_page *page)
{
    switch (page->data_size) {
        case SIZE_512:
            TAILQ_INSERT_HEAD(&heap->page_512, page, link);
            break;
        case SIZE_4K:
            TAILQ_INSERT_HEAD(&heap->page_4k, page, link);
            break;
        case SIZE_8K:
            TAILQ_INSERT_HEAD(&heap->page_8k, page, link);
            break;
        case SIZE_32K:
            TAILQ_INSERT_HEAD(&heap->page_32k, page, link);
            break;
        case SIZE_64K:
            TAILQ_INSERT_HEAD(&heap->page_64k, page, link);
            break;
        case SIZE_128K:
            TAILQ_INSERT_HEAD(&heap->page_128k, page, link);
            break;
        case SIZE_1M:
            TAILQ_INSERT_HEAD(&heap->page_1m, page, link);
            break;
        case SIZE_4M:
            TAILQ_INSERT_HEAD(&heap->page_4m, page, link);
            break;
        default:
            // should not happen
            return;
    }
}

void*
dma_page_get_buf(struct dma_page *page)
{
    return page->buf;
}
