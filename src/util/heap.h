/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#ifndef HEAP_H
#define HEAP_H
#include <stdlib.h>

typedef struct heap_s heap_t;

/**
 * Create new heap and initialise it.
 *
 * malloc()s space for heap.
 *
 * @param[in] cmp Callback used to get an item's priority
 * @param[in] udata User data passed through to cmp callback
 * @return initialised heap */
heap_t *heap_new(int (*cmp) (const void *,
                             const void *,
                             const void *udata),
                 const void *udata);

/**
 * Initialise heap. Use memory passed by user.
 *
 * No malloc()s are performed.
 *
 * @param[in] cmp Callback used to get an item's priority
 * @param[in] udata User data passed through to cmp callback
 * @param[in] size Initial size of the heap's array */
void heap_init(heap_t* h,
               int (*cmp) (const void *,
                           const void *,
                           const void *udata),
               const void *udata,
               unsigned int size);

void heap_free(heap_t * hp);

/**
 * Empties the heap and frees it.
 *
 * NOTE:
 *  Frees all items.
 *  Only use if item memory is NOT managed outside of heap.
 *  If `heap_clear` was invoked, the old data cannot be freed by the heap. */
void heap_destroy(heap_t * hp);

/**
 * Add item
 *
 * Ensures that the data structure can hold the item.
 *
 * NOTE:
 *  realloc() possibly called.
 *  The heap pointer will be changed if the heap needs to be enlarged.
 *
 * @param[in/out] hp_ptr Pointer to the heap. Changed when heap is enlarged.
 * @param[in] item The item to be added
 * @return 0 on success; -1 on failure */
int heap_offer(heap_t **hp_ptr, void *item);

/**
 * Add item
 *
 * An error will occur if there isn't enough space for this item.
 *
 * NOTE:
 *  no malloc()s called.
 *
 * @param[in] item The item to be added
 * @return 0 on success; -1 on error */
int heap_offerx(heap_t * hp, void *item);

/**
 * Remove the item with the top priority
 *
 * @return top item */
void *heap_poll(heap_t * hp);

/**
 * Replace root item
 *
 * @param[in] item The item to replace item at root
 * @return 0 on success; -1 on error */
void heap_replace(heap_t *h, void *item);

/**
 * @return top item of the heap */
void *heap_peek(const heap_t * hp);

/**
 * Clear all items
 *
 * NOTE:
 *  Does not free items.
 *  Only use if item memory is managed outside of heap */
void heap_clear(heap_t * hp);

/**
 * @return number of items in heap */
int heap_count(const heap_t * hp);

/**
 * @return size of array */
int heap_size(const heap_t * hp);

/**
 * @return number of bytes needed for a heap of this size. */
size_t heap_sizeof(unsigned int size);

/**
 * Remove item
 *
 * @param[in] item The item that is to be removed
 * @return item to be removed; NULL if item does not exist */
void *heap_remove_item(heap_t * hp, const void *item);

/**
 * Test membership of item
 *
 * @param[in] item The item to test
 * @return 1 if the heap contains this item; otherwise 0 */
int heap_contains_item(const heap_t * hp, const void *item);

/**
 * Called when an entry is removed
 */
typedef void (*HeapCallback)(void *dst, void *src);

/**
 * Run callback of all elements equal to root
 *
 * @param[in] callback The function to be called
 * @param[in] ctx The data required by the callback function
 * @return
 */
void heap_cb_root(const heap_t * hp, HeapCallback cb, void *ctx);

#endif /* HEAP_H */
