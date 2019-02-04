#ifndef BLOCK_ALLOC_H
#define BLOCK_ALLOC_H

#include <stdlib.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct BlkAllocBlock {
  struct BlkAllocBlock *next;
  size_t numUsed;
  size_t capacity;
  char data[0] __attribute__((aligned(16)));
} BlkAllocBlock;

typedef struct BlkAlloc {
  BlkAllocBlock *root;
  BlkAllocBlock *last;

  // Available blocks - used when recycling the allocator
  BlkAllocBlock *avail;
} BlkAlloc;

// Initialize a block allocator
static inline void BlkAlloc_Init(BlkAlloc *alloc) {
  alloc->root = NULL;
  alloc->last = NULL;
  alloc->avail = NULL;
}

/**
 * Allocate a new element from the block allocator. A pointer of size elemSize
 * will be returned. blockSize is the size of the new block to be created
 * (if the current block has no more room for elemSize). blockSize should be
 * greater than elemSize, and should likely be a multiple thereof.
 *
 * The returned pointer remains valid until FreeAll is called.
 */
void *BlkAlloc_Alloc(BlkAlloc *alloc, size_t elemSize, size_t blockSize);

typedef void (*BlkAllocCleaner)(void *ptr, void *arg);

/**
 * Free all memory allocated by the allocator.
 * If a cleaner function is called, it will be called for each element. Elements
 * are assumed to be elemSize spaces apart from each other.
 */
void BlkAlloc_FreeAll(BlkAlloc *alloc, BlkAllocCleaner cleaner, void *arg, size_t elemSize);

/**
 * Like FreeAll, except the blocks are recycled and placed inside the 'avail'
 * pool instead.
 */
void BlkAlloc_Clear(BlkAlloc *alloc, BlkAllocCleaner cleaner, void *arg, size_t elemSize);

#ifdef __cplusplus
}
#endif
#endif
