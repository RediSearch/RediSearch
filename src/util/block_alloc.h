#ifndef BLOCK_ALLOC_H
#define BLOCK_ALLOC_H

#include <stdlib.h>

typedef struct BlkAllocBlock {
  struct BlkAllocBlock *next;
  size_t numUsed;
  char data[0];
} BlkAllocBlock;

typedef struct BlkAlloc {
  BlkAllocBlock *root;
  BlkAllocBlock *last;
} BlkAlloc;

// Initialize a block allocator
static inline void BlkAlloc_Init(BlkAlloc *alloc) {
  alloc->root = NULL;
  alloc->last = NULL;
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

/**
 * Free all memory allocated by the allocator.
 * If a cleaner function is called, it will be called for each element. Elements
 * are assumed to be elemSize spaces apart from each other.
 */
void BlkAlloc_FreeAll(BlkAlloc *alloc, void (*cleaner)(void *), size_t elemSize);

#endif