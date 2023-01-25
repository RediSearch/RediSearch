/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#pragma once

#include <stdlib.h>
#include <stdbool.h>
#include "block_alloc.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
  BlkAllocBlock *root;
  BlkAllocBlock *current;
  size_t elemSize;
} FixedSizeElementsBlocksManager;

// Initialize blocks manager for blocks that contains elements of size @elemeSize.
// @blockSize is the number of elements in the first block.
void FixedSizeElementsBlocksManager_init(FixedSizeElementsBlocksManager *BlocksManager, size_t elemSize, size_t blockSize);
/**
 * Returns a pointer to a memory of size BlocksManager->elemSize to the current available element.
 * @blockSize is the number of elements in the new block to be created if the
 *  current block is full. 
 *
 * The returned pointer remains valid until FreeAll is called.
 */
void *FixedSizeElementsBlocksManager_getElement(FixedSizeElementsBlocksManager *BlocksManager, size_t blockSize);

/**
 * Check if all the blocks are empty
 */
bool FixedSizeElementsBlocksManager_isEmpty(const FixedSizeElementsBlocksManager *BlocksManager);

/**
 * Free all memory allocated by the allocator.
 */
void FixedSizeElementsBlocksManager_FreeAll(FixedSizeElementsBlocksManager *BlocksManager);



#ifdef __cplusplus
}
#endif
