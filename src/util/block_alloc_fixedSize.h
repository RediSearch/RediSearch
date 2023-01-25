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

typedef struct fixedSizeBlock fixedSizeBlock; 



typedef struct {
  fixedSizeBlock *root;
  fixedSizeBlock *current;
  size_t elemSize;
  // Block capacity in bytes
  size_t blockCapacity;
} FixedSizeBlocksManager;

typedef struct {
  fixedSizeBlock *currentBlock;
  size_t curr_elem_index;
  const FixedSizeBlocksManager *BlocksManager;
} FixedSizeBlocksIterator;
/**
 *  Initialize blocks manager for blocks that contains @blockSize elements of size @elemeSize.
*/
void FixedSizeBlocksManager_init(FixedSizeBlocksManager *BlocksManager, size_t elemSize, size_t blockSize);
/**
 * Returns a pointer to a memory of size BlocksManager->elemSize to the current available element.
 * The returned pointer remains valid until FreeAll is called.
 */
void *FixedSizeBlocksManager_getEmptyElement(FixedSizeBlocksManager *BlocksManager);


/**
 * Check if all the blocks are empty
 */
bool FixedSizeBlocksManager_isEmpty(const FixedSizeBlocksManager *BlocksManager);

/**
 * Free all memory allocated by the allocator.
 */
void FixedSizeBlocksManager_FreeAll(FixedSizeBlocksManager *BlocksManager);

/************ Iterator functions ************/

/**
 * initialize new iterator
*/
void FixedSizeElementsBlocksManager_InitIterator(const FixedSizeBlocksManager *BlocksManager, FixedSizeBlocksIterator* resultsIterator);

/**
 * Returns the next element after @iter or NULL if there are no more elements
 * Updates the iterator.
 */
void *FixedSizeBlocksManager_getNextElement(FixedSizeBlocksIterator* resultsIterator);

#ifdef __cplusplus
}
#endif
