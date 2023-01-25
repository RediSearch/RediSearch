/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "block_alloc_fixedElemSize.h"
#include "rmalloc.h"
#include <assert.h>


static BlkAllocBlock *getNewBlock(const FixedSizeElementsBlocksManager *BlocksManager, size_t blockSize) {
	size_t data_memory_size = blockSize * BlocksManager->elemSize;
	BlkAllocBlock *ret = rm_malloc(sizeof(BlkAllocBlock) + data_memory_size);
	ret->capacity = data_memory_size;
	ret->numUsed = 0;
	ret->next = NULL;

	return ret;
}

static bool isBlockFull(const BlkAllocBlock *block) {
	assert(block->capacity > block->numUsed);
	return block->capacity == block->numUsed;
}
static void *BlockgetNextElem(BlkAllocBlock *block, size_t ElemSize) {
	block->numUsed += ElemSize;
	return block->data + block->numUsed;
}

// Initialize blocks manager for blocks that contains elements of size @elemeSize.
// @blockSize is the number of elements in the first block
void FixedSizeElementsBlocksManager_init(FixedSizeElementsBlocksManager *BlocksManager, size_t elemSize, size_t blockSize) {
	BlocksManager->current = NULL;
	BlocksManager->elemSize = elemSize;

	BlocksManager->root = getNewBlock(BlocksManager, blockSize);

}

void *FixedSizeElementsBlocksManager_getElement(FixedSizeElementsBlocksManager *BlocksManager, size_t blockSize) {
	if(isBlockFull(BlocksManager->current)) {
		BlkAllocBlock *newBlock = getNewBlock(BlocksManager, blockSize);
		BlocksManager->current->next = newBlock;
		BlocksManager->current = newBlock;
	}

	return BlockgetNextElem(BlocksManager->current, BlocksManager->elemSize);
}

bool FixedSizeElementsBlocksManager_isEmpty(const FixedSizeElementsBlocksManager *BlocksManager) {
	return BlocksManager->root->numUsed == 0;
}

void FixedSizeElementsBlocksManager_FreeAll(FixedSizeElementsBlocksManager *BlocksManager) {
  BlkAllocBlock *curr = BlocksManager->root;
  while (curr) {
	BlkAllocBlock *next = curr->next;
	rm_free(curr);
    curr = next;
  }
  BlocksManager->root = NULL;
  BlocksManager->current = NULL;

}