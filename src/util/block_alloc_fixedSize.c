/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "block_alloc_fixedSize.h"
#include "rmalloc.h"
#include <assert.h>

struct fixedSizeBlock {
  	fixedSizeBlock *next;
	// Used memory in bytes.
	size_t usedMemory;
  	char data[0] __attribute__((aligned(16)));
};

static fixedSizeBlock *getNewBlock(const FixedSizeBlocksManager *BlocksManager) {
	fixedSizeBlock *ret = rm_malloc(sizeof(fixedSizeBlock) + BlocksManager->blockCapacity);
	ret->usedMemory = 0;
	ret->next = NULL;

	return ret;
}

static bool isBlockFull(const FixedSizeBlocksManager *BlocksManager, const fixedSizeBlock *block) {
	assert(BlocksManager->blockCapacity < block->usedMemory);
	return BlocksManager->blockCapacity == block->usedMemory;
}
static void *BlockGetNextEmptyElem(fixedSizeBlock *block, size_t ElemSize) {
	void *ret = block->data + block->usedMemory;
	block->usedMemory += ElemSize;
	return ret;
}

static void *BlockGetElem(fixedSizeBlock *block, size_t elemIndex, size_t elemSize) {
	size_t data_position = elemIndex * elemSize;
	if (data_position > block->usedMemory) {
		return NULL;
	}
	return block->data + data_position;

}

/**
 *  Initialize blocks manager for blocks that contains @blockSize elements of size @elemeSize.
*/
void FixedSizeBlocksManager_init(FixedSizeBlocksManager *BlocksManager, size_t elemSize, size_t blockSize) {
	BlocksManager->elemSize = elemSize;
	BlocksManager->blockCapacity = blockSize * elemSize;

	BlocksManager->root = getNewBlock(BlocksManager);
	BlocksManager->current = BlocksManager->root;

}

void *FixedSizeBlocksManager_getEmptyElement(FixedSizeBlocksManager *BlocksManager) {
	if(isBlockFull(BlocksManager, BlocksManager->current)) {
		BlkAllocBlock *newBlock = getNewBlock(BlocksManager);
		BlocksManager->current->next = newBlock;
		BlocksManager->current = newBlock;
	}

	return BlockGetNextEmptyElem(BlocksManager->current, BlocksManager->elemSize);
}

bool FixedSizeBlocksManager_isEmpty(const FixedSizeBlocksManager *BlocksManager) {
	return BlocksManager->root->usedMemory == 0;
}

void FixedSizeBlocksManager_FreeAll(FixedSizeBlocksManager *BlocksManager) {
  fixedSizeBlock *curr = BlocksManager->root;
  while (curr) {
	fixedSizeBlock *next = curr->next;
	rm_free(curr);
    curr = next;
  }
  BlocksManager->root = NULL;
  BlocksManager->current = NULL;

}

/************ Iterator functions ************/

/**
 * initialize new iterator
*/
void FixedSizeElementsBlocksManager_InitIterator(const FixedSizeBlocksManager *BlocksManager, FixedSizeBlocksIterator* resultsIterator) {
	resultsIterator->BlocksManager = BlocksManager;
	resultsIterator->currentBlock = BlocksManager->root;
	resultsIterator->curr_elem_index = 0;
}

/**
 * Returns the next element after @iter or NULL if there are no more elements
 * Updates the iterator.
 */
void *FixedSizeBlocksManager_getNextElement(FixedSizeBlocksIterator* resultsIterator) {
	void *ret = BlockGetElem(resultsIterator->currentBlock, resultsIterator->curr_elem_index, resultsIterator->BlocksManager->elemSize);
	// This is the end of the block
	if(!ret) {
		// if it's the last block, there are no more elements.
		if(resultsIterator->currentBlock == resultsIterator->BlocksManager->current) {
			return NULL;
		}
		// else get the next block
		resultsIterator->currentBlock = resultsIterator->currentBlock->next;
		resultsIterator->curr_elem_index = 0;
		return FixedSizeBlocksManager_getNextElement(resultsIterator);
	}
	++resultsIterator->curr_elem_index;

	return ret;

}
