/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#include "block_alloc.h"

#include "rmalloc.h"
#include "rmutil/rm_assert.h"

static void freeCommon(BlkAlloc *blocks, BlkAllocCleaner cleaner, void *arg, size_t elemSize,
                       int reuse, size_t maxBlockSize) {
  BlkAllocBlock *cur = blocks->root;
  while (cur) {
    if (cleaner) {
      for (char *p = cur->data; p < cur->data + cur->numUsed; p += elemSize) {
        cleaner(p, arg);
      }
    }
    BlkAllocBlock *curNext = cur->next;
    if (reuse && cur->capacity <= maxBlockSize) {
      // getNewBlock only looks at the head of the free list, which is as good a fit as any only while
      // every recycled block has the same capacity.
      RS_LOG_ASSERT(!blocks->avail || blocks->avail->capacity == cur->capacity, "recycled blocks must share one capacity");
      cur->next = blocks->avail;
      blocks->avail = cur;
    } else {
      rm_free(cur);
    }
    cur = curNext;
  }

  if (reuse) {
    // assert(blocks->avail);
  } else if (blocks->avail) {
    cur = blocks->avail;
    while (cur) {
      BlkAllocBlock *curNext = cur->next;
      rm_free(cur);
      cur = curNext;
    }
  }
}

void BlkAlloc_FreeAll(BlkAlloc *blocks, BlkAllocCleaner cleaner, void *arg, size_t elemSize) {
  freeCommon(blocks, cleaner, arg, elemSize, 0, 0);
}

void BlkAlloc_Clear(BlkAlloc *blocks, BlkAllocCleaner cleaner, void *arg, size_t elemSize, size_t maxBlockSize) {
  freeCommon(blocks, cleaner, arg, elemSize, 1, maxBlockSize);
  blocks->root = blocks->last = NULL;
}

static BlkAllocBlock *getNewBlock(BlkAlloc *alloc, size_t blockSize) {
  BlkAllocBlock *block = NULL;
  // Only the head is considered so that allocation never walks the free list (see freeCommon).
  if (alloc->avail && alloc->avail->capacity >= blockSize) {
    block = alloc->avail;
    alloc->avail = block->next;
  } else {
    block = rm_malloc(sizeof(*alloc->root) + blockSize);
    block->capacity = blockSize;
  }

  block->numUsed = 0;
  block->next = NULL;
  return block;
}

void *BlkAlloc_Alloc(BlkAlloc *blocks, size_t elemSize, size_t blockSize) {
  RS_ASSERT(blockSize >= elemSize);
  if (!blocks->root) {
    blocks->root = blocks->last = getNewBlock(blocks, blockSize);

  } else if (blocks->last->numUsed + elemSize > blockSize) {
    // Allocate a new element
    BlkAllocBlock *newBlock = getNewBlock(blocks, blockSize);
    blocks->last->next = newBlock;
    blocks->last = newBlock;
  }

  void *p = blocks->last->data + blocks->last->numUsed;
  blocks->last->numUsed += elemSize;
  return p;
}
