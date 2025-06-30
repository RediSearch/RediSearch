/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "block_alloc.h"
#include <assert.h>
#include <stdio.h>
#include "rmalloc.h"
#include "rmutil/rm_assert.h"

static void freeCommon(BlkAlloc *blocks, BlkAllocCleaner cleaner, void *arg, size_t elemSize,
                       int reuse) {
  BlkAllocBlock *cur = blocks->root;
  while (cur) {
    if (cleaner) {
      for (char *p = cur->data; p < cur->data + cur->numUsed; p += elemSize) {
        cleaner(p, arg);
      }
    }
    BlkAllocBlock *curNext = cur->next;
    if (reuse) {
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
  freeCommon(blocks, cleaner, arg, elemSize, 0);
}

void BlkAlloc_Clear(BlkAlloc *blocks, BlkAllocCleaner cleaner, void *arg, size_t elemSize) {
  freeCommon(blocks, cleaner, arg, elemSize, 1);
  blocks->root = blocks->last = NULL;
}

static BlkAllocBlock *getNewBlock(BlkAlloc *alloc, size_t blockSize) {
  BlkAllocBlock *block = NULL;
  if (alloc->avail) {
    BlkAllocBlock *prev = NULL;
    for (BlkAllocBlock *cur = alloc->avail; cur; cur = cur->next) {
      if (cur->capacity >= blockSize) {
        // Set our block
        block = cur;
        if (cur == alloc->avail) {
          alloc->avail = cur->next;
        } else {
          RS_ASSERT(prev != NULL);
          prev->next = cur->next;
        }
        break;
      } else {
        prev = cur;
      }
    }
  }

  if (!block) {
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
