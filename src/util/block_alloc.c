#include "block_alloc.h"

#include <assert.h>
#include <stdio.h>
#include "rmalloc.h"

///////////////////////////////////////////////////////////////////////////////////////////////

void BlkAlloc::freeCommon(Cleaner cleaner, void *arg, size_t elemSize, bool reuse) {
  Block *cur = root;
  while (cur) {
    if (cleaner) {
      for (char *p = cur->data; p < cur->data + cur->numUsed; p += elemSize) {
        cleaner(p, arg);
      }
    }
    Block *curNext = cur->next;
    if (reuse) {
      cur->next = avail;
      avail = cur;
    } else {
      rm_free(cur);
    }
    cur = curNext;
  }

  // size_t n = 0;
  // for (cur = avail; cur; cur = cur->next) {
  //   n++;
  // }
  // printf("%p: Have %lu available blocks\n", blocks, n);
  if (reuse) {
    // assert(avail);
  } else if (avail) {
    cur = avail;
    while (cur) {
      Block *curNext = cur->next;
      rm_free(cur);
      cur = curNext;
    }
  }
}

//---------------------------------------------------------------------------------------------

void BlkAlloc::FreeAll(Cleaner cleaner, void *arg, size_t elemSize) {
  freeCommon(cleaner, arg, elemSize, false);
}

//---------------------------------------------------------------------------------------------

void BlkAlloc::Clear(BlkAllocCleaner cleaner, void *arg, size_t elemSize) {
  freeCommon(cleaner, arg, elemSize, true);
  root = last = NULL;
}

//---------------------------------------------------------------------------------------------

BlkAlloc::Block *BlkAlloc::getNewBlock(size_t blockSize) {
  // printf("%p: getNewBlock BEGIN (sz=%llu)\n", alloc, blockSize);
  Block *block = NULL;
  if (alloc->avail) {
    // printf("%p: have avail..\n", alloc);
    Block *prev = NULL;
    for (Block *cur = alloc->avail; cur; cur = cur->next) {
      if (cur->capacity >= blockSize) {
        // Set our block
        block = cur;
        if (cur == alloc->avail) {
          alloc->avail = cur->next;
        } else {
          assert(prev != NULL);
          prev->next = cur->next;
        }
        break;
      } else {
        prev = cur;
      }
    }
  }

  if (!block) {
    // printf("Allocating new block..\n");
    block = rm_malloc(sizeof(*alloc->root) + blockSize);
    block->capacity = blockSize;
  } else {
    // printf("Reusing block %p. Alloc->Avail=%p\n", block, alloc->avail);
  }

  block->numUsed = 0;
  block->next = NULL;
  // printf("%p: getNewBlock END\n", alloc);
  return block;
}

//---------------------------------------------------------------------------------------------

void *BlkAlloc::Alloc(size_t elemSize, size_t blockSize) {
  assert(blockSize >= elemSize);
  if (!root) {
    root = blocks->last = getNewBlock(blockSize);
  } else if (last->numUsed + elemSize > blockSize) {
    // Allocate a new element
    Block *newBlock = getNewBlock(blockSize);
    last->next = newBlock;
    last = newBlock;

    // size_t n = 0;
    // for (Block *tmp = root; tmp; tmp = tmp->next) {
    //   n++;
    // }
    // printf("%p Allocated new block, elem size %d %d\n", this, elemSize, blockSize / elemSize);
  }

  void *p = last->data + last->numUsed;
  last->numUsed += elemSize;
  return p;
}

///////////////////////////////////////////////////////////////////////////////////////////////
