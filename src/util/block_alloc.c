#include "block_alloc.h"

#include <assert.h>
#include <stdio.h>
#include "rmalloc.h"

///////////////////////////////////////////////////////////////////////////////////////////////

BlkAllocBase::~BlkAllocBase() {
  Block *next;

  for (Block *cur = root; cur; cur = next) {
    next = cur->next;
    rm_free(cur);
  }

  for (Block *cur = avail; cur; cur = next) {
    next = cur->next;
    rm_free(cur);
  }
}

//---------------------------------------------------------------------------------------------

void BlkAllocBase::Clear() {
  Block *cur = root;
  while (cur) {
    Block *curNext = cur->next;
    cur->next = avail;
    avail = cur;
    cur = curNext;
  }
  root = last = nullptr;
}

//---------------------------------------------------------------------------------------------

BlkAllocBase::Block *BlkAllocBase::getNewBlock() {
  Block *block = nullptr;
  if (avail) {
    Block *prev = nullptr;
    for (Block *cur = avail; cur; cur = cur->next) {
      if (cur->capacity >= block_size) {
        block = cur;
        if (cur == avail) {
          avail = cur->next;
        } else {
          assert(prev != nullptr);
          prev->next = cur->next;
        }
        break;
      } else {
        prev = cur;
      }
    }
  }

  if (!block) {
    block = static_cast<Block *>(rm_malloc(sizeof *block + block_size));
    block->capacity = block_size;
  }

  block->numUsed = 0;
  block->next = nullptr;
  return block;
}

///////////////////////////////////////////////////////////////////////////////////////////////

char *StringBlkAlloc::strncpy(const char *s, size_t size) {
  size_t elemSize = size + 1;
  size_t blockSize = MAX(block_size, elemSize);

  if (!root) {
    root = last = getNewBlock();
  } else if (last->numUsed + elemSize > blockSize) {
    Block *block = getNewBlock();
    last->next = block;
    last = block;
  }

  char *p = last->data + last->numUsed;
  last->numUsed += elemSize;

  memcpy(p, s, size);
  p[size] = '\0';

  return p;
}

///////////////////////////////////////////////////////////////////////////////////////////////
