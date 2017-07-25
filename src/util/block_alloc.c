#include "block_alloc.h"
#include <assert.h>

void BlkAlloc_FreeAll(BlkAlloc *blocks, void (*cleaner)(void *m), size_t elemSize) {
  BlkAllocBlock *cur = blocks->root;
  while (cur) {
    if (cleaner) {
      for (char *p = cur->data; p < cur->data + cur->numUsed; p += elemSize) {
        cleaner(p);
      }
    }
    BlkAllocBlock *curNext = cur->next;
    free(cur);
    cur = curNext;
  }
}

void *BlkAlloc_Alloc(BlkAlloc *blocks, size_t elemSize, size_t blockSize) {
  assert(blockSize >= elemSize);

  if (!blocks->root) {
    blocks->root = blocks->last = malloc(sizeof(*blocks->root) + blockSize);
    blocks->root->next = NULL;
    blocks->root->numUsed = 0;
  } else if (blocks->last->numUsed + elemSize > blockSize) {
    // Allocate a new element
    BlkAllocBlock *newBlock = malloc(sizeof(*blocks->root) + blockSize);
    newBlock->next = NULL;

    blocks->last->next = newBlock;
    blocks->last = newBlock;
    blocks->last->numUsed = 0;
  }

  void *p = blocks->last->data + blocks->last->numUsed;
  blocks->last->numUsed += elemSize;
  return p;
}