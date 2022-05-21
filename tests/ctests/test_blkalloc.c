#include "../util/block_alloc.h"
#include "test_util.h"
#include <stdint.h>
#include <assert.h>
#include "rmutil/alloc.h"

int testBlockAlloc() {
  RMUTil_InitAlloc();
  BlkAlloc alloc;
  BlkAlloc_Init(&alloc);
  ASSERT(alloc.last == NULL);
  ASSERT(alloc.root == NULL);

  char *buf = BlkAlloc_Alloc(&alloc, 4, 16);
  ASSERT(buf != NULL);
  ASSERT(alloc.last != NULL);
  ASSERT(alloc.last == alloc.root);
  ASSERT(alloc.root->numUsed == 4);
  ASSERT(alloc.root->next == NULL);

  char *buf2 = BlkAlloc_Alloc(&alloc, 12, 16);
  ASSERT(buf2 != NULL);
  ASSERT(buf2 == buf + 4);
  ASSERT(alloc.root == alloc.last);

  char *buf3 = BlkAlloc_Alloc(&alloc, 4, 16);
  ASSERT(buf3 != NULL);
  ASSERT(alloc.root != alloc.last);
  ASSERT(alloc.last->numUsed == 4);

  BlkAllocBlock *lastHead = alloc.last;
  // Alloc a new item
  char *buf4 = BlkAlloc_Alloc(&alloc, 16, 16);
  ASSERT(alloc.last != lastHead);

  BlkAlloc_Clear(&alloc, NULL, NULL, 0);
  ASSERT(alloc.root == alloc.last && alloc.last == NULL);
  ASSERT(alloc.avail != NULL);

  for (BlkAllocBlock *blk = alloc.root; blk; blk = blk->next) {
    ASSERT(blk->capacity > 0);
  }

  BlkAllocBlock *oldAvail = alloc.avail;
  buf = BlkAlloc_Alloc(&alloc, 4, 16);
  ASSERT(buf != NULL);
  ASSERT(alloc.root == oldAvail);
  ASSERT(alloc.avail != oldAvail);

  BlkAlloc_FreeAll(&alloc, NULL, NULL, 0);
  return 0;
}

typedef struct {
  char fillerSpace[32];
  uint32_t num;
  char trailerSpace[43];
} myDummy;

static void freeFunc(void *elem, void *p) {
  myDummy *dummy = elem;
  uint32_t *count = p;
  assert(dummy->num == *count);
  (*count)++;
}

static int testFreeFunc() {
  BlkAlloc alloc;
  BlkAlloc_Init(&alloc);

  uint32_t count = 0;
  for (size_t i = 0; i < 30; i++) {
    myDummy *dummy = BlkAlloc_Alloc(&alloc, sizeof(*dummy), sizeof(*dummy) * 4);
    dummy->num = i;
  }

  // Let's check if the free func works appropriately
  BlkAlloc_FreeAll(&alloc, freeFunc, &count, sizeof(myDummy));
  ASSERT(count == 30);
  return 0;
}

TEST_MAIN({
  TESTFUNC(testBlockAlloc);
  TESTFUNC(testFreeFunc);
})
