#include "../util/block_alloc.h"
#include "test_util.h"
#include <stdint.h>

int testBlockAlloc() {
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

  BlkAlloc_FreeAll(&alloc, NULL, 0);
  return 0;
}

TEST_MAIN({ TESTFUNC(testBlockAlloc); })