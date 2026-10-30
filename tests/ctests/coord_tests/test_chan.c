/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#include "minunit.h"
#include "chan.h"
#include "rmalloc.h"
#include "rmutil/alloc.h"

void testChan() {
  MRChannel *c = MR_NewChannel();
  mu_check(c != NULL);
  mu_assert_int_eq(0, MRChannel_Size(c));

  for (int i = 0; i < 100; i++) {
    int *ptr = rm_malloc(sizeof(*ptr));
    *ptr = i;
    MRChannel_Push(c, ptr);
    mu_assert_int_eq(i + 1, MRChannel_Size(c));
  }

  int count = 0;
  void *p;
  while (MRChannel_Size(c) && (p = MRChannel_Pop(c))) {
    mu_assert_int_eq(*(int *)p, count);
    count++;
    rm_free(p);
  }
  mu_assert_int_eq(100, count);
  mu_assert_int_eq(0, MRChannel_Size(c));

  MRChannel_Free(c);
}

void testTryPop() {
  MRChannel *c = MR_NewChannel();
  int first = 1, second = 2, late = 3;
  mu_check(MRChannel_TryPop(c) == NULL);
  MRChannel_Push(c, &first);
  MRChannel_Push(c, &second);
  mu_check(MRChannel_TryPop(c) == &first);
  // A reply arriving during the drain remains eligible for collection.
  MRChannel_Push(c, &late);
  mu_check(MRChannel_TryPop(c) == &second);
  mu_check(MRChannel_TryPop(c) == &late);
  mu_check(MRChannel_TryPop(c) == NULL);
  mu_assert_int_eq(0, MRChannel_Size(c));
  MRChannel_Free(c);
}

int main(int argc, char **argv) {
  RMUTil_InitAlloc();
  MU_RUN_TEST(testChan);
  MU_RUN_TEST(testTryPop);
  MU_REPORT();

  return minunit_status;
}
