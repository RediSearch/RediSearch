/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#include "gc/disk_gc_policy.h"
#include <stdio.h>
#include <stdlib.h>

#define CHECK(expr) do { if (!(expr)) { fprintf(stderr, "%s:%d: %s\n", __FILE__, __LINE__, #expr); abort(); } } while (0)

int main(void) {
  DiskGCPolicyState state = {0};
  CHECK(!DiskGCPolicy_Due(&state, 1000000, 30000, 300000, 0, 10000, false));
  CHECK(!DiskGCPolicy_Due(&state, 1000000, 30000, 300000, 0, 0, false));
  CHECK(DiskGCPolicy_Due(&state, 0, 30000, 300000, 10000, 10000, false));
  CHECK(DiskGCPolicy_Due(&state, 0, 30000, 300000, 0, 10000, true));

  DiskGCPolicy_Observe(&state, true, 1000);
  DiskGCPolicy_Observe(&state, true, 2000);
  CHECK(!DiskGCPolicy_Due(&state, 300999, 30000, 300000, 1, 10000, false));
  CHECK(DiskGCPolicy_Due(&state, 301000, 30000, 300000, 1, 10000, false));
  // Inserts alone eventually consolidate and drive numeric maintenance.
  CHECK(DiskGCPolicy_Due(&state, 301000, 30000, 300000, 0, 10000, false));

  DiskGCPolicy_Completed(&state, 400000, false);
  CHECK(state.dirtySinceMs == 1000 && state.dirty);
  CHECK(!DiskGCPolicy_Due(&state, 429999, 30000, 300000, 10000, 10000, true));
  CHECK(DiskGCPolicy_Due(&state, 430000, 30000, 300000, 0, 10000, false));
  CHECK(!DiskGCPolicy_CooledDown(&state, 399999, 0));

  DiskGCPolicy_Completed(&state, 500000, true);
  CHECK(!state.dirty);
  CHECK(!DiskGCPolicy_Due(&state, 900000, 30000, 300000, 0, 10000, false));
  DiskGCPolicy_Observe(&state, true, 910000);
  CHECK(!DiskGCPolicy_Due(&state, 1209999, 30000, 300000, 0, 10000, false));
  CHECK(DiskGCPolicy_Due(&state, 1210000, 30000, 300000, 0, 10000, false));
  DiskGCPolicy_Observe(&state, false, 1210000);
  CHECK(!DiskGCPolicy_Due(&state, UINT64_MAX, 30000, 300000, 0, 10000, false));

  CHECK(!DiskGC_CountReached(30, 30, 39, 100));
  CHECK(DiskGC_CountReached(30, 30, 40, 100));
  CHECK(DiskGC_CountReached(0, 0, 0, 0));
  CHECK(DiskGC_CountReached(SIZE_MAX - 2, 2, 1, SIZE_MAX));
  CHECK(!DiskGC_CountReached(SIZE_MAX - 2, 0, 1, SIZE_MAX));
  puts("disk GC policy: all checks passed");
  return 0;
}
