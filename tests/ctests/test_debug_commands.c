/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#include "debug_commands.h"
#include "test_util.h"

#ifdef ENABLE_ASSERT
int test_syncPointPublishMaxSeqDoesNotRegress() {
  _Atomic uint64_t seq = 0;

  SyncPoint_PublishMaxSeq(&seq, 10);
  SyncPoint_PublishMaxSeq(&seq, 5);

  uint64_t actual = atomic_load(&seq);
  if (actual != 10) {
    fprintf(stderr, "%s:%d: %llu != 10\n", __FILE__, __LINE__, (unsigned long long)actual);
    return -1;
  }
  numAsserts++;

  return 0;
}

static void runDebugCommandTests(void) {
  TESTFUNC(test_syncPointPublishMaxSeqDoesNotRegress)
}
#else
static void runDebugCommandTests(void) {
}
#endif

TEST_MAIN({
  runDebugCommandTests();
})
