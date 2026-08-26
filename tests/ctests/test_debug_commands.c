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

  ASSERT_EQUAL(atomic_load(&seq), 10);
  return 0;
}
#endif

TEST_MAIN({
#ifdef ENABLE_ASSERT
  TESTFUNC(test_syncPointPublishMaxSeqDoesNotRegress);
#endif
});
