/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "redisearch_caps.h"
#include "version.h"
#include "test_util.h"

// The dev sentinel (99.99.99) is the only version range this predicate can verify
// today: the row-block format has not shipped in a numbered release, so master is
// the only build known to be capable. See redisearch_caps.c for where to add a
// real release's minor line once one ships the format.
int testHasRowBlock_devSentinel() {
  ASSERT(RediSearchCaps_HasRowBlock(REDISEARCH_MODULE_VERSION));
  return 0;
}

// A version one below the dev sentinel's minor line must not be treated as
// capable: ranges are per minor line, not `>=`, so this also guards against a
// future off-by-one when a real release line is added.
int testHasRowBlock_belowSupportedLine() {
  ASSERT(!RediSearchCaps_HasRowBlock(REDISEARCH_MODULE_VERSION - 1));
  return 0;
}

// Zero is the "no version was ever parsed" sentinel used by callers (e.g. a
// malformed or absent HELLO `ver` field); it must fail closed.
int testHasRowBlock_zero() {
  ASSERT(!RediSearchCaps_HasRowBlock(0));
  return 0;
}

// Garbage (negative, and a large value with no meaningful major/minor/patch
// decomposition) must fail closed rather than being silently accepted by
// integer-range arithmetic.
int testHasRowBlock_garbage() {
  ASSERT(!RediSearchCaps_HasRowBlock(-1));
  ASSERT(!RediSearchCaps_HasRowBlock(-999999));
  ASSERT(!RediSearchCaps_HasRowBlock(123456789));
  return 0;
}

TEST_MAIN({
  TESTFUNC(testHasRowBlock_devSentinel);
  TESTFUNC(testHasRowBlock_belowSupportedLine);
  TESTFUNC(testHasRowBlock_zero);
  TESTFUNC(testHasRowBlock_garbage);
})
