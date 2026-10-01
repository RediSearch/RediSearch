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
int testRowBlock_devSentinel() {
  ASSERT(RediSearchCaps_Supports(RS_CAP_ROW_BLOCK, REDISEARCH_MODULE_VERSION));
  return 0;
}

// A version one below the dev sentinel's minor line must not be treated as
// capable: ranges are per minor line, not `>=`, so this also guards against a
// future off-by-one when a real release line is added.
int testRowBlock_belowSupportedLine() {
  ASSERT(!RediSearchCaps_Supports(RS_CAP_ROW_BLOCK, REDISEARCH_MODULE_VERSION - 1));
  return 0;
}

// Zero is the "no version was ever parsed" sentinel used by callers (e.g. a
// malformed or absent HELLO `ver` field); it must fail closed.
int testRowBlock_zero() {
  ASSERT(!RediSearchCaps_Supports(RS_CAP_ROW_BLOCK, 0));
  return 0;
}

// Garbage (negative, and a large value with no meaningful major/minor/patch
// decomposition) must fail closed rather than being silently accepted by
// integer-range arithmetic.
int testRowBlock_garbage() {
  ASSERT(!RediSearchCaps_Supports(RS_CAP_ROW_BLOCK, -1));
  ASSERT(!RediSearchCaps_Supports(RS_CAP_ROW_BLOCK, -999999));
  ASSERT(!RediSearchCaps_Supports(RS_CAP_ROW_BLOCK, 123456789));
  return 0;
}

// A capability with no registered predicate must fail closed rather than crash -
// RS_CAP__COUNT is guaranteed to have no table entry since it is the table size,
// not a real capability.
int testSupports_unregisteredCapabilityFailsClosed() {
  ASSERT(!RediSearchCaps_Supports((RSCapability)RS_CAP__COUNT, REDISEARCH_MODULE_VERSION));
  return 0;
}

TEST_MAIN({
  TESTFUNC(testRowBlock_devSentinel);
  TESTFUNC(testRowBlock_belowSupportedLine);
  TESTFUNC(testRowBlock_zero);
  TESTFUNC(testRowBlock_garbage);
  TESTFUNC(testSupports_unregisteredCapabilityFailsClosed);
})
