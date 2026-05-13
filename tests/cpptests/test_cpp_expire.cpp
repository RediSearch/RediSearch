/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/


#include "gtest/gtest.h"
#include "aggregate/aggregate.h"
#include "redismock/redismock.h"
#include "redismock/util.h"
#include "redismock/internal.h"
#include "spec.h"
#include "common.h"
#include "config.h"
#include "module.h"
#include "version.h"
#include "tag_index.h"
#include "ttl_table.h"
#include "info/info_redis/threads/current_thread.h"

#include <vector>
#include <array>
#include <iostream>
#include <cstdarg>
#include <chrono>

class ExpireTest : public ::testing::Test {};
using RS::addDocument;

TEST_F(ExpireTest, testSkipTo) {
  RedisModuleCtx *ctx = RedisModule_GetThreadSafeContext(NULL);
  QueryError qerr = QueryError_Default();

  RMCK::ArgvList args(ctx, "FT.CREATE", "expire_idx", "ON", "HASH", "SKIPINITIALSCAN",
                      "SCHEMA", "t1", "TAG");
  IndexSpec *spec = IndexSpec_CreateNew(ctx, args, args.size(), &qerr);
  CurrentThread_SetIndexSpec(spec->own_ref);
  ASSERT_NE(spec, nullptr);
  const FieldSpec *fs = IndexSpec_GetFieldWithLength(spec, "t1", 2);
  ASSERT_NE(fs, nullptr);
  RSIndex *index = spec->own_ref.rm;
  ASSERT_TRUE(spec);
  RedisModuleString* hset_args[3] = { NULL,
                                 RedisModule_CreateString(ctx, "t1", strlen("t1")),
                                 RedisModule_CreateString(ctx, "one", strlen("one"))};
  static const t_docId maxDocId = 1000;
  // Add 1000 documents to the index and expire the fields
  for (t_docId doc = 1; doc <= maxDocId; ++doc) {
    char buf[1024];
    snprintf(buf, sizeof(buf), "doc:%ld", doc);
    hset_args[0] = RedisModule_CreateString(ctx, buf, strlen(buf));
    RedisModuleCallReply *hset = RedisModule_Call(ctx, "HSET", "!v", hset_args, sizeof(hset_args) / sizeof(hset_args[0]));
    RedisModule_FreeCallReply(hset);
    RedisModuleCallReply *hexpire = RedisModule_Call(ctx, "HPEXPIRE", "cvc", buf, 1ull/*expireAt*/, 1ull/*count*/, "t1");
    RedisModule_FreeCallReply(hexpire);
    RedisModule_FreeString(ctx, hset_args[0]);
  }
  RedisModule_FreeString(ctx, hset_args[1]);
  RedisModule_FreeString(ctx, hset_args[2]);

  RedisSearchCtx *sctx = NewSearchCtxC(ctx, HiddenString_GetUnsafe(spec->specName, NULL), true);
  const auto epoch = std::chrono::system_clock::now().time_since_epoch();
  const auto seconds = std::chrono::duration_cast<std::chrono::seconds>(epoch);
  const auto remaining = epoch - seconds;
  // set the time so all previous fields should now be considered expired
  sctx->time.current.tv_sec = seconds.count() + 1;
  sctx->time.current.tv_nsec = std::chrono::duration_cast<std::chrono::nanoseconds>(remaining).count();

  TagIndex *idx = TagIndex_Open(fs);
  ASSERT_NE(idx, nullptr);
  QueryIterator *it = TagIndex_OpenReader(idx, sctx, "one", strlen("one"), 1.0, 0);
  ASSERT_EQ(it->lastDocId, 0);
  // should skip to last document, we index every doc twice so we should have 2 * maxDocId entries in the inverted index
  for (t_docId doc = 2; doc < (2 * maxDocId); doc += 2) {
    RSIndexResult *result = NULL;
    // we index in hset and in hexpire
    // for hset there won't be an expiration
    // for hexpire there will be
    // if we skip to an odd doc number we should get the requested doc id
    // if we skip to an even doc number we should not get it since it will be expired
    it->SkipTo(it, doc);
    ASSERT_EQ(it->lastDocId, doc + 1);
  }
  it->Free(it);
  SearchCtx_Free(sctx);
  IndexSpec_RemoveFromGlobals(spec->own_ref, true);
  args.clear();
  RedisModule_FreeThreadSafeContext(ctx);
}

// Helper: build a one-entry FieldExpiration array on field index 0. Ownership
// transfers to the table on TimeToLiveTable_Add.
static arrayof(FieldExpiration) makeFE(t_expirationTimePoint p) {
  arrayof(FieldExpiration) fe = array_new(FieldExpiration, 1);
  FieldExpiration entry = {0, p};
  array_append(fe, entry);
  return fe;
}

// Returns true if field 0 is expired for the given docId. Mirrors what the
// VerifyDocAndField slow path observes during query iteration.
static bool fieldExpired(TimeToLiveTable *ttl, t_docId d, const struct timespec *now) {
  return !TimeToLiveTable_VerifyDocAndField(ttl, d, 0, FIELD_EXPIRATION_PREDICATE_DEFAULT, now);
}

// Exercises the direct-modulo + contiguous-vec chain implementation of
// TimeToLiveTable: seed it with more docIds than the bucket cap so every slot
// carries multiple entries, then verify each docId's expiration state is
// observed correctly. Also exercises removal from the middle of a chain via
// the swap-last path.
TEST_F(ExpireTest, testTTLCollisionChain) {
  // Force a small cap so the chain path is actually exercised at this size.
  const size_t savedMax = RSGlobalConfig.maxDocTableSize;
  const size_t cap = 32;
  RSGlobalConfig.maxDocTableSize = cap;

  TimeToLiveTable *ttl = nullptr;
  TimeToLiveTable_VerifyInit(&ttl, cap);

  // Insert 4x the cap so every slot has ~4 entries on average. Alternate
  // expired/fresh so we can assert the chain walk returns the right entry.
  const t_docId N = (t_docId)cap * 4;
  struct timespec past = {1, 0};
  struct timespec future = {LONG_MAX, LONG_MAX};
  for (t_docId d = 1; d <= N; ++d) {
    TimeToLiveTable_Add(ttl, d, makeFE((d & 1) ? past : future));
  }

  struct timespec now = {2, 0};
  for (t_docId d = 1; d <= N; ++d) {
    const bool expected = (d & 1) != 0;
    ASSERT_EQ(fieldExpired(ttl, d, &now), expected) << "docId=" << d;
  }

  // Remove every third docId and re-verify. This deletes from arbitrary chain
  // positions and triggers the swap-last codepath repeatedly.
  for (t_docId d = 3; d <= N; d += 3) {
    TimeToLiveTable_Remove(ttl, d);
  }
  for (t_docId d = 1; d <= N; ++d) {
    if (d % 3 == 0) {
      // Removed entries are absent => VerifyDocAndField treats the field as
      // valid, i.e. fieldExpired returns false.
      ASSERT_FALSE(fieldExpired(ttl, d, &now)) << "docId=" << d;
    } else {
      const bool expected = (d & 1) != 0;
      ASSERT_EQ(fieldExpired(ttl, d, &now), expected) << "docId=" << d;
    }
  }

  // Drain the rest and verify the table reports empty so the lifecycle
  // mechanism in DocTable_Pop can destroy it.
  for (t_docId d = 1; d <= N; ++d) {
    if (d % 3 != 0) {
      TimeToLiveTable_Remove(ttl, d);
    }
  }
  ASSERT_TRUE(TimeToLiveTable_IsEmpty(ttl));

  TimeToLiveTable_Destroy(&ttl);
  ASSERT_EQ(ttl, nullptr);
  RSGlobalConfig.maxDocTableSize = savedMax;
}

// Exercises the docId wrap at the cap boundary: docId < cap uses slot = docId,
// while docId >= cap uses slot = docId % cap. The two branches must land on
// the same bucket and stay distinguishable from each other.
TEST_F(ExpireTest, testTTLDocIdWrap) {
  const size_t savedMax = RSGlobalConfig.maxDocTableSize;
  const t_docId CAP = 32;
  RSGlobalConfig.maxDocTableSize = CAP;

  TimeToLiveTable *ttl = nullptr;
  TimeToLiveTable_VerifyInit(&ttl, CAP);
  // Collide docId X (fast path) with docId X + CAP and X + 2*CAP (modulo path).
  struct timespec past = {1, 0};
  struct timespec future = {LONG_MAX, LONG_MAX};
  struct timespec now = {2, 0};
  for (t_docId x = 1; x < 8; ++x) {
    TimeToLiveTable_Add(ttl, x, makeFE(past));
    TimeToLiveTable_Add(ttl, x + CAP, makeFE(future));
    TimeToLiveTable_Add(ttl, x + 2 * CAP, makeFE(past));
  }
  for (t_docId x = 1; x < 8; ++x) {
    ASSERT_TRUE(fieldExpired(ttl, x, &now));
    ASSERT_FALSE(fieldExpired(ttl, x + CAP, &now));
    ASSERT_TRUE(fieldExpired(ttl, x + 2 * CAP, &now));
    // A docId that hashes to the same slot but was never inserted must report
    // "no TTL" => fieldExpired returns false without matching another entry.
    ASSERT_FALSE(fieldExpired(ttl, x + 3 * CAP, &now));
  }

  // Remove the "middle" entries and confirm the remaining two are still
  // found in both directions (the swap-last must not corrupt the chain).
  for (t_docId x = 1; x < 8; ++x) {
    TimeToLiveTable_Remove(ttl, x + CAP);
  }
  for (t_docId x = 1; x < 8; ++x) {
    ASSERT_TRUE(fieldExpired(ttl, x, &now));
    ASSERT_FALSE(fieldExpired(ttl, x + CAP, &now));
    ASSERT_TRUE(fieldExpired(ttl, x + 2 * CAP, &now));
  }

  TimeToLiveTable_Destroy(&ttl);
  RSGlobalConfig.maxDocTableSize = savedMax;
}

// Exercises lazy bucket-array growth: with a large configured maxSize, a
// table that only ever holds a handful of entries must not allocate the
// full maxSize worth of buckets. Also verifies that the grown `cap` covers
// every slot that has been written, and that wrap-around docIds are still
// routed to their correct (already-allocated) slot after growth.
TEST_F(ExpireTest, testTTLLazyGrowth) {
  const size_t savedMax = RSGlobalConfig.maxDocTableSize;
  const size_t MAX = 1000000;  // matches production default
  RSGlobalConfig.maxDocTableSize = MAX;

  TimeToLiveTable *ttl = nullptr;
  TimeToLiveTable_VerifyInit(&ttl, MAX);
  // Init alone must not allocate any buckets.
  ASSERT_EQ(TimeToLiveTable_DebugAllocatedBuckets(ttl), 0u);

  struct timespec past = {1, 0};
  struct timespec now = {2, 0};

  // Insert a small, sparse set of low docIds. Growth should stop well below
  // MAX; concretely, the high-water slot is 100, so cap is bounded by the
  // geometric growth curve's next step above that, which is a small factor
  // of 100 — nowhere near MAX.
  const t_docId small_ids[] = {1, 5, 42, 100};
  for (t_docId d : small_ids) {
    TimeToLiveTable_Add(ttl, d, makeFE(past));
  }
  const size_t cap_after_small = TimeToLiveTable_DebugAllocatedBuckets(ttl);
  ASSERT_GE(cap_after_small, 101u);   // must cover slot 100
  ASSERT_LT(cap_after_small, MAX / 10);  // must be far below maxSize

  // Reads for docIds whose slot is still unallocated must report not-found.
  ASSERT_FALSE(fieldExpired(ttl, 999999, &now));
  // Writes must still work for those, and bump cap to cover them.
  TimeToLiveTable_Add(ttl, 999999, makeFE(past));
  ASSERT_GE(TimeToLiveTable_DebugAllocatedBuckets(ttl), 999999u + 1);
  ASSERT_TRUE(fieldExpired(ttl, 999999, &now));

  // Previously-inserted entries must not have moved during the grow.
  for (t_docId d : small_ids) {
    ASSERT_TRUE(fieldExpired(ttl, d, &now)) << "docId=" << d;
  }

  // A wrap-around docId (>= maxSize) routes via modulo into an already-
  // allocated slot, so it works without further growth.
  const size_t cap_before_wrap = TimeToLiveTable_DebugAllocatedBuckets(ttl);
  TimeToLiveTable_Add(ttl, MAX + 5, makeFE(past));  // slot = 5, already in range
  ASSERT_EQ(TimeToLiveTable_DebugAllocatedBuckets(ttl), cap_before_wrap);
  ASSERT_TRUE(fieldExpired(ttl, MAX + 5, &now));
  // The original docId=5 entry must still be distinct from the wrapped one.
  ASSERT_TRUE(fieldExpired(ttl, 5, &now));

  TimeToLiveTable_Destroy(&ttl);
  RSGlobalConfig.maxDocTableSize = savedMax;
}

// Exercises TimeToLiveTable_GetFieldExpirations, the borrowed read used by
// the indexer to recover ownership-transferred FieldExpiration arrays after
// DocTable_UpdateExpiration has consumed the original Document pointer.
TEST_F(ExpireTest, testTTLGetFieldExpirations) {
  TimeToLiveTable *ttl = nullptr;
  TimeToLiveTable_VerifyInit(&ttl, 32);

  // Empty table: any docId returns NULL.
  ASSERT_EQ(TimeToLiveTable_GetFieldExpirations(ttl, 1), nullptr);
  ASSERT_EQ(TimeToLiveTable_GetFieldExpirations(ttl, 999999), nullptr);

  // Add an entry and confirm Get returns the same pointer that was inserted,
  // with the expected length and content. (Add takes ownership; Get returns
  // a borrowed alias to that same storage.)
  struct timespec p = {123, 456};
  arrayof(FieldExpiration) inserted = makeFE(p);
  const FieldExpiration *insertedPtr = inserted;  // capture before Add transfers
  TimeToLiveTable_Add(ttl, 7, inserted);

  const arrayof(FieldExpiration) got = TimeToLiveTable_GetFieldExpirations(ttl, 7);
  ASSERT_EQ(got, insertedPtr);
  // array_len takes void*; the const-qualified return needs the cast.
  ASSERT_EQ(array_len((arrayof(FieldExpiration))got), 1u);
  ASSERT_EQ(got[0].index, 0);
  ASSERT_EQ(got[0].point.tv_sec, p.tv_sec);
  ASSERT_EQ(got[0].point.tv_nsec, p.tv_nsec);

  // A docId that was never added must report NULL even when other entries exist.
  ASSERT_EQ(TimeToLiveTable_GetFieldExpirations(ttl, 8), nullptr);

  // After Remove, Get for that docId returns NULL.
  TimeToLiveTable_Remove(ttl, 7);
  ASSERT_EQ(TimeToLiveTable_GetFieldExpirations(ttl, 7), nullptr);

  TimeToLiveTable_Destroy(&ttl);
}

