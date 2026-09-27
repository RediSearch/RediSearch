/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#include "gtest/gtest.h"

extern "C" {
#include "util/dict.h"
}
#include "search_ctx.h"
#include "aggregate/aggregate.h"
#include "cursor.h"
#include "rmalloc.h"
#include <future>
#include <stdexcept>
#include <thread>

class SpecLockTest : public ::testing::Test {
 protected:
  IndexSpec spec{};
  dictType type{};

  void SetUp() override {
    ASSERT_EQ(pthread_rwlock_init(&spec.rwlock, nullptr), 0);
    spec.keysDict = dictCreate(&type, nullptr);
  }

  void TearDown() override {
    IndexSpec_AssertLockNotHeld();
    EXPECT_EQ(spec.keysDict->pauserehash, 0);
    EXPECT_EQ(pthread_rwlock_destroy(&spec.rwlock), 0);
    dictRelease(spec.keysDict);
  }
};

TEST_F(SpecLockTest, SharedContextReadersOwnIndependentLocks) {
  const RedisSearchCtx ctx = SEARCH_CTX_STATIC(nullptr, &spec);
  std::promise<void> acquired, release;
  auto releaseFuture = release.get_future();
  IndexSpec_LockRead(ctx.spec);
  std::thread worker([&] {
    EXPECT_FALSE(IndexSpec_IsLocked(ctx.spec));
    IndexSpec_AssertLockNotHeld();
    // A cleanup on a non-owner must leave the original reader's lock intact.
    IndexSpec_Unlock(ctx.spec);
    EXPECT_EQ(IndexSpec_TryLockRead(ctx.spec), REDISMODULE_OK);
    EXPECT_TRUE(IndexSpec_IsReadLocked(ctx.spec));
    acquired.set_value();
    releaseFuture.wait();
    IndexSpec_Unlock(ctx.spec);
    EXPECT_FALSE(IndexSpec_IsLocked(ctx.spec));
    IndexSpec_AssertLockNotHeld();
  });
  acquired.get_future().wait();
  EXPECT_EQ(spec.keysDict->pauserehash, 2);
  IndexSpec_Unlock(ctx.spec);
  EXPECT_FALSE(IndexSpec_IsLocked(ctx.spec));
  EXPECT_EQ(spec.keysDict->pauserehash, 1);
  // The worker still owns its reader, even after the original owner exits.
  EXPECT_NE(pthread_rwlock_trywrlock(&spec.rwlock), 0);
  release.set_value();
  worker.join();
  IndexSpec_LockWrite(&spec);
  IndexSpec_Unlock(&spec);
}

TEST_F(SpecLockTest, FailedTryLockDoesNotInstallOwnership) {
  IndexSpec_LockWrite(&spec);
  std::thread worker([&] {
    EXPECT_EQ(IndexSpec_TryLockRead(&spec), REDISMODULE_ERR);
    EXPECT_FALSE(IndexSpec_IsLocked(&spec));
    IndexSpec_Unlock(&spec);
    IndexSpec_AssertLockNotHeld();
  });
  worker.join();
  EXPECT_TRUE(IndexSpec_IsLocked(&spec));
  EXPECT_FALSE(IndexSpec_IsReadLocked(&spec));
  IndexSpec_Unlock(&spec);
  EXPECT_EQ(IndexSpec_TryLockRead(&spec), REDISMODULE_OK);
  IndexSpec_Unlock(&spec);
}

TEST_F(SpecLockTest, BorrowedScopeKeepsOuterLockUntilReturned) {
  IndexSpec_LockRead(&spec);
  IndexSpec_BorrowReadLock(&spec);
  IndexSpec_Unlock(&spec);
  IndexSpec_Unlock(&spec);
  EXPECT_TRUE(IndexSpec_IsReadLocked(&spec));
  EXPECT_EQ(spec.keysDict->pauserehash, 1);
  std::thread writer([&] {
    EXPECT_FALSE(IndexSpec_IsLocked(&spec));
    EXPECT_NE(pthread_rwlock_trywrlock(&spec.rwlock), 0);
  });
  writer.join();
  IndexSpec_ReturnReadLock(&spec);
  EXPECT_TRUE(IndexSpec_IsReadLocked(&spec));
  IndexSpec_Unlock(&spec);
  IndexSpec_Unlock(&spec);
  EXPECT_FALSE(IndexSpec_IsLocked(&spec));
}

TEST_F(SpecLockTest, ContextCleanupOnAnotherThreadCannotUnlockOwner) {
  RedisSearchCtx ctx = SEARCH_CTX_STATIC(nullptr, &spec);
  IndexSpec_LockRead(&spec);
  std::thread worker([&] { SearchCtx_CleanUp(&ctx); });
  worker.join();
  EXPECT_TRUE(IndexSpec_IsReadLocked(&spec));
  EXPECT_EQ(spec.keysDict->pauserehash, 1);
  SearchCtx_CleanUp(&ctx);
  EXPECT_TRUE(IndexSpec_IsReadLocked(&spec));
  IndexSpec_Unlock(&spec);
}

TEST_F(SpecLockTest, LegacyGCWriteAPIUsesThreadOwnership) {
  IndexSpec_AcquireWriteLock(&spec);
  EXPECT_TRUE(IndexSpec_IsLocked(&spec));
  EXPECT_FALSE(IndexSpec_IsReadLocked(&spec));
  IndexSpec_ReleaseWriteLock(&spec);
  EXPECT_FALSE(IndexSpec_IsLocked(&spec));
}

TEST_F(SpecLockTest, RejectsRecursiveAcquisitionAndMismatchedUnlock) {
  IndexSpec other{};
  IndexSpec_LockRead(&spec);
  EXPECT_THROW(IndexSpec_LockRead(&spec), std::runtime_error);
  EXPECT_THROW(IndexSpec_LockWrite(&other), std::runtime_error);
  EXPECT_THROW(IndexSpec_Unlock(&other), std::runtime_error);
  EXPECT_TRUE(IndexSpec_IsReadLocked(&spec));
  IndexSpec_Unlock(&spec);
}

TEST_F(SpecLockTest, CursorReservationReclaimsIdleRequestsWithoutReleasingActiveLock) {
  IndexSpec other{};
  for (IndexSpec *idleSpec : {&spec, &other}) {
    StrongRef dummy{};
    AREQ *request = AREQ_New(nullptr, 0);
    request->sctx = static_cast<RedisSearchCtx *>(rm_malloc(sizeof(RedisSearchCtx)));
    *request->sctx = SEARCH_CTX_STATIC(nullptr, idleSpec);
    Cursor *idle = Cursors_Reserve(&g_CursorsList, dummy, 1000, nullptr);
    ASSERT_NE(idle, nullptr);
    idle->query = &request->base;
    ASSERT_EQ(Cursor_Pause(idle), REDISMODULE_OK);
    const auto idleId = idle->id;
    // Force the real reservation sweep without a clock-dependent sleep.
    idle->nextTimeoutNs = 0;
    g_CursorsList.nextIdleTimeoutNs = 0;
    g_CursorsList.lastCollect = 0;
    g_CursorsList.counter +=
        RSCURSORS_SWEEP_INTERVAL - g_CursorsList.counter % RSCURSORS_SWEEP_INTERVAL - 1;

    IndexSpec_LockRead(&spec);
    Cursor *active = Cursors_Reserve(&g_CursorsList, dummy, 1000, nullptr);
    EXPECT_NE(active, nullptr);
    EXPECT_TRUE(IndexSpec_IsReadLocked(&spec));
    EXPECT_EQ(spec.keysDict->pauserehash, 1);
    EXPECT_EQ(Cursors_TakeForExecution(&g_CursorsList, idleId), nullptr);
    IndexSpec_Unlock(&spec);
    if (active) Cursor_Free(active);
  }
}
