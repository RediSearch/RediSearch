/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#include "gtest/gtest.h"
#include "common.h"
#include "module.h"
#include "concurrent_ctx.h"
extern "C" {
#include "search_disk.h"
extern RedisSearchDiskAPI* disk;
extern RedisSearchDisk* disk_db;
}
#include <atomic>
#include <thread>
#include <unistd.h>

TEST(DiskMetricsLifecycle, CleanupStopsCollectionBeforeIndexesAndClosesAfterOtherWorkers) {
  GTEST_FLAG_SET(death_test_style, "threadsafe");
  ASSERT_EXIT(
      {
        struct State {
          bool stopped = false;
          bool closed = false;
          std::atomic<bool> otherWorkerFinished{false};
        } state;
        static State* active;
        active = &state;
        RedisSearchDiskAPI api{};
        api.metrics.stopMetrics = [](RedisSearchDisk*) {
          if (!active->stopped) {
            if (!specDict_g || dictSize(specDict_g) == 0) _exit(2);
            active->stopped = true;
          }
        };
        api.basic.close = [](RedisModuleCtx*, RedisSearchDisk*) {
          active->closed =
              active->stopped && active->otherWorkerFinished.load() && specDict_g == nullptr;
        };
        disk = &api;
        disk_db = reinterpret_cast<RedisSearchDisk*>(&state);
        ConcurrentSearch_CreatePool(1);
        ConcurrentSearch_ThreadPoolRun([](void*) { active->otherWorkerFinished.store(true); },
                                       nullptr);
        RMCK::ArgvList args(RSDummyContext, "FT.CREATE", "cleanup_metrics", "SCHEMA", "t", "TEXT");
        QueryError error = QueryError_Default();
        if (!Indexes_CreateNewSpec(RSDummyContext, args, args.size(), &error)) _exit(1);
        RedisModule_ThreadSafeContextLock(nullptr);
        RediSearch_CleanupModule(nullptr);
        RedisModule_ThreadSafeContextUnlock(nullptr);
        _exit(state.stopped && state.closed ? 0 : 1);
      },
      ::testing::ExitedWithCode(0), "");
}
