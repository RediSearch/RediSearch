/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#include "benchmark/benchmark.h"
#include "hybrid/hybrid_executor.h"
#include "redismock/util.h"

extern "C" {
#include "thpool/thpool.h"
}

#include <condition_variable>
#include <cstdint>
#include <mutex>

namespace {

struct Batch {
  redisearch_thpool_t *pool;
  size_t children;
  size_t workers;
  size_t rounds;
  std::mutex mutex;
  std::condition_variable done;
  size_t remaining = 0;
};

void ignoreLog(const char *, const char *, ...) {
}

int submit(void *pool, HybridTicket run, void *ticket) {
  return redisearch_thpool_add_work(static_cast<redisearch_thpool_t *>(pool), run, ticket,
                                    THPOOL_PRIORITY_HIGH);
}

void child(void *ctx, size_t ordinal) {
  auto &batch = *static_cast<Batch *>(ctx);
  uint64_t value = ordinal + 1;
  for (size_t i = 0; i < batch.rounds; ++i) {
    value ^= value << 13;
    value ^= value >> 7;
    value ^= value << 17;
  }
  benchmark::DoNotOptimize(value);
}

void finish(void *ctx) {
  auto &batch = *static_cast<Batch *>(ctx);
  std::lock_guard<std::mutex> guard(batch.mutex);
  if (--batch.remaining == 0) batch.done.notify_one();
}

void startRequest(void *ctx) {
  auto &batch = *static_cast<Batch *>(ctx);
  HybridExecutor_Run(batch.children, batch.workers, submit, batch.pool, child, finish, &batch);
}

void BM_HybridExecutor(benchmark::State &state) {
  RMCK::init();
  const auto children = static_cast<size_t>(state.range(0));
  const auto workers = static_cast<size_t>(state.range(1));
  const auto clients = static_cast<size_t>(state.range(2));
  const auto rounds = static_cast<size_t>(state.range(3));
  auto *pool = redisearch_thpool_create(workers, 1, ignoreLog, "hybrid");
  if (!pool) {
    state.SkipWithError("Unable to create worker pool");
    return;
  }
  Batch batch{pool, children, workers, rounds, {}, {}};
  for (auto _ : state) {
    {
      std::lock_guard<std::mutex> guard(batch.mutex);
      batch.remaining = clients;
    }
    bool rejected = false;
    for (size_t i = 0; i < clients; ++i) {
      if (submit(pool, startRequest, &batch) != 0) {
        rejected = true;
        finish(&batch);
      }
    }
    std::unique_lock<std::mutex> lock(batch.mutex);
    batch.done.wait(lock, [&] { return batch.remaining == 0; });
    if (rejected) {
      state.SkipWithError("Unable to submit initial request");
      break;
    }
  }
  redisearch_thpool_destroy(pool);
  state.SetItemsProcessed(state.iterations() * clients);
}

BENCHMARK(BM_HybridExecutor)
    ->ArgsProduct({{2, 4, 8, 16}, {1, 2, 4}, {1, 32}, {0, 2048}})
    ->ArgNames({"children", "workers", "clients", "rounds_per_child"})
    ->UseRealTime()
    ->MeasureProcessCPUTime()
    ->Unit(benchmark::kMicrosecond);

}  // namespace

BENCHMARK_MAIN();
