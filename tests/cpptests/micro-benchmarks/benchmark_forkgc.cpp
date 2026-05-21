/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

/**
 * Google Benchmark suite for the ForkGC scanners.
 *
 * Each benchmark measures one full GC cycle: fork(2) + child scan (trie/tree
 * iteration, scan_gc, pipe write) + parent apply (pipe read, spec write-lock
 * per item, apply_gc, post-apply cleanup).
 *
 * The setup/reset work (adding and deleting documents) is placed inside
 * state.PauseTiming() so it is not included in the measured time. The spec is
 * fully recreated on every iteration so the doc-table bucket array (which
 * grows with maxDocId) is bounded to the current iteration's n docs, keeping
 * fork(2) page-table overhead constant across iterations. The timed region is
 * a direct call to periodicCallback, matching production where the callback runs on
 * the main thread.
 */

#include "benchmark/benchmark.h"
#include "redismock/util.h"
#include "redismock/internal.h"

#include "common.h"
#include "index_utils.h"
#include "notifications.h"
#include "fork_gc.h"
#include "info/global_stats.h"
#include "module.h"
#include "version.h"
#include "ext/default.h"

#include <string>

// ============================================================================
// Base fixture
// ============================================================================

class BM_ForkGC : public benchmark::Fixture {
public:
  void SetUp(benchmark::State &) override {
    m_ctx = RedisModule_GetThreadSafeContext(NULL);
  }

  void TearDown(benchmark::State &) override {
    DropSpec();
    RedisModule_FreeThreadSafeContext(m_ctx);
    m_ctx = nullptr;
  }

protected:
  RedisModuleCtx *m_ctx = nullptr;
  RefManager *ism = nullptr;

  void DropSpec() {
    if (!ism) return;
    freeSpec(ism);
    ism = nullptr;
  }

  // Called at the start of each iteration's PauseTiming() region.
  // Drops the previous spec and creates a fresh one so the doc-table bucket
  // array (sized to maxDocId) never accumulates entries across iterations,
  // keeping fork(2) page-table overhead constant.
  void ResetForIteration() {
    DropSpec();
    RSGlobalStats.totalStats.logically_deleted = 0;
    ism = createSpec(m_ctx);
  }

  // One full timed GC cycle: fork + child scan + pipe + parent apply.
  void RunGcCycle() {
    auto *gc = get_spec(ism)->gc;
    gc->callbacks.periodicCallback(gc->gcCtx, true);
  }

  // Add `count` fresh docs each with a unique term, then delete them.
  void AddAndDeleteDocs_Text(int count, const char *field) {
    for (int i = 0; i < count; i++) {
      std::string id = numToDocStr(i);
      std::string term = "term" + std::to_string(i);
      addDocumentWrapper(m_ctx, ism, id.c_str(), field, term.c_str());
      RediSearch_DeleteDocument(ism, id.c_str(), id.size());
    }
  }

  // Add `count` fresh docs cycling through `numTagValues` distinct values,
  // then delete them.  Produces numTagValues inverted indexes each with
  // count/numTagValues entries.
  void AddAndDeleteDocs_Tag(int count, const char *field, int numTagValues) {
    for (int i = 0; i < count; i++) {
      std::string id = numToDocStr(i);
      std::string tag = "tag" + std::to_string(i % numTagValues);
      addDocumentWrapper(m_ctx, ism, id.c_str(), field, tag.c_str());
      RediSearch_DeleteDocument(ism, id.c_str(), id.size());
    }
  }

  // Add `count` fresh docs with sequential numeric values, then delete them.
  void AddAndDeleteDocs_Numeric(int count, const char *field) {
    for (int i = 0; i < count; i++) {
      std::string id = numToDocStr(i);
      addDocumentWrapper(m_ctx, ism, id.c_str(), field, std::to_string(i).c_str());
      RediSearch_DeleteDocument(ism, id.c_str(), id.size());
    }
  }

  // Enable the existingDocs inverted index (populated by writeExistingDocs in
  // indexer.c).
  void EnableExistingDocs() { get_spec(ism)->rule->index_all = true; }

  // Create a text field marked for MISSING indexing.  Any document added
  // without this field gets an entry in spec->missingFieldDict, giving the
  // missing-docs scanner work to do.
  void AddMissingField(const char *name) {
    RediSearch_CreateTextField(ism, name);
    const FieldSpec *fs =
        IndexSpec_GetFieldWithLength(get_spec(ism), name, strlen(name));
    auto *mfs = const_cast<FieldSpec *>(fs);
    mfs->options = static_cast<FieldSpecOptions>(static_cast<int>(mfs->options) | FieldSpec_IndexMissing);
  }
};

// ============================================================================
// Baseline — no scanner work
//
// Empty spec, no documents. Every scanner runs but immediately sends its
// terminator and returns without doing any real work. Measures the irreducible
// per-cycle overhead shared by all benchmarks below: fork(2), pipe creation,
// GIL acquire/release, child terminator writes, parent terminator reads, and
// child reaping. Subtract this from any other benchmark to isolate the cost
// attributable to the scanner itself.
// ============================================================================

BENCHMARK_DEFINE_F(BM_ForkGC, Baseline)(benchmark::State &state) {
  for (auto _ : state) {
    state.PauseTiming();
    ResetForIteration();
    state.ResumeTiming();

    RunGcCycle();
  }
}

// ============================================================================
// Terms scanner
//
// Each doc contributes one unique term → one inverted index entry.
// GC iterates every entry in the trie (FGC_childCollectTerms) and acquires
// the spec write lock once per collected term (FGC_parentHandleTerms).
//
// state.range(0): number of docs = number of distinct terms = number of
//                 lock acquisitions per cycle.
// ============================================================================

BENCHMARK_DEFINE_F(BM_ForkGC, Terms)(benchmark::State &state) {
  int n = state.range(0);
  for (auto _ : state) {
    state.PauseTiming();
    ResetForIteration();
    RediSearch_CreateTextField(ism, "text");
    AddAndDeleteDocs_Text(n, "text");
    state.ResumeTiming();

    RunGcCycle();
  }
}

// ============================================================================
// Tags scanner
//
// Docs spread across K distinct tag values → K inverted indexes each with
// n/K entries. GC iterates the TrieMap (FGC_childCollectTags) and acquires
// the write lock once per tag value (FGC_parentHandleTags).
//
// state.range(0): total docs
// state.range(1): number of distinct tag values = number of lock acquisitions
// ============================================================================

BENCHMARK_DEFINE_F(BM_ForkGC, Tags)(benchmark::State &state) {
  int n = state.range(0);
  int numTagValues = state.range(1);
  for (auto _ : state) {
    state.PauseTiming();
    ResetForIteration();
    RediSearch_CreateTagField(ism, "tag");
    AddAndDeleteDocs_Tag(n, "tag", numTagValues);
    state.ResumeTiming();

    RunGcCycle();
  }
}

// ============================================================================
// Numeric scanner
//
// Sequential values spread across the range tree. GC runs NumericGcScanner
// (DFS traversal + per-node msgpack/HLL serialisation) in the child and
// NumericRangeTree_ApplyGcEntry + CompactIfSparse in the parent.
//
// state.range(0): number of docs.
// ============================================================================

BENCHMARK_DEFINE_F(BM_ForkGC, Numeric)(benchmark::State &state) {
  int n = state.range(0);
  for (auto _ : state) {
    state.PauseTiming();
    ResetForIteration();
    RediSearch_CreateNumericField(ism, "num");
    AddAndDeleteDocs_Numeric(n, "num");
    state.ResumeTiming();

    RunGcCycle();
  }
}

// ============================================================================
// ExistingDocs scanner
//
// spec->existingDocs is a single DocIdsOnly inverted index that records every
// live document.  It is only populated when rule->index_all is true (wildcard
// query support).  GC scans it in FGC_childCollectExistingDocs and applies the
// delta in FGC_parentHandleExistingDocs — one lock acquisition per cycle.
//
// A single-value tag field ("data") is used to add documents so that the tag
// scanner also contributes one lock acquisition per cycle and O(N) scan work,
// matching the existingDocs scanner's pattern.  Using a unique-term text field
// would add N term-scanner lock acquisitions that would swamp the one
// existingDocs acquisition being measured.
//
// state.range(0): number of docs (= entries in existingDocs per cycle).
// ============================================================================

BENCHMARK_DEFINE_F(BM_ForkGC, ExistingDocs)(benchmark::State &state) {
  int n = state.range(0);
  for (auto _ : state) {
    state.PauseTiming();
    ResetForIteration();
    EnableExistingDocs();
    RediSearch_CreateTagField(ism, "data");
    AddAndDeleteDocs_Tag(n, "data", /*numTagValues=*/1);
    state.ResumeTiming();

    RunGcCycle();
  }
}

// ============================================================================
// MissingDocs scanner
//
// spec->missingFieldDict holds one DocIdsOnly inverted index per schema field
// that was created with FieldSpec_IndexMissing.  Any document that does not
// contain such a field gets an entry in its inverted index.  GC iterates the
// dict in FGC_childCollectMissingDocs and acquires the spec write lock once
// per field in FGC_parentHandleMissingDocs.
//
// A single-value tag field ("data") is used to index documents so that the tag
// scanner contributes one constant lock acquisition per cycle, making M the
// dominant variable.  Using a unique-term text field would add N term-scanner
// lock acquisitions, swamping the M missing-doc acquisitions being measured.
//
// Each doc is added with only the "data" tag; all M "required_i" text fields
// are absent, so each doc contributes one entry to each of the M missing-field
// inverted indexes.
//
// state.range(0): number of docs (= entries per missing-field inverted index).
// state.range(1): number of MISSING fields (= number of inverted indexes
//                 = number of lock acquisitions per cycle from missing-docs).
// ============================================================================

BENCHMARK_DEFINE_F(BM_ForkGC, MissingDocs)(benchmark::State &state) {
  int n = state.range(0);
  int numMissingFields = state.range(1);
  for (auto _ : state) {
    state.PauseTiming();
    ResetForIteration();
    RediSearch_CreateTagField(ism, "data");
    for (int i = 0; i < numMissingFields; i++) {
      AddMissingField(("required" + std::to_string(i)).c_str());
    }
    AddAndDeleteDocs_Tag(n, "data", /*numTagValues=*/1);
    state.ResumeTiming();

    RunGcCycle();
  }
}

// ============================================================================
// Registration
// ============================================================================

BENCHMARK_REGISTER_F(BM_ForkGC, Baseline);
BENCHMARK_REGISTER_F(BM_ForkGC, ExistingDocs)->Arg(100);
BENCHMARK_REGISTER_F(BM_ForkGC, MissingDocs)->Args({100, 3});
BENCHMARK_REGISTER_F(BM_ForkGC, Numeric)->Arg(100);
BENCHMARK_REGISTER_F(BM_ForkGC, Tags)->Args({100, 10});
BENCHMARK_REGISTER_F(BM_ForkGC, Terms)->Arg(10);

// ============================================================================
// main — custom instead of BENCHMARK_MAIN() for three reasons:
//
//  1. Module bootstrap: RMCK_Bootstrap + RediSearch_InitModuleInternal must
//     run before any benchmark fixture is instantiated.  BENCHMARK_MAIN()
//     expands to a fixed main() with no hook for pre-run setup.
//
//  2. NOGC: passing "NOGC" to moduleOnLoad sets enableGC=0, preventing
//     GCContext_Start from being called when specs are created.  Without it,
//     the GC threadpool would race with benchmark iterations.
//
//  3. Teardown ordering: RMCK_Shutdown() and RediSearch_CleanupModule() must
//     run exactly once, after ::benchmark::Shutdown() has finished writing
//     output.  TearDown() runs after each individual benchmark run and cannot
//     be used for global teardown.
// ============================================================================

static int moduleOnLoad(RedisModuleCtx *ctx, RedisModuleString ** /*argv*/, int /*argc*/) {
  if (RedisModule_Init(ctx, REDISEARCH_MODULE_NAME, REDISEARCH_MODULE_VERSION,
                       REDISMODULE_APIVER_1) == REDISMODULE_ERR) {
    return REDISMODULE_ERR;
  }
  RSGlobalConfig.defaultScorer = rm_strdup(DEFAULT_SCORER_NAME);
  return RediSearch_InitModuleInternal(ctx);
}

int main(int argc, char **argv) {
  RMCK_Bootstrap(moduleOnLoad, nullptr, 0);
  RMCK_LogLevel = 3; // LL_WARNING: suppress debug/verbose/notice output
  RSGlobalConfig.freeResourcesThread = false;
  RSGlobalConfig.gcConfigParams.enableGC = false; // prevent GCContext_Start; mock has no timer
  Initialize_KeyspaceNotifications();

  ::benchmark::Initialize(&argc, argv);
  if (::benchmark::ReportUnrecognizedArguments(argc, argv)) return 1;
  ::benchmark::RunSpecifiedBenchmarks();
  ::benchmark::Shutdown();

  RMCK_Shutdown();
  RediSearch_CleanupModule(NULL);
  return 0;
}
