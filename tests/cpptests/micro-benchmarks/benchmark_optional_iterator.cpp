/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "benchmark/benchmark.h"
#include "redismock/util.h"
#include "iterator_util.h"

#include <random>
#include <vector>

#include "src/iterators/iterator_api.h"
#include "src/iterators/optional_iterator.h"

#include "deprecated_iterator_util.h"
#include "src/index.h"

class MockQueryEvalCtx {
public:
  QueryEvalCtx qctx;
  RedisSearchCtx sctx;
  IndexSpec spec;
  DocTable docTable;
  SchemaRule rule;

  MockQueryEvalCtx(t_docId maxDocId, size_t numDocs, bool optimized = false) {
    // Initialize DocTable
    docTable.maxDocId = maxDocId;
    docTable.size = numDocs;
    
    // Initialize SchemaRule
    rule.index_all = optimized;
    
    // Initialize IndexSpec
    spec.rule = &rule;
    spec.existingDocs = nullptr; // For simplicity in benchmarks
    
    // Initialize RedisSearchCtx
    sctx.spec = &spec;
    
    // Initialize QueryEvalCtx
    qctx.sctx = &sctx;
    qctx.docTable = &docTable;
  }
};

template<bool WithChild>
class BM_OptionalIterator : public benchmark::Fixture {
public:
  std::vector<t_docId> childDocIds;
  static bool initialized;
  const t_docId maxDocId = 100000;
  const size_t numDocs = 50000;
  const double weight = 1.0;
  QueryIterator *iterator_base = nullptr;
  IndexIterator *old_iterator_base = nullptr;
  MockQueryEvalCtx *mockCtx = nullptr;

  void SetUp(::benchmark::State &state) {
    if (!initialized) {
      RMCK::init();
      initialized = true;
    }

    mockCtx = new MockQueryEvalCtx(maxDocId, numDocs);
    iterator_base = createOptionalIterator(state);
    old_iterator_base = createOldOptionalIterator(state);
  }

  void TearDown(::benchmark::State &state) {
    if (iterator_base) {
      iterator_base->Free(iterator_base);
      iterator_base = nullptr;
    }
    if (old_iterator_base) {
      old_iterator_base->Free(old_iterator_base);
      old_iterator_base = nullptr;
    }
    if (mockCtx) {
      delete mockCtx;
      mockCtx = nullptr;
    }
    childDocIds.clear();
  }

  IndexIterator* createOldOptionalIterator(::benchmark::State &state) {
    IndexIterator *oldChild = nullptr;
    if constexpr (WithChild) {
      // Generate child docIds based on benchmark parameters
      const auto numChildDocs = state.range(0);
      childDocIds.resize(numChildDocs);

      std::mt19937 rng(42);
      std::uniform_int_distribution<t_docId> dist(1, maxDocId);

      for (int i = 0; i < numChildDocs; ++i) {
        childDocIds[i] = dist(rng);
      }
      std::sort(childDocIds.begin(), childDocIds.end());
      childDocIds.erase(std::unique(childDocIds.begin(), childDocIds.end()), childDocIds.end());

      oldChild = (IndexIterator *)new MockOldIterator(childDocIds);
    }
    return NewOptionalIterator(oldChild, &mockCtx->qctx, weight);
  }

  // Template to create optional iterator with or without child
  QueryIterator* createOptionalIterator(::benchmark::State &state) {
    QueryIterator *child = nullptr;
    if constexpr (WithChild) {
      // Generate child docIds based on benchmark parameters
      const auto numChildDocs = state.range(0);
      childDocIds.resize(numChildDocs);

      std::mt19937 rng(42);
      std::uniform_int_distribution<t_docId> dist(1, maxDocId);

      for (int i = 0; i < numChildDocs; ++i) {
        childDocIds[i] = dist(rng);
      }
      std::sort(childDocIds.begin(), childDocIds.end());
      childDocIds.erase(std::unique(childDocIds.begin(), childDocIds.end()), childDocIds.end());

      child = (QueryIterator *)new MockIterator(childDocIds);      
    }
    return IT_V2(NewOptionalIterator)(child, maxDocId, numDocs, weight);
  }
};

template<bool WithChild>
bool BM_OptionalIterator<WithChild>::initialized = false;

// Type aliases for cleaner benchmark definitions
using BM_OptionalIteratorEmpty = BM_OptionalIterator<false>;
using BM_OptionalIteratorWithChild = BM_OptionalIterator<true>;
using BM_OldOptionalIteratorEmpty = BM_OptionalIterator<false>;
using BM_OldOptionalIteratorWithChild = BM_OptionalIterator<true>;

// Benchmark scenarios: different numbers of child documents
#define CHILD_DOCS_SCENARIOS() RangeMultiplier(2)->Range(2, 1024)->DenseRange(1500, 5000, 500)

BENCHMARK_DEFINE_F(BM_OptionalIteratorEmpty, ReadEmpty)(benchmark::State &state) {
  for (auto _ : state) {
    IteratorStatus rc = iterator_base->Read(iterator_base);
    if (rc == ITERATOR_EOF) {
      iterator_base->Rewind(iterator_base);
    }
    benchmark::DoNotOptimize(iterator_base->current);
  }
}

BENCHMARK_DEFINE_F(BM_OptionalIteratorWithChild, ReadWithChild)(benchmark::State &state) {
  for (auto _ : state) {
    IteratorStatus rc = iterator_base->Read(iterator_base);
    if (rc == ITERATOR_EOF) {
      iterator_base->Rewind(iterator_base);
    }
    benchmark::DoNotOptimize(iterator_base->current);
  }
}

BENCHMARK_DEFINE_F(BM_OptionalIteratorEmpty, SkipToEmpty)(benchmark::State &state) {
  t_docId docId = 1;
  for (auto _ : state) {
    IteratorStatus rc = iterator_base->SkipTo(iterator_base, docId);
    docId += 100;
    if (rc == ITERATOR_EOF || docId > maxDocId) {
      iterator_base->Rewind(iterator_base);
      docId = 1;
    }
    benchmark::DoNotOptimize(rc);
  }
}

BENCHMARK_DEFINE_F(BM_OptionalIteratorWithChild, SkipToWithChild)(benchmark::State &state) {  
  t_offset step = 10;  
  for (auto _ : state) {  
    IteratorStatus rc = iterator_base->SkipTo(iterator_base, iterator_base->lastDocId + step);  
    if (rc == ITERATOR_EOF) {  
      iterator_base->Rewind(iterator_base);  
    }  
    benchmark::DoNotOptimize(rc);  
  }  
}  

BENCHMARK_DEFINE_F(BM_OptionalIteratorWithChild, SkipToRealHits)(benchmark::State &state) {
  size_t childIndex = 0;
  for (auto _ : state) {
    if (childIndex >= childDocIds.size()) {
      iterator_base->Rewind(iterator_base);
      childIndex = 0;
    }

    t_docId target = childDocIds[childIndex++];
    IteratorStatus rc = iterator_base->SkipTo(iterator_base, target);
    benchmark::DoNotOptimize(rc);
  }
}

BENCHMARK_DEFINE_F(BM_OptionalIteratorWithChild, SkipToVirtualHits)(benchmark::State &state) {
  // Generate docIds that are NOT in childDocIds (virtual hits)
  std::vector<t_docId> virtualDocIds;
  for (t_docId i = 1; i <= maxDocId; i += 137) { // Use prime step to avoid patterns
    if (std::find(childDocIds.begin(), childDocIds.end(), i) == childDocIds.end()) {
      virtualDocIds.push_back(i);
    }
  }

  size_t virtualIndex = 0;
  for (auto _ : state) {
    if (virtualIndex >= virtualDocIds.size()) {
      iterator_base->Rewind(iterator_base);
      virtualIndex = 0;
    }

    t_docId target = virtualDocIds[virtualIndex++];
    IteratorStatus rc = iterator_base->SkipTo(iterator_base, target);
    benchmark::DoNotOptimize(rc);
  }
}

// Old Optional Iterator Benchmarks
BENCHMARK_DEFINE_F(BM_OldOptionalIteratorEmpty, ReadEmpty_NonOptimized)(benchmark::State &state) {
  for (auto _ : state) {
    RSIndexResult *hit = nullptr;
    int rc = old_iterator_base->Read(old_iterator_base->ctx, &hit);
    if (rc == INDEXREAD_EOF) {
      old_iterator_base->Rewind(old_iterator_base->ctx);
    }
    benchmark::DoNotOptimize(hit);
  }
}

BENCHMARK_DEFINE_F(BM_OldOptionalIteratorWithChild, ReadWithChild_NonOptimized)(benchmark::State &state) {
  for (auto _ : state) {
    RSIndexResult *hit = nullptr;
    int rc = old_iterator_base->Read(old_iterator_base->ctx, &hit);
    if (rc == INDEXREAD_EOF) {
      old_iterator_base->Rewind(old_iterator_base->ctx);
    }
    benchmark::DoNotOptimize(hit);
  }
}

BENCHMARK_DEFINE_F(BM_OldOptionalIteratorEmpty, SkipToEmpty_NonOptimized)(benchmark::State &state) {
  t_docId docId = 1;
  for (auto _ : state) {
    RSIndexResult *hit = nullptr;
    int rc = old_iterator_base->SkipTo(old_iterator_base->ctx, docId, &hit);
    docId += 100;
    if (rc == INDEXREAD_EOF || docId > maxDocId) {
      old_iterator_base->Rewind(old_iterator_base->ctx);
      docId = 1;
    }
    benchmark::DoNotOptimize(rc);
  }
}

BENCHMARK_DEFINE_F(BM_OldOptionalIteratorWithChild, SkipToWithChild_NonOptimized)(benchmark::State &state) {
  t_offset step = 10;
  for (auto _ : state) {
    RSIndexResult *hit = nullptr;
    int rc = old_iterator_base->SkipTo(old_iterator_base->ctx, old_iterator_base->LastDocId(old_iterator_base->ctx) + step, &hit);
    if (rc == INDEXREAD_EOF) {
      old_iterator_base->Rewind(old_iterator_base->ctx);
    }
    benchmark::DoNotOptimize(rc);
  }
}

BENCHMARK_DEFINE_F(BM_OldOptionalIteratorWithChild, SkipToRealHits_NonOptimized)(benchmark::State &state) {
  size_t childIndex = 0;
  for (auto _ : state) {
    if (childIndex >= childDocIds.size()) {
      old_iterator_base->Rewind(old_iterator_base->ctx);
      childIndex = 0;
    }

    t_docId target = childDocIds[childIndex++];
    RSIndexResult *hit = nullptr;
    int rc = old_iterator_base->SkipTo(old_iterator_base->ctx, target, &hit);
    benchmark::DoNotOptimize(rc);
  }
}

BENCHMARK_DEFINE_F(BM_OldOptionalIteratorWithChild, SkipToVirtualHits_NonOptimized)(benchmark::State &state) {
  // Generate docIds that are NOT in childDocIds (virtual hits)
  std::vector<t_docId> virtualDocIds;
  for (t_docId i = 1; i <= maxDocId; i += 137) { // Use prime step to avoid patterns
    if (std::find(childDocIds.begin(), childDocIds.end(), i) == childDocIds.end()) {
      virtualDocIds.push_back(i);
    }
  }

  size_t virtualIndex = 0;
  for (auto _ : state) {
    if (virtualIndex >= virtualDocIds.size()) {
      old_iterator_base->Rewind(old_iterator_base->ctx);
      virtualIndex = 0;
    }

    t_docId target = virtualDocIds[virtualIndex++];
    RSIndexResult *hit = nullptr;
    int rc = old_iterator_base->SkipTo(old_iterator_base->ctx, target, &hit);
    benchmark::DoNotOptimize(rc);
  }
}


// Register benchmarks with different child document counts
BENCHMARK_REGISTER_F(BM_OptionalIteratorEmpty, ReadEmpty);
BENCHMARK_REGISTER_F(BM_OptionalIteratorWithChild, ReadWithChild)->CHILD_DOCS_SCENARIOS();
BENCHMARK_REGISTER_F(BM_OptionalIteratorEmpty, SkipToEmpty);
BENCHMARK_REGISTER_F(BM_OptionalIteratorWithChild, SkipToWithChild)->CHILD_DOCS_SCENARIOS();
BENCHMARK_REGISTER_F(BM_OptionalIteratorWithChild, SkipToRealHits)->CHILD_DOCS_SCENARIOS();
BENCHMARK_REGISTER_F(BM_OptionalIteratorWithChild, SkipToVirtualHits)->CHILD_DOCS_SCENARIOS();

// Non-Optimized Version Benchmarks
BENCHMARK_REGISTER_F(BM_OldOptionalIteratorEmpty, ReadEmpty_NonOptimized);
BENCHMARK_REGISTER_F(BM_OldOptionalIteratorWithChild, ReadWithChild_NonOptimized)->CHILD_DOCS_SCENARIOS();
BENCHMARK_REGISTER_F(BM_OldOptionalIteratorEmpty, SkipToEmpty_NonOptimized);
BENCHMARK_REGISTER_F(BM_OldOptionalIteratorWithChild, SkipToWithChild_NonOptimized)->CHILD_DOCS_SCENARIOS();
BENCHMARK_REGISTER_F(BM_OldOptionalIteratorWithChild, SkipToRealHits_NonOptimized)->CHILD_DOCS_SCENARIOS();
BENCHMARK_REGISTER_F(BM_OldOptionalIteratorWithChild, SkipToVirtualHits_NonOptimized)->CHILD_DOCS_SCENARIOS();

BENCHMARK_MAIN();
