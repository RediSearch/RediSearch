/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "benchmark/benchmark.h"
#include "iterator_util.h"
#include "redismock/util.h"

#include <random>
#include <vector>

#include "src/iterators/iterator_api.h"
#include "src/iterators/union_iterator.h"

#include "deprecated_iterator_util.h"
#include "src/index.h"

class BM_UnionIterator : public benchmark::Fixture {
public:
    std::vector<std::vector<t_docId>> childrenIds;
    static bool initialized;

    void SetUp(::benchmark::State &state) {
        if (!initialized) {
            RMCK::init();
            initialized = true;
        }

        auto numChildren = state.range(0);
        auto idDistributionType = state.range(1); // 0 = consecutive, 1 = jumps of 100

        childrenIds.resize(numChildren);
        for (int i = 0; i < numChildren; ++i) {
            childrenIds[i].resize(10'000); // Reduced size for clearer performance differences

            // Each child iterator gets a unique base range to avoid overlaps
            t_docId baseOffset = i * 1'000'000;

            for (size_t j = 0; j < childrenIds[i].size(); ++j) {
                if (idDistributionType == 0) {
                    // Consecutive IDs scenario
                    childrenIds[i][j] = baseOffset + j + 1;
                } else {
                    // Jumps of 100 scenario
                    childrenIds[i][j] = baseOffset + (j + 1) * 100;
                }
            }

            // Sort to ensure IDs are in order (though they should already be)
            std::sort(childrenIds[i].begin(), childrenIds[i].end());
        }
    }

    QueryIterator **createChildren() {
        QueryIterator **children = (QueryIterator **)rm_malloc(sizeof(QueryIterator *) * childrenIds.size());
        for (size_t i = 0; i < childrenIds.size(); i++) {
            children[i] = (QueryIterator *)new MockIterator(childrenIds[i]);
        }
        return children;
    }
    IndexIterator **createChildrenOld() {
        IndexIterator **children = (IndexIterator **)rm_malloc(sizeof(IndexIterator *) * childrenIds.size());
        for (size_t i = 0; i < childrenIds.size(); i++) {
            children[i] = (IndexIterator *)new MockOldIterator(childrenIds[i]);
        }
        return children;
    }
};
bool BM_UnionIterator::initialized = false;
// First parameter: number of child iterators (2, 4, 8, 16)
// Second parameter: ID distribution type (0 = consecutive, 1 = jumps of 100)
#define UNION_SCENARIOS() \
    Args({2, 0})->Args({2, 1})-> \
    Args({4, 0})->Args({4, 1})-> \
    Args({8, 0})->Args({8, 1})-> \
    Args({16, 0})->Args({16, 1})

// Benchmark union iterator Read() with full result collection
// Tests performance difference between consecutive vs sparse (jumps of 100) document IDs
BENCHMARK_DEFINE_F(BM_UnionIterator, ReadFull)(benchmark::State &state) {
    QueryIterator **children = createChildren();
    QueryIterator *ui_base = IT_V2(NewUnionIterator)(children, childrenIds.size(), false,
                                                    1.0, QN_UNION, NULL, &RSGlobalConfig.iteratorsConfigParams);
    for (auto _ : state) {
        IteratorStatus rc = ui_base->Read(ui_base);
        if (rc == ITERATOR_EOF) {
            ui_base->Rewind(ui_base);
        }
    }

    ui_base->Free(ui_base);
}

// Benchmark union iterator Read() with quick exit (first match only)
// Tests performance difference between consecutive vs sparse (jumps of 100) document IDs
BENCHMARK_DEFINE_F(BM_UnionIterator, ReadQuick)(benchmark::State &state) {
    QueryIterator **children = createChildren();
    QueryIterator *ui_base = IT_V2(NewUnionIterator)(children, childrenIds.size(), true,
                                                    1.0, QN_UNION, NULL, &RSGlobalConfig.iteratorsConfigParams);
    for (auto _ : state) {
        IteratorStatus rc = ui_base->Read(ui_base);
        if (rc == ITERATOR_EOF) {
            ui_base->Rewind(ui_base);
        }
    }

    ui_base->Free(ui_base);
}

// Benchmark union iterator SkipTo() with full result collection
// Tests random access performance difference between consecutive vs sparse document IDs
BENCHMARK_DEFINE_F(BM_UnionIterator, SkipToFull)(benchmark::State &state) {
    QueryIterator **children = createChildren();
    QueryIterator *ui_base = IT_V2(NewUnionIterator)(children, childrenIds.size(), false,
                                                    1.0, QN_UNION, NULL, &RSGlobalConfig.iteratorsConfigParams);
    t_offset step = 10;
    for (auto _ : state) {
        IteratorStatus rc = ui_base->SkipTo(ui_base, ui_base->lastDocId + step);
        if (rc == ITERATOR_EOF) {
            ui_base->Rewind(ui_base);
        }
    }

    ui_base->Free(ui_base);
}

// Benchmark union iterator SkipTo() with quick exit (first match only)
// Tests random access performance difference between consecutive vs sparse document IDs
BENCHMARK_DEFINE_F(BM_UnionIterator, SkipToQuick)(benchmark::State &state) {
    QueryIterator **children = createChildren();
    QueryIterator *ui_base = IT_V2(NewUnionIterator)(children, childrenIds.size(), true,
                                                    1.0, QN_UNION, NULL, &RSGlobalConfig.iteratorsConfigParams);
    t_offset step = 10;
    for (auto _ : state) {
        IteratorStatus rc = ui_base->SkipTo(ui_base, ui_base->lastDocId + step);
        if (rc == ITERATOR_EOF) {
            ui_base->Rewind(ui_base);
        }
    }

    ui_base->Free(ui_base);
}

BENCHMARK_REGISTER_F(BM_UnionIterator, ReadFull)->UNION_SCENARIOS();
BENCHMARK_REGISTER_F(BM_UnionIterator, ReadQuick)->UNION_SCENARIOS();
BENCHMARK_REGISTER_F(BM_UnionIterator, SkipToFull)->UNION_SCENARIOS();
BENCHMARK_REGISTER_F(BM_UnionIterator, SkipToQuick)->UNION_SCENARIOS();

BENCHMARK_DEFINE_F(BM_UnionIterator, ReadFull_old)(benchmark::State &state) {
    IndexIterator **children = createChildrenOld();
    IndexIterator *ui_base = NewUnionIterator(children, childrenIds.size(), false,
                                                1.0, QN_UNION, NULL, &RSGlobalConfig.iteratorsConfigParams);
    RSIndexResult *hit;
    for (auto _ : state) {
        int rc = ui_base->Read(ui_base->ctx, &hit);
        if (rc == INDEXREAD_EOF) {
            ui_base->Rewind(ui_base->ctx);
        }
    }

    ui_base->Free(ui_base);
}

BENCHMARK_DEFINE_F(BM_UnionIterator, ReadQuick_old)(benchmark::State &state) {
    IndexIterator **children = createChildrenOld();
    IndexIterator *ui_base = NewUnionIterator(children, childrenIds.size(), true,
                                                1.0, QN_UNION, NULL, &RSGlobalConfig.iteratorsConfigParams);
    RSIndexResult *hit;
    for (auto _ : state) {
        int rc = ui_base->Read(ui_base->ctx, &hit);
        if (rc == INDEXREAD_EOF) {
            ui_base->Rewind(ui_base->ctx);
        }
    }

    ui_base->Free(ui_base);
}

BENCHMARK_DEFINE_F(BM_UnionIterator, SkipToFull_old)(benchmark::State &state) {
    IndexIterator **children = createChildrenOld();
    IndexIterator *ui_base = NewUnionIterator(children, childrenIds.size(), false,
                                                1.0, QN_UNION, NULL, &RSGlobalConfig.iteratorsConfigParams);
    RSIndexResult *hit = ui_base->current;
    hit->docId = 0; // Ensure initial docId is set to 0
    t_offset step = 10;
    for (auto _ : state) {
        int rc = ui_base->SkipTo(ui_base->ctx, hit->docId + step, &hit);
        if (rc == INDEXREAD_EOF) {
            ui_base->Rewind(ui_base->ctx);
            // Don't rely on the old iterator's Rewind to reset hit->docId
            hit = ui_base->current;
            hit->docId = 0;
        }
    }

    ui_base->Free(ui_base);
}

BENCHMARK_DEFINE_F(BM_UnionIterator, SkipToQuick_old)(benchmark::State &state) {
    IndexIterator **children = createChildrenOld();
    IndexIterator *ui_base = NewUnionIterator(children, childrenIds.size(), true,
                                                1.0, QN_UNION, NULL, &RSGlobalConfig.iteratorsConfigParams);
    RSIndexResult *hit = ui_base->current;
    hit->docId = 0; // Ensure initial docId is set to 0
    t_offset step = 10;
    for (auto _ : state) {
        int rc = ui_base->SkipTo(ui_base->ctx, hit->docId + step, &hit);
        if (rc == INDEXREAD_EOF) {
            ui_base->Rewind(ui_base->ctx);
            // Don't rely on the old iterator's Rewind to reset hit->docId
            hit = ui_base->current;
            hit->docId = 0;
        }
    }

    ui_base->Free(ui_base);
}

BENCHMARK_REGISTER_F(BM_UnionIterator, ReadFull_old)->UNION_SCENARIOS();
BENCHMARK_REGISTER_F(BM_UnionIterator, ReadQuick_old)->UNION_SCENARIOS();
BENCHMARK_REGISTER_F(BM_UnionIterator, SkipToFull_old)->UNION_SCENARIOS();
BENCHMARK_REGISTER_F(BM_UnionIterator, SkipToQuick_old)->UNION_SCENARIOS();

BENCHMARK_MAIN();
