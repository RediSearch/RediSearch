/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
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
        std::mt19937 rng(46);
        std::uniform_int_distribution<t_docId> dist(1, 2'000'000);

        childrenIds.resize(numChildren);
        for (int i = 0; i < numChildren; ++i) {
            childrenIds[i].resize(100'000);
            for (auto &id : childrenIds[i]) {
                id = dist(rng);
            }
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
#define UNION_SCENARIOS() RangeMultiplier(2)->Range(2, 20)->DenseRange(25, 100, 25)

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

BENCHMARK_DEFINE_F(BM_UnionIterator, SkipToFull)(benchmark::State &state) {
    QueryIterator **children = createChildren();
    QueryIterator *ui_base = IT_V2(NewUnionIterator)(children, childrenIds.size(), false,
                                                    1.0, QN_UNION, NULL, &RSGlobalConfig.iteratorsConfigParams);
    t_docId docId = 10;
    for (auto _ : state) {
        IteratorStatus rc = ui_base->SkipTo(ui_base, docId);
        docId += 10;
        if (rc == ITERATOR_EOF) {
            ui_base->Rewind(ui_base);
            docId = 10;
        }
    }

    ui_base->Free(ui_base);
}

BENCHMARK_DEFINE_F(BM_UnionIterator, SkipToQuick)(benchmark::State &state) {
    QueryIterator **children = createChildren();
    QueryIterator *ui_base = IT_V2(NewUnionIterator)(children, childrenIds.size(), true,
                                                    1.0, QN_UNION, NULL, &RSGlobalConfig.iteratorsConfigParams);
    t_docId docId = 10;
    for (auto _ : state) {
        IteratorStatus rc = ui_base->SkipTo(ui_base, docId);
        docId += 10;
        if (rc == ITERATOR_EOF) {
            ui_base->Rewind(ui_base);
            docId = 10;
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
    RSIndexResult *hit;
    t_docId docId = 10;
    for (auto _ : state) {
        int rc = ui_base->SkipTo(ui_base->ctx, docId, &hit);
        docId += 10;
        if (rc == INDEXREAD_EOF) {
            ui_base->Rewind(ui_base->ctx);
            docId = 10;
        }
    }

    ui_base->Free(ui_base);
}

BENCHMARK_DEFINE_F(BM_UnionIterator, SkipToQuick_old)(benchmark::State &state) {
    IndexIterator **children = createChildrenOld();
    IndexIterator *ui_base = NewUnionIterator(children, childrenIds.size(), true,
                                                1.0, QN_UNION, NULL, &RSGlobalConfig.iteratorsConfigParams);
    RSIndexResult *hit;
    t_docId docId = 10;
    for (auto _ : state) {
        int rc = ui_base->SkipTo(ui_base->ctx, docId, &hit);
        docId += 10;
        if (rc == INDEXREAD_EOF) {
            ui_base->Rewind(ui_base->ctx);
            docId = 10;
        }
    }

    ui_base->Free(ui_base);
}

BENCHMARK_REGISTER_F(BM_UnionIterator, ReadFull_old)->UNION_SCENARIOS();
BENCHMARK_REGISTER_F(BM_UnionIterator, ReadQuick_old)->UNION_SCENARIOS();
BENCHMARK_REGISTER_F(BM_UnionIterator, SkipToFull_old)->UNION_SCENARIOS();
BENCHMARK_REGISTER_F(BM_UnionIterator, SkipToQuick_old)->UNION_SCENARIOS();

BENCHMARK_MAIN();
