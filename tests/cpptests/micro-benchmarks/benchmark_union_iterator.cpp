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

template <bool quickExit>
class BM_UnionIterator : public benchmark::Fixture {
public:
    std::vector<std::vector<t_docId>> childrenIds;
    QueryIterator *ui_base;
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

        QueryIterator **children = createChildren();
        ui_base = NewUnionIterator(children, childrenIds.size(), quickExit,
                                  1.0, QN_UNION, NULL, &RSGlobalConfig.iteratorsConfigParams);
    }

    void TearDown(::benchmark::State &state) {
        ui_base->Free(ui_base);
    }

    QueryIterator **createChildren() {
        QueryIterator **children = (QueryIterator **)rm_malloc(sizeof(QueryIterator *) * childrenIds.size());
        for (size_t i = 0; i < childrenIds.size(); i++) {
            children[i] = (QueryIterator *)new MockIterator(childrenIds[i]);
        }
        return children;
    }
};
template <bool quickExit>
bool BM_UnionIterator<quickExit>::initialized = false;
// Translation - exponential range from 2 to 20 (double each time), then 25, 50, 75, and 100.
// This is the number of child iterators in each scenario
#define UNION_SCENARIOS() RangeMultiplier(2)->Range(2, 20)->DenseRange(25, 100, 25)

BENCHMARK_TEMPLATE1_DEFINE_F(BM_UnionIterator, ReadFull, false)(benchmark::State &state) {
    for (auto _ : state) {
        IteratorStatus rc = ui_base->Read(ui_base);
        if (rc == ITERATOR_EOF) {
            ui_base->Rewind(ui_base);
        }
    }
}

BENCHMARK_TEMPLATE1_DEFINE_F(BM_UnionIterator, ReadQuick, true)(benchmark::State &state) {
    for (auto _ : state) {
        IteratorStatus rc = ui_base->Read(ui_base);
        if (rc == ITERATOR_EOF) {
            ui_base->Rewind(ui_base);
        }
    }
}

BENCHMARK_TEMPLATE1_DEFINE_F(BM_UnionIterator, SkipToFull, false)(benchmark::State &state) {
    t_offset step = 10;
    for (auto _ : state) {
        IteratorStatus rc = ui_base->SkipTo(ui_base, ui_base->lastDocId + step);
        if (rc == ITERATOR_EOF) {
            ui_base->Rewind(ui_base);
        }
    }
}

BENCHMARK_TEMPLATE1_DEFINE_F(BM_UnionIterator, SkipToQuick, true)(benchmark::State &state) {
    t_offset step = 10;
    for (auto _ : state) {
        IteratorStatus rc = ui_base->SkipTo(ui_base, ui_base->lastDocId + step);
        if (rc == ITERATOR_EOF) {
            ui_base->Rewind(ui_base);
        }
    }
}

BENCHMARK_REGISTER_F(BM_UnionIterator, ReadFull)->UNION_SCENARIOS();
BENCHMARK_REGISTER_F(BM_UnionIterator, ReadQuick)->UNION_SCENARIOS();
BENCHMARK_REGISTER_F(BM_UnionIterator, SkipToFull)->UNION_SCENARIOS();
BENCHMARK_REGISTER_F(BM_UnionIterator, SkipToQuick)->UNION_SCENARIOS();

BENCHMARK_MAIN();
