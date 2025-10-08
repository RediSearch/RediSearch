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
#include "src/iterators/intersection_iterator.h"

class BM_IntersectionIterator : public benchmark::Fixture {
public:
    constexpr static size_t idsPerChild = 100'000;
    std::vector<std::vector<t_docId>> childrenIds;
    QueryIterator *iterator;
    static bool initialized;

    void SetUp(::benchmark::State &state) {
        if (!initialized) {
            RMCK::init();
            initialized = true;
        }

        auto numChildren = state.range(0);
        auto rawPercent = state.range(1);
        // The third parameter is a percentage representing the chance for a hit of each id
        // Convert the percentage to a probability
        double percent = std::pow(rawPercent / 100.0, 1.0 / numChildren);

        std::mt19937 rng(46);
        std::uniform_real_distribution<> dist;

        childrenIds.clear();
        childrenIds.resize(numChildren);
        for (auto &child : childrenIds) {
            child.reserve(idsPerChild);
            for (t_docId i = 1; child.size() < idsPerChild; i++) {
                if (dist(rng) < percent) {
                    child.emplace_back(i);
                }
            }
        }

        QueryIterator **children = createChildren();
        iterator = NewIntersectionIterator(children, childrenIds.size(), -1, false, 1.0);
    }

    void TearDown(::benchmark::State &state) {
        iterator->Free(iterator);
    }

    QueryIterator **createChildren() {
        QueryIterator **children = (QueryIterator **)rm_malloc(sizeof(QueryIterator *) * childrenIds.size());
        for (size_t i = 0; i < childrenIds.size(); i++) {
            children[i] = (QueryIterator *)new MockIterator(childrenIds[i]);
        }
        return children;
    }
};
bool BM_IntersectionIterator::initialized = false;

#define INTERSECTION_ARGS()                                                 \
    ArgNames({"numChildren", "percent"})                                    \
    ->ArgsProduct({                                                         \
        {2, 5, 10, 20},         /* numChildren */                           \
        {1, 5, 10, 20, 50, 80}  /* percent */                               \
    })

BENCHMARK_DEFINE_F(BM_IntersectionIterator, Read)(benchmark::State &state) {
    for (auto _ : state) {
        IteratorStatus rc = iterator->Read(iterator);
        if (rc == ITERATOR_EOF) {
            iterator->Rewind(iterator);
        }
    }
}

BENCHMARK_DEFINE_F(BM_IntersectionIterator, SkipTo)(benchmark::State &state) {
    t_offset step = 10;
    for (auto _ : state) {
        IteratorStatus rc = iterator->SkipTo(iterator, iterator->lastDocId + step);
        if (rc == ITERATOR_EOF) {
            iterator->Rewind(iterator);
        }
    }
}

BENCHMARK_REGISTER_F(BM_IntersectionIterator, Read)->INTERSECTION_ARGS();
BENCHMARK_REGISTER_F(BM_IntersectionIterator, SkipTo)->INTERSECTION_ARGS();

BENCHMARK_MAIN();
