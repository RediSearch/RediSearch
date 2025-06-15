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

#include "deprecated_iterator_util.h"
#include "src/index.h"

class BM_IntersectionIterator : public benchmark::Fixture {
public:
    constexpr static size_t idsPerChild = 100'000;
    std::vector<std::vector<t_docId>> childrenIds;
    static bool initialized;

    void SetUp(::benchmark::State &state) {
        if (!initialized) {
            RMCK::init();
            initialized = true;
        }

        auto numChildren = state.range(0);
        auto scale = state.range(1);
        auto rawPercent = state.range(2);
        // The third parameter is a percentage representing the chance for a hit of each id
        // Convert the percentage to a probability
        double percent = std::pow(rawPercent / 100.0, 1.0 / numChildren);

        std::mt19937 rng(46);
        std::uniform_real_distribution<> dist;

        childrenIds.clear();
        childrenIds.resize(numChildren);
        for (auto &child : childrenIds) {
            child.reserve(idsPerChild);
            for (size_t i = 1; i <= idsPerChild; i++) {
                if (dist(rng) < percent) {
                    child.emplace_back(i * scale);
                }
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
bool BM_IntersectionIterator::initialized = false;

#define INTERSECTION_ARGS()                                                 \
    ArgNames({"numChildren", "scale", "percent"})                           \
    ->ArgsProduct({                                                         \
        {2, 5, 10, 20},         /* numChildren */                           \
        {1, 10, 100},           /* scale (id density) */                    \
        {1, 5, 10, 20, 50, 80}  /* percent */                               \
    })

BENCHMARK_DEFINE_F(BM_IntersectionIterator, Read)(benchmark::State &state) {
    QueryIterator **children = createChildren();
    QueryIterator *iterator = NewIntersectionIterator(children, childrenIds.size(), -1, false, 1.0);
    for (auto _ : state) {
        IteratorStatus rc = iterator->Read(iterator);
        if (rc == ITERATOR_EOF) {
            iterator->Rewind(iterator);
        }
    }
    iterator->Free(iterator);
}

BENCHMARK_DEFINE_F(BM_IntersectionIterator, SkipTo)(benchmark::State &state) {
    t_offset step = 10;
    QueryIterator **children = createChildren();
    QueryIterator *iterator = NewIntersectionIterator(children, childrenIds.size(), -1, false, 1.0);
    for (auto _ : state) {
        IteratorStatus rc = iterator->SkipTo(iterator, iterator->lastDocId + step);
        if (rc == ITERATOR_EOF) {
            iterator->Rewind(iterator);
        }
    }
    iterator->Free(iterator);
}

BENCHMARK_REGISTER_F(BM_IntersectionIterator, Read)->INTERSECTION_ARGS();
BENCHMARK_REGISTER_F(BM_IntersectionIterator, SkipTo)->INTERSECTION_ARGS();

BENCHMARK_DEFINE_F(BM_IntersectionIterator, Read_old)(benchmark::State &state) {
    IndexIterator **children = createChildrenOld();
    IndexIterator *iterator = NewIntersectIterator(children, childrenIds.size(), NULL, 0, -1, false, 1.0);
    RSIndexResult *hit;
    for (auto _ : state) {
        int rc = iterator->Read(iterator->ctx, &hit);
        if (rc == INDEXREAD_EOF) {
            iterator->Rewind(iterator->ctx);
        }
    }
    iterator->Free(iterator);
}

BENCHMARK_DEFINE_F(BM_IntersectionIterator, SkipTo_old)(benchmark::State &state) {
    IndexIterator **children = createChildrenOld();
    IndexIterator *iterator = NewIntersectIterator(children, childrenIds.size(), NULL, 0, -1, false, 1.0);
    RSIndexResult *hit = iterator->current;
    hit->docId = 0; // Ensure initial docId is set to 0
    t_offset step = 10;
    for (auto _ : state) {
        int rc = iterator->SkipTo(iterator->ctx, hit->docId + step, &hit);
        if (rc == INDEXREAD_EOF) {
            iterator->Rewind(iterator->ctx);
            // Don't rely on the old iterator's Rewind to reset hit->docId
            hit = iterator->current;
            hit->docId = 0;
        }
    }
    iterator->Free(iterator);
}

BENCHMARK_REGISTER_F(BM_IntersectionIterator, Read_old)->INTERSECTION_ARGS();
BENCHMARK_REGISTER_F(BM_IntersectionIterator, SkipTo_old)->INTERSECTION_ARGS();

BENCHMARK_MAIN();
