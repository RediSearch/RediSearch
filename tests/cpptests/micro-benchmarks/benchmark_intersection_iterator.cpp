/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#define MICRO_BENCHMARKS

#include "benchmark/benchmark.h"
#include "redismock/util.h"

#include <random>
#include <vector>
#include <algorithm>

#include "index.h"

class BM_IntersectionIterator : public benchmark::Fixture {
public:
    // Data for two union iterators, each with multiple idlist iterators
    std::vector<std::vector<std::vector<t_docId>>> unionData;
    static bool initialized;

    void SetUp(::benchmark::State &state) {
        if (!initialized) {
            RMCK::init();
            initialized = true;
        }

        // Extract parameters from benchmark state
        auto numIdListsPerUnion = state.range(0);  // Number of idlist iterators per union
        auto docsPerIdList = state.range(1);       // Number of documents per idlist iterator
        auto idDistributionType = state.range(2);  // 0 = consecutive, 1 = jumps of 100

        // We'll always have 2 union iterators for intersection
        const size_t numUnions = 2;

        unionData.resize(numUnions);
        for (size_t unionIdx = 0; unionIdx < numUnions; unionIdx++) {
            unionData[unionIdx].resize(numIdListsPerUnion);

            for (size_t idListIdx = 0; idListIdx < numIdListsPerUnion; idListIdx++) {
                unionData[unionIdx][idListIdx].resize(docsPerIdList);

                // Generate IDs that will have some overlap between unions for intersection
                // Use a shared base range but different offsets within each union
                t_docId baseRange = 10000; // Shared base range for intersection
                t_docId unionOffset = unionIdx * 500; // Small offset between unions
                t_docId idListOffset = idListIdx * 200; // Offset within union

                for (size_t docIdx = 0; docIdx < docsPerIdList; docIdx++) {
                    if (idDistributionType == 0) {
                        // Consecutive IDs scenario
                        unionData[unionIdx][idListIdx][docIdx] = baseRange + unionOffset + idListOffset + docIdx + 1;
                    } else {
                        // Jumps of 100 scenario
                        unionData[unionIdx][idListIdx][docIdx] = baseRange + unionOffset + idListOffset + (docIdx + 1) * 100;
                    }
                }

                // Sort the IDs to ensure they're in order
                std::sort(unionData[unionIdx][idListIdx].begin(), unionData[unionIdx][idListIdx].end());
            }
        }
    }

    // Helper function to create union iterators with idlist children
    IndexIterator* createUnionIterator(size_t unionIdx) {
        const size_t numIdLists = unionData[unionIdx].size();
        IndexIterator **idListIterators = (IndexIterator **)rm_malloc(sizeof(IndexIterator *) * numIdLists);

        for (size_t i = 0; i < numIdLists; i++) {
            // Create a copy of the IDs for the idlist iterator
            t_docId *ids = (t_docId *)rm_malloc(sizeof(t_docId) * unionData[unionIdx][i].size());
            for (size_t j = 0; j < unionData[unionIdx][i].size(); j++) {
                ids[j] = unionData[unionIdx][i][j];
            }
            idListIterators[i] = NewIdListIterator(ids, unionData[unionIdx][i].size(), 1.0);
        }

        return NewUnionIterator(idListIterators, numIdLists, false, 1.0, QN_UNION, NULL, &RSGlobalConfig.iteratorsConfigParams);
    }

    // Helper function to create intersection iterator with two union children
    IndexIterator* createIntersectionIterator() {
        // Create two union iterators
        IndexIterator *union1 = createUnionIterator(0);
        IndexIterator *union2 = createUnionIterator(1);

        IndexIterator **children = (IndexIterator **)rm_malloc(sizeof(IndexIterator *) * 2);
        children[0] = union1;
        children[1] = union2;

        return NewIntersectIterator(children, 2, NULL, RS_FIELDMASK_ALL, -1, 0, 1.0);
    }
};

bool BM_IntersectionIterator::initialized = false;

// Benchmark scenarios:
// Parameter 0: Number of idlist iterators per union (2, 4, 8)
// Parameter 1: Number of documents per idlist iterator (1000, 5000, 10000)
// Parameter 2: ID distribution type (0 = consecutive, 1 = jumps of 100)
#define INTERSECTION_SCENARIOS() \
    Args({10, 1000, 0})->Args({10, 1000, 1})-> \
    Args({10, 5000, 0})->Args({10, 5000, 1})-> \
    Args({50, 1000, 0})->Args({50, 1000, 1})-> \
    Args({50, 5000, 0})->Args({50, 5000, 1})-> \
    Args({100, 1000, 0})->Args({100, 1000, 1})

// Benchmark intersection iterator Read() performance
// Tests how consecutive vs sparse (jumps of 100) document IDs affect intersection performance
// with varying numbers of child iterators and document counts
BENCHMARK_DEFINE_F(BM_IntersectionIterator, Read)(benchmark::State &state) {
    IndexIterator *intersectionIt = createIntersectionIterator();
    RSIndexResult *hit;

    for (auto _ : state) {
        int rc = intersectionIt->Read(intersectionIt->ctx, &hit);
        if (rc == INDEXREAD_EOF) {
            intersectionIt->Rewind(intersectionIt->ctx);
        }
    }

    intersectionIt->Free(intersectionIt);
}

// Benchmark intersection iterator SkipTo() performance
// Tests random access performance with consecutive vs sparse document IDs
// with varying numbers of child iterators and document counts
BENCHMARK_DEFINE_F(BM_IntersectionIterator, SkipTo)(benchmark::State &state) {
    IndexIterator *intersectionIt = createIntersectionIterator();
    RSIndexResult *hit;
    t_offset step = 50;
    t_docId lastDocId = 0;

    for (auto _ : state) {
        int rc = intersectionIt->SkipTo(intersectionIt->ctx, lastDocId + step, &hit);
        if (rc == INDEXREAD_OK) {
            lastDocId = hit->docId;
        } else if (rc == INDEXREAD_EOF) {
            intersectionIt->Rewind(intersectionIt->ctx);
            lastDocId = 0;
        }
    }

    intersectionIt->Free(intersectionIt);
}

BENCHMARK_REGISTER_F(BM_IntersectionIterator, Read)->INTERSECTION_SCENARIOS();
BENCHMARK_REGISTER_F(BM_IntersectionIterator, SkipTo)->INTERSECTION_SCENARIOS();

BENCHMARK_MAIN();
