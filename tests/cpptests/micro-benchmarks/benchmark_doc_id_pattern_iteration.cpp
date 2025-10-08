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
#include "redisearch.h"
#include "iterators/iterator_api.h"
#include "iterators/intersection_iterator.h"
#include "iterators/union_iterator.h"
#include "iterators/idlist_iterator.h"

#include <random>
#include <vector>
#include <algorithm>

// ID distribution types for benchmark scenarios
enum IdDistributionType {
    CONSECUTIVE = 0,        // IDs within each idlist iterator are consecutive
    SPARSE_JUMPS_100 = 1,   // IDs have gaps of 100 between them
    CONSECUTIVE_MODULO = 2  // Consecutive IDs distributed round-robin across idlist iterators
};

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
        auto idDistributionType = static_cast<IdDistributionType>(state.range(2));

        // We'll always have 2 union iterators for intersection
        const size_t numUnions = 2;

        unionData.resize(numUnions);
        for (size_t unionIdx = 0; unionIdx < numUnions; unionIdx++) {
            generateUnionData(unionIdx, numIdListsPerUnion, docsPerIdList, idDistributionType);
        }
    }

private:
    // Unified method to generate ID data for a union based on distribution type
    // Creates different ID patterns to test iterator performance characteristics:
    // - CONSECUTIVE: Each idlist gets a consecutive block of IDs
    // - SPARSE_JUMPS_100: Each idlist gets IDs with gaps of 100 between them
    // - CONSECUTIVE_MODULO: Consecutive IDs distributed round-robin across idlists
    void generateUnionData(size_t unionIdx, size_t numIdListsPerUnion, size_t docsPerIdList, IdDistributionType idDistributionType) {
        unionData[unionIdx].resize(numIdListsPerUnion);

        // Common base parameters for all distribution types
        // Note: unionOffset creates overlap between unions for meaningful intersection testing
        const t_docId baseRange = 10000;
        const t_docId unionOffset = unionIdx * 200;  // Reduced offset to ensure overlap

        if (idDistributionType == CONSECUTIVE_MODULO) {
            generateConsecutiveModuloDistribution(unionIdx, numIdListsPerUnion, docsPerIdList, baseRange, unionOffset);
        } else {
            generateStandardDistribution(unionIdx, numIdListsPerUnion, docsPerIdList, idDistributionType, baseRange, unionOffset);
        }
    }

    // Generate consecutive modulo distribution: consecutive IDs distributed round-robin
    // Example with 3 idlist iterators, 4 docs each:
    // All IDs: [10001, 10002, 10003, 10004, 10005, 10006, 10007, 10008, 10009, 10010, 10011, 10012]
    // Iterator 0: [10001, 10004, 10007, 10010] (positions 0, 3, 6, 9)
    // Iterator 1: [10002, 10005, 10008, 10011] (positions 1, 4, 7, 10)
    // Iterator 2: [10003, 10006, 10009, 10012] (positions 2, 5, 8, 11)
    //
    // Union 0 result: [10001, 10002, 10003, ..., 12000] (2000 consecutive IDs for 2×1000 scenario)
    // Union 1 result: [10201, 10202, 10203, ..., 12200] (2000 consecutive IDs, offset by 200)
    // Expected intersection: [10201, 10202, 10203, ..., 12000] (1800 overlapping IDs)
    void generateConsecutiveModuloDistribution(size_t unionIdx, size_t numIdListsPerUnion, size_t docsPerIdList,
                                             t_docId baseRange, t_docId unionOffset) {
        size_t totalDocs = numIdListsPerUnion * docsPerIdList;
        std::vector<t_docId> allUnionIds(totalDocs);

        // Generate consecutive IDs for the entire union
        for (size_t i = 0; i < totalDocs; i++) {
            allUnionIds[i] = baseRange + unionOffset + i + 1;
        }

        // Distribute IDs across idlist iterators in round-robin fashion
        for (size_t idListIdx = 0; idListIdx < numIdListsPerUnion; idListIdx++) {
            unionData[unionIdx][idListIdx].resize(docsPerIdList);
            for (size_t docIdx = 0; docIdx < docsPerIdList; docIdx++) {
                size_t globalIdx = docIdx * numIdListsPerUnion + idListIdx;
                unionData[unionIdx][idListIdx][docIdx] = allUnionIds[globalIdx];
            }
            std::sort(unionData[unionIdx][idListIdx].begin(), unionData[unionIdx][idListIdx].end());
        }
    }

    // Generate standard distribution (consecutive or sparse)
    // CONSECUTIVE example with 3 idlist iterators, 4 docs each:
    // Iterator 0: [10001, 10002, 10003, 10004] (baseId=10000, consecutive)
    // Iterator 1: [10201, 10202, 10203, 10204] (baseId=10200, consecutive)
    // Iterator 2: [10401, 10402, 10403, 10404] (baseId=10400, consecutive)
    //
    // Union 0 result: [10001, 10002, 10003, 10004, 10201, 10202, 10203, 10204, 10401, 10402, 10403, 10404]
    // Union 1 result: [10201, 10202, 10203, 10204, 10401, 10402, 10403, 10404, 10601, 10602, 10603, 10604]
    // Expected intersection: [10201, 10202, 10203, 10204, 10401, 10402, 10403, 10404] (8 overlapping IDs)
    //
    // SPARSE_JUMPS_100 example with 3 idlist iterators, 4 docs each:
    // Iterator 0: [10100, 10200, 10300, 10400] (baseId=10000, jumps of 100)
    // Iterator 1: [10300, 10400, 10500, 10600] (baseId=10200, jumps of 100)
    // Iterator 2: [10500, 10600, 10700, 10800] (baseId=10400, jumps of 100)
    //
    // Union 0 result: [10100, 10200, 10300, 10400, 10500, 10600, 10700, 10800] (merged and deduplicated)
    // Union 1 result: [10300, 10400, 10500, 10600, 10700, 10800, 10900, 11000] (merged and deduplicated)
    // Expected intersection: [10300, 10400, 10500, 10600, 10700, 10800] (6 overlapping IDs)
    void generateStandardDistribution(size_t unionIdx, size_t numIdListsPerUnion, size_t docsPerIdList,
                                    IdDistributionType idDistributionType, t_docId baseRange, t_docId unionOffset) {
        for (size_t idListIdx = 0; idListIdx < numIdListsPerUnion; idListIdx++) {
            unionData[unionIdx][idListIdx].resize(docsPerIdList);
            t_docId idListOffset = idListIdx * 200;

            for (size_t docIdx = 0; docIdx < docsPerIdList; docIdx++) {
                t_docId baseId = baseRange + unionOffset + idListOffset;

                if (idDistributionType == CONSECUTIVE) {
                    unionData[unionIdx][idListIdx][docIdx] = baseId + docIdx + 1;
                } else if (idDistributionType == SPARSE_JUMPS_100) {
                    unionData[unionIdx][idListIdx][docIdx] = baseId + (docIdx + 1) * 100;
                }
            }
            std::sort(unionData[unionIdx][idListIdx].begin(), unionData[unionIdx][idListIdx].end());
        }
    }

public:
    // Helper function to create union iterators with idlist children
    QueryIterator* createUnionIterator(size_t unionIdx) {
        const size_t numIdLists = unionData[unionIdx].size();
        QueryIterator **idListIterators = createIdListIterators(unionIdx, numIdLists);
        return NewUnionIterator(idListIterators, numIdLists, true, 1.0, QN_UNION, NULL, &RSGlobalConfig.iteratorsConfigParams);
    }

    // Helper function to create intersection iterator with two union children
    QueryIterator* createIntersectionIterator() {
        QueryIterator **unionIterators = (QueryIterator **)rm_malloc(sizeof(QueryIterator *) * 2);
        unionIterators[0] = createUnionIterator(0);
        unionIterators[1] = createUnionIterator(1);
        return NewIntersectIterator(unionIterators, 2, NULL, RS_FIELDMASK_ALL, -1, 0, 1.0);
    }

private:
    // Create array of idlist iterators for a union
    QueryIterator** createIdListIterators(size_t unionIdx, size_t numIdLists) {
        QueryIterator **idListIterators = (QueryIterator **)rm_malloc(sizeof(QueryIterator *) * numIdLists);

        for (size_t i = 0; i < numIdLists; i++) {
            t_docId *ids = copyIds(unionData[unionIdx][i]);
            idListIterators[i] = NewIdListIterator(ids, unionData[unionIdx][i].size(), 1.0);
        }

        return idListIterators;
    }

    // Create a copy of IDs for an idlist iterator
    t_docId* copyIds(const std::vector<t_docId>& sourceIds) {
        t_docId *ids = (t_docId *)rm_malloc(sizeof(t_docId) * sourceIds.size());
        for (size_t i = 0; i < sourceIds.size(); i++) {
            ids[i] = sourceIds[i];
        }
        return ids;
    }
};

bool BM_IntersectionIterator::initialized = false;

// Benchmark scenarios:
// Parameter 0: Number of idlist iterators per union (10, 25, 50)
// Parameter 1: Number of documents per idlist iterator (1000, 5000)
// Parameter 2: ID distribution type (CONSECUTIVE, SPARSE_JUMPS_100, CONSECUTIVE_MODULO)
#define INTERSECTION_SCENARIOS() \
  ArgNames({"IdListsPerUnion", "DocsPerIdList", "IdDistributionType"})-> \
  ArgsProduct({{10, 25, 50}, {1000, 5000}, {CONSECUTIVE, SPARSE_JUMPS_100, CONSECUTIVE_MODULO}})

// Benchmark intersection iterator Read() performance
// Tests how different ID distributions affect intersection performance:
// - Consecutive: IDs within each idlist iterator are consecutive
// - Sparse (jumps of 100): IDs have gaps of 100 between them
// - Consecutive modulo: Consecutive IDs distributed round-robin across idlist iterators
BENCHMARK_DEFINE_F(BM_IntersectionIterator, Read)(benchmark::State &state) {
    QueryIterator *intersectionIt = createIntersectionIterator();
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
// Tests random access performance with different ID distributions:
// - Consecutive: IDs within each idlist iterator are consecutive
// - Sparse (jumps of 100): IDs have gaps of 100 between them
// - Consecutive modulo: Consecutive IDs distributed round-robin across idlist iterators
BENCHMARK_DEFINE_F(BM_IntersectionIterator, SkipTo)(benchmark::State &state) {
    QueryIterator *intersectionIt = createIntersectionIterator();
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
