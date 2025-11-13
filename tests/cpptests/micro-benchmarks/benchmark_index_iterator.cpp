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
#include "index_utils.h"
#include "redismock/util.h"

#include <random>
#include <vector>

#include "src/iterators/iterator_api.h"
#include "src/iterators/inverted_index_iterator.h"
#include "src/numeric_filter.h"
#include "src/forward_index.h"

class BM_IndexIterator : public benchmark::Fixture {
public:
    static bool initialized;
    static constexpr size_t n_ids = 100'000;

    std::vector<t_docId> ids;
    InvertedIndex *index;
    QueryIterator *iterator;
    std::unique_ptr<MockQueryEvalCtx> q_mock;
    NumericFilter *numericFilter;

    void SetUp(::benchmark::State &state) {
        if (!initialized) {
            RMCK::init();
            initialized = true;
        }
        q_mock = std::make_unique<MockQueryEvalCtx>();
        numericFilter = nullptr;

        std::mt19937 rng(46);
        std::uniform_int_distribution<t_docId> dist(1, 2'000'000);

        // Generate unique random ids
        ids.resize(n_ids);
        for (auto &id : ids) {
            id = dist(rng);
        }
        // Sort the ids to ensure they are in order
        std::sort(ids.begin(), ids.end());
        // Remove duplicates
        ids.erase(std::unique(ids.begin(), ids.end()), ids.end());

        auto flags = static_cast<IndexFlags>(state.range(0));
        auto withExpiration = static_cast<bool>(state.range(1));

        if (withExpiration) {
            for (auto id : ids) {
                q_mock->TTL_Add(id, t_fieldMask{RS_FIELDMASK_ALL}); // Add expiration for all fields
            }
        }

        // Create an index with the specified flags
        createIndex(flags);
        // Create an iterator based on the flags.
        createIterator(flags);
    }
    void TearDown(::benchmark::State &state) {
        if (iterator) {
            iterator->Free(iterator);
            iterator = nullptr;
        }
        if (index) {
            InvertedIndex_Free(index);
            index = nullptr;
        }
        if (numericFilter) {
            NumericFilter_Free(numericFilter);
            numericFilter = nullptr;
        }
        ids.clear();
        RSGlobalConfig.invertedIndexRawDocidEncoding = false;
    }

    void createIndex(IndexFlags flags) {
        if (flags == (Index_DocIdsOnly | Index_Temporary)) {
            // Special case reserved for `Index_DocIdsOnly` with raw doc IDs
            RSGlobalConfig.invertedIndexRawDocidEncoding = true; // Enable raw doc ID encoding, until the benchmark's tearDown
        }

        // Create a new InvertedIndex with the given flags
        size_t dummy;
        index = NewInvertedIndex(flags, &dummy);

        if (flags == Index_StoreNumeric) {
            // Populate the index with numeric data
            for (size_t i = 0; i < ids.size(); ++i) {
                InvertedIndex_WriteNumericEntry(index, ids[i], static_cast<double>(i));
            }
        } else if (flags == Index_DocIdsOnly || flags == (Index_DocIdsOnly | Index_Temporary)) {
            // Populate the index with document IDs only
            for (size_t i = 0; i < ids.size(); ++i) {
                RSIndexResult rec = {.docId = ids[i], .data = {.tag = RSResultData_Virtual}};
                InvertedIndex_WriteEntryGeneric(index, &rec);
            }
        } else {
            // Populate the index with term data
            for (size_t i = 0; i < ids.size(); ++i) {
                ForwardIndexEntry h = {0};
                h.docId = ids[i];
                h.fieldMask = i + 1;
                h.freq = i + 1;
                h.term = "term";
                h.len = 4; // Length of the term "term"

                h.vw = NewVarintVectorWriter(8);
                VVW_Write(h.vw, i); // Just writing the index as a value
                InvertedIndex_WriteForwardIndexEntry(index, &h);
                VVW_Free(h.vw);
            }
        }
    }

    void createIterator(IndexFlags flags) {
        // Create an iterator based on the flags
        if (flags == Index_StoreNumeric) {
            FieldFilterContext fieldCtx = {.field = {.index_tag = FieldMaskOrIndex_Index, .index = 0}, .predicate = FIELD_EXPIRATION_DEFAULT};
            iterator = NewInvIndIterator_NumericQuery(index, &q_mock->sctx, &fieldCtx, nullptr, nullptr, -INFINITY, INFINITY);
        } else {
            iterator = NewInvIndIterator_TermQuery(index, &q_mock->sctx, {.mask_tag = FieldMaskOrIndex_Mask, .mask = RS_FIELDMASK_ALL}, nullptr, 1.0);
        }
    }

};
bool BM_IndexIterator::initialized = false;

#define INDEX_SCENARIOS() ArgNames({"Index Flags", "With expiration data"})                         \
    ->ArgsProduct({{                                                                                \
        Index_StoreFreqs | Index_StoreTermOffsets | Index_StoreFieldFlags,                          \
        Index_StoreFreqs | Index_StoreTermOffsets | Index_StoreFieldFlags | Index_WideSchema,       \
        Index_StoreFreqs | Index_StoreFieldFlags,                                                   \
        Index_StoreFreqs | Index_StoreFieldFlags | Index_WideSchema,                                \
        Index_StoreFreqs,                                                                           \
        Index_StoreFieldFlags,                                                                      \
        Index_StoreFieldFlags | Index_WideSchema,                                                   \
        Index_StoreFieldFlags | Index_StoreTermOffsets,                                             \
        Index_StoreFieldFlags | Index_StoreTermOffsets | Index_WideSchema,                          \
        Index_StoreTermOffsets,                                                                     \
        Index_StoreFreqs | Index_StoreTermOffsets,                                                  \
        Index_DocIdsOnly,                                                                           \
        Index_DocIdsOnly | Index_Temporary,                                                         \
        Index_StoreNumeric,                                                                         \
    }, {                                                                                            \
        false,                                                                                      \
        true                                                                                        \
    }});


BENCHMARK_DEFINE_F(BM_IndexIterator, Read)(benchmark::State &state) {
    for (auto _ : state) {
        IteratorStatus rc = iterator->Read(iterator);
        if (rc == ITERATOR_EOF) {
            iterator->Rewind(iterator);
        }
    }
}

BENCHMARK_DEFINE_F(BM_IndexIterator, SkipTo)(benchmark::State &state) {
    t_offset step = 10;
    for (auto _ : state) {
        IteratorStatus rc = iterator->SkipTo(iterator, iterator->lastDocId + step);
        if (rc == ITERATOR_EOF) {
            iterator->Rewind(iterator);
        }
    }
}

BENCHMARK_REGISTER_F(BM_IndexIterator, Read)->INDEX_SCENARIOS();
BENCHMARK_REGISTER_F(BM_IndexIterator, SkipTo)->INDEX_SCENARIOS();

BENCHMARK_MAIN();
