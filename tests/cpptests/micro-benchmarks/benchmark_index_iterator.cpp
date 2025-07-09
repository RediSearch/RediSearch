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

#include "deprecated_iterator_util.h"
#include "src/index.h"

class BM_IndexIterator_Base : public benchmark::Fixture {
public:
    static bool initialized;
    static constexpr size_t n_ids = 100'000;

    std::vector<t_docId> ids;
    InvertedIndex *index;
    QueryIterator *iterator;
    IndexIterator *iterator_old;
    std::unique_ptr<MockQueryEvalCtx> q_mock;

    void SetUp(::benchmark::State &state) {
        if (!initialized) {
            RMCK::init();
            initialized = true;
        }
        q_mock = std::make_unique<MockQueryEvalCtx>();

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
        // Create an iterator based on the flags. Expect only one of the iterators to be created (iterator or iterator_old)
        createIterator(flags);
    }
    void TearDown(::benchmark::State &state) {
        if (iterator) {
            iterator->Free(iterator);
            iterator = nullptr;
        }
        if (iterator_old) {
            iterator_old->Free(iterator_old);
            iterator_old = nullptr;
        }
        if (index) {
            InvertedIndex_Free(index);
            index = nullptr;
        }
        ids.clear();
        RSGlobalConfig.invertedIndexRawDocidEncoding = false;
    }

    void createIndex(IndexFlags flags) {
        // Create a new InvertedIndex with the given flags
        size_t dummy;
        index = NewInvertedIndex(flags, 1, &dummy);
        IndexEncoder encoder = InvertedIndex_GetEncoder(flags);

        if (flags == Index_StoreNumeric) {
            // Populate the index with numeric data
            for (size_t i = 0; i < ids.size(); ++i) {
                InvertedIndex_WriteNumericEntry(index, ids[i], static_cast<double>(i));
            }
        } else if (flags == Index_DocIdsOnly) {
            // Populate the index with document IDs only
            for (size_t i = 0; i < ids.size(); ++i) {
                InvertedIndex_WriteEntryGeneric(index, encoder, ids[i], nullptr);
            }
        } else if (flags == (Index_DocIdsOnly | Index_Temporary)) {
            // Special case reserved for `Index_DocIdsOnly` with raw doc IDs
            RSGlobalConfig.invertedIndexRawDocidEncoding = true; // Enable raw doc ID encoding, until the benchmark's tearDown
            RS_ASSERT_ALWAYS(encoder != InvertedIndex_GetEncoder(Index_DocIdsOnly)); // Ensure we are using the raw doc ID encoder
            encoder = InvertedIndex_GetEncoder(Index_DocIdsOnly);
            for (size_t i = 0; i < ids.size(); ++i) {
                InvertedIndex_WriteEntryGeneric(index, encoder, ids[i], nullptr);
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
                InvertedIndex_WriteForwardIndexEntry(index, encoder, &h);
                VVW_Free(h.vw);
            }
        }
    }

    protected:
    // Create an iterator based on the flags provided
    virtual void createIterator(IndexFlags flags) = 0;

};
bool BM_IndexIterator_Base::initialized = false;

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

class BM_IndexIterator : public BM_IndexIterator_Base {
protected:
    void createIterator(IndexFlags flags) override {
        // Create an iterator based on the flags
        if (flags == Index_StoreNumeric) {
            FieldFilterContext fieldCtx = {.field = {false, 0}, .predicate = FIELD_EXPIRATION_DEFAULT};
            iterator = NewInvIndIterator_NumericQuery(index, &q_mock->sctx, &fieldCtx, nullptr, -INFINITY, INFINITY);
        } else if (flags == Index_DocIdsOnly || flags == (Index_DocIdsOnly | Index_Temporary)) {
            iterator = NewInvIndIterator_GenericQuery(index, &q_mock->sctx, 0, FIELD_EXPIRATION_DEFAULT, 1.0);
        } else {
            iterator = NewInvIndIterator_TermQuery(index, &q_mock->sctx, {true, RS_FIELDMASK_ALL}, nullptr, 1.0);
        }
    }
};
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

class BM_IndexIterator_Old : public BM_IndexIterator_Base {
protected:
    void createIterator(IndexFlags flags) override {
        // Create an old iterator based on the flags
        IndexReader *reader;
        if (flags == Index_StoreNumeric) {
            FieldFilterContext fieldCtx = {.field = {false, 0}, .predicate = FIELD_EXPIRATION_DEFAULT};
            reader = NewNumericReader(&q_mock->sctx, index, nullptr, -INFINITY, INFINITY, true, &fieldCtx);
        } else if (flags == Index_DocIdsOnly || flags == (Index_DocIdsOnly | Index_Temporary)) {
            reader = NewGenericIndexReader(index, &q_mock->sctx, 1, 1, 0, FIELD_EXPIRATION_DEFAULT);
        } else {
            reader = NewTermIndexReaderEx(index, &q_mock->sctx, {true, RS_FIELDMASK_ALL}, nullptr, 1.0);
        }
        iterator_old = NewReadIterator(reader);
    }
};

BENCHMARK_DEFINE_F(BM_IndexIterator_Old, Read)(benchmark::State &state) {
    RSIndexResult *hit;
    for (auto _ : state) {
        int rc = iterator_old->Read(iterator_old->ctx, &hit);
        if (rc == INDEXREAD_EOF) {
            iterator_old->Rewind(iterator_old->ctx);
        }
    }
}

BENCHMARK_DEFINE_F(BM_IndexIterator_Old, SkipTo)(benchmark::State &state) {
    RSIndexResult *hit = iterator_old->current;
    hit->docId = 0; // Ensure initial docId is set to 0
    t_offset step = 10;
    for (auto _ : state) {
        int rc = iterator_old->SkipTo(iterator_old->ctx, hit->docId + step, &hit);
        if (rc == INDEXREAD_EOF) {
            iterator_old->Rewind(iterator_old->ctx);
            // Don't rely on the old iterator's Rewind to reset hit->docId
            hit = iterator_old->current;
            hit->docId = 0;
        }
    }
}


BENCHMARK_REGISTER_F(BM_IndexIterator_Old, Read)->INDEX_SCENARIOS();
BENCHMARK_REGISTER_F(BM_IndexIterator_Old, SkipTo)->INDEX_SCENARIOS();

BENCHMARK_MAIN();
