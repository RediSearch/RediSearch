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

    void SetUp(::benchmark::State &state) {
        if (!initialized) {
            RMCK::init();
            initialized = true;
        }

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

        IndexFlags flags = static_cast<IndexFlags>(state.range(0));
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
        } else {
            // Populate the index with term data
            for (size_t i = 0; i < n_ids; ++i) {
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

#define INDEX_SCENARIOS() ArgName("Index Flags") \
    ->Arg(Index_StoreFreqs | Index_StoreTermOffsets | Index_StoreFieldFlags) \
    ->Arg(Index_StoreFreqs | Index_StoreTermOffsets | Index_StoreFieldFlags | Index_WideSchema) \
    ->Arg(Index_StoreFreqs | Index_StoreFieldFlags) \
    ->Arg(Index_StoreFreqs | Index_StoreFieldFlags | Index_WideSchema) \
    ->Arg(Index_StoreFreqs) \
    ->Arg(Index_StoreFieldFlags) \
    ->Arg(Index_StoreFieldFlags | Index_WideSchema) \
    ->Arg(Index_StoreFieldFlags | Index_StoreTermOffsets) \
    ->Arg(Index_StoreFieldFlags | Index_StoreTermOffsets | Index_WideSchema) \
    ->Arg(Index_StoreTermOffsets) \
    ->Arg(Index_StoreFreqs | Index_StoreTermOffsets) \
    ->Arg(Index_DocIdsOnly) \
    ->Arg(Index_StoreNumeric)

class BM_IndexIterator : public BM_IndexIterator_Base {
protected:
    void createIterator(IndexFlags flags) override {
        // Create an iterator based on the flags
        if (flags == Index_StoreNumeric) {
            FieldMaskOrIndex fieldMaskOrIndex = {.isFieldMask = false, .value = {.index = RS_INVALID_FIELD_INDEX}};
            FieldFilterContext fieldCtx = {.field = fieldMaskOrIndex, .predicate = FIELD_EXPIRATION_DEFAULT};
            iterator = NewInvIndIterator_NumericQuery(index, nullptr, &fieldCtx, nullptr, -INFINITY, INFINITY);
        } else if (flags == Index_DocIdsOnly) {
            iterator = NewInvIndIterator_GenericQuery(index, nullptr, 0, FIELD_EXPIRATION_DEFAULT);
        } else {
            iterator = NewInvIndIterator_TermQuery(index, nullptr, {true, RS_FIELDMASK_ALL}, nullptr, 1.0);
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
    t_docId docId = 10;
    for (auto _ : state) {
        IteratorStatus rc = iterator->SkipTo(iterator, docId);
        docId += 10;
        if (rc == ITERATOR_EOF) {
            iterator->Rewind(iterator);
            docId = 10;
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
            FieldMaskOrIndex fieldMaskOrIndex = {.isFieldMask = false, .value = {.index = RS_INVALID_FIELD_INDEX}};
            FieldFilterContext fieldCtx = {.field = fieldMaskOrIndex, .predicate = FIELD_EXPIRATION_DEFAULT};
            reader = NewNumericReader(nullptr, index, nullptr, -INFINITY, INFINITY, true, &fieldCtx);
        } else if (flags == Index_DocIdsOnly) {
            reader = NewGenericIndexReader(index, nullptr, 1, 1, 1, FIELD_EXPIRATION_DEFAULT);
        } else {
            reader = NewTermIndexReaderEx(index, nullptr, {true, RS_FIELDMASK_ALL}, nullptr, 1.0);
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
    RSIndexResult *hit;
    t_docId docId = 10;
    for (auto _ : state) {
        int rc = iterator_old->SkipTo(iterator_old->ctx, docId, &hit);
        docId += 10;
        if (rc == INDEXREAD_EOF) {
            iterator_old->Rewind(iterator_old->ctx);
            docId = 10;
        }
    }
}


BENCHMARK_REGISTER_F(BM_IndexIterator_Old, Read)->INDEX_SCENARIOS();
BENCHMARK_REGISTER_F(BM_IndexIterator_Old, SkipTo)->INDEX_SCENARIOS();

BENCHMARK_MAIN();
