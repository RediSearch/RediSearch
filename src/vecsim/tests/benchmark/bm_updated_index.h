/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#pragma once

#include <benchmark/benchmark.h>
#include <random>
#include <unistd.h>
#include "VecSim/vec_sim.h"
#include "VecSim/query_results.h"
#include "VecSim/utils/serializer.h"
#include "VecSim/index_factories/hnsw_factory.h"
#include "bm_common.h"

/**************************************
  Basic tests for updated single value index.
***************************************/

template <typename index_type_t>
class BM_VecSimUpdatedIndex : public BM_VecSimCommon<index_type_t> {
public:
    // Offset to access updated index variants in indices array (e.g., INDEX_BF + 1 =
    // INDEX_BF_UPDATED)
    const static unsigned short updated_index_offset = 1;

    // Tracks initialization state for updated indices to ensure one-time initialization
    // Independent from BM_VecSimGeneral base class initialization since updated indices have
    // different setup
    static bool is_initialized;

    BM_VecSimUpdatedIndex() {
        if (!is_initialized) {
            // Initialize the updated indexes as well, if this is the first instance.
            Initialize();
            is_initialized = true;
        }
    }

    ~BM_VecSimUpdatedIndex() = default;

private:
    static const char *updated_hnsw_index_file;
    static void Initialize();
};

template <typename index_type_t>
bool BM_VecSimUpdatedIndex<index_type_t>::is_initialized = false;

template <typename index_type_t>
void BM_VecSimUpdatedIndex<index_type_t>::Initialize() {
    VecSimType type = index_type_t::get_index_type();
    if (BM_VecSimGeneral::enabled_index_types & IndexTypeFlags::INDEX_MASK_HNSW_UPDATED) {
        INDICES[INDEX_HNSW_UPDATED] = IndexPtr(HNSWFactory::NewIndex(
            BM_VecSimIndex<index_type_t>::AttachRootPath(updated_hnsw_index_file)));

        if (!BM_VecSimIndex<index_type_t>::CastToHNSW(GET_INDEX(INDEX_HNSW_UPDATED))
                 ->checkIntegrity()
                 .valid_state) {
            throw std::runtime_error("The loaded HNSW index is corrupted. Exiting...");
        }
    }

    if (BM_VecSimGeneral::enabled_index_types & IndexTypeFlags::INDEX_MASK_BF_UPDATED) {
        BFParams bf_params = {
            .type = type, .dim = DIM, .metric = VecSimMetric_Cosine, .multi = IS_MULTI};

        INDICES[INDEX_BF_UPDATED] =
            IndexPtr(BM_VecSimIndex<index_type_t>::CreateNewIndex(bf_params));

        // Initially, load all the vectors to the updated bf index (before we override it).
        assert(BM_VecSimGeneral::enabled_index_types &
               IndexTypeFlags::INDEX_MASK_HNSW); // we need the original HNSW index to get the
                                                 // initial vectors
        for (size_t i = 0; i < N_VECTORS; ++i) {
            const char *blob = BM_VecSimIndex<index_type_t>::GetHNSWDataByInternalId(i);
            size_t label = BM_VecSimIndex<index_type_t>::CastToHNSW(GET_INDEX(INDEX_HNSW))
                               ->getExternalLabel(i);
            VecSimIndex_AddVector(GET_INDEX(INDEX_BF_UPDATED), blob, label);
        }
        // Override the previous BF_UPDATED index vectors.
        assert(BM_VecSimGeneral::enabled_index_types & IndexTypeFlags::INDEX_MASK_HNSW_UPDATED);
        for (size_t i = 0; i < N_VECTORS; ++i) {
            const char *blob =
                BM_VecSimIndex<index_type_t>::GetHNSWDataByInternalId(i, updated_index_offset);
            size_t label = BM_VecSimIndex<index_type_t>::CastToHNSW(GET_INDEX(INDEX_HNSW_UPDATED))
                               ->getExternalLabel(i);
            VecSimIndex_AddVector(GET_INDEX(INDEX_BF_UPDATED), blob, label);
        }
    }
}
