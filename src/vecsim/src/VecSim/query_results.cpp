/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#include "VecSim/query_result_definitions.h"
#include "VecSim/vec_sim.h"
#include "VecSim/batch_iterator.h"
#include <assert.h>

struct VecSimQueryReply_Iterator {
    using iterator = decltype(VecSimQueryReply::results)::iterator;
    const iterator begin, end;
    iterator current;

    explicit VecSimQueryReply_Iterator(VecSimQueryReply *reply)
        : begin(reply->results.begin()), end(reply->results.end()), current(begin) {}
};

extern "C" size_t VecSimQueryReply_Len(VecSimQueryReply *qr) { return qr->results.size(); }

extern "C" VecSimQueryReply_Code VecSimQueryReply_GetCode(VecSimQueryReply *qr) { return qr->code; }

extern "C" void VecSimQueryReply_Free(VecSimQueryReply *qr) { delete qr; }

extern "C" VecSimQueryReply_Iterator *VecSimQueryReply_GetIterator(VecSimQueryReply *results) {
    return new VecSimQueryReply_Iterator(results);
}

extern "C" bool VecSimQueryReply_IteratorHasNext(VecSimQueryReply_Iterator *iterator) {
    return iterator->current != iterator->end;
}

extern "C" VecSimQueryResult *VecSimQueryReply_IteratorNext(VecSimQueryReply_Iterator *iterator) {
    if (iterator->current == iterator->end) {
        return nullptr;
    }

    return std::to_address(iterator->current++);
}

extern "C" int64_t VecSimQueryResult_GetId(const VecSimQueryResult *res) {
    if (res == nullptr) {
        return INVALID_ID;
    }
    return (int64_t)res->id;
}

extern "C" double VecSimQueryResult_GetScore(const VecSimQueryResult *res) {
    if (res == nullptr) {
        return INVALID_SCORE; // "NaN"
    }
    return res->score;
}

extern "C" void VecSimQueryReply_IteratorFree(VecSimQueryReply_Iterator *iterator) {
    delete iterator;
}

extern "C" void VecSimQueryReply_IteratorReset(VecSimQueryReply_Iterator *iterator) {
    iterator->current = iterator->begin;
}

/********************** batch iterator API ***************************/
VecSimQueryReply *VecSimBatchIterator_Next(VecSimBatchIterator *iterator, size_t n_results,
                                           VecSimQueryReply_Order order) {
    assert((order == BY_ID || order == BY_SCORE) &&
           "Possible order values are only 'BY_ID' or 'BY_SCORE'");
    return iterator->getNextResults(n_results, order);
}

bool VecSimBatchIterator_HasNext(VecSimBatchIterator *iterator) { return !iterator->isDepleted(); }

void VecSimBatchIterator_Free(VecSimBatchIterator *iterator) {
    // Batch iterator might be deleted after the index, so it should keep the allocator before
    // deleting.
    auto allocator = iterator->getAllocator();
    delete iterator;
}

void VecSimBatchIterator_Reset(VecSimBatchIterator *iterator) { iterator->reset(); }
