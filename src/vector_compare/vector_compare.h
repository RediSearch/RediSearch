/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#pragma once

#include "VecSim/vec_sim.h"

#include <stdbool.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Whether `label` in `index` already holds exactly the vector(s) in `blobs`, i.e. whether
 * adding them under `label` would leave the index unchanged.
 *
 * Lets an update that replaces a document skip re-adding a vector that did not change and
 * move the existing entry to the document's new doc-id instead. See
 * `VectorIndex_RelabelField`.
 *
 * Compares the *stored* bytes, having first put each blob through the same normalization an
 * insert would apply, so a cosine index -- which stores vectors normalized -- is answered
 * correctly rather than reported as changed every time.
 *
 * Answers false whenever the vectors differ **or the comparison cannot be made**: a lossy
 * storage that cannot reproduce the input, or a count that does not match what the label
 * holds. False is always the safe answer, costing a delete + re-add that could have been
 * avoided; a wrong true would leave a stale vector answering queries.
 *
 * Lives in a C++ translation unit because VecSim exposes stored vectors through
 * `getDataByLabel`, a template method that no C caller can name.
 *
 * @param index the index to inspect.
 * @param label the label to inspect.
 * @param blobs `numBlobs` contiguous binary vectors, each of the index's input blob size.
 * @param numBlobs how many vectors `blobs` holds; must match what `label` stores for the
 *        answer to be true.
 */
bool VectorIndex_HoldsVectors(VecSimIndex *index, size_t label, const void *blobs,
                              size_t numBlobs);

#ifdef __cplusplus
}
#endif
