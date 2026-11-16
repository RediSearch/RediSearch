/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#pragma once

#include "vec_sim.h"

#ifdef __cplusplus
extern "C" {
#endif

/**
 * @brief: Dump the neighbors of an element in HNSW index in the following format:
 * an array with <topLevel+2> entries, where each entry is an array itself.
 * Every internal array in a position <l> where <0<=l<=topLevel> corresponds to the neighbors of the
 * element in the graph in level <l>. It contains <n_l+1> entries, where <n_l> is the number of
 * neighbors in level l. The last entry in the external array is NULL (indicates its length).
 * The first entry in each internal array contains the number <n_l>, while the next
 * <n_l> entries are the the *labels* of the elements neighbors in this level.
 * Note: currently only HNSW indexes of type single are supported (multi not yet) - tiered included.
 * For cleanup, VecSimDebug_ReleaseElementNeighborsInHNSWGraph need to be called with the value
 * pointed by neighborsData as returned from this call.
 * @param index - the index in which the element resides.
 * @param label - the label to dump its neighbors in every level in which it exits.
 * @param neighborsData - a pointer to a 2-dim array of integer which is a placeholder for the
 * output of the neighbors' labels that will be allocated and stored in the format described above.
 *
 */
// TODO: Implement the full version that supports MULTI as well. This will require adding an
//  additional dim to the array and perhaps differentiating between internal ids of labels in the
//  output format. Also, we may want in the future to dump the incoming edges as well.
int VecSimDebug_GetElementNeighborsInHNSWGraph(VecSimIndex *index, size_t label,
                                               int ***neighborsData);

/**
 * @brief: Release the neighbors data allocated by VecSimDebug_GetElementNeighborsInHNSWGraph.
 * @param neighborsData - the 2-dim array returned in the placeholder to be de-allocated.
 */
void VecSimDebug_ReleaseElementNeighborsInHNSWGraph(int **neighborsData);

#ifdef __cplusplus
}
#endif
