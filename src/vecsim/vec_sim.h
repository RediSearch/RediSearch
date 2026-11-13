/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#pragma once

#ifdef __cplusplus
extern "C" {
#endif

#include <stdlib.h>
#include "query_results.h"
#include "vec_sim_common.h"
#include "info_iterator.h"

typedef struct VecSimIndexInterface VecSimIndex;

/**
 * @brief Create a new VecSim index based on the given params.
 * @param params index configurations (initial size, data type, dimension, metric, algorithm and the
 * algorithm-related params).
 * @return A pointer to the created index.
 */
VecSimIndex *VecSimIndex_New(const VecSimParams *params);

/**
 * @brief Estimates the size of an empty index according to the parameters.
 * @param params index configurations (initial size, data type, dimension, metric, algorithm and the
 * algorithm-related params).
 * @return Estimated index size, or SIZE_MAX if the parameters are invalid or unsupported.
 */
size_t VecSimIndex_EstimateInitialSize(const VecSimParams *params);

/**
 * @brief Estimates the size of a single vector and its metadata according to the parameters, WHEN
 * THE INDEX IS RESIZING BY A BLOCK. That is, this function estimates the allocation size of a new
 * block upon resizing all the internal data structures, and returns the size of a single vector in
 * that block. This value can be used later to decide what is the best block size for the block
 * size, when the memory limit is known.
 * ("memory limit for a block" / "size of a single vector in a block" = "block size")
 * For tiered HNSW indexes, the estimate is the larger of the frontend and backend per-vector
 * estimates, so the shared block size accounts for either tier's growth. It does not estimate
 * combined growth of both tiers or total index memory.
 * @param params index configurations (initial size, data type, dimension, metric, algorithm and the
 * algorithm-related params).
 * @return The estimated single vector memory consumption, considering the parameters.
 */
size_t VecSimIndex_EstimateElementSize(const VecSimParams *params);

/**
 * @brief Release an index and its internal data.
 * @param index the index to release.
 */
void VecSimIndex_Free(VecSimIndex *index);

/**
 * @brief Add a vector to an index.
 * @param index the index to which the vector is added.
 * @param blob binary representation of the vector. Blob size should match the index data type and
 * dimension.
 * @param label the label of the added vector
 * @return the number of new vectors inserted (1 for new insertion, 0 for override).
 */
int VecSimIndex_AddVector(VecSimIndex *index, const void *blob, size_t label);

/**
 * @brief Remove a vector from an index.
 * @param index the index from which the vector is removed.
 * @param label the label of the removed vector
 * @return the number of vectors removed (0 if the label was not found)
 */
int VecSimIndex_DeleteVector(VecSimIndex *index, size_t label);

/**
 * @brief Move the vector(s) stored under `old_label` to `new_label`, leaving the stored vector data
 * and the index structure untouched.
 *
 * Only label bookkeeping is rewritten, so this costs O(1) per stored vector - no distance
 * computations and, for HNSW, no graph mutation. Intended for callers whose external id changed
 * while the vector did not, as a cheap replacement for delete-then-add.
 *
 * The move is rejected, leaving the index untouched, if `old_label` does not exist, if `new_label`
 * is already in use, or if the labels are equal - each with its own code. Not all index types
 * support this; those report `VecSimRelabel_Unsupported`, which a caller has to serve by delete +
 * insert rather than treat as a no-op.
 *
 * @param index the index holding the vector(s).
 * @param old_label the label currently holding the vector(s).
 * @param new_label the label to move them to.
 * @return `VecSimRelabel_OK` if the label was moved, otherwise the reason it was not.
 */
VecSimRelabelCode VecSimIndex_RelabelVector(VecSimIndex *index, size_t old_label, size_t new_label);

/**
 * @brief Set the vectors stored under `label` to the given ones: whatever the label holds now is
 * removed, and these take its place.
 *
 * This is what overwriting with `VecSimIndex_AddVector` does, generalized to a label holding
 * several vectors - where adding appends instead of replacing. The label may end up holding more or
 * fewer vectors than it did; `n == 0` leaves it holding nothing, which is
 * `VecSimIndex_DeleteVector`.
 *
 * An index that holds a single vector per label accepts only `n == 1` and reports
 * `VecSimUpdate_MultiNotSupported` otherwise. Index types that do not implement this report
 * `VecSimUpdate_Unsupported`, which a caller has to serve by deleting the label and inserting. An
 * index that tried and could not store them reports `VecSimUpdate_Failed`, after which the label's
 * contents are undefined - the removal precedes the insertion, so it may hold nothing.
 *
 * @param index the index to update.
 * @param label the label whose vectors are replaced.
 * @param new_blobs `n` vectors laid out one after another, each matching the index's data type and
 * dimension.
 * @param n how many vectors `new_blobs` holds.
 * @return `VecSimUpdate_OK` if the label now holds exactly those vectors.
 */
VecSimUpdateCode VecSimIndex_UpdateVectors(VecSimIndex *index, size_t label, const void *new_blobs,
                                           size_t n);

/**
 * @brief Calculate the distance of a vector from an index to a vector. This function assumes that
 * the vector fits the index - its type and dimension are the same as the index's, and if the
 * index's distance metric is cosine, the vector is already normalized.
 * IMPORTANT: for tiered index, this should be called while *locks are locked for shared ownership*,
 * as we avoid acquiring the locks internally. That is since this is usually called for every vector
 * individually, and the overhead of acquiring and releasing the locks is significant in that case.
 * @param index the index from which the first vector is located, and that defines the distance
 * metric.
 * @param label the label of the vector in the index.
 * @param blob binary representation of the second vector. Blob size should match the index data
 * type and dimension, and pre-normalized if needed.
 * @return The distance between `blob` and the vector with `label`, or INVALID_SCORE if the label is
 * absent.
 * @note Quantized indexes allocate and preprocess a temporary query blob on each call.
 */
double VecSimIndex_GetDistanceFrom_Unsafe(VecSimIndex *index, size_t label, const void *blob);

/**
 * @brief normalize the vector blob in place.
 * @param blob binary representation of a vector. Blob size should match the specified type and
 * dimension.
 * @param dim vector dimension.
 * @param type vector type.
 */
void VecSim_Normalize(void *blob, size_t dim, VecSimType type);

/**
 * @brief Returns the required blob size for a query vector that will be normalized.
 *
 * For INT8/UINT8 vectors with Cosine metric, VecSim_Normalize appends the norm (a float)
 * at the end of the blob, so the required size is larger than just dim * sizeof(type).
 *
 * @param type vector element type.
 * @param dim vector dimension.
 * @param metric distance metric.
 * @return required blob size in bytes.
 */
size_t VecSimParams_GetQueryBlobSize(VecSimType type, size_t dim, VecSimMetric metric);

/**
 * @brief Return the number of vectors in the index.
 * @param index the index whose size is requested.
 * @return index size.
 */
size_t VecSimIndex_IndexSize(VecSimIndex *index);

/**
 * @brief Resolves VecSimRawParam array and generate VecSimQueryParams struct.
 * @param index the index whose size is requested.
 * @param rparams array of raw params to resolve.
 * @param paramNum number of params in rparams (or number of parames in rparams to resolve).
 * @param qparams pointer to VecSimQueryParams struct to set.
 * @param query_type indicates if query is hybrid, range or "standard" VSS query.
 * @param err_msg optional out param. On failure (a non-OK return code), set to a message
 * describing why that specific parameter/value was rejected; set to NULL on success. The pointer
 * is thread-local and is overwritten by the next call to this function on the same thread.
 * @return VecSim_OK if the resolve was successful, VecSimResolveCode error code if not.
 */
VecSimResolveCode VecSimIndex_ResolveParams(VecSimIndex *index, VecSimRawParam *rparams,
                                            int paramNum, VecSimQueryParams *qparams,
                                            VecsimQueryType query_type, const char **err_msg);

/**
 * @brief Search for the k closest vectors to a given vector in the index. The results can be
 * ordered by their score or id.
 * @param index the index to query in.
 * @param queryBlob binary representation of the query vector. Blob size should match the index data
 * type and dimension.
 * @param k the number of "nearest neighbours" to return (upper bound).
 * @param queryParams run time params for the search, which are algorithm-specific.
 * @param order the criterion to sort the results list by it. Options are by score, or by id.
 * @return An opaque object the represents a list of results. User can access the id and score
 * (which is the distance according to the index metric) of every result through
 * VecSimQueryReply_Iterator.
 */
VecSimQueryReply *VecSimIndex_TopKQuery(VecSimIndex *index, const void *queryBlob, size_t k,
                                        VecSimQueryParams *queryParams, VecSimQueryReply_Order);

/**
 * @brief Search for the vectors that are in a given range in the index with respect to a given
 * vector. The results can be ordered by their score or id.
 * @param index the index to query in.
 * @param queryBlob binary representation of the query vector. Blob size should match the index data
 * type and dimension.
 * @param radius the radius around the query vector to search vectors within it.
 * @param queryParams run time params for the search, which are algorithm-specific.
 * @param order the criterion to sort the results list by it. Options are by score, or by id.
 * @return An opaque object the represents a list of results. User can access the id and score
 * (which is the distance according to the index metric) of every result through
 * VecSimQueryReply_Iterator.
 */
VecSimQueryReply *VecSimIndex_RangeQuery(VecSimIndex *index, const void *queryBlob, double radius,
                                         VecSimQueryParams *queryParams, VecSimQueryReply_Order);
/**
 * @brief Return index information.
 * @param index the index to return its info.
 * @return Index general and specific meta-data.
 */
VecSimIndexDebugInfo VecSimIndex_DebugInfo(VecSimIndex *index);

/**
 * @brief Return basic immutable index information.
 * @param index the index to return its info.
 * @return Index basic meta-data.
 */
VecSimIndexBasicInfo VecSimIndex_BasicInfo(VecSimIndex *index);

/**
 * @brief Return statistics information.
 * @param index the index to return its info.
 * @return Index statistic data.
 */
VecSimIndexStatsInfo VecSimIndex_StatsInfo(VecSimIndex *index);

/**
 * @brief Returns an info iterator for generic reply purposes.
 *
 * @param index this index to return its info.
 * @return VecSimDebugInfoIterator* An iterable containing the index general and specific meta-data.
 */
VecSimDebugInfoIterator *VecSimIndex_DebugInfoIterator(VecSimIndex *index);

/**
 * @brief Create a new batch iterator for a specific index, for a specific query vector,
 * using the Index_BatchIteratorNew method of the index. Should be released with
 * VecSimBatchIterator_Free call.
 * @param index the index in which the search will be done (in batches)
 * @param queryBlob binary representation of the vector. Blob size should match the index data type
 * and dimension.
 * @param queryParams run time params for the search, which are algorithm-specific.
 * @return Fresh batch iterator
 */
VecSimBatchIterator *VecSimBatchIterator_New(VecSimIndex *index, const void *queryBlob,
                                             VecSimQueryParams *queryParams);

/**
 * @brief Run async garbage collection for tiered async index.
 */
void VecSimTieredIndex_GC(VecSimIndex *index);

/**
 * @brief Return True if heuristics says that it is better to use ad-hoc brute-force
 * search over the index instead of using batch iterator.
 *
 * @param subsetSize the estimated number of vectors in the index that pass the filter
 * (that is, query results can be only from a subset of vector of this size).
 *
 * @param k the number of required results to return from the query.
 *
 * @param initial_check flag to indicate if this check is performed for the first time (upon
 * creating the hybrid iterator), or after running batches.
 */
bool VecSimIndex_PreferAdHocSearch(VecSimIndex *index, size_t subsetSize, size_t k,
                                   bool initial_check);

/**
 * @brief Acquire/Release the required locks of the tiered index externally before executing an
 * an unsafe *READ* operation (as the locks are acquired for shared ownership).
 * @param index the tiered index to protect (no nothing for non-tiered indexes).
 */
void VecSimTieredIndex_AcquireSharedLocks(VecSimIndex *index);

void VecSimTieredIndex_ReleaseSharedLocks(VecSimIndex *index);

/**
 * @brief Ad-hoc Brute Force Context - opaque handle.
 * Currently implemented for disk indexes only; RAM indexes return NULL from _New.
 */
typedef struct VecSimAdhocBfCtx VecSimAdhocBfCtx;

/**
 * @brief Create an ad-hoc brute force context - preprocesses query once.
 * @param index the index to create the context for.
 * @param queryBlob binary representation of the query vector.
 * @return A new context, or NULL for index types that don't support this API (e.g., RAM indexes).
 */
VecSimAdhocBfCtx *VecSimIndex_AdhocBfCtx_New(VecSimIndex *index, const void *queryBlob);

/**
 * @brief Free an ad-hoc brute force context.
 * @param ctx the context to free.
 */
void VecSimIndex_AdhocBfCtx_Free(VecSimAdhocBfCtx *ctx);

/**
 * @brief Get distance from preprocessed query to a label.
 * For disk indexes: tries flat buffer first (exact), then SQ8 backend (approximate).
 * @param ctx the ad-hoc brute force context.
 * @param label the label to compute distance to.
 * @return The distance, or NAN if the label is not found.
 */
double VecSimIndex_AdhocBfCtx_GetDistanceFrom(VecSimAdhocBfCtx *ctx, size_t label);

/**
 * @brief Get exact distances for a batch of labels.
 * For disk indexes: fetches FP32 vectors from disk, computes exact distances.
 * @param ctx the ad-hoc brute force context.
 * @param labels input array of labels.
 * @param distances_out output array, filled with exact distances (NAN if not found).
 * @param count number of labels in the input array.
 */
void VecSimIndex_AdhocBfCtx_GetExactDistances(VecSimAdhocBfCtx *ctx, const size_t *labels,
                                              double *distances_out, size_t count);

/**
 * @brief Allow 3rd party memory functions to be used for memory management.
 *
 * @param memoryfunctions VecSimMemoryFunctions struct.
 */
void VecSim_SetMemoryFunctions(VecSimMemoryFunctions memoryfunctions);

/**
 * @brief Allow 3rd party timeout callback to be used for limiting runtime of a query.
 *
 * @param callback timeoutCallbackFunction function. should get void* and return int.
 */
void VecSim_SetTimeoutCallbackFunction(timeoutCallbackFunction callback);

/**
 * @brief Allow 3rd party log callback to be used for logging.
 *
 * @param callback logCallbackFunction function. should get void* and return void.
 */
void VecSim_SetLogCallbackFunction(logCallbackFunction callback);

/**
 * @brief Set the context for logging (e.g., test name or file name).
 *
 * @param test_name the name of the test.
 * @param test_type the type of the test (e.g., "unit" or "flow").
 */
void VecSim_SetTestLogContext(const char *test_name, const char *test_type);

/**
 * @brief Allow 3rd party to set the write mode for tiered index - async insert/delete using
 * background jobs, or insert/delete inplace.
 * @note In tiered index scenario, should be called from main thread only !! (that is, the thread
 * that is calling add/delete vector functions).
 *
 * @param mode VecSimWriteMode the mode in which we add/remove vectors (async or in-place).
 */
void VecSim_SetWriteMode(VecSimWriteMode mode);

/**
 * @brief Update the shared SVS thread pool size and set the write mode accordingly.
 * If new_size == 0, sets write mode to in-place (no background threads).
 * If new_size > 0, sets write mode to async and resizes the shared thread pool.
 * @note Should be called from the main thread only.
 *
 * @param new_size the new thread pool size (0 = in-place mode, >0 = async mode).
 */
void VecSim_UpdateThreadPoolSize(size_t new_size);

/**
 * @brief Return the total bytes of process-wide VecSim allocations that are NOT tied to any
 *        single index (e.g. the shared SVS thread-pool allocation).
 *        This does NOT include per-index memory; use VecSimIndex_Info() for per-index accounting.
 * @return Process-wide shared VecSim allocation size in bytes.
 */
size_t VecSim_GetSharedMemory(void);

#ifdef __cplusplus
}
#endif
