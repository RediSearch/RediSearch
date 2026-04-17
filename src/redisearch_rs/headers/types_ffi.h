#pragma once

#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>
#include "redisearch_types.h"
#include "inverted_index.h"

/**
 * A view over the records stored inside an [`RSAggregateResult`].
 *
 * It is designed to minimize the overhead of iterating over the records on
 * the C side, by providing a direct pointer to the records and avoiding unnecessary
 * C->Rust FFI calls.
 */
typedef struct AggregateRecordsSlice {
  const const struct RSIndexResult * *ptr;
  uintptr_t len;
} AggregateRecordsSlice;

#ifdef __cplusplus
extern "C" {
#endif // __cplusplus

/**
 * Check if this is a numeric filter and not a geo filter
 *
 * # Safety
 *
 * The following invariant must be upheld when calling this function:
 * - `filter` must point to a valid `NumericFilter` and cannot be NULL.
 */
bool NumericFilter_IsNumeric(const struct NumericFilter *filter);

/**
 * Check if the given value matches the numeric filter.
 *
 * # Safety
 *
 * The following invariant must be upheld when calling this function:
 * - `filter` must point to a valid `NumericFilter` and cannot be NULL.
 */
bool NumericFilter_Match(const struct NumericFilter *filter, double value);

/**
 * Allocate a new intersect result with a given capacity and weight. This result should be freed
 * using [`IndexResult_Free`].
 */
struct RSIndexResult *NewIntersectResult(uintptr_t cap, double weight);

/**
 * Allocate a new union result with a given capacity and weight. This result should be freed using
 * [`IndexResult_Free`].
 */
struct RSIndexResult *NewUnionResult(uintptr_t cap, double weight);

/**
 * Allocate a new virtual result with a given weight and field mask. This result should be freed
 * using [`IndexResult_Free`].
 */
struct RSIndexResult *NewVirtualResult(double weight, t_fieldMask field_mask);

/**
 * Allocate a new numeric result. This result should be freed using [`IndexResult_Free`].
 */
struct RSIndexResult *NewNumericResult(void);

/**
 * Allocate a new metric result. This result should be freed using [`IndexResult_Free`].
 */
struct RSIndexResult *NewMetricResult(void);

/**
 * Allocate a new hybrid result. This result should be freed using [`IndexResult_Free`].
 *
 * This constructor is only used by the hydrid reader which will pushed owned copies to it.
 * Therefore, this also returns an owned `RSIndexResult`.
 */
struct RSIndexResult *NewHybridResult(void);

/**
 * Allocate a new token record with a given term and weight. This result should be freed using
 * [`IndexResult_Free`].
 *
 * # Safety
 *
 * `term` must be a heap-allocated `RSQueryTerm` (e.g. created by `NewQueryTerm`) and the
 * caller transfers ownership — it must not be freed separately.
 */
struct RSIndexResult *NewTokenRecord(struct RSQueryTerm *term, double weight);

/**
 * Free an index result's internal allocations and also free the result itself.
 *
 * # Safety
 * The following invariants must be upheld when calling this function:
 * - `result` must point to a valid `RSIndexResult` and cannot be NULL.
 * - `result` must have been created using one of these:
 *   - [`NewIntersectResult`]
 *   - [`NewUnionResult`]
 *   - [`NewVirtualResult`]
 *   - [`NewNumericResult`]
 *   - [`NewMetricResult`]
 *   - [`NewHybridResult`]
 *   - [`NewTokenRecord`]
 *   - [`IndexResult_DeepCopy`]
 */
void IndexResult_Free(struct RSIndexResult *result);

/**
 * Create a deep copy of the results that is totally thread safe. This is very slow so use it with
 * caution.
 *
 * The created copy should be freed using [`IndexResult_Free`].
 *
 * # Safety
 * The following invariant must be upheld when calling this function:
 * - `result` must point to a valid `RSIndexResult` and cannot be NULL.
 */
struct RSIndexResult *IndexResult_DeepCopy(const struct RSIndexResult *source);

/**
 * Check if the result is an aggregate result.
 *
 * # Safety
 *
 * The following invariants must be upheld when calling this function:
 * - `result` must point to a valid `RSIndexResult` and cannot be NULL.
 */
bool IndexResult_IsAggregate(const struct RSIndexResult *result);

/**
 * Get the numeric value of the result if it is a numeric result. If the result is not numeric,
 * this function will return `0.0`.
 *
 * # Safety
 *
 * The following invariant must be upheld when calling this function:
 * - `result` must point to a valid `RSIndexResult` and cannot be NULL.
 */
double IndexResult_NumValue(const struct RSIndexResult *result);

/**
 * Set the numeric value of the result if it is a numeric result. If the result is not numeric,
 * this function will do nothing.
 *
 * # Safety
 *
 * The following invariant must be upheld when calling this function:
 * - `result` must point to a valid `RSIndexResult` and cannot be NULL.
 */
void IndexResult_SetNumValue(struct RSIndexResult *result, double value);

/**
 * Get the query term from a result if it is a term result. If the result is not a term, then
 * this function will return a `NULL` pointer.
 *
 * # Safety
 *
 * The following invariant must be upheld when calling this function:
 * - `result` must point to a valid `RSIndexResult` and cannot be NULL.
 */
struct RSQueryTerm *IndexResult_QueryTermRef(const struct RSIndexResult *result);

/**
 * Get the term offsets from a result if it is a term result. If the result is not a term, then
 * this function will return a `NULL` pointer.
 *
 * # Safety
 *
 * The following invariant must be upheld when calling this function:
 * - `result` must point to a valid `RSIndexResult` and cannot be NULL.
 */
const struct RSOffsetVector *IndexResult_TermOffsetsRef(const struct RSIndexResult *result);

/**
 * Get the aggregate result reference if the result is an aggregate result. If the result is
 * not an aggregate, this function will return a `NULL` pointer.
 *
 * # Safety
 *
 * The following invariant must be upheld when calling this function:
 * - `result` must point to a valid `RSIndexResult` and cannot be NULL.
 */
const union RSAggregateResult *IndexResult_AggregateRef(const struct RSIndexResult *result);

/**
 * Get the aggregate result reference without performing a runtime check
 * on the enum discriminant.
 *
 * Use this method if and only if you've already checked the enum
 * discriminant in C code and you don't want to incur the (small)
 * performance penalty of an additional redundant check.
 *
 * # Safety
 *
 * The following invariant must be upheld when calling this function:
 * 1. `result` must point to a valid `RSIndexResult` and cannot be NULL.
 * 2. `result`'s data payload must be of the aggregate kind
 */
const union RSAggregateResult *IndexResult_AggregateRefUnchecked(const struct RSIndexResult *result);

/**
 * Get a mutable aggregate result reference without performing a runtime check
 * on the enum discriminant.
 *
 * Use this method if and only if you've already checked the enum
 * discriminant in C code and you don't want to incur the (small)
 * performance penalty of an additional redundant check.
 *
 * # Safety
 *
 * The following invariant must be upheld when calling this function:
 * 1. `result` must point to a valid `RSIndexResult` and cannot be NULL.
 * 2. `result`'s data payload must be of the aggregate kind
 */
union RSAggregateResult *IndexResult_AggregateRefMutUnchecked(struct RSIndexResult *result);

/**
 * Reset the result if it is an aggregate result. This will clear the children vector
 * and reset the kind mask.
 *
 * # Safety
 *
 * The following invariant must be upheld when calling this function:
 * - `result` must point to a valid `RSIndexResult` and cannot be NULL.
 */
void IndexResult_AggregateReset(struct RSIndexResult *result);

/**
 * Get the result at the specified index in the aggregate result. This will return a `NULL` pointer
 * if the index is out of bounds.
 *
 * # Safety
 *
 * The following invariants must be upheld when calling this function:
 * - `agg` must point to a valid `RSAggregateResult` and cannot be NULL.
 */
const struct RSIndexResult *AggregateResult_Get(const union RSAggregateResult *agg, uintptr_t index);

/**
 * Get the result at the specified index in the aggregate result, without checking bounds.
 *
 * # Safety
 *
 * The following invariants must be upheld when calling this function:
 * 1. `agg` must point to a valid `RSAggregateResult` and cannot be NULL.
 * 2. `index` must be lower than the length of the aggregate result children vector.
 */
const struct RSIndexResult *AggregateResult_GetUnchecked(const union RSAggregateResult *agg, uintptr_t index);

/**
 * Get a mutable result at the specified index in the aggregate result, without checking bounds.
 *
 * # Safety
 *
 * The following invariants must be upheld when calling this function:
 * 1. `agg` must point to a valid `RSAggregateResult` and cannot be NULL.
 * 2. `index` must be lower than the length of the aggregate result children vector.
 * 3. `agg` must be of the `Owned` variant.
 */
struct RSIndexResult *AggregateResult_GetMutUnchecked(union RSAggregateResult *agg, uintptr_t index);

/**
 * Get the element count of the aggregate result.
 *
 * # Safety
 *
 * The following invariants must be upheld when calling this function:
 * - `agg` must point to a valid `RSAggregateResult` and cannot be NULL.
 */
uintptr_t AggregateResult_NumChildren(const union RSAggregateResult *agg);

/**
 * Get the capacity of the aggregate result.
 *
 * # Safety
 *
 * The following invariants must be upheld when calling this function:
 * - `agg` must point to a valid `RSAggregateResult` and cannot be NULL.
 */
uintptr_t AggregateResult_Capacity(const union RSAggregateResult *agg);

/**
 * Get the kind mask of the aggregate result.
 *
 * # Safety
 *
 * The following invariants must be upheld when calling this function:
 * - `agg` must point to a valid `RSAggregateResult` and cannot be NULL.
 */
uint8_t AggregateResult_KindMask(const union RSAggregateResult *agg);

/**
 * Create a new aggregate result with the specified capacity. This function will make the result
 * in Rust memory, but the ownership ends up being transferred to C's memory space. This ownership
 * should return to Rust to free up any heap memory using [`AggregateResult_Free`].
 */
union RSAggregateResult AggregateResult_New(uintptr_t cap);

/**
 * Take ownership of a `RSAggregateResult` to free any heap memory it owns. This function will not
 * free the individual children pointers, but rather the heap allocations owned by the aggregate
 * result itself (such as the internal vector buffer). The caller is responsible for managing the
 * memory of the children pointers before this call if needed.
 *
 * The `agg` parameter should have been created with [`AggregateResult_New`].
 */
void AggregateResult_Free(union RSAggregateResult agg);

/**
 * Add a child to a result if it is an aggregate result. Note, if `parent` only hold references
 * to results, then it will not take ownership of the `child` and will therefore not free it.
 * Instead, the caller is responsible for managing the memory of the `child` pointer *after* the
 * `parent` has been freed.
 *
 * If the `parent` is not an aggregate kind, then this is a no-op.
 *
 * # Safety
 *
 * The following invariants must be upheld when calling this function:
 * - `parent` must point to a valid `RSIndexResult` and cannot be NULL.
 * - `child` must point to a valid `RSIndexResult` and cannot be NULL.
 */
void AggregateResult_AddChild(struct RSIndexResult *parent, struct RSIndexResult *child);

/**
 * Get a view of the records stored inside the aggregate result.
 *
 * # Safety
 * The following invariants must be upheld when calling this function:
 * - `agg` must point to a valid `RSAggregateResult` and cannot be NULL.
 */
struct AggregateRecordsSlice AggregateResult_GetRecordsSlice(const union RSAggregateResult *agg);

/**
 * Retrieve the offsets array from an offset vector.
 *
 * Set the array length into the `len` pointer.
 * The returned array is borrowed and should not be modified.
 *
 * # Safety
 *
 * The following invariants must be upheld when calling this function:
 * - `offsets` must point to a valid offset vector (either [`RSOffsetSlice`] or [`RSOffsetVector`])
 *   and cannot be NULL.
 * - `len` cannot be NULL and must point to an allocated memory big enough to hold an u32.
 */
const char *RSOffsetVector_GetData(const struct RSOffsetVector *offsets, uint32_t *len);

/**
 * Set the offsets array on an offset vector.
 *
 * The vector will borrow the passed array so it's up to the caller to
 * ensure it stays alive during its lifetime.
 *
 * # Safety
 *
 * The following invariants must be upheld when calling this function:
 * - `offsets` must point to a valid offset vector (either [`RSOffsetSlice`] or [`RSOffsetVector`])
 *   and cannot be NULL.
 * - `data` must point to an array of `len` offsets.
 * - if `data` is NULL then `len` should be 0.
 */
void RSOffsetVector_SetData(struct RSOffsetVector *offsets, const char *data, uint32_t len);

/**
 * Free the data inside an offset vector.
 *
 * # Safety
 *
 * The following invariants must be upheld when calling this function:
 * - `offsets` must point to a valid [`RSOffsetVector`] and cannot be NULL.
 * - The data pointer of `offsets` had been allocated via the global allocator
 *   and points to an array matching the length of `offsets`.
 */
void RSOffsetVector_FreeData(struct RSOffsetVector *offsets);

/**
 * Copy the data from one offset vector to another.
 *
 * Deep copies the data array from `src` to `dest`.
 * It's up to the caller to free the copied array using [`RSOffsetVector_FreeData`].
 *
 * # Safety
 *
 * The following invariants must be upheld when calling this function:
 * - `dest` must point to a valid [`RSOffsetVector`] and cannot be NULL.
 * - `src` must point to a valid offset vector (either [`RSOffsetSlice`] or [`RSOffsetVector`])
 *   and cannot be NULL.
 * - `src` data should point to a valid array of `src.len` offsets.
 */
void RSOffsetVector_CopyData(struct RSOffsetVector *dest, const struct RSOffsetVector *src);

/**
 * Retrieve the number of offsets in an offset vector.
 *
 * # Safety
 *
 * The following invariants must be upheld when calling this function:
 * - `offsets` must point to a valid offset vector (either [`RSOffsetSlice`] or [`RSOffsetVector`])
 *   and cannot be NULL.
 */
uint32_t RSOffsetVector_Len(const struct RSOffsetVector *offsets);

#ifdef __cplusplus
}  // extern "C"
#endif  // __cplusplus
