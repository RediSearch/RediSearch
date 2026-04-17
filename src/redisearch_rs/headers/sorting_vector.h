#pragma once

#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

#ifndef THINVEC_SHAREDRSVALUE__U64_DEFINED
#define THINVEC_SHAREDRSVALUE__U64_DEFINED
/**
 * See the crate's top level documentation for a description of this type.
 */
typedef struct ThinVec_SharedRsValue__u64 {
  struct Header_u64 *ptr;
} ThinVec_SharedRsValue__u64;
#endif /* THINVEC_SHAREDRSVALUE__U64_DEFINED */

/**
 * [`RSSortingVector`] acts as a cache for sortable fields in a document.
 *
 * It has a constant length, determined upfront on creation. It can't be resized.
 * The [`RSSortingVector`] may contain values of different types, such as numbers, strings, or references to other values.
 * This depends on the fields in the source document.
 *
 * The fields in the sorting vector occur in the same order as they appeared in the document. Fields that are not sortable,
 * are not added at all to the sorting vector, i.e. the sorting vector does not contain null values for non-sortable fields.
 *
 * # Layout
 *
 * This struct is `#[repr(transparent)]` over [`ThinVec<SharedRsValue>`], which is
 * pointer-sized (8 bytes). The length is stored in the heap header alongside the data.
 *
 * The `ThinVec<T, u64>` heap layout is:
 * ```text
 *   Header { len: u64, cap: u64 }  (16 bytes, no padding for pointer-aligned T)
 *   data: [SharedRsValue; len]
 * ```
 *
 * An empty vector points to a static sentinel header (not null), so no allocation is needed.
 */
typedef struct ThinVec_SharedRsValue__u64 RSSortingVector;
