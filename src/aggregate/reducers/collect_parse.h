/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#ifndef RS_COLLECT_PARSE_H_
#define RS_COLLECT_PARSE_H_

#include "aggregate/reducer.h"
#include "util/arr.h"

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Pure parsed representation of a COLLECT reducer's arguments.
 *
 * Populated by `CollectArgs_Parse`. Names in `field_names` and `sort_names`
 * are stripped of their leading `@`. All strings are borrowed from the
 * caller's `ArgsCursor` argv and live exactly as long as it does. No
 * `RLookupKey` resolution happens here.
 *
 * `field_names` is non-NULL only when the user spelled out `<n> @field ...`.
 * `load_all` is mutually exclusive with `field_names`: when `*` is used,
 * `load_all == true` and `field_names == NULL`.
 *
 * `sort_names` is non-NULL only when SORTBY is present; `sortAscMap` is then
 * indexed by `sort_names` position (bit set = ASC, cleared = DESC). When
 * SORTBY is absent, `sortAscMap` is left at `SORTASCMAP_INIT`.
 */
typedef struct {
  arrayof(const char *) field_names;   // stripped names; NULL when `load_all`
  bool load_all;                       // true iff user wrote `FIELDS *`

  arrayof(const char *) sort_names;    // stripped names; NULL when no SORTBY
  uint64_t sortAscMap;                 // direction bitmap, indexed by sort_names position

  bool has_limit;
  uint64_t limit_offset;
  uint64_t limit_count;

  bool distinct;                       // true iff user wrote `DISTINCT`
} CollectArgs;

/**
 * Parse COLLECT arguments into `out`.
 *
 * Pure: no key opening, no allocation outside of `out`'s internal arrays.
 * On failure returns false and sets `options->status`. The caller must call
 * `CollectArgs_Free` in either case to release the internal arrays.
 */
bool CollectArgs_Parse(const ReducerOptions *options, CollectArgs *out);

/**
 * Release the internal arrays of `args`. The borrowed strings inside are not
 * freed (they belong to the caller's `ArgsCursor`).
 */
void CollectArgs_Free(CollectArgs *args);

/**
 * If `tok` is a COLLECT option keyword, return its normalized (lowercase,
 * static) spelling; otherwise NULL.
 */
const char *CollectArgs_NormalizedKeyword(const char *tok);

#ifdef __cplusplus
}
#endif

#endif
