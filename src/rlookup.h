/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#ifndef RLOOKUP_H
#define RLOOKUP_H
#include <stdint.h>
#include <assert.h>

#include <spec.h>
#include <search_ctx.h>
#include "value.h"
#include "sortable.h"
#include "util/arr.h"

#include "rlookup_rs.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef enum {
  RLOOKUP_C_STR = 0,
  RLOOKUP_C_INT = 1,
  RLOOKUP_C_DBL = 2,
  RLOOKUP_C_BOOL = 3
} RLookupCoerceType;

#define RLOOKUP_FOREACH(key, rlookup, block) \
    RLookupIterator iter = RLookup_Iter(rlookup); \
    const RLookupKey* key; \
    while (RLookupIterator_Next(&iter, &key)) { \
        block \
    }

/**
 * Advances the iterator to the next key places a pointer to it into `key`.
 *
 * Returns `true` while there are more keys or `false` to indicate the
 * last key ways returned and the caller should not call this function anymore.
 */
static inline bool RLookupIterator_Next(RLookupIterator* iterator, const RLookupKey** key) {
    const RLookupKey *current = iterator->current;
    if (current == NULL) {
        return false;
    } else {
        *key = current;
        iterator->current = current->next;

        return true;
    }
}

/**
 * Advances the iterator to the next key places a pointer to it into `key`.
 *
 * Returns `true` while there are more keys or `false` to indicate the
 * last key ways returned and the caller should not call this function anymore.
 */
static inline bool RLookupIteratorMut_Next(RLookupIteratorMut* iterator, RLookupKey** key) {
    RLookupKey *current = iterator->current;
    if (current == NULL) {
        return false;
    } else {
        *key = current;
        iterator->current = current->next;

        return true;
    }
}

// Static inline getters/setters for RLookupKey fields.
// These replace the equivalent Rust FFI functions (e.g. RLookupKey_GetFlags) with
// direct field accesses, eliminating the FFI call overhead.

static inline RLookupKeyFlags RLookupKey_GetFlags(const RLookupKey *key) {
    return key->flags;
}

static inline void RLookupKey_SetFlags(RLookupKey *key, RLookupKeyFlags flags) {
    key->flags = flags;
}

static inline uint16_t RLookupKey_GetDstIdx(const RLookupKey *key) {
    return key->dstidx;
}

static inline void RLookupKey_SetDstIdx(RLookupKey *key, uint16_t idx) {
    key->dstidx = idx;
}

static inline uint16_t RLookupKey_GetSvIdx(const RLookupKey *key) {
    return key->svidx;
}

static inline void RLookupKey_SetSvIdx(RLookupKey *key, uint16_t idx) {
    key->svidx = idx;
}

static inline const char *RLookupKey_GetName(const RLookupKey *key) {
    return key->name;
}

static inline size_t RLookupKey_GetNameLen(const RLookupKey *key) {
    return key->name_len;
}

static inline const char *RLookupKey_GetPath(const RLookupKey *key) {
    return key->path ? key->path : key->name;
}

#ifdef __cplusplus
}
#endif

#endif
