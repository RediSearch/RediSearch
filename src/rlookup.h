/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#pragma once

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

/** The index into the array where the value resides  */
static inline uint16_t RLookupKey_GetDstIdx(const RLookupKey* key) {
    return key->dstidx;
}

/**
 * If the source of this value points to a sort vector, then this is the
 * index within the sort vector that the value is located
 */
static inline uint16_t RLookupKey_GetSvIdx(const RLookupKey* key) {
    return key->svidx;
}

/** The name of this field. */
static inline const char * RLookupKey_GetName(const RLookupKey* key) {
    return key->name;
}

/** The path of this field. */
static inline const char * RLookupKey_GetPath(const RLookupKey* key) {
    return key->path;
}

/** The length of the name field in bytes. */
static inline size_t RLookupKey_GetNameLen(const RLookupKey* key) {
    return key->name_len;
}

/**
 * Indicate the type and other attributes
 * Can be F_SVSRC which means the target array is a sorting vector
 */
static inline uint32_t RLookupKey_GetFlags(const RLookupKey* key) {
    return key->flags;
}

#ifdef __cplusplus
}
#endif
