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
bool RLookupIterator_Next(RLookupIterator* iterator, const RLookupKey** key);

/**
 * Advances the iterator to the next key places a pointer to it into `key`.
 *
 * Returns `true` while there are more keys or `false` to indicate the
 * last key ways returned and the caller should not call this function anymore.
 */
bool RLookupIteratorMut_Next(RLookupIteratorMut* iterator, RLookupKey** key);

/**
 * Find a key in the lookup table by name. Returns NULL if not found.
 */
RLookupKey *RLookup_FindKey(RLookup *lookup, const char *name, size_t name_len);

#ifdef __cplusplus
}
#endif

#endif
