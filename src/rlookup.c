/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#include "rlookup.h"
#include "module.h"
#include "document.h"
#include "rmutil/rm_assert.h"
#include <util/arr.h>
#include "doc_types.h"
#include "value.h"
#include "util/arr.h"

/**
 * Advances the iterator to the next key places a pointer to it into `key`.
 *
 * Returns `true` while there are more keys or `false` to indicate the
 * last key ways returned and the caller should not call this function anymore.
 */
inline bool RLookupIterator_Next(RLookupIterator* iterator, const RLookupKey** key) {
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
inline bool RLookupIteratorMut_Next(RLookupIteratorMut* iterator, RLookupKey** key) {
    RLookupKey *current = iterator->current;
    if (current == NULL) {
        return false;
    } else {
        *key = current;
        iterator->current = current->next;

        return true;
    }
}

RLookupKey *RLookup_FindKey(RLookup *lookup, const char *name, size_t name_len) {
  RLookupIteratorMut iter = RLookup_IterMut(lookup);
  RLookupKey* key;

  while (RLookupIteratorMut_Next(&iter, &key)) {
    // match `name` to the name of the key
    if (RLookupKey_GetNameLen(key) == name_len && !strncmp(RLookupKey_GetName(key), name, name_len)) {
      return key;
    }
  }
  return NULL;
}
