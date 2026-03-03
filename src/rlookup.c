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

typedef struct RLookupKey {
  uint16_t _dstidx;
  uint16_t _svidx;

  uint32_t _flags;

  const char *_path;
  const char *_name;
  size_t _name_len;

  /** Pointer to next field in the list. */
  struct RLookupKey *_next;
} RLookupKey;

/** The index into the array where the value resides  */
inline uint16_t RLookupKey_GetDstIdx(const RLookupKey* key) {
    return key->_dstidx;
}

/**
 * If the source of this value points to a sort vector, then this is the
 * index within the sort vector that the value is located
 */
inline uint16_t RLookupKey_GetSvIdx(const RLookupKey* key) {
    return key->_svidx;
}

/** The name of this field. */
inline const char * RLookupKey_GetName(const RLookupKey* key) {
    return key->_name;
}

/** The path of this field. */
inline const char * RLookupKey_GetPath(const RLookupKey* key) {
    return key->_path;
}

/** The length of the name field in bytes. */
inline size_t RLookupKey_GetNameLen(const RLookupKey* key) {
    return key->_name_len;
}

/**
 * Indicate the type and other attributes
 * Can be F_SVSRC which means the target array is a sorting vector)
 */
inline uint32_t RLookupKey_GetFlags(const RLookupKey* key) {
    return key->_flags;
}

static inline void RLookupKey_MergeFlags(RLookupKey* key, uint32_t flags) {
    key->_flags |= flags;
}

void RLookupKey_SetPath(RLookupKey* key, const char * path) {
    key->_path = path;
}

// Allocate a new RLookupKey and add it to the RLookup table.
RLookupKey *createNewKey(RLookup *lookup, const char *name, size_t name_len, uint32_t flags) {
  RLookupKey *ret = rm_calloc(1, sizeof(*ret));

  if (!lookup->_head) {
    lookup->_head = lookup->_tail = ret;
  } else {
    lookup->_tail->_next = ret;
    lookup->_tail = ret;
  }

  // Set the name of the key.
  ret->_name = (flags & RLOOKUP_F_NAMEALLOC) ? rm_strndup(name, name_len) : name;
  ret->_name_len = name_len;
  ret->_path = ret->_name;
  ret->_dstidx = lookup->_rowlen;
  ret->_flags = flags & ~RLOOKUP_TRANSIENT_FLAGS;

  // Increase the RLookup table row length. (all rows have the same length).
  ++(lookup->_rowlen);

  return ret;
}

// Allocate a new RLookupKey and add it to the RLookup table.
static RLookupKey *overrideKey(RLookup *lk, RLookupKey *old, uint32_t flags) {
  RLookupKey *new = rm_calloc(1, sizeof(*new));

  /* Copy the old key to the new one */
  new->_name = old->_name; // taking ownership of the name
  new->_name_len = old->_name_len;
  new->_path = new->_name; // keeping the initial default of path = name. Path resolution will happen later.
  new->_dstidx = old->_dstidx;

  /* Set the new flags */
  new->_flags = flags & ~RLOOKUP_TRANSIENT_FLAGS;
  // If the old key was allocated, we take ownership of the name.
  new->_flags |= old->_flags & RLOOKUP_F_NAMEALLOC;

  /* Make the old key inaccessible for new lookups */
  if (old->_path == old->_name) {
    // If the old key allocated the name and not the path, we take ownership of the allocation
    old->_flags &= ~RLOOKUP_F_NAMEALLOC;
  }
  old->_name = NULL;
  // 0 is a valid length if the user provided an empty string as a name.
  // This is safe as whenever we compare key names, we first check that the length are equal.
  old->_name_len = -1;
  old->_flags |= RLOOKUP_F_HIDDEN; // Mark the old key as hidden so it won't be attempted to be returned

  /* Add the new key to the lookup table */
  new->_next = old->_next;
  old->_next = new;
  // If the old key was the tail, set the new key as the tail
  if (lk->_tail == old) {
    lk->_tail = new;
  }

  return new;
}

static void setKeyByFieldSpec(RLookupKey *key, const FieldSpec *fs) {
  key->_flags |= RLOOKUP_F_DOCSRC | RLOOKUP_F_SCHEMASRC;
  const char *path = HiddenString_GetUnsafe(fs->fieldPath, NULL);
  key->_path = key->_flags & RLOOKUP_F_NAMEALLOC ? rm_strdup(path) : path;
  if (FieldSpec_IsSortable(fs)) {
    key->_flags |= RLOOKUP_F_SVSRC;
    key->_svidx = fs->sortIdx;

    if (FieldSpec_IsUnf(fs)) {
      // If the field is sortable and not normalized (UNF), the available data in the
      // sorting vector is the same as the data in the document.
      key->_flags |= RLOOKUP_F_VALAVAILABLE;
    }
  }
  if (FIELD_IS(fs, INDEXFLD_T_NUMERIC)) {
    key->_flags |= RLOOKUP_F_NUMERIC;
  }
}

static void RLookupKey_Cleanup(RLookupKey *k) {
  if (k->_flags & RLOOKUP_F_NAMEALLOC) {
    if (RLookupKey_GetName(k) != k->_path) {
      rm_free((void *)k->_path);
    }
    rm_free((void *)k->_name);
  }
}

void RLookupKey_Free(RLookupKey *k) {
  RLookupKey_Cleanup(k);
  rm_free(k);
}

const FieldSpec *RLookup_FindFieldInSpecCache(const RLookup *lookup, const char *name) {
  const IndexSpecCache *cc = lookup->_spcache;
  if (!cc) {
    return NULL;
  }

  const FieldSpec *fs = NULL;
  for (size_t ii = 0; ii < cc->nfields; ++ii) {
    if (!HiddenString_CompareC(cc->fields[ii].fieldName, name, strlen(name))) {
      fs = cc->fields + ii;
      break;
    }
  }

  return fs;
}

// Gets a key from the schema if the field is sortable (so its data is available), unless an RP upstream
// has promised to load the entire document.
static RLookupKey *genKeyFromSpec(RLookup *lookup, const char *name, size_t name_len, uint32_t flags) {
  const FieldSpec *fs = RLookup_FindFieldInSpecCache(lookup, name);
  // FIXME: LOAD ALL loads the key properties by their name, and we won't find their value by the field name
  //        if the field has a different name (alias) than its path.
  if(!fs || (!FieldSpec_IsSortable(fs) && !(lookup->_options & RLOOKUP_OPT_ALLLOADED))) {
    return NULL;
  }

  RLookupKey *key = createNewKey(lookup, name, name_len, flags);
  setKeyByFieldSpec(key, fs);
  return key;
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
        iterator->current = current->_next;

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
        iterator->current = current->_next;

        return true;
    }
}

/** Returns an immutable iterator over the keys in this RLookup */
inline RLookupIterator RLookup_Iter(const RLookup* rlookup) {
    RLookupIterator iter = { 0 };
    iter.current = rlookup->_head;
    return iter;
}

/** Returns an mutable iterator over the keys in this RLookup */
inline RLookupIteratorMut RLookup_IterMut(const RLookup* rlookup) {
    RLookupIteratorMut iter = { 0 };
    iter.current = rlookup->_head;
    return iter;
}

static RLookupKey *RLookup_GetKey_common(RLookup *lookup, const char *name, size_t name_len, const char *field_name, RLookupMode mode, uint32_t flags) {
  // remove all flags that are not relevant to getting a key
  flags &= RLOOKUP_GET_KEY_FLAGS;
  // First, look for the key in the lookup table for an existing key with the same name
  RLookupKey *key = RLookup_FindKey(lookup, name, name_len);

  switch (mode) {
  // 1. if the key is already loaded, or it has created by earlier RP for writing, return NULL (unless override was requested)
  // 2. create a new key with the name of the field, and mark it as doc-source.
  // 3. if the key is in the schema, mark it as schema-source and apply all the relevant flags according to the field spec.
  // 4. if the key is "loaded" at this point (in schema, sortable and un-normalized), create the key but return NULL
  //    (no need to load it from the document).
  case RLOOKUP_M_LOAD:
    // NOTICE: you should not call GetKey for loading if it's illegal to load the key at the given state.
    // The responsibility of checking this is on the caller.
    if (!key) {
      key = createNewKey(lookup, name, name_len, flags);
    } else if (((RLookupKey_GetFlags(key) & RLOOKUP_F_VALAVAILABLE) && !(RLookupKey_GetFlags(key) & RLOOKUP_F_ISLOADED)) &&
                 !(flags & (RLOOKUP_F_OVERRIDE | RLOOKUP_F_FORCELOAD)) ||
                (RLookupKey_GetFlags(key) & RLOOKUP_F_ISLOADED &&       !(flags &  RLOOKUP_F_OVERRIDE)) ||
                (RLookupKey_GetFlags(key) & RLOOKUP_F_QUERYSRC &&       !(flags &  RLOOKUP_F_OVERRIDE))) {
      // We found a key with the same name. We return NULL if:
      // 1. The key has the origin data available (from the sorting vector, UNF) and the caller didn't
      //    request to override or forced loading.
      // 2. The key is already loaded (from the document) and the caller didn't request to override.
      // 3. The key was created by the query (upstream) and the caller didn't request to override.

      // If the caller wanted to mark this key as explicit return, mark it as such even if we don't return it.
      RLookupKey_MergeFlags(key, flags & RLOOKUP_F_EXPLICITRETURN);
      return NULL;
    } else {
      // overrides the key, and sets the new key according to the flags.
      key = overrideKey(lookup, key, flags);
    }

    // At this point we know for sure that it is not marked as loaded.
    const FieldSpec *fs = RLookup_FindFieldInSpecCache(lookup, field_name);
    if (fs) {
      setKeyByFieldSpec(key, fs);
      if (RLookupKey_GetFlags(key) & RLOOKUP_F_VALAVAILABLE && !(flags & RLOOKUP_F_FORCELOAD)) {
        // If the key is marked as "value available", it means that it is sortable and un-normalized.
        // so we can use the sorting vector as the source, and we don't need to load it from the document.
        return NULL;
      }
    } else {
      // Field not found in the schema.
      // We assume `field_name` is the path to load from in the document.
      if (!(RLookupKey_GetFlags(key) & RLOOKUP_F_NAMEALLOC)) {
        RLookupKey_SetPath(key, field_name);
      } else if (name != field_name) {
        RLookupKey_SetPath(key, rm_strdup(field_name));
      } // else
        // If the caller requested to allocate the name, and the name is the same as the path,
        // it was already set to the same allocation for the name, so we don't need to do anything.
    }
    // Mark the key as loaded from the document (for the rest of the pipeline usage).
    RLookupKey_MergeFlags(key, RLOOKUP_F_DOCSRC | RLOOKUP_F_ISLOADED);
    return key;

  // A. we found the key at the lookup table:
  //    1. if we are in exclusive mode, return NULL
  //    2. if we are in create mode, overwrite the key (remove schema related data, mark with new flags)
  // B. we didn't find the key at the lookup table:
  //    create a new key with the name and flags
  case RLOOKUP_M_WRITE:
    if (!key) {
      key = createNewKey(lookup, name, name_len, flags);
    } else if (!(flags & RLOOKUP_F_OVERRIDE)) {
      return NULL;
    } else {
      // overrides the key, and sets the new key according to the flags.
      key = overrideKey(lookup, key, flags);
    }

    RLookupKey_MergeFlags(key, RLOOKUP_F_QUERYSRC);
    return key;

  // Return the key if it exists in the lookup table, or if it exists in the schema as SORTABLE.
  case RLOOKUP_M_READ:
    if (!key) {
      // If we didn't find the key at the lookup table, check if it exists in
      // the schema as SORTABLE, and create only if so.
      key = genKeyFromSpec(lookup, name, name_len, flags);
    }

    // If we didn't find the key in the schema (there is no schema) and unresolved is OK, create an unresolved key.
    if (!key && (lookup->_options & RLOOKUP_OPT_ALLOWUNRESOLVED)) {
      key = createNewKey(lookup, name, name_len, flags);
      RLookupKey_MergeFlags(key, RLOOKUP_F_UNRESOLVED);
    }
    return key;
  }

  return NULL;
}

RLookupKey *RLookup_GetKey_Read(RLookup *lookup, const char *name, uint32_t flags) {
  return RLookup_GetKey_common(lookup, name, strlen(name), NULL, RLOOKUP_M_READ, flags);
}

RLookupKey *RLookup_GetKey_ReadEx(RLookup *lookup, const char *name, size_t name_len,
                                  uint32_t flags) {
  return RLookup_GetKey_common(lookup, name, name_len, NULL, RLOOKUP_M_READ, flags);
}

RLookupKey *RLookup_GetKey_Write(RLookup *lookup, const char *name, uint32_t flags) {
  return RLookup_GetKey_common(lookup, name, strlen(name), NULL, RLOOKUP_M_WRITE, flags);
}

RLookupKey *RLookup_GetKey_WriteEx(RLookup *lookup, const char *name, size_t name_len,
                                   uint32_t flags) {
  return RLookup_GetKey_common(lookup, name, name_len, NULL, RLOOKUP_M_WRITE, flags);
}

RLookupKey *RLookup_GetKey_Load(RLookup *lookup, const char *name, const char *field_name,
                                uint32_t flags) {
  return RLookup_GetKey_common(lookup, name, strlen(name), field_name, RLOOKUP_M_LOAD, flags);
}

RLookupKey *RLookup_GetKey_LoadEx(RLookup *lookup, const char *name, size_t name_len,
                                  const char *field_name, uint32_t flags) {
  return RLookup_GetKey_common(lookup, name, name_len, field_name, RLOOKUP_M_LOAD, flags);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

size_t RLookup_GetLength(const RLookup *lookup, const RLookupRow *r, bool *skipFieldIndex,
                         size_t skipFieldIndex_len, uint32_t requiredFlags, uint32_t excludeFlags,
                         SchemaRule *rule) {
  RS_LOG_ASSERT(skipFieldIndex_len >= lookup->_rowlen, "'skipFieldIndex_len' should be at least equal to lookup len");

  int i = 0;
  size_t nfields = 0;
  RLOOKUP_FOREACH(kk, lookup, {
    if (RLookupKey_GetName(kk) == NULL) {
        // Overridden key. Skip without incrementing the index
        continue;
    }
    if (requiredFlags && !(RLookupKey_GetFlags(kk) & requiredFlags)) {
        i +=1;
        continue;
    }
    if (excludeFlags && (RLookupKey_GetFlags(kk) & excludeFlags)) {
        i +=1;
        continue;
    }
    const RSValue *v = RLookupRow_Get(kk, r);
    if (!v) {
        i +=1;
        continue;
    }
    // on coordinator, we reach this code without sctx or rule,
    // we trust the shards to not send those fields.
    if (rule && ((rule->lang_field && strcmp(RLookupKey_GetName(kk), rule->lang_field) == 0) ||
                    (rule->score_field && strcmp(RLookupKey_GetName(kk), rule->score_field) == 0) ||
                    (rule->payload_field && strcmp(RLookupKey_GetName(kk), rule->payload_field) == 0))) {
        i +=1;
        continue;
    }

    skipFieldIndex[i] = true;
    ++nfields;
    i +=1;
  });
  RS_LOG_ASSERT(i == lookup->_rowlen, "'i' should be equal to lookup len");
  return nfields;
}

void RLookup_SetCache(RLookup *lk, IndexSpecCache *spcache) {
  lk->_spcache = spcache;
}

void RLookup_WriteOwnKey(const RLookupKey *key, RLookupRow *row, RSValue *v) {
  // Find the pointer to write to ...
  RSValue **vptr = array_ensure_at(&row->dyn, RLookupKey_GetDstIdx(key), RSValue *);
  if (*vptr) {
    RSValue_DecrRef(*vptr);
    row->ndyn--;
  }
  *vptr = v;
  row->ndyn++;
}

void RLookup_WriteKey(const RLookupKey *key, RLookupRow *row, RSValue *v) {
  RLookup_WriteOwnKey(key, row, RSValue_IncrRef(v));
}

void RLookup_WriteKeyByName(RLookup *lookup, const char *name, size_t len, RLookupRow *dst, RSValue *v) {
  // Get the key first
  RLookupKey *k = RLookup_FindKey(lookup, name, len);
  if (!k) {
    k = RLookup_GetKey_WriteEx(lookup, name, len, RLOOKUP_F_NAMEALLOC);
  }
  RLookup_WriteKey(k, dst, v);
}

void RLookupRow_WriteByNameOwned(RLookup *lookup, const char *name, size_t len, RLookupRow *row, RSValue *value) {
  RLookup_WriteKeyByName(lookup, name, len, row, value);
  RSValue_DecrRef(value);
}

void RLookupRow_Wipe(RLookupRow *r) {
  for (size_t ii = 0; ii < array_len(r->dyn) && r->ndyn; ++ii) {
    RSValue **vpp = r->dyn + ii;
    if (*vpp) {
      RSValue_DecrRef(*vpp);
      *vpp = NULL;
      r->ndyn--;
    }
  }
  r->sv = NULL;
}

void RLookupRow_Reset(RLookupRow *r) {
  RLookupRow_Wipe(r);
  if (r->dyn) {
    array_free(r->dyn);
    r->dyn = NULL;
  }
  RS_LOG_ASSERT(r->ndyn == 0, "ndyn should be 0 after reset");
}

void RLookupRow_MoveFieldsFrom(const RLookup *lk, RLookupRow *src, RLookupRow *dst) {
  RLookupIterator iter = RLookup_Iter(lk);
  const RLookupKey* k;

  while (RLookupIterator_Next(&iter, &k)) {
    RSValue *vv = RLookupRow_Get(k, src);
    if (vv) {
      RLookup_WriteKey(k, dst, vv);
    }
  }

  RLookupRow_Wipe(src);
}

void RLookup_Cleanup(RLookup *lk) {
  RLookupKey *next, *cur = lk->_head;
  while (cur) {
    next = cur->_next;
    RLookupKey_Free(cur);
    cur = next;
  }
  IndexSpecCache_Decref(lk->_spcache);

  lk->_head = lk->_tail = NULL;
  memset(lk, 0, sizeof(*lk));
}


void RLookup_AddKeysFrom(const RLookup *src, RLookup *dest, uint32_t flags) {
  RS_ASSERT(dest && src);
  RS_ASSERT(dest != src);  // Prevent self-addition

  // Iterate through all keys in source lookup
  RLookupIterator iter = RLookup_Iter(src);
  const RLookupKey* src_key;
  while (RLookupIterator_Next(&iter, &src_key)) {
    if (!RLookupKey_GetName(src_key)) {
      // Skip overridden keys (they have name == NULL)
      continue;
    }

    // Combine caller's control flags with source key's persistent properties
    // Only preserve non-transient flags from source (F_SVSRC, F_HIDDEN, etc.)
    // while respecting caller's control flags (F_OVERRIDE, F_FORCE_LOAD, etc.)
    uint32_t combined_flags = flags | (RLookupKey_GetFlags(src_key) & ~RLOOKUP_TRANSIENT_FLAGS);
    RLookupKey *dest_key = RLookup_GetKey_Write(dest, RLookupKey_GetName(src_key), combined_flags);
  }
}

void RLookupRow_WriteFieldsFrom(const RLookupRow *srcRow, const RLookup *srcLookup,
                               RLookupRow *destRow, RLookup *destLookup,
                               bool createMissingKeys) {
  RS_ASSERT(srcRow && srcLookup);
  RS_ASSERT(destRow && destLookup);

  // Iterate through all source keys
  RLookupIterator iter = RLookup_Iter(srcLookup);
  const RLookupKey* src_key;
  while (RLookupIterator_Next(&iter, &src_key)) {
    if (!RLookupKey_GetName(src_key)) {
      // Skip overridden keys
      continue;
    }

    // Get value from source row
    RSValue *value = RLookupRow_Get(src_key, srcRow);
    if (!value) {
      // No data for this key in source row
      continue;
    }

    // Find corresponding key in destination lookup
    RLookupKey *dest_key = RLookup_FindKey(destLookup, RLookupKey_GetName(src_key), RLookupKey_GetNameLen(src_key));
    if (!createMissingKeys) {
      RS_ASSERT(dest_key != NULL);  // Assumption: all source keys exist in destination
    } else if (!dest_key) {
        // Key doesn't exist in destination - create it on demand.
        // This can happen with LOAD * where keys are created dynamically.
        // Inherit non-transient flags from source.
        uint32_t flags = RLookupKey_GetFlags(src_key) & ~RLOOKUP_TRANSIENT_FLAGS;
        dest_key = RLookup_GetKey_WriteEx(destLookup, RLookupKey_GetName(src_key), RLookupKey_GetNameLen(src_key), flags);
    }
    // Write fields to destination (increments refcount, shares ownership)
    RLookup_WriteKey(dest_key, destRow, value);
  }
  // Caller is responsible for managing source row lifecycle
}