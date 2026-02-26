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

static inline RLookupKey* RLookupKey_GetNext(RLookupKey* key) {
    return key->_next;
}

static inline void RLookupKey_MergeFlags(RLookupKey* key, uint32_t flags) {
    key->_flags |= flags;
}

static inline void RLookupKey_SetPath(RLookupKey* key, const char * path) {
    key->_path = path;
}

// Allocate a new RLookupKey and add it to the RLookup table.
static RLookupKey *createNewKey(RLookup *lookup, const char *name, size_t name_len, uint32_t flags) {
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
      key->_flags |= RLOOKUP_F_VAL_AVAILABLE;
    }
  }
  if (FIELD_IS(fs, INDEXFLD_T_NUMERIC)) {
    key->_flags |= RLOOKUP_T_NUMERIC;
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
  if(!fs || (!FieldSpec_IsSortable(fs) && !(lookup->_options & RLOOKUP_OPT_ALL_LOADED))) {
    return NULL;
  }

  RLookupKey *key = createNewKey(lookup, name, name_len, flags);
  setKeyByFieldSpec(key, fs);
  return key;
}

static RLookupKey *RLookup_FindKey(RLookup *lookup, const char *name, size_t name_len) {
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
        iterator->current = RLookupKey_GetNext(current);

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
    } else if (((RLookupKey_GetFlags(key) & RLOOKUP_F_VAL_AVAILABLE) && !(RLookupKey_GetFlags(key) & RLOOKUP_F_ISLOADED)) &&
                 !(flags & (RLOOKUP_F_OVERRIDE | RLOOKUP_F_FORCE_LOAD)) ||
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
      if (RLookupKey_GetFlags(key) & RLOOKUP_F_VAL_AVAILABLE && !(flags & RLOOKUP_F_FORCE_LOAD)) {
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
    if (!key && (lookup->_options & RLOOKUP_OPT_UNRESOLVED_OK)) {
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
    next = RLookupKey_GetNext(cur);
    RLookupKey_Free(cur);
    cur = next;
  }
  IndexSpecCache_Decref(lk->_spcache);

  lk->_head = lk->_tail = NULL;
  memset(lk, 0, sizeof(*lk));
}

RSValue *hvalToValue(const RedisModuleString *src, RLookupCoerceType type) {
  if (type == RLOOKUP_C_BOOL || type == RLOOKUP_C_INT) {
    long long ll;
    RedisModule_StringToLongLong(src, &ll);
    return RSValue_NewNumberFromInt64(ll);
  } else if (type == RLOOKUP_C_DBL) {
    double dd;
    RedisModule_StringToDouble(src, &dd);
    return RSValue_NewNumber(dd);
  } else {
    RedisModule_RetainString(RSDummyContext, (RedisModuleString *)src);
    return RSValue_NewRedisString((RedisModuleString *)src);
  }
}

static RSValue *jsonValToValue(RedisModuleCtx *ctx, RedisJSON json) {
  size_t len;
  char *str;
  const char *constStr;
  RedisModuleString *rstr;
  long long ll;
  double dd;
  int i;

  // Currently `getJSON` cannot fail here also the other japi APIs below
  switch (japi->getType(json)) {
    case JSONType_String:
      japi->getString(json, &constStr, &len);
      str = rm_strndup(constStr, len);
      return RSValue_NewString(str, len);
    case JSONType_Int:
      japi->getInt(json, &ll);
      return RSValue_NewNumberFromInt64(ll);
    case JSONType_Double:
      japi->getDouble(json, &dd);
      return RSValue_NewNumber(dd);
    case JSONType_Bool:
      japi->getBoolean(json, &i);
      return RSValue_NewNumberFromInt64(i);
    case JSONType_Array:
    case JSONType_Object:
      japi->getJSON(json, ctx, &rstr);
      return RSValue_NewRedisString(rstr);
    case JSONType_Null:
      return RSValue_NullStatic();
    case JSONType__EOF:
      break;
  }
  RS_ABORT("Cannot get here");
  return NULL;
}

// {"a":1, "b":[2, 3, {"c": "foo"}, 4], "d": null}
static RSValue *jsonValToValueExpanded(RedisModuleCtx *ctx, RedisJSON json) {

  RSValue *ret;
  size_t len;
  JSONType type = japi->getType(json);
  if (type == JSONType_Object) {
    // Object
    japi->getLen(json, &len);
    RSValue **pairs = NULL;
    if (len) {
      JSONKeyValuesIterator iter = japi->getKeyValues(json);
      RedisModuleString *keyName;
      size_t i = 0;
      RedisJSON value;
      RedisJSONPtr value_ptr = japi->allocJson();

      RSValueMapBuilder *map = RSValue_NewMapBuilder(len);
      for (; (japi->nextKeyValue(iter, &keyName, value_ptr) == REDISMODULE_OK); ++i) {
        value = *value_ptr;
        RSValue_MapBuilderSetEntry(map, i, RSValue_NewRedisString(keyName),
          jsonValToValueExpanded(ctx, value));
      }
      japi->freeJson(value_ptr);
      value_ptr = NULL;
      japi->freeKeyValuesIter(iter);
      RS_ASSERT(i == len);

      ret = RSValue_NewMapFromBuilder(map);
    } else {
      ret = RSValue_NewMapFromBuilder(RSValue_NewMapBuilder(0));
    }
  } else if (type == JSONType_Array) {
    // Array
    japi->getLen(json, &len);
    if (len) {
      RSValue **arr = RSValue_NewArrayBuilder(len);
      RedisJSONPtr value_ptr = japi->allocJson();
      for (size_t i = 0; i < len; ++i) {
        japi->getAt(json, i, value_ptr);
        RedisJSON value = *value_ptr;
        arr[i] = jsonValToValueExpanded(ctx, value);
      }
      japi->freeJson(value_ptr);
      ret = RSValue_NewArrayFromBuilder(arr, len);
    } else {
      // Empty array
      RSValue **arr = RSValue_NewArrayBuilder(0);
      ret = RSValue_NewArrayFromBuilder(arr, 0);
    }
  } else {
    // Scalar
    ret = jsonValToValue(ctx, json);
  }
  return ret;
}

// Return an array of expanded values from an iterator.
// The iterator is being reset and is not being freed.
// Required japi_ver >= 4
RSValue* jsonIterToValueExpanded(RedisModuleCtx *ctx, JSONResultsIterator iter) {
  RSValue *ret;
  RSValue **arr;
  size_t len = japi->len(iter);
  if (len) {
    japi->resetIter(iter);
    RedisJSON json;
    RSValue **arr = RSValue_NewArrayBuilder(len);
    for (size_t i = 0; (json = japi->next(iter)); ++i) {
      arr[i] = jsonValToValueExpanded(ctx, json);
    }
    ret = RSValue_NewArrayFromBuilder(arr, len);
  } else {
    // Empty array
    RSValue **arr = RSValue_NewArrayBuilder(0);
    ret = RSValue_NewArrayFromBuilder(arr, 0);
  }
  return ret;
}


// Get the value from an iterator and free the iterator
// Return REDISMODULE_OK, and set rsv to the value, if value exists
// Return REDISMODULE_ERR otherwise
//
// Multi value is supported with apiVersion >= APIVERSION_RETURN_MULTI_CMP_FIRST
int jsonIterToValue(RedisModuleCtx *ctx, JSONResultsIterator iter, unsigned int apiVersion, RSValue **rsv) {

  int res = REDISMODULE_ERR;
  RedisModuleString *serialized = NULL;

  if (apiVersion < APIVERSION_RETURN_MULTI_CMP_FIRST) {
    // Preserve single value behavior for backward compatibility
    RedisJSON json = japi->next(iter);
    if (!json) {
      goto done;
    }
    *rsv = jsonValToValue(ctx, json);
    res = REDISMODULE_OK;
    goto done;
  }

  size_t len = japi->len(iter);
  if (len > 0) {
    // First get the JSON serialized value (since it does not consume the iterator)
    if (japi->getJSONFromIter(iter, ctx, &serialized) == REDISMODULE_ERR) {
      goto done;
    }

    // Second, get the first JSON value
    RedisJSON json = japi->next(iter);
    RedisJSONPtr json_alloc = NULL; // Used if we need to allocate a new JSON value (e.g if the value is an array)
    // If the value is an array, we currently try using the first element
    JSONType type = japi->getType(json);
    if (type == JSONType_Array) {
      json_alloc = japi->allocJson();
      // Empty array will return NULL
      if (japi->getAt(json, 0, json_alloc) == REDISMODULE_OK) {
        json = *json_alloc;
      } else {
        json = NULL;
      }
    }

    if (json) {
      RSValue *val = jsonValToValue(ctx, json);
      RSValue *otherval = RSValue_NewRedisString(serialized);
      RSValue *expand = jsonIterToValueExpanded(ctx, iter);
      *rsv = RSValue_NewTrio(val, otherval, expand);
      res = REDISMODULE_OK;
    } else if (serialized) {
      RedisModule_FreeString(ctx, serialized);
    }

    if (json_alloc) {
      japi->freeJson(json_alloc);
    }
  }

done:
  return res;
}

RSValue *replyElemToValue(RedisModuleCallReply *rep, RLookupCoerceType otype) {
  switch (RedisModule_CallReplyType(rep)) {
    case REDISMODULE_REPLY_STRING: {
      if (otype == RLOOKUP_C_BOOL || RLOOKUP_C_INT) {
        goto create_int;
      }

    create_string:;
      size_t len;
      const char *s = RedisModule_CallReplyStringPtr(rep, &len);
      if (otype == RLOOKUP_C_DBL) {
        // Convert to double -- calling code should check if NULL
        return RSValue_NewParsedNumber(s, len);
      }
      // Note, the pointer is within CallReply; we need to copy
      return RSValue_NewCopiedString(s, len);
    }

    case REDISMODULE_REPLY_INTEGER:
    create_int:
      if (otype == RLOOKUP_C_STR || otype == RLOOKUP_C_DBL) {
        goto create_string;
      }
      return RSValue_NewNumberFromInt64(RedisModule_CallReplyInteger(rep));

    case REDISMODULE_REPLY_UNKNOWN:
    case REDISMODULE_REPLY_NULL:
    case REDISMODULE_REPLY_ARRAY:
    default:
      // Nothing
      return RSValue_NullStatic();
  }
}

// returns true if the value of the key is already available
// avoids the need to call to redis api to get the value
// i.e we can use the sorting vector as a cache
static inline bool isValueAvailable(const RLookupKey *kk, const RLookupRow *dst, RLookupLoadOptions *options) {
  return (!options->forceLoad && (
        // No need to "write" this key. It's always implicitly loaded!
        (RLookupKey_GetFlags(kk) & RLOOKUP_F_VAL_AVAILABLE) ||
        // There is no value in the sorting vector, and we don't need to load it from the document.
        ((RLookupKey_GetFlags(kk) & RLOOKUP_F_SVSRC) && (RLookupRow_Get(kk, dst) == NULL))
    ));
}

static int getKeyCommonHash(const RLookupKey *kk, RLookupRow *dst, RLookupLoadOptions *options,
                        RedisModuleKey **keyobj) {
  if (isValueAvailable(kk, dst, options)) {
    return REDISMODULE_OK;
  }

  const char *keyPtr = options->dmd ? options->dmd->keyPtr : options->keyPtr;
  // In this case, the flag must be obtained via HGET
  if (!*keyobj) {
    RedisModuleCtx *ctx = options->sctx->redisCtx;
    RedisModuleString *keyName =
        RedisModule_CreateString(ctx, keyPtr, strlen(keyPtr));
    *keyobj = RedisModule_OpenKey(ctx, keyName, DOCUMENT_OPEN_KEY_QUERY_FLAGS);
    RedisModule_FreeString(ctx, keyName);
    if (!*keyobj) {
      QueryError_SetCode(options->status, QUERY_ERROR_CODE_NO_DOC);
      return REDISMODULE_ERR;
    }
    if (RedisModule_KeyType(*keyobj) != REDISMODULE_KEYTYPE_HASH) {
      QueryError_SetCode(options->status, QUERY_ERROR_CODE_REDIS_KEY_TYPE);
      return REDISMODULE_ERR;
    }
  }

  // Get the actual hash value
  RedisModuleString *val = NULL;
  RSValue *rsv = NULL;

  RedisModule_HashGet(*keyobj, REDISMODULE_HASH_CFIELDS, RLookupKey_GetPath(kk), &val, NULL);

  if (val != NULL) {
    // `val` was created by `RedisModule_HashGet` and is owned by us.
    // This function might retain it, but it's thread-safe to free it afterwards without any locks
    // as it will hold the only reference to it after the next line.
    rsv = hvalToValue(val, (RLookupKey_GetFlags(kk) & RLOOKUP_T_NUMERIC) ? RLOOKUP_C_DBL : RLOOKUP_C_STR);
    RedisModule_FreeString(RSDummyContext, val);
  } else if (!strcmp(RLookupKey_GetPath(kk), UNDERSCORE_KEY)) {
    const RedisModuleString *keyName = RedisModule_GetKeyNameFromModuleKey(*keyobj);
    rsv = hvalToValue(keyName, RLOOKUP_C_STR);
  } else {
    return REDISMODULE_OK;
  }

  // Value has a reference count of 1
  RLookup_WriteOwnKey(kk, dst, rsv);
  return REDISMODULE_OK;
}


static int getKeyCommonJSON(const RLookupKey *kk, RLookupRow *dst, RLookupLoadOptions *options,
                        RedisJSON *keyobj) {
  if (!japi) {
    QueryError_SetCode(options->status, QUERY_ERROR_CODE_UNSUPP_TYPE);
    RedisModule_Log(RSDummyContext, "warning", "cannot operate on a JSON index as RedisJSON is not loaded");
    return REDISMODULE_ERR;
  }

  if (isValueAvailable(kk, dst, options)) {
    return REDISMODULE_OK;
  }

  // In this case, the flag must be obtained from JSON
  RedisModuleCtx *ctx = options->sctx->redisCtx;
  const bool keyPtrFromDMD = options->dmd != NULL;
  char *keyPtr = keyPtrFromDMD ? options->dmd->keyPtr : (char *)options->keyPtr;
  if (!*keyobj) {

    RedisModuleString* keyName = RedisModule_CreateString(ctx, keyPtr, keyPtrFromDMD ? sdslen(keyPtr) : strlen(keyPtr));
    *keyobj = japi->openKeyWithFlags(ctx, keyName, DOCUMENT_OPEN_KEY_QUERY_FLAGS);
    RedisModule_FreeString(ctx, keyName);

    if (!*keyobj) {
      QueryError_SetCode(options->status, QUERY_ERROR_CODE_NO_DOC);
      return REDISMODULE_ERR;
    }
  }

  // Get the actual json value
  RedisModuleString *val = NULL;
  RSValue *rsv = NULL;

  JSONResultsIterator jsonIter = (*RLookupKey_GetPath(kk) == '$') ? japi->get(*keyobj, RLookupKey_GetPath(kk)) : NULL;

  if (!jsonIter) {
    // The field does not exist and and it isn't `__key`
    if (!strcmp(RLookupKey_GetPath(kk), UNDERSCORE_KEY)) {
      rsv = RSValue_NewString(rm_strdup(keyPtr), keyPtrFromDMD ? sdslen(keyPtr) : strlen(keyPtr));
    } else {
      return REDISMODULE_OK;
    }
  } else {
    int res = jsonIterToValue(ctx, jsonIter, options->sctx->apiVersion, &rsv);
    japi->freeIter(jsonIter);
    if (res == REDISMODULE_ERR) {
      return REDISMODULE_OK;
    }
  }

  // Value has a reference count of 1
  RLookup_WriteOwnKey(kk, dst, rsv);
  return REDISMODULE_OK;
}

typedef int (*GetKeyFunc)(const RLookupKey *kk, RLookupRow *dst, RLookupLoadOptions *options,
                          void **keyobj);


int loadIndividualKeys(RLookup *it, RLookupRow *dst, RLookupLoadOptions *options) {
  // Load the document from the schema. This should be simple enough...
  void *key = NULL;  // This is populated by getKeyCommon; we free it at the end
  DocumentType type = options->dmd ? options->dmd->type : options->type;
  GetKeyFunc getKey = (type == DocumentType_Hash) ? (GetKeyFunc)getKeyCommonHash :
                                                    (GetKeyFunc)getKeyCommonJSON;
  int rc = REDISMODULE_ERR;
  // On error we silently skip the rest
  // On success we continue
  // (success could also be when no value is found and nothing is loaded into `dst`,
  //  for example, with a JSONPath with no matches)
  if (options->nkeys) {
    for (size_t ii = 0; ii < options->nkeys; ++ii) {
      const RLookupKey *kk = options->keys[ii];
      if (getKey(kk, dst, options, &key) != REDISMODULE_OK) {
        goto done;
      }
    }
  } else { // If we called load to perform IF operation with FT.ADD command
    RLookupIterator iter = RLookup_Iter(it);
    const RLookupKey* kk;
    while (RLookupIterator_Next(&iter, &kk)) {
      /* key is not part of document schema. no need/impossible to 'load' it */
      if (!(RLookupKey_GetFlags(kk) & RLOOKUP_F_SCHEMASRC)) {
        continue;
      }
      if (!options->forceLoad) {
        /* wanted a sort key, but field is not sortable */
        if ((options->mode & RLOOKUP_LOAD_SVKEYS) && !(RLookupKey_GetFlags(kk) & RLOOKUP_F_SVSRC)) {
          continue;
        }
      }
      if (getKey(kk, dst, options, &key) != REDISMODULE_OK) {
        goto done;
      }
    }
  }
  rc = REDISMODULE_OK;

done:
  if (key) {
    switch (type) {
    case DocumentType_Hash: RedisModule_CloseKey(key); break;
    case DocumentType_Json: break;
    case DocumentType_Unsupported: RS_LOG_ASSERT(1, "placeholder");
    }
  }
  return rc;
}

typedef struct {
  RLookup *it;
  RLookupRow *dst;
  RLookupLoadOptions *options;
} RLookup_HGETALL_privdata;

static void RLookup_HGETALL_scan_callback(RedisModuleKey *key, RedisModuleString *field, RedisModuleString *value, void *privdata) {
  REDISMODULE_NOT_USED(key);
  RLookup_HGETALL_privdata *pd = privdata;
  size_t fieldCStrLen;
  const char *fieldCStr = RedisModule_StringPtrLen(field, &fieldCStrLen);
  RLookupKey *rlk = RLookup_FindKey(pd->it, fieldCStr, fieldCStrLen);
  if (!rlk) {
    // First returned document, create the key.
    rlk = RLookup_GetKey_LoadEx(pd->it, fieldCStr, fieldCStrLen, fieldCStr, RLOOKUP_F_FORCE_LOAD | RLOOKUP_F_NAMEALLOC);
  } else if ((RLookupKey_GetFlags(rlk) & RLOOKUP_F_QUERYSRC)
            /* || (rlk->flags & RLOOKUP_F_ISLOADED) TODO: skip loaded keys, EXCLUDING keys that were opened by this function*/) {
    return; // Key name is already taken by a query key, or it's already loaded.
  }

  RLookupCoerceType ctype = RLOOKUP_C_STR;
  if (!pd->options->forceString && RLookupKey_GetFlags(rlk) & RLOOKUP_T_NUMERIC) {
    ctype = RLOOKUP_C_DBL;
  }
  // This function will retain the value if it's a string. This is thread-safe because
  // the value was created just before calling this callback and will be freed right after
  // the callback returns, so this is a thread-local operation that will take ownership of
  // the string value.
  RSValue *vptr = hvalToValue(value, ctype);
  RLookup_WriteOwnKey(rlk, pd->dst, vptr);
}

static int RLookup_HGETALL(RLookup *it, RLookupRow *dst, RLookupLoadOptions *options) {
  int rc = REDISMODULE_ERR;
  RedisModuleCtx *ctx = options->sctx->redisCtx;
  RedisModuleString *krstr =
      RedisModule_CreateString(ctx, options->dmd->keyPtr, sdslen(options->dmd->keyPtr));

  RedisModuleKey *key = RedisModule_OpenKey(ctx, krstr, DOCUMENT_OPEN_KEY_QUERY_FLAGS);
  if (!key || RedisModule_KeyType(key) != REDISMODULE_KEYTYPE_HASH) {
    goto done;
  }
  RedisModuleScanCursor *cursor = RedisModule_ScanCursorCreate();
  RLookup_HGETALL_privdata pd = {
    .it = it,
    .dst = dst,
    .options = options,
  };
  while(RedisModule_ScanKey(key, cursor, RLookup_HGETALL_scan_callback, &pd));
  RedisModule_ScanCursorDestroy(cursor);

  rc = REDISMODULE_OK;

done:
  if (krstr) {
    RedisModule_FreeString(ctx, krstr);
  }
  if (key) {
    RedisModule_CloseKey(key);
  }
  return rc;
}

int RLookup_JSON_GetAll(RLookup *it, RLookupRow *dst, RLookupLoadOptions *options) {
  int rc = REDISMODULE_ERR;
  if (!japi) {
    return rc;
  }

  JSONResultsIterator jsonIter = NULL;
  RedisModuleCtx *ctx = options->sctx->redisCtx;

  RedisModuleString* keyName = RedisModule_CreateString(ctx, options->dmd->keyPtr, sdslen(options->dmd->keyPtr));
  RedisJSON jsonRoot = japi->openKeyWithFlags(ctx, keyName, DOCUMENT_OPEN_KEY_QUERY_FLAGS);
  RedisModule_FreeString(ctx, keyName);
  if (!jsonRoot) {
    goto done;
  }

  jsonIter = japi->get(jsonRoot, JSON_ROOT);
  if (jsonIter == NULL) {
    goto done;
  }

  RSValue *vptr;
  int res = jsonIterToValue(ctx, jsonIter, options->sctx->apiVersion, &vptr);
  japi->freeIter(jsonIter);
  if (res == REDISMODULE_ERR) {
    goto done;
  }
  RLookupKey *rlk = RLookup_FindKey(it, JSON_ROOT, strlen(JSON_ROOT));
  if (!rlk) {
    // First returned document, create the key.
    rlk = RLookup_GetKey_LoadEx(it, JSON_ROOT, strlen(JSON_ROOT), JSON_ROOT, RLOOKUP_F_NOFLAGS);
  }
  RLookup_WriteOwnKey(rlk, dst, vptr);

  rc = REDISMODULE_OK;

done:
  return rc;
}

int RLookup_LoadDocument(RLookup *it, RLookupRow *dst, RLookupLoadOptions *options) {
  int rv = REDISMODULE_ERR;
  if (options->dmd) {
    RLookupRow_SetSortingVector(dst, options->dmd->sortVector);
  }

  if (options->mode & RLOOKUP_LOAD_ALLKEYS) {
    if (options->dmd->type == DocumentType_Hash) {
      rv = RLookup_HGETALL(it, dst, options);
    } else if (options->dmd->type == DocumentType_Json) {
      rv = RLookup_JSON_GetAll(it, dst, options);
    }
  } else {
    rv = loadIndividualKeys(it, dst, options);
  }

  return rv;
}

int RLookup_LoadRuleFields(RedisSearchCtx *sctx, RLookup *it, RLookupRow *dst, IndexSpec *spec, const char *keyptr, QueryError *status) {
  SchemaRule *rule = spec->rule;

  // create rlookupkeys
  int nkeys = array_len(rule->filter_fields);
  RLookupKey **keys = rm_malloc(nkeys * sizeof(*keys));
  for (int i = 0; i < nkeys; ++i) {
    int idx = rule->filter_fields_index[i];
    if (idx == -1) {
      keys[i] = createNewKey(it, rule->filter_fields[i], strlen(rule->filter_fields[i]), RLOOKUP_F_NOFLAGS);
      continue;
    }
    FieldSpec *fs = spec->fields + idx;
    size_t length = 0;
    const char *name = HiddenString_GetUnsafe(fs->fieldName, &length);
    keys[i] = createNewKey(it, name, length, RLOOKUP_F_NOFLAGS);
    RLookupKey_SetPath(keys[i], HiddenString_GetUnsafe(fs->fieldPath, NULL));
  }

  // load
  RLookupLoadOptions opt = {.keys = (const RLookupKey **)keys,
                            .nkeys = nkeys,
                            .sctx = sctx,
                            .keyPtr = keyptr,
                            .type = rule->type,
                            .status = status,
                            .forceLoad = 1,
                            .mode = RLOOKUP_LOAD_KEYLIST };
  int rv = loadIndividualKeys(it, dst, &opt);
  rm_free(keys);
  return rv;
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

// added as entry point for the rust code
// Required from Rust therefore not an inline method anymore.
// Internally it handles different lengths encoded in 5,8,16,32 and 64 bit.
size_t sdslen__(const char* s) {
  return sdslen((char *)s);
}
