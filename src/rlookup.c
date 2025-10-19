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

// Allocate a new RLookupKey and add it to the RLookup table.
static RLookupKey *createNewKey(RLookup *lookup, const char *name, size_t name_len, uint32_t flags) {
  RLookupKey *ret = rm_calloc(1, sizeof(*ret));

  if (!lookup->head) {
    lookup->head = lookup->tail = ret;
  } else {
    lookup->tail->next = ret;
    lookup->tail = ret;
  }

  // Set the name of the key.
  ret->name = (flags & RLOOKUP_F_NAMEALLOC) ? rm_strndup(name, name_len) : name;
  ret->name_len = name_len;
  ret->path = ret->name;
  ret->dstidx = lookup->rowlen;
  ret->flags = flags & ~RLOOKUP_TRANSIENT_FLAGS;

  // Increase the RLookup table row length. (all rows have the same length).
  ++(lookup->rowlen);

  return ret;
}

// Allocate a new RLookupKey and add it to the RLookup table.
static RLookupKey *overrideKey(RLookup *lk, RLookupKey *old, uint32_t flags) {
  RLookupKey *new = rm_calloc(1, sizeof(*new));

  /* Copy the old key to the new one */
  new->name = old->name; // taking ownership of the name
  new->name_len = old->name_len;
  new->path = new->name; // keeping the initial default of path = name. Path resolution will happen later.
  new->dstidx = old->dstidx;

  /* Set the new flags */
  new->flags = flags & ~RLOOKUP_TRANSIENT_FLAGS;
  // If the old key was allocated, we take ownership of the name.
  new->flags |= old->flags & RLOOKUP_F_NAMEALLOC;

  /* Make the old key inaccessible for new lookups */
  if (old->path == old->name) {
    // If the old key allocated the name and not the path, we take ownership of the allocation
    old->flags &= ~RLOOKUP_F_NAMEALLOC;
  }
  old->name = NULL;
  // 0 is a valid length if the user provided an empty string as a name.
  // This is safe as whenever we compare key names, we first check that the length are equal.
  old->name_len = -1;
  old->flags |= RLOOKUP_F_HIDDEN; // Mark the old key as hidden so it won't be attempted to be returned

  /* Add the new key to the lookup table */
  new->next = old->next;
  old->next = new;
  // If the old key was the tail, set the new key as the tail
  if (lk->tail == old) {
    lk->tail = new;
  }

  return new;
}

const FieldSpec *findFieldInSpecCache(const RLookup *lookup, const char *name) {
  const IndexSpecCache *cc = lookup->spcache;
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

static void setKeyByFieldSpec(RLookupKey *key, const FieldSpec *fs) {
  key->flags |= RLOOKUP_F_DOCSRC | RLOOKUP_F_SCHEMASRC;
  const char *path = HiddenString_GetUnsafe(fs->fieldPath, NULL);
  key->path = key->flags & RLOOKUP_F_NAMEALLOC ? rm_strdup(path) : path;
  if (FieldSpec_IsSortable(fs)) {
    key->flags |= RLOOKUP_F_SVSRC;
    key->svidx = fs->sortIdx;

    if (FieldSpec_IsUnf(fs)) {
      // If the field is sortable and not normalized (UNF), the available data in the
      // sorting vector is the same as the data in the document.
      key->flags |= RLOOKUP_F_VAL_AVAILABLE;
    }
  }
  if (FIELD_IS(fs, INDEXFLD_T_NUMERIC)) {
    key->flags |= RLOOKUP_T_NUMERIC;
  }
}

// Gets a key from the schema if the field is sortable (so its data is available), unless an RP upstream
// has promised to load the entire document.
static RLookupKey *genKeyFromSpec(RLookup *lookup, const char *name, size_t name_len, uint32_t flags) {
  const FieldSpec *fs = findFieldInSpecCache(lookup, name);
  // FIXME: LOAD ALL loads the key properties by their name, and we won't find their value by the field name
  //        if the field has a different name (alias) than its path.
  if(!fs || (!FieldSpec_IsSortable(fs) && !(lookup->options & RLOOKUP_OPT_ALL_LOADED))) {
    return NULL;
  }

  RLookupKey *key = createNewKey(lookup, name, name_len, flags);
  setKeyByFieldSpec(key, fs);
  return key;
}

static RLookupKey *RLookup_FindKey(RLookup *lookup, const char *name, size_t name_len) {
  for (RLookupKey *kk = lookup->head; kk; kk = kk->next) {
    // match `name` to the name of the key
    if (kk->name_len == name_len && !strncmp(kk->name, name, name_len)) {
      return kk;
    }
  }
  return NULL;
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
    } else if (((key->flags & RLOOKUP_F_VAL_AVAILABLE) && !(key->flags & RLOOKUP_F_ISLOADED)) &&
                                                          !(flags & (RLOOKUP_F_OVERRIDE | RLOOKUP_F_FORCE_LOAD)) ||
                (key->flags & RLOOKUP_F_ISLOADED &&       !(flags &  RLOOKUP_F_OVERRIDE)) ||
                (key->flags & RLOOKUP_F_QUERYSRC &&       !(flags &  RLOOKUP_F_OVERRIDE))) {
      // We found a key with the same name. We return NULL if:
      // 1. The key has the origin data available (from the sorting vector, UNF) and the caller didn't
      //    request to override or forced loading.
      // 2. The key is already loaded (from the document) and the caller didn't request to override.
      // 3. The key was created by the query (upstream) and the caller didn't request to override.

      // If the caller wanted to mark this key as explicit return, mark it as such even if we don't return it.
      key->flags |= (flags & RLOOKUP_F_EXPLICITRETURN);
      return NULL;
    } else {
      // overrides the key, and sets the new key according to the flags.
      key = overrideKey(lookup, key, flags);
    }

    // At this point we know for sure that it is not marked as loaded.
    const FieldSpec *fs = findFieldInSpecCache(lookup, field_name);
    if (fs) {
      setKeyByFieldSpec(key, fs);
      if (key->flags & RLOOKUP_F_VAL_AVAILABLE && !(flags & RLOOKUP_F_FORCE_LOAD)) {
        // If the key is marked as "value available", it means that it is sortable and un-normalized.
        // so we can use the sorting vector as the source, and we don't need to load it from the document.
        return NULL;
      }
    } else {
      // Field not found in the schema.
      // We assume `field_name` is the path to load from in the document.
      if (!(key->flags & RLOOKUP_F_NAMEALLOC)) {
        key->path = field_name;
      } else if (name != field_name) {
        key->path = rm_strdup(field_name);
      } // else
        // If the caller requested to allocate the name, and the name is the same as the path,
        // it was already set to the same allocation for the name, so we don't need to do anything.
    }
    // Mark the key as loaded from the document (for the rest of the pipeline usage).
    key->flags |= RLOOKUP_F_DOCSRC | RLOOKUP_F_ISLOADED;
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

    key->flags |= RLOOKUP_F_QUERYSRC;
    return key;

  // Return the key if it exists in the lookup table, or if it exists in the schema as SORTABLE.
  case RLOOKUP_M_READ:
    if (!key) {
      // If we didn't find the key at the lookup table, check if it exists in
      // the schema as SORTABLE, and create only if so.
      key = genKeyFromSpec(lookup, name, name_len, flags);
    }

    // If we didn't find the key in the schema (there is no schema) and unresolved is OK, create an unresolved key.
    if (!key && (lookup->options & RLOOKUP_OPT_UNRESOLVED_OK)) {
      key = createNewKey(lookup, name, name_len, flags);
      key->flags |= RLOOKUP_F_UNRESOLVED;
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

size_t RLookup_GetLength(const RLookup *lookup, const RLookupRow *r, int *skipFieldIndex,
                         int requiredFlags, int excludeFlags, SchemaRule *rule) {
  int i = 0;
  size_t nfields = 0;
  for (const RLookupKey *kk = lookup->head; kk; kk = kk->next, ++i) {
    if (kk->name == NULL) {
      // Overridden key. Skip without incrementing the index
      --i;
      continue;
    }
    if (requiredFlags && !(kk->flags & requiredFlags)) {
      continue;
    }
    if (excludeFlags && (kk->flags & excludeFlags)) {
      continue;
    }
    const RSValue *v = RLookup_GetItem(kk, r);
    if (!v) {
      continue;
    }
    // on coordinator, we reach this code without sctx or rule,
    // we trust the shards to not send those fields.
    if (rule && ((rule->lang_field && strcmp(kk->name, rule->lang_field) == 0) ||
                  (rule->score_field && strcmp(kk->name, rule->score_field) == 0) ||
                  (rule->payload_field && strcmp(kk->name, rule->payload_field) == 0))) {
      continue;
    }

    skipFieldIndex[i] = 1;
    ++nfields;
  }
  RS_LOG_ASSERT(i == lookup->rowlen, "'i' should be equal to lookup len");
  return nfields;
}

void RLookup_Init(RLookup *lk, IndexSpecCache *spcache) {
  memset(lk, 0, sizeof(*lk));
  lk->spcache = spcache;
}

void RLookup_WriteOwnKey(const RLookupKey *key, RLookupRow *row, RSValue *v) {
  // Find the pointer to write to ...
  RSValue **vptr = array_ensure_at(&row->dyn, key->dstidx, RSValue *);
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

void RLookup_WriteOwnKeyByName(RLookup *lookup, const char *name, size_t len, RLookupRow *row, RSValue *value) {
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
  }
}

void RLookupRow_Move(const RLookup *lk, RLookupRow *src, RLookupRow *dst) {
  for (const RLookupKey *kk = lk->head; kk; kk = kk->next) {
    RSValue *vv = RLookup_GetItem(kk, src);
    if (vv) {
      RLookup_WriteKey(kk, dst, vv);
    }
  }
  RLookupRow_Wipe(src);
}

static void RLookupKey_Cleanup(RLookupKey *k) {
  if (k->flags & RLOOKUP_F_NAMEALLOC) {
    if (k->name != k->path) {
      rm_free((void *)k->path);
    }
    rm_free((void *)k->name);
  }
}

void RLookupKey_Free(RLookupKey *k) {
  RLookupKey_Cleanup(k);
  rm_free(k);
}

void RLookup_Cleanup(RLookup *lk) {
  RLookupKey *next, *cur = lk->head;
  while (cur) {
    next = cur->next;
    RLookupKey_Free(cur);
    cur = next;
  }
  IndexSpecCache_Decref(lk->spcache);

  lk->head = lk->tail = NULL;
  memset(lk, 0xff, sizeof(*lk));
}

static RSValue *hvalToValue(const RedisModuleString *src, RLookupCoerceType type) {
  if (type == RLOOKUP_C_BOOL || type == RLOOKUP_C_INT) {
    long long ll;
    RedisModule_StringToLongLong(src, &ll);
    return RSValue_NewNumberFromInt64(ll);
  } else if (type == RLOOKUP_C_DBL) {
    double dd;
    RedisModule_StringToDouble(src, &dd);
    return RSValue_NewNumber(dd);
  } else {
    return RSValue_NewOwnedRedisString((RedisModuleString *)src);
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
      return RSValue_NewStolenRedisString(rstr);
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

      RSValueMap map = RSValueMap_AllocUninit(len);
      for (; (japi->nextKeyValue(iter, &keyName, value_ptr) == REDISMODULE_OK); ++i) {
        value = *value_ptr;
        RSValueMap_SetEntry(&map, i, RSValue_NewStolenRedisString(keyName),
          jsonValToValueExpanded(ctx, value));
      }
      japi->freeJson(value_ptr);
      value_ptr = NULL;
      japi->freeKeyValuesIter(iter);
      RS_ASSERT(i == len);

      ret = RSValue_NewMap(map);
    } else {
      ret = RSValue_NewMap(RSValueMap_AllocUninit(0));
    }
  } else if (type == JSONType_Array) {
    // Array
    japi->getLen(json, &len);
    if (len) {
      RSValue **arr = RSValue_AllocateArray(len);
      RedisJSONPtr value_ptr = japi->allocJson();
      for (size_t i = 0; i < len; ++i) {
        japi->getAt(json, i, value_ptr);
        RedisJSON value = *value_ptr;
        arr[i] = jsonValToValueExpanded(ctx, value);
      }
      japi->freeJson(value_ptr);
      ret = RSValue_NewArray(arr, len);
    } else {
      // Empty array
      ret = RSValue_NewArray(NULL, 0);
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
    RSValue **arr = RSValue_AllocateArray(len);
    for (size_t i = 0; (json = japi->next(iter)); ++i) {
      arr[i] = jsonValToValueExpanded(ctx, json);
    }
    ret = RSValue_NewArray(arr, len);
  } else {
    // Empty array
    ret = RSValue_NewArray(NULL, 0);
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
      RSValue *otherval = RSValue_NewStolenRedisString(serialized);
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

static RSValue *replyElemToValue(RedisModuleCallReply *rep, RLookupCoerceType otype) {
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
        (kk->flags & RLOOKUP_F_VAL_AVAILABLE) ||
        // There is no value in the sorting vector, and we don't need to load it from the document.
        ((kk->flags & RLOOKUP_F_SVSRC) && (RLookup_GetItem(kk, dst) == NULL))
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
      QueryError_SetCode(options->status, QUERY_ENODOC);
      return REDISMODULE_ERR;
    }
    if (RedisModule_KeyType(*keyobj) != REDISMODULE_KEYTYPE_HASH) {
      QueryError_SetCode(options->status, QUERY_EREDISKEYTYPE);
      return REDISMODULE_ERR;
    }
  }

  // Get the actual hash value
  RedisModuleString *val = NULL;
  RSValue *rsv = NULL;

  RedisModule_HashGet(*keyobj, REDISMODULE_HASH_CFIELDS, kk->path, &val, NULL);

  if (val != NULL) {
    // `val` was created by `RedisModule_HashGet` and is owned by us.
    // This function might retain it, but it's thread-safe to free it afterwards without any locks
    // as it will hold the only reference to it after the next line.
    rsv = hvalToValue(val, (kk->flags & RLOOKUP_T_NUMERIC) ? RLOOKUP_C_DBL : RLOOKUP_C_STR);
    RedisModule_FreeString(RSDummyContext, val);
  } else if (!strcmp(kk->path, UNDERSCORE_KEY)) {
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
    QueryError_SetCode(options->status, QUERY_EUNSUPPTYPE);
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
      QueryError_SetCode(options->status, QUERY_ENODOC);
      return REDISMODULE_ERR;
    }
  }

  // Get the actual json value
  RedisModuleString *val = NULL;
  RSValue *rsv = NULL;

  JSONResultsIterator jsonIter = (*kk->path == '$') ? japi->get(*keyobj, kk->path) : NULL;

  if (!jsonIter) {
    // The field does not exist and and it isn't `__key`
    if (!strcmp(kk->path, UNDERSCORE_KEY)) {
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


static int loadIndividualKeys(RLookup *it, RLookupRow *dst, RLookupLoadOptions *options) {
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
    for (const RLookupKey *kk = it->head; kk; kk = kk->next) {
      /* key is not part of document schema. no need/impossible to 'load' it */
      if (!(kk->flags & RLOOKUP_F_SCHEMASRC)) {
        continue;
      }
      if (!options->forceLoad) {
        /* wanted a sort key, but field is not sortable */
        if ((options->mode & RLOOKUP_LOAD_SVKEYS) && !(kk->flags & RLOOKUP_F_SVSRC)) {
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
  } else if ((rlk->flags & RLOOKUP_F_QUERYSRC)
            /* || (rlk->flags & RLOOKUP_F_ISLOADED) TODO: skip loaded keys, EXCLUDING keys that were opened by this function*/) {
    return; // Key name is already taken by a query key, or it's already loaded.
  }

  RLookupCoerceType ctype = RLOOKUP_C_STR;
  if (!pd->options->forceString && rlk->flags & RLOOKUP_T_NUMERIC) {
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
  RedisModuleCallReply *rep = NULL;
  RedisModuleCtx *ctx = options->sctx->redisCtx;
  RedisModuleString *krstr =
      RedisModule_CreateString(ctx, options->dmd->keyPtr, sdslen(options->dmd->keyPtr));
  // We can only use the scan API from Redis version 6.0.6 and above
  // and when the deployment is not enterprise-crdt
  if(!isFeatureSupported(RM_SCAN_KEY_API_FIX) || isCrdt){
    rep = RedisModule_Call(ctx, "HGETALL", "s", krstr);
    if (rep == NULL || RedisModule_CallReplyType(rep) != REDISMODULE_REPLY_ARRAY) {
      goto done;
    }

    size_t len = RedisModule_CallReplyLength(rep);
    // Zero means the document does not exist in redis
    if (len == 0) {
      goto done;
    }

    for (size_t i = 0; i < len; i += 2) {
      size_t klen = 0;
      RedisModuleCallReply *repk = RedisModule_CallReplyArrayElement(rep, i);
      RedisModuleCallReply *repv = RedisModule_CallReplyArrayElement(rep, i + 1);

      const char *kstr = RedisModule_CallReplyStringPtr(repk, &klen);
      RLookupKey *rlk = RLookup_FindKey(it, kstr, klen);
      if (!rlk) {
        // First returned document, create the key.
        rlk = RLookup_GetKey_LoadEx(it, kstr, klen, kstr, RLOOKUP_F_NAMEALLOC | RLOOKUP_F_FORCE_LOAD);
      } else if ((rlk->flags & RLOOKUP_F_QUERYSRC)
                 /* || (rlk->flags & RLOOKUP_F_ISLOADED) TODO: skip loaded keys, EXCLUDING keys that were opened by this function*/) {
        continue; // Key name is already taken by a query key, or it's already loaded.
      }
      RLookupCoerceType ctype = RLOOKUP_C_STR;
      if (!options->forceString && rlk->flags & RLOOKUP_T_NUMERIC) {
        ctype = RLOOKUP_C_DBL;
      }
      RSValue *vptr = replyElemToValue(repv, ctype);
      RLookup_WriteOwnKey(rlk, dst, vptr);
    }
  } else {
    RedisModuleKey *key = RedisModule_OpenKey(ctx, krstr, DOCUMENT_OPEN_KEY_QUERY_FLAGS);
    if (!key || RedisModule_KeyType(key) != REDISMODULE_KEYTYPE_HASH) {
      // key does not exist or is not a hash
      if (key) {
        RedisModule_CloseKey(key);
      }
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
    RedisModule_CloseKey(key);
  }

  rc = REDISMODULE_OK;

done:
  if (krstr) {
    RedisModule_FreeString(ctx, krstr);
  }
  if (rep) {
    RedisModule_FreeCallReply(rep);
  }
  return rc;
}

static int RLookup_JSON_GetAll(RLookup *it, RLookupRow *dst, RLookupLoadOptions *options) {
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

int RLookup_LoadRuleFields(RedisModuleCtx *ctx, RLookup *it, RLookupRow *dst, IndexSpec *spec, const char *keyptr) {
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
    keys[i]->path = HiddenString_GetUnsafe(fs->fieldPath, NULL);
  }

  // load
  RedisSearchCtx sctx = {.redisCtx = ctx, .spec = spec };
  struct QueryError status = QueryError_Default(); // TODO: report errors
  RLookupLoadOptions opt = {.keys = (const RLookupKey **)keys,
                            .nkeys = nkeys,
                            .sctx = &sctx,
                            .keyPtr = keyptr,
                            .type = rule->type,
                            .status = &status,
                            .forceLoad = 1,
                            .mode = RLOOKUP_LOAD_KEYLIST };
  int rv = loadIndividualKeys(it, dst, &opt);
  QueryError_ClearError(&status);
  rm_free(keys);
  return rv;
}

void RLookup_AddKeysFrom(const RLookup *src, RLookup *dest, uint32_t flags) {
  RS_ASSERT(dest && src);
  RS_ASSERT(dest != src);  // Prevent self-addition

  // Iterate through all keys in source lookup
  for (const RLookupKey *src_key = src->head; src_key; src_key = src_key->next) {
    if (!src_key->name) {
      // Skip overridden keys (they have name == NULL)
      continue;
    }

    // Combine caller's control flags with source key's persistent properties
    // Only preserve non-transient flags from source (F_SVSRC, F_HIDDEN, etc.)
    // while respecting caller's control flags (F_OVERRIDE, F_FORCE_LOAD, etc.)
    uint32_t combined_flags = flags | (src_key->flags & ~RLOOKUP_TRANSIENT_FLAGS);
    RLookupKey *dest_key = RLookup_GetKey_Write(dest, src_key->name, combined_flags);
  }
}

void RLookupRow_WriteFieldsFrom(const RLookupRow *srcRow, const RLookup *srcLookup,
                               RLookupRow *destRow, RLookup *destLookup) {
  RS_ASSERT(srcRow && srcLookup);
  RS_ASSERT(destRow && destLookup);

  // Iterate through all source keys
  for (const RLookupKey *src_key = srcLookup->head; src_key; src_key = src_key->next) {
    if (!src_key->name) {
      // Skip overridden keys
      continue;
    }

    // Get value from source row
    RSValue *value = RLookup_GetItem(src_key, srcRow);
    if (!value) {
      // No data for this key in source row
      continue;
    }

    // Find corresponding key in destination lookup
    RLookupKey *dest_key = RLookup_FindKey(destLookup, src_key->name, src_key->name_len);
    RS_ASSERT(dest_key != NULL);  // Assumption: all source keys exist in destination
    // Write fields to destination (increments refcount, shares ownership)
    RLookup_WriteKey(dest_key, destRow, value);
  }
  // Caller is responsible for managing source row lifecycle
}
