/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#include "rlookup.h"
#include "rlookup_rs.h"
#include "document.h"
#include "rmutil/rm_assert.h"
#include <util/arr.h>
#include "doc_types.h"
#include "value.h"

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

size_t RLookup_GetLength(const RLookup *lookup, const RLookupRow *r, int *skipFieldIndex,
                         int requiredFlags, int excludeFlags, SchemaRule *rule) {
  int i = 0;
  size_t nfields = 0;
  for (const RLookupKey *kk = lookup->keys.head; kk; kk = kk->next, ++i) {
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
  RS_LOG_ASSERT(i == lookup->keys.rowlen, "'i' should be equal to lookup len");
  return nfields;
}

void RLookup_WriteOwnKey(const RLookupKey *key, RLookupRow *row, RSValue *v) {
  // Find the pointer to write to ...
  RSValue **vptr = array_ensure_at(&row->dyn, key->dstidx, RSValue *);
  if (*vptr) {
    RSValue_Decref(*vptr);
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
  RSValue_Decref(value);
}

void RLookupRow_Wipe(RLookupRow *r) {
  for (size_t ii = 0; ii < array_len(r->dyn) && r->ndyn; ++ii) {
    RSValue **vpp = r->dyn + ii;
    if (*vpp) {
      RSValue_Decref(*vpp);
      *vpp = NULL;
      r->ndyn--;
    }
  }
  r->sv = NULL;
}

void RLookupRow_Cleanup(RLookupRow *r) {
  RLookupRow_Wipe(r);
  if (r->dyn) {
    array_free(r->dyn);
  }
}

void RLookupRow_Move(const RLookup *lk, RLookupRow *src, RLookupRow *dst) {
  for (const RLookupKey *kk = lk->keys.head; kk; kk = kk->next) {
    RSValue *vv = RLookup_GetItem(kk, src);
    if (vv) {
      RLookup_WriteKey(kk, dst, vv);
    }
  }
  RLookupRow_Wipe(src);
}

sds RLookupRow_DumpSds(const RLookupRow *rr, bool obfuscate) {
  sds s = sdsempty();
  s = sdscatfmt(s, "Row @%p\n", rr);
  if (rr->dyn) {
    s = sdscatfmt(s, "  DYN @%p\n", rr->dyn);
    for (size_t ii = 0; ii < array_len(rr->dyn); ++ii) {
      s = sdscatfmt(s, "  [%lu]: %p\n", ii, rr->dyn[ii]);
      if (rr->dyn[ii]) {
        s = sdscat(s, "    ");
        s = RSValue_DumpSds(rr->dyn[ii], s, obfuscate);
        s = sdscat(s, "\n");
      }
    }
  }
  if (rr->sv) {
    s = sdscatfmt(s, "  SV @%p\n", rr->sv);
  }
  return s;
}

static void RLookupKey_Cleanup(RLookupKey *k) {
  if (k->flags & RLOOKUP_F_NAMEALLOC) {
    if (k->name != k->path) {
      rm_free((void *)k->path);
    }
    rm_free((void *)k->name);
  }
}

static void RLookupKey_Free(RLookupKey *k) {
  RLookupKey_Cleanup(k);
  rm_free(k);
}

static RSValue *hvalToValue(const RedisModuleString *src, RLookupCoerceType type) {
  if (type == RLOOKUP_C_BOOL || type == RLOOKUP_C_INT) {
    long long ll;
    RedisModule_StringToLongLong(src, &ll);
    return RS_Int64Val(ll);
  } else if (type == RLOOKUP_C_DBL) {
    double dd;
    RedisModule_StringToDouble(src, &dd);
    return RS_NumVal(dd);
  } else {
    return RS_OwnRedisStringVal((RedisModuleString *)src);
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
      return RS_StringVal(str, len);
    case JSONType_Int:
      japi->getInt(json, &ll);
      return RS_Int64Val(ll);
    case JSONType_Double:
      japi->getDouble(json, &dd);
      return RS_NumVal(dd);
    case JSONType_Bool:
      japi->getBoolean(json, &i);
      return RS_Int64Val(i);
    case JSONType_Array:
    case JSONType_Object:
      japi->getJSON(json, ctx, &rstr);
      return RS_StealRedisStringVal(rstr);
    case JSONType_Null:
      return RS_NullVal();
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
      pairs = rm_malloc(sizeof(RSValue*) * len * 2);
      for (; (value = japi->nextKeyValue(iter, &keyName)); ++i) {
        RS_ASSERT(i < len);
        pairs[RSVALUE_MAP_KEYPOS(i)] = RS_StealRedisStringVal(keyName);
        pairs[RSVALUE_MAP_VALUEPOS(i)] = jsonValToValueExpanded(ctx, value);
      }
      japi->freeKeyValuesIter(iter);
      RS_ASSERT(i == len && !value);
    }
    ret = RSValue_NewMap(pairs, len);
  } else if (type == JSONType_Array) {
    // Array
    japi->getLen(json, &len);
    if (len) {
      RSValue **arr = RSValue_AllocateArray(len);
      for (size_t i = 0; i < len; ++i) {
        RedisJSON value = japi->getAt(json, i);
        arr[i] = jsonValToValueExpanded(ctx, value);
      }
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

  if (apiVersion < APIVERSION_RETURN_MULTI_CMP_FIRST || japi_ver < 3) {
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
    // If the value is an array, we currently try using the first element
    JSONType type = japi->getType(json);
    if (type == JSONType_Array) {
      // Empty array will return NULL
      json = japi->getAt(json, 0);
    }

    if (json) {
      RSValue *val = jsonValToValue(ctx, json);
      RSValue *otherval = RS_StealRedisStringVal(serialized);
      RSValue *expand = japi_ver >= 4 ? jsonIterToValueExpanded(ctx, iter) : RS_NullVal();
      *rsv = RS_DuoVal(val, otherval, expand);
      res = REDISMODULE_OK;
    } else if (serialized) {
      RedisModule_FreeString(ctx, serialized);
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
        return RSValue_ParseNumber(s, len);
      }
      // Note, the pointer is within CallReply; we need to copy
      return RS_NewCopiedString(s, len);
    }

    case REDISMODULE_REPLY_INTEGER:
    create_int:
      if (otype == RLOOKUP_C_STR || otype == RLOOKUP_C_DBL) {
        goto create_string;
      }
      return RS_Int64Val(RedisModule_CallReplyInteger(rep));

    case REDISMODULE_REPLY_UNKNOWN:
    case REDISMODULE_REPLY_NULL:
    case REDISMODULE_REPLY_ARRAY:
    default:
      // Nothing
      return RS_NullVal();
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
  char *keyPtr = options->dmd ? options->dmd->keyPtr : (char *)options->keyPtr;
  if (!*keyobj) {

    if (japi_ver >= 5) {
      RedisModuleString* keyName = RedisModule_CreateString(ctx, keyPtr, strlen(keyPtr));
      *keyobj = japi->openKeyWithFlags(ctx, keyName, DOCUMENT_OPEN_KEY_QUERY_FLAGS);
      RedisModule_FreeString(ctx, keyName);
    } else {
      *keyobj = japi->openKeyFromStr(ctx, keyPtr);
    }
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
      rsv = RS_StringVal(rm_strdup(keyPtr), strlen(keyPtr));
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
    for (const RLookupKey *kk = it->keys.head; kk; kk = kk->next) {
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
  RedisJSON jsonRoot;
  if (japi_ver >= 5) {
    RedisModuleString* keyName = RedisModule_CreateString(ctx, options->dmd->keyPtr, strlen(options->dmd->keyPtr));
    jsonRoot = japi->openKeyWithFlags(ctx, keyName, DOCUMENT_OPEN_KEY_QUERY_FLAGS);
    RedisModule_FreeString(ctx, keyName);
  } else {
    jsonRoot = japi->openKeyFromStr(ctx, options->dmd->keyPtr);
  }
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
    dst->sv = options->dmd->sortVector;
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
  struct QueryError status = {0}; // TODO: report errors
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
