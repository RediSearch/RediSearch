/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#include "rlookup_load_document.h"
#include "json.h"
#include "module.h"
#include "document.h"
#include "rmutil/rm_assert.h"
#include "doc_types.h"
#include "value.h"
#include "util/arr.h"

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
    RedisModule_RetainString(RSDummyContext, (RedisModuleString *)src);
    return RSValue_NewRedisString((RedisModuleString *)src);
  }
}

// returns true if the value of the key is already available
// avoids the need to call to redis api to get the value
// i.e we can use the sorting vector as a cache
static inline bool isValueAvailable(const RLookupKey *kk, const RLookupRow *dst, RLookupLoadOptions *options) {
  return (!options->forceLoad && (
        // No need to "write" this key. It's always implicitly loaded!
        (RLookupKey_GetFlags(kk) & RLOOKUP_F_VALAVAILABLE) ||
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
    rsv = hvalToValue(val, (RLookupKey_GetFlags(kk) & RLOOKUP_F_NUMERIC) ? RLOOKUP_C_DBL : RLOOKUP_C_STR);
    RedisModule_FreeString(RSDummyContext, val);
  } else if (RLookupKey_GetPath(kk) == NULL || !strcmp(RLookupKey_GetPath(kk), UNDERSCORE_KEY)) {
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
    rlk = RLookup_GetKey_LoadEx(pd->it, fieldCStr, fieldCStrLen, fieldCStr, RLOOKUP_F_FORCELOAD | RLOOKUP_F_NAMEALLOC);
  } else if ((RLookupKey_GetFlags(rlk) & RLOOKUP_F_QUERYSRC)
            /* || (rlk->flags & RLOOKUP_F_ISLOADED) TODO: skip loaded keys, EXCLUDING keys that were opened by this function*/) {
    return; // Key name is already taken by a query key, or it's already loaded.
  }

  RLookupCoerceType ctype = RLOOKUP_C_STR;
  if (!pd->options->forceString && RLookupKey_GetFlags(rlk) & RLOOKUP_F_NUMERIC) {
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
