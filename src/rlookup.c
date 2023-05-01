/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "rlookup.h"
#include "module.h"
#include "document.h"
#include "rmutil/rm_assert.h"
#include <util/arr.h>
#include "doc_types.h"
#include "value.h"

static RLookupKey *createNewKey(RLookup *lookup, const char *name, size_t n, int flags,
                                uint16_t idx);

// Required attributes of a new rlookupkey to pass createNewLookupKeyFromOptions function 
// @member name: name to give the rlookup key. 
// @member path: the original path of this key in Redis key space. If the `name` and `path` point to the 
// address, the new key will also contain 'name' and 'path' pointing to the same address 
// (even if name was reallocated - see flags)
// @member flags: flags to set in the new key. if RLOOKUP_F_NAMEALLOC flag is set, `name` will be reallocated.
// @member dstidx: the column index of the key in the Rlookup table.
// @member svidx: If the key contains a sortable field, this is the index of the key's value in the sorting vector
// @member type: type of the field's value.
typedef struct {
  const char *name;
  size_t namelen;

  const char *path;
  int flags;
  uint16_t dstidx;
  uint16_t svidx;
  RLookupCoerceType type;
} RLookupKeyOptions;

static RLookupKey *createNewLookupKeyFromOptions(RLookup *lookup, RLookupKeyOptions *rlookupkey_options) {
  RLookupKey *ret = rm_calloc(1, sizeof(*ret));

   int flags = rlookupkey_options->flags;

  ret->flags = (flags & (~RLOOKUP_TRANSIENT_FLAGS));
  ret->dstidx = rlookupkey_options->dstidx;
  ret->svidx = rlookupkey_options->svidx;
  ret->fieldtype = rlookupkey_options->type;

  if (flags & RLOOKUP_F_NAMEALLOC) {
    ret->name = rm_strndup(rlookupkey_options->name, rlookupkey_options->namelen);
  } else {
    ret->name = rlookupkey_options->name;
  }
  ret->name_len = rlookupkey_options->namelen;

  ret->path = rlookupkey_options->path == rlookupkey_options->name ? ret->name : rlookupkey_options->path;

  if (!lookup->head) {
    lookup->head = lookup->tail = ret;
  } else {
    lookup->tail->next = ret;
    lookup->tail = ret;
  }

  // Increase the Rlookup table row length. (all rows have the same length).
  ++(lookup->rowlen);

  return ret;
}


const FieldSpec *findFieldInSpecCache(const RLookup *lookup, const char *name) {
  const IndexSpecCache *cc = lookup->spcache;
  if (!cc) {
    return NULL;
  }

  const FieldSpec *fs = NULL;
  for (size_t ii = 0; ii < cc->nfields; ++ii) {
    if (!strcmp(cc->fields[ii].name, name)) {
      fs = cc->fields + ii;
      break;
    }
  }

  return fs;

}

static void Lookupkey_ConfigKeyOptionsFromSpec(RLookupKeyOptions *key_options, const FieldSpec *fs) {
 
  key_options->path = fs->path;

  if (FieldSpec_IsSortable(fs)) {
    key_options->flags |= RLOOKUP_F_SVSRC;
    key_options->svidx = fs->sortIdx;

  // If the field is sortable and not normalized (UNF),
  // we can take its value from the sorting vector.
  // Otherwise, it needs to be externally loaded from Redis keyspace.
    if(FieldSpec_IsUnf(fs)) {
      key_options->flags |= RLOOKUP_F_UNFORMATTED;
    }
  }
  key_options->flags |= RLOOKUP_F_SCHEMASRC;
  if (fs->types == INDEXFLD_T_NUMERIC) {
    key_options->type = RLOOKUP_C_DBL;
  }
}

static RLookupKey *genKeyFromSpec(RLookup *lookup, const char *name, size_t name_len, int flags) {

  const FieldSpec *fs = findFieldInSpecCache(lookup, name);
  if(!fs) {
    return NULL;
  }

  RLookupKeyOptions options = { 
                              .name = name, 
                              .namelen = name_len, 
                              .path = NULL,
                              .flags = flags,
                              .dstidx = lookup->rowlen, 
                              .svidx = 0,
                              .type = 0,
                            };
  Lookupkey_ConfigKeyOptionsFromSpec(&options, fs);
  return createNewLookupKeyFromOptions(lookup, &options);

}

static RLookupKey *FindLookupKeyWithExistingPath(RLookup *lookup, const char *path, int flags, const FieldSpec** out_spec_field) {

  // Check if path exist in schema (as name).
  // If the users set an alias for the fields, they should address them by their aliased name,
  //  otherwise it will be loaded from redis key space (unless already exist in the rlookup)

  // TODO: optimize pipeline: add to documentation that addressing keys by their original path
  // and not the by their alias can harm performance.

  const FieldSpec *fs = findFieldInSpecCache(lookup, path);

  // If it exists in spec, search for the original path in the rlookup.
  // If the key doesn't have an alias, we don't want to change the path.
  if(fs) {
    if((flags & RLOOKUP_F_ALIAS)) {
      path = fs->path;
    }
    *out_spec_field = fs;
  }

  // Search for path in the rlookup.
  for (RLookupKey *kk = lookup->head; kk; kk = kk->next) {
    if (!strcmp(kk->path, path)) {
      // if the key has NO aliases, keep searching until the exact key is found. 
      if((flags & RLOOKUP_F_ALIAS) || !strcmp(kk->name, path)) {
        return kk;
      }
    }
  }
  return NULL;


}  

static RLookupKey *createNewKey(RLookup *lookup, const char *name, size_t n, int flags,
                                uint16_t idx) {
  
  RLookupKeyOptions options = { 
                                .name = name, 
                                .namelen = n, 
                                .path = name,
                                .flags = flags,
                                .dstidx = idx, 
                                .svidx = 0,
                                .type = 0,
                              };
  return createNewLookupKeyFromOptions(lookup, &options);
}

static RLookupKey *RLookup_GetOrCreateKeyEx(RLookup *lookup, const char *path, const char *name, size_t name_len, int flags) {
  const FieldSpec *fs = NULL;
  RLookupKey *lookupkey = FindLookupKeyWithExistingPath(lookup, path, flags, &fs);

  // if we found a RLookupKey and it has the same name, use it.
  if(lookupkey) {
    if (!strcmp(lookupkey->name, name)) {
      return lookupkey;
    }

  // Else, generate a new key and copy the meta data of the existing key.
  // The new key will point to the same RSValue as the existing key, so we can avoid loading
  // this field twice.
    RLookupKeyOptions options = { 
                                  .name = name, 
                                  .namelen = name_len, 
                                  .path = lookupkey->path,
                                  .flags = flags | lookupkey->flags,
                                  .dstidx = lookupkey->dstidx, 
                                  .svidx = lookupkey->svidx,
                                  .type = lookupkey->fieldtype,
                          };
    return createNewLookupKeyFromOptions(lookup, &options);
  } 
  RLookupKeyOptions options = { 
                                  .name = name, 
                                  .namelen = name_len, 
                                  .path = path,
                                  .flags = flags & ~RLOOKUP_F_UNRESOLVED,  // If the requester of this key is also its creator, remove the unresolved flag
                                  .dstidx = lookup->rowlen, 
                                  .svidx = 0,
                                  .type = 0,
                          };


  // if we didn't find any key, but the key exists in the schema 
  // use spec fields
  if(fs) {
    Lookupkey_ConfigKeyOptionsFromSpec(&options, fs);
  }  
  return createNewLookupKeyFromOptions(lookup, &options);
}

RLookupKey *RLookup_GetOrCreateKey(RLookup *lookup, const char *path, const char *name, int flags) {
 return RLookup_GetOrCreateKeyEx(lookup, path, name, strlen(name), flags);
}
RLookupKey *RLookup_GetKeyEx(RLookup *lookup, const char *name, size_t n, int flags) {
  if((flags & RLOOKUP_F_OCREAT) && !(flags & RLOOKUP_F_OEXCL) ) {
   return RLookup_GetOrCreateKeyEx(lookup, name, name, n, flags);
  }

  RLookupKey *ret = NULL;

  for (RLookupKey *kk = lookup->head; kk; kk = kk->next) {
    // match `name` to the name/path of the field
    if ((kk->name_len == n && !strncmp(kk->name, name, kk->name_len)) ||
        (kk->path != kk->name && !strcmp(kk->path, name))) {
      if (flags & RLOOKUP_F_OEXCL) {
        return NULL;
      }
      ret = kk;
      break;
    }
  }

  if (!ret) {
    ret = genKeyFromSpec(lookup, name, n, flags);
  }

  if (!ret) {
    if (!(flags & RLOOKUP_F_OCREAT) && !(lookup->options & RLOOKUP_OPT_UNRESOLVED_OK)) {
      return NULL;
    } else {
      ret = createNewKey(lookup, name, n, flags, lookup->rowlen);
      if (!(flags & RLOOKUP_F_OCREAT)) {
        ret->flags |= RLOOKUP_F_UNRESOLVED;
      }
    }
  }

  if (flags & RLOOKUP_F_OCREAT) {
    // If the requester of this key is also its creator, remove the unresolved
    // flag
    ret->flags &= ~RLOOKUP_F_UNRESOLVED;
  }
  return ret;
}

RLookupKey *RLookup_GetKey(RLookup *lookup, const char *name, int flags) {
  return RLookup_GetKeyEx(lookup, name, strlen(name), flags);
}

size_t RLookup_GetLength(const RLookup *lookup, const RLookupRow *r, int *skipFieldIndex,
                         int requiredFlags, int excludeFlags, SchemaRule *rule) {
  int i = 0;
  size_t nfields = 0;
  for (const RLookupKey *kk = lookup->head; kk; kk = kk->next, ++i) {
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
  RS_LOG_ASSERT(i == lookup->rowlen, "'i' should be equal lookup len");
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
    RSValue_Decref(*vptr);
    row->ndyn--;
  }
  *vptr = v;
  row->ndyn++;
}

void RLookup_WriteKey(const RLookupKey *key, RLookupRow *row, RSValue *v) {
  RLookup_WriteOwnKey(key, row, v);
  RSValue_IncrRef(v);
}

void RLookup_WriteKeyByName(RLookup *lookup, const char *name, RLookupRow *dst, RSValue *v) {
  // Get the key first
  RLookupKey *k =
      RLookup_GetKey(lookup, name, RLOOKUP_F_NAMEALLOC | RLOOKUP_F_OCREAT);
  RS_LOG_ASSERT(k, "failed to get key");
  RLookup_WriteKey(k, dst, v);
}

void RLookup_WriteOwnKeyByName(RLookup *lookup, const char *name, RLookupRow *row, RSValue *value) {
  RLookup_WriteKeyByName(lookup, name, row, value);
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
  for (const RLookupKey *kk = lk->head; kk; kk = kk->next) {
    RSValue *vv = RLookup_GetItem(kk, src);
    if (vv) {
      RLookup_WriteKey(kk, dst, vv);
    }
  }
  RLookupRow_Wipe(src);
}

void RLookupRow_Dump(const RLookupRow *rr) {
  printf("Row @%p\n", rr);
  if (rr->dyn) {
    printf("  DYN @%p\n", rr->dyn);
    for (size_t ii = 0; ii < array_len(rr->dyn); ++ii) {
      printf("  [%lu]: %p\n", ii, rr->dyn[ii]);
      if (rr->dyn[ii]) {
        printf("    ");
        RSValue_Print(rr->dyn[ii]);
        printf("\n");
      }
    }
  }
  if (rr->sv) {
    printf("  SV @%p\n", rr->sv);
  }
}

void RLookupKey_FreeInternal(RLookupKey *k) {
  if (k->flags & RLOOKUP_F_NAMEALLOC) {
    rm_free((void *)k->name);
  }
  rm_free(k);
}

void RLookup_Cleanup(RLookup *lk) {
  RLookupKey *next, *cur = lk->head;
  while (cur) {
    next = cur->next;
    RLookupKey_FreeInternal(cur);
    cur = next;
  }
  IndexSpecCache_Decref(lk->spcache);

  lk->head = lk->tail = NULL;
  memset(lk, 0xff, sizeof(*lk));
}

static RSValue *hvalToValue(RedisModuleString *src, RLookupCoerceType type) {
  if (type == RLOOKUP_C_BOOL || type == RLOOKUP_C_INT) {
    long long ll;
    RedisModule_StringToLongLong(src, &ll);
    return RS_Int64Val(ll);
  } else if (type == RLOOKUP_C_DBL) {
    double dd;
    RedisModule_StringToDouble(src, &dd);
    return RS_NumVal(dd);
  } else {
    return RS_OwnRedisStringVal(src);
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
  RS_LOG_ASSERT(0, "Cannot get here");
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
      *rsv = RS_DuoVal(val, otherval);
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

static int getKeyCommonHash(const RLookupKey *kk, RLookupRow *dst, RLookupLoadOptions *options,
                        RedisModuleKey **keyobj) {
  if (!options->noSortables && (kk->flags & RLOOKUP_F_SVSRC)) {
    // No need to "write" this key. It's always implicitly loaded!
    return REDISMODULE_OK;
  }

  const char *keyPtr = options->dmd ? options->dmd->keyPtr : options->keyPtr;
  // In this case, the flag must be obtained via HGET
  if (!*keyobj) {
    RedisModuleCtx *ctx = options->sctx->redisCtx;
    RedisModuleString *keyName =
        RedisModule_CreateString(ctx, keyPtr, strlen(keyPtr));
    *keyobj = RedisModule_OpenKey(ctx, keyName, REDISMODULE_READ);
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
  int rc = REDISMODULE_ERR;
  RedisModuleString *val = NULL;
  RSValue *rsv = NULL;

  rc = RedisModule_HashGet(*keyobj, REDISMODULE_HASH_CFIELDS, kk->path, &val, NULL);
  if (!val && options->sctx->spec->flags & Index_HasFieldAlias) {
    // name of field is the alias given on FT.CREATE
    // get the the actual path
    const FieldSpec *fs = IndexSpec_GetField(options->sctx->spec, kk->path, strlen(kk->path));
    if (fs) {
      rc = RedisModule_HashGet(*keyobj, REDISMODULE_HASH_CFIELDS, fs->path, &val, NULL);
    }
  }

  if (rc == REDISMODULE_OK && val != NULL) {
    // `val` was created by `RedisModule_HashGet` and is owned by us.
    // This function might retain it, but it's thread-safe to free it afterwards without any locks
    // as it will hold the only reference to it after the next line.
    rsv = hvalToValue(val, kk->fieldtype);
    RedisModule_FreeString(RSDummyContext, val);
  } else if (!strncmp(kk->name, UNDERSCORE_KEY, strlen(UNDERSCORE_KEY))) {
    RedisModuleString *keyName = RedisModule_CreateString(options->sctx->redisCtx,
                                  keyPtr, strlen(keyPtr));
    rsv = hvalToValue(keyName, RLOOKUP_C_STR);
    RedisModule_FreeString(options->sctx->redisCtx, keyName);
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

  if (!options->noSortables && (kk->flags & RLOOKUP_F_SVSRC)) {
    // No need to "write" this key. It's always implicitly loaded!
    return REDISMODULE_OK;
  }

  // In this case, the flag must be obtained from JSON
  RedisModuleCtx *ctx = options->sctx->redisCtx;
  char *keyPtr = options->dmd ? options->dmd->keyPtr : (char *)options->keyPtr;
  if (!*keyobj) {

    *keyobj = japi->openKeyFromStr(ctx, keyPtr);
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
    // name of field is the alias given on FT.CREATE
    // get the the actual path
    const FieldSpec *fs = IndexSpec_GetField(options->sctx->spec, kk->path, strlen(kk->path));
    if (fs) {
      jsonIter = japi->get(*keyobj, fs->path);
    }
  }

  if (!jsonIter) {
    // The field does not exist and and it isn't `__key`
    if (!strncmp(kk->name, UNDERSCORE_KEY, strlen(UNDERSCORE_KEY))) {
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
    for (const RLookupKey *kk = it->head; kk; kk = kk->next) {
      /* key is not part of document schema. no need/impossible to 'load' it */
      if (!(kk->flags & RLOOKUP_F_SCHEMASRC)) {
        continue;
      }
      if (!options->noSortables) {
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
  RLookupKey *rlk = RLookup_GetKeyEx(pd->it, fieldCStr, fieldCStrLen, RLOOKUP_F_OCREAT | RLOOKUP_F_NAMEALLOC);
  if (!pd->options->noSortables && (rlk->flags & RLOOKUP_F_SVSRC)) {
    return;  // Can load it from the sort vector on demand.
  }
  RLookupCoerceType ctype = rlk->fieldtype;
  if (pd->options->forceString) {
    ctype = RLOOKUP_C_STR;
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
      RLookupKey *rlk = RLookup_GetKeyEx(it, kstr, klen, RLOOKUP_F_OCREAT | RLOOKUP_F_NAMEALLOC);
      if (!options->noSortables && (rlk->flags & RLOOKUP_F_SVSRC)) {
        continue;  // Can load it from the sort vector on demand.
      }
      RLookupCoerceType ctype = rlk->fieldtype;
      if (options->forceString) {
        ctype = RLOOKUP_C_STR;
      }
      RSValue *vptr = replyElemToValue(repv, ctype);
      RLookup_WriteOwnKey(rlk, dst, vptr);
    }
  } else {
    RedisModuleKey *key = RedisModule_OpenKey(ctx, krstr, REDISMODULE_READ);
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
  RedisJSON jsonRoot = japi->openKeyFromStr(ctx, options->dmd->keyPtr);
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
  RLookupKey *rlk = RLookup_GetKeyEx(it, JSON_ROOT, strlen(JSON_ROOT), RLOOKUP_F_OCREAT);
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
      keys[i] = createNewKey(it, rule->filter_fields[i], strlen(rule->filter_fields[i]), 0, it->rowlen);
      continue;
    }
    FieldSpec *fs = spec->fields + idx;
    keys[i] = createNewKey(it, fs->name, strlen(fs->name), 0, it->rowlen);
    keys[i]->path = fs->path;
  }

  // load
  RedisSearchCtx sctx = {.redisCtx = ctx, .spec = spec };
  struct QueryError status = {0}; // TODO
  RLookupLoadOptions opt = {.keys = (const RLookupKey **)keys,
                            .nkeys = nkeys,
                            .sctx = &sctx,
                            .keyPtr = keyptr,
                            .type = rule->type,
                            .status = &status,
                            .noSortables = 1,
                            .mode = RLOOKUP_LOAD_KEYLIST };
  int rv = loadIndividualKeys(it, dst, &opt);
  rm_free(keys);
  return rv;
}
