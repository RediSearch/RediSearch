#include "rlookup.h"
#include "module.h"
#include "document.h"
#include "rmutil/rm_assert.h"
#include <util/arr.h>
#include "doc_types.h"

static RLookupKey *createNewKey(RLookup *lookup, const char *name, size_t n, int flags,
                                uint16_t idx) {
  RLookupKey *ret = rm_calloc(1, sizeof(*ret));

  ret->flags = (flags & (~RLOOKUP_TRANSIENT_FLAGS));
  ret->dstidx = idx;
  ret->refcnt = 1;

  if (flags & RLOOKUP_F_NAMEALLOC) {
    ret->name = rm_strndup(name, n);
  } else {
    ret->name = name;
  }
  ret->name_len = n;

  // This defaults path a name to the same string. Only changed with `AS` keyword
  ret->path = ret->name;

  if (!lookup->head) {
    lookup->head = lookup->tail = ret;
  } else {
    lookup->tail->next = ret;
    lookup->tail = ret;
  }
  return ret;
}

static RLookupKey *genKeyFromSpec(RLookup *lookup, const char *name, int flags) {
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

  if (!fs) {
    // Field does not exist in the schema at all
    return NULL;
  }

  uint16_t idx = lookup->rowlen++;

  RLookupKey *ret = createNewKey(lookup, name, strlen(name), flags, idx);
  if (FieldSpec_IsSortable(fs)) {
    ret->flags |= RLOOKUP_F_SVSRC;
    ret->svidx = fs->sortIdx;
  }
  ret->flags |= RLOOKUP_F_DOCSRC;
  if (fs->types == INDEXFLD_T_NUMERIC) {
    ret->fieldtype = RLOOKUP_C_DBL;
  }
  return ret;
}

RLookupKey *RLookup_GetKeyEx(RLookup *lookup, const char *name, size_t n, int flags) {
  RLookupKey *ret = NULL;
  int isNew = 0;

  for (RLookupKey *kk = lookup->head; kk; kk = kk->next) {
    // match `name` to the name/path of the field
    if ((kk->name_len == n && !strncmp(kk->name, name, kk->name_len)) ||
        (kk->path != kk->name && !strncmp(kk->path, name, n))) {
      if (flags & RLOOKUP_F_OEXCL) {
        return NULL;
      }
      ret = kk;
      break;
    }
  }

  if (!ret) {
    ret = genKeyFromSpec(lookup, name, flags);
  }

  if (!ret) {
    if (!(flags & RLOOKUP_F_OCREAT) && !(lookup->options & RLOOKUP_OPT_UNRESOLVED_OK)) {
      return NULL;
    } else {
      ret = createNewKey(lookup, name, n, flags, lookup->rowlen++);
      if (!(flags & RLOOKUP_F_OCREAT)) {
        ret->flags |= RLOOKUP_F_UNRESOLVED;
      }
    }
  }

  if (!(flags & RLOOKUP_F_NOINCREF)) {
    ret->refcnt++;
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
  if (spcache) {
    lk->spcache = spcache;
  }
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
      RLookup_GetKey(lookup, name, RLOOKUP_F_NAMEALLOC | RLOOKUP_F_NOINCREF | RLOOKUP_F_OCREAT);
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
  if (r->rmkey) {
    RedisModule_CloseKey(r->rmkey);
    r->rmkey = NULL;
  }
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
  if (lk->spcache) {
    IndexSpecCache_Decref(lk->spcache);
    lk->spcache = NULL;
  }

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
      RS_LOG_ASSERT(0, "Cannot get here");
  }
  return NULL;
}

// Get the value from an iterator and free the iterator
// Return REDISMODULE_OK, and set rsv to the value, if value exists
// Return REDISMODULE_ERR otherwise
static int jsonIterToValue(RedisModuleCtx *ctx, JSONResultsIterator iter, RSValue **rsv) {

  int res = REDISMODULE_ERR;
  if (japi->len(iter) == 1) {
    RedisJSON jsonValue = japi->next(iter);
    if (jsonValue) {
      *rsv = jsonValToValue(ctx, jsonValue);
      res = REDISMODULE_OK;
    }
  } else {
    RedisModuleString *rstr;
    if (japi->getJSONFromIter(iter, ctx, &rstr) == REDISMODULE_OK) {
      *rsv = RS_StealRedisStringVal(rstr);
      res = REDISMODULE_OK;
    }
  }
  japi->freeIter(iter);
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
  RLookup_WriteKey(kk, dst, rsv);
  RSValue_Decref(rsv);
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
    if (jsonIterToValue(ctx, jsonIter, &rsv) == REDISMODULE_ERR) {
      return REDISMODULE_OK;
    }
  }

  // Value has a reference count of 1
  RLookup_WriteKey(kk, dst, rsv);
  RSValue_Decref(rsv);
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
  if (options->nkeys) {
    for (size_t ii = 0; ii < options->nkeys; ++ii) {
      const RLookupKey *kk = options->keys[ii];
      if (getKey(kk, dst, options, &key) != REDISMODULE_OK) {
        goto done;
      }
    }
  } else {
    for (const RLookupKey *kk = it->head; kk; kk = kk->next) {
      /* key is not part of document schema. no need/impossible to 'load' it */
      if (!(kk->flags & RLOOKUP_F_DOCSRC)) {
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

  RedisModuleString *value = NULL;
  RedisJSON jsonValue = japi->next(jsonIter);
  if (!jsonValue || japi->getJSON(jsonRoot, ctx, &value) != REDISMODULE_OK) {
    if (value) {
      RedisModule_FreeString(ctx, value);
    }
    goto done;
  }

  RLookupKey *rlk = RLookup_GetKeyEx(it, JSON_ROOT, strlen(JSON_ROOT), RLOOKUP_F_OCREAT);
  RSValue *vptr = RS_StealRedisStringVal(value);
  RLookup_WriteOwnKey(rlk, dst, vptr);

  rc = REDISMODULE_OK;

done:
  if (jsonIter) {
    japi->freeIter(jsonIter);
  }
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
  // if loading the document failed b/c it does not exist, delete the document from DocTable
  // this will mark doc as deleted and reply with `(nil)`
  if (rv != REDISMODULE_OK) {
    RedisModuleCtx *ctx = options->sctx->redisCtx;
    RedisModuleString *rmstr = DMD_CreateKeyString(options->dmd, ctx);
    IndexSpec_DeleteDoc(options->sctx->spec, ctx, rmstr);
    RedisModule_FreeString(ctx, rmstr);
  }
  return rv;
}

int RLookup_LoadRuleFields(RedisModuleCtx *ctx, RLookup *it, RLookupRow *dst, SchemaRule *rule, const char *keyptr) {
  IndexSpec *spec = rule->spec;

  // create rlookupkeys
  int nkeys = array_len(rule->filter_fields);
  RLookupKey **keys = rm_malloc(nkeys * sizeof(*keys));
  for (int i = 0; i < nkeys; ++i) {
    int idx = rule->filter_fields_index[i];
    if (idx == -1) {
      keys[i] = createNewKey(it, rule->filter_fields[i], strlen(rule->filter_fields[i]), 0, it->rowlen++);
      continue;
    }
    FieldSpec *fs = spec->fields + idx;
    keys[i] = createNewKey(it, fs->name, strlen(fs->name), 0, it->rowlen++);
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
