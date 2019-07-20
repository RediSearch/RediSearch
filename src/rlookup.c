#include "rlookup.h"
#include <document.h>
#include <assert.h>
#include <util/arr.h>

static RLookupKey *createNewKey(RLookup *lookup, const char *name, size_t n, int flags,
                                uint16_t idx) {
  RLookupKey *ret = calloc(1, sizeof(*ret));

  ret->flags = (flags & (~RLOOKUP_TRANSIENT_FLAGS));
  ret->dstidx = idx;
  ret->refcnt = 1;

  if (flags & RLOOKUP_F_NAMEALLOC) {
    ret->name = strndup(name, n);
  } else {
    ret->name = name;
  }

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
  return ret;
}

RLookupKey *RLookup_GetKeyEx(RLookup *lookup, const char *name, size_t n, int flags) {
  RLookupKey *ret = NULL;
  int isNew = 0;

  for (RLookupKey *kk = lookup->head; kk; kk = kk->next) {
    size_t origlen = strlen(kk->name);
    if (origlen == n && !strncmp(kk->name, name, origlen)) {
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
  assert(k);
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
    free((void *)k->name);
  }
  free(k);
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

static RSValue *hvalToValue(RedisModuleString *src, RLookupCoerceType type, int copyStrings) {
  if (type == RLOOKUP_C_BOOL || type == RLOOKUP_C_INT) {
    long long ll = 0;
    RedisModule_StringToLongLong(src, &ll);
    return RS_Int64Val(ll);
  } else if (type == RLOOKUP_C_DBL) {
    double dd = 0.0;
    RedisModule_StringToDouble(src, &dd);
    return RS_NumVal(dd);
  } else {
    if (copyStrings) {
      size_t n;
      const char *s = RedisModule_StringPtrLen(src, &n);
      return RS_NewCopiedString(s, n);
    } else {
      return RS_RedisStringVal(src);
    }
  }
}

static RSValue *replyElemToValue(RedisModuleCallReply *rep, RLookupCoerceType otype,
                                 int copyStrings) {
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

      if (copyStrings) {
        return RS_NewCopiedString(s, len);
      } else {
        return RS_ConstStringVal(s, len);
      }
    }

    case REDISMODULE_REPLY_INTEGER:
    create_int:
      if (otype == RLOOKUP_C_STR) {
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

static int getKeyCommon(const RLookupKey *kk, RLookupRow *dst, RLookupLoadOptions *options) {
  if (!options->noSortables && (kk->flags & RLOOKUP_F_SVSRC)) {
    // No need to "write" this key. It's always implicitly loaded!
    return REDISMODULE_OK;
  }

  // In this case, the flag must be obtained via HGET
  if (!options->keyobj) {
    RedisModuleCtx *ctx = options->sctx->redisCtx;
    RedisModuleString *keyName =
        RedisModule_CreateString(ctx, options->dmd->keyPtr, strlen(options->dmd->keyPtr));
    options->keyobj = RedisModule_OpenKey(ctx, keyName, REDISMODULE_READ);
    RedisModule_FreeString(ctx, keyName);
    if (!options->keyobj) {
      QueryError_SetCode(options->status, QUERY_ENODOC);
      return REDISMODULE_ERR;
    }
    if (RedisModule_KeyType(options->keyobj) != REDISMODULE_KEYTYPE_HASH) {
      QueryError_SetCode(options->status, QUERY_EREDISKEYTYPE);
      return REDISMODULE_ERR;
    }
  }

  // Get the actual hash value
  RedisModuleString *val = NULL;
  int rc = RedisModule_HashGet(options->keyobj, REDISMODULE_HASH_CFIELDS, kk->name, &val, NULL);
  if (rc != REDISMODULE_OK || val == NULL) {
    return REDISMODULE_OK;
  }

  // Value has a reference count of 1
  RSValue *rsv = hvalToValue(val, kk->fieldtype, options->copyStrings);
  RLookup_WriteKey(kk, dst, rsv);
  RSValue_Decref(rsv);
  return REDISMODULE_OK;
}

static int loadIndividualKeys(RLookup *it, RLookupRow *dst, RLookupLoadOptions *options) {
  // Load the document from the schema. This should be simple enough...
  if (options->nkeys) {
    for (size_t ii = 0; ii < options->nkeys; ++ii) {
      const RLookupKey *kk = options->keys[ii];
      if (getKeyCommon(kk, dst, options) != REDISMODULE_OK) {
        goto error;
      }
    }
  } else {
    for (const RLookupKey *kk = it->head; kk; kk = kk->next) {
      if (!(kk->flags & RLOOKUP_F_DOCSRC)) {
        continue;
      }
      if (!options->noSortables) {
        if ((options->mode & RLOOKUP_LOAD_SVKEYS) && !(kk->flags & RLOOKUP_F_SVSRC)) {
          continue;
        }
      }
      if (getKeyCommon(kk, dst, options) != REDISMODULE_OK) {
        goto error;
      }
    }
  }

  if (options->keyobj && options->copyStrings) {
    RedisModule_CloseKey(options->keyobj);
    options->keyobj = NULL;
  }
  return REDISMODULE_OK;

error:
  if (options->keyobj) {
    RedisModule_CloseKey(options->keyobj);
    options->keyobj = NULL;
  }
  return REDISMODULE_ERR;
}

static int RLookup_HGETALL(RLookup *it, RLookupRow *dst, RLookupLoadOptions *options) {
  int rc = REDISMODULE_ERR;
  RedisModuleCallReply *rep = NULL;
  RedisModuleCtx *ctx = options->sctx->redisCtx;
  RedisModuleString *krstr =
      RedisModule_CreateString(ctx, options->dmd->keyPtr, sdslen(options->dmd->keyPtr));

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
    RSValue *vptr = replyElemToValue(repv, rlk->fieldtype, options->copyStrings);
    RLookup_WriteOwnKey(rlk, dst, vptr);
  }

  rc = REDISMODULE_OK;

done:
  if (krstr) {
    RedisModule_FreeString(ctx, krstr);
  }
  if (rep && (options->copyStrings || rc != REDISMODULE_OK)) {
    RedisModule_FreeCallReply(rep);
  }
  return rc;
}

int RLookup_LoadDocument(RLookup *it, RLookupRow *dst, RLookupLoadOptions *options) {
  if (options->dmd) {
    dst->sv = options->dmd->sortVector;
  }
  if (options->mode & RLOOKUP_LOAD_ALLKEYS) {
    return RLookup_HGETALL(it, dst, options);
  } else {
    return loadIndividualKeys(it, dst, options);
  }
}