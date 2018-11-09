#include "rlookup.h"
#include <document.h>
#include <assert.h>
#include <util/arr.h>

static RLookupKey *createNewKey(RLookup *lookup, const char *name, int flags, uint16_t idx) {
  RLookupKey *ret = calloc(1, sizeof(*ret));

  ret->flags = (flags & (~RLOOKUP_TRANSIENT_FLAGS));
  ret->dstidx = idx;
  ret->refcnt = 1;

  if (flags & RLOOKUP_F_NAMEALLOC) {
    ret->name = strdup(name);
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

  RLookupKey *ret = createNewKey(lookup, name, flags, idx);
  if (FieldSpec_IsSortable(fs)) {
    flags |= RLOOKUP_F_SVSRC;
    ret->svidx = fs->sortIdx;
  } else {
    flags |= RLOOKUP_F_DOCSRC;
  }
  return ret;
}

RLookupKey *RLookup_GetKey(RLookup *lookup, const char *name, int flags) {
  RLookupKey *ret = NULL;
  int isNew = 0;

  for (RLookupKey *kk = lookup->head; kk; kk = kk->next) {
    if (!strcmp(kk->name, name)) {
      ret = kk;
      break;
    }
  }

  if (!ret) {
    ret = genKeyFromSpec(lookup, name, flags);
  }

  if (!ret) {
    if (!(flags & RLOOKUP_F_OCREAT)) {
      return NULL;
    } else {
      ret = createNewKey(lookup, name, flags, lookup->rowlen++);
    }
  }

  if (!(flags & RLOOKUP_F_NOINCREF)) {
    ret->refcnt++;
  }
  return ret;
}

void RLookup_Init(RLookup *lk, IndexSpecCache *spcache) {
  memset(lk, 0, sizeof(*lk));
  if (spcache) {
    lk->spcache = spcache;
  }
}

void RLookup_WriteKey(const RLookupKey *key, RLookupRow *row, RSValue *v) {
  assert(!(key->flags & RLOOKUP_F_SVSRC));

  // Find the pointer to write to ...
  RSValue **vptr = array_ensure_at(&row->dyn, key->dstidx, RSValue *);
  if (*vptr) {
    RSValue_Decref(*vptr);
    row->ndyn--;
  }
  *vptr = v;
  RSValue_IncrRef(v);
  row->ndyn++;
}

void RLookup_WriteKeyByName(RLookup *lookup, const char *name, RLookupRow *dst, RSValue *v) {
  // Get the key first
  RLookupKey *k =
      RLookup_GetKey(lookup, name, RLOOKUP_F_NAMEALLOC | RLOOKUP_F_NOINCREF | RLOOKUP_F_OCREAT);
  assert(k);
  RLookup_WriteKey(k, dst, v);
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
}

void RLookupRow_Cleanup(RLookupRow *r) {
  RLookupRow_Wipe(r);
  array_free(r->dyn);
}

void RLookupKey_FreeInternal(RLookupKey *k) {
  free(k);
}

void RLookup_Cleanup(RLookup *lk) {
  RLookupKey *next, *cur = lk->head;
  while (cur) {
    next = cur->next;
    RLKEY_DECREF(cur);
    cur = next;
  }
  if (lk->spcache) {
    IndexSpecCache_Decref(lk->spcache);
    lk->spcache = NULL;
  }

  lk->head = lk->tail = NULL;
}

static RSValue *hvalToValue(RedisModuleString *src, RLookupCoerceType type) {
  if (type == RLOOKUP_C_BOOL || type == RLOOKUP_C_INT) {
    long long ll = 0;
    RedisModule_StringToLongLong(src, &ll);
    return RS_Int64Val(ll);
  } else if (type == RLOOKUP_C_DBL) {
    double dd = 0.0;
    RedisModule_StringToDouble(src, &dd);
    return RS_NumVal(dd);
  } else {
    size_t n;
    const char *s = RedisModule_StringPtrLen(src, &n);
    return RS_NewCopiedString(s, n);
  }
}

static void maybeWriteFromSV(RSSortingVector *sv, const RLookupKey *kk, RLookupRow *dst) {
  if (sv->len <= kk->svidx) {
    return;
  }
  RSValue *v = sv->values[kk->svidx];
  if (!v) {
    return;
  }
  RLookup_WriteKey(kk, dst, v);
}

static int RLookup_LoadFromSchema(RLookup *it, RLookupRow *dst, RLookupLoadOptions *options) {
  // Load the document from the schema. This should be simple enough...
  for (const RLookupKey *kk = it->head; kk; kk = kk->next) {
    if (kk->flags & RLOOKUP_F_SVSRC) {
      maybeWriteFromSV(options->dmd->sortVector, kk, dst);
      continue;
    }

    if (!options->loadNonCached) {
      continue;
    }

    if (!(kk->flags & RLOOKUP_F_DOCSRC)) {
      continue;
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
        goto error;
      }
      if (RedisModule_KeyType(options->keyobj) != REDISMODULE_KEYTYPE_HASH) {
        QueryError_SetCode(options->status, QUERY_EREDISKEYTYPE);
        goto error;
      }
    }

    // Get the actual hash value
    RedisModuleString *val = NULL;
    int rc = RedisModule_HashGet(options->keyobj, REDISMODULE_HASH_CFIELDS, kk->name, &val, NULL);
    if (rc != REDISMODULE_OK) {
      continue;  // Doesn't exist..
    }

    // Value has a reference count of 1
    RSValue *rsv = hvalToValue(val, kk->fieldtype);
    RLookup_WriteKey(kk, dst, rsv);
    RSValue_Decref(rsv);
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
  Document dd = {0};
  int rv =
      Redis_LoadDocumentC(options->sctx, options->dmd->keyPtr, strlen(options->dmd->keyPtr), &dd);
  if (rv != REDISMODULE_OK) {
    return rv;
  }

  for (size_t ii = 0; ii < dd.numFields; ++ii) {
    RLookupKey *kk = RLookup_GetKey(it, dd.fields[ii].name, RLOOKUP_F_OCREAT);
    if (!kk) {
      // wtf?
      abort();
    }
    if (kk->flags & RLOOKUP_F_SVSRC) {
      // Can we get this from the sort vector?
      maybeWriteFromSV(options->dmd->sortVector, kk, dst);
      continue;  // Can load it from the sort vector on demand.
    }
    RSValue *vv = hvalToValue(dd.fields[ii].text, kk->fieldtype);
    RLookup_WriteKey(kk, dst, vv);
    RSValue_Decref(vv);
  }
  Document_Free(&dd);
  return REDISMODULE_OK;
}

int RLookup_LoadDocument(RLookup *it, RLookupRow *dst, RLookupLoadOptions *options) {
  if (options->loadAllFields) {
    return RLookup_HGETALL(it, dst, options);
  }

  if (options->loadNonCached) {
    return RLookup_LoadFromSchema(it, dst, options);
  }
}