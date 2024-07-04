/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "document.h"
#include "stemmer.h"
#include "rmalloc.h"
#include "module.h"
#include "rmutil/rm_assert.h"

void Document_Init(Document *doc, RedisModuleString *docKey, double score, RSLanguage lang, DocumentType type) {
  doc->docKey = docKey;
  doc->score = (float)score;
  doc->numFields = 0;
  doc->fields = NULL;
  doc->language = lang ? lang : DEFAULT_LANGUAGE;
  doc->payload = NULL;
  doc->payloadSize = 0;
  doc->type = type;
}

// Nor related to AS attribute. Used by LLAPI.
static DocumentField *addFieldCommon(Document *d, const char *fieldname, uint32_t typemask) {
  d->fields = rm_realloc(d->fields, (++d->numFields) * sizeof(*d->fields));
  DocumentField *f = d->fields + d->numFields - 1;
  f->indexAs = typemask;
  if (d->flags & DOCUMENT_F_OWNSTRINGS) {
    f->name = rm_strdup(fieldname);
  } else {
    f->name = fieldname;
  }
  f->path = NULL;
  return f;
}

void Document_AddField(Document *d, const char *fieldname, RedisModuleString *fieldval,
                       uint32_t typemask) {
  DocumentField *f = addFieldCommon(d, fieldname, typemask);
  if (d->flags & DOCUMENT_F_OWNSTRINGS) {
    f->text = RedisModule_CreateStringFromString(RSDummyContext, fieldval);
  } else {
    f->text = fieldval;
  }
}

void Document_AddFieldC(Document *d, const char *fieldname, const char *val, size_t vallen,
                        uint32_t typemask) {
  RS_LOG_ASSERT(d->flags & DOCUMENT_F_OWNSTRINGS, "Document should own strings");
  DocumentField *f = addFieldCommon(d, fieldname, typemask);
  f->strval = rm_strndup(val, vallen);
  f->strlen = vallen;
  f->unionType = FLD_VAR_T_CSTR;
}

void Document_AddNumericField(Document *d, const char *fieldname, double val,
                        uint32_t typemask) {
  DocumentField *f = addFieldCommon(d, fieldname, typemask);
  f->numval = val;
  f->unionType = FLD_VAR_T_NUM;
}

void Document_AddGeoField(Document *d, const char *fieldname,
                          double lon, double lat, uint32_t typemask) {
  DocumentField *f = addFieldCommon(d, fieldname, typemask);
  f->lat = lat;
  f->lon = lon;
  f->unionType = FLD_VAR_T_GEO;
}

void Document_MakeStringsOwner(Document *d) {
  if (d->flags & DOCUMENT_F_OWNSTRINGS) {
    // Already the owner
    return;
  }
  RedisModuleString *oldDocKey = d->docKey;
  d->docKey = RedisModule_CreateStringFromString(RSDummyContext, oldDocKey);
  if (d->flags & DOCUMENT_F_OWNREFS) {
    RedisModule_FreeString(RSDummyContext, oldDocKey);
  }

  for (size_t ii = 0; ii < d->numFields; ++ii) {
    DocumentField *f = d->fields + ii;
    if (f->path != f->name) {
      f->name = rm_strdup(f->name);
    }
    f->path = rm_strdup(f->path);
    if (f->text && f->unionType == FLD_VAR_T_RMS) {
      RedisModuleString *oldText = f->text;
      f->text = RedisModule_CreateStringFromString(RSDummyContext, oldText);
      if (d->flags & DOCUMENT_F_OWNREFS) {
        RedisModule_FreeString(RSDummyContext, oldText);
      }
    }
  }
  if (d->payload) {
    void *tmp = rm_malloc(d->payloadSize);
    memcpy(tmp, d->payload, d->payloadSize);
    d->payload = tmp;
  }
  d->flags |= DOCUMENT_F_OWNSTRINGS;
  d->flags &= ~DOCUMENT_F_OWNREFS;
}

// TODO remove uncovered and clean DOCUMENT_F_OWNREFS from all code
void Document_MakeRefOwner(Document *doc) {
  doc->flags |= DOCUMENT_F_OWNREFS;
}

int Document_LoadSchemaFieldHash(Document *doc, RedisSearchCtx *sctx, QueryError *status) {
  RedisModuleKey *k = RedisModule_OpenKey(sctx->redisCtx, doc->docKey, REDISMODULE_READ);
  int rv = REDISMODULE_ERR;
  // DvirDu: Is this even possible?
  if (!k || RedisModule_KeyType(k) != REDISMODULE_KEYTYPE_HASH) {
    QueryError_SetErrorFmt(status, QUERY_EINVAL, "Key %s does not exist or is not a hash", RedisModule_StringPtrLen(doc->docKey, NULL));
    goto done;
  }

  size_t nitems = sctx->spec->numFields;
  IndexSpec *spec = sctx->spec;
  SchemaRule *rule = spec->rule;
  assert(rule);
  RedisModuleString *payload_rms = NULL;
  Document_MakeStringsOwner(doc); // TODO: necessary?
  const char *keyname = (const char *)RedisModule_StringPtrLen(doc->docKey, NULL);
  doc->language = SchemaRule_HashLang(sctx->redisCtx, rule, k, keyname);
  doc->score = SchemaRule_HashScore(sctx->redisCtx, rule, k, keyname);
  payload_rms = SchemaRule_HashPayload(sctx->redisCtx, rule, k, keyname);
  if (payload_rms) {
    doc->flags |= Document_HasPayload;
    const char *payload_str = RedisModule_StringPtrLen(payload_rms, &doc->payloadSize);
    doc->payload = rm_malloc(doc->payloadSize);
    memcpy((char *)doc->payload, payload_str, doc->payloadSize);
    RedisModule_FreeString(sctx->redisCtx, payload_rms);
  }

  // Load indexed fields from the document
  doc->fields = rm_calloc(nitems, sizeof(*doc->fields));
  for (size_t ii = 0; ii < spec->numFields; ++ii) {
    FieldSpec *field = &spec->fields[ii];
    RedisModuleString *v = NULL;
    RedisModule_HashGet(k, REDISMODULE_HASH_CFIELDS, field->path, &v, NULL);
    if (v == NULL) {
      continue;
    }
    size_t oix = doc->numFields++;
    doc->fields[oix].path = rm_strdup(field->path);
    doc->fields[oix].name = (field->name == field->path) ? doc->fields[oix].path
                                                         : rm_strdup(field->name);
    // on crdt the return value might be the underline value, we must copy it!!!
    doc->fields[oix].text = RedisModule_CreateStringFromString(sctx->redisCtx, v);
    doc->fields[oix].unionType = FLD_VAR_T_RMS;
    RedisModule_FreeString(sctx->redisCtx, v);
  }
  rv = REDISMODULE_OK;

done:
  if (k) {
    RedisModule_CloseKey(k);
  }
  return rv;
}

int Document_LoadSchemaFieldJson(Document *doc, RedisSearchCtx *sctx, QueryError* status) {
  int rv = REDISMODULE_ERR;
  if (!japi) {
    RedisModule_Log(sctx->redisCtx, "warning", "cannot operate on a JSON index as RedisJSON is not loaded");
    QueryError_SetError(status, QUERY_EGENERIC, "cannot operate on a JSON index as RedisJSON is not loaded");
    return REDISMODULE_ERR;
  }
  IndexSpec *spec = sctx->spec;
  SchemaRule *rule = spec->rule;
  RedisModuleCtx *ctx = sctx->redisCtx;
  size_t nitems = sctx->spec->numFields;
  JSONResultsIterator jsonIter = NULL;

  RedisJSON jsonRoot = japi->openKey(ctx, doc->docKey);
  if (!jsonRoot) {
    goto done;
  }
  Document_MakeStringsOwner(doc); // TODO: necessary??

  const char *keyName = RedisModule_StringPtrLen(doc->docKey, NULL);
  doc->language = SchemaRule_JsonLang(sctx->redisCtx, rule, jsonRoot, keyName);
  doc->score = SchemaRule_JsonScore(sctx->redisCtx, rule, jsonRoot, keyName);
  // No payload on JSON as RedisJSON does not support binary fields

  doc->fields = rm_calloc(nitems, sizeof(*doc->fields));
  size_t ii = 0;
  for (; ii < spec->numFields; ++ii) {
    FieldSpec *field = &spec->fields[ii];

    jsonIter = japi->get(jsonRoot, field->path);
    // if field does not exist or is empty (can happen after JSON.DEL)
    if (!jsonIter) {
        continue;
    }

    size_t len = japi->len(jsonIter);
    if (len == 0) {
      japi->freeIter(jsonIter);
      jsonIter = NULL;
      continue;
    }

    size_t oix = doc->numFields++;
    doc->fields[oix].path = rm_strdup(field->path);
    doc->fields[oix].name = (field->name == field->path) ? doc->fields[oix].path
                                                         : rm_strdup(field->name);

    // on crdt the return value might be the underline value, we must copy it!!!
    // TODO: change `fs->text` to support hash or json not RedisModuleString
    if (JSON_LoadDocumentField(jsonIter, len, field, &doc->fields[oix], ctx, status) != REDISMODULE_OK) {
      RedisModule_Log(ctx, "verbose", "Failed to load value from field %s", field->path);
      goto done;
    }
    japi->freeIter(jsonIter);
    jsonIter = NULL;
  }
  rv = REDISMODULE_OK;

done:
  if (jsonIter) {
    japi->freeIter(jsonIter);
  }
  return rv;
}

/* used only by unit tests */
int Document_LoadAllFields(Document *doc, RedisModuleCtx *ctx) {
  int rc = REDISMODULE_ERR;
  RedisModuleCallReply *rep = NULL;

  // Hash command is not related to other type such as JSON
  rep = RedisModule_Call(ctx, "HGETALL", "s", doc->docKey);
  if (rep == NULL || RedisModule_CallReplyType(rep) != REDISMODULE_REPLY_ARRAY) {
    goto done;
  }

  size_t len = RedisModule_CallReplyLength(rep);
  // Zero means the document does not exist in redis
  if (len == 0) {
    goto done;
  }

  Document_MakeStringsOwner(doc);

  doc->fields = rm_calloc(len / 2, sizeof(DocumentField));
  doc->numFields = len / 2;
  size_t n = 0;
  RedisModuleCallReply *k, *v;
  for (size_t i = 0; i < len; i += 2, ++n) {
    k = RedisModule_CallReplyArrayElement(rep, i);
    v = RedisModule_CallReplyArrayElement(rep, i + 1);
    size_t nlen = 0;
    const char *name = RedisModule_CallReplyStringPtr(k, &nlen);
    doc->fields[n].name = rm_strndup(name, nlen);
    doc->fields[n].text = RedisModule_CreateStringFromCallReply(v);
    doc->fields[n].unionType = FLD_VAR_T_RMS;
  }
  rc = REDISMODULE_OK;

done:
  if (rep) {
    RedisModule_FreeCallReply(rep);
  }
  return rc;
}

int Document_ReplyAllFields(RedisModuleCtx *ctx, IndexSpec *spec, RedisModuleString *id) {
  int rc = REDISMODULE_ERR;
  RedisModuleCallReply *rep = NULL;

  // Hash command is not related to other type such as JSON. Used for FT.GET which is deprecated.
  rep = RedisModule_Call(ctx, "HGETALL", "s", id);
  if (rep == NULL || RedisModule_CallReplyType(rep) != REDISMODULE_REPLY_ARRAY) {
    RedisModule_ReplyWithArray(ctx, 0);
    goto done;
  }

  size_t hashLen = RedisModule_CallReplyLength(rep);
  RS_LOG_ASSERT(hashLen % 2 == 0, "Number of elements must be even");
  // Zero means the document does not exist in redis
  if (hashLen == 0) {
    RedisModule_ReplyWithArray(ctx, 0);
    goto done;
  }

  size_t strLen;
  RedisModuleCallReply *e;
  SchemaRule *rule = spec->rule;
  RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
  size_t numElems = 0;

  size_t lang_len = rule->lang_field ? strlen(rule->lang_field) : 0;
  size_t score_len = rule->score_field ? strlen(rule->score_field) : 0;
  size_t payload_len = rule->payload_field ? strlen(rule->payload_field) : 0;

  for (size_t i = 0; i < hashLen; i += 2) {
    // parse field
    e = RedisModule_CallReplyArrayElement(rep, i);
    const char *str = RedisModule_CallReplyStringPtr(e, &strLen);
    RS_LOG_ASSERT(strLen > 0, "field string cannot be empty");
    if ((lang_len == strLen && strncasecmp(str, rule->lang_field, strLen) == 0) ||
        (score_len == strLen && strncasecmp(str, rule->score_field, strLen) == 0) ||
        (payload_len == strLen && strncasecmp(str, rule->payload_field, strLen) == 0)) {
      continue;
    }
    RedisModule_ReplyWithStringBuffer(ctx, str, strLen);

    // parse value
    e = RedisModule_CallReplyArrayElement(rep, i + 1);
    str = RedisModule_CallReplyStringPtr(e, &strLen);
    if (strLen != 0) {
      RedisModule_ReplyWithStringBuffer(ctx, str, strLen);
    } else {
      RedisModule_ReplyWithNull(ctx);
    }
    numElems += 2;
  }
  RedisModule_ReplySetArrayLength(ctx, numElems);
  rc = REDISMODULE_OK;

done:
  if (rep) {
    RedisModule_FreeCallReply(rep);
  }
  return rc;
}

void Document_LoadPairwiseArgs(Document *d, RedisModuleString **args, size_t nargs) {
  d->fields = rm_calloc(nargs / 2, sizeof(*d->fields));
  d->numFields = nargs / 2;
  size_t oix = 0;
  for (size_t ii = 0; ii < nargs; ii += 2, oix++) {
    DocumentField *dst = d->fields + oix;
    const char *name = RedisModule_StringPtrLen(args[ii], NULL);
    dst->name = name;
    dst->text = args[ii + 1];
    dst->unionType = FLD_VAR_T_RMS;
  }
}

void Document_Clear(Document *d) {
  if (d->flags & (DOCUMENT_F_OWNSTRINGS | DOCUMENT_F_OWNREFS)) {
    for (size_t ii = 0; ii < d->numFields; ++ii) {
      DocumentField *field = &d->fields[ii];
      if (d->flags & DOCUMENT_F_OWNSTRINGS) {
        rm_free((void *)field->name);
        if (field->path && (field->path != field->name)) {
          rm_free((void *)field->path);
        }
      }
      switch (field->unionType) {
        case FLD_VAR_T_RMS:
          RedisModule_FreeString(RSDummyContext, field->text);
          break;
        case FLD_VAR_T_CSTR:
          rm_free(field->strval);
          break;
        case FLD_VAR_T_ARRAY:
        // TODO: GEOMETRY Handle multi-value geometry fields
          if (field->indexAs & (INDEXFLD_T_FULLTEXT | INDEXFLD_T_TAG | INDEXFLD_T_GEO)) {
            for (int i = 0; i < field->arrayLen; ++i) {
              rm_free(field->multiVal[i]);
            }
            rm_free(field->multiVal);
            field->arrayLen = 0;
          } else if (field->indexAs & INDEXFLD_T_NUMERIC) {
            array_free(field->arrNumval);
          }
          if (field->multisv) {
            RSValue_Free(field->multisv);
          }
          break;
        case FLD_VAR_T_BLOB_ARRAY:
          rm_free(field->blobArr);
          field->blobArrLen = 0;
          break;
        case FLD_VAR_T_GEO:
        case FLD_VAR_T_NUM:
        case FLD_VAR_T_GEOMETRY:
        case FLD_VAR_T_NULL:
          break;
      }
    }
  }
  rm_free(d->fields);
  d->numFields = 0;
  d->fields = NULL;
}

void Document_Free(Document *doc) {
  Document_Clear(doc);
  if (doc->flags & (DOCUMENT_F_OWNREFS | DOCUMENT_F_OWNSTRINGS)) {
    RedisModule_FreeString(RSDummyContext, doc->docKey);
  }
  if (doc->flags & DOCUMENT_F_OWNSTRINGS) {
    if (doc->payload) {
      rm_free((void *)doc->payload);
    }
  }
  if (doc->fieldExpirations) {
    rm_free(doc->fieldExpirations);
  }
}

#define globalAddRSstringsSize 3
static RedisModuleString *globalAddRSstrings[globalAddRSstringsSize] = {0};

static void initGlobalAddStrings() {
  const char *Sscore = UNDERSCORE_SCORE;
  const char *Slang = UNDERSCORE_LANGUAGE;
  const char *Spayload = UNDERSCORE_PAYLOAD;

  globalAddRSstrings[0] = RedisModule_CreateString(NULL, Sscore, strlen(Sscore));
  globalAddRSstrings[1] = RedisModule_CreateString(NULL, Slang, strlen(Slang));
  globalAddRSstrings[2] = RedisModule_CreateString(NULL, Spayload, strlen(Spayload));
}

void freeGlobalAddStrings() {
  if (globalAddRSstrings[0] == NULL) return;

  for (size_t i = 0; i < 3; ++i) {
    RedisModule_FreeString(NULL, globalAddRSstrings[i]);
    globalAddRSstrings[i] = NULL;
  }
}

int Redis_SaveDocument(RedisSearchCtx *ctx, const AddDocumentOptions *opts, QueryError *status) {
  if (globalAddRSstrings[0] == NULL) {
    initGlobalAddStrings();
  }

  // create an array for key + all field/value + score/language/payload
  arrayof(RedisModuleString *) arguments =
      array_new(RedisModuleString *, 1 + opts->numFieldElems + 6);

  array_append(arguments, opts->keyStr);
  arguments = array_ensure_append_n(arguments, opts->fieldsArray, opts->numFieldElems);

  if (opts->score != DEFAULT_SCORE || (opts->options & DOCUMENT_ADD_PARTIAL)) {
    array_append(arguments, globalAddRSstrings[0]);
    array_append(arguments, opts->scoreStr);
    RedisSearchCtx_LockSpecWrite(ctx);
    if (ctx->spec->rule->score_field == NULL) {
      ctx->spec->rule->score_field = rm_strndup(UNDERSCORE_SCORE, strlen(UNDERSCORE_SCORE));
    }
    RedisSearchCtx_UnlockSpec(ctx);
  }

  if (opts->languageStr) {
    array_append(arguments, globalAddRSstrings[1]);
    array_append(arguments, opts->languageStr);
    RedisSearchCtx_LockSpecWrite(ctx);
    if (ctx->spec->rule->lang_field == NULL) {
      ctx->spec->rule->lang_field = rm_strndup(UNDERSCORE_LANGUAGE, strlen(UNDERSCORE_LANGUAGE));
    }
    RedisSearchCtx_UnlockSpec(ctx);
  }

  if (opts->payload) {
    array_append(arguments, globalAddRSstrings[2]);
    array_append(arguments, opts->payload);
    RedisSearchCtx_LockSpecWrite(ctx);
    if (ctx->spec->rule->payload_field == NULL) {
      ctx->spec->rule->payload_field = rm_strndup(UNDERSCORE_PAYLOAD, strlen(UNDERSCORE_PAYLOAD));
    }
    RedisSearchCtx_UnlockSpec(ctx);
  }

  RedisModuleCallReply *rep = NULL;
  if (isCrdt) {
    // crdt assumes that it gets its own copy of the arguments so lets give it to them
    for (size_t i = 0; i < array_len(arguments); ++i) {
      arguments[i] = RedisModule_CreateStringFromString(ctx->redisCtx, arguments[i]);
    }
  }
  rep = RedisModule_Call(ctx->redisCtx, "HSET", "!v", arguments, (size_t)array_len(arguments));
  if (rep) {
    RedisModule_FreeCallReply(rep);
  }

  if (isCrdt) {
    for (size_t i = 0; i < array_len(arguments); ++i) {
      RedisModule_FreeString(ctx->redisCtx, arguments[i]);
    }
  }
  array_free(arguments);

  return REDISMODULE_OK;
}
