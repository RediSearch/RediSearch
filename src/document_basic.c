#include "document.h"
#include "stemmer.h"
#include "rmalloc.h"

void Document_Init(Document *doc, RedisModuleString *docKey, double score, int numFields,
                   const char *lang, const char *payload, size_t payloadSize) {
  doc->docKey = docKey;
  doc->score = (float)score;
  doc->numFields = numFields;
  doc->fields = numFields ? calloc(doc->numFields, sizeof(DocumentField)) : NULL;
  doc->language = lang;
  doc->payload = payload;
  doc->payloadSize = payloadSize;
}

void Document_AddField(Document *d, const char *fieldname, RedisModuleString *fieldval,
                       uint32_t typemask) {
  d->fields = realloc(d->fields, (++d->numFields) * sizeof(*d->fields));
  DocumentField *f = d->fields + d->numFields - 1;
  f->indexAs = typemask;
  f->name = strdup(fieldname);  // free_detached called on this later on..
  f->text = fieldval;
}

void Document_PrepareForAdd(Document *doc, RedisModuleString *docKey, double score,
                            const AddDocumentOptions *opts, RedisModuleCtx *ctx) {
  size_t payloadSize = 0;
  const char *payloadStr = NULL;
  if (opts->payload) {
    payloadStr = RedisModule_StringPtrLen(opts->payload, &payloadSize);
  }

  Document_Init(doc, docKey, score, opts->numFieldElems / 2,
                opts->language ? opts->language : DEFAULT_LANGUAGE, payloadStr, payloadSize);
  for (size_t ii = 0, n = 0; ii < opts->numFieldElems; ii += 2, n++) {
    // printf("indexing '%s' => '%s'\n", RedisModule_StringPtrLen(opts->fieldsArray[ii], NULL),
    //        RedisModule_StringPtrLen(opts->fieldsArray[ii + 1], NULL));
    doc->fields[n].name = RedisModule_StringPtrLen(opts->fieldsArray[ii], NULL);
    doc->fields[n].text = RedisModule_CreateStringFromString(ctx, opts->fieldsArray[ii + 1]);
  }

  Document_Detach(doc, ctx);
}

void Document_DetachFields(Document *doc, RedisModuleCtx *ctx) {
  for (size_t ii = 0; ii < doc->numFields; ++ii) {
    DocumentField *f = doc->fields + ii;
    f->name = strdup(f->name);
  }
}

void Document_ClearDetachedFields(Document *doc, RedisModuleCtx *anyCtx) {
  for (size_t ii = 0; ii < doc->numFields; ++ii) {
    if (doc->fields[ii].text) {
      RedisModule_FreeString(anyCtx, doc->fields[ii].text);
    }
    free((void *)doc->fields[ii].name);
  }
  free(doc->fields);
  doc->fields = NULL;
  doc->numFields = 0;
}

void Document_Detach(Document *doc, RedisModuleCtx *srcCtx) {
  RedisModule_RetainString(srcCtx, doc->docKey);

  Document_DetachFields(doc, srcCtx);
  if (doc->payload) {
    char *tmp = malloc(doc->payloadSize);
    memcpy(tmp, doc->payload, doc->payloadSize);
    doc->payload = tmp;
  }
  if (doc->language) {
    doc->language = strdup(doc->language);
  }
}

void Document_Free(Document *doc) {
  free(doc->fields);
}

void Document_FreeDetached(Document *doc, RedisModuleCtx *anyCtx) {
  RedisModule_FreeString(anyCtx, doc->docKey);
  Document_ClearDetachedFields(doc, anyCtx);
  free((char *)doc->payload);
  free((char *)doc->language);

  Document_Free(doc);
}

int Redis_SaveDocument(RedisSearchCtx *ctx, Document *doc, int options, QueryError *status) {
  RedisModuleKey *k =
      RedisModule_OpenKey(ctx->redisCtx, doc->docKey, REDISMODULE_WRITE | REDISMODULE_READ);
  if (k == NULL || (RedisModule_KeyType(k) != REDISMODULE_KEYTYPE_EMPTY &&
                    RedisModule_KeyType(k) != REDISMODULE_KEYTYPE_HASH)) {
    QueryError_SetError(status, QUERY_EREDISKEYTYPE, NULL);
    if (k) {
      RedisModule_CloseKey(k);
    }
    return REDISMODULE_ERR;
  }
  if ((options & REDIS_SAVEDOC_NOCREATE) && RedisModule_KeyType(k) == REDISMODULE_KEYTYPE_EMPTY) {
    RedisModule_CloseKey(k);
    QueryError_SetError(status, QUERY_ENODOC, "Document does not exist");
    return REDISMODULE_ERR;
  }

  for (int i = 0; i < doc->numFields; i++) {
    RedisModule_HashSet(k, REDISMODULE_HASH_CFIELDS, doc->fields[i].name, doc->fields[i].text,
                        NULL);
  }
  RedisModule_CloseKey(k);
  return REDISMODULE_OK;
}

int Document_ReplyFields(RedisModuleCtx *ctx, Document *doc) {
  if (!doc) {
    return REDISMODULE_ERR;
  }
  RedisModule_ReplyWithArray(ctx, doc->numFields * 2);
  for (size_t j = 0; j < doc->numFields; ++j) {
    RedisModule_ReplyWithStringBuffer(ctx, doc->fields[j].name, strlen(doc->fields[j].name));
    if (doc->fields[j].text) {
      RedisModule_ReplyWithString(ctx, doc->fields[j].text);
    } else {
      RedisModule_ReplyWithNull(ctx);
    }
  }
  return REDISMODULE_OK;
}
