#include "document.h"
#include "stemmer.h"

void Document_Init(Document *doc, RedisModuleString *docKey, double score, int numFields,
                   const char *lang, const char *payload, size_t payloadSize) {
  doc->docKey = docKey;
  doc->score = (float)score;
  doc->numFields = numFields;
  doc->fields = calloc(doc->numFields, sizeof(DocumentField));
  doc->language = lang;
  doc->payload = payload;
  doc->payloadSize = payloadSize;
}

void Document_PrepareForAdd(Document *doc, RedisModuleString *docKey, double score,
                            RedisModuleString **argv, size_t fieldsOffset, size_t argc,
                            const char *language, RedisModuleString *payload, RedisModuleCtx *ctx) {
  size_t payloadSize = 0;
  const char *payloadStr = NULL;
  if (payload) {
    payloadStr = RedisModule_StringPtrLen(payload, &payloadSize);
  }

  Document_Init(doc, docKey, score, (argc - fieldsOffset) / 2,
                language ? language : DEFAULT_LANGUAGE, payloadStr, payloadSize);
  int n = 0;
  for (int i = fieldsOffset + 1; i < argc - 1; i += 2, n++) {
    // printf ("indexing '%s' => '%s'\n", RedisModule_StringPtrLen(argv[i],
    // NULL),
    // RedisModule_StringPtrLen(argv[i+1], NULL));
    doc->fields[n].name = RedisModule_StringPtrLen(argv[i], NULL);
    doc->fields[n].text = RedisModule_CreateStringFromString(ctx, argv[i + 1]);
  }

  Document_Detach(doc, ctx);
}

void Document_DetachFields(Document *doc, RedisModuleCtx *ctx) {
  for (size_t ii = 0; ii < doc->numFields; ++ii) {
    DocumentField *f = doc->fields + ii;
    RedisModule_RetainString(ctx, f->text);
    f->name = strdup(f->name);
  }
}

void Document_ClearDetachedFields(Document *doc, RedisModuleCtx *anyCtx) {
  for (size_t ii = 0; ii < doc->numFields; ++ii) {
    RedisModule_FreeString(anyCtx, doc->fields[ii].text);
    free((void *)doc->fields[ii].name);
  }
  free(doc->fields);
  doc->fields = NULL;
  doc->numFields = 0;
}

void Document_Detach(Document *doc, RedisModuleCtx *srcCtx) {
  RedisModule_RetainString(srcCtx, doc->docKey);
  doc->stringOwner = 1;

  Document_DetachFields(doc, srcCtx);
  if (doc->payload) {
    doc->payload = strndup(doc->payload, doc->payloadSize);
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

int Redis_SaveDocument(RedisSearchCtx *ctx, Document *doc) {

  RedisModuleKey *k =
      RedisModule_OpenKey(ctx->redisCtx, doc->docKey, REDISMODULE_WRITE | REDISMODULE_READ);
  if (k == NULL || (RedisModule_KeyType(k) != REDISMODULE_KEYTYPE_EMPTY &&
                    RedisModule_KeyType(k) != REDISMODULE_KEYTYPE_HASH)) {
    return REDISMODULE_ERR;
  }

  for (int i = 0; i < doc->numFields; i++) {
    RedisModule_HashSet(k, REDISMODULE_HASH_CFIELDS, doc->fields[i].name, doc->fields[i].text,
                        NULL);
  }
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