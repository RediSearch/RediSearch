#include "document.h"

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

void Document_Detach(Document *doc, RedisModuleCtx *srcCtx) {
  RedisModule_RetainString(srcCtx, doc->docKey);
  for (size_t ii = 0; ii < doc->numFields; ++ii) {
    DocumentField *f = doc->fields + ii;
    RedisModule_RetainString(srcCtx, f->text);
    f->name = strdup(f->name);
  }
  doc->stringOwner = 1;
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

  for (size_t ii = 0; ii < doc->numFields; ++ii) {
    RedisModule_FreeString(anyCtx, doc->fields[ii].text);
    free((void *)doc->fields[ii].name);
  }

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
  ctx->spec->stats.numDocuments++;
  return REDISMODULE_OK;
}
