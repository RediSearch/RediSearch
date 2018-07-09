#include "debug_commads.h"
#include "inverted_index.h"
#include "index.h"
#include "redis_index.h"
#include "tag_index.h"
#include "numeric_index.h"

static void ReplyReaderResults(IndexReader *reader, RedisModuleCtx *ctx) {
  IndexIterator *iter = NewReadIterator(reader);
  RSIndexResult *r;
  size_t resultSize = 0;
  RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
  while (iter->Read(iter->ctx, &r) != INDEXREAD_EOF) {
    RedisModule_ReplyWithLongLong(ctx, r->docId);
    ++resultSize;
  }
  RedisModule_ReplySetArrayLength(ctx, resultSize);
  ReadIterator_Free(iter);
}

static void DumpInvertedIndex(RedisSearchCtx *sctx, RedisModuleString *invidxName) {
  RedisModuleKey *keyp = NULL;
  size_t len;
  const char *invIdxName = RedisModule_StringPtrLen(invidxName, &len);
  InvertedIndex *invidx = Redis_OpenInvertedIndexEx(sctx, invIdxName, len, 0, &keyp);
  if (!invidx) {
    RedisModule_ReplyWithError(sctx->redisCtx, "Can not find the inverted index");
    goto end;
  }
  IndexReader *reader = NewTermIndexReader(invidx, NULL, RS_FIELDMASK_ALL, NULL, 1);
  ReplyReaderResults(reader, sctx->redisCtx);

end:
  if (keyp) {
    RedisModule_CloseKey(keyp);
  }
}

static RedisModuleString *getFieldKeyName(IndexSpec *spec, RedisModuleString *fieldNameRS) {
  const char *fieldName = RedisModule_StringPtrLen(fieldNameRS, NULL);
  FieldSpec *fieldSpec = IndexSpec_GetField(spec, fieldName, strlen(fieldName));
  if (!fieldSpec) {
    return NULL;
  }
  return IndexSpec_GetFormattedKey(spec, fieldSpec);
}

static void DumpTagIndex(RedisSearchCtx *sctx, RedisModuleString *fieldNameRS) {
  RedisModuleKey *keyp = NULL;
  RedisModuleString *keyName = getFieldKeyName(sctx->spec, fieldNameRS);
  if (!keyName) {
    RedisModule_ReplyWithError(sctx->redisCtx, "Could not find given field in index spec");
    goto end;
  }
  TagIndex *tagIndex = TagIndex_Open(sctx->redisCtx, keyName, false, &keyp);
  if (!tagIndex) {
    RedisModule_ReplyWithError(sctx->redisCtx, "can not open tag field");
    goto end;
  }

  TrieMapIterator *iter = TrieMap_Iterate(tagIndex->values, "", 0);

  char *tag;
  tm_len_t len;
  InvertedIndex *iv;

  size_t resultSize = 0;
  RedisModule_ReplyWithArray(sctx->redisCtx, REDISMODULE_POSTPONED_ARRAY_LEN);
  while (TrieMapIterator_Next(iter, &tag, &len, (void **)&iv)) {
    RedisModule_ReplyWithArray(sctx->redisCtx, 2);
    RedisModule_ReplyWithStringBuffer(sctx->redisCtx, tag, len);
    IndexReader *reader = NewTermIndexReader(iv, NULL, RS_FIELDMASK_ALL, NULL, 1);
    ReplyReaderResults(reader, sctx->redisCtx);
    ++resultSize;
  }
  RedisModule_ReplySetArrayLength(sctx->redisCtx, resultSize);
  TrieMapIterator_Free(iter);

end:
  if (keyp) {
    RedisModule_CloseKey(keyp);
  }
}

static void DumpNumericIndex(RedisSearchCtx *sctx, RedisModuleString *fieldNameRS) {
  RedisModuleKey *keyp = NULL;
  RedisModuleString *keyName = getFieldKeyName(sctx->spec, fieldNameRS);
  if (!keyName) {
    RedisModule_ReplyWithError(sctx->redisCtx, "Could not find given field in index spec");
    goto end;
  }
  NumericRangeTree *rt = OpenNumericIndex(sctx, keyName, &keyp);
  if (!rt) {
    RedisModule_ReplyWithError(sctx->redisCtx, "can not open numeric field");
    goto end;
  }
  NumericRangeNode *currNode;
  NumericRangeTreeIterator *iter = NumericRangeTreeIterator_New(rt);
  size_t resultSize = 0;
  RedisModule_ReplyWithArray(sctx->redisCtx, REDISMODULE_POSTPONED_ARRAY_LEN);
  while ((currNode = NumericRangeTreeIterator_Next(iter))) {
    if (currNode->range) {
      IndexReader *reader = NewNumericReader(currNode->range->entries, NULL);
      ReplyReaderResults(reader, sctx->redisCtx);
      ++resultSize;
    }
  }
  RedisModule_ReplySetArrayLength(sctx->redisCtx, resultSize);
  NumericRangeTreeIterator_Free(iter);
end:
  if (keyp) {
    RedisModule_CloseKey(keyp);
  }
}

int DebugCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {

  if (argc != 4) return RedisModule_WrongArity(ctx);

  IndexSpec *sp = IndexSpec_Load(ctx, RedisModule_StringPtrLen(argv[2], NULL), 0);
  if (!sp) {
    RedisModule_ReplyWithError(ctx, "Unknown index name");
    return REDISMODULE_OK;
  }
  RedisSearchCtx sctx = {NULL};
  sctx.redisCtx = ctx;
  sctx.spec = sp;

  const char *subCommand = RedisModule_StringPtrLen(argv[1], NULL);

  if (strcmp(subCommand, DUMP_INVIDX_COMMAND) == 0) {
    DumpInvertedIndex(&sctx, argv[3]);
  } else if (strcmp(subCommand, DUMP_NUMIDX_COMMAND) == 0) {
    DumpNumericIndex(&sctx, argv[3]);
  } else if (strcmp(subCommand, DUMP_TAGIDX_COMMAND) == 0) {
    DumpTagIndex(&sctx, argv[3]);
  } else {
    RedisModule_ReplyWithError(ctx, "no such subcommand");
  }

  return REDISMODULE_OK;
}
