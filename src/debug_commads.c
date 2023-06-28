/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "debug_commads.h"
#include "inverted_index.h"
#include "index.h"
#include "redis_index.h"
#include "tag_index.h"
#include "numeric_index.h"
#include "geometry/geometry_api.h"
#include "geometry_index.h"
#include "phonetic_manager.h"
#include "gc.h"
#include "module.h"
#include "suffix.h"
#include "util/workers.h"
#include "util/threadpool_api.h"

#define DUMP_PHONETIC_HASH "DUMP_PHONETIC_HASH"

#define DEBUG_COMMAND(name) static int name(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)

#define GET_SEARCH_CTX(name)                                        \
  RedisSearchCtx *sctx = NewSearchCtx(ctx, name, true);             \
  if (!sctx) {                                                      \
    RedisModule_ReplyWithError(ctx, "Can not create a search ctx"); \
    return REDISMODULE_OK;                                          \
  }

#define REPLY_WITH_LONG_LONG(name, val, len)                  \
  RedisModule_ReplyWithStringBuffer(ctx, name, strlen(name)); \
  RedisModule_ReplyWithLongLong(ctx, val);                    \
  len += 2;

#define REPLY_WITH_Str(name, val)                             \
  RedisModule_ReplyWithStringBuffer(ctx, name, strlen(name)); \
  RedisModule_ReplyWithStringBuffer(ctx, val, strlen(val));   \
  bulkLen += 2;

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

static RedisModuleString *getFieldKeyName(IndexSpec *spec, RedisModuleString *fieldNameRS,
                                          FieldType t) {
  const char *fieldName = RedisModule_StringPtrLen(fieldNameRS, NULL);
  const FieldSpec *fieldSpec = IndexSpec_GetField(spec, fieldName, strlen(fieldName));
  if (!fieldSpec) {
    return NULL;
  }
  return IndexSpec_GetFormattedKey(spec, fieldSpec, t);
}

DEBUG_COMMAND(DumpTerms) {
  if (argc != 1) {
    return RedisModule_WrongArity(ctx);
  }
  GET_SEARCH_CTX(argv[0])

  rune *rstr = NULL;
  t_len slen = 0;
  float score = 0;
  int dist = 0;
  size_t termLen;

  RedisModule_ReplyWithArray(ctx, sctx->spec->terms->size);

  TrieIterator *it = Trie_Iterate(sctx->spec->terms, "", 0, 0, 1);
  while (TrieIterator_Next(it, &rstr, &slen, NULL, &score, &dist)) {
    char *res = runesToStr(rstr, slen, &termLen);
    RedisModule_ReplyWithStringBuffer(ctx, res, termLen);
    rm_free(res);
  }
  TrieIterator_Free(it);

  SearchCtx_Free(sctx);
  return REDISMODULE_OK;
}

DEBUG_COMMAND(InvertedIndexSummary) {
  if (argc != 2) {
    return RedisModule_WrongArity(ctx);
  }
  GET_SEARCH_CTX(argv[0])
  RedisModuleKey *keyp = NULL;
  size_t len;
  const char *invIdxName = RedisModule_StringPtrLen(argv[1], &len);
  InvertedIndex *invidx = Redis_OpenInvertedIndexEx(sctx, invIdxName, len, 0, NULL, &keyp);
  if (!invidx) {
    RedisModule_ReplyWithError(sctx->redisCtx, "Can not find the inverted index");
    goto end;
  }
  size_t invIdxBulkLen = 0;
  RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);

  REPLY_WITH_LONG_LONG("numDocs", invidx->numDocs, invIdxBulkLen);
  REPLY_WITH_LONG_LONG("lastId", invidx->lastId, invIdxBulkLen);
  REPLY_WITH_LONG_LONG("flags", invidx->flags, invIdxBulkLen);
  REPLY_WITH_LONG_LONG("numberOfBlocks", invidx->size, invIdxBulkLen);

  RedisModule_ReplyWithStringBuffer(ctx, "blocks", strlen("blocks"));

  for (uint32_t i = 0; i < invidx->size; ++i) {
    size_t blockBulkLen = 0;
    IndexBlock *block = invidx->blocks + i;
    RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);

    REPLY_WITH_LONG_LONG("firstId", block->firstId, blockBulkLen);
    REPLY_WITH_LONG_LONG("lastId", block->lastId, blockBulkLen);
    REPLY_WITH_LONG_LONG("numEntries", block->numEntries, blockBulkLen);

    RedisModule_ReplySetArrayLength(ctx, blockBulkLen);
  }

  invIdxBulkLen += 2;

  RedisModule_ReplySetArrayLength(ctx, invIdxBulkLen);

end:
  if (keyp) {
    RedisModule_CloseKey(keyp);
  }
  SearchCtx_Free(sctx);
  return REDISMODULE_OK;
}

DEBUG_COMMAND(DumpInvertedIndex) {
  if (argc != 2) {
    return RedisModule_WrongArity(ctx);
  }
  GET_SEARCH_CTX(argv[0])
  RedisModuleKey *keyp = NULL;
  size_t len;
  const char *invIdxName = RedisModule_StringPtrLen(argv[1], &len);
  InvertedIndex *invidx = Redis_OpenInvertedIndexEx(sctx, invIdxName, len, 0, NULL, &keyp);
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
  SearchCtx_Free(sctx);
  return REDISMODULE_OK;
}

DEBUG_COMMAND(NumericIndexSummary) {
  if (argc != 2) {
    return RedisModule_WrongArity(ctx);
  }
  GET_SEARCH_CTX(argv[0])
  RedisModuleKey *keyp = NULL;
  RedisModuleString *keyName = getFieldKeyName(sctx->spec, argv[1], INDEXFLD_T_NUMERIC);
  if (!keyName) {
    RedisModule_ReplyWithError(sctx->redisCtx, "Could not find given field in index spec");
    goto end;
  }
  NumericRangeTree *rt = OpenNumericIndex(sctx, keyName, &keyp);
  if (!rt) {
    RedisModule_ReplyWithError(sctx->redisCtx, "can not open numeric field");
    goto end;
  }

  size_t invIdxBulkLen = 0;
  RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);

  REPLY_WITH_LONG_LONG("numRanges", rt->numRanges, invIdxBulkLen);
  REPLY_WITH_LONG_LONG("numEntries", rt->numEntries, invIdxBulkLen);
  REPLY_WITH_LONG_LONG("lastDocId", rt->lastDocId, invIdxBulkLen);
  REPLY_WITH_LONG_LONG("revisionId", rt->revisionId, invIdxBulkLen);

  RedisModule_ReplySetArrayLength(ctx, invIdxBulkLen);

end:
  if (keyp) {
    RedisModule_CloseKey(keyp);
  }
  SearchCtx_Free(sctx);
  return REDISMODULE_OK;
}

DEBUG_COMMAND(DumpNumericIndex) {
  if (argc != 2) {
    return RedisModule_WrongArity(ctx);
  }
  GET_SEARCH_CTX(argv[0])
  RedisModuleKey *keyp = NULL;
  RedisModuleString *keyName = getFieldKeyName(sctx->spec, argv[1], INDEXFLD_T_NUMERIC);
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
    NumericRange *range = currNode->range;
    if (range) {
      IndexReader *reader = NewNumericReader(NULL, range->entries, NULL, range->minVal, range->maxVal, true);
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
  SearchCtx_Free(sctx);
  return REDISMODULE_OK;
}

DEBUG_COMMAND(DumpGeometryIndex) {
  if (argc != 2) {
    return RedisModule_WrongArity(ctx);
  }
  GET_SEARCH_CTX(argv[0])
  RedisModuleKey *keyp = NULL;
  const char *fieldName = RedisModule_StringPtrLen(argv[1], NULL);
  const FieldSpec *fs = IndexSpec_GetField(sctx->spec, fieldName, strlen(fieldName));
  if (!fs) {
    RedisModule_ReplyWithError(sctx->redisCtx, "Could not find given field in index spec");
    goto end;
  }
  GeometryIndex *idx = OpenGeometryIndex(sctx->redisCtx, sctx->spec, &keyp, fs);
  if (!idx) {
    RedisModule_ReplyWithError(sctx->redisCtx, "Could not open geoshape index");
    goto end;
  }
  const GeometryApi *api = GeometryApi_Get(idx);
  api->dump(idx, ctx);

end:
  if (keyp) {
    RedisModule_CloseKey(keyp);
  }
  SearchCtx_Free(sctx);
  return REDISMODULE_OK;
}

void InvertedIndex_DebugReply(RedisModuleCtx *ctx, InvertedIndex *idx) {
  size_t len = 0;
  RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);

  REPLY_WITH_LONG_LONG("numDocs", idx->numDocs, len);
  REPLY_WITH_LONG_LONG("lastId", idx->lastId, len);
  REPLY_WITH_LONG_LONG("size", idx->size, len);

  RedisModule_ReplyWithStringBuffer(ctx, "values", strlen("values"));
  RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
  len += 2;
  size_t len_values = 0;
  RSIndexResult *res = NULL;
  IndexReader *ir = NewNumericReader(NULL, idx, NULL ,0, 0, false);
  while (INDEXREAD_OK == IR_Read(ir, &res)) {
    REPLY_WITH_LONG_LONG("value", res->num.value, len_values);
    REPLY_WITH_LONG_LONG("docId", res->docId, len_values);
  }
  IR_Free(ir);
  RedisModule_ReplySetArrayLength(ctx, len_values);

  RedisModule_ReplySetArrayLength(ctx, len);
}

void NumericRange_DebugReply(RedisModuleCtx *ctx, NumericRange *r) {
  size_t len = 0;
  RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
  if (r) {
    REPLY_WITH_LONG_LONG("minVal", r->minVal, len);
    REPLY_WITH_LONG_LONG("maxVal", r->maxVal, len);
    REPLY_WITH_LONG_LONG("unique_sum", r->unique_sum, len);
    REPLY_WITH_LONG_LONG("invertedIndexSize", r->invertedIndexSize, len);
    REPLY_WITH_LONG_LONG("card", r->card, len);
    REPLY_WITH_LONG_LONG("cardCheck", r->cardCheck, len);
    REPLY_WITH_LONG_LONG("splitCard", r->splitCard, len);

    RedisModule_ReplyWithStringBuffer(ctx, "entries", strlen("entries"));
    InvertedIndex_DebugReply(ctx, r->entries);

    len += 2;
  }

  RedisModule_ReplySetArrayLength(ctx, len);
}

void NumericRangeNode_DebugReply(RedisModuleCtx *ctx, NumericRangeNode *n) {

  size_t len = 0;
  RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
  if (n) {
    REPLY_WITH_LONG_LONG("value", n->value, len);
    REPLY_WITH_LONG_LONG("maxDepth", n->maxDepth, len);

    RedisModule_ReplyWithStringBuffer(ctx, "range", strlen("range"));
    NumericRange_DebugReply(ctx, n->range);
    len += 2;

    RedisModule_ReplyWithStringBuffer(ctx, "left", strlen("left"));
    NumericRangeNode_DebugReply(ctx, n->left);
    len += 2;

    RedisModule_ReplyWithStringBuffer(ctx, "right", strlen("right"));
    NumericRangeNode_DebugReply(ctx, n->right);
    len += 2;
  }

  RedisModule_ReplySetArrayLength(ctx, len);
}

void NumericRangeTree_DebugReply(RedisModuleCtx *ctx, NumericRangeTree *rt) {

  size_t len = 0;
  RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
  REPLY_WITH_LONG_LONG("numRanges", rt->numRanges, len);
  REPLY_WITH_LONG_LONG("numEntries", rt->numEntries, len);
  REPLY_WITH_LONG_LONG("lastDocId", rt->lastDocId, len);
  REPLY_WITH_LONG_LONG("revisionId", rt->revisionId, len);
  REPLY_WITH_LONG_LONG("uniqueId", rt->uniqueId, len);

  RedisModule_ReplyWithStringBuffer(ctx, "root", strlen("root"));
  NumericRangeNode_DebugReply(ctx, rt->root);
  len += 2;

  RedisModule_ReplySetArrayLength(ctx, len);
}

DEBUG_COMMAND(DumpNumericIndexTree) {
  if (argc != 2) {
    return RedisModule_WrongArity(ctx);
  }
  GET_SEARCH_CTX(argv[0])
  RedisModuleKey *keyp = NULL;
  RedisModuleString *keyName = getFieldKeyName(sctx->spec, argv[1], INDEXFLD_T_NUMERIC);
  if (!keyName) {
    RedisModule_ReplyWithError(sctx->redisCtx, "Could not find given field in index spec");
    goto end;
  }
  NumericRangeTree *rt = OpenNumericIndex(sctx, keyName, &keyp);
  if (!rt) {
    RedisModule_ReplyWithError(sctx->redisCtx, "can not open numeric field");
    goto end;
  }

  NumericRangeTree_DebugReply(sctx->redisCtx, rt);

  end:
  if (keyp) {
    RedisModule_CloseKey(keyp);
  }
  SearchCtx_Free(sctx);
  return REDISMODULE_OK;
}

DEBUG_COMMAND(DumpTagIndex) {
  if (argc != 2) {
    return RedisModule_WrongArity(ctx);
  }
  GET_SEARCH_CTX(argv[0])
  RedisModuleKey *keyp = NULL;
  RedisModuleString *keyName = getFieldKeyName(sctx->spec, argv[1], INDEXFLD_T_TAG);
  if (!keyName) {
    RedisModule_ReplyWithError(sctx->redisCtx, "Could not find given field in index spec");
    goto end;
  }
  TagIndex *tagIndex = TagIndex_Open(sctx, keyName, false, &keyp);
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
  SearchCtx_Free(sctx);
  return REDISMODULE_OK;
}

DEBUG_COMMAND(DumpSuffix) {
  if (argc != 1 && argc != 2) {
    return RedisModule_WrongArity(ctx);
  }
  GET_SEARCH_CTX(argv[0]);
  if (argc == 1) { // suffix trie of global text field
    Trie *suffix = sctx->spec->suffix;
    if (!suffix) {
      RedisModule_ReplyWithError(ctx, "Index does not have suffix trie");
      goto end;
    }

    RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
    long resultSize = 0;

    // iterate trie and reply with terms
    TrieIterator *it = TrieNode_Iterate(suffix->root, NULL, NULL, NULL);
    rune *rstr;
    t_len len;
    float score;

    while (TrieIterator_Next(it, &rstr, &len, NULL, &score, NULL)) {
      size_t slen = 0;
      char *s = runesToStr(rstr, len, &slen);
      RedisModule_ReplyWithSimpleString(ctx, s);
      rm_free(s);
      ++resultSize;
    }

    TrieIterator_Free(it);

    RedisModule_ReplySetArrayLength(ctx, resultSize);

  } else { // suffix triemap of tag field
    RedisModuleString *keyName = getFieldKeyName(sctx->spec, argv[1], INDEXFLD_T_TAG);
    if (!keyName) {
      RedisModule_ReplyWithError(sctx->redisCtx, "Could not find given field in index spec");
      goto end;
    }
    const TagIndex *idx = TagIndex_Open(sctx, keyName, false, NULL);
    if (!idx) {
      RedisModule_ReplyWithError(sctx->redisCtx, "can not open tag field");
      goto end;
    }
    if (!idx->suffix) {
      RedisModule_ReplyWithError(sctx->redisCtx, "tag field does have suffix trie");
      goto end;
    }

    RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
    long resultSize = 0;

    TrieMapIterator *it = TrieMap_Iterate(idx->suffix, "", 0);
    char *str;
    tm_len_t len;
    void *value;
    while (TrieMapIterator_Next(it, &str, &len, &value)) {
      str[len] = '\0';
      RedisModule_ReplyWithSimpleString(ctx, str);
      ++resultSize;
    }

    TrieMapIterator_Free(it);

    RedisModule_ReplySetArrayLength(ctx, resultSize);
  }
end:
  SearchCtx_Free(sctx);
  return REDISMODULE_OK;
}

DEBUG_COMMAND(IdToDocId) {
  if (argc != 2) {
    return RedisModule_WrongArity(ctx);
  }
  GET_SEARCH_CTX(argv[0])
  long long id;
  if (RedisModule_StringToLongLong(argv[1], &id) != REDISMODULE_OK) {
    RedisModule_ReplyWithError(sctx->redisCtx, "bad id given");
    goto end;
  }
  const RSDocumentMetadata *doc = DocTable_Borrow(&sctx->spec->docs, id);
  if (!doc || (doc->flags & Document_Deleted)) {
    RedisModule_ReplyWithError(sctx->redisCtx, "document was removed");
  } else {
    RedisModule_ReplyWithStringBuffer(sctx->redisCtx, doc->keyPtr, strlen(doc->keyPtr));
  }
  DMD_Return(doc);
end:
  SearchCtx_Free(sctx);
  return REDISMODULE_OK;
}

DEBUG_COMMAND(DocIdToId) {
  if (argc != 2) {
    return RedisModule_WrongArity(ctx);
  }
  GET_SEARCH_CTX(argv[0])
  size_t n;
  const char *key = RedisModule_StringPtrLen(argv[1], &n);
  t_docId id = DocTable_GetId(&sctx->spec->docs, key, n);
  RedisModule_ReplyWithLongLong(sctx->redisCtx, id);
  SearchCtx_Free(sctx);
  return REDISMODULE_OK;
}

DEBUG_COMMAND(DumpPhoneticHash) {
  if (argc != 1) {
    return RedisModule_WrongArity(ctx);
  }
  size_t len;
  const char *term_c = RedisModule_StringPtrLen(argv[0], &len);

  char *primary = NULL;
  char *secondary = NULL;

  PhoneticManager_ExpandPhonetics(NULL, term_c, len, &primary, &secondary);

  RedisModule_ReplyWithArray(ctx, 2);
  RedisModule_ReplyWithStringBuffer(ctx, primary, strlen(primary));
  RedisModule_ReplyWithStringBuffer(ctx, secondary, strlen(secondary));

  rm_free(primary);
  rm_free(secondary);
  return REDISMODULE_OK;
}

static int GCForceInvokeReply(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
#define REPLY "DONE"
  RedisModule_ReplyWithStringBuffer(ctx, REPLY, strlen(REPLY));
  return REDISMODULE_OK;
}

static int GCForceInvokeReplyTimeout(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
#define ERROR_REPLY "INVOCATION FAILED"
  RedisModule_ReplyWithError(ctx, ERROR_REPLY);
  return REDISMODULE_OK;
}

DEBUG_COMMAND(GCForceInvoke) {
#define INVOKATION_TIMEOUT 30000  // gc invocation timeout ms
  if (argc < 1) {
    return RedisModule_WrongArity(ctx);
  }
  StrongRef ref = IndexSpec_LoadUnsafe(ctx, RedisModule_StringPtrLen(argv[0], NULL), 0);
  IndexSpec *sp = StrongRef_Get(ref);
  if (!sp) {
    return RedisModule_ReplyWithError(ctx, "Unknown index name");
  }

  RedisModuleBlockedClient *bc = RedisModule_BlockClient(
      ctx, GCForceInvokeReply, GCForceInvokeReplyTimeout, NULL, INVOKATION_TIMEOUT);
  GCContext_ForceInvoke(sp->gc, bc);
  return REDISMODULE_OK;
}

DEBUG_COMMAND(GCForceBGInvoke) {
  if (argc < 1) {
    return RedisModule_WrongArity(ctx);
  }
  StrongRef ref = IndexSpec_LoadUnsafe(ctx, RedisModule_StringPtrLen(argv[0], NULL), 0);
  IndexSpec *sp = StrongRef_Get(ref);
  if (!sp) {
    return RedisModule_ReplyWithError(ctx, "Unknown index name");
  }
  GCContext_ForceBGInvoke(sp->gc);
  RedisModule_ReplyWithSimpleString(ctx, "OK");
  return REDISMODULE_OK;
}

DEBUG_COMMAND(GCCleanNumeric) {

  if (argc != 2) {
    return RedisModule_WrongArity(ctx);
  }
  GET_SEARCH_CTX(argv[0])
  RedisModuleKey *keyp = NULL;
  RedisModuleString *keyName = getFieldKeyName(sctx->spec, argv[1], INDEXFLD_T_NUMERIC);
  if (!keyName) {
    RedisModule_ReplyWithError(sctx->redisCtx, "Could not find given field in index spec");
    goto end;
  }
  NumericRangeTree *rt = OpenNumericIndex(sctx, keyName, &keyp);
  if (!rt) {
    RedisModule_ReplyWithError(sctx->redisCtx, "can not open numeric field");
    goto end;
  }

  NRN_AddRv rv = NumericRangeTree_TrimEmptyLeaves(rt);

  rt->numRanges += rv.numRanges;
  rt->emptyLeaves = 0;

end:
  if (keyp) {
    RedisModule_CloseKey(keyp);
  }
  SearchCtx_Free(sctx);
  RedisModule_ReplyWithSimpleString(ctx, "OK");
  return REDISMODULE_OK;
}

DEBUG_COMMAND(ttl) {
  if (argc < 1) {
    return RedisModule_WrongArity(ctx);
  }
  IndexLoadOptions lopts = {.flags = INDEXSPEC_LOAD_NOTIMERUPDATE,
                            .name = {.cstring = RedisModule_StringPtrLen(argv[0], NULL)}};
  lopts.flags |= INDEXSPEC_LOAD_KEYLESS;

  StrongRef ref = IndexSpec_LoadUnsafeEx(ctx, &lopts);
  IndexSpec *sp = StrongRef_Get(ref);
  if (!sp) {
    return RedisModule_ReplyWithError(ctx, "Unknown index name");
  }

  if (!(sp->flags & Index_Temporary)) {
    return RedisModule_ReplyWithError(ctx, "Index is not temporary");
  }

  uint64_t remaining = 0;
  if (RedisModule_GetTimerInfo(RSDummyContext, sp->timerId, &remaining, NULL) != REDISMODULE_OK) {
    // timer was called but free operation is async so its gone be free each moment.
    // lets return 0 timeout.
    return RedisModule_ReplyWithLongLong(ctx, 0);
  }

  return RedisModule_ReplyWithLongLong(ctx, remaining / 1000);  // return the results in seconds
}

DEBUG_COMMAND(GitSha) {
#ifdef GIT_SHA
  RedisModule_ReplyWithStringBuffer(ctx, GIT_SHA, strlen(GIT_SHA));
#else
  RedisModule_ReplyWithError(ctx, "GIT SHA was not defined on compilation");
#endif
  return REDISMODULE_OK;
}

typedef struct {
  // Whether to enumerate the number of docids per entry
  int countValueEntries;

  // Whether to enumerate the *actual* document IDs in the entry
  int dumpIdEntries;

  // offset and limit for the tag entry
  unsigned offset, limit;

  // only inspect this value
  const char *prefix;
} DumpOptions;

static void seekTagIterator(TrieMapIterator *it, size_t offset) {
  char *tag;
  tm_len_t len;
  InvertedIndex *iv;

  for (size_t n = 0; n < offset; n++) {
    if (!TrieMapIterator_Next(it, &tag, &len, (void **)&iv)) {
      break;
    }
  }
}

/**
 * INFO_TAGIDX <index> <field> [OPTIONS...]
 */
DEBUG_COMMAND(InfoTagIndex) {
  if (argc < 2) {
    return RedisModule_WrongArity(ctx);
  }
  GET_SEARCH_CTX(argv[0]);
  DumpOptions options = {0};
  ACArgSpec argspecs[] = {
      {.name = "count_value_entries",
       .type = AC_ARGTYPE_BOOLFLAG,
       .target = &options.countValueEntries},
      {.name = "dump_id_entries", .type = AC_ARGTYPE_BOOLFLAG, .target = &options.dumpIdEntries},
      {.name = "prefix", .type = AC_ARGTYPE_STRING, .target = &options.prefix},
      {.name = "offset", .type = AC_ARGTYPE_UINT, .target = &options.offset},
      {.name = "limit", .type = AC_ARGTYPE_UINT, .target = &options.limit},
      {NULL}};
  RedisModuleKey *keyp = NULL;
  ArgsCursor ac = {0};
  ACArgSpec *errSpec = NULL;
  ArgsCursor_InitRString(&ac, argv + 2, argc - 2);
  int rv = AC_ParseArgSpec(&ac, argspecs, &errSpec);
  if (rv != AC_OK) {
    RedisModule_ReplyWithError(ctx, "Could not parse argument (argspec fixme)");
    goto end;
  }

  RedisModuleString *keyName = getFieldKeyName(sctx->spec, argv[1], INDEXFLD_T_TAG);
  if (!keyName) {
    RedisModule_ReplyWithError(sctx->redisCtx, "Could not find given field in index spec");
    goto end;
  }

  const TagIndex *idx = TagIndex_Open(sctx, keyName, false, &keyp);
  if (!idx) {
    RedisModule_ReplyWithError(sctx->redisCtx, "can not open tag field");
    goto end;
  }

  size_t nelem = 0;
  RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
  RedisModule_ReplyWithSimpleString(ctx, "num_values");
  RedisModule_ReplyWithLongLong(ctx, idx->values->cardinality);
  nelem += 2;

  if (options.dumpIdEntries) {
    options.countValueEntries = 1;
  }
  int shouldDescend = options.countValueEntries || options.dumpIdEntries;
  if (!shouldDescend) {
    goto reply_done;
  }

  size_t limit = options.limit ? options.limit : 0;
  TrieMapIterator *iter = TrieMap_Iterate(idx->values, "", 0);
  char *tag;
  tm_len_t len;
  InvertedIndex *iv;

  nelem += 2;
  RedisModule_ReplyWithSimpleString(ctx, "values");
  RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);

  seekTagIterator(iter, options.offset);
  size_t nvalues = 0;
  while (nvalues++ < limit && TrieMapIterator_Next(iter, &tag, &len, (void **)&iv)) {
    size_t nsubelem = 8;
    if (!options.dumpIdEntries) {
      nsubelem -= 2;
    }
    RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);

    RedisModule_ReplyWithSimpleString(ctx, "value");
    RedisModule_ReplyWithStringBuffer(ctx, tag, len);

    RedisModule_ReplyWithSimpleString(ctx, "num_entries");
    RedisModule_ReplyWithLongLong(ctx, iv->numDocs);

    RedisModule_ReplyWithSimpleString(ctx, "num_blocks");
    RedisModule_ReplyWithLongLong(ctx, iv->size);

    if (options.dumpIdEntries) {
      RedisModule_ReplyWithSimpleString(ctx, "entries");
      IndexReader *reader = NewTermIndexReader(iv, NULL, RS_FIELDMASK_ALL, NULL, 1);
      ReplyReaderResults(reader, sctx->redisCtx);
    }

    RedisModule_ReplySetArrayLength(ctx, nsubelem);
  }
  TrieMapIterator_Free(iter);
  RedisModule_ReplySetArrayLength(ctx, nvalues - 1);

reply_done:
  RedisModule_ReplySetArrayLength(ctx, nelem);

end:
  if (keyp) {
    RedisModule_CloseKey(keyp);
  }
  SearchCtx_Free(sctx);
  return REDISMODULE_OK;
}

static void replyDocFlags(const char *name, const RSDocumentMetadata *dmd, RedisModule_Reply *reply) {
  char buf[1024] = {0};
  sprintf(buf, "(0x%x):", dmd->flags);
  if (dmd->flags & Document_Deleted) {
    strcat(buf, "Deleted,");
  }
  if (dmd->flags & Document_HasPayload) {
    strcat(buf, "HasPayload,");
  }
  if (dmd->flags & Document_HasSortVector) {
    strcat(buf, "HasSortVector,");
  }
  if (dmd->flags & Document_HasOffsetVector) {
    strcat(buf, "HasOffsetVector,");
  }
  RedisModule_ReplyKV_SimpleString(reply, name, buf);
}

static void replySortVector(const char *name, const RSDocumentMetadata *dmd,
                            RedisSearchCtx *sctx, RedisModule_Reply *reply) {
  RSSortingVector *sv = dmd->sortVector;
  RedisModule_ReplyKV_Array(reply, name);
  for (size_t ii = 0; ii < sv->len; ++ii) {
    if (!sv->values[ii]) {
      continue;
    }
    RedisModule_Reply_Array(reply);
      RedisModule_ReplyKV_LongLong(reply, "index", ii);

      RedisModule_Reply_SimpleString(reply, "field");
      const FieldSpec *fs = IndexSpec_GetFieldBySortingIndex(sctx->spec, ii);
      RedisModule_Reply_Stringf(reply, "%s AS %s", fs ? fs->path : "!!!", fs ? fs->name : "???");

      RedisModule_Reply_SimpleString(reply, "value");
      RSValue_SendReply(reply, sv->values[ii], 0);
    RedisModule_Reply_ArrayEnd(reply);
  }
  RedisModule_Reply_ArrayEnd(reply);
}

/**
 * FT.DEBUG DOC_INFO <index> <doc>
 */
DEBUG_COMMAND(DocInfo) {
  if (argc < 2) {
    return RedisModule_WrongArity(ctx);
  }
  GET_SEARCH_CTX(argv[0]);

  const RSDocumentMetadata *dmd = DocTable_BorrowByKeyR(&sctx->spec->docs, argv[1]);
  if (!dmd) {
    SearchCtx_Free(sctx);
    return RedisModule_ReplyWithError(ctx, "Document not found in index");
  }

  RedisModule_Reply _reply = RedisModule_NewReply(ctx), *reply = &_reply;

  RedisModule_Reply_Map(reply);
    RedisModule_ReplyKV_LongLong(reply, "internal_id", dmd->id);
    replyDocFlags("flags", dmd, reply);
    RedisModule_ReplyKV_Double(reply, "score", dmd->score);
    RedisModule_ReplyKV_LongLong(reply, "num_tokens", dmd->len);
    RedisModule_ReplyKV_LongLong(reply, "max_freq", dmd->maxFreq);
    RedisModule_ReplyKV_LongLong(reply, "refcount", dmd->ref_count - 1); // TODO: should include the refcount of the command call?
    if (dmd->sortVector) {
      replySortVector("sortables", dmd, sctx, reply);
    }
  RedisModule_Reply_MapEnd(reply);

  RedisModule_EndReply(reply);
  DMD_Return(dmd);
  SearchCtx_Free(sctx);

  return REDISMODULE_OK;
}

static void VecSim_Reply_Info_Iterator(RedisModuleCtx *ctx, VecSimInfoIterator *infoIter) {
  RedisModule_ReplyWithArray(ctx, VecSimInfoIterator_NumberOfFields(infoIter)*2);
  while(VecSimInfoIterator_HasNextField(infoIter)) {
    VecSim_InfoField* infoField = VecSimInfoIterator_NextField(infoIter);
    RedisModule_ReplyWithSimpleString(ctx, infoField->fieldName);
    switch (infoField->fieldType) {
    case INFOFIELD_STRING:
      RedisModule_ReplyWithSimpleString(ctx, infoField->fieldValue.stringValue);
      break;
    case INFOFIELD_FLOAT64:
      RedisModule_ReplyWithDouble(ctx, infoField->fieldValue.floatingPointValue);
      break;
    case INFOFIELD_INT64:
      RedisModule_ReplyWithLongLong(ctx, infoField->fieldValue.integerValue);
      break;
    case INFOFIELD_UINT64:
      RedisModule_ReplyWithLongLong(ctx, infoField->fieldValue.uintegerValue);
      break;
    case INFOFIELD_ITERATOR:
      VecSim_Reply_Info_Iterator(ctx, infoField->fieldValue.iteratorValue);
      break;
    }
  }
}

/**
 * FT.DEBUG VECSIM_INFO <index> <field>
 */
DEBUG_COMMAND(VecsimInfo) {
  if (argc != 2) {
    return RedisModule_WrongArity(ctx);
  }
  GET_SEARCH_CTX(argv[0]);

  RedisModuleString *keyName = getFieldKeyName(sctx->spec, argv[1], INDEXFLD_T_VECTOR);
  if (!keyName) {
    SearchCtx_Free(sctx);
    return RedisModule_ReplyWithError(ctx, "Vector index not found");
  }
  // This call can't fail, since we already checked that the key exists
  // (or should exist, and this call will create it).
  VecSimIndex *vecsimIndex = OpenVectorIndex(sctx->spec, keyName);

  VecSimInfoIterator *infoIter = VecSimIndex_InfoIterator(vecsimIndex);
  // Recursively reply with the info iterator
  VecSim_Reply_Info_Iterator(ctx, infoIter);

  // Cleanup
  VecSimInfoIterator_Free(infoIter); // Free the iterator (and all its nested children)
  SearchCtx_Free(sctx);
  return REDISMODULE_OK;
}

static void RS_ThreadpoolsPrintBacktrace(RedisModule_Reply *reply) {
  GC_ThreadPoolPrintBacktrace(reply);
#ifdef MT_BUILD
  workersThreadPool_PrintBacktrace(reply);
#endif // MT_BUILD
  ConcurrentSearch_PrintBacktrace(reply);
  CleanPool_ThreadPoolPrintBacktrace(reply);
}

#define REPLY_THPOOL_BACKTRACE(thpool_name) \
    RedisModule_Reply_Map(reply); /*Threadpools dict*/ \
    thpool_name##PauseBeforeDump(); \
    thpool_name##PrintBacktrace(reply); \
    thpool_name##Resume(); \
    RedisModule_Reply_MapEnd(reply); // Thredpools dict

/**
 * FT.DEBUG DUMP_THREADPOOL_BACKTRACE thpool_name
 *
 */
DEBUG_COMMAND(DumpThreadPoolBacktrace) {
  if (argc != 1) {
    return RedisModule_WrongArity(ctx);
  }
  RedisModule_Reply _reply = RedisModule_NewReply(ctx), *reply = &_reply;

  if (!redisearch_thpool_StateLog_test_and_start()) {
    RedisModule_Reply_Error(reply, "Collecting threads' state is already in progress.");
    RedisModule_EndReply(reply);
    return REDISMODULE_OK;
  }
  const char *thpool_name = RedisModule_StringPtrLen(argv[0], NULL);

  // Initialize reply ctx

  // find the requested thpool
  if(!strcmp(thpool_name, "ALL")) {
    REPLY_THPOOL_BACKTRACE(RS_Threadpools);
  } else if(!strcmp(thpool_name, "GC")) {
    REPLY_THPOOL_BACKTRACE(GC_ThreadPool);
  } else if(!strcmp(thpool_name, "ConcurrentSearch")) {
    REPLY_THPOOL_BACKTRACE(ConcurrentSearch_);
  } else if(!strcmp(thpool_name, "CLEANSPEC")) {
    REPLY_THPOOL_BACKTRACE(CleanPool_ThreadPool);
  }
#ifdef MT_BUILD
  else if(!strcmp(thpool_name, "WORKERS")) {
    REPLY_THPOOL_BACKTRACE(workersThreadPool_);
  }
#endif // MT_BUILD
  else {
    char buff[100];
    sprintf(buff, "no such threadpool %s", thpool_name);
    RedisModule_Reply_Error(reply, buff);
  }

  // General cleanups.
  redisearch_thpool_StateLog_done();
  RedisModule_EndReply(reply);

  return REDISMODULE_OK;
}

typedef struct DebugCommandType {
  char *name;
  int (*callback)(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
} DebugCommandType;

DebugCommandType commands[] = {{"DUMP_INVIDX", DumpInvertedIndex},
                               {"DUMP_NUMIDX", DumpNumericIndex},
                               {"DUMP_NUMIDXTREE", DumpNumericIndexTree},
                               {"DUMP_TAGIDX", DumpTagIndex},
                               {"INFO_TAGIDX", InfoTagIndex},
                               {"DUMP_GEOMIDX", DumpGeometryIndex},
                               {"IDTODOCID", IdToDocId},
                               {"DOCIDTOID", DocIdToId},
                               {"DOCINFO", DocInfo},
                               {"DUMP_PHONETIC_HASH", DumpPhoneticHash},
                               {"DUMP_SUFFIX_TRIE", DumpSuffix},
                               {"DUMP_TERMS", DumpTerms},
                               {"INVIDX_SUMMARY", InvertedIndexSummary},
                               {"NUMIDX_SUMMARY", NumericIndexSummary},
                               {"GC_FORCEINVOKE", GCForceInvoke},
                               {"GC_FORCEBGINVOKE", GCForceBGInvoke},
                               {"GC_CLEAN_NUMERIC", GCCleanNumeric},
                               {"GIT_SHA", GitSha},
                               {"TTL", ttl},
                               {"VECSIM_INFO", VecsimInfo},
                               {"DUMP_THREADPOOL_BACKTRACE", DumpThreadPoolBacktrace},
                               {NULL, NULL}};

int DebugCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {

  if (argc < 2) {
    return RedisModule_WrongArity(ctx);
  }

  const char *subCommand = RedisModule_StringPtrLen(argv[1], NULL);

  if (strcasecmp(subCommand, "help") == 0) {
    size_t len = 0;
    RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
    for (DebugCommandType *c = &commands[0]; c->name != NULL; c++) {
      RedisModule_ReplyWithStringBuffer(ctx, c->name, strlen(c->name));
      ++len;
    }
    RedisModule_ReplySetArrayLength(ctx, len);
    return REDISMODULE_OK;
  }

  for (DebugCommandType *c = &commands[0]; c->name != NULL; c++) {
    if (strcasecmp(c->name, subCommand) == 0) {
      return c->callback(ctx, argv + 2, argc - 2);
    }
  }

  RedisModule_ReplyWithError(ctx, "subcommand was not found");

  return REDISMODULE_OK;
}

#if (defined(DEBUG) || defined(_DEBUG)) && !defined(NDEBUG)
#include "readies/cetara/diag/gdb.c"
#endif
