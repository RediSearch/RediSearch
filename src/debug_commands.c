/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "debug_commands.h"
#include "coord/src/debug_command_names.h"
#include "VecSim/vec_sim_debug.h"
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
#include "cursor.h"

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

#define REPLY_WITH_DOUBLE(name, val, len)                     \
  RedisModule_ReplyWithStringBuffer(ctx, name, strlen(name)); \
  RedisModule_ReplyWithDouble(ctx, val);                      \
  len += 2;

#define REPLY_WITH_STR(name, len)                        \
  RedisModule_ReplyWithStringBuffer(ctx, name, strlen(name)); \
  len += 1;

#define START_POSTPONED_LEN_ARRAY(array_name) \
  size_t len_##array_name = 0;                \
  RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN)

#define ARRAY_LEN_VAR(array_name) len_##array_name

#define END_POSTPONED_LEN_ARRAY(array_name) \
  RedisModule_ReplySetArrayLength(ctx, len_##array_name)

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

typedef struct {
  // The ratio between *num entries to the index size (in blocks)* an inverted index.
  double blocks_efficiency;
} InvertedIndexStats;

DEBUG_COMMAND(DumpTerms) {
  if (argc != 3) {
    return RedisModule_WrongArity(ctx);
  }
  GET_SEARCH_CTX(argv[2])

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

static double InvertedIndexGetEfficiency(InvertedIndex *invidx) {
  return ((double)invidx->numEntries)/(invidx->size);
}

static size_t InvertedIndexSummaryHeader(RedisModuleCtx *ctx, InvertedIndex *invidx) {
  size_t invIdxBulkLen = 0;
  REPLY_WITH_LONG_LONG("numDocs", invidx->numDocs, invIdxBulkLen);
  REPLY_WITH_LONG_LONG("numEntries", invidx->numEntries, invIdxBulkLen);
  REPLY_WITH_LONG_LONG("lastId", invidx->lastId, invIdxBulkLen);
  REPLY_WITH_LONG_LONG("flags", invidx->flags, invIdxBulkLen);
  REPLY_WITH_LONG_LONG("numberOfBlocks", invidx->size, invIdxBulkLen);
  if (invidx->flags & Index_StoreNumeric) {
    REPLY_WITH_DOUBLE("blocks_efficiency (numEntries/numberOfBlocks)", InvertedIndexGetEfficiency(invidx), invIdxBulkLen);
  }
  return invIdxBulkLen;
}

DEBUG_COMMAND(InvertedIndexSummary) {
  if (argc != 4) {
    return RedisModule_WrongArity(ctx);
  }
  GET_SEARCH_CTX(argv[2])
  RedisModuleKey *keyp = NULL;
  size_t len;
  const char *invIdxName = RedisModule_StringPtrLen(argv[3], &len);
  InvertedIndex *invidx = Redis_OpenInvertedIndexEx(sctx, invIdxName, len, 0, NULL, &keyp);
  if (!invidx) {
    RedisModule_ReplyWithError(sctx->redisCtx, "Can not find the inverted index");
    goto end;
  }

  RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
  size_t invIdxBulkLen = InvertedIndexSummaryHeader(ctx, invidx);

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
  if (argc != 4) {
    return RedisModule_WrongArity(ctx);
  }
  GET_SEARCH_CTX(argv[2])
  RedisModuleKey *keyp = NULL;
  size_t len;
  const char *invIdxName = RedisModule_StringPtrLen(argv[3], &len);
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

// FT.DEBUG NUMIDX_SUMMARY INDEX_NAME NUMERIC_FIELD_NAME
DEBUG_COMMAND(NumericIndexSummary) {
  if (argc != 4) {
    return RedisModule_WrongArity(ctx);
  }
  GET_SEARCH_CTX(argv[2])
  RedisModuleKey *keyp = NULL;
  RedisModuleString *keyName = getFieldKeyName(sctx->spec, argv[3], INDEXFLD_T_NUMERIC);
  if (!keyName) {
    RedisModule_ReplyWithError(sctx->redisCtx, "Could not find given field in index spec");
    goto end;
  }
  NumericRangeTree *rt = OpenNumericIndex(sctx, keyName, &keyp);
  if (!rt) {
    RedisModule_ReplyWithError(sctx->redisCtx, "can not open numeric field");
    goto end;
  }

  START_POSTPONED_LEN_ARRAY(numIdxSum);
  REPLY_WITH_LONG_LONG("numRanges", rt->numRanges, ARRAY_LEN_VAR(numIdxSum));
  REPLY_WITH_LONG_LONG("numEntries", rt->numEntries, ARRAY_LEN_VAR(numIdxSum));
  REPLY_WITH_LONG_LONG("lastDocId", rt->lastDocId, ARRAY_LEN_VAR(numIdxSum));
  REPLY_WITH_LONG_LONG("revisionId", rt->revisionId, ARRAY_LEN_VAR(numIdxSum));
  REPLY_WITH_LONG_LONG("emptyLeaves", rt->emptyLeaves, ARRAY_LEN_VAR(numIdxSum));
  REPLY_WITH_LONG_LONG("RootMaxDepth", rt->root->maxDepth, ARRAY_LEN_VAR(numIdxSum));
  END_POSTPONED_LEN_ARRAY(numIdxSum);

end:
  if (keyp) {
    RedisModule_CloseKey(keyp);
  }
  SearchCtx_Free(sctx);
  return REDISMODULE_OK;
}

// FT.DEBUG DUMP_NUMIDX <INDEX_NAME> <NUMERIC_FIELD_NAME> [WITH_HEADERS]
DEBUG_COMMAND(DumpNumericIndex) {
  if (argc < 4) {
    return RedisModule_WrongArity(ctx);
  }
  GET_SEARCH_CTX(argv[2])
  RedisModuleKey *keyp = NULL;
  RedisModuleString *keyName = getFieldKeyName(sctx->spec, argv[3], INDEXFLD_T_NUMERIC);
  if (!keyName) {
    RedisModule_ReplyWithError(sctx->redisCtx, "Could not find given field in index spec");
    goto end;
  }

  // It's a debug command... lets not waste time on string comparison.
  int with_headers = argc == 5 ? true : false;

  NumericRangeTree *rt = OpenNumericIndex(sctx, keyName, &keyp);
  if (!rt) {
    RedisModule_ReplyWithError(sctx->redisCtx, "can not open numeric field");
    goto end;
  }
  NumericRangeNode *currNode;
  NumericRangeTreeIterator *iter = NumericRangeTreeIterator_New(rt);
  size_t InvertedIndexNumber = 0;
  START_POSTPONED_LEN_ARRAY(numericInvertedIndex);
  while ((currNode = NumericRangeTreeIterator_Next(iter))) {
    NumericRange *range = currNode->range;
    if (range) {
      if (with_headers) {
        RedisModule_ReplyWithArray(sctx->redisCtx, 2); // start 1) Header 2)entries

        START_POSTPONED_LEN_ARRAY(numericHeader); // Header array
        InvertedIndex* invidx = range->entries;
        ARRAY_LEN_VAR(numericHeader) += InvertedIndexSummaryHeader(sctx->redisCtx, invidx);
        END_POSTPONED_LEN_ARRAY(numericHeader);
      }
      IndexReader *reader = NewNumericReader(NULL, range->entries, NULL, range->minVal, range->maxVal, true);
      ReplyReaderResults(reader, sctx->redisCtx);
      ++ARRAY_LEN_VAR(numericInvertedIndex); // end (1)Header 2)entries (header is optional)
    }
  }
  END_POSTPONED_LEN_ARRAY(numericInvertedIndex); // end InvIdx array
  NumericRangeTreeIterator_Free(iter);
end:
  if (keyp) {
    RedisModule_CloseKey(keyp);
  }
  SearchCtx_Free(sctx);
  return REDISMODULE_OK;
}

DEBUG_COMMAND(DumpGeometryIndex) {
  if (argc != 4) {
    return RedisModule_WrongArity(ctx);
  }
  GET_SEARCH_CTX(argv[2])
  RedisModuleKey *keyp = NULL;
  const char *fieldName = RedisModule_StringPtrLen(argv[3], NULL);
  const FieldSpec *fs = IndexSpec_GetField(sctx->spec, fieldName, strlen(fieldName));
  if (!fs) {
    RedisModule_ReplyWithError(sctx->redisCtx, "Could not find given field in index spec");
    goto end;
  }
  const GeometryIndex *idx = OpenGeometryIndex(sctx->redisCtx, sctx->spec, &keyp, fs);
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

// TODO: Elaborate prefixes dictionary information
// FT.DEBUG DUMP_PREFIX_TRIE
DEBUG_COMMAND(DumpPrefixTrie) {

  TrieMap *prefixes_map = SchemaPrefixes_g;

  START_POSTPONED_LEN_ARRAY(prefixesMapDump);
  REPLY_WITH_LONG_LONG("prefixes_count", prefixes_map->cardinality, ARRAY_LEN_VAR(prefixesMapDump));
  REPLY_WITH_LONG_LONG("prefixes_trie_nodes", prefixes_map->size, ARRAY_LEN_VAR(prefixesMapDump));
  END_POSTPONED_LEN_ARRAY(prefixesMapDump);

  return REDISMODULE_OK;
}

InvertedIndexStats InvertedIndex_DebugReply(RedisModuleCtx *ctx, InvertedIndex *idx) {
  InvertedIndexStats indexStats = {.blocks_efficiency = InvertedIndexGetEfficiency(idx)};
  START_POSTPONED_LEN_ARRAY(invertedIndexDump);

  REPLY_WITH_LONG_LONG("numDocs", idx->numDocs, ARRAY_LEN_VAR(invertedIndexDump));
  REPLY_WITH_LONG_LONG("numEntries", idx->numEntries, ARRAY_LEN_VAR(invertedIndexDump));
  REPLY_WITH_LONG_LONG("lastId", idx->lastId, ARRAY_LEN_VAR(invertedIndexDump));
  REPLY_WITH_LONG_LONG("size", idx->size, ARRAY_LEN_VAR(invertedIndexDump));
  REPLY_WITH_DOUBLE("blocks_efficiency (numEntries/size)", indexStats.blocks_efficiency, ARRAY_LEN_VAR(invertedIndexDump));

  REPLY_WITH_STR("values", ARRAY_LEN_VAR(invertedIndexDump));
  START_POSTPONED_LEN_ARRAY(invertedIndexValues);
  RSIndexResult *res = NULL;
  IndexReader *ir = NewNumericReader(NULL, idx, NULL ,0, 0, false);
  while (INDEXREAD_OK == IR_Read(ir, &res)) {
    REPLY_WITH_DOUBLE("value", res->num.value, ARRAY_LEN_VAR(invertedIndexValues));
    REPLY_WITH_LONG_LONG("docId", res->docId, ARRAY_LEN_VAR(invertedIndexValues));
  }
  IR_Free(ir);
  END_POSTPONED_LEN_ARRAY(invertedIndexValues);
  ARRAY_LEN_VAR(invertedIndexDump)++;

  END_POSTPONED_LEN_ARRAY(invertedIndexDump);
  return indexStats;
}

InvertedIndexStats NumericRange_DebugReply(RedisModuleCtx *ctx, NumericRange *r) {
  InvertedIndexStats ret = {0};
  START_POSTPONED_LEN_ARRAY(numericRangeInfo);
  if (r) {
    REPLY_WITH_DOUBLE("minVal", r->minVal, ARRAY_LEN_VAR(numericRangeInfo));
    REPLY_WITH_DOUBLE("maxVal", r->maxVal, ARRAY_LEN_VAR(numericRangeInfo));
    REPLY_WITH_DOUBLE("unique_sum", r->unique_sum, ARRAY_LEN_VAR(numericRangeInfo));
    REPLY_WITH_DOUBLE("invertedIndexSize [bytes]", r->invertedIndexSize, ARRAY_LEN_VAR(numericRangeInfo));
    REPLY_WITH_LONG_LONG("card", r->card, ARRAY_LEN_VAR(numericRangeInfo));
    REPLY_WITH_LONG_LONG("cardCheck", r->cardCheck, ARRAY_LEN_VAR(numericRangeInfo));
    REPLY_WITH_LONG_LONG("splitCard", r->splitCard, ARRAY_LEN_VAR(numericRangeInfo));

    REPLY_WITH_STR("entries", ARRAY_LEN_VAR(numericRangeInfo))
    ret = InvertedIndex_DebugReply(ctx, r->entries);
    ++ARRAY_LEN_VAR(numericRangeInfo);
  }

  END_POSTPONED_LEN_ARRAY(numericRangeInfo);
  return ret;
}

InvertedIndexStats NumericRangeNode_DebugReply(RedisModuleCtx *ctx, NumericRangeNode *n) {

  size_t len = 0;
  RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
  InvertedIndexStats invIdxStats = {0};
  if (n) {
    if (n->range) {
      RedisModule_ReplyWithStringBuffer(ctx, "range", strlen("range"));
      invIdxStats.blocks_efficiency += NumericRange_DebugReply(ctx, n->range).blocks_efficiency;
      len += 2;
    } else {
      REPLY_WITH_DOUBLE("value", n->value, len);
      REPLY_WITH_LONG_LONG("maxDepth", n->maxDepth, len);

      RedisModule_ReplyWithStringBuffer(ctx, "left", strlen("left"));
      invIdxStats.blocks_efficiency += NumericRangeNode_DebugReply(ctx, n->left).blocks_efficiency;
      len += 2;

      RedisModule_ReplyWithStringBuffer(ctx, "right", strlen("right"));
      invIdxStats.blocks_efficiency += NumericRangeNode_DebugReply(ctx, n->right).blocks_efficiency;
      len += 2;
    }
  }

  RedisModule_ReplySetArrayLength(ctx, len);

  return invIdxStats;
}

void NumericRangeTree_DebugReply(RedisModuleCtx *ctx, NumericRangeTree *rt) {

  size_t len = 0;
  START_POSTPONED_LEN_ARRAY(NumericTreeSum);

  REPLY_WITH_LONG_LONG("numRanges", rt->numRanges, ARRAY_LEN_VAR(NumericTreeSum));
  REPLY_WITH_LONG_LONG("numEntries", rt->numEntries, ARRAY_LEN_VAR(NumericTreeSum));
  REPLY_WITH_LONG_LONG("lastDocId", rt->lastDocId, ARRAY_LEN_VAR(NumericTreeSum));
  REPLY_WITH_LONG_LONG("revisionId", rt->revisionId, ARRAY_LEN_VAR(NumericTreeSum));
  REPLY_WITH_LONG_LONG("uniqueId", rt->uniqueId, ARRAY_LEN_VAR(NumericTreeSum));
  REPLY_WITH_LONG_LONG("emptyLeaves", rt->emptyLeaves, ARRAY_LEN_VAR(NumericTreeSum));

  REPLY_WITH_STR("root", ARRAY_LEN_VAR(NumericTreeSum));
  InvertedIndexStats invIndexStats = NumericRangeNode_DebugReply(ctx, rt->root);
  ++ARRAY_LEN_VAR(NumericTreeSum);

  REPLY_WITH_STR("Tree stats:", ARRAY_LEN_VAR(NumericTreeSum));

  START_POSTPONED_LEN_ARRAY(tree_stats);
  REPLY_WITH_DOUBLE("Average memory efficiency (numEntries/size)/numRanges", (invIndexStats.blocks_efficiency)/rt->numRanges, ARRAY_LEN_VAR(tree_stats));
  END_POSTPONED_LEN_ARRAY(tree_stats);
  ++ARRAY_LEN_VAR(NumericTreeSum);

  END_POSTPONED_LEN_ARRAY(NumericTreeSum);
}

// FT.DEBUG DUMP_NUMIDXTREE INDEX_NAME NUMERIC_FIELD_NAME
DEBUG_COMMAND(DumpNumericIndexTree) {
  if (argc != 4) {
    return RedisModule_WrongArity(ctx);
  }
  GET_SEARCH_CTX(argv[2])
  RedisModuleKey *keyp = NULL;
  RedisModuleString *keyName = getFieldKeyName(sctx->spec, argv[3], INDEXFLD_T_NUMERIC);
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
  if (argc != 4) {
    return RedisModule_WrongArity(ctx);
  }
  GET_SEARCH_CTX(argv[2])
  RedisModuleKey *keyp = NULL;
  RedisModuleString *keyName = getFieldKeyName(sctx->spec, argv[3], INDEXFLD_T_TAG);
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
  if (argc != 3 && argc != 4) {
    return RedisModule_WrongArity(ctx);
  }
  GET_SEARCH_CTX(argv[2]);
  if (argc == 3) { // suffix trie of global text field
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
      size_t slen;
      char *s = runesToStr(rstr, len, &slen);
      RedisModule_ReplyWithStringBuffer(ctx, s, slen);
      rm_free(s);
      ++resultSize;
    }

    TrieIterator_Free(it);

    RedisModule_ReplySetArrayLength(ctx, resultSize);

  } else { // suffix triemap of tag field
    RedisModuleString *keyName = getFieldKeyName(sctx->spec, argv[3], INDEXFLD_T_TAG);
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
      RedisModule_ReplyWithStringBuffer(ctx, str, len);
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
  if (argc != 4) {
    return RedisModule_WrongArity(ctx);
  }
  GET_SEARCH_CTX(argv[2])
  long long id;
  if (RedisModule_StringToLongLong(argv[3], &id) != REDISMODULE_OK) {
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
  if (argc != 4) {
    return RedisModule_WrongArity(ctx);
  }
  GET_SEARCH_CTX(argv[2])
  size_t n;
  const char *key = RedisModule_StringPtrLen(argv[3], &n);
  t_docId id = DocTable_GetId(&sctx->spec->docs, key, n);
  RedisModule_ReplyWithLongLong(sctx->redisCtx, id);
  SearchCtx_Free(sctx);
  return REDISMODULE_OK;
}

DEBUG_COMMAND(DumpPhoneticHash) {
  if (argc != 3) {
    return RedisModule_WrongArity(ctx);
  }
  size_t len;
  const char *term_c = RedisModule_StringPtrLen(argv[2], &len);

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
  return RedisModule_ReplyWithSimpleString(ctx, "DONE");
}

static int GCForceInvokeReplyTimeout(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  return RedisModule_ReplyWithError(ctx, "INVOCATION FAILED");
}

// FT.DEBUG GC_FORCEINVOKE [TIMEOUT]
DEBUG_COMMAND(GCForceInvoke) {
  if (argc < 3 || argc > 4) {
    return RedisModule_WrongArity(ctx);
  }
  long long timeout = 30000;

  if (argc == 4) {
    RedisModule_StringToLongLong(argv[3], &timeout);
  }
  StrongRef ref = IndexSpec_LoadUnsafe(ctx, RedisModule_StringPtrLen(argv[2], NULL));
  IndexSpec *sp = StrongRef_Get(ref);
  if (!sp) {
    return RedisModule_ReplyWithError(ctx, "Unknown index name");
  }

  RedisModuleBlockedClient *bc = RedisModule_BlockClient(
      ctx, GCForceInvokeReply, GCForceInvokeReplyTimeout, NULL, timeout);
  GCContext_ForceInvoke(sp->gc, bc);
  return REDISMODULE_OK;
}

DEBUG_COMMAND(GCForceBGInvoke) {
  if (argc < 3) {
    return RedisModule_WrongArity(ctx);
  }
  StrongRef ref = IndexSpec_LoadUnsafe(ctx, RedisModule_StringPtrLen(argv[2], NULL));
  IndexSpec *sp = StrongRef_Get(ref);
  if (!sp) {
    return RedisModule_ReplyWithError(ctx, "Unknown index name");
  }
  GCContext_ForceBGInvoke(sp->gc);
  RedisModule_ReplyWithSimpleString(ctx, "OK");
  return REDISMODULE_OK;
}

DEBUG_COMMAND(GCStopFutureRuns) {
  if (argc < 3) {
    return RedisModule_WrongArity(ctx);
  }
  StrongRef ref = IndexSpec_LoadUnsafe(ctx, RedisModule_StringPtrLen(argv[2], NULL));
  IndexSpec *sp = StrongRef_Get(ref);
  if (!sp) {
    return RedisModule_ReplyWithError(ctx, "Unknown index name");
  }
  // Make sure there is no pending timer
  RedisModule_StopTimer(RSDummyContext, sp->gc->timerID, NULL);
  // mark as stopped. This will prevent the GC from scheduling itself again if it was already running.
  sp->gc->timerID = 0;
  RedisModule_Log(ctx, "verbose", "Stopped GC %p periodic run for index %s", sp->gc, sp->name);
  return RedisModule_ReplyWithSimpleString(ctx, "OK");
}

DEBUG_COMMAND(GCContinueFutureRuns) {
  if (argc < 3) {
    return RedisModule_WrongArity(ctx);
  }
  StrongRef ref = IndexSpec_LoadUnsafe(ctx, RedisModule_StringPtrLen(argv[2], NULL));
  IndexSpec *sp = StrongRef_Get(ref);
  if (!sp) {
    return RedisModule_ReplyWithError(ctx, "Unknown index name");
  }
  if (sp->gc->timerID) {
    return RedisModule_ReplyWithError(ctx, "GC is already running periodically");
  }
  GCContext_StartNow(sp->gc);
  return RedisModule_ReplyWithSimpleString(ctx, "OK");
}

// Wait for all GC jobs **THAT CURRENTLY IN THE QUEUE** to finish.
// This command blocks the client and adds a job to the end of the GC queue, that will later unblock it.
DEBUG_COMMAND(GCWaitForAllJobs) {
  RedisModuleBlockedClient *bc = RedisModule_BlockClient(ctx, GCForceInvokeReply, NULL, NULL, 0);
  RedisModule_BlockedClientMeasureTimeStart(bc);
  GCContext_WaitForAllOperations(bc);
  return REDISMODULE_OK;
}

DEBUG_COMMAND(GCCleanNumeric) {

  if (argc != 4) {
    return RedisModule_WrongArity(ctx);
  }
  GET_SEARCH_CTX(argv[2])
  RedisModuleKey *keyp = NULL;
  RedisModuleString *keyName = getFieldKeyName(sctx->spec, argv[3], INDEXFLD_T_NUMERIC);
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

end:
  if (keyp) {
    RedisModule_CloseKey(keyp);
  }
  SearchCtx_Free(sctx);
  RedisModule_ReplyWithSimpleString(ctx, "OK");
  return REDISMODULE_OK;
}

DEBUG_COMMAND(ttl) {
  if (argc < 3) {
    return RedisModule_WrongArity(ctx);
  }
  IndexLoadOptions lopts = {.nameC = RedisModule_StringPtrLen(argv[2], NULL),
                            .flags = INDEXSPEC_LOAD_NOTIMERUPDATE};

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

DEBUG_COMMAND(ttlPause) {
  if (argc < 3) {
    return RedisModule_WrongArity(ctx);
  }
  IndexLoadOptions lopts = {.nameC = RedisModule_StringPtrLen(argv[2], NULL),
                            .flags = INDEXSPEC_LOAD_NOTIMERUPDATE};

  StrongRef ref = IndexSpec_LoadUnsafeEx(ctx, &lopts);
  IndexSpec *sp = StrongRef_Get(ref);
  if (!sp) {
    return RedisModule_ReplyWithError(ctx, "Unknown index name");
  }

  if (!(sp->flags & Index_Temporary)) {
    return RedisModule_ReplyWithError(ctx, "Index is not temporary");
  }

  if (!sp->isTimerSet) {
    return RedisModule_ReplyWithError(ctx, "Index does not have a timer");
  }

  WeakRef timer_ref;
  // The timed-out callback is called from the main thread and removes the index from the global
  // dictionary, so at this point we know that the timer exists.
  RedisModule_Assert(RedisModule_StopTimer(RSDummyContext, sp->timerId, (void**)&timer_ref) == REDISMODULE_OK);
  WeakRef_Release(timer_ref);
  sp->timerId = 0;
  sp->isTimerSet = false;

  return RedisModule_ReplyWithSimpleString(ctx, "OK");
}

DEBUG_COMMAND(ttlExpire) {
  if (argc < 3) {
    return RedisModule_WrongArity(ctx);
  }
  IndexLoadOptions lopts = {.nameC = RedisModule_StringPtrLen(argv[2], NULL),
                            .flags = INDEXSPEC_LOAD_NOTIMERUPDATE};

  StrongRef ref = IndexSpec_LoadUnsafeEx(ctx, &lopts);
  IndexSpec *sp = StrongRef_Get(ref);
  if (!sp) {
    return RedisModule_ReplyWithError(ctx, "Unknown index name");
  }

  if (!(sp->flags & Index_Temporary)) {
    return RedisModule_ReplyWithError(ctx, "Index is not temporary");
  }

  long long timeout = sp->timeout;
  sp->timeout = 1; // Expire in 1ms
  lopts.flags &= ~INDEXSPEC_LOAD_NOTIMERUPDATE; // Re-enable timer updates
  // We validated that the index exists and is temporary, so we know that
  // calling this function will set or reset a timer.
  IndexSpec_LoadUnsafeEx(ctx, &lopts);
  sp->timeout = timeout; // Restore the original timeout

  return RedisModule_ReplyWithSimpleString(ctx, "OK");
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
  if (argc < 4) {
    return RedisModule_WrongArity(ctx);
  }
  GET_SEARCH_CTX(argv[2]);
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
  ArgsCursor_InitRString(&ac, argv + 4, argc - 4);
  int rv = AC_ParseArgSpec(&ac, argspecs, &errSpec);
  if (rv != AC_OK) {
    RedisModule_ReplyWithError(ctx, "Could not parse argument (argspec fixme)");
    goto end;
  }

  RedisModuleString *keyName = getFieldKeyName(sctx->spec, argv[3], INDEXFLD_T_TAG);
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
  RedisModule_ReplyWithLiteral(ctx, "num_values");
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
  RedisModule_ReplyWithLiteral(ctx, "values");
  RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);

  seekTagIterator(iter, options.offset);
  size_t nvalues = 0;
  while (nvalues++ < limit && TrieMapIterator_Next(iter, &tag, &len, (void **)&iv)) {
    size_t nsubelem = 8;
    if (!options.dumpIdEntries) {
      nsubelem -= 2;
    }
    RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);

    RedisModule_ReplyWithLiteral(ctx, "value");
    RedisModule_ReplyWithStringBuffer(ctx, tag, len);

    RedisModule_ReplyWithLiteral(ctx, "num_entries");
    RedisModule_ReplyWithLongLong(ctx, iv->numDocs);

    RedisModule_ReplyWithLiteral(ctx, "num_blocks");
    RedisModule_ReplyWithLongLong(ctx, iv->size);

    if (options.dumpIdEntries) {
      RedisModule_ReplyWithLiteral(ctx, "entries");
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
  RedisModule_Reply_CString(reply, name);
  RedisModule_Reply_CString(reply, buf);
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

      RedisModule_Reply_CString(reply, "field");
      const FieldSpec *fs = IndexSpec_GetFieldBySortingIndex(sctx->spec, ii);
      RedisModule_Reply_Stringf(reply, "%s AS %s", fs ? fs->path : "!!!", fs ? fs->name : "???");

      RedisModule_Reply_CString(reply, "value");
      RSValue_SendReply(reply, sv->values[ii], 0);
    RedisModule_Reply_ArrayEnd(reply);
  }
  RedisModule_Reply_ArrayEnd(reply);
}

/**
 * FT.DEBUG DOC_INFO <index> <doc>
 */
DEBUG_COMMAND(DocInfo) {
  if (argc < 4) {
    return RedisModule_WrongArity(ctx);
  }
  GET_SEARCH_CTX(argv[2]);

  const RSDocumentMetadata *dmd = DocTable_BorrowByKeyR(&sctx->spec->docs, argv[3]);
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
    RedisModule_ReplyWithCString(ctx, infoField->fieldName);
    switch (infoField->fieldType) {
    case INFOFIELD_STRING:
      RedisModule_ReplyWithCString(ctx, infoField->fieldValue.stringValue);
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
  if (argc != 4) {
    return RedisModule_WrongArity(ctx);
  }
  GET_SEARCH_CTX(argv[2]);

  RedisModuleString *keyName = getFieldKeyName(sctx->spec, argv[3], INDEXFLD_T_VECTOR);
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

/**
 * FT.DEBUG DEL_CURSORS
 * Deletes the local cursors of the shard.
*/
DEBUG_COMMAND(DeleteCursors) {
  if (argc != 2) {
    return RedisModule_WrongArity(ctx);
  }

  RedisModule_Log(ctx, "warning", "Deleting local cursors!");
  CursorList_Empty(&g_CursorsList);
  RedisModule_Log(ctx, "warning", "Done deleting local cursors.");
  return RedisModule_ReplyWithSimpleString(ctx, "OK");
}

void replyDumpHNSW(RedisModuleCtx *ctx, VecSimIndex *index, t_docId doc_id) {
	int **neighbours_data = NULL;
	VecSimDebugCommandCode res = VecSimDebug_GetElementNeighborsInHNSWGraph(index, doc_id, &neighbours_data);
	RedisModule_Reply reply = RedisModule_NewReply(ctx);
	if (res == VecSimDebugCommandCode_LabelNotExists){
		RedisModule_Reply_Stringf(&reply, "Doc id %d doesn't contain the given field", doc_id);
		RedisModule_EndReply(&reply);
		return;
	}
	START_POSTPONED_LEN_ARRAY(response);
	REPLY_WITH_LONG_LONG("Doc id", (long long)doc_id, ARRAY_LEN_VAR(response));

	size_t level = 0;
	while (neighbours_data[level]) {
		RedisModule_ReplyWithArray(ctx, neighbours_data[level][0] + 1);
		RedisModule_Reply_Stringf(&reply, "Neighbors in level %d", level);
		for (size_t i = 0; i < neighbours_data[level][0]; i++) {
			RedisModule_ReplyWithLongLong(ctx, neighbours_data[level][i + 1]);
		}
    level++; ARRAY_LEN_VAR(response)++;
	}
	END_POSTPONED_LEN_ARRAY(response);
	VecSimDebug_ReleaseElementNeighborsInHNSWGraph(neighbours_data);
	RedisModule_EndReply(&reply);
}

DEBUG_COMMAND(dumpHNSWData) {
  if (argc < 4 || argc > 5) { // it should be 4 or 5 (allowing specifying a certain doc)
    return RedisModule_WrongArity(ctx);
  }
  GET_SEARCH_CTX(argv[2])

  RedisModuleString *keyName = getFieldKeyName(sctx->spec, argv[3], INDEXFLD_T_VECTOR);
  if (!keyName) {
    RedisModule_ReplyWithError(ctx, "Vector index not found");
	  goto cleanup;
  }
  // This call can't fail, since we already checked that the key exists
  // (or should exist, and this call will create it).
  VecSimIndex *vecsimIndex = OpenVectorIndex(sctx->spec, keyName);
  VecSimIndexBasicInfo info = VecSimIndex_BasicInfo(vecsimIndex);
  if (info.algo != VecSimAlgo_HNSWLIB) {
	  RedisModule_ReplyWithError(ctx, "Vector index is not an HNSW index");
	  goto cleanup;
  }
  if (info.isMulti) {
	  RedisModule_ReplyWithError(ctx, "Command not supported for HNSW multi-value index");
	  goto cleanup;
  }

  if (argc == 5) {  // we want the neighbors of a specific vector only
	  size_t key_len;
	  const char *key_name = RedisModule_StringPtrLen(argv[4], &key_len);
	  t_docId doc_id = DocTable_GetId(&sctx->spec->docs, key_name, key_len);
	  if (doc_id == 0) {
		  RedisModule_ReplyWithError(ctx, "The given key does not exist in index");
		  goto cleanup;
	  }
	  replyDumpHNSW(ctx, vecsimIndex, doc_id);
	  goto cleanup;
  }
  // Otherwise, dump neighbors for every document in the index.
  START_POSTPONED_LEN_ARRAY(num_docs);
  DOCTABLE_FOREACH((&sctx->spec->docs), {replyDumpHNSW(ctx, vecsimIndex, dmd->id); (ARRAY_LEN_VAR(num_docs))++;})
  END_POSTPONED_LEN_ARRAY(num_docs);

  cleanup:
  SearchCtx_Free(sctx);
  return REDISMODULE_OK;
}

#ifdef MT_BUILD
/**
 * FT.DEBUG WORKERS [PAUSE / RESUME / DRAIN / STATS / N_THREADS]
 */
DEBUG_COMMAND(WorkerThreadsSwitch) {
  if (argc != 3) {
    return RedisModule_WrongArity(ctx);
  }
  const char* op = RedisModule_StringPtrLen(argv[2], NULL);
  if (!strcasecmp(op, "pause")) {
    if (workersThreadPool_pause() != REDISMODULE_OK) {
      return RedisModule_ReplyWithError(ctx, "Operation failed: workers thread pool doesn't exists"
                                      " or is not running");
    }
  } else if (!strcasecmp(op, "resume")) {
    if (workersThreadPool_resume() != REDISMODULE_OK) {
      return RedisModule_ReplyWithError(ctx, "Operation failed: workers thread pool doesn't exists"
                                        " or is already running");
    }
  } else if (!strcasecmp(op, "drain")) {
    if (workerThreadPool_isPaused()) {
      return RedisModule_ReplyWithError(ctx, "Operation failed: workers thread pool is not running");
    }
    workersThreadPool_Drain(RSDummyContext, 0);
    // After we drained the thread pool and there are no more jobs in the queue, we wait until all
    // threads are idle, so we can be sure that all jobs were executed.
    workersThreadPool_wait();
  } else if (!strcasecmp(op, "stats")) {
    thpool_stats stats = workersThreadPool_getStats();
    START_POSTPONED_LEN_ARRAY(num_stats_fields);
    REPLY_WITH_LONG_LONG("totalJobsDone", stats.total_jobs_done, ARRAY_LEN_VAR(num_stats_fields));
    REPLY_WITH_LONG_LONG("totalPendingJobs", stats.total_pending_jobs, ARRAY_LEN_VAR(num_stats_fields));
    REPLY_WITH_LONG_LONG("highPriorityPendingJobs", stats.high_priority_pending_jobs, ARRAY_LEN_VAR(num_stats_fields));
    REPLY_WITH_LONG_LONG("lowPriorityPendingJobs", stats.low_priority_pending_jobs, ARRAY_LEN_VAR(num_stats_fields));
    REPLY_WITH_LONG_LONG("numThreadsAlive", stats.num_threads_alive, ARRAY_LEN_VAR(num_stats_fields));
    END_POSTPONED_LEN_ARRAY(num_stats_fields);
    return REDISMODULE_OK;
  }  else if (!strcasecmp(op, "n_threads")) {
    return RedisModule_ReplyWithLongLong(ctx, workersThreadPool_NumThreads());
  } else {
    return RedisModule_ReplyWithError(ctx, "Invalid argument for 'WORKERS' subcommand");
  }
  return RedisModule_ReplyWithSimpleString(ctx, "OK");
}
#endif

DebugCommandType commands[] = {{"DUMP_INVIDX", DumpInvertedIndex}, // Print all the inverted index entries.
                               {"DUMP_NUMIDX", DumpNumericIndex}, // Print all the headers (optional) + entries of the numeric tree.
                               {"DUMP_NUMIDXTREE", DumpNumericIndexTree}, // Print tree general info, all leaves + nodes + stats
                               {"DUMP_TAGIDX", DumpTagIndex},
                               {"INFO_TAGIDX", InfoTagIndex},
                               {"DUMP_GEOMIDX", DumpGeometryIndex},
                               {"DUMP_PREFIX_TRIE", DumpPrefixTrie},
                               {"IDTODOCID", IdToDocId},
                               {"DOCIDTOID", DocIdToId},
                               {"DOCINFO", DocInfo},
                               {"DUMP_PHONETIC_HASH", DumpPhoneticHash},
                               {"DUMP_SUFFIX_TRIE", DumpSuffix},
                               {"DUMP_TERMS", DumpTerms},
                               {"INVIDX_SUMMARY", InvertedIndexSummary}, // Print info about an inverted index and each of its blocks.
                               {"NUMIDX_SUMMARY", NumericIndexSummary}, // Quick summary of the numeric index
                               {"GC_FORCEINVOKE", GCForceInvoke},
                               {"GC_FORCEBGINVOKE", GCForceBGInvoke},
                               {"GC_CLEAN_NUMERIC", GCCleanNumeric},
                               {"GC_STOP_SCHEDULE", GCStopFutureRuns},
                               {"GC_CONTINUE_SCHEDULE", GCContinueFutureRuns},
                               {"GC_WAIT_FOR_JOBS", GCWaitForAllJobs},
                               {"GIT_SHA", GitSha},
                               {"TTL", ttl},
                               {"TTL_PAUSE", ttlPause},
                               {"TTL_EXPIRE", ttlExpire},
                               {"VECSIM_INFO", VecsimInfo},
                               {"DELETE_LOCAL_CURSORS", DeleteCursors},
                               {"DUMP_HNSW", dumpHNSWData},
#ifdef MT_BUILD
                               {"WORKERS", WorkerThreadsSwitch},
#endif
                               {NULL, NULL}};

int DebugHelpCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
  size_t len = 0;
  for (DebugCommandType *c = &commands[0]; c->name != NULL; c++) {
    RedisModule_ReplyWithCString(ctx, c->name);
    ++len;
  }
#ifdef RS_COORDINATOR
  for (size_t i = 0; coordCommandsNames[i]; i++) {
    RedisModule_ReplyWithCString(ctx, coordCommandsNames[i]);
    ++len;
  }
#endif
  RedisModule_ReplySetArrayLength(ctx, len);
  return REDISMODULE_OK;
}

int RegisterDebugCommands(RedisModuleCommand *debugCommand) {
  for (DebugCommandType *c = &commands[0]; c->name != NULL; c++) {
    int rc = RedisModule_CreateSubcommand(debugCommand, c->name, c->callback, RS_DEBUG_FLAGS);
    if (rc != REDISMODULE_OK) return rc;
  }
  return RedisModule_CreateSubcommand(debugCommand, "HELP", DebugHelpCommand, RS_DEBUG_FLAGS);
}

#if (defined(DEBUG) || defined(_DEBUG)) && !defined(NDEBUG)
#include "readies/cetara/diag/gdb.c"
#endif
