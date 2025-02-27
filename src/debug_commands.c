/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "debug_commands.h"
#include "coord/debug_command_names.h"
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


DebugCTX globalDebugCtx = {0};

void validateDebugMode(DebugCTX *debugCtx) {
  // Debug mode is enabled if any of its field is non-default
  // Should be called after each debug command that changes the debugCtx
  debugCtx->debugMode =
    (debugCtx->bgIndexing.maxDocsTBscanned > 0) ||
    (debugCtx->bgIndexing.maxDocsTBscannedPause > 0) ||
    (debugCtx->bgIndexing.pauseBeforeScan);

}


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
  size_t len;
  const char *fieldName = RedisModule_StringPtrLen(fieldNameRS, &len);
  const FieldSpec *fieldSpec = IndexSpec_GetFieldWithLength(spec, fieldName, len);
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
  if (!debugCommandsEnabled(ctx)) {
    return RedisModule_ReplyWithError(ctx, NODEBUG_ERR);
  }
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
  if (!debugCommandsEnabled(ctx)) {
    return RedisModule_ReplyWithError(ctx, NODEBUG_ERR);
  }
  if (argc != 4) {
    return RedisModule_WrongArity(ctx);
  }
  GET_SEARCH_CTX(argv[2])
  size_t len;
  const char *invIdxName = RedisModule_StringPtrLen(argv[3], &len);
  InvertedIndex *invidx = Redis_OpenInvertedIndex(sctx, invIdxName, len, 0, NULL);
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
  SearchCtx_Free(sctx);
  return REDISMODULE_OK;
}

DEBUG_COMMAND(DumpInvertedIndex) {
  if (!debugCommandsEnabled(ctx)) {
    return RedisModule_ReplyWithError(ctx, NODEBUG_ERR);
  }
  if (argc != 4) {
    return RedisModule_WrongArity(ctx);
  }
  GET_SEARCH_CTX(argv[2])
  size_t len;
  const char *invIdxName = RedisModule_StringPtrLen(argv[3], &len);
  InvertedIndex *invidx = Redis_OpenInvertedIndex(sctx, invIdxName, len, 0, NULL);
  if (!invidx) {
    RedisModule_ReplyWithError(sctx->redisCtx, "Can not find the inverted index");
    goto end;
  }
  IndexReader *reader = NewTermIndexReader(invidx);
  ReplyReaderResults(reader, sctx->redisCtx);

end:
  SearchCtx_Free(sctx);
  return REDISMODULE_OK;
}

// FT.DEBUG NUMIDX_SUMMARY INDEX_NAME NUMERIC_FIELD_NAME
DEBUG_COMMAND(NumericIndexSummary) {
  if (!debugCommandsEnabled(ctx)) {
    return RedisModule_ReplyWithError(ctx, NODEBUG_ERR);
  }
  if (argc != 4) {
    return RedisModule_WrongArity(ctx);
  }
  GET_SEARCH_CTX(argv[2])
  RedisModuleString *keyName = getFieldKeyName(sctx->spec, argv[3], INDEXFLD_T_NUMERIC);
  if (!keyName) {
    RedisModule_ReplyWithError(sctx->redisCtx, "Could not find given field in index spec");
    goto end;
  }
  NumericRangeTree rt_info = {0};
  int root_max_depth = 0;

  NumericRangeTree *rt = openNumericKeysDict(sctx->spec, keyName, DONT_CREATE_INDEX);
  // If we failed to open the numeric index, it was not initialized yet.
  // Else, we copy the data to a local variable.
  if (rt) {
    rt_info = *rt;
    root_max_depth = rt->root->maxDepth;
  }

  START_POSTPONED_LEN_ARRAY(numIdxSum);
  REPLY_WITH_LONG_LONG("numRanges", rt_info.numRanges, ARRAY_LEN_VAR(numIdxSum));
  REPLY_WITH_LONG_LONG("numLeaves", rt_info.numLeaves, ARRAY_LEN_VAR(numIdxSum));
  REPLY_WITH_LONG_LONG("numEntries", rt_info.numEntries, ARRAY_LEN_VAR(numIdxSum));
  REPLY_WITH_LONG_LONG("lastDocId", rt_info.lastDocId, ARRAY_LEN_VAR(numIdxSum));
  REPLY_WITH_LONG_LONG("revisionId", rt_info.revisionId, ARRAY_LEN_VAR(numIdxSum));
  REPLY_WITH_LONG_LONG("emptyLeaves", rt_info.emptyLeaves, ARRAY_LEN_VAR(numIdxSum));
  REPLY_WITH_LONG_LONG("RootMaxDepth", root_max_depth, ARRAY_LEN_VAR(numIdxSum));
  REPLY_WITH_LONG_LONG("MemoryUsage", rt ? NumericIndexType_MemUsage(rt) : 0, ARRAY_LEN_VAR(numIdxSum));
  END_POSTPONED_LEN_ARRAY(numIdxSum);

end:
  SearchCtx_Free(sctx);
  return REDISMODULE_OK;
}

// FT.DEBUG DUMP_NUMIDX <INDEX_NAME> <NUMERIC_FIELD_NAME> [WITH_HEADERS]
DEBUG_COMMAND(DumpNumericIndex) {
  if (!debugCommandsEnabled(ctx)) {
    return RedisModule_ReplyWithError(ctx, NODEBUG_ERR);
  }
  if (argc < 4) {
    return RedisModule_WrongArity(ctx);
  }
  GET_SEARCH_CTX(argv[2])
  RedisModuleString *keyName = getFieldKeyName(sctx->spec, argv[3], INDEXFLD_T_NUMERIC);
  if (!keyName) {
    RedisModule_ReplyWithError(sctx->redisCtx, "Could not find given field in index spec");
    goto end;
  }

  // It's a debug command... lets not waste time on string comparison.
  int with_headers = argc == 5 ? true : false;

  NumericRangeTree *rt = openNumericKeysDict(sctx->spec, keyName, DONT_CREATE_INDEX);
  // If we failed to open the numeric index, it was not initialized yet.
  if (!rt) {
    RedisModule_ReplyWithEmptyArray(sctx->redisCtx);
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
      FieldFilterContext fieldCtx = {.field.isFieldMask = false, .field.value.index = RS_INVALID_FIELD_INDEX, .predicate = FIELD_EXPIRATION_DEFAULT};
      IndexReader *reader = NewNumericReader(NULL, range->entries, NULL, range->minVal, range->maxVal, true, &fieldCtx);
      ReplyReaderResults(reader, sctx->redisCtx);
      ++ARRAY_LEN_VAR(numericInvertedIndex); // end (1)Header 2)entries (header is optional)
    }
  }
  END_POSTPONED_LEN_ARRAY(numericInvertedIndex); // end InvIdx array
  NumericRangeTreeIterator_Free(iter);
end:
  SearchCtx_Free(sctx);
  return REDISMODULE_OK;
}

DEBUG_COMMAND(DumpGeometryIndex) {
  if (!debugCommandsEnabled(ctx)) {
    return RedisModule_ReplyWithError(ctx, NODEBUG_ERR);
  }
  if (argc != 4) {
    return RedisModule_WrongArity(ctx);
  }
  GET_SEARCH_CTX(argv[2])
  size_t len;
  const char *fieldName = RedisModule_StringPtrLen(argv[3], &len);
  const FieldSpec *fs = IndexSpec_GetFieldWithLength(sctx->spec, fieldName, len);
  if (!fs) {
    RedisModule_ReplyWithError(sctx->redisCtx, "Could not find given field in index spec");
    goto end;
  }

  // TODO: use DONT_CREATE_INDEX and imitate the reply struct of an empty index.
  const GeometryIndex *idx = OpenGeometryIndex(sctx->spec, fs, CREATE_INDEX);
  if (!idx) {
    RedisModule_ReplyWithError(sctx->redisCtx, "Could not open geoshape index");
    goto end;
  }
  const GeometryApi *api = GeometryApi_Get(idx);
  api->dump(idx, ctx);

end:
  SearchCtx_Free(sctx);
  return REDISMODULE_OK;
}

// TODO: Elaborate prefixes dictionary information
// FT.DEBUG DUMP_PREFIX_TRIE
DEBUG_COMMAND(DumpPrefixTrie) {
  if (!debugCommandsEnabled(ctx)) {
    return RedisModule_ReplyWithError(ctx, NODEBUG_ERR);
  }

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
  IndexReader *ir = NewMinimalNumericReader(idx, false);
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
    REPLY_WITH_DOUBLE("invertedIndexSize [bytes]", r->invertedIndexSize, ARRAY_LEN_VAR(numericRangeInfo));
    REPLY_WITH_LONG_LONG("card", NumericRange_GetCardinality(r), ARRAY_LEN_VAR(numericRangeInfo));

    REPLY_WITH_STR("entries", ARRAY_LEN_VAR(numericRangeInfo))
    ret = InvertedIndex_DebugReply(ctx, r->entries);
    ++ARRAY_LEN_VAR(numericRangeInfo);
  }

  END_POSTPONED_LEN_ARRAY(numericRangeInfo);
  return ret;
}

/**
 * It is safe to use @param n equals to NULL.
 */
static InvertedIndexStats NumericRangeNode_DebugReply(RedisModuleCtx *ctx, NumericRangeNode *n, bool minimal) {
  InvertedIndexStats invIdxStats = {0};
  if (!n) {
    RedisModule_ReplyWithMap(ctx, 0);
    return invIdxStats;
  }
  size_t len = 0;
  RedisModule_ReplyWithMap(ctx, REDISMODULE_POSTPONED_LEN);

  if (n->range) {
    RedisModule_ReplyWithLiteral(ctx, "range");
    if (minimal) {
      RedisModule_ReplyWithEmptyArray(ctx);
    } else {
      invIdxStats.blocks_efficiency += NumericRange_DebugReply(ctx, n->range).blocks_efficiency;
    }
    len++;
  }
  if (!NumericRangeNode_IsLeaf(n)) {
    RedisModule_ReplyWithLiteral(ctx, "value");
    RedisModule_ReplyWithDouble(ctx, n->value);
    len++;
    RedisModule_ReplyWithLiteral(ctx, "maxDepth");
    RedisModule_ReplyWithLongLong(ctx, n->maxDepth);
    len++;

    RedisModule_ReplyWithLiteral(ctx, "left");
    invIdxStats.blocks_efficiency += NumericRangeNode_DebugReply(ctx, n->left, minimal).blocks_efficiency;
    len++;

    RedisModule_ReplyWithLiteral(ctx, "right");
    invIdxStats.blocks_efficiency += NumericRangeNode_DebugReply(ctx, n->right, minimal).blocks_efficiency;
    len++;
  }

  RedisModule_ReplySetMapLength(ctx, len);

  return invIdxStats;
}

/**
 * It is safe to use @param rt with all fields initialized to 0, including a NULL root.
 */
void NumericRangeTree_DebugReply(RedisModuleCtx *ctx, NumericRangeTree *rt, bool minimal) {

  RedisModule_ReplyWithMap(ctx, 8);

  RedisModule_ReplyWithLiteral(ctx, "numRanges");
  RedisModule_ReplyWithLongLong(ctx, rt->numRanges);

  RedisModule_ReplyWithLiteral(ctx, "numEntries");
  RedisModule_ReplyWithLongLong(ctx, rt->numEntries);

  RedisModule_ReplyWithLiteral(ctx, "lastDocId");
  RedisModule_ReplyWithLongLong(ctx, rt->lastDocId);

  RedisModule_ReplyWithLiteral(ctx, "revisionId");
  RedisModule_ReplyWithLongLong(ctx, rt->revisionId);

  RedisModule_ReplyWithLiteral(ctx, "uniqueId");
  RedisModule_ReplyWithLongLong(ctx, rt->uniqueId);

  RedisModule_ReplyWithLiteral(ctx, "emptyLeaves");
  RedisModule_ReplyWithLongLong(ctx, rt->emptyLeaves);

  RedisModule_ReplyWithLiteral(ctx, "root");
  InvertedIndexStats invIndexStats = NumericRangeNode_DebugReply(ctx, rt->root, minimal);

  RedisModule_ReplyWithLiteral(ctx, "Tree stats");
  RedisModule_ReplyWithMap(ctx, 1);
  RedisModule_ReplyWithLiteral(ctx, "Average memory efficiency (numEntries/size)/numRanges");
  RedisModule_ReplyWithDouble(ctx, (invIndexStats.blocks_efficiency)/rt->numRanges);

}

// FT.DEBUG DUMP_NUMIDXTREE INDEX_NAME NUMERIC_FIELD_NAME [MINIMAL]
DEBUG_COMMAND(DumpNumericIndexTree) {
  if (!debugCommandsEnabled(ctx)) {
    return RedisModule_ReplyWithError(ctx, NODEBUG_ERR);
  }
  if (argc < 4 || argc > 5) {
    return RedisModule_WrongArity(ctx);
  }
  GET_SEARCH_CTX(argv[2])
  RedisModuleString *keyName = getFieldKeyName(sctx->spec, argv[3], INDEXFLD_T_NUMERIC);
  if (!keyName) {
    RedisModule_ReplyWithError(sctx->redisCtx, "Could not find given field in index spec");
    goto end;
  }
  NumericRangeTree dummy_rt = {0};
  NumericRangeTree *rt = openNumericKeysDict(sctx->spec, keyName, DONT_CREATE_INDEX);
  // If we failed to open the numeric index, it was not initialized yet,
  // reply as if the tree is empty.
  if (!rt) {
    rt = &dummy_rt;
  }
  bool minimal = argc > 4 && !strcasecmp(RedisModule_StringPtrLen(argv[4], NULL), "minimal");

  NumericRangeTree_DebugReply(sctx->redisCtx, rt, minimal);

  end:
  SearchCtx_Free(sctx);
  return REDISMODULE_OK;
}

// FT.DEBUG SPEC_INVIDXES_INFO INDEX_NAME
DEBUG_COMMAND(SpecInvertedIndexesInfo) {
  if (!debugCommandsEnabled(ctx)) {
    return RedisModule_ReplyWithError(ctx, NODEBUG_ERR);
  }
  if (argc != 3) {
    return RedisModule_WrongArity(ctx);
  }
  GET_SEARCH_CTX(argv[2])
  START_POSTPONED_LEN_ARRAY(specInvertedIndexesInfo);
	REPLY_WITH_LONG_LONG("inverted_indexes_dict_size", dictSize(sctx->spec->keysDict), ARRAY_LEN_VAR(specInvertedIndexesInfo));
	REPLY_WITH_LONG_LONG("inverted_indexes_memory", sctx->spec->stats.invertedSize, ARRAY_LEN_VAR(specInvertedIndexesInfo));
  END_POSTPONED_LEN_ARRAY(specInvertedIndexesInfo);

  SearchCtx_Free(sctx);
  return REDISMODULE_OK;
}

DEBUG_COMMAND(DumpTagIndex) {
  if (!debugCommandsEnabled(ctx)) {
    return RedisModule_ReplyWithError(ctx, NODEBUG_ERR);
  }
  if (argc != 4) {
    return RedisModule_WrongArity(ctx);
  }
  GET_SEARCH_CTX(argv[2])
  RedisModuleString *keyName = getFieldKeyName(sctx->spec, argv[3], INDEXFLD_T_TAG);
  if (!keyName) {
    RedisModule_ReplyWithError(sctx->redisCtx, "Could not find given field in index spec");
    goto end;
  }
  const TagIndex *tagIndex = TagIndex_Open(sctx, keyName, DONT_CREATE_INDEX);

  // Field was not initialized yet
  if (!tagIndex) {
    RedisModule_ReplyWithEmptyArray(sctx->redisCtx);
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
    IndexReader *reader = NewTermIndexReader(iv);
    ReplyReaderResults(reader, sctx->redisCtx);
    ++resultSize;
  }
  RedisModule_ReplySetArrayLength(sctx->redisCtx, resultSize);
  TrieMapIterator_Free(iter);

end:
  SearchCtx_Free(sctx);
  return REDISMODULE_OK;
}

DEBUG_COMMAND(DumpSuffix) {
  if (!debugCommandsEnabled(ctx)) {
    return RedisModule_ReplyWithError(ctx, NODEBUG_ERR);
  }
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
    const TagIndex *idx = TagIndex_Open(sctx, keyName, DONT_CREATE_INDEX);

    // Field was not initialized yet
    if (!idx) {
      RedisModule_ReplyWithEmptyArray(sctx->redisCtx);
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
  if (!debugCommandsEnabled(ctx)) {
    return RedisModule_ReplyWithError(ctx, NODEBUG_ERR);
  }
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
  if (!debugCommandsEnabled(ctx)) {
    return RedisModule_ReplyWithError(ctx, NODEBUG_ERR);
  }
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
  if (!debugCommandsEnabled(ctx)) {
    return RedisModule_ReplyWithError(ctx, NODEBUG_ERR);
  }
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
  if (!debugCommandsEnabled(ctx)) {
    return RedisModule_ReplyWithError(ctx, NODEBUG_ERR);
  }
  return RedisModule_ReplyWithSimpleString(ctx, "DONE");
}

static int GCForceInvokeReplyTimeout(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  if (!debugCommandsEnabled(ctx)) {
    return RedisModule_ReplyWithError(ctx, NODEBUG_ERR);
  }
  return RedisModule_ReplyWithError(ctx, "INVOCATION FAILED");
}

// FT.DEBUG GC_FORCEINVOKE [TIMEOUT]
DEBUG_COMMAND(GCForceInvoke) {
  if (!debugCommandsEnabled(ctx)) {
    return RedisModule_ReplyWithError(ctx, NODEBUG_ERR);
  }
  if (argc < 3 || argc > 4) {
    return RedisModule_WrongArity(ctx);
  }
  long long timeout = 30000;

  if (argc == 4) {
    RedisModule_StringToLongLong(argv[3], &timeout);
  }
  StrongRef ref = IndexSpec_LoadUnsafe(RedisModule_StringPtrLen(argv[2], NULL));
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
  if (!debugCommandsEnabled(ctx)) {
    return RedisModule_ReplyWithError(ctx, NODEBUG_ERR);
  }
  if (argc < 3) {
    return RedisModule_WrongArity(ctx);
  }
  StrongRef ref = IndexSpec_LoadUnsafe(RedisModule_StringPtrLen(argv[2], NULL));
  IndexSpec *sp = StrongRef_Get(ref);
  if (!sp) {
    return RedisModule_ReplyWithError(ctx, "Unknown index name");
  }
  GCContext_ForceBGInvoke(sp->gc);
  RedisModule_ReplyWithSimpleString(ctx, "OK");
  return REDISMODULE_OK;
}

DEBUG_COMMAND(GCStopFutureRuns) {
  if (!debugCommandsEnabled(ctx)) {
    return RedisModule_ReplyWithError(ctx, NODEBUG_ERR);
  }
  if (argc < 3) {
    return RedisModule_WrongArity(ctx);
  }
  StrongRef ref = IndexSpec_LoadUnsafe(RedisModule_StringPtrLen(argv[2], NULL));
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
  if (!debugCommandsEnabled(ctx)) {
    return RedisModule_ReplyWithError(ctx, NODEBUG_ERR);
  }
  if (argc < 3) {
    return RedisModule_WrongArity(ctx);
  }
  StrongRef ref = IndexSpec_LoadUnsafe(RedisModule_StringPtrLen(argv[2], NULL));
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
  if (!debugCommandsEnabled(ctx)) {
    return RedisModule_ReplyWithError(ctx, NODEBUG_ERR);
  }
  RedisModuleBlockedClient *bc = RedisModule_BlockClient(ctx, GCForceInvokeReply, NULL, NULL, 0);
  RedisModule_BlockedClientMeasureTimeStart(bc);
  GCContext_WaitForAllOperations(bc);
  return REDISMODULE_OK;
}

// GC_CLEAN_NUMERIC INDEX_NAME NUMERIC_FIELD_NAME
DEBUG_COMMAND(GCCleanNumeric) {
  if (!debugCommandsEnabled(ctx)) {
    return RedisModule_ReplyWithError(ctx, NODEBUG_ERR);
  }

  if (argc != 4) {
    return RedisModule_WrongArity(ctx);
  }
  GET_SEARCH_CTX(argv[2])
  RedisModuleString *keyName = getFieldKeyName(sctx->spec, argv[3], INDEXFLD_T_NUMERIC);
  if (!keyName) {
    RedisModule_ReplyWithError(sctx->redisCtx, "Could not find given field in index spec");
    goto end;
  }
  NumericRangeTree *rt = openNumericKeysDict(sctx->spec, keyName, DONT_CREATE_INDEX);
  if (!rt) {
    goto end;
  }

  NRN_AddRv rv = NumericRangeTree_TrimEmptyLeaves(rt);

end:
  SearchCtx_Free(sctx);
  RedisModule_ReplyWithSimpleString(ctx, "OK");
  return REDISMODULE_OK;
}

DEBUG_COMMAND(ttl) {
  if (!debugCommandsEnabled(ctx)) {
    return RedisModule_ReplyWithError(ctx, NODEBUG_ERR);
  }
  if (argc < 3) {
    return RedisModule_WrongArity(ctx);
  }
  IndexLoadOptions lopts = {.nameC = RedisModule_StringPtrLen(argv[2], NULL),
                            .flags = INDEXSPEC_LOAD_NOTIMERUPDATE};

  StrongRef ref = IndexSpec_LoadUnsafeEx(&lopts);
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
  if (!debugCommandsEnabled(ctx)) {
    return RedisModule_ReplyWithError(ctx, NODEBUG_ERR);
  }
  if (argc < 3) {
    return RedisModule_WrongArity(ctx);
  }
  IndexLoadOptions lopts = {.nameC = RedisModule_StringPtrLen(argv[2], NULL),
                            .flags = INDEXSPEC_LOAD_NOTIMERUPDATE};

  StrongRef ref = IndexSpec_LoadUnsafeEx(&lopts);
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
  int rc = RedisModule_StopTimer(RSDummyContext, sp->timerId, (void**)&timer_ref);
  RS_ASSERT(rc == REDISMODULE_OK);
  WeakRef_Release(timer_ref);
  sp->timerId = 0;
  sp->isTimerSet = false;

  return RedisModule_ReplyWithSimpleString(ctx, "OK");
}

DEBUG_COMMAND(ttlExpire) {
  if (!debugCommandsEnabled(ctx)) {
    return RedisModule_ReplyWithError(ctx, NODEBUG_ERR);
  }
  if (argc < 3) {
    return RedisModule_WrongArity(ctx);
  }
  IndexLoadOptions lopts = {.nameC = RedisModule_StringPtrLen(argv[2], NULL),
                            .flags = INDEXSPEC_LOAD_NOTIMERUPDATE};

  StrongRef ref = IndexSpec_LoadUnsafeEx(&lopts);
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
  IndexSpec_LoadUnsafeEx(&lopts);
  sp->timeout = timeout; // Restore the original timeout

  return RedisModule_ReplyWithSimpleString(ctx, "OK");
}

typedef struct {
  int docs;
  int notDocs;
  int fields;
  int notFields;
} MonitorExpirationOptions;

DEBUG_COMMAND(setMonitorExpiration) {
  if (!debugCommandsEnabled(ctx)) {
    return RedisModule_ReplyWithError(ctx, NODEBUG_ERR);
  }
  if (argc < 3) {
    return RedisModule_WrongArity(ctx);
  }

  IndexLoadOptions lopts = {.nameC = RedisModule_StringPtrLen(argv[2], NULL),
                            .flags = INDEXSPEC_LOAD_NOTIMERUPDATE};

  StrongRef ref = IndexSpec_LoadUnsafeEx(&lopts);
  IndexSpec *sp = StrongRef_Get(ref);
  if (!sp) {
    return RedisModule_ReplyWithError(ctx, "Unknown index name");
  }

  MonitorExpirationOptions options = {0};
  ACArgSpec argspecs[] = {
      {.name = "not-documents", .type = AC_ARGTYPE_BOOLFLAG, .target = &options.notDocs},
      {.name = "documents", .type = AC_ARGTYPE_BOOLFLAG, .target = &options.docs},
      {.name = "fields", .type = AC_ARGTYPE_BOOLFLAG, .target = &options.fields},
      {.name = "not-fields", .type = AC_ARGTYPE_BOOLFLAG, .target = &options.notFields},
      {NULL}};
  RedisModuleKey *keyp = NULL;
  ArgsCursor ac = {0};
  ACArgSpec *errSpec = NULL;
  ArgsCursor_InitRString(&ac, argv + 3, argc - 3);
  int rv = AC_ParseArgSpec(&ac, argspecs, &errSpec);
  if (rv != AC_OK) {
    return RedisModule_ReplyWithError(ctx, "Could not parse argument (argspec fixme)");
  }
  if (options.docs && options.notDocs) {
    return RedisModule_ReplyWithError(ctx, "Can't set both documents and not-documents");
  }
  if (options.fields && options.notFields) {
    return RedisModule_ReplyWithError(ctx, "Can't set both fields and not-fields");
  }

  if (options.docs || options.notDocs) {
    sp->monitorDocumentExpiration = options.docs && !options.notDocs;
  }
  if (options.fields || options.notFields) {
    sp->monitorFieldExpiration = options.fields && !options.notFields && RedisModule_HashFieldMinExpire != NULL;
  }
  RedisModule_ReplyWithSimpleString(ctx, "OK");
  return REDISMODULE_OK;
}

DEBUG_COMMAND(GitSha) {
  if (!debugCommandsEnabled(ctx)) {
    return RedisModule_ReplyWithError(ctx, NODEBUG_ERR);
  }
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
  if (!debugCommandsEnabled(ctx)) {
    return RedisModule_ReplyWithError(ctx, NODEBUG_ERR);
  }
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

  const TagIndex *idx = TagIndex_Open(sctx, keyName, DONT_CREATE_INDEX);

  // Field was not initialized yet
  if (!idx) {
    RedisModule_ReplyWithEmptyArray(sctx->redisCtx);
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
      IndexReader *reader = NewTermIndexReader(iv);
      ReplyReaderResults(reader, sctx->redisCtx);
    }

    RedisModule_ReplySetArrayLength(ctx, nsubelem);
  }
  TrieMapIterator_Free(iter);
  RedisModule_ReplySetArrayLength(ctx, nvalues - 1);

reply_done:
  RedisModule_ReplySetArrayLength(ctx, nelem);

end:
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
  if (!debugCommandsEnabled(ctx)) {
    return RedisModule_ReplyWithError(ctx, NODEBUG_ERR);
  }
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
  if (!debugCommandsEnabled(ctx)) {
    return RedisModule_ReplyWithError(ctx, NODEBUG_ERR);
  }
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
  VecSimIndex *vecsimIndex = openVectorIndex(sctx->spec, keyName, CREATE_INDEX);
  if(!vecsimIndex) {
    SearchCtx_Free(sctx);
    return RedisModule_ReplyWithError(ctx, "Can't open vector index");
  }

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
  if (!debugCommandsEnabled(ctx)) {
    return RedisModule_ReplyWithError(ctx, NODEBUG_ERR);
  }
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
  if (!debugCommandsEnabled(ctx)) {
    return RedisModule_ReplyWithError(ctx, NODEBUG_ERR);
  }
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
  VecSimIndex *vecsimIndex = openVectorIndex(sctx->spec, keyName, CREATE_INDEX);
  if(!vecsimIndex) {
    RedisModule_ReplyWithError(ctx, "Can't open vector index");
    goto cleanup;
  }


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

/**
 * FT.DEBUG WORKERS [PAUSE / RESUME / DRAIN / STATS / N_THREADS]
 */
DEBUG_COMMAND(WorkerThreadsSwitch) {
  if (!debugCommandsEnabled(ctx)) {
    return RedisModule_ReplyWithError(ctx, NODEBUG_ERR);
  }
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

DEBUG_COMMAND(setMaxScannedDocs) {
  if (!debugCommandsEnabled(ctx)) {
    return RedisModule_ReplyWithError(ctx, NODEBUG_ERR);
  }
  if (argc != 3) {
    return RedisModule_WrongArity(ctx);
  }
  long long max_scanned_docs;
  if (RedisModule_StringToLongLong(argv[2], &max_scanned_docs) != REDISMODULE_OK) {
    return RedisModule_ReplyWithError(ctx, "Invalid argument for 'SET_MAX_SCANNED_DOCS'");
  }

  // Negative maxDocsTBscanned represents no limit

  globalDebugCtx.bgIndexing.maxDocsTBscanned = (int) max_scanned_docs;

  // Check if we need to enable debug mode
  validateDebugMode(&globalDebugCtx);

  return RedisModule_ReplyWithSimpleString(ctx, "OK");
}

DEBUG_COMMAND(setPauseOnScannedDocs) {
  if (!debugCommandsEnabled(ctx)) {
    return RedisModule_ReplyWithError(ctx, NODEBUG_ERR);
  }
  if (argc != 3) {
    return RedisModule_WrongArity(ctx);
  }
  long long pause_scanned_docs;
  if (RedisModule_StringToLongLong(argv[2], &pause_scanned_docs) != REDISMODULE_OK) {
    return RedisModule_ReplyWithError(ctx, "Invalid argument for 'SET_PAUSE_ON_SCANNED_DOCS'");
  }

  globalDebugCtx.bgIndexing.maxDocsTBscannedPause = (int) pause_scanned_docs;

  // Check if we need to enable debug mode
  validateDebugMode(&globalDebugCtx);

  return RedisModule_ReplyWithSimpleString(ctx, "OK");
}

DEBUG_COMMAND(setBgIndexResume) {
  if (!debugCommandsEnabled(ctx)) {
    return RedisModule_ReplyWithError(ctx, NODEBUG_ERR);
  }
  if (argc != 3) {
    return RedisModule_WrongArity(ctx);
  }
  const char* op = RedisModule_StringPtrLen(argv[2], NULL);

  if (!strcasecmp(op, "true")) {
    globalDebugCtx.bgIndexing.pause = false;
  } else {
    return RedisModule_ReplyWithError(ctx, "Invalid argument for 'SET_BG_INDEX_RESUME'");
  }

  return RedisModule_ReplyWithSimpleString(ctx, "OK");
}

DEBUG_COMMAND(getDebugScannerStatus) {
  if (!debugCommandsEnabled(ctx)) {
    return RedisModule_ReplyWithError(ctx, NODEBUG_ERR);
  }
  if (argc != 3) {
    return RedisModule_WrongArity(ctx);
  }

  IndexLoadOptions lopts = {.nameC = RedisModule_StringPtrLen(argv[2], NULL),
                            .flags = INDEXSPEC_LOAD_NOTIMERUPDATE};

  StrongRef ref = IndexSpec_LoadUnsafeEx(&lopts);
  IndexSpec *sp = StrongRef_Get(ref);

  if (!sp) {
    return RedisModule_ReplyWithError(ctx, "Unknown index name");
  }

  if (!sp->scanner) {
    return RedisModule_ReplyWithError(ctx, "Scanner is not initialized");
  }

  if(!(sp->scanner->isDebug)) {
    return RedisModule_ReplyWithError(ctx, "Debug mode enabled but scanner is not a debug scanner");
  }

  // Assuming this file is aware of spec.h, via direct or in-direct include
  DebugIndexesScanner *dScanner = (DebugIndexesScanner*)sp->scanner;


  return RedisModule_ReplyWithSimpleString(ctx, DEBUG_INDEX_SCANNER_STATUS_STRS[dScanner->status]);
}

DEBUG_COMMAND(setPauseBeforeScan) {
  if (!debugCommandsEnabled(ctx)) {
    return RedisModule_ReplyWithError(ctx, NODEBUG_ERR);
  }
  if (argc != 3) {
    return RedisModule_WrongArity(ctx);
  }
  const char* op = RedisModule_StringPtrLen(argv[2], NULL);

  if (!strcasecmp(op, "true")) {
    globalDebugCtx.bgIndexing.pauseBeforeScan = true;
  } else if (!strcasecmp(op, "false")) {
    globalDebugCtx.bgIndexing.pauseBeforeScan = false;
  } else {
    return RedisModule_ReplyWithError(ctx, "Invalid argument for 'SET_PAUSE_BEFORE_SCAN'");
  }

  validateDebugMode(&globalDebugCtx);

  return RedisModule_ReplyWithSimpleString(ctx, "OK");
}
DEBUG_COMMAND(bgScanController) {
  if (!debugCommandsEnabled(ctx)) {
    return RedisModule_ReplyWithError(ctx, NODEBUG_ERR);
  }
  if (argc < 3) {
    return RedisModule_WrongArity(ctx);
  }
  const char* op = RedisModule_StringPtrLen(argv[2], NULL);

  // Check here all background indexing possible commands
  if (!strcmp("SET_MAX_SCANNED_DOCS", op)) {
    return setMaxScannedDocs(ctx, argv+1, argc-1);
  }
  if (!strcmp("SET_PAUSE_ON_SCANNED_DOCS", op)) {
    return setPauseOnScannedDocs(ctx, argv+1, argc-1);
  }
  if (!strcmp("SET_BG_INDEX_RESUME", op)) {
    return setBgIndexResume(ctx, argv+1, argc-1);
  }
  if (!strcmp("GET_DEBUG_SCANNER_STATUS", op)) {
    return getDebugScannerStatus(ctx, argv+1, argc-1);
  }
  if (!strcmp("SET_PAUSE_BEFORE_SCAN", op)) {
    return setPauseBeforeScan(ctx, argv+1, argc-1);
  }
  return RedisModule_ReplyWithError(ctx, "Invalid command for 'BG_SCAN_CONTROLLER'");

}
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
                               {"SPEC_INVIDXES_INFO", SpecInvertedIndexesInfo}, // Print general information about the inverted indexes in the spec
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
                               {"SET_MONITOR_EXPIRATION", setMonitorExpiration},
                               {"WORKERS", WorkerThreadsSwitch},
                               {"BG_SCAN_CONTROLLER", bgScanController},


                               /* IMPORTANT NOTE: Every debug command starts with
                                * checking if redis allows this context to execute
                                * debug commands by calling `debugCommandsEnabled(ctx)`.
                                * If you add a new debug command, make sure to add it.
                               */
                               {NULL, NULL}};

int DebugHelpCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
  size_t len = 0;
  for (DebugCommandType *c = &commands[0]; c->name != NULL; c++) {
    RedisModule_ReplyWithCString(ctx, c->name);
    ++len;
  }
  for (size_t i = 0; coordCommandsNames[i]; i++) {
    RedisModule_ReplyWithCString(ctx, coordCommandsNames[i]);
    ++len;
  }
  RedisModule_ReplySetArrayLength(ctx, len);
  return REDISMODULE_OK;
}

int RegisterDebugCommands(RedisModuleCommand *debugCommand) {
  for (DebugCommandType *c = &commands[0]; c->name != NULL; c++) {
    int rc = RedisModule_CreateSubcommand(debugCommand, c->name, c->callback,
              IsEnterprise() ? "readonly " CMD_PROXY_FILTERED : "readonly",
              RS_DEBUG_FLAGS);
    if (rc != REDISMODULE_OK) return rc;
  }
  return RedisModule_CreateSubcommand(debugCommand, "HELP", DebugHelpCommand,
          IsEnterprise() ? "readonly " CMD_PROXY_FILTERED : "readonly",
          RS_DEBUG_FLAGS);
}

#if (defined(DEBUG) || defined(_DEBUG)) && !defined(NDEBUG)
#include "readies/cetara/diag/gdb.c"
#endif
