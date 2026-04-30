/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "commands.h"
#include "debug_commands.h"
#include "coord/debug_command_names.h"
#include "VecSim/vec_sim_debug.h"
#include "inverted_index.h"
#include "redis_index.h"
#include "redisearch_rs/headers/numeric_range_tree.h"
#include "tag_index.h"
#include "redisearch_rs/headers/iterators_rs.h"
#include "geometry/geometry_api.h"
#include "geometry_index.h"
#include "phonetic_manager.h"
#include "gc.h"
#include "module.h"
#include "suffix.h"
#include "triemap.h"
#include "util/workers.h"
#include "cursor.h"
#include "module.h"
#include "aggregate/aggregate.h"
#include "aggregate/aggregate_debug.h"
#include "hybrid/hybrid_debug.h"
#include "hybrid/hybrid_exec.h"
#include "reply.h"
#include "reply_macros.h"
#include "obfuscation/obfuscation_api.h"
#include "info/info_command.h"
#include "search_disk.h"
#include "ext/debug_scorers.h"
#include "query_error.h"
#include "doc_id_meta.h"

DebugCTX globalDebugCtx = {0};

// QueryDebugCtx API implementations
bool QueryDebugCtx_IsPaused(void) {
  return globalDebugCtx.query.pause;
}

void QueryDebugCtx_SetPause(bool pause) {
  globalDebugCtx.query.pause = pause;
}

ResultProcessor* QueryDebugCtx_GetDebugRP(void) {
  return globalDebugCtx.query.debugRP;
}

void QueryDebugCtx_SetDebugRP(ResultProcessor* debugRP) {
  globalDebugCtx.query.debugRP = debugRP;
}

bool QueryDebugCtx_HasDebugRP(void) {
  return globalDebugCtx.query.debugRP != NULL;
}

#ifdef ENABLE_ASSERT
// Global coordinator reduce debug context (separate from DebugCTX since it uses atomics)
static CoordReduceDebugCtx globalCoordReduceDebugCtx = {0};

bool CoordReduceDebugCtx_IsPaused(void) {
  return atomic_load(&globalCoordReduceDebugCtx.pause);
}

void CoordReduceDebugCtx_SetPause(bool pause) {
  atomic_store(&globalCoordReduceDebugCtx.pause, pause);
}

int CoordReduceDebugCtx_GetPauseBeforeN(void) {
  return atomic_load(&globalCoordReduceDebugCtx.pauseBeforeN);
}

void CoordReduceDebugCtx_SetPauseBeforeN(int n) {
  atomic_store(&globalCoordReduceDebugCtx.pauseBeforeN, n);
  // Reset reduce count when setting a new pause point
  atomic_store(&globalCoordReduceDebugCtx.reduceCount, 0);
}

void CoordReduceDebugCtx_IncrementReduceCount(void) {
  atomic_fetch_add(&globalCoordReduceDebugCtx.reduceCount, 1);
}

int CoordReduceDebugCtx_GetReduceCount(void) {
  return atomic_load(&globalCoordReduceDebugCtx.reduceCount);
}

// Global store results debug context
static StoreResultsDebugCtx globalStoreResultsDebugCtx = {0};

bool StoreResultsDebugCtx_IsPauseBeforeEnabled(void) {
  return atomic_load(&globalStoreResultsDebugCtx.pauseBeforeEnabled);
}

void StoreResultsDebugCtx_SetPauseBeforeEnabled(bool enabled) {
  atomic_store(&globalStoreResultsDebugCtx.pauseBeforeEnabled, enabled);
  atomic_store(&globalStoreResultsDebugCtx.pause, false);
}

bool StoreResultsDebugCtx_IsPauseAfterEnabled(void) {
  return atomic_load(&globalStoreResultsDebugCtx.pauseAfterEnabled);
}

void StoreResultsDebugCtx_SetPauseAfterEnabled(bool enabled) {
  atomic_store(&globalStoreResultsDebugCtx.pauseAfterEnabled, enabled);
  atomic_store(&globalStoreResultsDebugCtx.pause, false);
}

bool StoreResultsDebugCtx_IsPaused(void) {
  return atomic_load(&globalStoreResultsDebugCtx.pause);
}

void StoreResultsDebugCtx_SetPause(bool pause) {
  atomic_store(&globalStoreResultsDebugCtx.pause, pause);
}

// ============================================================================
// Named Sync Points Implementation
// ============================================================================

// Maximum number of named sync points that can be armed simultaneously
#define SYNC_POINT_MAX_ARMED 16
// Maximum length of a sync point name
#define SYNC_POINT_NAME_MAX_LEN 64

// State of a single sync point
typedef struct SyncPointState {
  char name[SYNC_POINT_NAME_MAX_LEN];   // Name of the sync point
  atomic_bool armed;                    // Whether this sync point is armed (will block)
  _Atomic uint32_t waiting;             // Number of threads currently waiting at this point
} SyncPointState;

// Container for all sync point states
typedef struct SyncPointCtx {
  SyncPointState points[SYNC_POINT_MAX_ARMED];   // Array of sync points
  _Atomic uint32_t count;                        // Number of armed sync points
} SyncPointCtx;

static SyncPointCtx globalSyncPointCtx = {0};

// Internal helper: find sync point by name
static SyncPointState* SyncPoint_FindByName(const char *name) {
  // Use acquire semantics to synchronize with the release fence in SyncPoint_Arm,
  // ensuring we see fully initialized slots when iterating.
  uint32_t count = atomic_load_explicit(&globalSyncPointCtx.count, memory_order_acquire);
  for (uint32_t i = 0; i < count; i++) {
    if (strcmp(globalSyncPointCtx.points[i].name, name) == 0) {
      return &globalSyncPointCtx.points[i];
    }
  }
  return NULL;
}

bool SyncPoint_Arm(const char *name) {
  SyncPointState *existing = SyncPoint_FindByName(name);
  if (existing) {
    atomic_store(&existing->armed, true);
    return true;
  }
  // Reserve a slot atomically. We use a simple counter since ARM is only called
  // from the main thread (via FT.DEBUG command), so no concurrent ARMs occur.
  uint32_t idx = atomic_load(&globalSyncPointCtx.count);
  if (idx >= SYNC_POINT_MAX_ARMED) {
    return false;
  }
  // Initialize the slot BEFORE making it visible to avoid data race:
  // Other threads calling SyncPoint_FindByName iterate up to `count`,
  // so we must fully initialize before incrementing count.
  SyncPointState *sp = &globalSyncPointCtx.points[idx];
  strncpy(sp->name, name, SYNC_POINT_NAME_MAX_LEN - 1);
  sp->name[SYNC_POINT_NAME_MAX_LEN - 1] = '\0';
  atomic_store(&sp->armed, true);
  // Note: We intentionally do NOT reset sp->waiting here.
  // The slot is either newly allocated (waiting is 0 from static init) or
  // reused after ClearAll drained it to 0. Resetting it here would race with
  // threads executing atomic_fetch_sub after exiting the spin-wait loop.

  // Memory fence: ensure all writes above are visible before incrementing count
  atomic_thread_fence(memory_order_release);
  atomic_fetch_add(&globalSyncPointCtx.count, 1);
  return true;
}

void SyncPoint_Signal(const char *name) {
  SyncPointState *sp = SyncPoint_FindByName(name);
  if (sp) atomic_store(&sp->armed, false);  // Disarm to release waiting thread
}

bool SyncPoint_IsWaiting(const char *name) {
  SyncPointState *sp = SyncPoint_FindByName(name);
  return sp ? (atomic_load(&sp->waiting) > 0) : false;
}

bool SyncPoint_IsArmed(const char *name) {
  SyncPointState *sp = SyncPoint_FindByName(name);
  return sp ? atomic_load(&sp->armed) : false;
}

void SyncPoint_ClearAll(void) {
  uint32_t count = atomic_load(&globalSyncPointCtx.count);

  // First, disarm all sync points to release waiting threads
  for (uint32_t i = 0; i < count; i++) {
    atomic_store(&globalSyncPointCtx.points[i].armed, false);
  }

  // Wait for all waiting threads to exit their spin-wait loops.
  // This prevents a slot reuse race: if we reset count while a thread still
  // holds a pointer to a slot, a subsequent Arm could reuse that slot and
  // set armed=true, causing the old thread to get trapped waiting on the
  // wrong sync point.
  for (uint32_t i = 0; i < count; i++) {
    while (atomic_load(&globalSyncPointCtx.points[i].waiting) > 0) {
      usleep(1000);  // Brief sleep to avoid busy-waiting
    }
  }

  // Now it's safe to reset count - no threads hold pointers to slots
  atomic_store(&globalSyncPointCtx.count, 0);
}

void SyncPoint_Wait(const char *name) {
  SyncPointState *sp = SyncPoint_FindByName(name);
  if (!sp || !atomic_load(&sp->armed)) return;

  atomic_fetch_add(&sp->waiting, 1);  // Increment waiting counter
  while (atomic_load(&sp->armed)) {
    usleep(1000);  // Spin-wait with 1ms sleep (matches existing pattern)
  }
  atomic_fetch_sub(&sp->waiting, 1);  // Decrement waiting counter
}

void SyncPoint_WaitUntil(const char *name, SyncPointStopFn stop_fn, void *arg) {
  SyncPointState *sp = SyncPoint_FindByName(name);
  if (!sp || !atomic_load(&sp->armed)) return;

  atomic_fetch_add(&sp->waiting, 1);
  while (atomic_load(&sp->armed)) {
    if (stop_fn && stop_fn(arg)) break;
    usleep(1000);
  }
  atomic_fetch_sub(&sp->waiting, 1);
}

// Global hybrid store cursors debug context (for HREQ cursor storage only)
static HybridStoreCursorsDebugCtx globalHybridStoreCursorsDebugCtx = {0};

bool HybridStoreCursorsDebugCtx_IsPauseBeforeEnabled(void) {
  return atomic_load(&globalHybridStoreCursorsDebugCtx.pauseBeforeEnabled);
}

void HybridStoreCursorsDebugCtx_SetPauseBeforeEnabled(bool enabled) {
  atomic_store(&globalHybridStoreCursorsDebugCtx.pauseBeforeEnabled, enabled);
  atomic_store(&globalHybridStoreCursorsDebugCtx.pause, false);
}

bool HybridStoreCursorsDebugCtx_IsPauseAfterEnabled(void) {
  return atomic_load(&globalHybridStoreCursorsDebugCtx.pauseAfterEnabled);
}

void HybridStoreCursorsDebugCtx_SetPauseAfterEnabled(bool enabled) {
  atomic_store(&globalHybridStoreCursorsDebugCtx.pauseAfterEnabled, enabled);
  atomic_store(&globalHybridStoreCursorsDebugCtx.pause, false);
}

bool HybridStoreCursorsDebugCtx_IsPaused(void) {
  return atomic_load(&globalHybridStoreCursorsDebugCtx.pause);
}

void HybridStoreCursorsDebugCtx_SetPause(bool pause) {
  atomic_store(&globalHybridStoreCursorsDebugCtx.pause, pause);
}
#endif

void validateDebugMode(DebugCTX *debugCtx) {
  // Debug mode is enabled if any of its field is non-default
  // Should be called after each debug command that changes the debugCtx
  debugCtx->debugMode =
    (debugCtx->bgIndexing.maxDocsTBscanned > 0) ||
    (debugCtx->bgIndexing.maxDocsTBscannedPause > 0) ||
    (debugCtx->bgIndexing.pauseBeforeScan) ||
    (debugCtx->bgIndexing.pauseOnOOM) ||
    (debugCtx->bgIndexing.pauseBeforeOOMretry);

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

static void ReplyIteratorResultsIDs(QueryIterator *iterator, RedisModuleCtx *ctx) {
  size_t resultSize = 0;
  RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
  while (iterator->Read(iterator) == ITERATOR_OK) {
    RedisModule_ReplyWithLongLong(ctx, iterator->lastDocId);
    ++resultSize;
  }
  RedisModule_ReplySetArrayLength(ctx, resultSize);
  iterator->Free(iterator);
}

static void ReplyReaderResultsIDs(IndexReader *reader, RSIndexResult *res, RedisModuleCtx *ctx) {
  size_t resultSize = 0;
  RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
  while (IndexReader_Next(reader, res)) {
      RedisModule_ReplyWithLongLong(ctx, res->docId);
      ++resultSize;
  }
  RedisModule_ReplySetArrayLength(ctx, resultSize);
  IndexReader_Free(reader);
  IndexResult_Free(res);
}

static FieldSpec *getFieldByNameAndType(IndexSpec *spec, RedisModuleString *fieldNameRS,
                                          FieldType t) {
  size_t len;
  const char *fieldName = RedisModule_StringPtrLen(fieldNameRS, &len);
  const FieldSpec *fieldSpec = IndexSpec_GetFieldWithLength(spec, fieldName, len);
  if (!fieldSpec || !FIELD_IS(fieldSpec, t)) {
    return NULL;
  }
  return (FieldSpec *)fieldSpec;
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
  while (TrieIterator_Next(it, &rstr, &slen, NULL, &score, NULL, &dist)) {
    char *res = runesToStr(rstr, slen, &termLen);
    RedisModule_ReplyWithStringBuffer(ctx, res, termLen);
    rm_free(res);
  }
  TrieIterator_Free(it);

  SearchCtx_Free(sctx);
  return REDISMODULE_OK;
}


static size_t InvertedIndexSummaryHeader(RedisModuleCtx *ctx, InvertedIndex *invidx) {
  IISummary summary = InvertedIndex_Summary(invidx);
  size_t invIdxBulkLen = 0;

  REPLY_WITH_LONG_LONG("numDocs", summary.number_of_docs, invIdxBulkLen);
  REPLY_WITH_LONG_LONG("numEntries", summary.number_of_entries, invIdxBulkLen);
  REPLY_WITH_LONG_LONG("lastId", summary.last_doc_id, invIdxBulkLen);
  REPLY_WITH_LONG_LONG("flags", summary.flags, invIdxBulkLen);
  REPLY_WITH_LONG_LONG("numberOfBlocks", summary.number_of_blocks, invIdxBulkLen);
  if (summary.has_efficiency) {
    REPLY_WITH_DOUBLE("blocks_efficiency (numEntries/numberOfBlocks)", summary.block_efficiency, invIdxBulkLen);
  }
  return invIdxBulkLen;
}

DEBUG_COMMAND(InvertedIndexSummary) {
  size_t len;
  const char *invIdxName = NULL;
  InvertedIndex *invidx = NULL;
  size_t invIdxBulkLen = 0;
  size_t blockCount = 0;
  IIBlockSummary *blocksSummary = NULL;

  if (!debugCommandsEnabled(ctx)) {
    return RedisModule_ReplyWithError(ctx, NODEBUG_ERR);
  }
  if (argc != 4) {
    return RedisModule_WrongArity(ctx);
  }
  GET_SEARCH_CTX(argv[2])
  invIdxName = RedisModule_StringPtrLen(argv[3], &len);
  invidx = Redis_OpenInvertedIndex(sctx, invIdxName, len, 0, NULL);
  if (!invidx) {
    RedisModule_ReplyWithError(sctx->redisCtx, "Can not find the inverted index");
    goto end;
  }

  RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
  invIdxBulkLen = InvertedIndexSummaryHeader(ctx, invidx);

  RedisModule_ReplyWithStringBuffer(ctx, "blocks", strlen("blocks"));

  blocksSummary = InvertedIndex_BlocksSummary(invidx, &blockCount);

  for (size_t i = 0; i < blockCount; i++) {
    IIBlockSummary *blockSummary = blocksSummary + i;
    size_t blockBulkLen = 0;
    RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);

    REPLY_WITH_LONG_LONG("firstId", blockSummary->first_doc_id, blockBulkLen);
    REPLY_WITH_LONG_LONG("lastId", blockSummary->last_doc_id, blockBulkLen);
    REPLY_WITH_LONG_LONG("numEntries", blockSummary->number_of_entries, blockBulkLen);

    RedisModule_ReplySetArrayLength(ctx, blockBulkLen);
  }

  InvertedIndex_BlocksSummaryFree(blocksSummary, blockCount);

  invIdxBulkLen += 2;

  RedisModule_ReplySetArrayLength(ctx, invIdxBulkLen);

end:
  SearchCtx_Free(sctx);
  return REDISMODULE_OK;
}

DEBUG_COMMAND(DumpInvertedIndex) {
  size_t len = 0;
  const char *invIdxName = NULL;
  InvertedIndex *invidx = NULL;
  IndexDecoderCtx decoderCtx;
  IndexReader *reader = NULL;
  RSIndexResult *res = NULL;

  if (!debugCommandsEnabled(ctx)) {
    return RedisModule_ReplyWithError(ctx, NODEBUG_ERR);
  }
  if (argc != 4) {
    return RedisModule_WrongArity(ctx);
  }
  GET_SEARCH_CTX(argv[2])
  invIdxName = RedisModule_StringPtrLen(argv[3], &len);
  invidx = Redis_OpenInvertedIndex(sctx, invIdxName, len, 0, NULL);
  if (!invidx) {
    RedisModule_ReplyWithError(sctx->redisCtx, "Can not find the inverted index");
    goto end;
  }
  decoderCtx = (IndexDecoderCtx){.field_mask_tag = IndexDecoderCtx_FieldMask, .field_mask = RS_FIELDMASK_ALL};
  reader = NewIndexReader(invidx, decoderCtx);
  res = NewTokenRecord(NULL, 1);
  res->freq = 1;
  res->fieldMask = RS_FIELDMASK_ALL;
  ReplyReaderResultsIDs(reader, res, sctx->redisCtx);

end:
  SearchCtx_Free(sctx);
  return REDISMODULE_OK;
}

// FT.DEBUG NUMIDX_SUMMARY INDEX_NAME NUMERIC_FIELD_NAME
DEBUG_COMMAND(NumericIndexSummary) {
  FieldSpec *fs = NULL;
  NumericRangeTree *rt = NULL;

  if (!debugCommandsEnabled(ctx)) {
    return RedisModule_ReplyWithError(ctx, NODEBUG_ERR);
  }
  if (argc != 4) {
    return RedisModule_WrongArity(ctx);
  }
  GET_SEARCH_CTX(argv[2])
  fs = getFieldByNameAndType(sctx->spec, argv[3], INDEXFLD_T_NUMERIC | INDEXFLD_T_GEO);
  if (!fs) {
    RedisModule_ReplyWithError(sctx->redisCtx, "Could not find given field in index spec");
    goto end;
  }
  rt = openNumericOrGeoIndex(sctx->spec, fs, DONT_CREATE_INDEX);
  NumericRangeTree_DebugSummary(sctx->redisCtx, rt);
end:
  SearchCtx_Free(sctx);
  return REDISMODULE_OK;
}

// FT.DEBUG DUMP_NUMIDX <INDEX_NAME> <NUMERIC_FIELD_NAME> [WITH_HEADERS]
DEBUG_COMMAND(DumpNumericIndex) {
  FieldSpec *fs = NULL;
  bool with_headers = false;
  NumericRangeTree *rt = NULL;

  if (!debugCommandsEnabled(ctx)) {
    return RedisModule_ReplyWithError(ctx, NODEBUG_ERR);
  }
  if (argc < 4) {
    return RedisModule_WrongArity(ctx);
  }
  GET_SEARCH_CTX(argv[2])
  fs = getFieldByNameAndType(sctx->spec, argv[3], INDEXFLD_T_NUMERIC | INDEXFLD_T_GEO);
  if (!fs) {
    RedisModule_ReplyWithError(sctx->redisCtx, "Could not find given field in index spec");
    goto end;
  }

  // It's a debug command... lets not waste time on string comparison.
  with_headers = argc == 5 ? true : false;

  rt = openNumericOrGeoIndex(sctx->spec, fs, DONT_CREATE_INDEX);
  NumericRangeTree_DebugDumpIndex(sctx->redisCtx, rt, with_headers);
end:
  SearchCtx_Free(sctx);
  return REDISMODULE_OK;
}

DEBUG_COMMAND(DumpGeometryIndex) {
  FieldSpec *fs = NULL;
  const GeometryIndex *idx = NULL;
  const GeometryApi *api = NULL;

  if (!debugCommandsEnabled(ctx)) {
    return RedisModule_ReplyWithError(ctx, NODEBUG_ERR);
  }
  if (argc != 4) {
    return RedisModule_WrongArity(ctx);
  }
  GET_SEARCH_CTX(argv[2])

  fs = getFieldByNameAndType(sctx->spec, argv[3], INDEXFLD_T_GEOMETRY);
  if (!fs) {
    RedisModule_ReplyWithError(sctx->redisCtx, "Could not find given field in index spec");
    goto end;
  }

  // TODO: use DONT_CREATE_INDEX and imitate the reply struct of an empty index.
  idx = OpenGeometryIndex(fs, CREATE_INDEX);
  if (!idx) {
    RedisModule_ReplyWithError(sctx->redisCtx, "Could not open geoshape index");
    goto end;
  }
  api = GeometryApi_Get(idx);
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
  REPLY_WITH_LONG_LONG("prefixes_count", TrieMap_NUniqueKeys(prefixes_map), ARRAY_LEN_VAR(prefixesMapDump));
  REPLY_WITH_LONG_LONG("prefixes_trie_nodes", TrieMap_NNodes(prefixes_map), ARRAY_LEN_VAR(prefixesMapDump));
  END_POSTPONED_LEN_ARRAY(prefixesMapDump);

  return REDISMODULE_OK;
}

// FT.DEBUG DUMP_NUMIDXTREE INDEX_NAME NUMERIC_FIELD_NAME [MINIMAL]
DEBUG_COMMAND(DumpNumericIndexTree) {
  FieldSpec *fs = NULL;
  NumericRangeTree *rt = NULL;
  bool minimal = false;

  if (!debugCommandsEnabled(ctx)) {
    return RedisModule_ReplyWithError(ctx, NODEBUG_ERR);
  }
  if (argc < 4 || argc > 5) {
    return RedisModule_WrongArity(ctx);
  }
  GET_SEARCH_CTX(argv[2])
  fs = getFieldByNameAndType(sctx->spec, argv[3], INDEXFLD_T_NUMERIC | INDEXFLD_T_GEO);
  if (!fs) {
    RedisModule_ReplyWithError(sctx->redisCtx, "Could not find given field in index spec");
    goto end;
  }
  rt = openNumericOrGeoIndex(sctx->spec, fs, DONT_CREATE_INDEX);
  minimal = argc > 4 && !strcasecmp(RedisModule_StringPtrLen(argv[4], NULL), "minimal");
  NumericRangeTree_DebugDumpTree(sctx->redisCtx, rt, minimal);
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
  FieldSpec *fs = NULL;
  const TagIndex *tagIndex = NULL;
  TrieMapIterator *iter = NULL;
  char *tag = NULL;
  tm_len_t len = 0;
  InvertedIndex *iv = NULL;
  size_t resultSize = 0;

  if (!debugCommandsEnabled(ctx)) {
    return RedisModule_ReplyWithError(ctx, NODEBUG_ERR);
  }
  if (argc != 4) {
    return RedisModule_WrongArity(ctx);
  }
  GET_SEARCH_CTX(argv[2])
  fs = getFieldByNameAndType(sctx->spec, argv[3], INDEXFLD_T_TAG);
  if (!fs) {
    RedisModule_ReplyWithError(sctx->redisCtx, "Could not find given field in index spec");
    goto end;
  }
  tagIndex = TagIndex_Open(fs);

  // Field was not initialized yet
  if (!tagIndex) {
    RedisModule_ReplyWithEmptyArray(sctx->redisCtx);
    goto end;
  }

  // Debug dump not supported for disk-mode tag indexes (TrieMap contains NULL sentinels)
  if (tagIndex->diskSpec) {
    RedisModule_ReplyWithError(sctx->redisCtx, "DUMP_TAGIDX not supported for disk-mode indexes");
    goto end;
  }

  iter = TrieMap_Iterate(tagIndex->values);
  RedisModule_ReplyWithArray(sctx->redisCtx, REDISMODULE_POSTPONED_ARRAY_LEN);
  while (TrieMapIterator_Next(iter, &tag, &len, (void **)&iv)) {
    RedisModule_ReplyWithArray(sctx->redisCtx, 2);
    RedisModule_ReplyWithStringBuffer(sctx->redisCtx, tag, len);
    IndexDecoderCtx decoderCtx = {.field_mask_tag = IndexDecoderCtx_FieldMask, .field_mask = RS_FIELDMASK_ALL};
    IndexReader *reader = NewIndexReader(iv, decoderCtx);
    RSIndexResult *res = NewTokenRecord(NULL, 1);
    res->freq = 1;
    res->fieldMask = RS_FIELDMASK_ALL;
    ReplyReaderResultsIDs(reader, res, sctx->redisCtx);
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

    while (TrieIterator_Next(it, &rstr, &len, NULL, &score, NULL, NULL)) {
      size_t slen;
      char *s = runesToStr(rstr, len, &slen);
      RedisModule_ReplyWithStringBuffer(ctx, s, slen);
      rm_free(s);
      ++resultSize;
    }

    TrieIterator_Free(it);

    RedisModule_ReplySetArrayLength(ctx, resultSize);

  } else { // suffix triemap of tag field
    FieldSpec *fs = getFieldByNameAndType(sctx->spec, argv[3], INDEXFLD_T_TAG);
    if (!fs) {
      RedisModule_ReplyWithError(sctx->redisCtx, "Could not find given field in index spec");
      goto end;
    }
    const TagIndex *idx = TagIndex_Open(fs);

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

    TrieMapIterator *it = TrieMap_Iterate(idx->suffix);
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

static t_docId getDocIdFromKey(RedisModuleCtx *ctx, const IndexSpec *spec, RedisModuleString *key) {
  if (!SearchDisk_IsEnabled()) {
    return DocTable_GetIdR(&spec->docs, key);
  } else {
    uint64_t metaDocId;
    if (DocIdMeta_Get(ctx, key, spec->specId, &metaDocId) == REDISMODULE_OK) {
      return metaDocId;
    }
    return 0;
  }
}

DEBUG_COMMAND(IdToDocId) {
  long long id;
  const RSDocumentMetadata *doc;

  if (!debugCommandsEnabled(ctx)) {
    return RedisModule_ReplyWithError(ctx, NODEBUG_ERR);
  }
  if (argc != 4) {
    return RedisModule_WrongArity(ctx);
  }
  GET_SEARCH_CTX(argv[2])
  if (RedisModule_StringToLongLong(argv[3], &id) != REDISMODULE_OK) {
    RedisModule_ReplyWithError(sctx->redisCtx, "bad id given");
    goto end;
  }
  doc = DocTable_Borrow(&sctx->spec->docs, id);
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
  t_docId docId = getDocIdFromKey(sctx->redisCtx, sctx->spec, argv[3]);
  SearchCtx_Free(sctx);

  return RedisModule_ReplyWithLongLong(ctx, docId);
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
    const char *idx = RedisModule_StringPtrLen(argv[2], NULL);
    return RedisModule_ReplyWithErrorFormat(ctx, "%s: %s", QueryError_Strerror(QUERY_ERROR_CODE_NO_INDEX), idx);
  }

  if (sp->diskSpec) {
    SearchDisk_RunGC(sp->diskSpec, sp);
    RedisModule_ReplyWithSimpleString(ctx, "DONE");
    return REDISMODULE_OK;
  } else if (sp->gc) {
    RedisModuleBlockedClient *bc = RedisModule_BlockClient(
        ctx, GCForceInvokeReply, GCForceInvokeReplyTimeout, NULL, timeout);
    GCContext_ForceInvoke(sp->gc, bc);
    return REDISMODULE_OK;
  }
  return RedisModule_ReplyWithError(ctx, "GC is not available for this index");
}

// FT.DEBUG DISK_FLUSH <index>
// Flush the index
DEBUG_COMMAND(DiskFlush) {
  if (!debugCommandsEnabled(ctx)) {
    return RedisModule_ReplyWithError(ctx, NODEBUG_ERR);
  }
  if (argc != 3) {
    return RedisModule_WrongArity(ctx);
  }
  StrongRef ref = IndexSpec_LoadUnsafe(RedisModule_StringPtrLen(argv[2], NULL));
  IndexSpec *sp = StrongRef_Get(ref);
  if (!sp) {
    const char *idx = RedisModule_StringPtrLen(argv[2], NULL);
    return RedisModule_ReplyWithErrorFormat(ctx, "%s: %s", QueryError_Strerror(QUERY_ERROR_CODE_NO_INDEX), idx);
  }

  if (!sp->diskSpec) {
    return RedisModule_ReplyWithError(ctx, "Index is not a disk index");
  }

  SearchDisk_Flush(sp->diskSpec);
  RedisModule_ReplyWithSimpleString(ctx, "OK");
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
    const char *idx = RedisModule_StringPtrLen(argv[2], NULL);
    return RedisModule_ReplyWithErrorFormat(ctx, "%s: %s", QueryError_Strerror(QUERY_ERROR_CODE_NO_INDEX), idx);
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
    const char *idx = RedisModule_StringPtrLen(argv[2], NULL);
    return RedisModule_ReplyWithErrorFormat(ctx, "%s: %s", QueryError_Strerror(QUERY_ERROR_CODE_NO_INDEX), idx);
  }
  // Make sure there is no pending timer
  RedisModule_StopTimer(RSDummyContext, sp->gc->timerID, NULL);
  // mark as stopped. This will prevent the GC from scheduling itself again if it was already running.
  sp->gc->timerID = 0;
  RedisModule_Log(ctx, "verbose", "Stopped GC %p periodic run for index %s", sp->gc, IndexSpec_FormatName(sp, RSGlobalConfig.hideUserDataFromLog));
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
    const char *idx = RedisModule_StringPtrLen(argv[2], NULL);
    return RedisModule_ReplyWithErrorFormat(ctx, "%s: %s", QueryError_Strerror(QUERY_ERROR_CODE_NO_INDEX), idx);
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
  FieldSpec *fs;
  NumericRangeTree *rt;
  TrimEmptyLeavesResult rv;

  if (!debugCommandsEnabled(ctx)) {
    return RedisModule_ReplyWithError(ctx, NODEBUG_ERR);
  }

  if (argc != 4) {
    return RedisModule_WrongArity(ctx);
  }
  GET_SEARCH_CTX(argv[2])
  fs = getFieldByNameAndType(sctx->spec, argv[3], INDEXFLD_T_NUMERIC | INDEXFLD_T_GEO);
  if (!fs) {
    RedisModule_ReplyWithError(sctx->redisCtx, "Could not find given field in index spec");
    goto end;
  }
  rt = openNumericOrGeoIndex(sctx->spec, fs, DONT_CREATE_INDEX);
  if (!rt) {
    goto end;
  }

  rv = NumericRangeTree_TrimEmptyLeaves(rt);

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
    const char *idx = RedisModule_StringPtrLen(argv[2], NULL);
    return RedisModule_ReplyWithErrorFormat(ctx, "%s: %s", QueryError_Strerror(QUERY_ERROR_CODE_NO_INDEX), idx);
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
    const char *idx = RedisModule_StringPtrLen(argv[2], NULL);
    return RedisModule_ReplyWithErrorFormat(ctx, "%s: %s", QueryError_Strerror(QUERY_ERROR_CODE_NO_INDEX), idx);
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
    const char *idx = RedisModule_StringPtrLen(argv[2], NULL);
    return RedisModule_ReplyWithErrorFormat(ctx, "%s: %s", QueryError_Strerror(QUERY_ERROR_CODE_NO_INDEX), idx);
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
    const char *idx = RedisModule_StringPtrLen(argv[2], NULL);
    return RedisModule_ReplyWithErrorFormat(ctx, "%s: %s", QueryError_Strerror(QUERY_ERROR_CODE_NO_INDEX), idx);
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
  int rv;
  FieldSpec *fs;
  const TagIndex *idx;
  size_t nelem = 0;
  int shouldDescend;
  size_t limit;
  TrieMapIterator *iter;
  char *tag;
  tm_len_t len;
  InvertedIndex *iv;
  size_t nvalues = 0;

  if (!debugCommandsEnabled(ctx)) {
    return RedisModule_ReplyWithError(ctx, NODEBUG_ERR);
  }
  if (argc < 4) {
    return RedisModule_WrongArity(ctx);
  }
  GET_SEARCH_CTX(argv[2]);
  ArgsCursor_InitRString(&ac, argv + 4, argc - 4);
  rv = AC_ParseArgSpec(&ac, argspecs, &errSpec);
  if (rv != AC_OK) {
    RedisModule_ReplyWithError(ctx, "Could not parse argument (argspec fixme)");
    goto end;
  }

  fs = getFieldByNameAndType(sctx->spec, argv[3], INDEXFLD_T_TAG);
  if (!fs) {
    RedisModule_ReplyWithError(sctx->redisCtx, "Could not find given field in index spec");
    goto end;
  }

  idx = TagIndex_Open(fs);

  // Field was not initialized yet
  if (!idx) {
    RedisModule_ReplyWithEmptyArray(sctx->redisCtx);
    goto end;
  }

  // Debug info not supported for disk-mode tag indexes (TrieMap contains NULL sentinels)
  if (idx->diskSpec) {
    RedisModule_ReplyWithError(sctx->redisCtx, "INFO_TAGIDX not supported for disk-mode indexes");
    goto end;
  }

  RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
  RedisModule_ReplyWithLiteral(ctx, "num_values");
  RedisModule_ReplyWithLongLong(ctx, TrieMap_NUniqueKeys(idx->values));
  nelem += 2;

  if (options.dumpIdEntries) {
    options.countValueEntries = 1;
  }
  shouldDescend = options.countValueEntries || options.dumpIdEntries;
  if (!shouldDescend) {
    goto reply_done;
  }

  limit = options.limit ? options.limit : 0;
  iter = TrieMap_Iterate(idx->values);

  nelem += 2;
  RedisModule_ReplyWithLiteral(ctx, "values");
  RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);

  seekTagIterator(iter, options.offset);
  while (nvalues++ < limit && TrieMapIterator_Next(iter, &tag, &len, (void **)&iv)) {
    size_t nsubelem = 8;
    if (!options.dumpIdEntries) {
      nsubelem -= 2;
    }
    RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);

    RedisModule_ReplyWithLiteral(ctx, "value");
    RedisModule_ReplyWithStringBuffer(ctx, tag, len);

    RedisModule_ReplyWithLiteral(ctx, "num_entries");
    RedisModule_ReplyWithLongLong(ctx, InvertedIndex_NumDocs(iv));

    RedisModule_ReplyWithLiteral(ctx, "num_blocks");
    RedisModule_ReplyWithLongLong(ctx, InvertedIndex_NumBlocks(iv));

    if (options.dumpIdEntries) {
      RedisModule_ReplyWithLiteral(ctx, "entries");
      IndexDecoderCtx decoderCtx = {.field_mask_tag = IndexDecoderCtx_FieldMask, .field_mask = RS_FIELDMASK_ALL};
      IndexReader *reader = NewIndexReader(iv, decoderCtx);
      RSIndexResult *res = NewTokenRecord(NULL, 1);
      res->freq = 1;
      res->fieldMask = RS_FIELDMASK_ALL;
      ReplyReaderResultsIDs(reader, res, sctx->redisCtx);
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
  snprintf(buf, sizeof(buf), "(0x%x):", dmd->flags);
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
                            RedisSearchCtx *sctx, bool obfuscate, RedisModule_Reply *reply) {
  const RSSortingVector *sv = &dmd->sortVector;
  RedisModule_ReplyKV_Array(reply, name);
  for (size_t ii = 0; ii < RSSortingVector_Length(sv); ++ii) {
    if (!RSSortingVector_Get(sv, ii)) {
      continue;
    }
    RedisModule_Reply_Array(reply);
      RedisModule_ReplyKV_LongLong(reply, "index", ii);

      RedisModule_Reply_CString(reply, "field");
      const FieldSpec *fs = IndexSpec_GetFieldBySortingIndex(sctx->spec, ii);

      if (!fs) {
        RedisModule_Reply_CString(reply, "!!! AS ???");
      } else if (!fs->fieldPath) {
        char *name = FieldSpec_FormatName(fs, obfuscate);
        RedisModule_Reply_CString(reply, name);
        rm_free(name);
      } else {
        char *path = FieldSpec_FormatPath(fs, obfuscate);
        char *name = FieldSpec_FormatName(fs, obfuscate);
        RedisModule_Reply_Stringf(reply, "%s AS %s", path, name);
        rm_free(path);
        rm_free(name);
      }

      RedisModule_Reply_CString(reply, "value");
      RedisModule_Reply_RSValue(reply, RSSortingVector_Get(sv, ii), 0);
    RedisModule_Reply_ArrayEnd(reply);
  }
  RedisModule_Reply_ArrayEnd(reply);
}

/**
 * FT.DEBUG DOC_INFO <index> <doc> [OBFUSCATE/REVEAL]
 */
DEBUG_COMMAND(DocInfo) {
  if (!debugCommandsEnabled(ctx)) {
    return RedisModule_ReplyWithError(ctx, NODEBUG_ERR);
  }
  if (argc < 5) {
    return RedisModule_WrongArity(ctx);
  }
  GET_SEARCH_CTX(argv[2]);

  const RSDocumentMetadata *dmd = NULL;
  if (SearchDisk_IsEnabled()) {
    uint64_t docId;
    if (DocIdMeta_Get(ctx, argv[3], sctx->spec->specId, &docId) == REDISMODULE_OK) {
      RSDocumentMetadata *dmd_disk = rm_calloc(1, sizeof(RSDocumentMetadata));
      dmd_disk->sortVector = RSSortingVector_Empty();
      dmd_disk->ref_count = 1;
      if (SearchDisk_GetDocumentMetadata(sctx->spec->diskSpec, docId, dmd_disk, NULL)) {
        dmd = dmd_disk;
      } else {
        DMD_Return(dmd_disk);
      }
    }
  } else {
    dmd = DocTable_BorrowByKeyR(&sctx->spec->docs, argv[3]);
  }
  if (!dmd) {
    SearchCtx_Free(sctx);
    return RedisModule_ReplyWithError(ctx, "Document not found in index");
  }

  const char *obfuscateOrReveal = RedisModule_StringPtrLen(argv[4], NULL);
  const bool reveal = !strcasecmp(obfuscateOrReveal, "REVEAL");
  const bool obfuscate = !strcasecmp(obfuscateOrReveal, "OBFUSCATE");
  if (!reveal && !obfuscate) {
    DMD_Return(dmd);
    SearchCtx_Free(sctx);
    return RedisModule_ReplyWithError(ctx, "Invalid argument. Expected REVEAL or OBFUSCATE as the last argument");
  }
  RedisModule_Reply _reply = RedisModule_NewReply(ctx), *reply = &_reply;

  RedisModule_Reply_Map(reply);
    RedisModule_ReplyKV_LongLong(reply, "internal_id", dmd->id);
    replyDocFlags("flags", dmd, reply);
    RedisModule_ReplyKV_Double(reply, "score", dmd->score);
    RedisModule_ReplyKV_LongLong(reply, "num_tokens", dmd->docLen);
    RedisModule_ReplyKV_LongLong(reply, "max_freq", dmd->maxTermFreq);
    RedisModule_ReplyKV_LongLong(reply, "refcount", dmd->ref_count - 1); // TODO: should include the refcount of the command call?
    if (RSSortingVector_Length(&dmd->sortVector)) {
      replySortVector("sortables", dmd, sctx, obfuscate, reply);
    }
  RedisModule_Reply_MapEnd(reply);

  RedisModule_EndReply(reply);
  DMD_Return(dmd);
  SearchCtx_Free(sctx);

  return REDISMODULE_OK;
}

static void VecSim_Reply_Info_Iterator(RedisModuleCtx *ctx, VecSimDebugInfoIterator *infoIter) {
  RedisModule_ReplyWithArray(ctx, VecSimDebugInfoIterator_NumberOfFields(infoIter)*2);
  while(VecSimDebugInfoIterator_HasNextField(infoIter)) {
    VecSim_InfoField* infoField = VecSimDebugInfoIterator_NextField(infoIter);
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

  FieldSpec *fs = getFieldByNameAndType(sctx->spec, argv[3], INDEXFLD_T_VECTOR);
  if (!fs) {
    SearchCtx_Free(sctx);
    return RedisModule_ReplyWithError(ctx, "Vector index not found");
  }
  // This call can't fail, since we already checked that the key exists
  // (or should exist, and this call will create it).
  VecSimIndex *vecsimIndex = openVectorIndex(ctx, fs, CREATE_INDEX);
  if(!vecsimIndex) {
    SearchCtx_Free(sctx);
    return RedisModule_ReplyWithError(ctx, "Can't open vector index");
  }

  VecSimDebugInfoIterator *infoIter = VecSimIndex_DebugInfoIterator(vecsimIndex);
  // Recursively reply with the info iterator
  VecSim_Reply_Info_Iterator(ctx, infoIter);

  // Cleanup
  VecSimDebugInfoIterator_Free(infoIter); // Free the iterator (and all its nested children)
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
  FieldSpec *fs;
  VecSimIndex *vecsimIndex;
  VecSimIndexBasicInfo info;
  size_t len_num_docs = 0;

  if (!debugCommandsEnabled(ctx)) {
    return RedisModule_ReplyWithError(ctx, NODEBUG_ERR);
  }
  if (argc < 4 || argc > 5) { // it should be 4 or 5 (allowing specifying a certain doc)
    return RedisModule_WrongArity(ctx);
  }
  if (SearchDisk_IsEnabled()) {
    return RedisModule_ReplyWithError(ctx, "Command not supported in disk mode");
  }
  GET_SEARCH_CTX(argv[2])

  fs = getFieldByNameAndType(sctx->spec, argv[3], INDEXFLD_T_VECTOR);
  if (!fs) {
    RedisModule_ReplyWithError(ctx, "Vector index not found");
    goto cleanup;
  }
  // This call can't fail, since we already checked that the key exists
  // (or should exist, and this call will create it).
  vecsimIndex = openVectorIndex(ctx, fs, CREATE_INDEX);

  if(!vecsimIndex) {
    RedisModule_ReplyWithError(ctx, "Can't open vector index");
    goto cleanup;
  }


  info = VecSimIndex_BasicInfo(vecsimIndex);
  if (info.algo != VecSimAlgo_HNSWLIB) {
    RedisModule_ReplyWithError(ctx, "Vector index is not an HNSW index");
    goto cleanup;
  }
  if (info.isMulti) {
    RedisModule_ReplyWithError(ctx, "Command not supported for HNSW multi-value index");
    goto cleanup;
  }

  if (argc == 5) {  // we want the neighbors of a specific vector only
    t_docId doc_id = getDocIdFromKey(ctx, sctx->spec, argv[4]);
    if (doc_id == 0) {
      RedisModule_ReplyWithError(ctx, "The given key does not exist in index");
      goto cleanup;
    }
    replyDumpHNSW(ctx, vecsimIndex, doc_id);
    goto cleanup;
  }
  // Otherwise, dump neighbors for every document in the index.
  RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
  DOCTABLE_FOREACH((&sctx->spec->docs), {replyDumpHNSW(ctx, vecsimIndex, dmd->id); len_num_docs++;})
  RedisModule_ReplySetArrayLength(ctx, len_num_docs);

  cleanup:
  SearchCtx_Free(sctx);
  return REDISMODULE_OK;
}

/**
 * FT.DEBUG WORKERS [PAUSE / RESUME / DRAIN / STATS / N_THREADS]
 *
 * @warning Calling FT.DEBUG WORKERS DRAIN will block the main thread until all workers are idle, this could lead to a deadlock,
 *          if there are pending jobs that require to acquire the GIL (like when LOAD is called from a worker thread)
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
    // Log that we're waiting for the workers to finish.
    RedisModule_Log(RSDummyContext, "notice", "Debug workers drain");

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
    REPLY_WITH_LONG_LONG("numJobsInProgress", stats.num_jobs_in_progress, ARRAY_LEN_VAR(num_stats_fields));
    END_POSTPONED_LEN_ARRAY(num_stats_fields);
    return REDISMODULE_OK;
  }  else if (!strcasecmp(op, "n_threads")) {
    return RedisModule_ReplyWithLongLong(ctx, workersThreadPool_NumThreads());
  } else {
    return RedisModule_ReplyWithError(ctx, "Invalid argument for 'WORKERS' subcommand");
  }
  return RedisModule_ReplyWithSimpleString(ctx, "OK");
}

/**
 * FT.DEBUG COORD_THREADS [PAUSE / RESUME / STATS]
 *
 */
DEBUG_COMMAND(CoordThreadsSwitch) {
  if (!debugCommandsEnabled(ctx)) {
    return RedisModule_ReplyWithError(ctx, NODEBUG_ERR);
  }
  if (argc != 3) {
    return RedisModule_WrongArity(ctx);
  }
  const char* op = RedisModule_StringPtrLen(argv[2], NULL);
  if (!strcasecmp(op, "pause")) {
    if (ConcurrentSearch_pause() != REDISMODULE_OK) {
      return RedisModule_ReplyWithError(ctx, "Operation failed: coordinator thread pool doesn't exists"
                                      " or is not running");
    }
  } else if (!strcasecmp(op, "resume")) {
    if (ConcurrentSearch_resume() != REDISMODULE_OK) {
      return RedisModule_ReplyWithError(ctx, "Operation failed: coordinator thread pool doesn't exists"
                                        " or is already running");
    }
  } else if (!strcasecmp(op, "is_paused")) {
    return RedisModule_ReplyWithLongLong(ctx, ConcurrentSearch_isPaused());
  } else if (!strcasecmp(op, "stats")) {
    thpool_stats stats = ConcurrentSearch_getStats();
    START_POSTPONED_LEN_ARRAY(num_stats_fields);
    REPLY_WITH_LONG_LONG("totalJobsDone", stats.total_jobs_done, ARRAY_LEN_VAR(num_stats_fields));
    REPLY_WITH_LONG_LONG("totalPendingJobs", stats.total_pending_jobs, ARRAY_LEN_VAR(num_stats_fields));
    END_POSTPONED_LEN_ARRAY(num_stats_fields);
    return REDISMODULE_OK;
  } else {
    return RedisModule_ReplyWithError(ctx, "Invalid argument for 'COORD_THREADS' subcommand");
  }
  return RedisModule_ReplyWithSimpleString(ctx, "OK");
}

DEBUG_COMMAND(DistSearchCommand_DebugWrapper) {
  if (!debugCommandsEnabled(ctx)) {
    return RedisModule_ReplyWithError(ctx, NODEBUG_ERR);
  }
  // at least one debug_param should be provided
  // (1)_FT.DEBUG (2)FT.SEARCH (3)<index> (4)<query> [query_options] (5)[debug_params] (6)DEBUG_PARAMS_COUNT (7)<debug_params_count>
  if (argc < 7) {
    return RedisModule_WrongArity(ctx);
  }

  if (GetNumShards_UnSafe() == 1) {
    // skip _FT.DEBUG
    return DEBUG_RSSearchCommand(ctx, ++argv, --argc);
  }

  return DistSearchCommandImp(ctx, ++argv, --argc, true);
}

DEBUG_COMMAND(DistAggregateCommand_DebugWrapper) {
  if (!debugCommandsEnabled(ctx)) {
    return RedisModule_ReplyWithError(ctx, NODEBUG_ERR);
  }
  // at least one debug_param should be provided
  // (1)_FT.DEBUG (2)FT.AGGREGATE (3)<index> (4)<query> [query_options] (5)[debug_params] (6)DEBUG_PARAMS_COUNT (7)<debug_params_count>
  if (argc < 7) {
    return RedisModule_WrongArity(ctx);
  }

  if (GetNumShards_UnSafe() == 1) {
    // skip _FT.DEBUG
    return DEBUG_RSAggregateCommand(ctx, ++argv, --argc);
  }

  return DistAggregateCommandImp(ctx, ++argv, --argc, true);
}

DEBUG_COMMAND(RSSearchCommandShard) {
  if (!debugCommandsEnabled(ctx)) {
    return RedisModule_ReplyWithError(ctx, NODEBUG_ERR);
  }
  return DEBUG_RSSearchCommand(ctx, ++argv, --argc);
}

DEBUG_COMMAND(RSAggregateCommandShard) {
  if (!debugCommandsEnabled(ctx)) {
    return RedisModule_ReplyWithError(ctx, NODEBUG_ERR);
  }
  return DEBUG_RSAggregateCommand(ctx, ++argv, --argc);
}

DEBUG_COMMAND(RSProfileCommandShard) {
  if (!debugCommandsEnabled(ctx)) {
    return RedisModule_ReplyWithError(ctx, NODEBUG_ERR);
  }
  return RSProfileCommandImp(ctx, ++argv, --argc, true);
}

DEBUG_COMMAND(ProfileCommandCommand_DebugWrapper) {
  if (!debugCommandsEnabled(ctx)) {
    return RedisModule_ReplyWithError(ctx, NODEBUG_ERR);
  }

  // at least one debug_param should be provided
  // (1)_FT.DEBUG (2) FT.PROFILE (3) <index> (4) SEARCH | AGGREGATE [LIMITED] (6) QUERY <query> [query_options] (5) debug_params (6)DEBUG_PARAMS_COUNT (7) <debug_params_count>
  if (argc < 7) {
    return RedisModule_WrongArity(ctx);
  }

  return ProfileCommandHandlerImp(ctx, ++argv, --argc, true);
}

DEBUG_COMMAND(RSShardedHybridCommand_Debug) {
  if (!debugCommandsEnabled(ctx)) {
    return RedisModule_ReplyWithError(ctx, NODEBUG_ERR);
  }
  if (argc < 9) {
    return RedisModule_WrongArity(ctx);
  }
  // Skip _FT.DEBUG prefix — argv now starts at FT.HYBRID / _FT.HYBRID
  ++argv; --argc;

  QueryError status = QueryError_Default();
  HybridDebugParams params = parseHybridDebugParamsCount(argv, argc, &status);
  if (QueryError_HasError(&status)) {
    return QueryError_ReplyAndClear(ctx, &status);
  }
  if (parseHybridDebugParams(&params, &status) != REDISMODULE_OK) {
    return QueryError_ReplyAndClear(ctx, &status);
  }

  // Strip debug params from argc so hybridCommandHandler sees a normal command
  int stripped_argc = argc - (int)params.debug_params_count - 2;
  return hybridCommandHandler(ctx, argv, stripped_argc, true, EXEC_NO_FLAGS, &params);
}

DEBUG_COMMAND(HybridCommand_DebugWrapper) {
  if (!debugCommandsEnabled(ctx)) {
    return RedisModule_ReplyWithError(ctx, NODEBUG_ERR);
  }
  if (argc < 9) {
    // Minimum: _FT.DEBUG FT.HYBRID idx SEARCH query VSIM field vector DEBUG_PARAMS_COUNT count
    return RedisModule_WrongArity(ctx);
  }

  if (GetNumShards_UnSafe() == 1) {
    // Single shard — use standalone handler (skip _FT.DEBUG)
    return DEBUG_hybridCommandHandler(ctx, ++argv, --argc);
  }

  return DistHybridCommandInternal(ctx, ++argv, --argc, true, false /* isProfile */);
}

/**
 * FT.DEBUG BG_SCAN_CONTROLLER SET_MAX_SCANNED_DOCS <max_scanned_docs>
 */
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

/**
 * FT.DEBUG BG_SCAN_CONTROLLER SET_PAUSE_ON_SCANNED_DOCS <pause_scanned_docs>
 */
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

/**
 * FT.DEBUG BG_SCAN_CONTROLLER SET_BG_INDEX_RESUME
 */
DEBUG_COMMAND(setBgIndexResume) {
  if (!debugCommandsEnabled(ctx)) {
    return RedisModule_ReplyWithError(ctx, NODEBUG_ERR);
  }
  if (argc != 2) {
    return RedisModule_WrongArity(ctx);
  }

  globalDebugCtx.bgIndexing.pause = false;

  return RedisModule_ReplyWithSimpleString(ctx, "OK");
}

/**
 * FT.DEBUG BG_SCAN_CONTROLLER GET_DEBUG_SCANNER_STATUS <index_name>
 */
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
    const char *idx = RedisModule_StringPtrLen(argv[2], NULL);
    return RedisModule_ReplyWithErrorFormat(ctx, "%s: %s", QueryError_Strerror(QUERY_ERROR_CODE_NO_INDEX), idx);
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

/**
 * FT.DEBUG BG_SCAN_CONTROLLER SET_PAUSE_BEFORE_SCAN <true/false>
 */
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

/**
 * FT.DEBUG BG_SCAN_CONTROLLER SET_PAUSE_ON_OOM <true/false>
 */
DEBUG_COMMAND(setPauseOnOOM) {
  if (!debugCommandsEnabled(ctx)) {
    return RedisModule_ReplyWithError(ctx, NODEBUG_ERR);
  }
  if (argc != 3) {
    return RedisModule_WrongArity(ctx);
  }
  const char* op = RedisModule_StringPtrLen(argv[2], NULL);

  if (!strcasecmp(op, "true")) {
    globalDebugCtx.bgIndexing.pauseOnOOM = true;
  } else if (!strcasecmp(op, "false")) {
    globalDebugCtx.bgIndexing.pauseOnOOM = false;
  } else {
    return RedisModule_ReplyWithError(ctx, "Invalid argument for 'SET_PAUSE_ON_OOM'");
  }

  validateDebugMode(&globalDebugCtx);

  return RedisModule_ReplyWithSimpleString(ctx, "OK");
}

/**
 * FT.DEBUG BG_SCAN_CONTROLLER TERMINATE_BG_POOL
 */
DEBUG_COMMAND(terminateBgPool) {
  if (!debugCommandsEnabled(ctx)) {
    return RedisModule_ReplyWithError(ctx, NODEBUG_ERR);
  }
  if (argc != 2) {
    return RedisModule_WrongArity(ctx);
  }

  ReindexPool_ThreadPoolDestroy();
  // We do not create a new thread pool here, as it will automatically be created on the next background indexing job

  return RedisModule_ReplyWithSimpleString(ctx, "OK");
}

/**
 * FT.DEBUG BG_SCAN_CONTROLLER SET_PAUSE_BEFORE_OOM_RETRY <true/false>
 */
DEBUG_COMMAND(setPauseBeforeOOMretry) {
  if (!debugCommandsEnabled(ctx)) {
    return RedisModule_ReplyWithError(ctx, NODEBUG_ERR);
  }
  if (argc != 3) {
    return RedisModule_WrongArity(ctx);
  }
  const char* op = RedisModule_StringPtrLen(argv[2], NULL);

  if (!strcasecmp(op, "true")) {
    globalDebugCtx.bgIndexing.pauseBeforeOOMretry = true;
  } else if (!strcasecmp(op, "false")) {
    globalDebugCtx.bgIndexing.pauseBeforeOOMretry = false;
  } else {
    return RedisModule_ReplyWithError(ctx, "Invalid argument for 'SET_PAUSE_BEFORE_OOM_RETRY'");
  }

  validateDebugMode(&globalDebugCtx);

  return RedisModule_ReplyWithSimpleString(ctx, "OK");
}

/**
 * FT.DEBUG BG_SCAN_CONTROLLER DEBUG_SCANNER_UPDATE_CONFIG <index_name>
 */
DEBUG_COMMAND(debugScannerUpdateConfig) {
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
    const char *idx = RedisModule_StringPtrLen(argv[2], NULL);
    return RedisModule_ReplyWithErrorFormat(ctx, "%s: %s", QueryError_Strerror(QUERY_ERROR_CODE_NO_INDEX), idx);
  }

  if (!sp->scanner) {
    return RedisModule_ReplyWithError(ctx, "Scanner is not initialized");
  }

  if(!(sp->scanner->isDebug)) {
    return RedisModule_ReplyWithError(ctx, "Debug mode enabled but scanner is not a debug scanner");
  }

  // Assuming this file is aware of spec.h, via direct or in-direct include
  DebugIndexesScanner *dScanner = (DebugIndexesScanner*)sp->scanner;
  // Update the scanner with the new settings
  dScanner->maxDocsTBscanned = globalDebugCtx.bgIndexing.maxDocsTBscanned;
  dScanner->maxDocsTBscannedPause = globalDebugCtx.bgIndexing.maxDocsTBscannedPause;
  dScanner->wasPaused = false;
  dScanner->pauseOnOOM = globalDebugCtx.bgIndexing.pauseOnOOM;
  dScanner->pauseBeforeOOMRetry = globalDebugCtx.bgIndexing.pauseBeforeOOMretry;

  return RedisModule_ReplyWithSimpleString(ctx, "OK");
}


/**
 * FT.DEBUG BG_SCAN_CONTROLLER <command> [options]
 */
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
  if (!strcmp("SET_PAUSE_ON_OOM", op)) {
    return setPauseOnOOM(ctx, argv+1, argc-1);
  }
  if (!strcmp("TERMINATE_BG_POOL", op)) {
    return terminateBgPool(ctx, argv+1, argc-1);
  }
  if (!strcmp("SET_PAUSE_BEFORE_OOM_RETRY", op)) {
    return setPauseBeforeOOMretry(ctx, argv+1, argc-1);
  }
  if (!strcmp("DEBUG_SCANNER_UPDATE_CONFIG", op)) {
    return debugScannerUpdateConfig(ctx, argv+1, argc-1);
  }
  return RedisModule_ReplyWithError(ctx, "Invalid command for 'BG_SCAN_CONTROLLER'");

}
DEBUG_COMMAND(ListIndexesSwitch) {
  if (!debugCommandsEnabled(ctx)) {
    return RedisModule_ReplyWithError(ctx, NODEBUG_ERR);
  }
  RedisModule_Reply _reply = RedisModule_NewReply(ctx);
  Indexes_List(&_reply, true);
  return REDISMODULE_OK;
}

DEBUG_COMMAND(getHideUserDataFromLogs) {
  if (!debugCommandsEnabled(ctx)) {
    return RedisModule_ReplyWithError(ctx, NODEBUG_ERR);
  }
  const long long value = RSGlobalConfig.hideUserDataFromLog;
  return RedisModule_ReplyWithLongLong(ctx, value);
}

// Global counter for tracking yield calls
typedef struct {
  size_t yieldOnLoadCounter;
  size_t yieldOnBgIndexCounter;
  size_t indexerSleepBeforeYieldMicros;
} YieldCallHandler;

static YieldCallHandler g_yieldCallHandler = {0};

// Function to increment the yield counter upon loading (to be called from IndexerBulkAdd)
void IncrementLoadYieldCounter(void) {
  g_yieldCallHandler.yieldOnLoadCounter++;
}

// Function to increment the yield counter upon bg indexing
void IncrementBgIndexYieldCounter(void) {
  g_yieldCallHandler.yieldOnBgIndexCounter++;
}

// Reset the yield counter
void ResetYieldCounters(void) {
  g_yieldCallHandler.yieldOnLoadCounter = 0;
  g_yieldCallHandler.yieldOnBgIndexCounter = 0;
}

// Get the current sleep time before yielding (in microseconds)
unsigned int GetIndexerSleepBeforeYieldMicros(void) {
  return g_yieldCallHandler.indexerSleepBeforeYieldMicros;
}

/**
 * FT.DEBUG YIELDS_COUNTER LOAD/BG_INDEX/RESET
 * Get or reset the counter for yields indexing / loading operations
 */
DEBUG_COMMAND(YieldCounter) {
  if (!debugCommandsEnabled(ctx)) {
    return RedisModule_ReplyWithError(ctx, NODEBUG_ERR);
  }

  if (argc != 3) {
    return RedisModule_WrongArity(ctx);
  }

  size_t len;
  const char *subCmd = RedisModule_StringPtrLen(argv[2], &len);
  if (STR_EQCASE(subCmd, len, "RESET")) {
    ResetYieldCounters();
    return RedisModule_ReplyWithSimpleString(ctx, "OK");
  }
  if (STR_EQCASE(subCmd, len, "BG_INDEX")) {
    return RedisModule_ReplyWithLongLong(ctx, (long long)g_yieldCallHandler.yieldOnBgIndexCounter);
  }
  if (STR_EQCASE(subCmd, len, "LOAD")) {
    return RedisModule_ReplyWithLongLong(ctx, (long long)g_yieldCallHandler.yieldOnLoadCounter);
  }
  return RedisModule_ReplyWithError(ctx, "Unknown subcommand");
}

/**
 * FT.DEBUG INDEXER_SLEEP_BEFORE_YIELD [<microseconds>]
 * Get or set the sleep time in microseconds before yielding during indexing while loading
 */
DEBUG_COMMAND(IndexerSleepBeforeYieldMicros) {
  if (!debugCommandsEnabled(ctx)) {
    return RedisModule_ReplyWithError(ctx, NODEBUG_ERR);
  }

  if (argc > 3) {
    return RedisModule_WrongArity(ctx);
  }

  // Set new sleep time
  if (argc == 3) {
    long long sleepMicros;
    if (RedisModule_StringToLongLong(argv[2], &sleepMicros) != REDISMODULE_OK || sleepMicros < 0) {
      return RedisModule_ReplyWithError(ctx, "Invalid sleep time. Must be a non-negative integer.");
    }

    g_yieldCallHandler.indexerSleepBeforeYieldMicros = (unsigned int)sleepMicros;
    return RedisModule_ReplyWithSimpleString(ctx, "OK");
  }

  return RedisModule_WrongArity(ctx);
}

/**
 * FT.DEBUG QUERY_CONTROLLER SET_PAUSE_RP_RESUME
 */
DEBUG_COMMAND(setPauseRPResume) {
  if (!debugCommandsEnabled(ctx)) {
    return RedisModule_ReplyWithError(ctx, NODEBUG_ERR);
  }
  if (argc != 2) {
    return RedisModule_WrongArity(ctx);
  }

  if (!QueryDebugCtx_IsPaused()) {
    return RedisModule_ReplyWithError(ctx, "Query is not paused");
  }

  QueryDebugCtx_SetPause(false);

  return RedisModule_ReplyWithSimpleString(ctx, "OK");
}

/**
 * FT.DEBUG QUERY_CONTROLLER GET_IS_RP_PAUSED
 */
DEBUG_COMMAND(getIsRPPaused) {
  if (!debugCommandsEnabled(ctx)) {
    return RedisModule_ReplyWithError(ctx, NODEBUG_ERR);
  }
  if (argc != 2) {
    return RedisModule_WrongArity(ctx);
  }

  return RedisModule_ReplyWithLongLong(ctx, QueryDebugCtx_IsPaused());
}

#ifdef ENABLE_ASSERT
/**
 * FT.DEBUG QUERY_CONTROLLER SET_PAUSE_BEFORE_REDUCE <N>
 * COORD_REDUCE_NO_PAUSE (0): no pause
 * COORD_REDUCE_PAUSE_BEFORE_REDUCER_INIT (-2): pause after acquiring the
 *         REDUCING state but before reducer context setup (used to test the
 *         edge case where the background reducer starts, but a timeout fires
 *         before it can finish setting up req->rctx)
 * COORD_REDUCE_PAUSE_AFTER_LAST_RESULT (-1): pause after the last result is reduced
 * N>0: pause before the Nth result is reduced (1-based)
 */
DEBUG_COMMAND(setPauseBeforeReduce) {
  if (!debugCommandsEnabled(ctx)) {
    return RedisModule_ReplyWithError(ctx, NODEBUG_ERR);
  }
  if (argc != 3) {
    return RedisModule_WrongArity(ctx);
  }

  long long n;
  if (RedisModule_StringToLongLong(argv[2], &n) != REDISMODULE_OK) {
    return RedisModule_ReplyWithError(ctx, "Invalid argument for 'SET_PAUSE_BEFORE_REDUCE'");
  }

  CoordReduceDebugCtx_SetPauseBeforeN((int)n);

  return RedisModule_ReplyWithSimpleString(ctx, "OK");
}

/**
 * FT.DEBUG QUERY_CONTROLLER GET_IS_COORD_REDUCE_PAUSED
 */
DEBUG_COMMAND(getIsCoordReducePaused) {
  if (!debugCommandsEnabled(ctx)) {
    return RedisModule_ReplyWithError(ctx, NODEBUG_ERR);
  }
  if (argc != 2) {
    return RedisModule_WrongArity(ctx);
  }

  return RedisModule_ReplyWithBool(ctx, CoordReduceDebugCtx_IsPaused());
}

/**
 * FT.DEBUG QUERY_CONTROLLER SET_COORD_REDUCE_RESUME
 */
DEBUG_COMMAND(setCoordReduceResume) {
  if (!debugCommandsEnabled(ctx)) {
    return RedisModule_ReplyWithError(ctx, NODEBUG_ERR);
  }
  if (argc != 2) {
    return RedisModule_WrongArity(ctx);
  }

  if (!CoordReduceDebugCtx_IsPaused()) {
    return RedisModule_ReplyWithError(ctx, "Coordinator reduce is not paused");
  }

  CoordReduceDebugCtx_SetPause(false);

  return RedisModule_ReplyWithSimpleString(ctx, "OK");
}

/**
 * FT.DEBUG QUERY_CONTROLLER GET_COORD_REDUCE_COUNT
 */
DEBUG_COMMAND(getCoordReduceCount) {
  if (!debugCommandsEnabled(ctx)) {
    return RedisModule_ReplyWithError(ctx, NODEBUG_ERR);
  }
  if (argc != 2) {
    return RedisModule_WrongArity(ctx);
  }

  return RedisModule_ReplyWithLongLong(ctx, CoordReduceDebugCtx_GetReduceCount());
}

/**
 * FT.DEBUG QUERY_CONTROLLER SET_PAUSE_BEFORE_STORE_RESULTS <true/false>
 * Enable/disable pausing before AREQ_StoreResults/HREQ_StoreResults.
 */
DEBUG_COMMAND(setPauseBeforeStoreResults) {
  if (!debugCommandsEnabled(ctx)) {
    return RedisModule_ReplyWithError(ctx, NODEBUG_ERR);
  }
  if (argc != 3) {
    return RedisModule_WrongArity(ctx);
  }
  const char *op = RedisModule_StringPtrLen(argv[2], NULL);

  if (!strcasecmp(op, "true")) {
    StoreResultsDebugCtx_SetPauseBeforeEnabled(true);
  } else if (!strcasecmp(op, "false")) {
    StoreResultsDebugCtx_SetPauseBeforeEnabled(false);
  } else {
    return RedisModule_ReplyWithError(ctx, "Invalid argument for 'SET_PAUSE_BEFORE_STORE_RESULTS'");
  }

  return RedisModule_ReplyWithSimpleString(ctx, "OK");
}

/**
 * FT.DEBUG QUERY_CONTROLLER SET_PAUSE_AFTER_STORE_RESULTS <true/false>
 * Enable/disable pausing after AREQ_StoreResults/HREQ_StoreResults.
 */
DEBUG_COMMAND(setPauseAfterStoreResults) {
  if (!debugCommandsEnabled(ctx)) {
    return RedisModule_ReplyWithError(ctx, NODEBUG_ERR);
  }
  if (argc != 3) {
    return RedisModule_WrongArity(ctx);
  }
  const char *op = RedisModule_StringPtrLen(argv[2], NULL);

  if (!strcasecmp(op, "true")) {
    StoreResultsDebugCtx_SetPauseAfterEnabled(true);
  } else if (!strcasecmp(op, "false")) {
    StoreResultsDebugCtx_SetPauseAfterEnabled(false);
  } else {
    return RedisModule_ReplyWithError(ctx, "Invalid argument for 'SET_PAUSE_AFTER_STORE_RESULTS'");
  }

  return RedisModule_ReplyWithSimpleString(ctx, "OK");
}

/**
 * FT.DEBUG QUERY_CONTROLLER GET_IS_STORE_RESULTS_PAUSED
 */
DEBUG_COMMAND(getIsStoreResultsPaused) {
  if (!debugCommandsEnabled(ctx)) {
    return RedisModule_ReplyWithError(ctx, NODEBUG_ERR);
  }
  if (argc != 2) {
    return RedisModule_WrongArity(ctx);
  }

  return RedisModule_ReplyWithBool(ctx, StoreResultsDebugCtx_IsPaused());
}

/**
 * FT.DEBUG QUERY_CONTROLLER SET_STORE_RESULTS_RESUME
 */
DEBUG_COMMAND(setStoreResultsResume) {
  if (!debugCommandsEnabled(ctx)) {
    return RedisModule_ReplyWithError(ctx, NODEBUG_ERR);
  }
  if (argc != 2) {
    return RedisModule_WrongArity(ctx);
  }

  if (!StoreResultsDebugCtx_IsPaused()) {
    return RedisModule_ReplyWithError(ctx, "Store results is not paused");
  }

  StoreResultsDebugCtx_SetPause(false);

  return RedisModule_ReplyWithSimpleString(ctx, "OK");
}

/**
 * FT.DEBUG QUERY_CONTROLLER SET_PAUSE_BEFORE_HYBRID_STORE_CURSORS <true/false>
 */
DEBUG_COMMAND(setPauseBeforeHybridStoreCursors) {
  if (!debugCommandsEnabled(ctx)) {
    return RedisModule_ReplyWithError(ctx, NODEBUG_ERR);
  }
  if (argc != 3) {
    return RedisModule_WrongArity(ctx);
  }
  const char *op = RedisModule_StringPtrLen(argv[2], NULL);

  if (!strcasecmp(op, "true")) {
    HybridStoreCursorsDebugCtx_SetPauseBeforeEnabled(true);
  } else if (!strcasecmp(op, "false")) {
    HybridStoreCursorsDebugCtx_SetPauseBeforeEnabled(false);
  } else {
    return RedisModule_ReplyWithError(ctx, "Invalid argument");
  }

  return RedisModule_ReplyWithSimpleString(ctx, "OK");
}

/**
 * FT.DEBUG QUERY_CONTROLLER SET_PAUSE_AFTER_HYBRID_STORE_CURSORS <true/false>
 */
DEBUG_COMMAND(setPauseAfterHybridStoreCursors) {
  if (!debugCommandsEnabled(ctx)) {
    return RedisModule_ReplyWithError(ctx, NODEBUG_ERR);
  }
  if (argc != 3) {
    return RedisModule_WrongArity(ctx);
  }
  const char *op = RedisModule_StringPtrLen(argv[2], NULL);

  if (!strcasecmp(op, "true")) {
    HybridStoreCursorsDebugCtx_SetPauseAfterEnabled(true);
  } else if (!strcasecmp(op, "false")) {
    HybridStoreCursorsDebugCtx_SetPauseAfterEnabled(false);
  } else {
    return RedisModule_ReplyWithError(ctx, "Invalid argument");
  }

  return RedisModule_ReplyWithSimpleString(ctx, "OK");
}

/**
 * FT.DEBUG QUERY_CONTROLLER GET_IS_HYBRID_STORE_CURSORS_PAUSED
 */
DEBUG_COMMAND(getIsHybridStoreCursorsPaused) {
  if (!debugCommandsEnabled(ctx)) {
    return RedisModule_ReplyWithError(ctx, NODEBUG_ERR);
  }
  if (argc != 2) {
    return RedisModule_WrongArity(ctx);
  }

  return RedisModule_ReplyWithBool(ctx, HybridStoreCursorsDebugCtx_IsPaused());
}

/**
 * FT.DEBUG QUERY_CONTROLLER SET_HYBRID_STORE_CURSORS_RESUME
 */
DEBUG_COMMAND(setHybridStoreCursorsResume) {
  if (!debugCommandsEnabled(ctx)) {
    return RedisModule_ReplyWithError(ctx, NODEBUG_ERR);
  }
  if (argc != 2) {
    return RedisModule_WrongArity(ctx);
  }

  if (!HybridStoreCursorsDebugCtx_IsPaused()) {
    return RedisModule_ReplyWithError(ctx, "Hybrid store cursors is not paused");
  }

  HybridStoreCursorsDebugCtx_SetPause(false);

  return RedisModule_ReplyWithSimpleString(ctx, "OK");
}
#endif

/**
 * FT.DEBUG QUERY_CONTROLLER PRINT_RP_STREAM
 */
DEBUG_COMMAND(printRPStream) {
  if (!debugCommandsEnabled(ctx)) {
    return RedisModule_ReplyWithError(ctx, NODEBUG_ERR);
  }
  if (argc != 2) {
    return RedisModule_WrongArity(ctx);
  }

  if (!QueryDebugCtx_HasDebugRP()) {
    return RedisModule_ReplyWithError(ctx, "No debug RP is set");
  }

  ResultProcessor* root = QueryDebugCtx_GetDebugRP()->parent->endProc;
  ResultProcessor *cur = root;

  RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);


  size_t resultSize = 0;

  while (cur) {
    if (cur->type < RP_MAX) {
      RedisModule_ReplyWithSimpleString(ctx, RPTypeToString(cur->type));
    }
    else {
      RedisModule_ReplyWithSimpleString(ctx, "DEBUG_RP");
    }
    cur = cur->upstream;
    resultSize++;
  }
  RedisModule_ReplySetArrayLength(ctx, resultSize);

  return REDISMODULE_OK;
}

#ifdef ENABLE_ASSERT
// Subcommand constants for SYNC_POINT
#define SYNC_POINT_SUBCMD_ARM        "ARM"
#define SYNC_POINT_SUBCMD_SIGNAL     "SIGNAL"
#define SYNC_POINT_SUBCMD_IS_WAITING "IS_WAITING"
#define SYNC_POINT_SUBCMD_IS_ARMED   "IS_ARMED"
#define SYNC_POINT_SUBCMD_CLEAR      "CLEAR"

/**
 * FT.DEBUG SYNC_POINT <subcommand> [point_name]
 *
 * Subcommands:
 *   ARM <name>        - Enable a sync point (queries will pause when reaching it)
 *   SIGNAL <name>     - Resume execution at a sync point
 *   IS_WAITING <name> - Check if a query is paused at a sync point
 *   IS_ARMED <name>   - Check if a sync point is armed
 *   CLEAR             - Reset all sync points
 */
DEBUG_COMMAND(syncPoint) {
  if (!debugCommandsEnabled(ctx)) return RedisModule_ReplyWithError(ctx, NODEBUG_ERR);
  // argc layout: FT.DEBUG SYNC_POINT <subcommand> [<point_name>]
  // argv[0] = FT.DEBUG, argv[1] = SYNC_POINT, argv[2] = subcommand, argv[3] = point_name
  if (argc < 3) return RedisModule_WrongArity(ctx);

  const char *subOp = RedisModule_StringPtrLen(argv[2], NULL);

  if (!strcmp(SYNC_POINT_SUBCMD_ARM, subOp)) {
    if (argc != 4) return RedisModule_WrongArity(ctx);
    const char *name = RedisModule_StringPtrLen(argv[3], NULL);
    if (!SyncPoint_Arm(name)) {
      return RedisModule_ReplyWithError(ctx, "ERR max sync points reached");
    }
    return RedisModule_ReplyWithSimpleString(ctx, "OK");
  }
  if (!strcmp(SYNC_POINT_SUBCMD_SIGNAL, subOp)) {
    if (argc != 4) return RedisModule_WrongArity(ctx);
    const char *name = RedisModule_StringPtrLen(argv[3], NULL);
    SyncPoint_Signal(name);
    return RedisModule_ReplyWithSimpleString(ctx, "OK");
  }
  if (!strcmp(SYNC_POINT_SUBCMD_IS_WAITING, subOp)) {
    if (argc != 4) return RedisModule_WrongArity(ctx);
    const char *name = RedisModule_StringPtrLen(argv[3], NULL);
    return RedisModule_ReplyWithBool(ctx, SyncPoint_IsWaiting(name));
  }
  if (!strcmp(SYNC_POINT_SUBCMD_IS_ARMED, subOp)) {
    if (argc != 4) return RedisModule_WrongArity(ctx);
    const char *name = RedisModule_StringPtrLen(argv[3], NULL);
    return RedisModule_ReplyWithBool(ctx, SyncPoint_IsArmed(name));
  }
  if (!strcmp(SYNC_POINT_SUBCMD_CLEAR, subOp)) {
    SyncPoint_ClearAll();
    return RedisModule_ReplyWithSimpleString(ctx, "OK");
  }
  return RedisModule_ReplyWithError(ctx, "Unknown SYNC_POINT subcommand. Valid: ARM, SIGNAL, IS_WAITING, IS_ARMED, CLEAR");
}

/**
 * FT.DEBUG QUERY_CONTROLLER SET_CURSOR_READ_SIZE <N>
 * Override RSGlobalConfig.cursorReadSize at runtime. Returns the previous
 * value so the caller can restore it. N must be >= 1.
 */
DEBUG_COMMAND(setCursorReadSize) {
  if (!debugCommandsEnabled(ctx)) {
    return RedisModule_ReplyWithError(ctx, NODEBUG_ERR);
  }
  if (argc != 3) {
    return RedisModule_WrongArity(ctx);
  }
  long long n;
  if (RedisModule_StringToLongLong(argv[2], &n) != REDISMODULE_OK || n < 1) {
    return RedisModule_ReplyWithError(ctx, "Invalid argument for 'SET_CURSOR_READ_SIZE'");
  }

  long long previous = RSGlobalConfig.cursorReadSize;
  RSGlobalConfig.cursorReadSize = n;
  return RedisModule_ReplyWithLongLong(ctx, previous);
}
#endif

/**
 * FT.DEBUG QUERY_CONTROLLER <command> [options]
 */
DEBUG_COMMAND(queryController) {
  if (!debugCommandsEnabled(ctx)) {
    return RedisModule_ReplyWithError(ctx, NODEBUG_ERR);
  }
  if (argc < 3) {
    return RedisModule_WrongArity(ctx);
  }
  const char *op = RedisModule_StringPtrLen(argv[2], NULL);

  // Query pause RP commands
  if (!strcmp("SET_PAUSE_RP_RESUME", op)) {
    return setPauseRPResume(ctx, argv + 1, argc - 1);
  }
  if (!strcmp("GET_IS_RP_PAUSED", op)) {
    return getIsRPPaused(ctx, argv + 1, argc - 1);
  }
  if (!strcmp("PRINT_RP_STREAM", op)) {
    return printRPStream(ctx, argv + 1, argc - 1);
  }
#ifdef ENABLE_ASSERT
  if (!strcmp("SET_CURSOR_READ_SIZE", op)) {
    return setCursorReadSize(ctx, argv + 1, argc - 1);
  }
  // Coordinator reduce pause commands (only available with ENABLE_ASSERT)
  if (!strcmp("SET_PAUSE_BEFORE_REDUCE", op)) {
    return setPauseBeforeReduce(ctx, argv + 1, argc - 1);
  }
  if (!strcmp("GET_IS_COORD_REDUCE_PAUSED", op)) {
    return getIsCoordReducePaused(ctx, argv + 1, argc - 1);
  }
  if (!strcmp("SET_COORD_REDUCE_RESUME", op)) {
    return setCoordReduceResume(ctx, argv + 1, argc - 1);
  }
  if (!strcmp("GET_COORD_REDUCE_COUNT", op)) {
    return getCoordReduceCount(ctx, argv + 1, argc - 1);
  }
  // Store results pause commands
  if (!strcmp("SET_PAUSE_BEFORE_STORE_RESULTS", op)) {
    return setPauseBeforeStoreResults(ctx, argv + 1, argc - 1);
  }
  if (!strcmp("SET_PAUSE_AFTER_STORE_RESULTS", op)) {
    return setPauseAfterStoreResults(ctx, argv + 1, argc - 1);
  }
  if (!strcmp("GET_IS_STORE_RESULTS_PAUSED", op)) {
    return getIsStoreResultsPaused(ctx, argv + 1, argc - 1);
  }
  if (!strcmp("SET_STORE_RESULTS_RESUME", op)) {
    return setStoreResultsResume(ctx, argv + 1, argc - 1);
  }
  if (!strcmp("SET_PAUSE_BEFORE_HYBRID_STORE_CURSORS", op)) {
    return setPauseBeforeHybridStoreCursors(ctx, argv + 1, argc - 1);
  }
  if (!strcmp("SET_PAUSE_AFTER_HYBRID_STORE_CURSORS", op)) {
    return setPauseAfterHybridStoreCursors(ctx, argv + 1, argc - 1);
  }
  if (!strcmp("GET_IS_HYBRID_STORE_CURSORS_PAUSED", op)) {
    return getIsHybridStoreCursorsPaused(ctx, argv + 1, argc - 1);
  }
  if (!strcmp("SET_HYBRID_STORE_CURSORS_RESUME", op)) {
    return setHybridStoreCursorsResume(ctx, argv + 1, argc - 1);
  }
#endif
  return RedisModule_ReplyWithError(ctx, "Invalid command for 'QUERY_CONTROLLER'");
}


/**
 * FT.DEBUG DUMP_SCHEMA <index>
 * Dump the schema of the index in a serialized format.
 * Returns an array with two elements:
 * 1. The serialized schema string.
 * 2. The version of the index at the time of serialization.
 */
DEBUG_COMMAND(DumpSchema) {
  if (!debugCommandsEnabled(ctx)) {
    return RedisModule_ReplyWithError(ctx, NODEBUG_ERR);
  }
  if (argc != 3) {
    return RedisModule_WrongArity(ctx);
  }
  GET_SEARCH_CTX(argv[2]);

  RedisModuleString *schemaStr = IndexSpec_Serialize(sctx->spec);
  SearchCtx_Free(sctx);

  if (!schemaStr) return RedisModule_ReplyWithError(ctx, "Failed to serialize schema");

  RedisModule_ReplyWithArray(ctx, 2);
  RedisModule_ReplyWithString(ctx, schemaStr);
  RedisModule_ReplyWithLongLong(ctx, INDEX_CURRENT_VERSION);
  RedisModule_FreeString(NULL, schemaStr);
  return REDISMODULE_OK;
}

static inline int TimedOut_Always(TimeoutCtx *ctx) {
  (void)ctx; // Unused parameter
  return TIMED_OUT;
}

// Global timeout callback for VecSim searches.
// Need the redirection so tests can pass a mock function to test timeout behavior.
// Used in hybrid_reader.c in computeDistances
extern int (*vecsimTimeoutCallback)(TimeoutCtx *ctx);

/**
 * FT.DEBUG VECSIM_MOCK_TIMEOUT <enable|disable>
 * Set the timeout callback for VecSim searches globally
 * enable - will cause an immediate timeout for all VecSim searches
 * disable - will remove the timeout callback and restore normal behavior
 */
DEBUG_COMMAND(VecSimMockTimeout) {
  if (!debugCommandsEnabled(ctx)) {
    return RedisModule_ReplyWithError(ctx, NODEBUG_ERR);
  }
  if (argc != 3) {
    return RedisModule_WrongArity(ctx);
  }
  const char *op = RedisModule_StringPtrLen(argv[2], NULL);
  if (!strcmp("enable", op)) {
    vecsimTimeoutCallback = TimedOut_Always;
    VecSim_SetTimeoutCallbackFunction((timeoutCallbackFunction)TimedOut_Always);
    return RedisModule_ReplyWithSimpleString(ctx, "OK");
  } else if (!strcmp("disable", op)) {
    vecsimTimeoutCallback = TimedOut_WithCtx;
    VecSim_SetTimeoutCallbackFunction((timeoutCallbackFunction)TimedOut_WithCtx);
    return RedisModule_ReplyWithSimpleString(ctx, "OK");
  } else {
    return RedisModule_ReplyWithError(ctx, "Invalid command for 'VECSIM_MOCK_TIMEOUT'");
  }
}

/**
 * FT.DEBUG DISK_IO_CONTROL <enable|disable|status>
 *
 * Control async disk I/O behavior for testing and debugging.
 * - enable: Enable async I/O (default)
 * - disable: Disable async I/O, use sync path instead
 * - status: Show current async I/O status
 */
DEBUG_COMMAND(DiskIOControl) {
  if (!debugCommandsEnabled(ctx)) {
    return RedisModule_ReplyWithError(ctx, NODEBUG_ERR);
  }
  if (argc != 3) {
    return RedisModule_WrongArity(ctx);
  }

  const char *op = RedisModule_StringPtrLen(argv[2], NULL);

  if (!strcasecmp("enable", op)) {
    // Check if disk is available first
    if (!SearchDisk_IsAsyncIOSupported()) {
      return RedisModule_ReplyWithError(ctx, "Async I/O is not supported (disk API not available or disk doesn't support async I/O)");
    }
    SearchDisk_SetAsyncIOEnabled(true);
    return RedisModule_ReplyWithSimpleString(ctx, "OK - Async I/O enabled");
  } else if (!strcasecmp("disable", op)) {
    SearchDisk_SetAsyncIOEnabled(false);
    return RedisModule_ReplyWithSimpleString(ctx, "OK - Async I/O disabled");
  } else if (!strcasecmp("status", op)) {
    bool flagEnabled = SearchDisk_GetAsyncIOEnabled();
    bool diskSupported = SearchDisk_IsAsyncIOSupported();

    if (!diskSupported) {
      return RedisModule_ReplyWithSimpleString(ctx, "Async I/O: not supported by disk");
    }
    return RedisModule_ReplyWithSimpleString(ctx, flagEnabled ? "Async I/O: enabled" : "Async I/O: disabled");
  } else {
    return RedisModule_ReplyWithError(ctx, "Invalid command for 'DISK_IO_CONTROL'. Use: enable, disable, or status");
  }
}

// FT.DEBUG GET_MAX_DOC_ID INDEX_NAME
DEBUG_COMMAND(GetMaxDocId) {
  if (!debugCommandsEnabled(ctx)) {
    return RedisModule_ReplyWithError(ctx, NODEBUG_ERR);
  }
  if (argc != 3) {
    return RedisModule_WrongArity(ctx);
  }
  GET_SEARCH_CTX(argv[2])

  t_docId maxDocId;
  if (sctx->spec->diskSpec) {
    maxDocId = SearchDisk_GetMaxDocId(sctx->spec->diskSpec);
  } else {
    maxDocId = DocTable_GetMaxDocId(&sctx->spec->docs);
  }

  RedisModule_ReplyWithLongLong(sctx->redisCtx, maxDocId);
  SearchCtx_Free(sctx);
  return REDISMODULE_OK;
}

// FT.DEBUG DUMP_DELETED_IDS INDEX_NAME
DEBUG_COMMAND(DumpDeletedIds) {
  if (!debugCommandsEnabled(ctx)) {
    return RedisModule_ReplyWithError(ctx, NODEBUG_ERR);
  }
  if (argc != 3) {
    return RedisModule_WrongArity(ctx);
  }
  GET_SEARCH_CTX(argv[2])

  if (sctx->spec->diskSpec) {
    // Disk-based index
    uint64_t count = SearchDisk_GetDeletedIdsCount(sctx->spec->diskSpec);

    if (count == 0) {
      RedisModule_ReplyWithEmptyArray(sctx->redisCtx);
    } else {
      t_docId *buffer = rm_malloc(count * sizeof(t_docId));
      if (!buffer) {
        RedisModule_ReplyWithError(sctx->redisCtx, "Out of memory");
        SearchCtx_Free(sctx);
        return REDISMODULE_OK;
      }

      // Note: There is a TOCTOU window between obtaining `count` and fetching
      // the IDs.
      // This command is for debugging only, so it is acceptable if some IDs are
      // missed or added between these calls. `count` is treated as a hard upper
      // bound on the number of IDs written into `buffer`.
      size_t actual_count = SearchDisk_GetDeletedIds(sctx->spec->diskSpec, buffer, count);
      if (actual_count > count) {
        // Clamp to buffer capacity to avoid reading beyond the allocated array
        actual_count = count;
      }
      RedisModule_ReplyWithArray(sctx->redisCtx, actual_count);
      for (size_t i = 0; i < actual_count; i++) {
        RedisModule_ReplyWithLongLong(sctx->redisCtx, buffer[i]);
      }
      rm_free(buffer);
    }
  } else {
    // In-memory index - we do not hold a deleted-ids set here, so we return an empty array
    RedisModule_ReplyWithEmptyArray(sctx->redisCtx);
  }

  SearchCtx_Free(sctx);
  return REDISMODULE_OK;
}

/**
 * FT.DEBUG REGISTER_TEST_SCORERS
 * Register the test scorers for testing purposes.
 * Registers: TEST_NUM_DOCS, TEST_NUM_TERMS, TEST_AVG_DOC_LEN, TEST_SUM_IDF, TEST_SUM_BM25_IDF
 */
DEBUG_COMMAND(RegisterTestScorers) {
  if (!debugCommandsEnabled(ctx)) {
    return RedisModule_ReplyWithError(ctx, NODEBUG_ERR);
  }
  if (argc != 2) {
    return RedisModule_WrongArity(ctx);
  }

  int result = Ext_RegisterTestScorers();
  if (result == REDISEARCH_OK) {
    return RedisModule_ReplyWithSimpleString(ctx, "OK");
  } else {
    return RedisModule_ReplyWithError(ctx, "Scorer already registered or registration failed");
  }
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
                               {"DISK_FLUSH", DiskFlush},
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
                               {"COORD_THREADS", CoordThreadsSwitch},
                               {"BG_SCAN_CONTROLLER", bgScanController},
                               {"INDEXES", ListIndexesSwitch},
                               {"INFO", IndexObfuscatedInfo},
                               {"GET_HIDE_USER_DATA_FROM_LOGS", getHideUserDataFromLogs},
                               {"YIELDS_COUNTER", YieldCounter},
                               {"INDEXER_SLEEP_BEFORE_YIELD_MICROS", IndexerSleepBeforeYieldMicros},
                               {"QUERY_CONTROLLER", queryController},
                               {"DUMP_SCHEMA", DumpSchema},
                               {"VECSIM_MOCK_TIMEOUT", VecSimMockTimeout},
                               {"GET_MAX_DOC_ID", GetMaxDocId},
                               {"DUMP_DELETED_IDS", DumpDeletedIds},
                               {"DISK_IO_CONTROL", DiskIOControl},
                               {"REGISTER_TEST_SCORERS", RegisterTestScorers}, // Register test scorers
                               /**
                                * The following commands are for debugging distributed search/aggregation.
                                */
                               {"FT.AGGREGATE", DistAggregateCommand_DebugWrapper},
                               {"_FT.AGGREGATE", RSAggregateCommandShard}, // internal use only, in SA use FT.AGGREGATE
                               {"FT.SEARCH", DistSearchCommand_DebugWrapper},
                               {"_FT.SEARCH", RSSearchCommandShard}, // internal use only, in SA use FT.SEARCH
                               {"FT.HYBRID", HybridCommand_DebugWrapper},
                               {"_FT.HYBRID", RSShardedHybridCommand_Debug},
                               {"FT.PROFILE", ProfileCommandCommand_DebugWrapper},
                               {"_FT.PROFILE", RSProfileCommandShard},
                               /* IMPORTANT NOTE: Every debug command starts with
                                * checking if redis allows this context to execute
                                * debug commands by calling `debugCommandsEnabled(ctx)`.
                                * If you add a new debug command, make sure to add it.
                               */
                               {NULL, NULL}};

#ifdef ENABLE_ASSERT
// Debug commands only available with ENABLE_ASSERT (debug/test builds)
// Add new assert-only commands to this array instead of hard-coding #ifdef blocks
static DebugCommandType assertOnlyCommands[] = {
    {"SYNC_POINT", syncPoint},
    {NULL, NULL}};
#endif

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
#ifdef ENABLE_ASSERT
  for (DebugCommandType *c = &assertOnlyCommands[0]; c->name != NULL; c++) {
    RedisModule_ReplyWithCString(ctx, c->name);
    ++len;
  }
#endif
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
#ifdef ENABLE_ASSERT
  for (DebugCommandType *c = &assertOnlyCommands[0]; c->name != NULL; c++) {
    int rc = RedisModule_CreateSubcommand(debugCommand, c->name, c->callback,
              IsEnterprise() ? "readonly " CMD_PROXY_FILTERED : "readonly",
              RS_DEBUG_FLAGS);
    if (rc != REDISMODULE_OK) return rc;
  }
#endif
  return RedisModule_CreateSubcommand(debugCommand, "HELP", DebugHelpCommand,
          IsEnterprise() ? "readonly " CMD_PROXY_FILTERED : "readonly",
          RS_DEBUG_FLAGS);
}

#if (defined(DEBUG) || defined(_DEBUG)) && !defined(NDEBUG)
#include "readies/cetara/diag/gdb.c"
#endif
