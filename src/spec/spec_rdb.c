/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#include "spec_rdb.h"
#include "spec.h"
#include "spec_field_parse.h"

#include "rmalloc.h"
#include "rmutil/rm_assert.h"
#include "rdb.h"
#include "config.h"
#include "cursor.h"
#include "alias.h"
#include "module.h"
#include "rules.h"
#include "commands.h"
#include "notifications.h"
#include "redis_index.h"
#include "search_ctx.h"
#include "trie/trie_type.h"
#include "vector_index.h"
#include "util/misc.h"
#include "util/logging.h"
#include "info/global_stats.h"
#include "info/field_spec_info.h"
#include "obfuscation/hidden.h"
#include "obfuscation/obfuscation_api.h"
#include "search_disk.h"
#include "search_disk_utils.h"
#include "suffix.h"

#define INITIAL_DOC_TABLE_SIZE 1000

extern RedisModuleCtx *RSDummyContext;
extern RedisModuleType *IndexSpecType;
extern dict *specDict_g;
extern dict *legacySpecDict;
extern dict *legacySpecRules;

// Internal helpers from spec.c
extern void initializeIndexSpec(IndexSpec *sp, const HiddenString *name, IndexFlags flags,
                                int16_t numFields);
extern void IndexSpec_InitLock(IndexSpec *sp);
extern void addPendingIndexDrop(void);
extern void Cursors_initSpec(IndexSpec *spec);

// Forward declaration
static inline bool isSpecOnDisk(const IndexSpec *sp);

bool CheckRdbSstPersistence(RedisModuleCtx *ctx, const char* prefix) {
  const bool useSst = IS_SST_RDB_IN_PROCESS(ctx);
  RedisModule_Log(ctx, "notice", "%s, SST persistence: %s", prefix, useSst ? "true" : "false");
  return useSst;
}

static int bit(t_fieldMask id) {
  for (int i = 0; i < sizeof(t_fieldMask) * 8; i++) {
    if (((id >> i) & 1) == 1) {
      return i;
    }
  }
  return 0;
}

///////////////////////////////////////////////////////////////////////////////////////////////

// Backwards compat version of load for rdbs with version < 8
static int FieldSpec_RdbLoadCompat8(RedisModuleIO *rdb, FieldSpec *f, int encver) {
  char* name = NULL;
  size_t len = 0;
  LoadStringBufferAlloc_IOErrors(rdb, name, &len, true, goto fail);
  f->fieldName = NewHiddenString(name, len, true);
  // the old versions encoded the bit id of the field directly
  // we convert that to a power of 2
  if (encver < INDEX_MIN_WIDESCHEMA_VERSION) {
    f->ftId = bit(LoadUnsigned_IOError(rdb, goto fail));
  } else {
    // the new version encodes just the power of 2 of the bit
    f->ftId = LoadUnsigned_IOError(rdb, goto fail);
  }
  f->types = LoadUnsigned_IOError(rdb, goto fail);
  f->ftWeight = LoadDouble_IOError(rdb, goto fail);
  f->tagOpts.tagFlags = TAG_FIELD_DEFAULT_FLAGS;
  f->tagOpts.tagSep = TAG_FIELD_DEFAULT_HASH_SEP;
  if (encver >= 4) {
    f->options = LoadUnsigned_IOError(rdb, goto fail);
    f->sortIdx = LoadSigned_IOError(rdb, goto fail);
  }
  return REDISMODULE_OK;

fail:
  return REDISMODULE_ERR;
}

static void FieldSpec_RdbSave(RedisModuleIO *rdb, FieldSpec *f) {
  HiddenString_SaveToRdb(f->fieldName, rdb);
  if (HiddenString_Compare(f->fieldPath, f->fieldName) != 0) {
    RedisModule_SaveUnsigned(rdb, 1);
    HiddenString_SaveToRdb(f->fieldPath, rdb);
  } else {
    RedisModule_SaveUnsigned(rdb, 0);
  }
  RedisModule_SaveUnsigned(rdb, f->types);
  RedisModule_SaveUnsigned(rdb, f->options);
  RedisModule_SaveSigned(rdb, f->sortIdx);
  // Save text specific options
  if (FIELD_IS(f, INDEXFLD_T_FULLTEXT) || (f->options & FieldSpec_Dynamic)) {
    RedisModule_SaveUnsigned(rdb, f->ftId);
    RedisModule_SaveDouble(rdb, f->ftWeight);
  }
  if (FIELD_IS(f, INDEXFLD_T_TAG) || (f->options & FieldSpec_Dynamic)) {
    RedisModule_SaveUnsigned(rdb, f->tagOpts.tagFlags);
    RedisModule_SaveStringBuffer(rdb, &f->tagOpts.tagSep, 1);
  }
  if (FIELD_IS(f, INDEXFLD_T_VECTOR)) {
    RedisModule_SaveUnsigned(rdb, f->vectorOpts.expBlobSize);
    VecSim_RdbSave(rdb, &f->vectorOpts.vecSimParams);
  }
  if (FIELD_IS(f, INDEXFLD_T_GEOMETRY) || (f->options & FieldSpec_Dynamic)) {
    RedisModule_SaveUnsigned(rdb, f->geometryOpts.geometryCoords);
  }
}

static const FieldType fieldTypeMap[] = {[IDXFLD_LEGACY_FULLTEXT] = INDEXFLD_T_FULLTEXT,
                                         [IDXFLD_LEGACY_NUMERIC] = INDEXFLD_T_NUMERIC,
                                         [IDXFLD_LEGACY_GEO] = INDEXFLD_T_GEO,
                                         [IDXFLD_LEGACY_TAG] = INDEXFLD_T_TAG};
                                         // CHECKED: Not related to new data types - legacy code

static int FieldSpec_RdbLoad(RedisModuleIO *rdb, FieldSpec *f, StrongRef sp_ref, int encver) {

  f->ftId = RS_INVALID_FIELD_ID;
  // Fall back to legacy encoding if needed
  if (encver < INDEX_MIN_TAGFIELD_VERSION) {
    return FieldSpec_RdbLoadCompat8(rdb, f, encver);
  }

  char* name = NULL;
  size_t len = 0;
  LoadStringBufferAlloc_IOErrors(rdb, name, &len, true, goto fail);
  f->fieldName = NewHiddenString(name, len, false);
  f->fieldPath = f->fieldName;
  if (encver >= INDEX_JSON_VERSION) {
    if (LoadUnsigned_IOError(rdb, goto fail) == 1) {
      LoadStringBufferAlloc_IOErrors(rdb, name, &len, true, goto fail);
      f->fieldPath = NewHiddenString(name, len, false);
    }
  }

  f->types = LoadUnsigned_IOError(rdb, goto fail);
  f->options = LoadUnsigned_IOError(rdb, goto fail);
  f->sortIdx = LoadSigned_IOError(rdb, goto fail);

  if (encver < INDEX_MIN_MULTITYPE_VERSION) {
    RS_LOG_ASSERT(f->types <= IDXFLD_LEGACY_MAX, "field type should be string or numeric");
    f->types = fieldTypeMap[f->types];
  }

  // Load text specific options
  if (FIELD_IS(f, INDEXFLD_T_FULLTEXT) || (f->options & FieldSpec_Dynamic)) {
    f->ftId = LoadUnsigned_IOError(rdb, goto fail);
    f->ftWeight = LoadDouble_IOError(rdb, goto fail);
  }
  // Load tag specific options
  if (FIELD_IS(f, INDEXFLD_T_TAG) || (f->options & FieldSpec_Dynamic)) {
    f->tagOpts.tagFlags = LoadUnsigned_IOError(rdb, goto fail);
    // Load the separator
    size_t l;
    char *s = LoadStringBuffer_IOError(rdb, &l, goto fail);
    RS_LOG_ASSERT(l == 1, "buffer length should be 1");
    f->tagOpts.tagSep = *s;
    RedisModule_Free(s);
  }
  // Load vector specific options
  if (encver >= INDEX_VECSIM_VERSION && FIELD_IS(f, INDEXFLD_T_VECTOR)) {
    if (encver >= INDEX_VECSIM_2_VERSION) {
      f->vectorOpts.expBlobSize = LoadUnsigned_IOError(rdb, goto fail);
    }
    if (encver >= INDEX_VECSIM_SVS_VAMANA_VERSION) {
      if (VecSim_RdbLoad_v4(rdb, &f->vectorOpts.vecSimParams, sp_ref, HiddenString_GetUnsafe(f->fieldName, NULL)) != REDISMODULE_OK) {
        goto fail;
      }
    } else if (encver >= INDEX_VECSIM_TIERED_VERSION) {
      if (VecSim_RdbLoad_v3(rdb, &f->vectorOpts.vecSimParams, sp_ref, HiddenString_GetUnsafe(f->fieldName, NULL)) != REDISMODULE_OK) {
        goto fail;
      }
    } else {
      if (encver >= INDEX_VECSIM_MULTI_VERSION) {
        if (VecSim_RdbLoad_v2(rdb, &f->vectorOpts.vecSimParams) != REDISMODULE_OK) {
          goto fail;
        }
      } else {
        if (VecSim_RdbLoad(rdb, &f->vectorOpts.vecSimParams) != REDISMODULE_OK) {
          goto fail;
        }
      }
      // If we're loading an old (< INDEX_VECSIM_TIERED_VERSION) rdb, we need to convert an HNSW
      // index to a tiered index.
      VecSimLogCtx *logCtx = rm_new(VecSimLogCtx);
      logCtx->index_field_name = HiddenString_GetUnsafe(f->fieldName, NULL);
      f->vectorOpts.vecSimParams.logCtx = logCtx;
      if (f->vectorOpts.vecSimParams.algo == VecSimAlgo_HNSWLIB) {
        VecSimParams hnswParams = f->vectorOpts.vecSimParams;

        f->vectorOpts.vecSimParams.algo = VecSimAlgo_TIERED;
        VecSim_TieredParams_Init(&f->vectorOpts.vecSimParams.algoParams.tieredParams, sp_ref);
        f->vectorOpts.vecSimParams.algoParams.tieredParams.specificParams.tieredHnswParams.swapJobThreshold = 0;
        memcpy(f->vectorOpts.vecSimParams.algoParams.tieredParams.primaryIndexParams, &hnswParams, sizeof(VecSimParams));
      }
    }
    // Calculate blob size limitation on lower encvers.
    if (encver < INDEX_VECSIM_2_VERSION) {
      switch (f->vectorOpts.vecSimParams.algo) {
      case VecSimAlgo_HNSWLIB:
        f->vectorOpts.expBlobSize = f->vectorOpts.vecSimParams.algoParams.hnswParams.dim * VecSimType_sizeof(f->vectorOpts.vecSimParams.algoParams.hnswParams.type);
        break;
      case VecSimAlgo_BF:
        f->vectorOpts.expBlobSize = f->vectorOpts.vecSimParams.algoParams.bfParams.dim * VecSimType_sizeof(f->vectorOpts.vecSimParams.algoParams.bfParams.type);
        break;
      case VecSimAlgo_TIERED:
        if (f->vectorOpts.vecSimParams.algoParams.tieredParams.primaryIndexParams->algo == VecSimAlgo_HNSWLIB) {
          f->vectorOpts.expBlobSize = f->vectorOpts.vecSimParams.algoParams.tieredParams.primaryIndexParams->algoParams.hnswParams.dim * VecSimType_sizeof(f->vectorOpts.vecSimParams.algoParams.tieredParams.primaryIndexParams->algoParams.hnswParams.type);
        } else if (f->vectorOpts.vecSimParams.algoParams.tieredParams.primaryIndexParams->algo == VecSimAlgo_SVS) {
          goto fail;  // svs is not supported in old encvers
        }
        break;
      case VecSimAlgo_SVS:
        goto fail;  // svs is not supported in old encvers
      }
    }
  }

  // Load geometry specific options
  if (FIELD_IS(f, INDEXFLD_T_GEOMETRY) || (f->options & FieldSpec_Dynamic)) {
    if (encver >= INDEX_GEOMETRY_VERSION) {
      f->geometryOpts.geometryCoords = LoadUnsigned_IOError(rdb, goto fail);
    } else {
      // In RedisSearch RC (2.8.1 - 2.8.3) we supported default coordinate system which was not written to RDB
      f->geometryOpts.geometryCoords = GEOMETRY_COORDS_Cartesian;
    }
  }

  return REDISMODULE_OK;

fail:
  return REDISMODULE_ERR;
}

static void IndexScoringStats_RdbLoad(RedisModuleIO *rdb, ScoringIndexStats *stats, int encver) {
  stats->numDocuments = RedisModule_LoadUnsigned(rdb);
  stats->numTerms = RedisModule_LoadUnsigned(rdb);
  if (encver >= INDEX_DISK_VERSION) {
    stats->totalDocsLen = RedisModule_LoadUnsigned(rdb);
  } else {
    stats->totalDocsLen = 0;
  }
}

static void IndexScoringStats_RdbSave(RedisModuleIO *rdb, ScoringIndexStats *stats) {
  RedisModule_SaveUnsigned(rdb, stats->numDocuments);
  RedisModule_SaveUnsigned(rdb, stats->numTerms);
  RedisModule_SaveUnsigned(rdb, stats->totalDocsLen);
}

static void IndexStats_RdbLoad(RedisModuleIO *rdb, IndexStats *stats, int encver) {
  IndexScoringStats_RdbLoad(rdb, &stats->scoring, encver);
  stats->numRecords = RedisModule_LoadUnsigned(rdb);
  stats->invertedSize = RedisModule_LoadUnsigned(rdb);
  RedisModule_LoadUnsigned(rdb); // Consume `invertedCap`
  RedisModule_LoadUnsigned(rdb); // Consume `skipIndexesSize`
  RedisModule_LoadUnsigned(rdb); // Consume `scoreIndexesSize`
  stats->offsetVecsSize = RedisModule_LoadUnsigned(rdb);
  stats->offsetVecRecords = RedisModule_LoadUnsigned(rdb);
  stats->termsSize = RedisModule_LoadUnsigned(rdb);
}

///////////////////////////////////////////////////////////////////////////////////////////////

inline static bool isSpecOnDisk(const IndexSpec *sp) {
  return SearchDisk_IsEnabled();
}

// Populate diskCtx for all HNSW vector fields in the spec.
// This must be called after sp->diskSpec is set.
static void IndexSpec_PopulateVectorDiskParams(IndexSpec *sp) {
  if (!sp->diskSpec) return;

  for (int i = 0; i < sp->numFields; i++) {
    FieldSpec *fs = &sp->fields[i];
    if (!FIELD_IS(fs, INDEXFLD_T_VECTOR)) continue;

    // Only HNSW indexes support disk mode (tiered with HNSW primary)
    VecSimParams *params = &fs->vectorOpts.vecSimParams;
    if (params->algo != VecSimAlgo_TIERED) continue;

    VecSimParams *primaryParams = params->algoParams.tieredParams.primaryIndexParams;
    if (!primaryParams || primaryParams->algo != VecSimAlgo_HNSWLIB) continue;

    const HNSWParams *hnsw = &primaryParams->algoParams.hnswParams;
    size_t nameLen;
    const char *namePtr = HiddenString_GetUnsafe(fs->fieldName, &nameLen);

    // Free any existing indexName to avoid memory leak
    if (fs->vectorOpts.diskCtx.indexName) {
      rm_free((void*)fs->vectorOpts.diskCtx.indexName);
    }

    // TODO: rerank is not persisted in RDB, defaulting to true on load.
    fs->vectorOpts.diskCtx = (VecSimDiskContext){
      .storage = sp->diskSpec,
      .indexName = rm_strndup(namePtr, nameLen),
      .indexNameLen = nameLen,
      .rerank = true,
    };
  }
}

///////////////////////////////////////////////////////////////////////////////////////////////

// Legacy key formatting helpers — only used by IndexSpec_DropLegacyIndexFromKeySpace

static RedisModuleString *fmtRedisNumericIndexKey(const RedisSearchCtx *ctx, const HiddenString *field) {
  return RedisModule_CreateStringPrintf(ctx->redisCtx, "nm:%s/%s", HiddenString_GetUnsafe(ctx->spec->specName, NULL), HiddenString_GetUnsafe(field, NULL));
}

static RedisModuleString *TagIndex_FormatName(const IndexSpec *spec, const HiddenString* field) {
  return RedisModule_CreateStringPrintf(RSDummyContext, "tag:%s/%s", HiddenString_GetUnsafe(spec->specName, NULL), HiddenString_GetUnsafe(field, NULL));
}

static RedisModuleString *IndexSpec_LegacyGetFormattedKey(IndexSpec *sp, const FieldSpec *fs,
                                             FieldType forType) {

  size_t typeix = INDEXTYPE_TO_POS(forType);

  RedisModuleString *ret = NULL;

  RedisSearchCtx sctx = {.redisCtx = RSDummyContext, .spec = sp};
  switch (forType) {
    case INDEXFLD_T_NUMERIC:
    case INDEXFLD_T_GEO:
      ret = fmtRedisNumericIndexKey(&sctx, fs->fieldName);
      break;
    case INDEXFLD_T_TAG:
      ret = TagIndex_FormatName(sctx.spec, fs->fieldName);
      break;
    case INDEXFLD_T_VECTOR:    // Not in legacy
    case INDEXFLD_T_GEOMETRY:  // Not in legacy
    case INDEXFLD_T_FULLTEXT:  // Text fields don't get a per-field index
    default:
      RS_ABORT_ALWAYS("Unsupported field type for legacy formatted key");
      break;
  }
  return ret;
}

// only used on "RDB load finished" event (before the server is ready to accept commands)
// so it threadsafe
void IndexSpec_DropLegacyIndexFromKeySpace(IndexSpec *sp) {
  RedisSearchCtx ctx = SEARCH_CTX_STATIC(RSDummyContext, sp);

  rune *rstr = NULL;
  t_len slen = 0;
  float score = 0;
  int dist = 0;
  size_t termLen;

  TrieIterator *it = Trie_Iterate(ctx.spec->terms, "", 0, 0, 1);
  while (TrieIterator_Next(it, &rstr, &slen, NULL, &score, NULL, &dist)) {
    char *res = runesToStr(rstr, slen, &termLen);
    RedisModuleString *keyName = Legacy_fmtRedisTermKey(&ctx, res, strlen(res));
    Redis_LegacyDropScanHandler(ctx.redisCtx, keyName, &ctx);
    RedisModule_FreeString(ctx.redisCtx, keyName);
    rm_free(res);
  }
  TrieIterator_Free(it);

  // Delete the numeric, tag, and geo indexes which reside on separate keys
  for (size_t i = 0; i < ctx.spec->numFields; i++) {
    const FieldSpec *fs = ctx.spec->fields + i;
    if (FIELD_IS(fs, INDEXFLD_T_NUMERIC)) {
      RedisModuleString *key = IndexSpec_LegacyGetFormattedKey(ctx.spec, fs, INDEXFLD_T_NUMERIC);
      Redis_LegacyDeleteKey(ctx.redisCtx, key);
    }
    if (FIELD_IS(fs, INDEXFLD_T_TAG)) {
      RedisModuleString *key = IndexSpec_LegacyGetFormattedKey(ctx.spec, fs, INDEXFLD_T_TAG);
      Redis_LegacyDeleteKey(ctx.redisCtx, key);
    }
    if (FIELD_IS(fs, INDEXFLD_T_GEO)) {
      RedisModuleString *key = IndexSpec_LegacyGetFormattedKey(ctx.spec, fs, INDEXFLD_T_GEO);
      Redis_LegacyDeleteKey(ctx.redisCtx, key);
    }
  }
  HiddenString_LegacyDropFromKeySpace(ctx.redisCtx, INDEX_SPEC_KEY_FMT, sp->specName);
}

void Indexes_UpgradeLegacyIndexes() {
  dictIterator *iter = dictGetIterator(legacySpecDict);
  dictEntry *entry = NULL;
  while ((entry = dictNext(iter))) {
    StrongRef spec_ref = dictGetRef(entry);
    IndexSpec *sp = StrongRef_Get(spec_ref);
    IndexSpec_DropLegacyIndexFromKeySpace(sp);

    // recreate the doctable
    DocTable_Free(&sp->docs);
    sp->docs = DocTable_New(INITIAL_DOC_TABLE_SIZE);

    // clear index stats
    memset(&sp->stats, 0, sizeof(sp->stats));
    // Init the index error
    sp->stats.indexError = IndexError_Init();

    // put the new index in the specDict_g with weak and strong references
    dictAdd(specDict_g, (void*)sp->specName, spec_ref.rm);
  }
  dictReleaseIterator(iter);
}

///////////////////////////////////////////////////////////////////////////////////////////////

void IndexSpec_RdbSave(RedisModuleIO *rdb, IndexSpec *sp, int contextFlags) {
  // Save the name plus the null terminator
  HiddenString_SaveToRdb(sp->specName, rdb);
  RedisModule_SaveUnsigned(rdb, (uint64_t)sp->flags);
  RedisModule_SaveUnsigned(rdb, sp->numFields);
  for (int i = 0; i < sp->numFields; i++) {
    FieldSpec_RdbSave(rdb, &sp->fields[i]);
  }

  SchemaRule_RdbSave(sp->rule, rdb);

  // If we have custom stopwords, save them
  if (sp->flags & Index_HasCustomStopwords) {
    StopWordList_RdbSave(rdb, sp->stopwords);
  }

  if (sp->flags & Index_HasSmap) {
    SynonymMap_RdbSave(rdb, sp->smap);
  }

  RedisModule_SaveUnsigned(rdb, sp->timeout);

  if (sp->aliases) {
    RedisModule_SaveUnsigned(rdb, array_len(sp->aliases));
    for (size_t ii = 0; ii < array_len(sp->aliases); ++ii) {
      HiddenString_SaveToRdb(sp->aliases[ii], rdb);
    }
  } else {
    RedisModule_SaveUnsigned(rdb, 0);
  }

  const bool storeDiskRdbData = contextFlags & REDISMODULE_CTX_FLAGS_SST_RDB;
  // Disk index
  // Check if we are using SST files with this RDB. If so, we save the disk-related
  // RAM-based data-structures to the RDB.
  // We assume symmetry w.r.t this context flag. I.e., If it is not set, we
  // assume it was not set in when the RDB will be loaded as well
  if (sp->diskSpec && storeDiskRdbData) {
    // If we're saving from the main process (not a fork), we need to acquire
    // the read lock to ensure consistent access to the data structures.
    // In a forked child process, the memory is a snapshot so no lock is needed.
    RedisModuleCtx *ctx = RedisModule_GetContextFromIO(rdb);
    RedisSearchCtx sctx = SEARCH_CTX_STATIC(ctx, sp);
    const bool inMainProcess = !(contextFlags & REDISMODULE_CTX_FLAGS_IS_CHILD);
    if (inMainProcess) {
      RedisSearchCtx_LockSpecRead(&sctx);
    }
    IndexScoringStats_RdbSave(rdb, &sp->stats.scoring);
    TrieType_GenericSave(rdb, sp->terms, false, true);
    SearchDisk_IndexSpecRdbSave(rdb, sp->diskSpec);
    if (inMainProcess) {
      RedisSearchCtx_UnlockSpec(&sctx);
    }
  }
}

IndexSpec *IndexSpec_RdbLoad(RedisModuleIO *rdb, int encver, bool useSst, QueryError *status) {
  char *rawName = NULL;
  size_t len = 0;
  HiddenString* specName = NULL;
  IndexSpec *sp = NULL;
  StrongRef spec_ref = {0};
  IndexFlags flags = 0;
  int16_t numFields = 0;
  size_t narr = 0;
  RedisModuleCtx *ctx = RedisModule_GetContextFromIO(rdb);
  rawName = LoadStringBuffer_IOError(rdb, NULL, goto cleanup_no_index);
  len = strlen(rawName);
  specName = NewHiddenString(rawName, len, true);
  RedisModule_Free(rawName);

  sp = rm_calloc(1, sizeof(IndexSpec));
  spec_ref = StrongRef_New(sp, (RefManager_Free)IndexSpec_Free);
  sp->own_ref = spec_ref;

  // Note: indexError, fieldIdToIndex, docs, specName, obfuscatedName, terms, and monitor flags are already initialized in initializeIndexSpec
  flags = (IndexFlags)LoadUnsigned_IOError(rdb, goto cleanup);
  // Note: monitorDocumentExpiration and monitorFieldExpiration are already set in initializeIndexSpec
  if (encver < INDEX_MIN_NOFREQ_VERSION) {
    flags |= Index_StoreFreqs;
  }
  numFields = LoadUnsigned_IOError(rdb, goto cleanup);

  initializeIndexSpec(sp, specName, flags, numFields);

  sp->isDuplicate = dictFetchValue(specDict_g, sp->specName) != NULL;

  IndexSpec_MakeKeyless(sp);
  for (int i = 0; i < sp->numFields; i++) {
    FieldSpec *fs = sp->fields + i;
    if (FieldSpec_RdbLoad(rdb, fs, spec_ref, encver) != REDISMODULE_OK) {
      QueryError_SetWithUserDataFmt(status, QUERY_ERROR_CODE_PARSE_ARGS, "Failed to load index field", " %d", i);
      goto cleanup;
    }
    if (fs->ftId != RS_INVALID_FIELD_ID) {
      // Prefer not to rely on the ordering of fields in the RDB file
      *array_ensure_at(&sp->fieldIdToIndex, fs->ftId, t_fieldIndex) = fs->index;
    }
    if (FieldSpec_IsSortable(fs)) {
      sp->numSortableFields++;
    }
    if (FieldSpec_HasSuffixTrie(fs) && FIELD_IS(fs, INDEXFLD_T_FULLTEXT)) {
      sp->flags |= Index_HasSuffixTrie;
      sp->suffixMask |= FIELD_BIT(fs);
      if (!sp->suffix) {
        sp->suffix = NewTrie(suffixTrie_freeCallback, Trie_Sort_Lex);
      }
    }
  }
  // After loading all the fields, we can build the spec cache
  sp->spcache = IndexSpec_BuildSpecCache(sp);

  if (SchemaRule_RdbLoad(spec_ref, rdb, encver, status) != REDISMODULE_OK) {
    QueryError_SetError(status, QUERY_ERROR_CODE_PARSE_ARGS, "Failed to load schema rule");
    goto cleanup;
  }

  if (sp->flags & Index_HasCustomStopwords) {
    sp->stopwords = StopWordList_RdbLoad(rdb, encver);
    if (sp->stopwords == NULL)
      goto cleanup;
  } else {
    sp->stopwords = DefaultStopWordList();
  }

  if (sp->flags & Index_HasSmap) {
    sp->smap = SynonymMap_RdbLoad(rdb, encver);
    if (sp->smap == NULL)
      goto cleanup;
  }

  sp->timeout = LoadUnsigned_IOError(rdb, goto cleanup);

  narr = LoadUnsigned_IOError(rdb, goto cleanup);
  for (size_t ii = 0; ii < narr; ++ii) {
    QueryError _status;
    char *s = LoadStringBuffer_IOError(rdb, NULL, goto cleanup);
    HiddenString* alias = NewHiddenString(s, strlen(s), false);
    int rc = IndexAlias_Add(alias, spec_ref, 0, &_status);
    HiddenString_Free(alias, false);
    RedisModule_Free(s);
    if (rc != REDISMODULE_OK) {
      RedisModule_Log(RSDummyContext, "notice", "Loading existing alias failed");
    }
  }

  // Open the index on disk only if we are on Flex, and this is not a duplicate.
  // If SST persistence was NOT used, delete existing disk data before opening
  // since there's no SST data to restore from (stale data must be cleared).
  if (isSpecOnDisk(sp) && !sp->isDuplicate) {
    RS_ASSERT(disk_db);
    size_t len;
    const char* name = HiddenString_GetUnsafe(sp->specName, &len);

    sp->diskSpec = SearchDisk_OpenIndex(ctx, name, len, sp->rule->type, !useSst);
    IndexSpec_PopulateVectorDiskParams(sp);
    if (!sp->diskSpec) {
      goto cleanup;
    }
    SearchDisk_RegisterIndex(ctx, sp->diskSpec);
  }

  // Load the disk-related index data if we are on disk and the save flow used
  // sst-files, even if this is a duplicate.
  // In the case of a duplicate, `sp->diskSpec=NULL` thus handled appropriately
  // On the disk side (RDB is depleted, without updating index fields).
  if (encver >= INDEX_DISK_VERSION && isSpecOnDisk(sp) && useSst) {
    IndexScoringStats_RdbLoad(rdb, &sp->stats.scoring, encver);
    if (sp->terms) {
      TrieType_Free(sp->terms);
    }
    sp->terms = TrieType_GenericLoad(rdb, false, true);
    RS_LOG_ASSERT(sp->terms, "Failed to load terms trie");
    if (SearchDisk_IndexSpecRdbLoad(rdb, sp->diskSpec) != REDISMODULE_OK) {
      goto cleanup;
    }
  }

  return sp;

cleanup:
  if (sp->diskSpec) {
    SearchDisk_UnregisterIndex(ctx, sp->diskSpec);
  }
  StrongRef_Release(spec_ref);
cleanup_no_index:
  QueryError_SetError(status, QUERY_ERROR_CODE_PARSE_ARGS, "while reading an index");
  return NULL;
}

static int IndexSpec_StoreAfterRdbLoad(IndexSpec *sp) {
  if (!sp) {
    addPendingIndexDrop();
    return REDISMODULE_ERR;
  }

  StrongRef spec_ref = sp->own_ref;

  Cursors_initSpec(sp);

  // setting isDuplicate to true will make sure index will not be removed from aliases container.
  // It may have already been set.
  if (!sp->isDuplicate && dictFetchValue(specDict_g, sp->specName) != NULL) {
    sp->isDuplicate = true;
  }

  if (sp->isDuplicate) {
    // spec already exists, however we need to finish consuming the rdb so redis won't issue an error(expecting an eof but seeing remaining data)
    // right now this can cause nasty side effects, to avoid them we will set isDuplicate to true
    RedisModule_Log(RSDummyContext, "notice", "Loading an already existing index, will just ignore.");

    // spec already exists lets just free this one
    // Remove the new spec from the global prefixes dictionary.
    // This is the only global structure that we added the new spec to at this point
    SchemaPrefixes_RemoveSpec(spec_ref);
    addPendingIndexDrop();
    StrongRef_Release(spec_ref);
  } else {
    IndexSpec_StartGC(spec_ref, sp, sp->diskSpec ? GCPolicy_Disk : GCPolicy_Fork);
    dictAdd(specDict_g, (void*)sp->specName, spec_ref.rm);

    for (int i = 0; i < sp->numFields; i++) {
      FieldsGlobalStats_UpdateStats(sp->fields + i, 1);
    }
  }
  return REDISMODULE_OK;
}

static int IndexSpec_CreateFromRdb(RedisModuleIO *rdb, int encver, bool useSst, QueryError *status) {
  // Load the index spec using the new function
  IndexSpec *sp = IndexSpec_RdbLoad(rdb, encver, useSst, status);
  return IndexSpec_StoreAfterRdbLoad(sp);
}

void IndexSpec_LegacyFree(void *spec) {
  // free legacy index do nothing, it will be called only
  // when the index key will be deleted and we keep the legacy
  // index pointer in the legacySpecDict so we will free it when needed
}

void *IndexSpec_LegacyRdbLoad(RedisModuleIO *rdb, int encver) {
  if (encver < LEGACY_INDEX_MIN_VERSION || encver > LEGACY_INDEX_MAX_VERSION) {
    return NULL;
  }
  RS_ASSERT(!SearchDisk_IsEnabled());
  char *legacyName = RedisModule_LoadStringBuffer(rdb, NULL);

  RedisModuleCtx *ctx = RedisModule_GetContextFromIO(rdb);
  IndexSpec *sp = rm_calloc(1, sizeof(IndexSpec));
  IndexSpec_InitLock(sp);
  StrongRef spec_ref = StrongRef_New(sp, (RefManager_Free)IndexSpec_Free);
  sp->own_ref = spec_ref;

  IndexSpec_MakeKeyless(sp);
  sp->numSortableFields = 0;
  sp->terms = NULL;
  sp->docs = DocTable_New(INITIAL_DOC_TABLE_SIZE);

  sp->specName = NewHiddenString(legacyName, strlen(legacyName), true);
  sp->obfuscatedName = IndexSpec_FormatObfuscatedName(sp->specName);
  RedisModule_Free(legacyName);
  sp->flags = (IndexFlags)RedisModule_LoadUnsigned(rdb);
  if (encver < INDEX_MIN_NOFREQ_VERSION) {
    sp->flags |= Index_StoreFreqs;
  }

  sp->numFields = RedisModule_LoadUnsigned(rdb);
  sp->fields = rm_calloc(sp->numFields, sizeof(FieldSpec));
  int maxSortIdx = -1;
  for (int i = 0; i < sp->numFields; i++) {
    FieldSpec *fs = sp->fields + i;
    initializeFieldSpec(fs, i);
    FieldSpec_RdbLoad(rdb, fs, spec_ref, encver);
    if (FieldSpec_IsSortable(fs)) {
      sp->numSortableFields++;
    }
  }
  // After loading all the fields, we can build the spec cache
  sp->spcache = IndexSpec_BuildSpecCache(sp);

  IndexStats_RdbLoad(rdb, &sp->stats, encver);

  DocTable_LegacyRdbLoad(&sp->docs, rdb, encver);
  /* For version 3 or up - load the generic trie */
  if (encver >= 3) {
    sp->terms = TrieType_GenericLoad(rdb, false, false);
  } else {
    sp->terms = NewTrie(NULL, Trie_Sort_Lex);
  }

  if (sp->flags & Index_HasCustomStopwords) {
    sp->stopwords = StopWordList_RdbLoad(rdb, encver);
  } else {
    sp->stopwords = DefaultStopWordList();
  }

  sp->smap = NULL;
  if (sp->flags & Index_HasSmap) {
    sp->smap = SynonymMap_RdbLoad(rdb, encver);
  }
  if (encver < INDEX_MIN_EXPIRE_VERSION) {
    sp->timeout = -1;
  } else {
    sp->timeout = RedisModule_LoadUnsigned(rdb);
  }

  if (encver >= INDEX_MIN_ALIAS_VERSION) {
    size_t narr = RedisModule_LoadUnsigned(rdb);
    for (size_t ii = 0; ii < narr; ++ii) {
      QueryError status;
      size_t dummy;
      char *s = RedisModule_LoadStringBuffer(rdb, &dummy);
      HiddenString* alias = NewHiddenString(s, strlen(s), false);
      int rc = IndexAlias_Add(alias, spec_ref, 0, &status);
      HiddenString_Free(alias, false);
      RedisModule_Free(s);
      RS_ASSERT(rc == REDISMODULE_OK);
    }
  }

  const char *formattedIndexName = IndexSpec_FormatName(sp, RSGlobalConfig.hideUserDataFromLog);
  SchemaRuleArgs *rule_args = dictFetchValue(legacySpecRules, sp->specName);
  if (!rule_args) {
    RedisModule_LogIOError(rdb, "warning",
                           "Could not find upgrade definition for legacy index '%s'", formattedIndexName);
    StrongRef_Release(spec_ref);
    return NULL;
  }

  QueryError status;
  sp->rule = SchemaRule_Create(rule_args, spec_ref, &status);

  dictDelete(legacySpecRules, sp->specName);
  SchemaRuleArgs_Free(rule_args);

  if (!sp->rule) {
    RedisModule_LogIOError(rdb, "warning", "Failed creating rule for legacy index '%s', error='%s'",
                           formattedIndexName, QueryError_GetDisplayableError(&status, RSGlobalConfig.hideUserDataFromLog));
    StrongRef_Release(spec_ref);
    return NULL;
  }

  // Start GC after diskSpec is available so the disk GC callback can use it immediately.
  IndexSpec_StartGC(spec_ref, sp, sp->diskSpec ? GCPolicy_Disk : GCPolicy_Fork);
  Cursors_initSpec(sp);

  dictAdd(legacySpecDict, (void*)sp->specName, spec_ref.rm);
  // Subscribe to keyspace notifications
  Initialize_KeyspaceNotifications();

  return spec_ref.rm;
}

void IndexSpec_LegacyRdbSave(RedisModuleIO *rdb, void *value) {
  // we do not save legacy indexes
  return;
}

int Indexes_RdbLoad(RedisModuleIO *rdb, int encver, int when) {
  const bool useSst = CheckRdbSstPersistence(RedisModule_GetContextFromIO(rdb), "RDB Load");
  size_t nIndexes = 0;
  QueryError status = QueryError_Default();

  if (encver < INDEX_MIN_COMPAT_VERSION) {
    return REDISMODULE_ERR;
  }

  nIndexes = LoadUnsigned_IOError(rdb, goto cleanup);
  if (!SearchDisk_CheckLimitNumberOfIndexes(nIndexes)) {
    RedisModule_LogIOError(rdb, "warning", "Too many indexes for flex. Having %zu indexes, but flex only supports %d.", nIndexes, FLEX_MAX_INDEX_COUNT);
    return REDISMODULE_ERR;
  }
  for (size_t i = 0; i < nIndexes; ++i) {
    if (IndexSpec_CreateFromRdb(rdb, encver, useSst, &status) != REDISMODULE_OK) {
      RedisModule_LogIOError(rdb, "warning", "RDB Load: %s", QueryError_GetDisplayableError(&status, RSGlobalConfig.hideUserDataFromLog));
      QueryError_ClearError(&status);
      return REDISMODULE_ERR;
    }
  }

  // If we have indexes in the auxiliary data, we need to subscribe to the
  // keyspace notifications
  Initialize_KeyspaceNotifications();

  return REDISMODULE_OK;

cleanup:
  return REDISMODULE_ERR;
}

void Indexes_RdbSave(RedisModuleIO *rdb, int when) {
  RedisModuleCtx *ctx = RedisModule_GetContextFromIO(rdb);
  const int contextFlags = RedisModule_GetContextFlags(ctx);

  RedisModule_SaveUnsigned(rdb, dictSize(specDict_g));

  dictIterator *iter = dictGetIterator(specDict_g);
  dictEntry *entry = NULL;
  while ((entry = dictNext(iter))) {
    StrongRef spec_ref = dictGetRef(entry);
    IndexSpec *sp = StrongRef_Get(spec_ref);
    IndexSpec_RdbSave(rdb, sp, contextFlags);
  }

  dictReleaseIterator(iter);
}

void Indexes_RdbSave2(RedisModuleIO *rdb, int when) {
  if (dictSize(specDict_g)) {
    Indexes_RdbSave(rdb, when);
  }
}

void *IndexSpec_RdbLoad_Logic(RedisModuleIO *rdb, int encver) {
  const bool useSst = CheckRdbSstPersistence(RedisModule_GetContextFromIO(rdb), "RDB Load Logic");
  if (encver < INDEX_VECSIM_SVS_VAMANA_VERSION) {
    // Legacy index, loaded in order to upgrade from an old version
    return IndexSpec_LegacyRdbLoad(rdb, encver);
  } else {
    // New index, loaded normally.
    // Even though we don't actually load or save the index spec in the key space, this implementation is useful
    // because it allows us to serialize and deserialize the index spec in a clean way.
    QueryError status = QueryError_Default();
    IndexSpec *sp = IndexSpec_RdbLoad(rdb, encver, useSst, &status);
    if (!sp) {
      RedisModule_LogIOError(rdb, "warning", "RDB Load: %s", QueryError_GetDisplayableError(&status, RSGlobalConfig.hideUserDataFromLog));
      QueryError_ClearError(&status);
    }
    return sp;
  }
}

RedisModuleString * IndexSpec_Serialize(IndexSpec *sp) {
  return RedisModule_SaveDataTypeToString(NULL, sp, IndexSpecType);
}

int IndexSpec_Deserialize(const RedisModuleString *serialized, int encver) {
  IndexSpec *sp = RedisModule_LoadDataTypeFromStringEncver(serialized, IndexSpecType, encver);
  if (sp) Initialize_KeyspaceNotifications();
  return IndexSpec_StoreAfterRdbLoad(sp);
}

void Indexes_Propagate(RedisModuleCtx *ctx) {
  dictIterator *iter = dictGetIterator(specDict_g);
  dictEntry *entry;
  while ((entry = dictNext(iter))) {
    StrongRef spec_ref = dictGetRef(entry);
    IndexSpec *sp = StrongRef_Get(spec_ref);
    RS_ASSERT(sp != NULL);
    RedisModuleString *serialized = IndexSpec_Serialize(sp);
    RS_ASSERT(serialized != NULL);
    int rc = RedisModule_ClusterPropagateForSlotMigration(ctx, RS_RESTORE_IF_NX, "cls", SPEC_SCHEMA_STR, INDEX_CURRENT_VERSION, serialized);
    if (rc != REDISMODULE_OK) {
      RedisModule_Log(ctx, "warning", "Failed to propagate index '%s' during slot migration. errno: %d", IndexSpec_FormatName(sp, RSGlobalConfig.hideUserDataFromLog), errno);
    }
    RedisModule_FreeString(NULL, serialized);
  }
  dictReleaseIterator(iter);
}

static void IndexSpec_RdbSave_Wrapper(RedisModuleIO *rdb, void *value) {
  RedisModuleCtx *ctx = RedisModule_GetContextFromIO(rdb);
  const int contextFlags = RedisModule_GetContextFlags(ctx);
  IndexSpec_RdbSave(rdb, value, contextFlags);
}

int IndexSpec_RegisterType(RedisModuleCtx *ctx) {
  RedisModuleTypeMethods tm = {
      .version = REDISMODULE_TYPE_METHOD_VERSION,
      .rdb_load = IndexSpec_RdbLoad_Logic,    // We don't store the index spec in the key space,
      .rdb_save = IndexSpec_RdbSave_Wrapper,  // but these are useful for serialization/deserialization (and legacy loading)
      .aux_load = Indexes_RdbLoad,
      .aux_save = Indexes_RdbSave,
      .free = IndexSpec_LegacyFree,
      .aof_rewrite = GenericAofRewrite_DisabledHandler,
      .aux_save_triggers = REDISMODULE_AUX_BEFORE_RDB,
      .aux_save2 = Indexes_RdbSave2,
  };

  IndexSpecType = RedisModule_CreateDataType(ctx, "ft_index0", INDEX_CURRENT_VERSION, &tm);
  if (IndexSpecType == NULL) {
    RedisModule_Log(ctx, "warning", "Could not create index spec type");
    return REDISMODULE_ERR;
  }
  return REDISMODULE_OK;
}
