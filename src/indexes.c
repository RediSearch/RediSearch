/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

// indexes.c -- the global index registry and keyspace dispatch.
//
// Owns the global spec dictionaries (specDict_g / specIdDict_g) and everything
// that operates over them: RDB load/save, propagation, the
// Indexes_*MatchingWithSchemaRules keyspace-notification dispatch, the RDB-load
// lifecycle events, and replica-side SST replication completion/abort.
//
// Dependency direction (one-way): indexes.c -> spec.h (IndexSpec lifecycle) and
// indexes.c -> indexes_scan.h (RDB-load events trigger full rescans). The
// scanner only reads specDict_g as a data dependency; it never calls back here.

#include "spec.h"
#include "indexes.h"
#include "indexes_scan.h"
#include "document.h"
#include "inverted_index_ffi.h"
#include "rlookup_load_document.h"

#include "util/logging.h"
#include "util/likely.h"
#include "util/misc.h"
#include "triemap_ffi.h"
#include "commands.h"
#include "dictionary.h"
#include "rmutil/util.h"
#include "rmutil/rm_assert.h"
#include "rmalloc.h"
#include "config.h"
#include "cursor.h"
#include "redis_index.h"
#include "indexer.h"
#include "alias.h"
#include "module.h"
#include "rules.h"
#include "doc_types.h"
#include "doc_id_meta.h"
#include "rdb.h"
#include "obfuscation/obfuscation_api.h"
#include "util/workers.h"
#include "info/global_stats.h"
#include "info/info_redis/threads/current_thread.h"
#include "reply_macros.h"
#include "notifications.h"
#include "search_disk.h"
#include "search_disk_utils.h"

// The global index registry, keyed by name and by spec id. Other translation
// units read these as externs (declared in indexes.h).
dict *specDict_g = NULL;
dict *specIdDict_g = NULL;

// Legacy (pre-RDB-event) spec staging dictionaries are defined in spec.c.
extern dict *legacySpecDict;
extern dict *legacySpecRules;
extern uint32_t maxIndexes_g;

static bool checkIfSpecExists(const char *rawSpecName, size_t rawSpecNameLen) {
  HiddenString *specName = NewHiddenString(rawSpecName, rawSpecNameLen, false);
  bool found = dictFetchValue(specDict_g, specName) != NULL;
  HiddenString_Free(specName, false);
  return found;
}

// Entry point for FT.CREATE: enforce the registry preconditions (name
// uniqueness, index-count limit), build the spec via the IndexSpec core, publish
// it into the global registry, and schedule its initial scan.
IndexSpec *Indexes_CreateNewSpec(RedisModuleCtx *ctx, RedisModuleString **argv, int argc,
                             QueryError *status) {
  size_t rawSpecNameLen = 0;
  const char *rawSpecName = RedisModule_StringPtrLen(argv[1], &rawSpecNameLen);
  if (checkIfSpecExists(rawSpecName, rawSpecNameLen)) {
    QueryError_SetCode(status, QUERY_ERROR_CODE_INDEX_EXISTS);
    return NULL;
  }
  if (Indexes_Count() >= maxIndexes_g) {
    QueryError_SetWithoutUserDataFmt(status, QUERY_ERROR_CODE_LIMIT,
                                     "Maximum number of indexes (%u) reached", maxIndexes_g);
    return NULL;
  }

  IndexSpec *sp = IndexSpec_CreateNew(ctx, argv, argc, status);
  if (sp == NULL) {
    return NULL;
  }
  // IndexSpec_CreateNew leaves the single owning reference in sp->own_ref; the
  // registry entries take over ownership of it below.
  StrongRef spec_ref = IndexSpec_GetStrongRefUnsafe(sp);

  // Add the spec to the global spec dictionaries (by name and by specId)
  if (dictAdd(specDict_g, (void *)sp->specName, spec_ref.rm) != DICT_OK) {
    RedisModule_Log(ctx, "warning", "Failed adding index to global dictionary");
    StrongRef_Release(spec_ref);
    RS_ABORT("dictAdd shouldn't fail here - index shouldn't exists in the dictionary");
    return NULL;
  }
  if (dictAdd(specIdDict_g, (void *)(uintptr_t)sp->specId, spec_ref.rm) != DICT_OK) {
    dictDelete(specDict_g, sp->specName);
    RedisModule_Log(ctx, "warning", "Failed adding index to global spec ID dictionary");
    StrongRef_Release(spec_ref);
    RS_ABORT("dictAdd shouldn't fail here - index shouldn't exists in the dictionary");
    return NULL;
  }

  if (!(sp->flags & Index_SkipInitialScan)) {
    IndexSpec_ScanAndReindex(ctx, spec_ref);
  }
  return sp;
}

// For testing purposes only
void Spec_AddToDict(RefManager *rm) {
  IndexSpec *spec = ((IndexSpec *)__RefManager_Get_Object(rm));
  dictAdd(specDict_g, (void *)spec->specName, (void *)rm);
  dictAdd(specIdDict_g, (void *)(uintptr_t)spec->specId, (void *)rm);
}

// Assumes this is called from the main thread with no competing threads.
// Also assumes that the spec exists in the global dictionary, so we use the
// global reference as our guard and access the spec directly.
// This function consumes the Strong reference it gets.
void Indexes_RemoveSpecFromGlobals(StrongRef spec_ref, bool removeActive) {
  IndexSpec *spec = StrongRef_Get(spec_ref);
  // Remove spec from the global index registry (by name and by specId)
  dictDelete(specDict_g, spec->specName);
  dictDelete(specIdDict_g, (void *)(uintptr_t)spec->specId);

  // Unwind the spec's remaining global state and consume the reference.
  IndexSpec_Unlink(spec_ref, removeActive);
}

// Look up a spec by name (or alias) in the global registry - the specDict_g
// access - then run the per-spec post-load bookkeeping via IndexSpec_OnAcquire.
// The call does not increase the spec's strong reference counter (the returned
// StrongRef is a borrow; NULL payload if the index does not exist).
StrongRef Indexes_LoadIndexSpecUnsafeEx(IndexLoadOptions *options) {
  const char *ixname = NULL;
  if (options->flags & INDEXSPEC_LOAD_KEY_RSTRING) {
    ixname = RedisModule_StringPtrLen(options->nameR, NULL);
  } else {
    ixname = options->nameC;
  }

  HiddenString *specNameOrAlias = NewHiddenString(ixname, strlen(ixname), false);
  StrongRef spec_ref = {dictFetchValue(specDict_g, specNameOrAlias)};
  IndexSpec *sp = StrongRef_Get(spec_ref);
  if (!sp && !(options->flags & INDEXSPEC_LOAD_NOALIAS)) {
    spec_ref = IndexAlias_Get(specNameOrAlias);
    sp = StrongRef_Get(spec_ref);
  }
  HiddenString_Free(specNameOrAlias, false);
  if (!sp) {
    return spec_ref;
  }

  IndexSpec_OnAcquire(spec_ref, options);
  return spec_ref;
}

StrongRef Indexes_LoadIndexSpecUnsafe(const char *name) {
  IndexLoadOptions lopts = {.nameC = name};
  return Indexes_LoadIndexSpecUnsafeEx(&lopts);
}

// Assuming the GIL is locked before calling this function.
void Indexes_SetTempSpecsTimers(TimerOp op) {
  dictIterator *iter = dictGetIterator(specDict_g);
  dictEntry *entry = NULL;
  while ((entry = dictNext(iter))) {
    StrongRef spec_ref = dictGetRef(entry);
    IndexSpec *sp = StrongRef_Get(spec_ref);
    if (sp->flags & Index_Temporary) {
      switch (op) {
        case TimerOp_Add: IndexSpec_SetTimeoutTimer(sp, StrongRef_Demote(spec_ref)); break;
        case TimerOp_Del: IndexSpec_ResetTimeoutTimer(sp);    break;
      }
    }
  }
  dictReleaseIterator(iter);
}

void Indexes_Free(RedisModuleCtx *ctx, dict *d, bool deleteDiskData) {
  // free the schema dictionary this way avoid iterating over it for each combination of
  // spec<-->prefix
  SchemaPrefixes_Free(SchemaPrefixes_g);
  SchemaPrefixes_g = NULL;
  SchemaPrefixes_Create();

  CursorList_Empty(&g_CursorsListCoord);
  // cursor list is iterating through the list as well and consuming a lot of CPU
  CursorList_Empty(&g_CursorsList);

  arrayof(StrongRef) specs = array_new(StrongRef, dictSize(d));
  dictIterator *iter = dictGetIterator(d);
  dictEntry *entry = NULL;
  while ((entry = dictNext(iter))) {
    StrongRef spec_ref = dictGetRef(entry);
    array_append(specs, spec_ref);
  }
  dictReleaseIterator(iter);

  for (size_t i = 0; i < array_len(specs); ++i) {
    IndexSpec *spec = StrongRef_Get(specs[i]);
    if (spec && spec->diskSpec) {
      // Unregister must always precede close (triggered by Indexes_RemoveSpecFromGlobals)
      SearchDisk_UnregisterIndex(ctx, spec);
      if (deleteDiskData) {
        SearchDisk_MarkIndexForDeletion(spec->diskSpec);
      }
    }
    Indexes_RemoveSpecFromGlobals(specs[i], false);
  }
  array_free(specs);
}

// Load one spec from the RDB stream and resolve its registry-dependent state:
// parse it (IndexSpec core), detect a duplicate against the registry, then open
// its on-disk index if appropriate. Returns the spec (not yet registered - the
// caller passes it to Indexes_StoreSpecAfterRdbLoad), or NULL on failure.
static IndexSpec *Indexes_LoadSpecFromRdb(RedisModuleIO *rdb, int encver, bool useSst, QueryError *status) {
  IndexSpec *sp = IndexSpec_RdbLoad(rdb, encver, useSst, status);
  if (!sp) {
    return NULL;
  }
  // Duplicate detection is a registry read, so it lives here rather than in the
  // IndexSpec core. It also gates the non-SST on-disk index open below.
  sp->isDuplicate = dictFetchValue(specDict_g, sp->specName) != NULL;
  if (IndexSpec_RdbLoadOpenDisk(RedisModule_GetContextFromIO(rdb), sp, useSst, status) != REDISMODULE_OK) {
    StrongRef_Release(sp->own_ref);
    return NULL;
  }
  return sp;
}

int Indexes_RdbLoad(RedisModuleIO *rdb, int encver, int when) {
  const bool useSst = CheckRdbSstPersistence(RedisModule_GetContextFromIO(rdb), "RDB Load");
  size_t nIndexes = 0;
  QueryError status = QueryError_Default();

  if (encver < INDEX_MIN_COMPAT_VERSION) {
    return REDISMODULE_ERR;
  }

  nIndexes = LoadUnsigned_IOError(rdb, return REDISMODULE_ERR);

  if (unlikely(nIndexes > maxIndexes_g)) {
    RedisModule_LogIOError(
        rdb, "warning",
        "RDB Load: Number of indexes (%zu) exceeds maximum allowed (%u)",
        nIndexes, maxIndexes_g);
    return REDISMODULE_ERR;
  }
  if (!SearchDisk_CheckLimitNumberOfIndexes(nIndexes)) {
    RedisModule_LogIOError(rdb, "warning", "Too many indexes for flex. Having %zu indexes, but flex only supports %d.", nIndexes, FLEX_MAX_INDEX_COUNT);
    return REDISMODULE_ERR;
  }
  for (size_t i = 0; i < nIndexes; ++i) {
    // Load one spec (parse + duplicate detection + disk open), then publish it
    // into the registry.
    IndexSpec *sp = Indexes_LoadSpecFromRdb(rdb, encver, useSst, &status);
    if (Indexes_StoreSpecAfterRdbLoad(sp) != REDISMODULE_OK) {
      RedisModule_LogIOError(rdb, "warning", "RDB Load: %s", QueryError_GetDisplayableError(&status, RSGlobalConfig.hideUserDataFromLog));
      QueryError_ClearError(&status);
      return REDISMODULE_ERR;
    }
  }

  // If we have indexes in the auxiliary data, we need to subscribe to the
  // keyspace notifications
  Initialize_KeyspaceNotifications();

  return REDISMODULE_OK;
}

// Finalize a spec loaded from RDB: detect a name collision against the registry,
// then either discard the duplicate or publish the spec into specDict_g/
// specIdDict_g and start its GC. Owns the registry writes for the load path.
int Indexes_StoreSpecAfterRdbLoad(IndexSpec *sp) {
  if (!sp) {
    addPendingIndexDrop();
    return REDISMODULE_ERR;
  }

  StrongRef spec_ref = sp->own_ref;

  // Initialize the spec's cursor-related fields.
  sp->activeCursors = 0;

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
    // In the SST replication path diskSpec is still NULL here — it's opened
    // later by Indexes_FinishSSTReplication, which also starts the Disk GC.
    // Start GC eagerly only when the spec is fully ready: memory mode, or a
    // disk spec whose diskSpec was opened during IndexSpec_RdbLoad (non-SST
    // RDB path).
    if (!SearchDisk_IsEnabled()) {
      IndexSpec_StartGC(spec_ref, sp, GCPolicy_Fork);
    } else if (sp->diskSpec) {
      RS_ASSERT(!IS_SST_RDB_IN_PROCESS(RSDummyContext));
      IndexSpec_StartGC(spec_ref, sp, GCPolicy_Disk);
    }
    dictAdd(specDict_g, (void *)sp->specName, spec_ref.rm);
    dictAdd(specIdDict_g, (void *)(uintptr_t)sp->specId, spec_ref.rm);

    for (int i = 0; i < sp->numFields; i++) {
      FieldsGlobalStats_UpdateStats(sp->fields + i, 1);
    }
  }
  return REDISMODULE_OK;
}

static void Indexes_RdbSave(RedisModuleIO *rdb, int when) {
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

static void Indexes_RdbSave2(RedisModuleIO *rdb, int when) {
  if (dictSize(specDict_g)) {
    Indexes_RdbSave(rdb, when);
  }
}

// Per-key rdb_load callback for the IndexSpecType module type: load a single
// spec (legacy or current) and resolve its registry-dependent state. Used for
// serialization/deserialization and ASM migration; does not register the spec
// (that is the caller's job, e.g. via Indexes_StoreSpecAfterRdbLoad).
static void *IndexSpecType_RdbLoad(RedisModuleIO *rdb, int encver) {
  const bool useSst = CheckRdbSstPersistence(RedisModule_GetContextFromIO(rdb), "RDB Load Logic");
  if (encver <= LEGACY_INDEX_MAX_VERSION) {
    // Legacy index, loaded in order to upgrade from an old version
    return IndexSpec_LegacyRdbLoad(rdb, encver);
  }
  // New index, loaded normally. Even though we don't store the index spec in the
  // key space, this is useful for clean serialize/deserialize (and ASM migration).
  RS_ASSERT(encver >= INDEX_ASM_PROPAGATE_DEFINITIONS_VERSION);
  if (encver < INDEX_ASM_PROPAGATE_DEFINITIONS_VERSION) {
    RedisModule_LogIOError(rdb, "warning", "RDB Load: Unexpected encver %d found in RDB_Load, encver not expected to be lower than %d", encver, INDEX_ASM_PROPAGATE_DEFINITIONS_VERSION);
    return NULL;
  }
  QueryError status = QueryError_Default();
  IndexSpec *sp = Indexes_LoadSpecFromRdb(rdb, encver, useSst, &status);
  if (!sp) {
    RedisModule_LogIOError(rdb, "warning", "RDB Load: %s", QueryError_GetDisplayableError(&status, RSGlobalConfig.hideUserDataFromLog));
    QueryError_ClearError(&status);
  }
  return sp;
}

// Register the IndexSpecType module type. Its aux callbacks serialize the whole
// registry (Indexes_RdbLoad/Save), while its per-key callbacks serialize a single
// spec (defined in spec.c); wiring them together is a registry-layer concern.
int Indexes_RegisterType(RedisModuleCtx *ctx) {
  RedisModuleTypeMethods tm = {
      .version = REDISMODULE_TYPE_METHOD_VERSION,
      .rdb_load = IndexSpecType_RdbLoad,       // We don't store the index spec in the key space,
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

void Indexes_Propagate(RedisModuleCtx *ctx) {
  dictIterator *iter = dictGetIterator(specDict_g);
  dictEntry *entry;
  while ((entry = dictNext(iter))) {
    StrongRef spec_ref = dictGetRef(entry);
    IndexSpec *sp = StrongRef_Get(spec_ref);
    RS_ASSERT(sp != NULL);
    RedisModuleString *serialized = IndexSpec_Serialize(sp);
    RS_ASSERT(serialized != NULL);
    int rc = RedisModule_ClusterPropagateForSlotMigration(ctx, CMD_FOR_ENV(RS_RESTORE_IF_NX), "cls", SPEC_SCHEMA_STR, INDEX_CURRENT_VERSION, serialized);
    if (rc != REDISMODULE_OK) {
      RedisModule_Log(ctx, "warning", "Failed to propagate index '%s' during slot migration. errno: %d", IndexSpec_FormatName(sp, RSGlobalConfig.hideUserDataFromLog), errno);
    }
    RedisModule_FreeString(NULL, serialized);
  }
  dictReleaseIterator(iter);
}

static void onFlush(RedisModuleCtx *ctx, RedisModuleEvent eid, uint64_t subevent, void *data) {
  if (subevent != REDISMODULE_SUBEVENT_FLUSHDB_START) {
    return;
  }
  if (specDict_g) {
    Indexes_Free(ctx, specDict_g, true);
    // specDict_g itself is not actually freed
  }
  Dictionary_Clear();
  RSGlobalStats.totalStats.used_dialects = 0;
}

void Indexes_Init(RedisModuleCtx *ctx) {
  if (!specDict_g) {
    specDict_g = dictCreate(&dictTypeHeapHiddenStrings, NULL);
  }
  if (!specIdDict_g) {
    specIdDict_g = dictCreate(&dictTypeUint64, NULL);
  }
  RedisModule_SubscribeToServerEvent(ctx, RedisModuleEvent_FlushDB, onFlush);
  SchemaPrefixes_Create();
}

size_t Indexes_Count() {
  return dictSize(specDict_g);
}

SpecOpIndexingCtx *Indexes_FindMatchingSchemaRules(RedisModuleCtx *ctx, RedisModuleString *key,
                                                   DocumentType type, bool runFilters,
                                                   RedisModuleString *keyToReadData) {
  if (!keyToReadData) {
    keyToReadData = key;
  }
  SpecOpIndexingCtx *res = rm_malloc(sizeof(*res));
  res->specs = dictCreate(&dictTypeHeapHiddenStrings, NULL);
  res->specsOps = array_new(SpecOpCtx, 10);
  if (dictSize(specDict_g) == 0) {
    return res;
  }
  dict *specs = res->specs;

#if defined(_DEBUG) && 0
  RLookupKey *k = RLookup_GetKey_LoadEx(&r->lk, UNDERSCORE_KEY, strlen(UNDERSCORE_KEY), UNDERSCORE_KEY, RLOOKUP_F_NOFLAGS);
  RSValue *v = RLookup_GetItem(k, &r->row);
  const char *x = RSValue_StringPtrLen(v, NULL);
  RedisModule_Log(RSDummyContext, "notice", "Indexes_FindMatchingSchemaRules: x=%s", x);
  const char *f = "name";
  k = RLookup_GetKey_ReadEx(&r->lk, f, strlen(f), RLOOKUP_F_NOFLAGS);
  if (k) {
    v = RLookup_GetItem(k, &r->row);
    x = RSValue_StringPtrLen(v, NULL);
  }
#endif  // _DEBUG

  size_t n;
  const char *key_p = RedisModule_StringPtrLen(key, &n);
  // collect specs that their name is prefixed by the key name
  // `prefixes` includes list of arrays of specs, one for each prefix of key name
  TrieMapResultBuf prefixes = TrieMap_FindPrefixes(SchemaPrefixes_g, key_p, n);
  for (int i = 0; i < TrieMapResultBuf_Len(&prefixes); ++i) {
    SchemaPrefixNode *node = TrieMapResultBuf_GetByIndex(&prefixes, i);
    for (int j = 0; j < array_len(node->index_specs); ++j) {
      StrongRef global = node->index_specs[j];
      IndexSpec *spec = StrongRef_Get(global);
      if (spec && !dictFind(specs, spec->specName)) {
        // skip if document type does not match the index type
        // The unsupported type is needed for crdt empty keys (deleted)
        if (type != DocumentType_Unsupported && type != spec->rule->type) {
          continue;
        }

        SpecOpCtx specOp = {
            .spec = spec,
            .op = SpecOp_Add,
        };
        array_append(res->specsOps, specOp);
        dictEntry *entry = dictAddRaw(specs, (void*)spec->specName, NULL);
        // put the location on the specsOps array so we can get it
        // fast using index name
        entry->v.u64 = array_len(res->specsOps) - 1;
      }
    }
  }
  TrieMapResultBuf_Free(prefixes);

  if (runFilters) {
    // We load the data from the `keyToReadData` key, which is the key the old
    // key was changed to, since the old key is already deleted.
    key_p = RedisModule_StringPtrLen(keyToReadData, NULL);

    EvalCtx *r = NULL;
    for (size_t i = 0; i < array_len(res->specsOps); ++i) {
      SpecOpCtx *specOp = res->specsOps + i;
      IndexSpec *spec = specOp->spec;
      if (!spec->rule->filter_exp) {
        continue;
      }

      // load document only if required
      if (!r) {
        r = EvalCtx_Create(EVAL_MODE_INDEX);
      }

      RedisSearchCtx sctx = { .redisCtx = ctx };
      QueryError status = QueryError_Default();
      RLookup_LoadRuleFields(&sctx, &r->lk, &r->row, spec, key_p, &status);
      QueryError_ClearError(&status); // TODO: report errors

      if (!SchemaRule_FilterPasses(r, spec->rule->filter_exp)) {
        if (dictFind(specs, spec->specName)) {
          specOp->op = SpecOp_Del;
        }
      }
      // Clean up state between iterations (indexes)
      QueryError_ClearError(&r->status);
      RLookup_Cleanup(&r->lk);
      r->lk = RLookup_New();
      RLookupRow_Reset(&r->row);
    }

    if (r) {
      EvalCtx_Destroy(r);
    }
  }
  return res;
}

static bool hashFieldChanged(IndexSpec *spec, RedisModuleString **hashFields) {
  if (hashFields == NULL) {
    return true;
  }

  // TODO: improve implementation to avoid O(n^2)
  for (size_t i = 0; hashFields[i] != NULL; ++i) {
    size_t length = 0;
    const char *field = RedisModule_StringPtrLen(hashFields[i], &length);
    for (size_t j = 0; j < spec->numFields; ++j) {
      if (!HiddenString_CompareC(spec->fields[j].fieldName, field, length)) {
        return true;
      }
    }
    // optimize. change of score and payload fields just require an update of the doc table
    if ((spec->rule->lang_field && !strcmp(field, spec->rule->lang_field)) ||
        (spec->rule->score_field && !strcmp(field, spec->rule->score_field)) ||
        (spec->rule->payload_field && !strcmp(field, spec->rule->payload_field))) {
      return true;
    }
  }
  return false;
}

void Indexes_SpecOpsIndexingCtxFree(SpecOpIndexingCtx *specs) {
  dictRelease(specs->specs);
  array_free(specs->specsOps);
  rm_free(specs);
}

void Indexes_UpdateMatchingWithSchemaRules(RedisModuleCtx *ctx, RedisModuleString *key, DocumentType type,
                                           RedisModuleString **hashFields) {
  if (type == DocumentType_Unsupported) {
    // COPY could overwrite a hash/json with other types so we must try and remove old doc.
    Indexes_DeleteMatchingWithSchemaRules(ctx, key, type, hashFields);
    return;
  }

  SpecOpIndexingCtx *specs = Indexes_FindMatchingSchemaRules(ctx, key, type, true, NULL);

  for (size_t i = 0; i < array_len(specs->specsOps); ++i) {
    SpecOpCtx *specOp = specs->specsOps + i;

    if (hashFieldChanged(specOp->spec, hashFields)) {
      if (specOp->op == SpecOp_Add) {
        IndexSpec_UpdateDoc(specOp->spec, ctx, key, type);
      } else {
        // specOp->op is SpecOp_Del when the key matches the index prefix but
        // the filter expression fails (e.g. a field value changed so the filter
        // no longer passes, or a required field is missing). If the document was
        // previously indexed, it must be removed now.
        IndexSpec_DeleteDoc(specOp->spec, ctx, key);
      }
    }
  }

  Indexes_SpecOpsIndexingCtxFree(specs);
}

void Indexes_UpdateMatchingDocExpiration(RedisModuleCtx *ctx, RedisModuleString *key, DocumentType type) {
  if (type == DocumentType_Unsupported || !RSGlobalConfig.monitorExpiration) {
    return;
  }

  // Find all indexes that match the key's name prefix. runFilters=false:
  // EXPIRE/PERSIST do not change field values, so a doc's schema-rule FILTER
  // outcome cannot have flipped since the last write. The DocTable lookup
  // below (DocTable_BorrowByKeyR) is the only discriminator we need — it
  // returns the existing DMD when the doc is currently indexed, and NULL
  // otherwise. Re-evaluating the filter here would re-read hash/JSON fields
  // on the main thread for a result the loop would ignore.
  SpecOpIndexingCtx *specs = Indexes_FindMatchingSchemaRules(ctx, key, type, false, NULL);

  // EXPIRE/PERSIST notifications fire for every key in the keyspace. When no
  // spec prefix matches we have nothing to update, so skip the OpenKey/TTL
  // read entirely — that overhead would otherwise be paid on every event.
  if (array_len(specs->specsOps) == 0) {
    Indexes_SpecOpsIndexingCtxFree(specs);
    return;
  }

  RedisModuleKey *kp = RedisModule_OpenKey(ctx, key, DOCUMENT_OPEN_KEY_INDEXING_FLAGS);
  RS_ASSERT(kp);
  t_expirationTimePoint ttl = GetKeyExpirationTime(kp);
  RedisModule_CloseKey(kp);

  for (size_t i = 0; i < array_len(specs->specsOps); ++i) {
    SpecOpCtx *specOp = &specs->specsOps[i];
    IndexSpec *spec = specOp->spec;
    // Skip TTL update for specs that opted out of doc-level TTL tracking,
    // matching the gate in Document_LoadSchemaFieldHash/Json. Otherwise
    // DocTable_IsDocExpired could drop rows for an index that doesn't
    // monitor TTLs.
    //
    // No spec lock needed: monitorDocumentExpiration is only written from
    // main-thread callbacks (spec init, FT.CONFIG SET, FT.DEBUG
    // MONITOR_EXPIRATION), and this keyspace-notification callback also
    // runs on the main thread, so the Redis event loop serializes them.
    // The spec write lock guards index data against background workers,
    // not main-thread config flags (same pattern as the load path).
    if (!spec->monitorDocumentExpiration) {
      continue;
    }
    RedisSearchCtx sctx = SEARCH_CTX_STATIC(ctx, spec);
    // Read lock is sufficient: the only mutation is the relaxed atomic store on
    // `dmd->expirationTimeNs` inside DocTable_SetDocExpiration (paired with a
    // relaxed atomic load in DocTable_IsDocExpired). The DMD chain traversal
    // and refcount manipulation in DocTable_BorrowByKeyR / DMD_Return are
    // explicitly documented as safe under either lock mode (doc_table.c, near
    // DocTable_GetOwn). Concurrent writers cannot race here because keyspace
    // notifications all dispatch on the Redis main thread, so this callback is
    // serialized against itself and against other notification-driven writers
    // by the event loop, not by the spec lock.
    RedisSearchCtx_LockSpecRead(&sctx);
    const RSDocumentMetadata *cdmd = DocTable_BorrowByKeyR(&spec->docs, key);
    if (cdmd) {
      // Only the doc-level TTL changes here. EXPIRE/PERSIST do not affect
      // HEXPIRE state, and the per-field TTL table must be left untouched —
      // mutating it would also require the spec write lock, which we do not
      // hold on this fast path.
      DocTable_SetDocExpiration((RSDocumentMetadata *)cdmd, ttl);
      DMD_Return(cdmd);
    }
    RedisSearchCtx_UnlockSpec(&sctx);
  }

  Indexes_SpecOpsIndexingCtxFree(specs);
}

void Indexes_DeleteMatchingWithSchemaRules(RedisModuleCtx *ctx, RedisModuleString *key,
                                           DocumentType type,
                                           RedisModuleString **hashFields) {
  SpecOpIndexingCtx *specs = Indexes_FindMatchingSchemaRules(ctx, key, type, false, NULL);

  for (size_t i = 0; i < array_len(specs->specsOps); ++i) {
    SpecOpCtx *specOp = specs->specsOps + i;
    if (hashFieldChanged(specOp->spec, hashFields)) {
      IndexSpec_DeleteDoc(specOp->spec, ctx, key);
    }
  }

  Indexes_SpecOpsIndexingCtxFree(specs);
}

// True iff the spec has any field with INDEXMISSING. Linear scan over the
// schema's fields[] array; called from the HEXPIRE fast path on the main
// thread without the spec lock, which is safe because the schema descriptors
// read here are only mutated by FT.CREATE / FT.ALTER / RDB load on the same
// thread.
static bool specHasIndexMissing(const IndexSpec *spec) {
  for (size_t i = 0; i < spec->numFields; ++i) {
    if (FieldSpec_IndexesMissing(&spec->fields[i])) {
      return true;
    }
  }
  return false;
}

void Indexes_UpdateMatchingHashFieldExpiration(RedisModuleCtx *ctx, RedisModuleString *key,
                                               DocumentType type) {

  if (type == DocumentType_Unsupported) {
    return;
  }
  RS_ASSERT(type == DocumentType_Hash);

  SpecOpIndexingCtx *specs = Indexes_FindMatchingSchemaRules(ctx, key, DocumentType_Hash, false, NULL);

  // HEXPIRE/HPERSIST notifications fire for every hash in the keyspace. When
  // no spec prefix matches we have nothing to update, so skip the OpenKey /
  // HashFieldMinExpire read entirely — that overhead would otherwise be paid
  // on every event on non-indexed hashes.
  if (array_len(specs->specsOps) == 0) {
    Indexes_SpecOpsIndexingCtxFree(specs);
    return;
  }

  RedisModuleKey *k = RedisModule_OpenKey(ctx, key, DOCUMENT_OPEN_KEY_INDEXING_FLAGS);
  RS_ASSERT(k);

  // Hash-level gate: if no field on this hash has a TTL, every per-spec
  // FieldExpiration array is empty, so we can skip the per-field HashGet
  // pass entirely. The Redis hash is owned by the main thread, so this read
  // is safe without the spec lock.
  const bool hashHasAnyFieldExpire =
      (RedisModule_HashFieldMinExpire(k) != REDISMODULE_NO_EXPIRE);

  for (size_t i = 0; i < array_len(specs->specsOps); ++i) {
    SpecOpCtx *specOp = &specs->specsOps[i];
    IndexSpec *spec = specOp->spec;
    // Specs that opted out of HFE tracking have no TTL table state to refresh
    // and HEXPIRE cannot otherwise affect their indexed view of the doc.
    //
    // No spec lock needed for the read: monitorFieldExpiration is only
    // written from main-thread callbacks, and this keyspace-notification
    // callback also runs on the main thread, so the Redis event loop
    // serializes them. The spec write lock guards index data against
    // background workers, not main-thread config flags.
    if (!spec->monitorFieldExpiration) {
      continue;
    }

    // INDEXMISSING relies on reindexing: the missing-docs iterator walks
    // an inverted index that requires monotonically increasing docIds, so
    // we cannot patch it from the fast path. Fall back to full reindex.
    //
    // Indexes_FindMatchingSchemaRules was called with runFilters=false, so
    // the schema FILTER was not evaluated. Re-evaluate it here via
    // SchemaRule_ShouldIndex so an HEXPIRE on a hash whose PREFIX matches
    // but whose FILTER rejects it does not incorrectly add the doc to the
    // index. Matches the SpecOp_Add / SpecOp_Del split the slow path
    // produces in Indexes_UpdateMatchingWithSchemaRules.
    if (specHasIndexMissing(spec)) {
      if (SchemaRule_ShouldIndex(spec, key, type)) {
        IndexSpec_UpdateDoc(spec, ctx, key, type);
      } else {
        IndexSpec_DeleteDoc(spec, ctx, key);
      }
      continue;
    }

    // Build the FieldExpiration array lock-free. Two reads happen outside
    // the spec write lock here:
    //   1. spec->fields / spec->numFields — only mutated by FT.CREATE,
    //      FT.ALTER, and RDB load, all of which run on the main thread.
    //      This keyspace-notification callback also runs on the main
    //      thread, so the Redis event loop serializes them; the schema
    //      cannot change between iterations here. (Same invariant as
    //      specHasIndexMissing above.)
    //   2. The Redis hash — owned by the main thread and not guarded by
    //      the spec lock at all; the spec rwlock protects index state,
    //      not the Redis keyspace.
    FieldExpirations sorted = FieldExpirations_Empty();
    if (hashHasAnyFieldExpire) {
      for (size_t ii = 0; ii < spec->numFields; ++ii) {
        Document_LoadHashFieldExpiration(k, &spec->fields[ii], ii, &sorted);
      }
    }

    RedisSearchCtx sctx = SEARCH_CTX_STATIC(ctx, spec);
    RedisSearchCtx_LockSpecWrite(&sctx);

    const RSDocumentMetadata *cdmd = DocTable_BorrowByKeyR(&spec->docs, key);
    if (cdmd) {
      DocTable_UpdateFieldExpiration(&spec->docs, (RSDocumentMetadata *)cdmd,
                                     DocTable_TakeFieldExpirations(&sorted));
      DMD_Return(cdmd);
    }

    RedisSearchCtx_UnlockSpec(&sctx);

    // Doc not in this index (filter failed or never indexed): free the list
    // we built speculatively. FieldExpirations_Free handles the empty sentinel.
    FieldExpirations_Free(&sorted);
  }

  RedisModule_CloseKey(k);
  Indexes_SpecOpsIndexingCtxFree(specs);
}

void Indexes_ReplaceMatchingWithSchemaRules(RedisModuleCtx *ctx, RedisModuleString *from_key,
                                            RedisModuleString *to_key) {
  DocumentType type = getDocTypeFromString(to_key);
  if (type == DocumentType_Unsupported) {
    return;
  }

  SpecOpIndexingCtx *from_specs = Indexes_FindMatchingSchemaRules(ctx, from_key, type, true, to_key);
  SpecOpIndexingCtx *to_specs = Indexes_FindMatchingSchemaRules(ctx, to_key, type, true, NULL);

  size_t from_len, to_len;
  const char *from_str = RedisModule_StringPtrLen(from_key, &from_len);
  const char *to_str = RedisModule_StringPtrLen(to_key, &to_len);

  // Handle specs that match the old key (whether they match the new key or not)
  for (size_t i = 0; i < array_len(from_specs->specsOps); ++i) {
    SpecOpCtx *specOp = from_specs->specsOps + i;
    IndexSpec *spec = specOp->spec;
    if (specOp->op == SpecOp_Del) {
      // the document is not in the index from the first place
      continue;
    }
    dictEntry *entry = dictFind(to_specs->specs, spec->specName);
    if (entry) {
      // The document should be indexed by the new key as well, so we need to update the key name in the index.
      RedisSearchCtx sctx = SEARCH_CTX_STATIC(ctx, spec);
      RedisSearchCtx_LockSpecWrite(&sctx);

      // Perform the rename
      if (SearchDisk_IsEnabled()) {
        uint64_t docId;
        // After RENAME, the metadata lives on to_key (rename callback keeps it).
        if (DocIdMeta_Get(ctx, to_key, spec->specId, &docId) == REDISMODULE_OK) {
          // Update the key name in the disk doc table
          SearchDisk_ReplaceKey(spec->diskSpec, docId, to_str, to_len);
        }
      } else {
        DocTable_Replace(&spec->docs, from_str, from_len, to_str, to_len);
      }

      RedisSearchCtx_UnlockSpec(&sctx);
      size_t index = entry->v.u64;
      dictDelete(to_specs->specs, spec->specName);
      array_del_fast(to_specs->specsOps, index);
    } else {
      // The document should not be indexed by the new key, so we need to delete the old document from the index.
      if (SearchDisk_IsEnabled()) {
        // After RENAME, from_key no longer exists. The metadata is on to_key.
        // Look up the docId from to_key's metadata and delete by id.
        uint64_t docId;
        if (DocIdMeta_Get(ctx, to_key, spec->specId, &docId) == REDISMODULE_OK) {
          IndexSpec_DeleteDocById(spec, (t_docId)docId);
          DocIdMeta_Delete(ctx, to_key, spec->specId);
        }
      } else {
        // For RAM case, look up by old key name and delete
        IndexSpec_DeleteDoc(spec, ctx, from_key);
      }
    }
  }

  // Handle specs that didn't match the old key but match the new key
  for (size_t i = 0; i < array_len(to_specs->specsOps); ++i) {
    SpecOpCtx *specOp = to_specs->specsOps + i;
    if (specOp->op == SpecOp_Del) {
      // not need to index
      // also no need to delete because we know that the document is
      // not in the index because if it was there we would handle it
      // on the spec from section.
      continue;
    }
    IndexSpec_UpdateDoc(specOp->spec, ctx, to_key, type);
  }
  Indexes_SpecOpsIndexingCtxFree(from_specs);
  Indexes_SpecOpsIndexingCtxFree(to_specs);
}

void Indexes_List(RedisModule_Reply* reply, bool obfuscate) {
  RedisModule_Reply_Set(reply);
  dictIterator *iter = dictGetIterator(specDict_g);
  dictEntry *entry = NULL;
  while ((entry = dictNext(iter))) {
    StrongRef ref = dictGetRef(entry);
    IndexSpec *sp = StrongRef_Get(ref);
    CurrentThread_SetIndexSpec(ref);
    const char *specName = IndexSpec_FormatName(sp, obfuscate);
    REPLY_SIMPLE_SAFE(specName);
    CurrentThread_ClearIndexSpec();
  }
  dictReleaseIterator(iter);
  RedisModule_Reply_SetEnd(reply);
  RedisModule_EndReply(reply);
}

void Indexes_StartRDBLoadingEvent(RedisModuleCtx* ctx) {
  Indexes_Free(ctx, specDict_g, false);
  if (!SearchDisk_IsEnabled()) {
    if (legacySpecDict) {
      dictEmpty(legacySpecDict, NULL);
    } else {
      legacySpecDict = dictCreate(&dictTypeHeapHiddenStrings, NULL);
    }
  }
  g_isLoading = true;
}

void Indexes_EndRDBLoadingEvent(RedisModuleCtx *ctx) {
  int hasLegacyIndexes = dictSize(legacySpecDict);
  Indexes_UpgradeLegacyIndexes();

  // we do not need the legacy dict specs anymore
  dictRelease(legacySpecDict);
  legacySpecDict = NULL;

  LegacySchemaRulesArgs_Free(ctx);

  if (hasLegacyIndexes) {
    Indexes_ScanAndReindex();
  }
}

void Indexes_EndLoading() {
  g_isLoading = false;
}

// Replica-side SST replication: Open with the pending RDB State every spec
// that was staged in this round.
void Indexes_FinishSSTReplication(RedisModuleCtx *ctx) {
  RS_ASSERT(SearchDisk_IsEnabled());
  dictIterator *iter = dictGetIterator(specDict_g);
  dictEntry *entry = NULL;
  while ((entry = dictNext(iter))) {
    StrongRef spec_ref = dictGetRef(entry);
    IndexSpec *sp = StrongRef_Get(spec_ref);
    RS_ASSERT(sp);
    RS_ASSERT(sp->pendingDiskRdbState);
    RS_ASSERT(sp->diskSpec == NULL);
    RS_ASSERT(!sp->diskRegistered);
    bool ok = IndexSpec_SSTRdbOpenAndApply(ctx, sp);
    RS_LOG_ASSERT_ALWAYS(ok, "SST replication: failed to open disk index for spec during LOADING_ENDED");
    // GC start was deferred by IndexSpec_StoreAfterRdbLoad for the SST path
    // (diskSpec was NULL there); start it now that the disk handle exists.
    IndexSpec_StartGC(spec_ref, sp, GCPolicy_Disk);
  }
  dictReleaseIterator(iter);
}

// Replica-side SST replication: abort the in-progress replication round.
//
// Drops every spec that was built or staged in this round, freeing any
// pending disk RDB state and closing or unregistering+closing the disk index
// as appropriate. Specs at every replication phase are torn down — the
// replica is left as if replication had never started, and Flex will retry
// from scratch on the next attempt.
void Indexes_AbortSSTReplicationLoading(RedisModuleCtx *ctx) {
  RS_ASSERT(SearchDisk_IsEnabled());
  if (dictSize(specDict_g) == 0) return;

  RedisModule_Log(ctx, "warning",
                  "SST replication aborted; tearing down %lu staged index(es)",
                  (unsigned long)dictSize(specDict_g));

  // Snapshot the refs first since Indexes_RemoveSpecFromGlobals mutates specDict_g.
  arrayof(StrongRef) specs = array_new(StrongRef, dictSize(specDict_g));
  dictIterator *iter = dictGetIterator(specDict_g);
  dictEntry *entry = NULL;
  while ((entry = dictNext(iter))) {
    array_append(specs, (StrongRef)dictGetRef(entry));
  }
  dictReleaseIterator(iter);

  for (size_t i = 0; i < array_len(specs); ++i) {
    IndexSpec *spec = StrongRef_Get(specs[i]);
    if (!spec) continue;
    // Unregister here (not in the destructor) because the destructor may run
    // on a background thread when the last StrongRef drops, and unregister
    // needs the main-thread RedisModuleCtx. Must precede the close that
    // IndexSpec_FreeUnlinkedData performs. SearchDisk_UnregisterIndex is
    // idempotent, so it safely handles specs aborted before LOADING_ENDED
    // registered them.
    if (spec->diskSpec) {
      SearchDisk_UnregisterIndex(ctx, spec);
    }
    // pendingDiskRdbState and diskSpec are freed by IndexSpec_FreeUnlinkedData
    // once the last StrongRef is dropped below.
    Indexes_RemoveSpecFromGlobals(specs[i], false);
  }
  array_free(specs);
}
