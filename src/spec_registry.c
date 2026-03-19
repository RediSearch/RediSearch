/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#include "spec.h"
#include "rmalloc.h"
#include "rmutil/rm_assert.h"
#include "config.h"
#include "cursor.h"
#include "alias.h"
#include "module.h"
#include "rules.h"
#include "doc_types.h"
#include "commands.h"
#include "notifications.h"
#include "dictionary.h"
#include "util/dict.h"
#include "util/workers.h"
#include "util/logging.h"
#include "obfuscation/obfuscation_api.h"
#include "info/global_stats.h"
#include "info/info_redis/threads/current_thread.h"
#include "info/field_spec_info.h"
#include "reply_macros.h"
#include "search_disk.h"
#include "rlookup_load_document.h"
#include "aggregate/expr/expression.h"
#include "triemap.h"

extern RedisModuleCtx *RSDummyContext;
extern void Cursors_initSpec(struct IndexSpec *spec);
int IndexSpec_UpdateDoc(IndexSpec *spec, RedisModuleCtx *ctx, RedisModuleString *key, DocumentType type);

///////////////////////////////////////////////////////////////////////////////////////////////
// Globals owned by the registry
///////////////////////////////////////////////////////////////////////////////////////////////

dict *specDict_g = NULL;
size_t pending_global_indexing_ops = 0;
dict *legacySpecDict;
dict *legacySpecRules;

// Pending or in-progress index drops
uint16_t pendingIndexDropCount_g = 0;

static redisearch_thpool_t *cleanPool = NULL;

///////////////////////////////////////////////////////////////////////////////////////////////
// Temporary index timer management
///////////////////////////////////////////////////////////////////////////////////////////////

static void IndexSpec_TimedOutProc(RedisModuleCtx *ctx, WeakRef w_ref) {
  // we need to delete the spec from the specDict_g, as far as the user see it,
  // this spec was deleted and its memory will be freed in a background thread.

  // attempt to promote the weak ref to a strong ref
  StrongRef spec_ref = WeakRef_Promote(w_ref);
  WeakRef_Release(w_ref);

  IndexSpec *sp = StrongRef_Get(spec_ref);
  if (!sp) {
    // the spec was already deleted, nothing to do here
    return;
  }
  const char* name = IndexSpec_FormatName(sp, RSGlobalConfig.hideUserDataFromLog);
  RedisModule_Log(RSDummyContext, REDISMODULE_LOGLEVEL_VERBOSE, "Freeing index %s by timer", name);

  sp->isTimerSet = false;
  if (RS_IsMock) {
    IndexSpec_Free(sp);
  } else {
    // called on master shard for temporary indexes and deletes all documents by defaults
    // pass FT.DROPINDEX with "DD" flag to self.
    RedisModuleCallReply *rep = RedisModule_Call(RSDummyContext, RS_DROP_INDEX_CMD, "cc!", HiddenString_GetUnsafe(sp->specName, NULL), "DD");
    if (rep) {
      RedisModule_FreeCallReply(rep);
    }
  }

  RedisModule_Log(RSDummyContext, REDISMODULE_LOGLEVEL_VERBOSE, "Freeing index '%s' by timer: done", name);
  StrongRef_Release(spec_ref);
}

// Assuming the GIL is held.
// This can be done without locking the spec for write, since the timer is not modified or read by any other thread.
static void IndexSpec_SetTimeoutTimer(IndexSpec *sp, WeakRef spec_ref) {
  if (sp->isTimerSet) {
    WeakRef old_timer_ref;
    if (RedisModule_StopTimer(RSDummyContext, sp->timerId, (void **)&old_timer_ref) == REDISMODULE_OK) {
      WeakRef_Release(old_timer_ref);
    }
  }
  sp->timerId = RedisModule_CreateTimer(RSDummyContext, sp->timeout,
                                        (RedisModuleTimerProc)IndexSpec_TimedOutProc, spec_ref.rm);
  sp->isTimerSet = true;
}

// Assuming the spec is properly guarded before calling this function (GIL or write lock).
static void IndexSpec_ResetTimeoutTimer(IndexSpec *sp) {
  if (sp->isTimerSet) {
    WeakRef old_timer_ref;
    if (RedisModule_StopTimer(RSDummyContext, sp->timerId, (void **)&old_timer_ref) == REDISMODULE_OK) {
      WeakRef_Release(old_timer_ref);
    }
  }
  sp->timerId = 0;
  sp->isTimerSet = false;
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

///////////////////////////////////////////////////////////////////////////////////////////////
// Index existence check
///////////////////////////////////////////////////////////////////////////////////////////////

static bool checkIfSpecExists(const char *rawSpecName) {
  bool found = false;
  HiddenString* specName = NewHiddenString(rawSpecName, strlen(rawSpecName), false);
  found = dictFetchValue(specDict_g, specName);
  HiddenString_Free(specName, false);
  return found;
}

///////////////////////////////////////////////////////////////////////////////////////////////
// Index creation
///////////////////////////////////////////////////////////////////////////////////////////////

/* Create a new index spec from a redis command */
// TODO: multithreaded: use global metadata locks to protect global data structures
IndexSpec *IndexSpec_CreateNew(RedisModuleCtx *ctx, RedisModuleString **argv, int argc,
                               QueryError *status) {
  const char *rawSpecName = RedisModule_StringPtrLen(argv[1], NULL);
  setMemoryInfo(ctx);
  if (checkIfSpecExists(rawSpecName)) {
    QueryError_SetCode(status, QUERY_ERROR_CODE_INDEX_EXISTS);
    return NULL;
  }
  size_t nameLen;
  const char *rawName = RedisModule_StringPtrLen(argv[1], &nameLen);
  HiddenString *name = NewHiddenString(rawName, nameLen, true);
  // Create the IndexSpec, along with its corresponding weak\strong refs
  StrongRef spec_ref = IndexSpec_ParseRedisArgs(ctx, name, &argv[2], argc - 2, status);
  IndexSpec *sp = StrongRef_Get(spec_ref);
  if (sp == NULL) {
    return NULL;
  }

  // Add the spec to the global spec dictionary
  if (dictAdd(specDict_g, name, spec_ref.rm) != DICT_OK) {
    RedisModule_Log(ctx, "warning", "Failed adding index to global dictionary");
    StrongRef_Release(spec_ref);
    RS_ABORT("dictAdd shouldn't fail here - index shouldn't exists in the dictionary");
    return NULL;
  }
  // Start the garbage collector
  IndexSpec_StartGC(spec_ref, sp, sp->diskSpec ? GCPolicy_Disk : GCPolicy_Fork);

  Cursors_initSpec(sp);

  // set timeout for temporary index on master
  if ((sp->flags & Index_Temporary) && IsMaster()) {
    IndexSpec_SetTimeoutTimer(sp, StrongRef_Demote(spec_ref));
  }

  // (Lazily) Subscribe to keyspace notifications, now that we have at least one
  // spec
  Initialize_KeyspaceNotifications();

  if (!(sp->flags & Index_SkipInitialScan)) {
    IndexSpec_ScanAndReindex(ctx, spec_ref);
  }
  return sp;
}

///////////////////////////////////////////////////////////////////////////////////////////////
// Clean pool & pending drops
///////////////////////////////////////////////////////////////////////////////////////////////

void CleanPool_ThreadPoolStart() {
  if (!cleanPool) {
    cleanPool = redisearch_thpool_create(1, DEFAULT_HIGH_PRIORITY_BIAS_THRESHOLD, LogCallback, "cleanPool");
  }
}

void CleanPool_ThreadPoolDestroy() {
  if (cleanPool) {
    RedisModule_ThreadSafeContextUnlock(RSDummyContext);
    redisearch_thpool_destroy(cleanPool);
    cleanPool = NULL;
    RedisModule_ThreadSafeContextLock(RSDummyContext);
  }
}

uint16_t getPendingIndexDrop() {
  return __atomic_load_n(&pendingIndexDropCount_g, __ATOMIC_RELAXED);
}

void addPendingIndexDrop() {
  __atomic_add_fetch(&pendingIndexDropCount_g, 1, __ATOMIC_RELAXED);
}

void removePendingIndexDrop() {
  __atomic_sub_fetch(&pendingIndexDropCount_g, 1, __ATOMIC_RELAXED);
}

size_t CleanInProgressOrPending() {
  return getPendingIndexDrop();
}

void CleanPool_AddWork(void (*proc)(void *), void *arg) {
  redisearch_thpool_add_work(cleanPool, (redisearch_thpool_proc)proc, arg, THPOOL_PRIORITY_HIGH);
}

///////////////////////////////////////////////////////////////////////////////////////////////
// Index removal
///////////////////////////////////////////////////////////////////////////////////////////////

// Assumes this is called from the main thread with no competing threads
// Also assumes that the spec is existing in the global dictionary, so
// we use the global reference as our guard and access the spec directly.
// This function consumes the Strong reference it gets
void IndexSpec_RemoveFromGlobals(StrongRef spec_ref, bool removeActive) {
  IndexSpec *spec = StrongRef_Get(spec_ref);

  // Remove spec from global index list
  dictDelete(specDict_g, (void*)spec->specName);

  if (!spec->isDuplicate) {
    // Remove spec from global aliases list
    IndexSpec_ClearAliases(spec_ref);
  }

  SchemaPrefixes_RemoveSpec(spec_ref);

  // For temporary index
  // We are dropping the index from the mainthread, but the freeing process might happen later from
  // another thread. We cannot deal with timers from other threads, so we need to stop the timer
  // now. We don't need it anymore anyway.
  if (spec->isTimerSet) {
    WeakRef old_timer_ref;
    if (RedisModule_StopTimer(RSDummyContext, spec->timerId, (void **)&old_timer_ref) == REDISMODULE_OK) {
      WeakRef_Release(old_timer_ref);
    }
    spec->isTimerSet = false;
  }

  // Remove spec's fields from global statistics
  for (size_t i = 0; i < spec->numFields; i++) {
    FieldSpec *field = spec->fields + i;
    FieldsGlobalStats_UpdateStats(field, -1);
    FieldsGlobalStats_UpdateIndexError(field->types, -FieldSpec_GetIndexErrorCount(field));
  }

  // Mark there are pending index drops.
  // if ref count is > 1, the actual cleanup will be done only when StrongRefs are released.
  addPendingIndexDrop();

  // Nullify the spec's quick access to the strong ref. (doesn't decrement references count).
  spec->own_ref = (StrongRef){0};

  if (removeActive) {
    // Remove thread from active-threads container
    CurrentThread_ClearIndexSpec();
  }

  // mark the spec as deleted and decrement the ref counts owned by the global dictionaries
  StrongRef_Invalidate(spec_ref);
  StrongRef_Release(spec_ref);
}

void Indexes_Free(dict *d, bool deleteDiskData) {
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
    // Delete disk index before removing from globals
    IndexSpec *spec = StrongRef_Get(specs[i]);
    if (deleteDiskData && spec && spec->diskSpec) {
      SearchDisk_MarkIndexForDeletion(spec->diskSpec);
    }
    IndexSpec_RemoveFromGlobals(specs[i], false);
  }
  array_free(specs);
}

///////////////////////////////////////////////////////////////////////////////////////////////
// Index load/lookup
///////////////////////////////////////////////////////////////////////////////////////////////

// atomic update of usage counter
inline static void IndexSpec_IncreasCounter(IndexSpec *sp) {
  __atomic_fetch_add(&sp->counter , 1, __ATOMIC_RELAXED);
}

StrongRef IndexSpec_LoadUnsafe(const char *name) {
  IndexLoadOptions lopts = {.nameC = name};
  return IndexSpec_LoadUnsafeEx(&lopts);
}

StrongRef IndexSpec_LoadUnsafeEx(IndexLoadOptions *options) {
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

  if (!(options->flags & INDEXSPEC_LOAD_NOCOUNTERINC)){
    // Increment the number of uses.
    IndexSpec_IncreasCounter(sp);
  }

  if (!RS_IsMock && (sp->flags & Index_Temporary) && !(options->flags & INDEXSPEC_LOAD_NOTIMERUPDATE)) {
    IndexSpec_SetTimeoutTimer(sp, StrongRef_Demote(spec_ref));
  }
  return spec_ref;
}

///////////////////////////////////////////////////////////////////////////////////////////////
// Flush event & initialization
///////////////////////////////////////////////////////////////////////////////////////////////

static void onFlush(RedisModuleCtx *ctx, RedisModuleEvent eid, uint64_t subevent, void *data) {
  if (subevent != REDISMODULE_SUBEVENT_FLUSHDB_START) {
    return;
  }
  if (specDict_g) {
    Indexes_Free(specDict_g, true);
    // specDict_g itself is not actually freed
  }
  Dictionary_Clear();
  RSGlobalStats.totalStats.used_dialects = 0;
}

void Indexes_Init(RedisModuleCtx *ctx) {
  if (!specDict_g) {
    specDict_g = dictCreate(&dictTypeHeapHiddenStrings, NULL);
  }
  RedisModule_SubscribeToServerEvent(ctx, RedisModuleEvent_FlushDB, onFlush);
  SchemaPrefixes_Create();
}

size_t Indexes_Count() {
  return dictSize(specDict_g);
}

///////////////////////////////////////////////////////////////////////////////////////////////
// Schema-rule matching dispatch
///////////////////////////////////////////////////////////////////////////////////////////////

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
    // COPY could overwrite a hash/json with other types so we must try and remove old doc
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
        IndexSpec_DeleteDoc(specOp->spec, ctx, key);
      }
    }
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

  for (size_t i = 0; i < array_len(from_specs->specsOps); ++i) {
    SpecOpCtx *specOp = from_specs->specsOps + i;
    IndexSpec *spec = specOp->spec;
    if (specOp->op == SpecOp_Del) {
      // the document is not in the index from the first place
      continue;
    }
    dictEntry *entry = dictFind(to_specs->specs, spec->specName);
    if (entry) {
      RedisSearchCtx sctx = SEARCH_CTX_STATIC(ctx, spec);
      RedisSearchCtx_LockSpecWrite(&sctx);
      DocTable_Replace(&spec->docs, from_str, from_len, to_str, to_len);
      RedisSearchCtx_UnlockSpec(&sctx);
      size_t index = entry->v.u64;
      dictDelete(to_specs->specs, spec->specName);
      array_del_fast(to_specs->specsOps, index);
    } else {
      IndexSpec_DeleteDoc(spec, ctx, from_key);
    }
  }

  // add to a different index
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
