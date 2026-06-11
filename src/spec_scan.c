/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "spec_scan.h"

#include "index_scan.h"
#include "doc_types.h"
#include "document.h"
#include "rmalloc.h"

// Privdata for a single-index reindex: a weak ref to the spec being scanned.
typedef struct {
  WeakRef spec_ref;
} SpecScanTarget;

// The one in-flight global (scan-all) scanner, or NULL. Main-thread only.
static IndexesScanner *g_globalScan = NULL;

// Resolve a scanned key's document type. `key` is best-effort from RM_Scan and may be
// NULL (and is unreliable under CRDT), so re-open when needed.
static DocumentType resolveDocType(RedisModuleCtx *ctx, RedisModuleString *keyname, RedisModuleKey *key) {
  bool opened = false;
  if (!key || isCrdt) {
    key = RedisModule_OpenKey(ctx, keyname, DOCUMENT_OPEN_KEY_INDEXING_FLAGS);
    opened = true;
  }
  DocumentType type = getDocType(key);
  if (opened) {
    RedisModule_CloseKey(key);
  }
  return type;
}

//---------------------------------------------------------------------------------------------
// Single-index reindex callbacks

static IndexScanResult specScan_processKey(void *pd, RedisModuleCtx *ctx,
                                           RedisModuleString *keyname, RedisModuleKey *key) {
  SpecScanTarget *t = pd;
  DocumentType type = resolveDocType(ctx, keyname, key);
  if (type == DocumentType_Unsupported) {
    return INDEX_SCAN_SKIP;
  }
  StrongRef ref = IndexSpecRef_Promote(t->spec_ref);
  IndexSpec *sp = StrongRef_Get(ref);
  if (!sp) {
    // spec was dropped mid-scan — stop.
    return INDEX_SCAN_STOP;
  }
  // Safe to read without locking the spec: we hold the GIL, so the main thread is not
  // mutating it and GC is not touching the relevant data.
  if (SchemaRule_ShouldIndex(sp, keyname, type)) {
    IndexSpec_UpdateDoc(sp, ctx, keyname, type);
  }
  IndexSpecRef_Release(ref);
  return INDEX_SCAN_CONTINUE;
}

static void specScan_onOOM(void *pd, RedisModuleCtx *ctx, const char *error, RedisModuleString *oom_key) {
  SpecScanTarget *t = pd;
  StrongRef ref = WeakRef_Promote(t->spec_ref);
  IndexSpec *sp = StrongRef_Get(ref);
  if (sp) {
    // Error message does not contain user data.
    sp->scan_failed_OOM = true;
    IndexError_AddError(&sp->stats.indexError, error, error, oom_key);
    IndexError_RaiseBackgroundIndexFailureFlag(&sp->stats.indexError);
    StrongRef_Release(ref);
  } else {
    RedisModule_Log(ctx, "notice", "Background scan cancelled due to OOM and index was dropped");
  }
}

static void specScan_onFinished(void *pd, IndexesScanner *scanner) {
  SpecScanTarget *t = pd;
  StrongRef ref = WeakRef_Promote(t->spec_ref);
  IndexSpec *sp = StrongRef_Get(ref);
  if (sp) {
    if (sp->scanner == scanner) {
      sp->scanner = NULL;
      sp->scan_in_progress = false;
    }
    StrongRef_Release(ref);
  }
  WeakRef_Release(t->spec_ref);
  rm_free(t);
}

static const IndexScanCallbacks SPEC_SCAN_CBS = {
  .process_key = specScan_processKey,
  .on_oom = specScan_onOOM,
  .on_finished = specScan_onFinished,
};

//---------------------------------------------------------------------------------------------
// Global (scan-all) callbacks

static IndexScanResult globalScan_processKey(void *pd, RedisModuleCtx *ctx,
                                             RedisModuleString *keyname, RedisModuleKey *key) {
  REDISMODULE_NOT_USED(pd);
  DocumentType type = resolveDocType(ctx, keyname, key);
  if (type == DocumentType_Unsupported) {
    return INDEX_SCAN_SKIP;
  }
  Indexes_UpdateMatchingWithSchemaRules(ctx, keyname, type, NULL);
  return INDEX_SCAN_CONTINUE;
}

static void globalScan_onFinished(void *pd, IndexesScanner *scanner) {
  REDISMODULE_NOT_USED(pd);
  if (!IndexScan_IsCancelled(scanner)) {
    Indexes_SetTempSpecsTimers(TimerOp_Add);
  }
  g_globalScan = NULL;
}

static const IndexScanCallbacks GLOBAL_SCAN_CBS = {
  .process_key = globalScan_processKey,
  .on_oom = NULL,  // no single target to attach the error to
  .on_finished = globalScan_onFinished,
};

//---------------------------------------------------------------------------------------------

static void IndexSpec_ScanAndReindexAsync(StrongRef spec_ref) {
  IndexSpec *spec = StrongRef_Get(spec_ref);

  // scan already in progress? cancel it and restart, keeping the in-progress flag on.
  if (spec->scanner) {
    IndexScan_Cancel(spec->scanner);
    RedisModule_Log(RSDummyContext, "notice", "Scanning index %s in background: cancelled and restarted",
                    IndexSpec_FormatName(spec, RSGlobalConfig.hideUserDataFromLog));
  }

  SpecScanTarget *t = rm_calloc(1, sizeof(*t));
  t->spec_ref = StrongRef_Demote(spec_ref);

  IndexesScanner *scanner =
      IndexScan_Start(IndexSpec_FormatName(spec, RSGlobalConfig.hideUserDataFromLog),
                      /*global=*/false, &SPEC_SCAN_CBS, t);
  spec->scanner = scanner;
  spec->scan_in_progress = true;
}

// Assumes that the spec is in a safe state to set a scanner on it (write lock or main thread)
void IndexSpec_ScanAndReindex(RedisModuleCtx *ctx, StrongRef spec_ref) {
  size_t nkeys = RedisModule_DbSize(ctx);
  if (nkeys > 0) {
    IndexSpec_ScanAndReindexAsync(spec_ref);
  }
}

void Indexes_ScanAndReindex(void) {
  // check no global scan is in progress
  if (g_globalScan) {
    return;
  }
  RedisModule_Log(RSDummyContext, "notice", "Scanning all indexes");
  g_globalScan = IndexScan_Start(NULL, /*global=*/true, &GLOBAL_SCAN_CBS, NULL);
}

bool Indexes_IsScanInProgress(const IndexSpec *sp) {
  return g_globalScan != NULL || sp->scan_in_progress;
}

double Indexes_ScanIndexedPercent(RedisModuleCtx *ctx, const IndexSpec *sp) {
  IndexesScanner *scanner = g_globalScan ? g_globalScan : sp->scanner;
  if (scanner || sp->scan_in_progress) {
    if (scanner) {
      size_t totalKeys = RedisModule_DbSize(ctx);
      return totalKeys > 0 ? (double)IndexScan_ScannedKeys(scanner) / totalKeys : 0;
    }
    return 0;
  }
  return 1.0;
}
