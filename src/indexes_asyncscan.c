/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "indexes_asyncscan.h"

#include <pthread.h>
#include <unistd.h>

#include "redismodule.h"
#include "spec.h"
#include "indexes_scan.h"
#include "rules.h"
#include "doc_types.h"
#include "doc_id_meta.h"
#include "rmutil/rm_assert.h"

// Per-cursor driver state, shared between the reindexPool worker (which owns the
// cursor lifecycle) and the callbacks (which run on the main thread under the GIL).
// done_cb signals the condvar the worker waits on; the engine provides no waiter.
typedef struct {
  IndexesScanner *scanner;
  pthread_mutex_t mutex;
  pthread_cond_t cond;
  bool batch_done;                          // set by done_cb, cleared by the driver
  RedisModuleAsyncScanDoneReason reason;    // reason from the last done_cb
} AsyncReindexDriver;

// Per-key callback. Runs on the main thread, GIL held, value pinned for the call.
// Mirrors the per-spec branch of Indexes_ScanProc, with an idempotency guard: at
// least-once delivery means a key may arrive more than once (or after a live
// notification already indexed it), so we skip keys that already have a DocIdMeta
// mapping for this spec and index the rest through the normal pipeline.
static void Indexes_AsyncScanKeyCB(RedisModuleCtx *ctx, RedisModuleScanCursor *cursor,
                                   RedisModuleKey *key, RedisModuleString *name, void *privdata) {
  REDISMODULE_NOT_USED(cursor);
  AsyncReindexDriver *driver = privdata;
  IndexesScanner *scanner = driver->scanner;

  if (scanner->cancelled) {
    return;
  }

  if (isBgIndexingMemoryOverLimit(ctx)) {
    scanner->scanFailedOnOOM = true;
    if (scanner->OOMkey) {
      RedisModule_FreeString(RSDummyContext, scanner->OOMkey);
    }
    scanner->OOMkey = RedisModule_HoldString(RSDummyContext, name);
    return;
  }

  DocumentType type = getDocType(key);
  if (type == DocumentType_Unsupported) {
    return;
  }

  StrongRef curr_run_ref = IndexSpecRef_Promote(scanner->spec_ref);
  IndexSpec *sp = StrongRef_Get(curr_run_ref);
  if (sp) {
    // Safe to read without locking the spec: we hold the GIL, so the main thread
    // is not mutating it and GC is not touching the relevant data.
    if (SchemaRule_ShouldIndex(sp, name, type)) {
      uint64_t docId = 0;
      if (DocIdMeta_Get(ctx, name, sp->specId, &docId) == REDISMODULE_OK && docId != 0) {
        // Already indexed in this spec (earlier delivery or a live notification);
        // skip to stay idempotent and avoid clobbering a fresher version.
      } else {
        IndexSpec_UpdateDoc(sp, ctx, name, type);
      }
    }
    IndexSpecRef_Release(curr_run_ref);
  } else {
    // Spec was dropped mid-scan; cancel.
    scanner->cancelled = true;
  }
  ++scanner->scannedKeys;
}

// Call-completion callback. Runs on the main thread, GIL held, once per
// Start/NextBatch that returned OK. Records the terminal reason and wakes the
// driver waiting on the condvar.
static void Indexes_AsyncScanDoneCB(RedisModuleCtx *ctx, RedisModuleScanCursor *cursor,
                                    void *privdata, RedisModuleAsyncScanDoneReason reason) {
  REDISMODULE_NOT_USED(ctx);
  REDISMODULE_NOT_USED(cursor);
  AsyncReindexDriver *driver = privdata;
  pthread_mutex_lock(&driver->mutex);
  driver->reason = reason;
  driver->batch_done = true;
  pthread_cond_signal(&driver->cond);
  pthread_mutex_unlock(&driver->mutex);
}

// Issue Start (first==true) or NextBatch, retrying transient BUSY. The caller must
// NOT hold the GIL; we take it for the call and release it immediately after
// (required by the API), arming batch_done before queuing the drain. Returns the
// first non-BUSY result, or BUSY if the scan was cancelled while retrying.
static RedisModuleAsyncScanResult Indexes_AsyncScanIssue(RedisModuleCtx *ctx,
                                                         RedisModuleScanCursor *cursor,
                                                         AsyncReindexDriver *driver, bool first) {
  for (;;) {
    RedisModule_ThreadSafeContextLock(ctx);
    // Arm before queuing: done_cb can only run once we release the GIL.
    pthread_mutex_lock(&driver->mutex);
    driver->batch_done = false;
    pthread_mutex_unlock(&driver->mutex);
    RedisModuleAsyncScanResult rc =
        first ? RedisModule_AsyncScanStart(ctx, cursor, NULL,
                                           REDISMODULE_ASYNCSCAN_MODE_META_AND_VALUE, NULL,
                                           Indexes_AsyncScanKeyCB, Indexes_AsyncScanDoneCB, driver)
              : RedisModule_AsyncScanNextBatch(ctx, cursor);
    RedisModule_ThreadSafeContextUnlock(ctx);
    if (rc != REDISMODULE_ASYNCSCAN_BUSY || driver->scanner->cancelled) {
      return rc;
    }
    usleep(1000);
  }
}

void Indexes_AsyncScanAndReindexTask(IndexesScanner *scanner) {
  RS_LOG_ASSERT(scanner, "invalid IndexesScanner");

  RedisModuleCtx *ctx = RedisModule_GetDetachedThreadSafeContext(RSDummyContext);
  // Name the cursor after the index, as the spec recommends for INFO observability.
  RedisModuleScanCursor *cursor = RedisModule_ScanCursorCreateWithName(scanner->spec_name_for_logs);

  AsyncReindexDriver driver = {0};
  driver.scanner = scanner;
  pthread_mutex_init(&driver.mutex, NULL);
  pthread_cond_init(&driver.cond, NULL);

  RedisModule_Log(ctx, "notice", "AsyncScan: scanning index %s in background",
                  scanner->spec_name_for_logs);

  RedisModuleAsyncScanResult rc = REDISMODULE_ASYNCSCAN_OK;
  if (scanner->cancelled) {
    goto cleanup;
  }

  rc = Indexes_AsyncScanIssue(ctx, cursor, &driver, true);
  while (rc == REDISMODULE_ASYNCSCAN_OK) {
    // Wait for this batch's done_cb (fires on the main thread under the GIL).
    pthread_mutex_lock(&driver.mutex);
    while (!driver.batch_done) {
      pthread_cond_wait(&driver.cond, &driver.mutex);
    }
    RedisModuleAsyncScanDoneReason reason = driver.reason;
    pthread_mutex_unlock(&driver.mutex);

    // COMPLETED, or a terminal ABORTED / DATASET_RESET / OUT_OF_MEMORY — the
    // cursor is finished and no further batch can be requested.
    if (reason != REDISMODULE_ASYNCSCAN_DONE_BATCH_DONE) {
      break;
    }

    // Minimal OOM handling (pause/retry is Phase 2): a key_cb hit the memory
    // limit. Record the failure and fall through to the cancellation/abort path.
    if (scanner->scanFailedOnOOM && !scanner->cancelled) {
      RedisModule_ThreadSafeContextLock(ctx);
      scanStopAfterOOM(ctx, scanner);
      RedisModule_ThreadSafeContextUnlock(ctx);
    }

    if (scanner->cancelled) {
      RedisModule_ThreadSafeContextLock(ctx);
      RedisModule_AsyncScanAbort(cursor);
      RedisModule_ThreadSafeContextUnlock(ctx);
      break;
    }

    rc = Indexes_AsyncScanIssue(ctx, cursor, &driver, false);
  }

  RedisModule_Log(ctx, "notice", "AsyncScan: scanning index %s in background: done (scanned=%zu)",
                  scanner->spec_name_for_logs, scanner->scannedKeys);

cleanup:
  IndexesScanner_Free(scanner);
  pthread_cond_destroy(&driver.cond);
  pthread_mutex_destroy(&driver.mutex);
  // Safe: by here the cursor is terminal or never started (not in the in-flight
  // window), which RM_ScanCursorDestroy requires.
  RedisModule_ScanCursorDestroy(cursor);
  RedisModule_FreeThreadSafeContext(ctx);
}
