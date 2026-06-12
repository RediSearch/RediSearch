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
#include "indexes_scanner.h"
#include "rules.h"
#include "doc_types.h"
#include "doc_id_meta.h"
#include "rmutil/rm_assert.h"

// Emit a throttled progress line roughly every this many scanned keys, so a long
// backfill is observable in the logs without a line per batch.
#define ASYNC_SCAN_PROGRESS_LOG_KEYS 100000

// Per-cursor driver state, shared between the reindexPool worker (which owns the
// cursor lifecycle) and the callbacks (which run on the main thread under the GIL).
// done_cb signals the condvar the worker waits on; the engine provides no waiter.
typedef struct {
  IndexesScanner *scanner;
  pthread_mutex_t mutex;
  pthread_cond_t cond;
  bool call_done;   // set by done_cb, consumed by the driver's wait
} AsyncReindexDriver;

// Wait (GIL released) until done_cb signals this call's completion, then consume
// the flag. Mirrors wait_for_call_done in the Async Scan requirements §3.1.1.
static void Indexes_AsyncScanWaitForDone(AsyncReindexDriver *driver) {
  pthread_mutex_lock(&driver->mutex);
  while (!driver->call_done) {
    pthread_cond_wait(&driver->cond, &driver->mutex);
  }
  driver->call_done = false;
  pthread_mutex_unlock(&driver->mutex);
}

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

  DocumentType type = getDocType(key);
  if (type == DocumentType_Unsupported) {
    return;
  }

  StrongRef curr_run_ref = IndexSpecRef_Promote(scanner->spec_ref);
  IndexSpec *sp = StrongRef_Get(curr_run_ref);
  // If IndexSpec was dropped mid-scan, cancel.
  scanner->cancelled |= !sp;
  if (!sp) {
    RedisModule_Log(ctx, "notice",
                    "AsyncScan: index %s dropped mid-scan; cancelling (scanned=%zu)",
                    scanner->spec_name_for_logs, scanner->scannedKeys);
  }
  if (!scanner->cancelled) {
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
    RedisModule_AsyncScanAbort(cursor);
  }
  ++scanner->scannedKeys;
}

// Call-completion callback. Runs on the main thread, GIL held, once per
// Start/NextBatch that returned OK. The loop discovers terminal state from the
// return code of the next AsyncScanNextBatch (§3.1.1), so we ignore `reason` here
// and only wake the driver waiting on the condvar.
static void Indexes_AsyncScanDoneCB(RedisModuleCtx *ctx, RedisModuleScanCursor *cursor,
                                    void *privdata, RedisModuleAsyncScanDoneReason reason) {
  REDISMODULE_NOT_USED(ctx);
  REDISMODULE_NOT_USED(cursor);
  REDISMODULE_NOT_USED(reason);
  AsyncReindexDriver *driver = privdata;
  pthread_mutex_lock(&driver->mutex);
  driver->call_done = true;
  pthread_cond_signal(&driver->cond);
  pthread_mutex_unlock(&driver->mutex);
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

  // Progress accounting for the throttled progress log in the drive loop.
  size_t batchesDone = 0;
  size_t lastProgressKeys = 0;

  RedisModule_Log(ctx, "notice", "AsyncScan: scanning index %s in background",
                  scanner->spec_name_for_logs);

  if (scanner->cancelled) {
    RedisModule_Log(ctx, "notice", "AsyncScan: index %s cancelled before scan started",
                    scanner->spec_name_for_logs);
    goto done;
  }

  // First call: bind all parameters to the cursor and queue the first batch. Start
  // is subject to the same max-inflight cap as NextBatch and can return BUSY, so we
  // retry the *same* call (no binding exists yet) until it is accepted. The GIL is
  // held only for the call and released immediately; the backoff sleeps with it
  // released. (Async Scan requirements §3.1.1.)
  RedisModuleAsyncScanResult rc;
  for (;;) {
    RedisModule_ThreadSafeContextLock(ctx);
    rc = RedisModule_AsyncScanStart(ctx, cursor, NULL,
                                    REDISMODULE_ASYNCSCAN_MODE_META_AND_VALUE, NULL,
                                    Indexes_AsyncScanKeyCB, Indexes_AsyncScanDoneCB, &driver);
    RedisModule_ThreadSafeContextUnlock(ctx);
    if (rc != REDISMODULE_ASYNCSCAN_BUSY) {
      break;
    }
    // Cancelled while retrying: the cursor was never bound, so there is nothing to
    // abort — just tear down.
    if (scanner->cancelled) {
      RedisModule_Log(ctx, "notice", "AsyncScan: index %s cancelled while starting",
                      scanner->spec_name_for_logs);
      goto done;
    }
    usleep(1000);
  }

  // Drive the cursor one batch at a time, switching on the result code exactly as
  // the canonical indexer loop in the Async Scan requirements §3.1.1. done_cb only
  // wakes us; terminal state arrives as the return code of the next NextBatch.
  for (;;) {
    switch (rc) {
    case REDISMODULE_ASYNCSCAN_OK:
      // Batch queued. Wait (GIL released) for done_cb to fire on the main thread.
      Indexes_AsyncScanWaitForDone(&driver);
      ++batchesDone;

      // Throttled progress: one line every ASYNC_SCAN_PROGRESS_LOG_KEYS keys so a
      // long backfill is observable without a line per batch.
      if (scanner->scannedKeys - lastProgressKeys >= ASYNC_SCAN_PROGRESS_LOG_KEYS) {
        lastProgressKeys = scanner->scannedKeys;
        RedisModule_Log(ctx, "notice",
                        "AsyncScan: index %s progress: scanned=%zu keys, batches=%zu",
                        scanner->spec_name_for_logs, scanner->scannedKeys, batchesDone);
      }

      // Cancelled (FT.DROP / FT.ALTER, detected per-key): abort the sweep instead of
      // requesting another batch.
      if (scanner->cancelled) {
        RedisModule_Log(ctx, "notice",
                        "AsyncScan: aborting scan of index %s (cancelled, scanned=%zu)",
                        scanner->spec_name_for_logs, scanner->scannedKeys);
        RedisModule_ThreadSafeContextLock(ctx);
        RedisModule_AsyncScanAbort(cursor);
        RedisModule_ThreadSafeContextUnlock(ctx);
        goto done;
      }

      RedisModule_ThreadSafeContextLock(ctx);
      rc = RedisModule_AsyncScanNextBatch(ctx, cursor);
      RedisModule_ThreadSafeContextUnlock(ctx);
      break;

    case REDISMODULE_ASYNCSCAN_BUSY:
      // Concurrency cap full; back off (GIL released) and retry the same NextBatch.
      RedisModule_Log(ctx, "debug", "AsyncScan: index %s throttled (max-inflight); backing off",
                      scanner->spec_name_for_logs);
      usleep(1000);
      RedisModule_ThreadSafeContextLock(ctx);
      rc = RedisModule_AsyncScanNextBatch(ctx, cursor);
      RedisModule_ThreadSafeContextUnlock(ctx);
      break;

    case REDISMODULE_ASYNCSCAN_EXHAUSTED:
      // Natural completion: the full keyspace was swept.
      RedisModule_Log(ctx, "notice",
                      "AsyncScan: index %s completed: scanned=%zu keys in %zu batches",
                      scanner->spec_name_for_logs, scanner->scannedKeys, batchesDone);
      goto done;

    case REDISMODULE_ASYNCSCAN_DATASET_RESET:
      // The sweep ended early: FLUSHALL / DEBUG RELOAD (dataset reset) or an abort.
      // The cursor is no longer usable.
      RedisModule_Log(ctx, "notice", "AsyncScan: scan of index %s ended early (dataset reset, scanned=%zu)",
                      scanner->spec_name_for_logs,
                      scanner->scannedKeys);
      goto done;
    case REDISMODULE_ASYNCSCAN_ABORTED:
      // The sweep ended early: FLUSHALL / DEBUG RELOAD (dataset reset) or an abort.
      // The cursor is no longer usable.
      RedisModule_Log(ctx, "notice", "AsyncScan: scan of index %s ended early (aborted, scanned=%zu)",
                      scanner->spec_name_for_logs,
                      scanner->scannedKeys);
      goto done;

    case REDISMODULE_ASYNCSCAN_OUT_OF_MEMORY:
      // Engine memory pressure cut the sweep short. The index may be incomplete; the
      // engine does not resume transparently (a fresh cursor is Phase 2).
      RedisModule_Log(ctx, "warning",
                      "AsyncScan: engine OOM during scan of index %s; index may be incomplete (scanned=%zu)",
                      scanner->spec_name_for_logs, scanner->scannedKeys);
      goto done;

    case REDISMODULE_ASYNCSCAN_IN_PROGRESS:
      // Should not happen: we never reissue before done_cb fires.
      RedisModule_Log(ctx, "warning", "AsyncScan: unexpected IN_PROGRESS for index %s (caller bug)",
                      scanner->spec_name_for_logs);
      RS_ASSERT(false);
      goto done;

    case REDISMODULE_ASYNCSCAN_UNSUPPORTED:
      // AsyncScan unavailable in this build/runtime. We only route disk indexes
      // here, where it is guaranteed present; an RM_Scan fallback is Phase 2.
      RedisModule_Log(ctx, "warning", "AsyncScan: unsupported for index %s",
                      scanner->spec_name_for_logs);
      RS_ASSERT(false);
      goto done;

    case REDISMODULE_ASYNCSCAN_INVALID:
      // Bad argument — programming error. Cursor state is unchanged; no done_cb.
      RedisModule_Log(ctx, "warning", "AsyncScan: invalid argument for index %s",
                      scanner->spec_name_for_logs);
      RS_ASSERT(false);
      goto done;
    }
  }

done:
  // Each exit path above logs its own outcome (completed / cancelled / ended early /
  // OOM / error); this is just the teardown marker.
  RedisModule_Log(ctx, "debug", "AsyncScan: index %s scan task exiting (scanned=%zu)",
                  scanner->spec_name_for_logs, scanner->scannedKeys);

  IndexesScanner_Free(scanner);
  pthread_cond_destroy(&driver.cond);
  pthread_mutex_destroy(&driver.mutex);
  // Safe: by here the cursor is terminal or never started (not in the in-flight
  // window), which RM_ScanCursorDestroy requires.
  RedisModule_ScanCursorDestroy(cursor);
  RedisModule_FreeThreadSafeContext(ctx);
}
