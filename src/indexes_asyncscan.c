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
#include <stdbool.h>
#include <stdint.h>

#include "redismodule.h"
#include "spec.h"
#include "indexes_scanner.h"
#include "rules.h"
#include "doc_types.h"
#include "doc_id_meta.h"
#include "debug_commands.h"
#include "rmutil/rm_assert.h"
#include "rmalloc.h"
#include "rs_wall_clock.h"
#include "search_disk.h"
#include "document_rs.h"
#include "obfuscation/hidden_unicode.h"
#include "util/arr/arr.h"
#include "util/references.h"

// Debug context (owned by debug_commands.c); read only for the SET_SIMULATE_ASYNC_OOM hook.
extern DebugCTX globalDebugCtx;

// Emit a progress log line roughly every this many scanned keys.
#define ASYNC_SCAN_PROGRESS_LOG_KEYS 100000

// Give up if the initial Start keeps returning BUSY (max-inflight saturated) for this long,
// so a wedged engine can't pin a reindex worker spinning forever.
#define ASYNC_SCAN_START_BUSY_TIMEOUT_MS 60000

// Backoff between BUSY retries of Start / NextBatch.
#define ASYNC_SCAN_BUSY_BACKOFF_US 1000

// Per-batch work hint for Start / NextBatch: sizes the hash range each batch sweeps. A hint,
// not a delivery cap; passed explicitly so we don't defer to the engine's default (128).
#define ASYNC_SCAN_BATCH_SIZE_HINT_DEFAULT 100

// While parked behind the vector flat-buffer throttle, re-check the index still exists every
// this many backoff iterations: FT.DROPINDEX / FLUSHDB don't set cancelled, and the throttle
// is global (another index can hold it after ours is gone), so its clearing is not a reliable
// drop wake-up. ~1s at the backoff interval; coarse because mid-scan drops are rare.
#define ASYNC_SCAN_THROTTLE_DROP_RECHECK_ITERS 1000

// Per-cursor driver state, shared between the reindex worker (owns the cursor) and the
// callbacks (main thread, GIL held). done_cb signals the condvar the worker waits on.
typedef struct {
  IndexesScanner *scanner;
  pthread_mutex_t mutex;
  pthread_cond_t cond;
  bool call_done;   // set by done_cb, consumed by the driver's wait
} AsyncReindexDriver;

// Wait (GIL released) until done_cb signals this call's completion, then consume the flag.
static void Indexes_AsyncScanWaitForDone(AsyncReindexDriver *driver) {
  pthread_mutex_lock(&driver->mutex);
  while (!driver->call_done) {
    pthread_cond_wait(&driver->cond, &driver->mutex);
  }
  driver->call_done = false;
  pthread_mutex_unlock(&driver->mutex);
}

// Per-key callback. Main thread, GIL held, value pinned. AsyncScan is at-least-once, so a key
// may arrive more than once; the DocIdMeta guard below skips keys already indexed in this spec.
static void Indexes_AsyncScanKeyCB(RedisModuleCtx *ctx, RedisModuleScanCursor *cursor,
                                   RedisModuleKey *key, RedisModuleString *name, void *privdata) {
  REDISMODULE_NOT_USED(cursor);
  AsyncReindexDriver *driver = privdata;
  IndexesScanner *scanner = driver->scanner;

  if (IndexesScanner_IsCancelled(scanner) || scanner->scanFailedOnOOM) {
    return;
  }

  if (isAsyncBgIndexingMemoryOverLimit(ctx)) {
    scanner->scanFailedOnOOM = true;
    if (scanner->OOMkey) {
      RedisModule_FreeString(RSDummyContext, scanner->OOMkey);
    }
    // Hold the key that triggered OOM in case we need to attach an index error.
    scanner->OOMkey = RedisModule_HoldString(RSDummyContext, name);
    return;
  }

  DocumentType type = getDocType(key);
  if (type == DocumentType_Unsupported) {
    return;
  }

  StrongRef curr_run_ref = IndexSpecRef_Promote(scanner->spec_ref);
  IndexSpec *sp = StrongRef_Get(curr_run_ref);
  if (!sp) {
    // IndexSpec dropped mid-scan: cancel.
    IndexesScanner_Cancel(scanner);
    RedisModule_Log(ctx, "notice",
                    "AsyncScan: index %s dropped mid-scan; cancelling (scanned=%zu)",
                    scanner->spec_name_for_logs, scanner->scannedKeys);
  }
  if (!IndexesScanner_IsCancelled(scanner)) {
    // GIL held → sp and its rule stay stable across ShouldIndex and the DocIdMeta write.
    // ShouldIndex is the authority (it also evaluates the index FILTER expression the pre-filter
    // can't); the redundant type/prefix check is cheap. `key` is already pinned, so pass it
    // through instead of reopening by name.
    if (SchemaRule_ShouldIndex(sp, name, type, key)) {
      uint64_t docId = 0;
      if (DocIdMeta_GetWithOpenKey(key, sp->specId, &docId) == REDISMODULE_OK && docId != 0) {
        // Already indexed in this spec; skip to avoid clobbering a fresher version.
      } else {
        IndexSpec_UpdateDoc(sp, ctx, name, type, key);
      }
    }
  } else {
    RedisModule_AsyncScanAbort(cursor);
  }
  if (sp) {
    IndexSpecRef_Release(curr_run_ref);
  }
  ++scanner->scannedKeys;
}

// Call-completion callback. Main thread, GIL held, once per OK Start / NextBatch. Terminal state
// arrives as the next NextBatch's return code, so ignore `reason` and just wake the driver.
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

// Engine-side AsyncScan pre-filter built from an index's type and key prefixes. `type_name`
// (a static literal) backs filter.types; `prefix_copies` (our owned copies) back filter.prefixes.
// Must outlive the Start call. We copy the prefixes (few and short) so the ctx is self-contained
// rather than pinning the spec's StrongRef across the Start retry loop.
typedef struct {
  RedisModuleAsyncScanFilter filter;
  const char *type_name;   // static literal backing filter.types (single entry)
  char **prefix_copies;    // owned NUL-terminated copies of the prefixes backing filter.prefixes
} AsyncScanFilterCtx;

// Build the engine pre-filter from a live spec so the engine never delivers a key that can't
// belong to it. Coarse only: key_cb's SchemaRule_ShouldIndex stays the source of truth. `sp` must
// be kept alive by the caller; type/prefixes are immutable for the spec's lifetime, so no spec
// lock is needed off the GIL. Returns the filter for Start, or NULL to scan everything. The caller
// must call Indexes_AsyncScanFreeFilter(ctx) on every exit path.
static RedisModuleAsyncScanFilter *Indexes_AsyncScanBuildFilter(const IndexSpec *sp,
                                                                AsyncScanFilterCtx *ctx) {
  // One doc type per index. "hash" is the core type; "ReJSON-RL" is RedisJSON's module type.
  // Unknown type → leave unfiltered so ShouldIndex decides.
  switch (sp->rule->type) {
  case DocumentType_Hash:
    ctx->type_name = "hash";
    break;
  case DocumentType_Json:
    ctx->type_name = "ReJSON-RL";
    break;
  default:
    break;
  }
  if (ctx->type_name) {
    ctx->filter.types = &ctx->type_name;
    ctx->filter.num_types = 1;
  }

  // Copy the prefixes, unless the index matches every key (its sole prefix is the empty string,
  // the FT.CREATE default): for match-all the engine's no-filter path beats an empty-prefix
  // compare. Byte-prefix match has the same semantics as ShouldIndex's strncmp, so this never
  // drops a key ShouldIndex would have indexed.
  HiddenUnicodeString **prefixes = sp->rule->prefixes;
  size_t nprefixes = array_len(prefixes);
  bool match_all = false;
  for (size_t i = 0; i < nprefixes; ++i) {
    size_t len = 0;
    HiddenUnicodeString_GetUnsafe(prefixes[i], &len);
    if (len == 0) {  // empty-string prefix is a prefix of every key
      match_all = true;
      break;
    }
  }
  if (nprefixes > 0 && !match_all) {
    ctx->prefix_copies = rm_malloc(nprefixes * sizeof(*ctx->prefix_copies));
    for (size_t i = 0; i < nprefixes; ++i) {
      size_t len = 0;
      const char *p = HiddenUnicodeString_GetUnsafe(prefixes[i], &len);
      ctx->prefix_copies[i] = rm_strndup(p, len);
    }
    ctx->filter.prefixes = (const char **)ctx->prefix_copies;
    ctx->filter.num_prefixes = nprefixes;
  }

  // NULL (not an empty struct) → engine's no-filter fast path.
  return (ctx->filter.num_types || ctx->filter.num_prefixes) ? &ctx->filter : NULL;
}

// Free the owned prefix copies. Idempotent; safe on a zero-initialized ctx.
static void Indexes_AsyncScanFreeFilter(AsyncScanFilterCtx *ctx) {
  if (ctx->prefix_copies) {
    for (size_t i = 0; i < ctx->filter.num_prefixes; ++i) {
      rm_free(ctx->prefix_copies[i]);
    }
    rm_free(ctx->prefix_copies);
    ctx->prefix_copies = NULL;
  }
}

// Record a background-indexing failure. Call WITHOUT the GIL: takes it internally, because
// IndexError_AddError mutates the shared NA_rstr sentinel's refcount (needs the GIL). `oom` routes
// to the spec's scan_failed_OOM flag (warns at query time, aggregated in FT.INFO). Shared by
// several terminal result codes.
static void Indexes_AsyncScanRecordFailure(RedisModuleCtx *ctx, IndexesScanner *scanner,
                                                 const char *msg, bool oom) {
  RedisModule_ThreadSafeContextLock(ctx);
  IndexesScanner_RecordBackgroundFailure(ctx, scanner, msg, oom);
  RedisModule_ThreadSafeContextUnlock(ctx);
}

// Bind parameters to the cursor and queue the first batch. Start shares NextBatch's max-inflight
// cap and can return BUSY, so retry the same call until accepted (GIL held only for the call;
// backoff sleeps released). The pre-filter is built lazily the first time we hold the spec alive,
// reusing the cancellation-check promotion; `filter_ptr == NULL` is the "not built yet" sentinel
// so a BUSY retry doesn't rebuild it. The filter is owned here and freed on every exit (the engine
// deep-copies it at Start). Returns the accepted Start's code; sets *out_terminal when the scan
// must tear down without driving (cancelled/dropped while starting, or the BUSY-timeout valve).
static RedisModuleAsyncScanResult Indexes_AsyncScanStartWithRetry(
    RedisModuleCtx *ctx, RedisModuleScanCursor *cursor, IndexesScanner *scanner,
    AsyncReindexDriver *driver, bool *out_terminal) {
  *out_terminal = false;
  AsyncScanFilterCtx filter_ctx = {0};
  RedisModuleAsyncScanFilter *filter_ptr = NULL;
  RedisModuleAsyncScanResult rc = REDISMODULE_ASYNCSCAN_ABORTED;
  rs_wall_clock busy_since;
  rs_wall_clock_init(&busy_since);
  for (;;) {
    RedisModule_ThreadSafeContextLock(ctx);
    // Check cancellation / a dropped spec before Start, under the same GIL hold, so nothing
    // interleaves before the bind. FT.DROPINDEX / FLUSHDB invalidate the weak ref without setting
    // cancelled, so promote it to notice a drop. Checking here stops the BUSY retries from binding
    // a cursor for a dead index (on an empty or filtered DB nothing else observes the drop).
    if (!IndexesScanner_IsCancelled(scanner)) {
      StrongRef live_ref = IndexSpecRef_Promote(scanner->spec_ref);
      IndexSpec *sp = StrongRef_Get(live_ref);
      if (!sp) {
        IndexesScanner_Cancel(scanner);
      } else {
        // Build the pre-filter lazily, the first time we hold the spec alive.
        if (!filter_ptr) {
          filter_ptr = Indexes_AsyncScanBuildFilter(sp, &filter_ctx);
          RS_ASSERT(filter_ptr);
        }
        IndexSpecRef_Release(live_ref);
      }
    }
    if (IndexesScanner_IsCancelled(scanner)) {
      RedisModule_ThreadSafeContextUnlock(ctx);
      // Start never bound the cursor — nothing to abort, just tear down.
      RedisModule_Log(ctx, "notice", "AsyncScan: index %s cancelled while starting",
                      scanner->spec_name_for_logs);
      *out_terminal = true;
      goto out;
    }
    rc = RedisModule_AsyncScanStart(ctx, cursor, filter_ptr,
                                    REDISMODULE_ASYNCSCAN_MODE_META_AND_VALUE, NULL,
                                    Indexes_AsyncScanKeyCB, Indexes_AsyncScanDoneCB,
                                    ASYNC_SCAN_BATCH_SIZE_HINT_DEFAULT, driver);
    RedisModule_ThreadSafeContextUnlock(ctx);
    if (rc != REDISMODULE_ASYNCSCAN_BUSY) {
      goto out;
    }
    // BUSY safety valve: if never accepted within the timeout, give up. The cursor was never
    // bound, so there is nothing to abort — record the failure (index flagged incomplete).
    if (rs_wall_clock_convert_ns_to_ms(rs_wall_clock_elapsed_ns(&busy_since)) >=
        ASYNC_SCAN_START_BUSY_TIMEOUT_MS) {
      RedisModule_Log(ctx, "warning",
                      "AsyncScan: index %s could not start within %dms (max-inflight stayed "
                      "saturated); giving up (rc=BUSY)",
                      scanner->spec_name_for_logs, ASYNC_SCAN_START_BUSY_TIMEOUT_MS);
      Indexes_AsyncScanRecordFailure(
          ctx, scanner,
          "Background indexing failed: the background scan could not be started because the "
          "server stayed busy; the index may be incomplete",
          /*oom=*/false);
      *out_terminal = true;
      goto out;
    }
    usleep(ASYNC_SCAN_BUSY_BACKOFF_US);
  }
out:
  // Free our filter copies (the engine already deep-copied at Start, or it was never built).
  Indexes_AsyncScanFreeFilter(&filter_ctx);
  return rc;
}

// Fold a dropped index into the cancellation latch: FT.DROPINDEX / FLUSHDB invalidate the weak ref
// without setting cancelled, so a failed promote means the spec is gone.
//
// Runs WITHOUT the GIL (called from the GIL-released throttle backoff) — no lock/unlock per
// re-check. Safe because it touches nothing GIL-bound: `cancelled` goes through the relaxed-atomic
// accessors, and the promote/release is lock-free (util/references.c). A last-ref release runs the
// spec free callback, which is safe off the main thread (the GIL-bound teardown already ran in
// IndexSpec_Unlink). GIL-holding callers (batch start / between-batches) inline the promote.
static void Indexes_AsyncScanCancelIfDropped(IndexesScanner *scanner) {
  if (!IndexesScanner_IsCancelled(scanner)) {
    StrongRef live_ref = IndexSpecRef_Promote(scanner->spec_ref);
    if (!StrongRef_Get(live_ref)) {
      IndexesScanner_Cancel(scanner);
    } else {
      IndexSpecRef_Release(live_ref);
    }
  }
}

// Process the completed batch and queue the next. Returns the next NextBatch's code; sets
// *out_terminal when the scan was cancelled or the spec dropped between batches (cursor aborted,
// already logged).
static RedisModuleAsyncScanResult Indexes_AsyncScanDriveNextBatch(
    RedisModuleCtx *ctx, RedisModuleScanCursor *cursor, IndexesScanner *scanner,
    AsyncReindexDriver *driver, size_t *batchesDone, size_t *lastProgressKeys,
    bool *out_terminal) {
  *out_terminal = false;

  // Batch queued. Wait (GIL released) for done_cb to fire on the main thread.
  Indexes_AsyncScanWaitForDone(driver);
  ++*batchesDone;

  // Test hook (ENABLE_ASSERT): park between batches, GIL released, so a test can drop the index /
  // flush the keyspace after a batch but before the next, exercising the between-batches re-check.
#ifdef ENABLE_ASSERT
  SyncPoint_Wait(SYNC_POINT_ASYNC_SCAN_BETWEEN_BATCHES);
#endif

  // Vector flat-buffer back-pressure. A disk tiered vector index raises the client-postpone
  // throttle when its in-memory flat buffer fills, but that only gates client CMD_DENYOOM
  // commands — this scan bypasses command dispatch and would grow the buffer without bound. Wait
  // (GIL released) until the async insert jobs drain it, applying the same back-pressure to
  // ourselves; releasing the GIL also lets the main thread run the reads / frees that recover
  // memory. Re-check cancelled each iteration, and periodically re-promote the weak ref so a drop
  // that doesn't set cancelled can't park us forever.
  //
  // Skip if the batch already tripped scanFailedOnOOM: the OOM branch below must abort promptly,
  // not sit behind a throttle that may be slow to clear. OOM is only set in key_cb, so gating
  // entry suffices; the loop re-checks it defensively.
  if (SearchDisk_IsVectorWriteThrottling() && !IndexesScanner_IsCancelled(scanner) && !scanner->scanFailedOnOOM) {
    RedisModule_Log(ctx, "debug",
                    "AsyncScan: index %s throttled (vector flat buffer full); backing off",
                    scanner->spec_name_for_logs);
    size_t backoffIters = 0;
    while (SearchDisk_IsVectorWriteThrottling() && !IndexesScanner_IsCancelled(scanner) && !scanner->scanFailedOnOOM) {
      usleep(ASYNC_SCAN_BUSY_BACKOFF_US);
      // FT.DROPINDEX / FLUSHDB don't set cancelled and the global throttle isn't a reliable drop
      // wake-up, so periodically promote the weak ref (lock-free, no GIL) to notice a drop; a dead
      // ref cancels the scan so the loop exits and the re-check below aborts the sweep.
      if (++backoffIters % ASYNC_SCAN_THROTTLE_DROP_RECHECK_ITERS == 0) {
        Indexes_AsyncScanCancelIfDropped(scanner);
      }
    }
    // Log the resume/abort so the pause is bounded and observable. ~ms waited = iters*backoff/1000.
    RedisModule_Log(ctx, "debug",
                    "AsyncScan: index %s throttle %s after ~%zu ms; resuming batches",
                    scanner->spec_name_for_logs,
                    IndexesScanner_IsCancelled(scanner) ? "wait aborted (index dropped or scan cancelled)"
                                                        : "cleared",
                    (backoffIters * (size_t)ASYNC_SCAN_BUSY_BACKOFF_US) / 1000);
  }

  RedisModule_ThreadSafeContextLock(ctx);

  // Re-check for a spec dropped between batches: with a selective filter no key_cb may run to
  // observe an FT.DROPINDEX / FLUSHDB, so without this the worker keeps requesting batches for a
  // dead index. A failed promote collapses into cancellation.
  if (!IndexesScanner_IsCancelled(scanner)) {
    StrongRef live_ref = IndexSpecRef_Promote(scanner->spec_ref);
    if (!StrongRef_Get(live_ref)) {
      IndexesScanner_Cancel(scanner);
      RedisModule_Log(ctx, "notice",
                      "AsyncScan: index %s dropped mid-scan; cancelling (scanned=%zu)",
                      scanner->spec_name_for_logs, scanner->scannedKeys);
    } else {
      IndexSpecRef_Release(live_ref);
    }
  }

  // Cancelled (per-key or just above): abort the sweep. Abort is a NOOP on an already-terminal
  // cursor and publishes terminal state on an active one, so teardown's ScanCursorDestroy is
  // always valid.
  if (IndexesScanner_IsCancelled(scanner)) {
    RedisModule_AsyncScanAbort(cursor);
    RedisModule_ThreadSafeContextUnlock(ctx);
    RedisModule_Log(ctx, "notice",
                    "AsyncScan: aborting scan of index %s (cancelled, scanned=%zu)",
                    scanner->spec_name_for_logs, scanner->scannedKeys);
    *out_terminal = true;
    return REDISMODULE_ASYNCSCAN_ABORTED;
  }

  // OOM flagged by key_cb this batch: abort the still-active cursor and surface OOM like the
  // engine's OUT_OF_MEMORY branch. Not resumed, so the index may be incomplete.
  if (scanner->scanFailedOnOOM) {
    RedisModule_AsyncScanAbort(cursor);
    RedisModule_ThreadSafeContextUnlock(ctx);
    RedisModule_Log(ctx, "warning",
                    "AsyncScan: index %s exceeded the indexing memory limit; aborting "
                    "(scanned=%zu). The index may be incomplete; once memory pressure is "
                    "relieved, drop and recreate the index to rebuild it",
                    scanner->spec_name_for_logs, scanner->scannedKeys);
    // RecordFailure re-takes the GIL, so call it unlocked. Offending key came from key_cb.
    Indexes_AsyncScanRecordFailure(
        ctx, scanner,
        "Background indexing cancelled: used memory exceeded the configured indexing memory "
        "limit; the index may be incomplete",
        /*oom=*/true);
    *out_terminal = true;
    return REDISMODULE_ASYNCSCAN_OUT_OF_MEMORY;
  }

  // Throttled progress: one line every ASYNC_SCAN_PROGRESS_LOG_KEYS keys.
  if (scanner->scannedKeys - *lastProgressKeys >= ASYNC_SCAN_PROGRESS_LOG_KEYS) {
    *lastProgressKeys = scanner->scannedKeys;
    RedisModule_Log(ctx, "notice",
                    "AsyncScan: index %s progress: scanned=%zu keys, batches=%zu",
                    scanner->spec_name_for_logs, scanner->scannedKeys, *batchesDone);
  }

  // Test hook (ENABLE_ASSERT): force the OOM terminal branch after a batch, since the engine's
  // terminal OUT_OF_MEMORY is not reproducible from the module side. Armed via
  // _FT.DEBUG BG_SCAN_CONTROLLER SET_SIMULATE_ASYNC_OOM true.
#ifdef ENABLE_ASSERT
  if (globalDebugCtx.bgIndexing.simulateAsyncOOM) {
    RedisModule_ThreadSafeContextUnlock(ctx);
    RedisModule_Log(ctx, "warning",
                    "AsyncScan: SIMULATE_ASYNC_OOM hook forcing OOM for index %s",
                    scanner->spec_name_for_logs);
    return REDISMODULE_ASYNCSCAN_OUT_OF_MEMORY;
  }
#endif

  RedisModuleAsyncScanResult rc =
      RedisModule_AsyncScanNextBatch(ctx, cursor, ASYNC_SCAN_BATCH_SIZE_HINT_DEFAULT);
  RedisModule_ThreadSafeContextUnlock(ctx);
  return rc;
}

void Indexes_AsyncScanAndReindexTask(IndexesScanner *scanner) {
  RS_LOG_ASSERT(scanner, "invalid IndexesScanner");

  RedisModuleCtx *ctx = RedisModule_GetDetachedThreadSafeContext(RSDummyContext);
  // Name the cursor after the index, for INFO observability.
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

  // Test hook (ENABLE_ASSERT): park before the first batch (no GIL held) so a test can drop the
  // index / cancel the scan mid-flight.
#ifdef ENABLE_ASSERT
  SyncPoint_Wait(SYNC_POINT_ASYNC_SCAN_BEFORE_FIRST_BATCH);
#endif

  // Start the scan (binds the cursor, queues the first batch, retries through BUSY). Terminal here
  // means the cursor was never bound — just tear down.
  bool start_terminal = false;
  RedisModuleAsyncScanResult rc =
      Indexes_AsyncScanStartWithRetry(ctx, cursor, scanner, &driver, &start_terminal);
  if (start_terminal) {
    goto done;
  }

  // Drive the cursor one batch at a time. done_cb only wakes us; terminal state arrives as the
  // return code of the next NextBatch.
  for (;;) {
    switch (rc) {
    case REDISMODULE_ASYNCSCAN_OK: {
      // Process the batch and queue the next; bails to teardown if cancelled/dropped between batches.
      bool batch_terminal = false;
      rc = Indexes_AsyncScanDriveNextBatch(ctx, cursor, scanner, &driver, &batchesDone,
                                           &lastProgressKeys, &batch_terminal);
      if (batch_terminal) {
        goto done;
      }
      break;
    }

    case REDISMODULE_ASYNCSCAN_BUSY:
      // Concurrency cap full; back off (GIL released) and retry the same NextBatch.
      RedisModule_Log(ctx, "debug",
                      "AsyncScan: index %s throttled (max-inflight); backing off (rc=BUSY)",
                      scanner->spec_name_for_logs);
      usleep(ASYNC_SCAN_BUSY_BACKOFF_US);
      RedisModule_ThreadSafeContextLock(ctx);
      rc = RedisModule_AsyncScanNextBatch(ctx, cursor, ASYNC_SCAN_BATCH_SIZE_HINT_DEFAULT);
      RedisModule_ThreadSafeContextUnlock(ctx);
      break;

    case REDISMODULE_ASYNCSCAN_EXHAUSTED:
      // Natural completion: the full keyspace was swept.
      RedisModule_Log(ctx, "notice",
                      "AsyncScan: index %s completed: scanned=%zu keys in %zu batches "
                      "(rc=EXHAUSTED)",
                      scanner->spec_name_for_logs, scanner->scannedKeys, batchesDone);
      goto done;

    case REDISMODULE_ASYNCSCAN_DATASET_RESET:
      // Dataset reset (FLUSHALL / FLUSHDB / DEBUG RELOAD) ended the sweep; the cursor is unusable.
      RedisModule_Log(ctx, "notice",
                      "AsyncScan: scan of index %s ended early "
                      "(dataset reset, scanned=%zu, rc=DATASET_RESET)",
                      scanner->spec_name_for_logs,
                      scanner->scannedKeys);
      goto done;
    case REDISMODULE_ASYNCSCAN_ABORTED:
      // The sweep was aborted; the cursor is unusable.
      RedisModule_Log(ctx, "notice",
                      "AsyncScan: scan of index %s ended early (aborted, scanned=%zu, rc=ABORTED)",
                      scanner->spec_name_for_logs,
                      scanner->scannedKeys);
      goto done;

    case REDISMODULE_ASYNCSCAN_OUT_OF_MEMORY:
      // Engine memory pressure cut the sweep short; the index may be incomplete, not auto-resumed.
      RedisModule_Log(ctx, "warning",
                      "AsyncScan: engine OOM during scan of index %s; index may be incomplete "
                      "(scanned=%zu, rc=OUT_OF_MEMORY). The scan is not retried automatically; "
                      "once memory pressure is relieved, drop and recreate the index to rebuild it",
                      scanner->spec_name_for_logs, scanner->scannedKeys);
      // Surface like the sync scan: set scan_failed_OOM + raise the failure flag. No single key to
      // blame (global pressure cut delivery), so the offending key is NULL.
      Indexes_AsyncScanRecordFailure(
          ctx, scanner,
          "Background indexing cancelled: the server reported out of memory; the index may be "
          "incomplete",
          /*oom=*/true);
      goto done;

    case REDISMODULE_ASYNCSCAN_IN_PROGRESS:
      // Should not happen: the driver waits for done_cb before reissuing, so no batch is ever in
      // flight here. IN_PROGRESS means we reissued anyway — a driver bug.
      RS_ASSERT(false);
      RedisModule_Log(ctx, "warning",
                      "AsyncScan: requested the next batch for index %s while a previous batch "
                      "was still in flight (done_cb had not fired); driver bug (rc=IN_PROGRESS)",
                      scanner->spec_name_for_logs);
      Indexes_AsyncScanRecordFailure(
          ctx, scanner,
          "Background indexing failed: the background scan reported an unexpected in-progress "
          "state; the index may be incomplete",
          /*oom=*/false);
      goto done;

    case REDISMODULE_ASYNCSCAN_UNSUPPORTED:
      // AsyncScan unavailable in this build. We only route disk indexes here, where it is present.
      RS_ASSERT(false);
      RedisModule_Log(ctx, "warning", "AsyncScan: unsupported for index %s (rc=UNSUPPORTED)",
                      scanner->spec_name_for_logs);
      Indexes_AsyncScanRecordFailure(
          ctx, scanner,
          "Background indexing failed: the background scan is unsupported in this "
          "configuration; the index may be incomplete",
          /*oom=*/false);
      goto done;

    case REDISMODULE_ASYNCSCAN_INVALID:
      RS_ASSERT(false);
      RedisModule_Log(ctx, "warning", "AsyncScan: invalid argument for index %s (rc=INVALID)",
                      scanner->spec_name_for_logs);
      Indexes_AsyncScanRecordFailure(
          ctx, scanner,
          "Background indexing failed: the background scan was rejected as invalid; the index "
          "may be incomplete",
          /*oom=*/false);
      goto done;

    case REDISMODULE_ASYNCSCAN_IO_ERROR:
      // Unrecoverable I/O error; the index may be incomplete.
      RedisModule_Log(ctx, "warning",
        "AsyncScan: I/O error for index %s (rc=IO_ERROR); index may be incomplete. The scan is not "
        "retried automatically; resolve the underlying disk/I/O condition, then drop and recreate "
        "the index to rebuild it",
        scanner->spec_name_for_logs);
      Indexes_AsyncScanRecordFailure(
          ctx, scanner,
          "Background indexing failed: the server reported an I/O error; the index may be "
          "incomplete",
          /*oom=*/false);
      goto done;
    }
  }

done:
  // Each exit path above logs its own outcome; this is just the teardown marker.
  RedisModule_Log(ctx, "debug", "AsyncScan: index %s scan task exiting (scanned=%zu)",
                  scanner->spec_name_for_logs, scanner->scannedKeys);

  // Take the GIL around IndexesScanner_Free: it clears sp->scanner / sp->scan_in_progress and
  // frees scanner->OOMkey, all read by FT.INFO (fillReplyWithIndexInfo) on the main thread. This
  // task runs GIL-released between batches, so without the GIL a scan finishing concurrently with
  // INFO could race or leave INFO dereferencing a freed scanner. The spec lock is not enough: Free
  // also touches the process-global global_spec_scanner, and INFO reads sp->scanner without any
  // per-spec lock (it relies on the GIL). The GIL is the real contract here, same as the sync scan.
  RedisModule_ThreadSafeContextLock(ctx);
  IndexesScanner_Free(scanner);
  RedisModule_ThreadSafeContextUnlock(ctx);

  pthread_cond_destroy(&driver.cond);
  pthread_mutex_destroy(&driver.mutex);
  RedisModule_ScanCursorDestroy(cursor);
  RedisModule_FreeThreadSafeContext(ctx);
}
