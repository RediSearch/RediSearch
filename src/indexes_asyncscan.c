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
#include "config.h"
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

// While parked behind the vector flat-buffer throttle, call the job's shouldStop every this many
// backoff iterations (its isCancelled runs on every one): FT.DROPINDEX / FLUSHDB don't set the
// backfill's cancelled latch, and the throttle is global (another index can hold it after ours is
// gone), so its clearing is not a reliable drop wake-up. ~1s at the backoff interval; coarse
// because mid-scan drops are rare.
#define ASYNC_SCAN_THROTTLE_DROP_RECHECK_ITERS 1000

const char *AsyncScanOutcome_ToString(AsyncScanOutcome outcome) {
  switch (outcome) {
  case ASYNC_SCAN_OUTCOME_COMPLETED: return "completed";
  case ASYNC_SCAN_OUTCOME_CANCELLED: return "cancelled";
  case ASYNC_SCAN_OUTCOME_RESET: return "reset";
  case ASYNC_SCAN_OUTCOME_OOM: return "OOM";
  case ASYNC_SCAN_OUTCOME_IO_ERROR: return "I/O error";
  case ASYNC_SCAN_OUTCOME_ERROR: return "error";
  }
  return "unknown";
}

// Per-cursor driver state, shared between the reindex worker (owns the cursor) and the
// callbacks (main thread, GIL held). done_cb signals the condvar the worker waits on.
typedef struct {
  AsyncScanJob *job;
  pthread_mutex_t mutex;
  pthread_cond_t cond;
  bool call_done;   // set by done_cb, consumed by the driver's wait
  // Reason done_cb reported for the call `call_done` refers to; read only while call_done holds.
  RedisModuleAsyncScanDoneReason done_reason;
  // Set by key_cb, under the GIL, once the job's onKey asked to stop and the cursor was aborted.
  bool stopped;
  // Latched by the between-batches memory check when the sweep cannot continue.
  bool overMemoryBudget;
  size_t batchesDone;
  size_t lastProgressKeys;
} AsyncScanDriver;

// Wait (GIL released) until done_cb signals this call's completion, then consume the flag and
// return the reason that call reported.
static RedisModuleAsyncScanDoneReason AsyncScan_WaitForDone(AsyncScanDriver *driver) {
  pthread_mutex_lock(&driver->mutex);
  while (!driver->call_done) {
    pthread_cond_wait(&driver->cond, &driver->mutex);
  }
  driver->call_done = false;
  RedisModuleAsyncScanDoneReason reason = driver->done_reason;
  pthread_mutex_unlock(&driver->mutex);
  return reason;
}

// Per-key callback. Main thread, GIL held, value pinned.
static void AsyncScan_KeyCB(RedisModuleCtx *ctx, RedisModuleScanCursor *cursor,
                            RedisModuleKey *key, RedisModuleString *name, void *privdata) {
  AsyncScanDriver *driver = privdata;
  if (driver->stopped) {
    return;
  }
  if (!driver->job->onKey(driver->job, ctx, key, name)) {
    driver->stopped = true;
    RedisModule_AsyncScanAbort(cursor);
  }
}

// Call-completion callback. Main thread, GIL held, once per OK Start / NextBatch. The drive loop
// switches on the next NextBatch's return code; `reason` is passed along only so it can tell
// whether the batch it just consumed finished the sweep.
static void AsyncScan_DoneCB(RedisModuleCtx *ctx, RedisModuleScanCursor *cursor,
                             void *privdata, RedisModuleAsyncScanDoneReason reason) {
  REDISMODULE_NOT_USED(ctx);
  REDISMODULE_NOT_USED(cursor);
  AsyncScanDriver *driver = privdata;
  pthread_mutex_lock(&driver->mutex);
  driver->done_reason = reason;
  driver->call_done = true;
  pthread_cond_signal(&driver->cond);
  pthread_mutex_unlock(&driver->mutex);
}

// Bind the cursor and queue the first batch. Start shares NextBatch's max-inflight cap and can
// return BUSY, so retry the same call until accepted (GIL held only for the call; backoff sleeps
// released). Returns the accepted Start's code, or sets *out_end when the sweep ends without a
// bound cursor: the job stopped while starting, or the BUSY-timeout valve.
static RedisModuleAsyncScanResult AsyncScan_StartWithRetry(RedisModuleCtx *ctx,
                                                           RedisModuleScanCursor *cursor,
                                                           AsyncScanDriver *driver,
                                                           AsyncScanEnd *out_end,
                                                           bool *out_terminal) {
  AsyncScanJob *job = driver->job;
  *out_terminal = false;
  rs_wall_clock busy_since;
  rs_wall_clock_init(&busy_since);
  for (;;) {
    ReindexPool_LockGIL(ctx);
    // Under the same GIL hold as Start, so nothing interleaves before the bind, and the BUSY
    // retries never bind a cursor for a job that is gone (on an empty or filtered DB nothing
    // else would notice).
    if (job->shouldStop(job)) {
      ReindexPool_UnlockGIL(ctx);
      RedisModule_Log(ctx, "notice", "AsyncScan: %s cancelled while starting", job->nameForLogs);
      *out_end = (AsyncScanEnd){ASYNC_SCAN_OUTCOME_CANCELLED, REDISMODULE_ASYNCSCAN_OK};
      *out_terminal = true;
      return REDISMODULE_ASYNCSCAN_ABORTED;
    }
    RedisModuleAsyncScanResult rc = job->start(job, ctx, cursor, AsyncScan_KeyCB, AsyncScan_DoneCB,
                                               ASYNC_SCAN_BATCH_SIZE_HINT_DEFAULT, driver);
    ReindexPool_UnlockGIL(ctx);
    if (rc != REDISMODULE_ASYNCSCAN_BUSY) {
      return rc;
    }
    if (rs_wall_clock_convert_ns_to_ms(rs_wall_clock_elapsed_ns(&busy_since)) >=
        ASYNC_SCAN_START_BUSY_TIMEOUT_MS) {
      RedisModule_Log(ctx, "warning",
                      "AsyncScan: %s could not start within %dms (max-inflight stayed "
                      "saturated); giving up (rc=BUSY)",
                      job->nameForLogs, ASYNC_SCAN_START_BUSY_TIMEOUT_MS);
      *out_end = (AsyncScanEnd){ASYNC_SCAN_OUTCOME_ERROR, REDISMODULE_ASYNCSCAN_BUSY};
      *out_terminal = true;
      return rc;
    }
    usleep(ASYNC_SCAN_BUSY_BACKOFF_US);
  }
}

// React to Flex memory pressure between batches. Latches driver->overMemoryBudget when the sweep
// cannot continue, which the caller's abort branch acts on. Does nothing for a stopped job: the
// sweep is going away, and the OOM pause below would sleep on a dead cursor while a replacement
// scan waits for this worker.
//
// Caller must hold the GIL; returns with it held.
static void AsyncScan_HandleMemoryPressure(RedisModuleCtx *ctx, AsyncScanDriver *driver) {
  AsyncScanJob *job = driver->job;
  if (job->isCancelled(job)) {
    return;
  }

  const BgIndexingMemVerdict verdict = AsyncBgIndexingMemVerdict(ctx);
  if (verdict == BG_INDEXING_MEM_OK) {
    return;
  }

  // Quota spent: it covers flash as well as RAM, so there is no lower tier left to spill to and
  // nothing this scan can wait for. Fail now instead of sleeping first. Logged here because the
  // abort below cannot tell an operator whether to free quota or to add RAM.
  if (verdict == BG_INDEXING_MEM_EXHAUSTED) {
    RedisModule_Log(ctx, "warning",
                    "AsyncScan: %s: the server's memory quota is spent; not retrying "
                    "(scanned=%zu)",
                    job->nameForLogs, *job->scannedKeys);
    driver->overMemoryBudget = true;
    return;
  }

  const uint32_t graceSecs = RSGlobalConfig.bgIndexingOomPauseTimeBeforeRetry;
  if (graceSecs > 0) {
    RedisModule_Log(ctx, "notice",
                    "AsyncScan: %s: RAM is over its budget; pausing %u seconds before "
                    "retrying (scanned=%zu)",
                    job->nameForLogs, graceSecs, *job->scannedKeys);
    ReindexPool_UnlockGIL(ctx);
    sleep(graceSecs);
    ReindexPool_LockGIL(ctx);
    // Cancelled during the pause: the caller's check below owns the teardown.
    if (job->isCancelled(job)) {
      return;
    }
    if (AsyncBgIndexingMemVerdict(ctx) == BG_INDEXING_MEM_OK) {
      RedisModule_Log(ctx, "notice",
                      "AsyncScan: %s is back within the server's memory budget; resuming "
                      "(scanned=%zu)",
                      job->nameForLogs, *job->scannedKeys);
      return;
    }
  }

  // On recovery the scan resumes in place, so there is nothing to re-sweep and no cursor restart;
  // still over budget, and the abort branch below tears the sweep down.
  driver->overMemoryBudget = true;
}

// Process the completed batch and queue the next. Returns the next NextBatch's code; sets
// *out_terminal (and *out_end) when the sweep ends between batches, with the cursor aborted.
static RedisModuleAsyncScanResult AsyncScan_DriveNextBatch(RedisModuleCtx *ctx,
                                                           RedisModuleScanCursor *cursor,
                                                           AsyncScanDriver *driver,
                                                           AsyncScanEnd *out_end,
                                                           bool *out_terminal) {
  AsyncScanJob *job = driver->job;
  *out_terminal = false;

  // Batch queued. Wait (GIL released) for done_cb to fire on the main thread.
  const RedisModuleAsyncScanDoneReason done_reason = AsyncScan_WaitForDone(driver);
  ++driver->batchesDone;

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
  // memory. Re-check the job each iteration, and periodically ask whether it went away silently, so
  // a drop that sets no latch can't park us forever.
  //
  // Runs before the memory check below, not after: draining the flat buffer to disk is itself
  // memory relief, so sampling memory on the far side of this wait measures what the scan actually
  // costs rather than a peak the throttle is about to give back.
  if (SearchDisk_IsVectorWriteThrottling() && !job->isCancelled(job)) {
    RedisModule_Log(ctx, "debug", "AsyncScan: %s throttled (vector flat buffer full); backing off",
                    job->nameForLogs);
    size_t backoffIters = 0;
    while (SearchDisk_IsVectorWriteThrottling() && !job->isCancelled(job)) {
      usleep(ASYNC_SCAN_BUSY_BACKOFF_US);
      if (++backoffIters % ASYNC_SCAN_THROTTLE_DROP_RECHECK_ITERS == 0 && job->shouldStop(job)) {
        break;
      }
    }
    // Log the resume/abort so the pause is bounded and observable. ~ms waited = iters*backoff/1000.
    RedisModule_Log(ctx, "debug", "AsyncScan: %s throttle %s after ~%zu ms; resuming batches",
                    job->nameForLogs,
                    job->isCancelled(job) ? "wait aborted (job stopped)" : "cleared",
                    (backoffIters * (size_t)ASYNC_SCAN_BUSY_BACKOFF_US) / 1000);
  }

  ReindexPool_LockGIL(ctx);

  if (done_reason != REDISMODULE_ASYNCSCAN_DONE_COMPLETED) {
    AsyncScan_HandleMemoryPressure(ctx, driver);
  }

  // With a selective filter no key_cb may run to notice that the job is gone, so check it here
  // too; otherwise the worker keeps requesting batches for it.
  if (driver->stopped || job->shouldStop(job)) {
    RedisModule_AsyncScanAbort(cursor);
    ReindexPool_UnlockGIL(ctx);
    RedisModule_Log(ctx, "notice", "AsyncScan: aborting scan of %s (cancelled, scanned=%zu)",
                    job->nameForLogs, *job->scannedKeys);
    *out_end = (AsyncScanEnd){ASYNC_SCAN_OUTCOME_CANCELLED, REDISMODULE_ASYNCSCAN_OK};
    *out_terminal = true;
    return REDISMODULE_ASYNCSCAN_ABORTED;
  }

  if (driver->overMemoryBudget) {
    RedisModule_AsyncScanAbort(cursor);
    ReindexPool_UnlockGIL(ctx);
    RedisModule_Log(ctx, "warning",
                    "AsyncScan: %s exceeded the server's memory quota; aborting (scanned=%zu)",
                    job->nameForLogs, *job->scannedKeys);
    *out_end = (AsyncScanEnd){ASYNC_SCAN_OUTCOME_OOM, REDISMODULE_ASYNCSCAN_OK};
    *out_terminal = true;
    return REDISMODULE_ASYNCSCAN_OUT_OF_MEMORY;
  }

  // Throttled progress: one line every ASYNC_SCAN_PROGRESS_LOG_KEYS keys.
  if (*job->scannedKeys - driver->lastProgressKeys >= ASYNC_SCAN_PROGRESS_LOG_KEYS) {
    driver->lastProgressKeys = *job->scannedKeys;
    RedisModule_Log(ctx, "notice", "AsyncScan: %s progress: scanned=%zu keys, batches=%zu",
                    job->nameForLogs, *job->scannedKeys, driver->batchesDone);
  }

  // Test hook (ENABLE_ASSERT): force the OOM terminal branch after a batch, since the engine's
  // terminal OUT_OF_MEMORY is not reproducible from the module side. Armed via
  // _FT.DEBUG BG_SCAN_CONTROLLER SET_SIMULATE_ASYNC_OOM true.
#ifdef ENABLE_ASSERT
  if (globalDebugCtx.bgIndexing.simulateAsyncOOM) {
    ReindexPool_UnlockGIL(ctx);
    RedisModule_Log(ctx, "warning", "AsyncScan: SIMULATE_ASYNC_OOM hook forcing OOM for %s",
                    job->nameForLogs);
    return REDISMODULE_ASYNCSCAN_OUT_OF_MEMORY;
  }
#endif

  RedisModuleAsyncScanResult rc =
      RedisModule_AsyncScanNextBatch(ctx, cursor, ASYNC_SCAN_BATCH_SIZE_HINT_DEFAULT);
  ReindexPool_UnlockGIL(ctx);
  return rc;
}

// Map a terminal return code of Start / NextBatch to the sweep's outcome, logging it.
static AsyncScanOutcome AsyncScan_TerminalOutcome(RedisModuleCtx *ctx, AsyncScanDriver *driver,
                                                  RedisModuleAsyncScanResult rc) {
  AsyncScanJob *job = driver->job;
  switch (rc) {
  case REDISMODULE_ASYNCSCAN_EXHAUSTED:
    RedisModule_Log(ctx, "notice",
                    "AsyncScan: %s completed: scanned=%zu keys in %zu batches (rc=EXHAUSTED)",
                    job->nameForLogs, *job->scannedKeys, driver->batchesDone);
    return ASYNC_SCAN_OUTCOME_COMPLETED;

  case REDISMODULE_ASYNCSCAN_DATASET_RESET:
    RedisModule_Log(ctx, "notice",
                    "AsyncScan: scan of %s ended early (dataset reset, scanned=%zu, "
                    "rc=DATASET_RESET)",
                    job->nameForLogs, *job->scannedKeys);
    return ASYNC_SCAN_OUTCOME_RESET;

  case REDISMODULE_ASYNCSCAN_ABORTED:
    RedisModule_Log(ctx, "notice",
                    "AsyncScan: scan of %s ended early (aborted, scanned=%zu, rc=ABORTED)",
                    job->nameForLogs, *job->scannedKeys);
    return ASYNC_SCAN_OUTCOME_CANCELLED;

  case REDISMODULE_ASYNCSCAN_OUT_OF_MEMORY:
    RedisModule_Log(ctx, "warning",
                    "AsyncScan: engine OOM during scan of %s (scanned=%zu, rc=OUT_OF_MEMORY). "
                    "The scan is not retried automatically",
                    job->nameForLogs, *job->scannedKeys);
    return ASYNC_SCAN_OUTCOME_OOM;

  case REDISMODULE_ASYNCSCAN_IO_ERROR:
    RedisModule_Log(ctx, "warning",
                    "AsyncScan: I/O error during scan of %s (scanned=%zu, rc=IO_ERROR). The "
                    "scan is not retried automatically",
                    job->nameForLogs, *job->scannedKeys);
    return ASYNC_SCAN_OUTCOME_IO_ERROR;

  case REDISMODULE_ASYNCSCAN_IN_PROGRESS:
    // The driver waits for done_cb before reissuing, so no batch is ever in flight here.
    RS_ASSERT(false);
    RedisModule_Log(ctx, "warning",
                    "AsyncScan: requested the next batch of %s while a previous batch was still "
                    "in flight; driver bug (rc=IN_PROGRESS)",
                    job->nameForLogs);
    return ASYNC_SCAN_OUTCOME_ERROR;

  case REDISMODULE_ASYNCSCAN_UNSUPPORTED:
    // Only disk jobs run here, where AsyncScan is present.
    RS_ASSERT(false);
    RedisModule_Log(ctx, "warning", "AsyncScan: unsupported for %s (rc=UNSUPPORTED)",
                    job->nameForLogs);
    return ASYNC_SCAN_OUTCOME_ERROR;

  case REDISMODULE_ASYNCSCAN_INVALID:
    RS_ASSERT(false);
    RedisModule_Log(ctx, "warning", "AsyncScan: invalid argument for %s (rc=INVALID)",
                    job->nameForLogs);
    return ASYNC_SCAN_OUTCOME_ERROR;

  case REDISMODULE_ASYNCSCAN_OK:
  case REDISMODULE_ASYNCSCAN_BUSY:
    break;
  }
  RS_ABORT("AsyncScan: not a terminal result");
  return ASYNC_SCAN_OUTCOME_ERROR;
}

void AsyncScan_Drive(AsyncScanJob *job) {
  RedisModuleCtx *ctx = RedisModule_GetDetachedThreadSafeContext(RSDummyContext);
  // Name the cursor after the job, for INFO observability.
  RedisModuleScanCursor *cursor = RedisModule_ScanCursorCreateWithName(job->nameForLogs);

  AsyncScanDriver driver = {0};
  driver.job = job;
  pthread_mutex_init(&driver.mutex, NULL);
  pthread_cond_init(&driver.cond, NULL);

  RedisModule_Log(ctx, "notice", "AsyncScan: scanning %s in background", job->nameForLogs);

  // Test hook (ENABLE_ASSERT): park before the first batch (no GIL held) so a test can drop the
  // index / cancel the scan mid-flight.
#ifdef ENABLE_ASSERT
  SyncPoint_Wait(SYNC_POINT_ASYNC_SCAN_BEFORE_FIRST_BATCH);
#endif

  AsyncScanEnd end = {ASYNC_SCAN_OUTCOME_ERROR, REDISMODULE_ASYNCSCAN_OK};
  bool terminal = false;
  RedisModuleAsyncScanResult rc = AsyncScan_StartWithRetry(ctx, cursor, &driver, &end, &terminal);

  // Drive the cursor one batch at a time. done_cb only wakes us; terminal state arrives as the
  // return code of the next NextBatch.
  while (!terminal) {
    switch (rc) {
    case REDISMODULE_ASYNCSCAN_OK:
      rc = AsyncScan_DriveNextBatch(ctx, cursor, &driver, &end, &terminal);
      break;

    case REDISMODULE_ASYNCSCAN_BUSY:
      // Concurrency cap full; back off (GIL released) and retry the same NextBatch.
      RedisModule_Log(ctx, "debug", "AsyncScan: %s throttled (max-inflight); backing off (rc=BUSY)",
                      job->nameForLogs);
      usleep(ASYNC_SCAN_BUSY_BACKOFF_US);
      ReindexPool_LockGIL(ctx);
      rc = RedisModule_AsyncScanNextBatch(ctx, cursor, ASYNC_SCAN_BATCH_SIZE_HINT_DEFAULT);
      ReindexPool_UnlockGIL(ctx);
      break;

    default:
      end = (AsyncScanEnd){AsyncScan_TerminalOutcome(ctx, &driver, rc), rc};
      terminal = true;
      break;
    }
  }

  RedisModule_Log(ctx, "debug", "AsyncScan: %s scan task exiting (%s, scanned=%zu)",
                  job->nameForLogs, AsyncScanOutcome_ToString(end.outcome), *job->scannedKeys);

  // `finish` may free the job, so nothing below may touch it.
  ReindexPool_LockGIL(ctx);
  job->finish(job, ctx, &end);
  ReindexPool_UnlockGIL(ctx);

  pthread_cond_destroy(&driver.cond);
  pthread_mutex_destroy(&driver.mutex);
  RedisModule_ScanCursorDestroy(cursor);
  RedisModule_FreeThreadSafeContext(ctx);
}

//---------------------------------------------------------------------------------------------
// Per-index backfill: the sweep FT.CREATE, FT.ALTER and loading schedule for one disk index.

// Engine-side AsyncScan pre-filter built from an index's type and key prefixes. `type_name`
// (a static literal) backs filter.types; `prefix_copies` (our owned copies) back filter.prefixes.
// We copy the prefixes (few and short) so the job is self-contained rather than pinning the
// spec's StrongRef across the Start retry loop.
typedef struct {
  RedisModuleAsyncScanFilter filter;
  const char *type_name;   // static literal backing filter.types (single entry)
  char **prefix_copies;    // owned NUL-terminated copies of the prefixes backing filter.prefixes
} AsyncScanFilterCtx;

typedef struct {
  AsyncScanJob base;
  IndexesScanner *scanner;
  char *nameForLogs;
  AsyncScanFilterCtx filterCtx;
  // NULL until built, the first time Start runs with the spec alive.
  RedisModuleAsyncScanFilter *filter;
} BackfillJob;

// Build the engine pre-filter from a live spec so the engine never delivers a key that can't
// belong to it. Coarse only: onKey's SchemaRule_ShouldIndex stays the source of truth. `sp` must
// be kept alive by the caller; type/prefixes are immutable for the spec's lifetime, so no spec
// lock is needed off the GIL. Returns the filter for Start, or NULL to scan everything.
static RedisModuleAsyncScanFilter *Backfill_BuildFilter(const IndexSpec *sp,
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
static void Backfill_FreeFilter(AsyncScanFilterCtx *ctx) {
  if (ctx->prefix_copies) {
    for (size_t i = 0; i < ctx->filter.num_prefixes; ++i) {
      rm_free(ctx->prefix_copies[i]);
    }
    rm_free(ctx->prefix_copies);
    ctx->prefix_copies = NULL;
  }
}

// Fold a dropped index into the cancellation latch: FT.DROPINDEX / FLUSHDB invalidate the weak ref
// without setting cancelled, so a failed promote means the spec is gone. Lock-free: `cancelled`
// goes through the relaxed-atomic accessors, and the promote/release is lock-free
// (util/references.c). A last-ref release runs the spec free callback, which is safe off the main
// thread (the GIL-bound teardown already ran in IndexSpec_Unlink).
static bool Backfill_ShouldStop(AsyncScanJob *base) {
  BackfillJob *job = (BackfillJob *)base;
  IndexesScanner *scanner = job->scanner;
  if (IndexesScanner_IsCancelled(scanner)) {
    return true;
  }
  StrongRef live_ref = IndexSpecRef_Promote(scanner->spec_ref);
  if (!StrongRef_Get(live_ref)) {
    IndexesScanner_Cancel(scanner);
    RedisModule_Log(RSDummyContext, "notice",
                    "AsyncScan: index %s dropped mid-scan; cancelling (scanned=%zu)",
                    scanner->spec_name_for_logs, scanner->scannedKeys);
    return true;
  }
  IndexSpecRef_Release(live_ref);
  return false;
}

// The driver calls shouldStop under the same GIL hold first, so the spec is alive here. The engine
// deep-copies the filter at Start; ours is freed in finish.
static bool Backfill_IsCancelled(AsyncScanJob *base) {
  return IndexesScanner_IsCancelled(((BackfillJob *)base)->scanner);
}

static RedisModuleAsyncScanResult Backfill_Start(AsyncScanJob *base, RedisModuleCtx *ctx,
                                                 RedisModuleScanCursor *cursor,
                                                 RedisModuleAsyncScanKeyCB key_cb,
                                                 RedisModuleAsyncScanCallDoneCB done_cb,
                                                 size_t batchSizeHint, void *privdata) {
  BackfillJob *job = (BackfillJob *)base;
  if (!job->filter) {
    StrongRef live_ref = IndexSpecRef_Promote(job->scanner->spec_ref);
    IndexSpec *sp = StrongRef_Get(live_ref);
    RS_ASSERT(sp);
    job->filter = Backfill_BuildFilter(sp, &job->filterCtx);
    RS_ASSERT(job->filter);
    IndexSpecRef_Release(live_ref);
  }
  return RedisModule_AsyncScanStart(ctx, cursor, job->filter,
                                    REDISMODULE_ASYNCSCAN_MODE_META_AND_VALUE, NULL, key_cb,
                                    done_cb, batchSizeHint, privdata);
}

bool Indexes_AsyncScanIndexKey(IndexSpec *sp, RedisModuleCtx *ctx, RedisModuleKey *key,
                               RedisModuleString *name, DocumentType type) {
  // ShouldIndex is the authority (it also evaluates the index FILTER expression a pre-filter
  // can't). `key` is already pinned, so pass it through instead of reopening by name.
  if (!SchemaRule_ShouldIndex(sp, name, type, key)) {
    return false;
  }
  uint64_t docId = 0;
  if (DocIdMeta_GetWithOpenKey(key, sp->specId, &docId) == REDISMODULE_OK && docId != 0) {
    // Already indexed in this spec; skip to avoid clobbering a fresher version.
    return false;
  }
  IndexSpec_UpdateDoc(sp, ctx, name, type, key, NULL, 0);
  return true;
}

// AsyncScan is at-least-once, so a key may arrive more than once; Indexes_AsyncScanIndexKey skips
// keys already indexed in this spec.
static bool Backfill_OnKey(AsyncScanJob *base, RedisModuleCtx *ctx, RedisModuleKey *key,
                           RedisModuleString *name) {
  BackfillJob *job = (BackfillJob *)base;
  IndexesScanner *scanner = job->scanner;

  // Cancelled: the between-batches check aborts the sweep.
  if (IndexesScanner_IsCancelled(scanner)) {
    return true;
  }

  DocumentType type = getDocType(key);
  if (type == DocumentType_Unsupported) {
    return true;
  }

  StrongRef curr_run_ref = IndexSpecRef_Promote(scanner->spec_ref);
  IndexSpec *sp = StrongRef_Get(curr_run_ref);
  if (!sp) {
    IndexesScanner_Cancel(scanner);
    RedisModule_Log(ctx, "notice", "AsyncScan: index %s dropped mid-scan; cancelling (scanned=%zu)",
                    scanner->spec_name_for_logs, scanner->scannedKeys);
    ++scanner->scannedKeys;
    return false;
  }
  // GIL held → sp and its rule stay stable across ShouldIndex and the DocIdMeta write.
  Indexes_AsyncScanIndexKey(sp, ctx, key, name, type);
  IndexSpecRef_Release(curr_run_ref);
  ++scanner->scannedKeys;
  return true;
}

// The FT.INFO "Index Errors" entry for a sweep that ended with `end`, or NULL if it built the
// index or was stopped on purpose.
static const char *Backfill_FailureMessage(const AsyncScanEnd *end) {
  switch (end->outcome) {
  case ASYNC_SCAN_OUTCOME_COMPLETED:
  case ASYNC_SCAN_OUTCOME_CANCELLED:
  case ASYNC_SCAN_OUTCOME_RESET:
    return NULL;
  case ASYNC_SCAN_OUTCOME_OOM:
    return end->rc == REDISMODULE_ASYNCSCAN_OUT_OF_MEMORY
               ? "Background indexing cancelled: the server reported out of memory; the index may "
                 "be incomplete"
               : "Background indexing cancelled: used memory exceeded the server's memory budget; "
                 "the index may be incomplete";
  case ASYNC_SCAN_OUTCOME_IO_ERROR:
    return "Background indexing failed: the server reported an I/O error; the index may be "
           "incomplete";
  case ASYNC_SCAN_OUTCOME_ERROR:
    break;
  }
  switch (end->rc) {
  case REDISMODULE_ASYNCSCAN_BUSY:
    return "Background indexing failed: the background scan could not be started because the "
           "server stayed busy; the index may be incomplete";
  case REDISMODULE_ASYNCSCAN_IN_PROGRESS:
    return "Background indexing failed: the background scan reported an unexpected in-progress "
           "state; the index may be incomplete";
  case REDISMODULE_ASYNCSCAN_UNSUPPORTED:
    return "Background indexing failed: the background scan is unsupported in this "
           "configuration; the index may be incomplete";
  default:
    return "Background indexing failed: the background scan was rejected as invalid; the index "
           "may be incomplete";
  }
}

// Record why the build is incomplete on the spec; an OOM also flags the spec as scan_failed_OOM,
// which queries warn about.
static void Backfill_Finish(AsyncScanJob *base, RedisModuleCtx *ctx, const AsyncScanEnd *end) {
  BackfillJob *job = (BackfillJob *)base;
  IndexesScanner *scanner = job->scanner;
  const bool oom = end->outcome == ASYNC_SCAN_OUTCOME_OOM;
  if (oom) {
    // Only the module's own memory check latches it, as the sync strategy does.
    scanner->scanFailedOnOOM = end->rc != REDISMODULE_ASYNCSCAN_OUT_OF_MEMORY;
    RedisModule_Log(ctx, "warning",
                    "AsyncScan: index %s may be incomplete; once memory pressure is relieved, drop "
                    "and recreate the index to rebuild it",
                    scanner->spec_name_for_logs);
  } else if (end->outcome == ASYNC_SCAN_OUTCOME_IO_ERROR) {
    RedisModule_Log(ctx, "warning",
                    "AsyncScan: index %s may be incomplete; resolve the underlying disk/I/O "
                    "condition, then drop and recreate the index to rebuild it",
                    scanner->spec_name_for_logs);
  }
  const char *failure = Backfill_FailureMessage(end);
  if (failure) {
    // No async failure has an offending key, so scanner->OOMkey is NULL and the error is recorded
    // without one.
    IndexesScanner_RecordBackgroundFailure(ctx, scanner, failure, oom);
  }

  // Under the GIL: IndexesScanner_Free clears sp->scanner / sp->scan_in_progress and the
  // process-global global_spec_scanner, which FT.INFO (fillReplyWithIndexInfo) reads on the main
  // thread without any per-spec lock.
  IndexesScanner_Free(scanner);
  Backfill_FreeFilter(&job->filterCtx);
  rm_free(job->nameForLogs);
  rm_free(job);
}

void Indexes_AsyncScanAndReindexTask(IndexesScanner *scanner) {
  RS_LOG_ASSERT(scanner, "invalid IndexesScanner");
  BackfillJob *job = rm_calloc(1, sizeof(*job));
  job->scanner = scanner;
  rm_asprintf(&job->nameForLogs, "index %s", scanner->spec_name_for_logs);
  job->base = (AsyncScanJob){
      .nameForLogs = job->nameForLogs,
      .scannedKeys = &scanner->scannedKeys,
      .shouldStop = Backfill_ShouldStop,
      .isCancelled = Backfill_IsCancelled,
      .start = Backfill_Start,
      .onKey = Backfill_OnKey,
      .finish = Backfill_Finish,
  };
  AsyncScan_Drive(&job->base);
}
