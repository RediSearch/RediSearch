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
#include "debug_commands.h"
#include "rmutil/rm_assert.h"
#include "rmalloc.h"
#include "rs_wall_clock.h"
#include "search_disk.h"

// The background-indexing debug context, owned by debug_commands.c. Read here only
// for the SET_SIMULATE_ASYNC_OOM test hook (ENABLE_ASSERT builds).
extern DebugCTX globalDebugCtx;

// Emit a throttled progress line roughly every this many scanned keys, so a long
// background indexing run is observable in the logs without a line per batch.
#define ASYNC_SCAN_PROGRESS_LOG_KEYS 100000

// Upper bound on how long the initial AsyncScan Start may keep returning BUSY (the
// max-inflight cap is saturated) before we give up. Under normal load the cap drains as
// other scans complete and Start is accepted in well under this; the bound is a safety
// valve so a wedged engine — or a bug where the cap never clears — cannot pin a reindex
// worker spinning on the retry loop forever.
#define ASYNC_SCAN_START_BUSY_TIMEOUT_MS 60000

// Backoff between retries when Start or NextBatch returns BUSY (the max-inflight cap is
// saturated). Short enough that the scan resumes promptly once the cap drains, long enough
// that the worker is not spinning hot on the GIL while it waits.
#define ASYNC_SCAN_BUSY_BACKOFF_US 1000

// Per-batch work hint passed to AsyncScan Start / NextBatch, sizing the hash range each
// batch sweeps. We pass an explicit value rather than 0 (which would defer to the engine's
// module-async-scan-default-batch-size config, currently 128). It is a hint, not a delivery
// cap — the engine may deliver more or fewer keys per batch.
#define ASYNC_SCAN_BATCH_SIZE_HINT_DEFAULT 100

// While parked behind the vector flat-buffer throttle, re-check (under the GIL) that the index
// still exists every this many backoff iterations. FT.DROPINDEX / FLUSHDB invalidate the weak
// spec ref without setting scanner->cancelled, and the throttle is global (another disk-vector
// index can hold it engaged after ours is gone), so the throttle clearing is not a reliable
// wake-up for a drop. At ASYNC_SCAN_BUSY_BACKOFF_US per iteration this bounds the "sleeping on a
// dropped spec" window to ~1s. Dropping mid-scan is rare, so a coarse interval keeps GIL churn
// during a long throttle wait negligible while still guaranteeing eventual teardown.
#define ASYNC_SCAN_THROTTLE_DROP_RECHECK_ITERS 1000

// Per-cursor driver state, shared between the reindexPool worker (which owns the
// cursor lifecycle) and the callbacks (which run on the main thread under the GIL).
// done_cb signals the condvar the worker waits on; the engine provides no waiter.
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

// Per-key callback. Runs on the main thread, GIL held, value pinned for the call.
// AsyncScan delivers at-least-once, so a key may arrive more than once (or after a live
// notification already indexed it); the idempotency guard below skips keys that already
// have a DocIdMeta mapping for this spec and indexes the rest.
static void Indexes_AsyncScanKeyCB(RedisModuleCtx *ctx, RedisModuleScanCursor *cursor,
                                   RedisModuleKey *key, RedisModuleString *name, void *privdata) {
  REDISMODULE_NOT_USED(cursor);
  AsyncReindexDriver *driver = privdata;
  IndexesScanner *scanner = driver->scanner;

  if (scanner->cancelled || scanner->scanFailedOnOOM) {
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
  // If IndexSpec was dropped mid-scan, cancel.
  scanner->cancelled |= !sp;
  if (!sp) {
    RedisModule_Log(ctx, "notice",
                    "AsyncScan: index %s dropped mid-scan; cancelling (scanned=%zu)",
                    scanner->spec_name_for_logs, scanner->scannedKeys);
  }
  if (!scanner->cancelled) {
    // Running on the main thread under the GIL serializes us against writers, so sp and
    // its rule stay stable across ShouldIndex and the DocIdMeta write below.
    //
    // ShouldIndex stays the authority even though the engine pre-filter already enforces
    // type and prefixes (those checks are redundant here): it also evaluates the index
    // FILTER expression, which the filter cannot express, and is the same predicate the
    // synchronous scan and keyspace notifications use. The redundant type/prefix check is
    // one cheap comparison per key.
    // `key` is already open and pinned for this call; pass it through so a FILTER
    // expression is evaluated against the pinned handle instead of reopening the
    // document by name, the same handle the open-key variants below reuse.
    if (SchemaRule_ShouldIndex(sp, name, type, key)) {
      uint64_t docId = 0;
      // `key` is already open and pinned for this call; the open-key variants reuse it
      // instead of reopening by name.
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

// Call-completion callback. Runs on the main thread, GIL held, once per Start/NextBatch
// that returned OK. The driver loop discovers terminal state from the return code of the
// next NextBatch, so we ignore `reason` here and only wake the waiting driver.
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

// Engine-side AsyncScan pre-filter built from an index's document type and key prefixes.
// The filter points into this context: `type_name` (a static literal) backs filter.types,
// and `prefix_copies` (our own NUL-terminated copies) backs filter.prefixes. The context
// must outlive the AsyncScanStart call.
//
// We copy the prefixes even though the engine copies the whole filter again at Start: an
// index has few, short prefixes so the extra copy is trivial, and owning the strings keeps
// this self-contained. Borrowing the spec's own bytes instead would force holding the
// spec's StrongRef across the whole Start retry loop for no real saving. Type names are
// static literals and need no copy.
typedef struct {
  RedisModuleAsyncScanFilter filter;
  const char *type_name;   // static literal backing filter.types (single entry)
  char **prefix_copies;    // owned NUL-terminated copies of the prefixes backing filter.prefixes
} AsyncScanFilterCtx;

// Build an engine-side pre-filter from a live index spec so the engine never delivers a
// key that cannot belong to it (every delivered key is processed on the main thread under
// the GIL with its value pinned, so pre-filtering is a pure win). This is only a coarse
// pre-filter: key_cb's SchemaRule_ShouldIndex stays the source of truth.
//
// `sp` must be kept alive by the caller (it holds a StrongRef across this call); we read
// type and prefixes from it but copy the prefixes, so nothing of sp's is held after Start.
// No spec read lock is needed even though this runs on the worker thread without the GIL:
// type and prefixes are immutable for the spec's lifetime (set at FT.CREATE / RDB-load,
// cleared at free; FT.ALTER does not touch them).
//
// Detecting a dropped spec is the caller's job, not this function's — it only translates a
// live spec into a filter. Returns the filter to pass to AsyncScanStart, or NULL when no
// pre-filter is needed (scan everything). The caller must call
// Indexes_AsyncScanFreeFilter(ctx) on every exit path regardless.
static RedisModuleAsyncScanFilter *Indexes_AsyncScanBuildFilter(const IndexSpec *sp,
                                                                AsyncScanFilterCtx *ctx) {
  // Type set: an index targets exactly one document type. "hash" is the core type
  // name; "ReJSON-RL" is RedisJSON's module type name (the engine resolves it to its
  // type id). An unexpected type is left unfiltered so ShouldIndex stays the sole
  // arbiter.
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

  // Prefix set: copy the index prefixes unless the index matches every key — its sole
  // prefix is the empty string, the default when FT.CREATE omits PREFIX. For a
  // match-all index the engine's no-filter fast path beats a per-key empty-prefix
  // comparison, so we leave the prefix set empty. The engine's byte-prefix match has
  // the same semantics as ShouldIndex's strncmp (neither is glob), so this never drops
  // a key ShouldIndex would have indexed.
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

  // The prefix copies above are self-owned, so nothing of sp's is held across Start; the
  // caller releases its StrongRef once this returns.

  // Pass NULL (not an empty struct) when there is nothing to filter, so the engine
  // takes its no-filter fast path.
  return (ctx->filter.num_types || ctx->filter.num_prefixes) ? &ctx->filter : NULL;
}

// Free the prefix copies the filter owns. The engine deep-copies the filter at Start,
// so these are unreferenced by the time the scan tears down, regardless of how it
// ended. Idempotent and safe on a zero-initialized context.
static void Indexes_AsyncScanFreeFilter(AsyncScanFilterCtx *ctx) {
  if (ctx->prefix_copies) {
    for (size_t i = 0; i < ctx->filter.num_prefixes; ++i) {
      rm_free(ctx->prefix_copies[i]);
    }
    rm_free(ctx->prefix_copies);
    ctx->prefix_copies = NULL;
  }
}

// Record a background-indexing failure. Call WITHOUT the GIL held: this helper takes the
// GIL itself around IndexesScanner_RecordBackgroundFailure and releases it before
// returning. Several terminal result codes surface the same way, so they share it. The GIL
// is required because IndexesScanner_RecordBackgroundFailure → IndexError_AddError mutates
// the shared NA_rstr sentinel's refcount via RedisModule_HoldString/FreeString, which
// require it. `oom` routes to the spec's scan_failed_OOM flag (warns at query time,
// aggregated in FT.INFO).
static void Indexes_AsyncScanRecordFailure(RedisModuleCtx *ctx, IndexesScanner *scanner,
                                                 const char *msg, bool oom) {
  RedisModule_ThreadSafeContextLock(ctx);
  IndexesScanner_RecordBackgroundFailure(ctx, scanner, msg, oom);
  RedisModule_ThreadSafeContextUnlock(ctx);
}

// First call: bind all parameters to the cursor and queue the first batch. Start is
// subject to the same max-inflight cap as NextBatch and can return BUSY, so we retry the
// *same* call (no binding exists yet) until it is accepted. The GIL is held only for the
// call; the backoff sleeps with it released.
//
// The engine-side pre-filter (type + prefixes) is built lazily the first time we hold the
// spec alive — it cannot be built from a dropped spec, and building it here reuses the
// cancellation-check promotion instead of taking the ref a second time. A real index always
// targets a document type, so the build always yields a non-NULL filter (asserted); a local
// `filter_ptr == NULL` is the "not built yet" sentinel so a BUSY retry reuses it rather than
// rebuilding (and leaking) it. The filter is owned entirely by this function: the engine
// deep-copies it during Start, so it is unreferenced the moment Start returns and is freed
// before every exit below.
//
// Returns the result code of the accepted Start, to drive from. Sets *out_terminal when the
// scan must tear down without driving — cancelled/dropped while starting, or the BUSY
// safety-valve timeout fired (both already logged / recorded here); the returned code is
// meaningless in that case.
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
    // Check for cancellation / a dropped spec *before* issuing Start, under the same GIL
    // hold that issues it, so nothing can interleave between the check and the bind. Two
    // writers we must notice, neither of which can race us while we hold the GIL:
    //   - IndexesScanner_Cancel sets scanner->cancelled on the main thread under the GIL
    //     (e.g. an FT.ALTER that schedules a replacement scan); and
    //   - FT.DROPINDEX / FLUSHDB invalidate the weak ref but do NOT call
    //     IndexesScanner_Cancel, so promote the weak ref to fold a drop into the same flag
    //     (mirrors the per-key and between-batches re-checks).
    // Checking before Start means a drop during the BUSY spin stops the retries instead of
    // letting us eventually bind a cursor for a dead index — on an empty DB, or with a
    // filter that yields no key_cb, nothing else would observe the invalid weak ref and the
    // sweep would otherwise run to exhaustion.
    if (!scanner->cancelled) {
      StrongRef live_ref = IndexSpecRef_Promote(scanner->spec_ref);
      IndexSpec *sp = StrongRef_Get(live_ref);
      if (!sp) {
        scanner->cancelled = true;
      } else {
        // Build the pre-filter lazily, the first time we hold the spec alive (see the
        // function comment). key_cb's SchemaRule_ShouldIndex stays the source of truth.
        if (!filter_ptr) {
          filter_ptr = Indexes_AsyncScanBuildFilter(sp, &filter_ctx);
          RS_ASSERT(filter_ptr);
        }
        IndexSpecRef_Release(live_ref);
      }
    }
    if (scanner->cancelled) {
      RedisModule_ThreadSafeContextUnlock(ctx);
      // Start never bound the cursor, so there is nothing to abort — just tear down.
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
    // Safety valve against an unbounded BUSY spin: if Start never gets accepted within
    // the timeout, give up rather than pin this worker forever. The cursor was never
    // bound (Start never succeeded), so there is nothing to abort — record the failure so
    // the index is flagged incomplete, then tear down.
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
  // The engine has deep-copied the filter during Start (or it was never built, on the
  // cancelled path) — free our copies here so the caller has nothing to clean up. Idempotent
  // and safe on a zero-initialized context.
  Indexes_AsyncScanFreeFilter(&filter_ctx);
  return rc;
}

// Handle one OK batch: wait for done_cb (GIL released), then under a single GIL hold
// re-check for cancellation / a spec dropped between batches, emit throttled progress, and
// queue the next batch. Inspecting shared scanner/spec state and driving the next batch
// under a single GIL hold is the same synchronization contract the synchronous scanner
// relies on: it serializes the read of scanner->cancelled against IndexesScanner_Cancel
// (main thread, under the GIL) instead of racing it.
//
// Fold a dropped index into scanner->cancelled, taking the GIL for the check. FT.DROPINDEX /
// FLUSHDB invalidate the weak spec ref WITHOUT calling IndexesScanner_Cancel, so promoting the
// ref is how we notice a drop; a dead ref is collapsed into scanner->cancelled. Acquires and
// releases the GIL itself, so call it only when the GIL is NOT already held (i.e. from the
// throttle backoff wait, which sleeps GIL-released). Sites that already hold the GIL inline the
// promote instead.
static void Indexes_AsyncScanCancelIfDropped(RedisModuleCtx *ctx, IndexesScanner *scanner) {
  RedisModule_ThreadSafeContextLock(ctx);
  if (!scanner->cancelled) {
    StrongRef live_ref = IndexSpecRef_Promote(scanner->spec_ref);
    if (!StrongRef_Get(live_ref)) {
      scanner->cancelled = true;
    } else {
      IndexSpecRef_Release(live_ref);
    }
  }
  RedisModule_ThreadSafeContextUnlock(ctx);
}

// Returns the result code of the next NextBatch, to drive from. Sets *out_terminal when the
// scan was cancelled or the spec was dropped between batches — the cursor has been aborted
// and the caller must tear down (already logged here); the returned code is meaningless then.
static RedisModuleAsyncScanResult Indexes_AsyncScanDriveNextBatch(
    RedisModuleCtx *ctx, RedisModuleScanCursor *cursor, IndexesScanner *scanner,
    AsyncReindexDriver *driver, size_t *batchesDone, size_t *lastProgressKeys,
    bool *out_terminal) {
  *out_terminal = false;

  // Batch queued. Wait (GIL released) for done_cb to fire on the main thread.
  Indexes_AsyncScanWaitForDone(driver);
  ++*batchesDone;

  // Test hook: park the driver between batches, with the GIL still released, so a test can
  // deterministically drop the index / flush the keyspace after a batch has been processed
  // but before the next is requested — exercising the between-batches drop/reset re-check
  // below (which the pre-first-batch hook cannot reach). No-op unless armed via
  // _FT.DEBUG SYNC_POINT (ENABLE_ASSERT builds).
#ifdef ENABLE_ASSERT
  SyncPoint_Wait(SYNC_POINT_ASYNC_SCAN_BETWEEN_BATCHES);
#endif

  // Vector-index flat-buffer back-pressure. When a disk tiered vector index fills its
  // in-memory flat buffer it raises the Redis client-postpone throttle to slow writers, but
  // that throttle only gates client CMD_DENYOOM commands — this background scan bypasses
  // command dispatch and would keep calling addVector, growing the buffer without bound.
  // Wait here (GIL released) until the async insert jobs drain the buffer and clear the
  // throttle before requesting the next batch, applying the same back-pressure to ourselves.
  // The drain jobs run on a separate worker pool that does not need the GIL, so they make
  // progress while we sleep; sleeping with the GIL released also keeps the main thread free
  // to run the reads / memory-freeing commands that let the system recover. Re-check
  // scanner->cancelled each iteration so an FT.DROP / FT.ALTER detected mid-scan tears down
  // promptly, and periodically re-promote the weak spec ref (see below) so a drop that does
  // NOT set scanner->cancelled cannot leave us parked forever. Mirrors the BUSY backoff below.
  //
  // Skip the wait if the just-finished batch tripped scanFailedOnOOM: the OOM branch below must
  // abort the cursor and surface the background-indexing failure promptly, not sit behind a
  // throttle that (for a disk vector index also under flat-buffer back-pressure) may be slow or
  // stuck to clear. OOM is only ever set by key_cb during a batch, never during this wait, so
  // gating entry is sufficient; the loop condition re-checks it defensively.
  if (SearchDisk_IsThrottling() && !scanner->cancelled && !scanner->scanFailedOnOOM) {
    RedisModule_Log(ctx, "debug",
                    "AsyncScan: index %s throttled (vector flat buffer full); backing off",
                    scanner->spec_name_for_logs);
    size_t backoffIters = 0;
    while (SearchDisk_IsThrottling() && !scanner->cancelled && !scanner->scanFailedOnOOM) {
      usleep(ASYNC_SCAN_BUSY_BACKOFF_US);
      // FT.DROPINDEX / FLUSHDB invalidate the weak ref WITHOUT setting scanner->cancelled, and
      // the throttle is global — another disk-vector index can hold it engaged after ours is
      // gone — so the throttle clearing is not a reliable wake-up for a drop. Without this the
      // worker could sleep here indefinitely on a spec that no longer exists. Every
      // ASYNC_SCAN_THROTTLE_DROP_RECHECK_ITERS iterations, promote the weak ref under the GIL;
      // if the spec is gone, mark the scan cancelled so the loop exits and the promotion
      // re-check below aborts the sweep.
      if (++backoffIters % ASYNC_SCAN_THROTTLE_DROP_RECHECK_ITERS == 0) {
        Indexes_AsyncScanCancelIfDropped(ctx, scanner);
      }
    }
    // Log the transition back to indexing so the pause is bounded and observable in the
    // logs: either the buffer drained and we resume batching, or the scan ended (cancelled or
    // its index dropped) while we waited (handled by the re-checks below). ~ms waited =
    // iters * backoff(us)/1000.
    RedisModule_Log(ctx, "debug",
                    "AsyncScan: index %s throttle %s after ~%zu ms; resuming batches",
                    scanner->spec_name_for_logs,
                    scanner->cancelled ? "wait aborted (index dropped or scan cancelled)"
                                       : "cleared",
                    (backoffIters * (size_t)ASYNC_SCAN_BUSY_BACKOFF_US) / 1000);
  }

  RedisModule_ThreadSafeContextLock(ctx);

  // Re-check for a spec dropped between batches. FT.DROPINDEX / FLUSHDB invalidate the
  // weak ref but do NOT call IndexesScanner_Cancel; with a selective type/prefix filter no
  // key_cb may run to observe the drop, so without this re-check the worker would keep
  // requesting batches until the whole keyspace is exhausted for an index that no longer
  // exists. Promoting here collapses that case into cancellation.
  if (!scanner->cancelled) {
    StrongRef live_ref = IndexSpecRef_Promote(scanner->spec_ref);
    if (!StrongRef_Get(live_ref)) {
      scanner->cancelled = true;
      RedisModule_Log(ctx, "notice",
                      "AsyncScan: index %s dropped mid-scan; cancelling (scanned=%zu)",
                      scanner->spec_name_for_logs, scanner->scannedKeys);
    } else {
      IndexSpecRef_Release(live_ref);
    }
  }

  // Cancelled (FT.DROP / FT.ALTER, detected per-key or just above): abort the sweep
  // instead of requesting another batch. Abort is safe on a cursor that has already
  // reached terminal state (it is a NOOP) and on an active one (it publishes terminal
  // state), so the subsequent ScanCursorDestroy at teardown is always valid.
  if (scanner->cancelled) {
    RedisModule_AsyncScanAbort(cursor);
    RedisModule_ThreadSafeContextUnlock(ctx);
    RedisModule_Log(ctx, "notice",
                    "AsyncScan: aborting scan of index %s (cancelled, scanned=%zu)",
                    scanner->spec_name_for_logs, scanner->scannedKeys);
    *out_terminal = true;
    return REDISMODULE_ASYNCSCAN_ABORTED;
  }

  // OOM flagged by key_cb during this batch (mirrors the sync scan's between-iterations
  // handling in Indexes_ScanAndReindexTask). The configured indexing memory limit was hit;
  // abort the still-active cursor and surface OOM the same way the engine OUT_OF_MEMORY
  // branch does. We do not restart the cursor to resume, so the sweep ends here and the
  // index may be incomplete.
  if (scanner->scanFailedOnOOM) {
    RedisModule_AsyncScanAbort(cursor);
    RedisModule_ThreadSafeContextUnlock(ctx);
    RedisModule_Log(ctx, "warning",
                    "AsyncScan: index %s exceeded the indexing memory limit; aborting "
                    "(scanned=%zu). The index may be incomplete; once memory pressure is "
                    "relieved, drop and recreate the index to rebuild it",
                    scanner->spec_name_for_logs, scanner->scannedKeys);
    // RecordFailure re-acquires the GIL itself, so it must be called unlocked. The offending
    // key is taken from scanner->OOMkey (held in key_cb).
    Indexes_AsyncScanRecordFailure(
        ctx, scanner,
        "Background indexing cancelled: used memory exceeded the configured indexing memory "
        "limit; the index may be incomplete",
        /*oom=*/true);
    *out_terminal = true;
    return REDISMODULE_ASYNCSCAN_OUT_OF_MEMORY;
  }

  // Throttled progress: one line every ASYNC_SCAN_PROGRESS_LOG_KEYS keys so a
  // long background indexing run is observable without a line per batch.
  if (scanner->scannedKeys - *lastProgressKeys >= ASYNC_SCAN_PROGRESS_LOG_KEYS) {
    *lastProgressKeys = scanner->scannedKeys;
    RedisModule_Log(ctx, "notice",
                    "AsyncScan: index %s progress: scanned=%zu keys, batches=%zu",
                    scanner->spec_name_for_logs, scanner->scannedKeys, *batchesDone);
  }

  // Test hook: force the OOM terminal branch after a batch has been processed, so a
  // flow test can exercise the OOM-surfacing path deterministically (the engine's
  // terminal OUT_OF_MEMORY is not reproducible from the module side). No-op unless
  // armed via _FT.DEBUG BG_SCAN_CONTROLLER SET_SIMULATE_ASYNC_OOM true. The caller's
  // switch handles the returned OUT_OF_MEMORY on its next iteration.
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

  // Test hook: park the driver before any batch is queued (no GIL held here), so a
  // test can deterministically drop the index / cancel the scan mid-flight. No-op
  // unless armed via _FT.DEBUG SYNC_POINT (ENABLE_ASSERT builds).
#ifdef ENABLE_ASSERT
  SyncPoint_Wait(SYNC_POINT_ASYNC_SCAN_BEFORE_FIRST_BATCH);
#endif

  // Start the scan (binds the cursor, queues the first batch, retries through BUSY). On a
  // cancelled/dropped start or the BUSY-timeout safety valve this returns terminal — the
  // cursor was never bound, so we just tear down.
  bool start_terminal = false;
  RedisModuleAsyncScanResult rc =
      Indexes_AsyncScanStartWithRetry(ctx, cursor, scanner, &driver, &start_terminal);
  if (start_terminal) {
    goto done;
  }

  // Drive the cursor one batch at a time, switching on the result code. done_cb only
  // wakes us; terminal state arrives as the return code of the next NextBatch.
  for (;;) {
    switch (rc) {
    case REDISMODULE_ASYNCSCAN_OK: {
      // Process the completed batch and queue the next one; bails to teardown if the scan
      // was cancelled or the spec dropped between batches.
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
      // Dataset reset (FLUSHALL / FLUSHDB / DEBUG RELOAD) ended the sweep early; the
      // cursor is no longer usable.
      RedisModule_Log(ctx, "notice",
                      "AsyncScan: scan of index %s ended early "
                      "(dataset reset, scanned=%zu, rc=DATASET_RESET)",
                      scanner->spec_name_for_logs,
                      scanner->scannedKeys);
      goto done;
    case REDISMODULE_ASYNCSCAN_ABORTED:
      // The sweep was aborted; the cursor is no longer usable.
      RedisModule_Log(ctx, "notice",
                      "AsyncScan: scan of index %s ended early (aborted, scanned=%zu, rc=ABORTED)",
                      scanner->spec_name_for_logs,
                      scanner->scannedKeys);
      goto done;

    case REDISMODULE_ASYNCSCAN_OUT_OF_MEMORY:
      // Engine memory pressure cut the sweep short; the index may be incomplete and is
      // not resumed automatically.
      RedisModule_Log(ctx, "warning",
                      "AsyncScan: engine OOM during scan of index %s; index may be incomplete "
                      "(scanned=%zu, rc=OUT_OF_MEMORY). The scan is not retried automatically; "
                      "once memory pressure is relieved, drop and recreate the index to rebuild it",
                      scanner->spec_name_for_logs, scanner->scannedKeys);
      // Surface the failure like the synchronous scan does: set the spec's scan_failed_OOM
      // flag (warns at query time, aggregated in FT.INFO) and raise the background-indexing
      // failure flag. No single key is to blame (the engine cut delivery for global memory
      // pressure), so the offending key is NULL.
      Indexes_AsyncScanRecordFailure(
          ctx, scanner,
          "Background indexing cancelled: the server reported out of memory; the index may be "
          "incomplete",
          /*oom=*/true);
      goto done;

    case REDISMODULE_ASYNCSCAN_IN_PROGRESS:
      // Should not happen: the driver always waits for done_cb before reissuing, so no
      // batch is ever in flight when we call NextBatch. IN_PROGRESS means we reissued
      // anyway while the previous batch was still running — a driver bug.
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
      // AsyncScan unavailable in this build/runtime. We only route disk indexes here,
      // where it is guaranteed present.
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
  // Each exit path above logs its own outcome (completed / cancelled / ended early /
  // OOM / error); this is just the teardown marker.
  RedisModule_Log(ctx, "debug", "AsyncScan: index %s scan task exiting (scanned=%zu)",
                  scanner->spec_name_for_logs, scanner->scannedKeys);

  // Take the GIL around IndexesScanner_Free: it clears sp->scanner /
  // sp->scan_in_progress and frees scanner->OOMkey, all of which FT.INFO reads on the
  // main thread (fillReplyWithIndexInfo). Unlike the synchronous scanner — which holds
  // the GIL through teardown — this task runs with the GIL released between batches, so
  // an initial scan finishing concurrently with INFO would otherwise race or leave INFO
  // dereferencing a freed scanner. Mirror the sync path: lock, free, unlock.
  //
  // Taking the IndexSpec lock instead is not enough, for two reasons:
  //   1. IndexesScanner_Free also reads and clears `global_spec_scanner`, which is
  //      process-global state owned by no single spec; fillReplyWithIndexInfo reads that
  //      same global. A per-spec lock cannot serialize an access to it.
  //   2. fillReplyWithIndexInfo loads `sp->scanner` into a local and then dereferences it
  //      (IndexesScanner_IndexedPercent reads scanner->scannedKeys) without taking any
  //      per-spec lock — it relies on the GIL. So even if this worker held the spec write
  //      lock, the reader takes no matching read lock; the lock would not actually
  //      serialize the two, and the free could leave INFO with a dangling pointer.
  // The GIL is therefore the real synchronization contract here, the same one the
  // synchronous scanner relies on.
  RedisModule_ThreadSafeContextLock(ctx);
  IndexesScanner_Free(scanner);
  RedisModule_ThreadSafeContextUnlock(ctx);

  pthread_cond_destroy(&driver.cond);
  pthread_mutex_destroy(&driver.mutex);
  RedisModule_ScanCursorDestroy(cursor);
  RedisModule_FreeThreadSafeContext(ctx);
}
