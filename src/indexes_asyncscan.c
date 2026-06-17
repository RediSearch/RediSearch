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

// The background-indexing debug context, owned by debug_commands.c. Read here only
// for the SET_SIMULATE_ASYNC_OOM test hook (ENABLE_ASSERT builds).
extern DebugCTX globalDebugCtx;

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
    // Running on the main thread under the GIL serializes us against writers, so sp and
    // its rule stay stable across ShouldIndex and the DocIdMeta write below.
    //
    // ShouldIndex stays the authority even though the engine pre-filter already enforces
    // type and prefixes (those checks are redundant here): it also evaluates the index
    // FILTER expression, which the filter cannot express, and is the same predicate the
    // synchronous scan and keyspace notifications use. The redundant type/prefix check is
    // one cheap comparison per key.
    if (SchemaRule_ShouldIndex(sp, name, type)) {
      uint64_t docId = 0;
      // `key` is already open and pinned for this call; the open-key variants reuse it
      // instead of reopening by name.
      if (DocIdMeta_GetWithOpenKey(key, sp->specId, &docId) == REDISMODULE_OK && docId != 0) {
        // Already indexed in this spec; skip to avoid clobbering a fresher version.
      } else {
        IndexSpec_UpdateDoc(sp, ctx, name, type, key);
      }
    }
    IndexSpecRef_Release(curr_run_ref);
  } else {
    RedisModule_AsyncScanAbort(cursor);
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
// Indexes_AsyncScanFreeFilter(ctx) at teardown regardless.
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

  // Engine-side pre-filter for the sweep, built just before the first Start.
  // Zero-initialized here (before any `goto done`) so every teardown path can free
  // it; `filter_ptr` stays NULL until built, which means "scan everything".
  AsyncScanFilterCtx filter_ctx = {0};
  RedisModuleAsyncScanFilter *filter_ptr = NULL;

  RedisModule_Log(ctx, "notice", "AsyncScan: scanning index %s in background",
                  scanner->spec_name_for_logs);

  // Test hook: park the driver before any batch is queued (no GIL held here), so a
  // test can deterministically drop the index / cancel the scan mid-flight. No-op
  // unless armed via _FT.DEBUG SYNC_POINT (ENABLE_ASSERT builds).
#ifdef ENABLE_ASSERT
  SyncPoint_Wait(SYNC_POINT_ASYNC_SCAN_BEFORE_FIRST_BATCH);
#endif

  // Promote the weak ref once, before the first Start, to decide whether the sweep should
  // run at all. If the spec was already dropped (FT.DROPINDEX / FLUSHDB before we got here)
  // we must not start: key_cb cancels a sweep on a dropped spec, but on an empty DB — or one
  // holding only unsupported-type keys — it never runs to do so, and a filter built from a
  // dead spec is impossible anyway, so an unfiltered sweep would otherwise run to
  // exhaustion for a dead index. Cancel and tear down instead.
  {
    StrongRef build_ref = IndexSpecRef_Promote(scanner->spec_ref);
    IndexSpec *sp = StrongRef_Get(build_ref);
    if (!sp) {
      scanner->cancelled = true;
      RedisModule_Log(ctx, "notice", "AsyncScan: index %s dropped before scan started; skipping",
                      scanner->spec_name_for_logs);
      goto done;  // promote failed: nothing to release
    }
    // Build the engine-side pre-filter (type + prefixes) once, before the first Start, while
    // we hold the spec alive. NULL means "scan everything"; key_cb's SchemaRule_ShouldIndex
    // stays the source of truth. Built outside the retry loop so a BUSY retry reuses it
    // rather than rebuilding (and leaking) it.
    filter_ptr = Indexes_AsyncScanBuildFilter(sp, &filter_ctx);
    IndexSpecRef_Release(build_ref);
  }

  // First call: bind all parameters to the cursor and queue the first batch. Start is
  // subject to the same max-inflight cap as NextBatch and can return BUSY, so we retry the
  // *same* call (no binding exists yet) until it is accepted. The GIL is held only for the
  // call; the backoff sleeps with it released.
  RedisModuleAsyncScanResult rc;
  for (;;) {
    RedisModule_ThreadSafeContextLock(ctx);
    rc = RedisModule_AsyncScanStart(ctx, cursor, filter_ptr,
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

  // Start has bound the filter — the engine deep-copies prefixes and resolves type
  // names during the call (see RedisModuleAsyncScanFilter) — so our copies are
  // unreferenced from here on and we free them now rather than holding them for the
  // whole sweep. Teardown calls this too (it is idempotent) for the
  // cancelled-while-starting path above.
  Indexes_AsyncScanFreeFilter(&filter_ctx);

  // Drive the cursor one batch at a time, switching on the result code. done_cb only
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

      // Test hook: force the OOM terminal branch after a batch has been processed, so a
      // flow test can exercise the OOM-surfacing path deterministically (the engine's
      // terminal OUT_OF_MEMORY is not reproducible from the module side). No-op unless
      // armed via _FT.DEBUG BG_SCAN_CONTROLLER SET_SIMULATE_ASYNC_OOM true.
#ifdef ENABLE_ASSERT
      if (globalDebugCtx.bgIndexing.simulateAsyncOOM) {
        RedisModule_Log(ctx, "warning",
                        "AsyncScan: SIMULATE_ASYNC_OOM hook forcing OOM for index %s",
                        scanner->spec_name_for_logs);
        rc = REDISMODULE_ASYNCSCAN_OUT_OF_MEMORY;
        break;
      }
#endif

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
      // Dataset reset (FLUSHALL / FLUSHDB / DEBUG RELOAD) ended the sweep early; the
      // cursor is no longer usable.
      RedisModule_Log(ctx, "notice", "AsyncScan: scan of index %s ended early (dataset reset, scanned=%zu)",
                      scanner->spec_name_for_logs,
                      scanner->scannedKeys);
      goto done;
    case REDISMODULE_ASYNCSCAN_ABORTED:
      // The sweep was aborted; the cursor is no longer usable.
      RedisModule_Log(ctx, "notice", "AsyncScan: scan of index %s ended early (aborted, scanned=%zu)",
                      scanner->spec_name_for_logs,
                      scanner->scannedKeys);
      goto done;

    case REDISMODULE_ASYNCSCAN_OUT_OF_MEMORY:
      // Engine memory pressure cut the sweep short; the index may be incomplete and is
      // not resumed automatically.
      RedisModule_Log(ctx, "warning",
                      "AsyncScan: engine OOM during scan of index %s; index may be incomplete (scanned=%zu). "
                      "The scan is not retried automatically; once memory pressure is relieved, drop and "
                      "recreate the index to rebuild it",
                      scanner->spec_name_for_logs, scanner->scannedKeys);
      // Surface the failure like the synchronous scan does: set the spec's scan_failed_OOM
      // flag (warns at query time, aggregated in FT.INFO) and raise the background-indexing
      // failure flag. No single key is to blame (the engine cut delivery for global memory
      // pressure), so the offending key is NULL.
      //
      // Take the GIL around the record: IndexError_AddError mutates the shared NA_rstr
      // sentinel's refcount via RedisModule_HoldString/FreeString, which require the GIL.
      RedisModule_ThreadSafeContextLock(ctx);
      IndexesScanner_RecordBackgroundFailure(
          ctx, scanner,
          "Background indexing cancelled: the server reported out of memory; the index may be "
          "incomplete",
          /*oom=*/true);
      RedisModule_ThreadSafeContextUnlock(ctx);
      goto done;

    case REDISMODULE_ASYNCSCAN_IN_PROGRESS:
      // Should not happen: we never reissue before done_cb fires.
      RedisModule_Log(ctx, "warning", "AsyncScan: unexpected IN_PROGRESS for index %s (caller bug)",
                      scanner->spec_name_for_logs);
      RS_ASSERT(false);
      goto done;

    case REDISMODULE_ASYNCSCAN_UNSUPPORTED:
      // AsyncScan unavailable in this build/runtime. We only route disk indexes here,
      // where it is guaranteed present.
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
    case REDISMODULE_ASYNCSCAN_IO_ERROR:
      // Unrecoverable I/O error; the index may be incomplete.
      RedisModule_Log(ctx, "warning",
        "AsyncScan: I/O error for index %s; index may be incomplete. The scan is not retried automatically; "
        "resolve the underlying disk/I/O condition, then drop and recreate the index to rebuild it",
        scanner->spec_name_for_logs);
      // Surface the failure so clients don't treat the partial index as fully built:
      // record it as the spec's last indexing error (visible in FT.INFO). Pass oom=false
      // so it is not mislabelled as out-of-memory. Take the GIL around the record, as the
      // OOM branch does (shared NA_rstr refcount churn).
      RedisModule_ThreadSafeContextLock(ctx);
      IndexesScanner_RecordBackgroundFailure(
          ctx, scanner,
          "Background indexing failed: the server reported an I/O error; the index may be "
          "incomplete",
          /*oom=*/false);
      RedisModule_ThreadSafeContextUnlock(ctx);
      goto done;
    }
  }

done:
  // Each exit path above logs its own outcome (completed / cancelled / ended early /
  // OOM / error); this is just the teardown marker.
  RedisModule_Log(ctx, "debug", "AsyncScan: index %s scan task exiting (scanned=%zu)",
                  scanner->spec_name_for_logs, scanner->scannedKeys);

  Indexes_AsyncScanFreeFilter(&filter_ctx);

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
