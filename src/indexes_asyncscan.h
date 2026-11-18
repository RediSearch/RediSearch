/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#ifndef __INDEXES_ASYNCSCAN_H__
#define __INDEXES_ASYNCSCAN_H__

#include <stdbool.h>
#include <stddef.h>

#include "redismodule.h"
#include "document_rs.h"

#ifdef __cplusplus
extern "C" {
#endif

// AsyncScan-driven background reindex (disk), a sibling of the synchronous
// scanner in indexes_scan.{c,h}; both build on the shared core indexes_scanner.{c,h}.
//
// On disk the per-key value load is a synchronous blocking disk read; the
// synchronous RM_Scan path performs it on the scanning thread while it holds the
// GIL. The RedisModule_AsyncScan API offloads that read to engine IO threads with
// the GIL released, then delivers each key to a callback that runs on the main
// thread under a single continuous GIL hold. We index inside that callback exactly
// as a live keyspace notification would.

// Forward declaration: the scanner type lives in the shared core indexes_scanner.{c,h}.
// The async driver only needs it by pointer here; the .c includes indexes_scanner.h
// for the full definition. It never depends on indexes_scan.h, so the synchronous and
// AsyncScan strategies stay siblings with no dependency cycle between them.
typedef struct IndexesScanner IndexesScanner;
typedef struct IndexSpec IndexSpec;

// Entry point for the reindexPool worker. Runs the full cursor lifecycle for a
// single index (Start -> wait for done_cb -> NextBatch, until a terminal reason),
// takes ownership of `scanner`, and frees it before returning. One task per index,
// exactly as the synchronous scan is scheduled.
void Indexes_AsyncScanAndReindexTask(IndexesScanner *scanner);

// How a sweep driven by AsyncScan_Drive ended. Exactly one is reported per sweep.
typedef enum {
  // Every selected key was delivered (the cursor is exhausted).
  ASYNC_SCAN_OUTCOME_COMPLETED,
  // The job stopped the sweep (AsyncScanJob.shouldStop or AsyncScanJob.onKey).
  ASYNC_SCAN_OUTCOME_CANCELLED,
  // A dataset reset (FLUSHALL, FLUSHDB, DEBUG RELOAD) ended the sweep.
  ASYNC_SCAN_OUTCOME_RESET,
  // The server's memory budget ended the sweep, as the module or the engine saw it.
  ASYNC_SCAN_OUTCOME_OOM,
  ASYNC_SCAN_OUTCOME_IO_ERROR,
  // The sweep never started (the engine stayed busy) or the engine rejected it.
  ASYNC_SCAN_OUTCOME_ERROR,
} AsyncScanOutcome;

const char *AsyncScanOutcome_ToString(AsyncScanOutcome outcome);

// One sweep of an AsyncScan cursor, run by AsyncScan_Drive. The driver owns the cursor and
// the batch loop: BUSY retries, the vector flat-buffer back-pressure, the memory checks and
// the mapping of the engine's terminal codes to an AsyncScanOutcome. The job decides which
// keys the sweep selects and what each key does.
//
// Embed it in the job's own state and recover that state from the pointer the hooks get.
typedef struct AsyncScanJob AsyncScanJob;
struct AsyncScanJob {
  // Names the sweep in logs, e.g. "index idx". Must stay valid until `finish` is called.
  const char *nameForLogs;
  // Keys delivered so far, maintained by `onKey`; read for the progress logs.
  const size_t *scannedKeys;
  // Whether the sweep must stop. Called with or without the GIL, so it must be lock-free.
  bool (*shouldStop)(AsyncScanJob *job);
  // Binds `cursor` and queues the first batch: RedisModule_AsyncScanStart or one of its
  // variants, passing `key_cb`, `done_cb`, `batchSizeHint` and `privdata` through unchanged.
  // GIL held. Called again after each REDISMODULE_ASYNCSCAN_BUSY.
  RedisModuleAsyncScanResult (*start)(AsyncScanJob *job, RedisModuleCtx *ctx,
                                      RedisModuleScanCursor *cursor,
                                      RedisModuleAsyncScanKeyCB key_cb,
                                      RedisModuleAsyncScanCallDoneCB done_cb,
                                      size_t batchSizeHint, void *privdata);
  // One delivered key. GIL held, value pinned. Returns false to stop the sweep.
  bool (*onKey)(AsyncScanJob *job, RedisModuleCtx *ctx, RedisModuleKey *key,
                RedisModuleString *name);
  // The sweep ended with `outcome`. GIL held. Called exactly once and last, so it may free
  // the job.
  void (*finish)(AsyncScanJob *job, RedisModuleCtx *ctx, AsyncScanOutcome outcome);
};

// Runs one sweep of `job` to its end, on the calling thread, which must not hold the GIL
// (a reindexPool worker). Returns after `job->finish`.
void AsyncScan_Drive(AsyncScanJob *job);

// Indexes a key an AsyncScan delivered into `sp`, if the spec's rule selects it and the key
// carries no docId for `sp` yet; returns whether it did. A document that fails to load counts as
// indexed: the failure is recorded on the spec, as for a keyspace notification. GIL held; `key` is
// the pinned key of the delivery.
bool Indexes_AsyncScanIndexKey(IndexSpec *sp, RedisModuleCtx *ctx, RedisModuleKey *key,
                               RedisModuleString *name, DocumentType type);

#ifdef __cplusplus
}
#endif

#endif  // __INDEXES_ASYNCSCAN_H__
