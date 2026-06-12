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

// Entry point for the reindexPool worker. Runs the full cursor lifecycle for a
// single index (Start -> wait for done_cb -> NextBatch, until a terminal reason),
// takes ownership of `scanner`, and frees it before returning. One task per index,
// exactly as the synchronous scan is scheduled.
void Indexes_AsyncScanAndReindexTask(IndexesScanner *scanner);

#ifdef __cplusplus
}
#endif

#endif  // __INDEXES_ASYNCSCAN_H__
