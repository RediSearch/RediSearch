/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#ifndef __INDEXES_SCAN_H__
#define __INDEXES_SCAN_H__

#include "redismodule.h"
#include "util/references.h"
// The IndexesScanner type, its lifecycle, and the OOM helpers are shared with the
// AsyncScan strategy and live in the scanner core. Re-exported here so existing
// includers of indexes_scan.h keep seeing the scanner types.
#include "indexes_scanner.h"

#ifdef __cplusplus
extern "C" {
#endif

///////////////////////////////////////////////////////////////////////////////////////////////

// Schedule a background scan + reindex of the keyspace into the given spec.
// Assumes that the spec is in a safe state to set a scanner on it (write lock
// or main thread).
void IndexSpec_ScanAndReindex(RedisModuleCtx *ctx, StrongRef ref);

// Schedule a background scan + reindex of all registered indexes.
void Indexes_ScanAndReindex();

// Upgrade legacy (pre-RDB-event) indexes by dropping their old keyspace
// representation and publishing them into the global registry.
void Indexes_UpgradeLegacyIndexes();

// Expose reindexpool for debug
void ReindexPool_ThreadPoolDestroy();

///////////////////////////////////////////////////////////////////////////////////////////////

#ifdef __cplusplus
}
#endif

#endif  // __INDEXES_SCAN_H__
