/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

// Spec-side bridge over the generic background scan engine (index_scan.{c,h}).
// Supplies the per-key indexing callbacks and owns the scanner<->spec linkage and the
// global (scan-all) scanner. This is the only translation unit that depends on both
// the spec layer (spec.h) and the scan engine (index_scan.h); the engine itself stays
// free of any spec dependency.

#ifndef SPEC_SCAN_H__
#define SPEC_SCAN_H__

#include "spec.h"

#ifdef __cplusplus
extern "C" {
#endif

// Schedule a background scan+reindex of existing keys for a single index. Assumes the
// spec is in a safe state to set a scanner on it (write lock or main thread).
void IndexSpec_ScanAndReindex(RedisModuleCtx *ctx, StrongRef spec_ref);

// Schedule a background scan+reindex across all indexes (single global scanner).
void Indexes_ScanAndReindex(void);

// FT.INFO helpers: whether a scan (global or this spec's) is in progress, and the
// fraction (0..1) of the keyspace already scanned (1.0 when none is in progress).
bool Indexes_IsScanInProgress(const IndexSpec *sp);
double Indexes_ScanIndexedPercent(RedisModuleCtx *ctx, const IndexSpec *sp);

#ifdef __cplusplus
}
#endif

#endif  // SPEC_SCAN_H__
