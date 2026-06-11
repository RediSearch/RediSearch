/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

// Generic background keyspace-scan engine. Walks the keyspace on a background thread
// and invokes caller-supplied callbacks per key, handling GIL management, the
// background-indexing memory limit (OOM), cancellation, yielding, and the FT.DEBUG
// pause/step scanner. It knows nothing about index specs — the spec-specific reindex
// logic lives in spec.c, which drives this engine through IndexScanCallbacks.

#ifndef INDEX_SCAN_H__
#define INDEX_SCAN_H__

#include <stdbool.h>
#include <stddef.h>

#include "redismodule.h"

#ifdef __cplusplus
extern "C" {
#endif

// Status codes reported by the debug scanner (FT.DEBUG), indexing into
// DEBUG_INDEX_SCANNER_STATUS_STRS.
typedef enum {
    DEBUG_INDEX_SCANNER_CODE_NEW,
    DEBUG_INDEX_SCANNER_CODE_RUNNING,
    DEBUG_INDEX_SCANNER_CODE_DONE,
    DEBUG_INDEX_SCANNER_CODE_CANCELLED,
    DEBUG_INDEX_SCANNER_CODE_PAUSED,
    DEBUG_INDEX_SCANNER_CODE_RESUMED,
    DEBUG_INDEX_SCANNER_CODE_PAUSED_ON_OOM,
    DEBUG_INDEX_SCANNER_CODE_PAUSED_BEFORE_OOM_RETRY,

    //Insert new codes here (before COUNT)
    DEBUG_INDEX_SCANNER_CODE_COUNT  // Helps with array size checks
    //Do not add new codes after COUNT
} DebugIndexScannerCode;

extern const char *DEBUG_INDEX_SCANNER_STATUS_STRS[];

// Outcome of handling one scanned key, returned by the process_key callback.
typedef enum {
  INDEX_SCAN_CONTINUE,  // key handled — count it towards progress
  INDEX_SCAN_SKIP,      // key not applicable (e.g. unsupported type) — do not count
  INDEX_SCAN_STOP,      // stop the whole scan (e.g. the target is gone)
} IndexScanResult;

struct IndexesScanner;  // defined below; referenced by the on_finished callback

// Callbacks the engine invokes. All run on the scan thread with the GIL held.
typedef struct IndexScanCallbacks {
  // Index one scanned key. `key` is best-effort (may be NULL); resolve it if needed.
  IndexScanResult (*process_key)(void *privdata, RedisModuleCtx *ctx,
                                 RedisModuleString *keyname, RedisModuleKey *key);
  // The memory limit was hit and the scan is stopping. Optional (NULL for scans with
  // no single target to attach the error to). `error` is engine-owned and transient.
  void (*on_oom)(void *privdata, RedisModuleCtx *ctx, const char *error,
                 RedisModuleString *oom_key);
  // Fires exactly once when the scan finishes (completed or cancelled), before the
  // scanner is freed, so the caller can drop its reference and free privdata.
  void (*on_finished)(void *privdata, struct IndexesScanner *scanner);
} IndexScanCallbacks;

typedef struct IndexesScanner {
  bool global;
  bool cancelled;
  bool isDebug;
  bool scanFailedOnOOM;
  char *name;                 // for logs (owned); NULL for global scans
  size_t scannedKeys;
  RedisModuleString *OOMkey;  // last key that tripped the memory limit
  IndexScanCallbacks cbs;
  void *privdata;
} IndexesScanner;

typedef struct DebugIndexesScanner {
  IndexesScanner base;
  int maxDocsTBscanned;
  int maxDocsTBscannedPause;
  bool wasPaused;
  bool pauseOnOOM;
  int status;
  bool pauseBeforeOOMRetry;
} DebugIndexesScanner;

// Start a background scan. Copies `name`, allocates a scanner (a debug scanner when
// FT.DEBUG mode is on and `global` is false), submits it to the reindex thread pool,
// and returns it. `global` selects scan-all semantics: no debug scanner and no
// per-target OOM record. The returned scanner is owned by the engine and freed when
// the scan finishes (on_finished fires first).
IndexesScanner *IndexScan_Start(const char *name, bool global,
                                const IndexScanCallbacks *cbs, void *privdata);

void IndexScan_Cancel(IndexesScanner *scanner);
bool IndexScan_IsCancelled(const IndexesScanner *scanner);
size_t IndexScan_ScannedKeys(const IndexesScanner *scanner);

// Tear down the reindex thread pool (shutdown / FT.DEBUG).
void ReindexPool_ThreadPoolDestroy(void);

#ifdef __cplusplus
}
#endif

#endif  // INDEX_SCAN_H__
