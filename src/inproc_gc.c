/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

// In-process (fork-less) garbage collector. See inproc_gc.h for the rationale.
//
// Unlike the fork GC, there is no child process, no COW snapshot and no pipe:
// the cycle runs on the GC thread. Reader safety is provided by the
// Arc-refcounted inverted-index blocks (a query holds its own snapshot; blocks
// GC removes stay alive by refcount until the reader drops it), so no
// safe-memory-reclamation epoch is needed for the inverted index.
//
// Locking, per term:
//   * take an owned snapshot under a brief spec READ lock (clones the block Arcs
//     + deep-copies the tail) -> the read lock is held only for the O(blocks)
//     clone, not the scan;
//   * scan the owned snapshot with NO spec lock held -> writers make progress
//     during the (longer) scan. Doc-existence checks read the lock-free doc table,
//     bracketed by the doc-table reclamation epoch (DocTable_ReadBegin/End) so a
//     concurrent writer defers freeing a DMD we may walk;
//   * apply under the spec WRITE lock -> apply_gc rebuilds the sealed block list
//     copy-on-write; outstanding reader snapshots are unaffected.
// The delta is computed on the snapshot and applied to the (possibly newer)
// write-locked state; apply_gc reconciles drift, exactly as the fork GC's parent
// applies a delta computed on the older child snapshot.

#include "inproc_gc.h"
#include "gc.h"
#include "config.h"
#include "spec.h"
#include "search_ctx.h"
#include "redis_index.h"
#include "doc_table.h"
#include "module.h"
#include "redismodule.h"
#include "rmalloc.h"
#include "inverted_index.h"
#include "inverted_index_ffi.h"
#include "trie/trie.h"
#include "trie/trie_node.h"
#include "trie/rune_util.h"
#include "suffix.h"
#include "vector_index.h"
#include "info/global_stats.h"
#include "rmutil/rm_assert.h"
#include "obfuscation/obfuscation_api.h"
#include "obfuscation/hidden.h"

#include <stdatomic.h>
#include <time.h>
#include <string.h>

struct InProcGC {
  // Owner of the GC (weak; promoted per cycle).
  WeakRef index;
  RedisModuleCtx *ctx;

  // Number of docs deleted/updated since the last run; gates whether a cycle runs.
  _Atomic size_t deletedOrUpdatedDocsFromLastRun;

  // Cumulative stats (read by INFO/FT.INFO off-thread, hence atomic).
  _Atomic ssize_t totalCollectedBytes;
  _Atomic size_t totalCycles;
  _Atomic size_t totalTimeMs;
  _Atomic long long lastRunTimeMs;

  size_t intervalSec;
};

// One term name captured under a brief read lock before per-term processing, so
// we do not hold a live TrieIterator across lock release/reacquire.
typedef struct {
  char *term;
  size_t len;
} CollectedTerm;

// A simple owned, growable list of collected term names.
typedef struct {
  CollectedTerm *items;
  size_t count;
  size_t cap;
} TermList;

static void TermList_Push(TermList *tl, char *term, size_t len) {
  if (tl->count == tl->cap) {
    tl->cap = tl->cap ? tl->cap * 2 : 16;
    tl->items = rm_realloc(tl->items, tl->cap * sizeof(*tl->items));
  }
  tl->items[tl->count].term = term;
  tl->items[tl->count].len = len;
  tl->count++;
}

static void TermList_Free(TermList *tl) {
  for (size_t i = 0; i < tl->count; ++i) {
    rm_free(tl->items[i].term);
  }
  rm_free(tl->items);
}

static long long timespecDeltaMs(const struct timespec *a, const struct timespec *b) {
  return (b->tv_sec - a->tv_sec) * 1000LL + (b->tv_nsec - a->tv_nsec) / 1000000LL;
}

// Walk the terms trie under the read lock and copy out every term name. The walk
// only touches trie nodes (no posting decode), so the read lock is held briefly.
static void collectTermNames(RedisSearchCtx *sctx, TermList *out) {
  RedisSearchCtx_LockSpecRead(sctx);
  TrieIterator *iter = Trie_IterateAll(sctx->spec->terms);
  rune *rstr = NULL;
  t_len slen = 0;
  float score = 0;
  while (TrieIterator_Next(iter, &rstr, &slen, NULL, &score, NULL, NULL)) {
    size_t termLen;
    char *term = runesToStr(rstr, slen, &termLen);
    TermList_Push(out, term, termLen);
  }
  TrieIterator_Free(iter);
  RedisSearchCtx_UnlockSpec(sctx);
}

// Remove a term whose inverted index became empty after GC. Mirrors the
// empty-index branch of FGC_parentHandleTerms. Must run under the spec write
// lock; `idx` is sampled before dictDelete, which frees it.
static void deleteEmptyTerm(RedisSearchCtx *sctx, InvertedIndex *idx, const CollectedTerm *ct,
                            II_GCScanStats *info) {
  if (sctx->spec->keysDict) {
    CharBuf termKey = {.buf = ct->term, .len = ct->len};
    size_t inv_idx_size = InvertedIndex_MemUsage(idx);
    size_t remaining_blocks = InvertedIndex_NumBlocks(idx);
    if (dictDelete(sctx->spec->keysDict, &termKey) == DICT_OK) {
      info->bytes_freed += inv_idx_size;
      IndexStats_BlockCountAdd(&sctx->spec->stats, -(int64_t)remaining_blocks);
    }
  }

  if (!Trie_Delete(sctx->spec->terms, ct->term, ct->len)) {
    const char *name = IndexSpec_FormatName(sctx->spec, RSGlobalConfig.hideUserDataFromLog);
    const char *term_str = RSGlobalConfig.hideUserDataFromLog ? Obfuscate_Text(ct->term) : ct->term;
    int term_display_len =
        RSGlobalConfig.hideUserDataFromLog ? (int)strlen(term_str) : (int)ct->len;
    RedisModule_Log(
        sctx->redisCtx, "warning",
        "RedisSearch in-process GC: deleting a term '%.*s' from trie in index '%s' failed",
        term_display_len, term_str, name);
  }
  sctx->spec->stats.scoring.numTerms--;
  sctx->spec->stats.termsSize -= ct->len;
  // Empty terms (INDEXEMPTY) are never inserted into the suffix trie, so skip the delete.
  if (sctx->spec->suffix && ct->len) {
    deleteSuffixTrie(sctx->spec->suffix, ct->term, ct->len);
  }
}

// Scan + apply GC for a single term. Returns bytes freed by this term.
static size_t gcOneTerm(InProcGC *gc, RedisSearchCtx *sctx, const CollectedTerm *ct,
                        size_t *entriesRemoved) {
  // --- Take an owned snapshot under a brief read lock, then scan it lock-free ---
  RedisSearchCtx_LockSpecRead(sctx);
  InvertedIndex *idx =
      Redis_OpenInvertedIndex(sctx->spec, ct->term, ct->len, DONT_CREATE_INDEX, NULL);
  struct OwnedGcSnapshot *snap = idx ? InvertedIndex_TakeGcSnapshot(idx) : NULL;
  RedisSearchCtx_UnlockSpec(sctx);

  if (!snap) {
    return 0;  // term gone, or nothing indexed
  }

  // Scan the owned snapshot with NO spec lock held: the snapshot owns its II blocks
  // (Arc/Vec clones), so concurrent writers cannot affect it. `doc_exist` reads the
  // lock-free doc table, so bracket the scan in the reclamation epoch so a concurrent
  // writer defers freeing a DMD we may walk.
  LFReadToken tok = DocTable_ReadBegin();
  InvertedIndexGcDelta *delta = InvertedIndex_GcSnapshot_ScanDirect(sctx, snap);
  DocTable_ReadEnd(tok);
  InvertedIndex_GcSnapshot_Free(snap);

  if (!delta) {
    return 0;  // nothing to collect
  }

  // --- Apply under the write lock ---
  RedisSearchCtx_LockSpecWrite(sctx);
  idx = Redis_OpenInvertedIndex(sctx->spec, ct->term, ct->len, DONT_CREATE_INDEX, NULL);
  if (!idx) {
    // Term vanished between scan and apply (should not happen: only GC deletes
    // terms and it is single-threaded). Drop the delta without applying.
    InvertedIndex_GcDelta_Free(delta);
    RedisSearchCtx_UnlockSpec(sctx);
    return 0;
  }

  II_GCScanStats info = {0};
  InvertedIndex_ApplyGCDelta(idx, delta, &info);  // takes ownership of `delta`
  IndexStats_BlockCountAdd(&sctx->spec->stats, info.block_count_delta);

  if (InvertedIndex_NumDocs(idx) == 0) {
    deleteEmptyTerm(sctx, idx, ct, &info);
    idx = NULL;  // freed by dictDelete above
  }

  // Update per-spec index stats (mirrors FGC_updateStats).
  sctx->spec->stats.numRecords -= info.entries_removed;
  sctx->spec->stats.invertedSize += info.bytes_allocated;
  sctx->spec->stats.invertedSize -= info.bytes_freed;
  RedisSearchCtx_UnlockSpec(sctx);

  atomic_fetch_add(&gc->totalCollectedBytes,
                   (ssize_t)info.bytes_freed - (ssize_t)info.bytes_allocated);
  *entriesRemoved += info.entries_removed;
  return info.bytes_freed;
}

static bool periodicCb(void *privdata, bool force) {
  InProcGC *gc = privdata;

  StrongRef spec_ref = IndexSpecRef_Promote(gc->index);
  IndexSpec *sp = StrongRef_Get(spec_ref);
  if (!sp) {
    // Index was deleted: stop rescheduling.
    return false;
  }

  size_t num_docs = atomic_load(&gc->deletedOrUpdatedDocsFromLastRun);
  if (!force && num_docs < RSGlobalConfig.gcConfigParams.gcSettings.forkGcCleanThreshold) {
    IndexSpecRef_Release(spec_ref);
    return true;
  }
  atomic_fetch_sub(&gc->deletedOrUpdatedDocsFromLastRun, num_docs);

  struct timespec t0;
  clock_gettime(CLOCK_MONOTONIC_RAW, &t0);

  RedisSearchCtx sctx = SEARCH_CTX_STATIC(gc->ctx, sp);
  size_t entriesRemoved = 0;

  TermList terms = {0};
  collectTermNames(&sctx, &terms);
  for (size_t i = 0; i < terms.count; ++i) {
    gcOneTerm(gc, &sctx, &terms.items[i], &entriesRemoved);
  }
  TermList_Free(&terms);

  // Tiered vector indexes still run their own GC (as under the fork GC).
  VecSim_CallTieredIndexesGC(gc->index);

  struct timespec t1;
  clock_gettime(CLOCK_MONOTONIC_RAW, &t1);
  long long ms = timespecDeltaMs(&t0, &t1);
  atomic_fetch_add(&gc->totalCycles, (size_t)1);
  atomic_fetch_add(&gc->totalTimeMs, (size_t)ms);
  atomic_store(&gc->lastRunTimeMs, ms);

  IndexsGlobalStats_DecreaseLogicallyDeleted(num_docs);
  gc->intervalSec = RSGlobalConfig.gcConfigParams.gcSettings.forkGcRunIntervalSec;

  IndexSpecRef_Release(spec_ref);
  return true;
}

static void onTerminateCb(void *privdata) {
  InProcGC *gc = privdata;
  size_t leftover = atomic_exchange(&gc->deletedOrUpdatedDocsFromLastRun, 0);
  IndexsGlobalStats_DecreaseLogicallyDeleted(leftover);
  WeakRef_Release(gc->index);
  RedisModule_FreeThreadSafeContext(gc->ctx);
  rm_free(gc);
}

static void statsCb(RedisModule_Reply *reply, void *gcCtx) {
#define REPLY_KVNUM(k, v) RedisModule_ReplyKV_Double(reply, (k), (v))
  InProcGC *gc = gcCtx;
  if (!gc) return;
  ssize_t bytes = atomic_load(&gc->totalCollectedBytes);
  size_t cycles = atomic_load(&gc->totalCycles);
  size_t total_ms = atomic_load(&gc->totalTimeMs);
  REPLY_KVNUM("bytes_collected", (double)bytes);
  REPLY_KVNUM("total_ms_run", (double)total_ms);
  REPLY_KVNUM("total_cycles", (double)cycles);
  REPLY_KVNUM("average_cycle_time_ms", cycles ? (double)total_ms / (double)cycles : 0.0);
  REPLY_KVNUM("last_run_time_ms", (double)atomic_load(&gc->lastRunTimeMs));
#undef REPLY_KVNUM
}

static void statsForInfoCb(RedisModuleInfoCtx *ctx, void *gcCtx) {
  InProcGC *gc = gcCtx;
  size_t cycles = atomic_load(&gc->totalCycles);
  size_t total_ms = atomic_load(&gc->totalTimeMs);
  RedisModule_InfoBeginDictField(ctx, "gc_stats");
  RedisModule_InfoAddFieldLongLong(ctx, "bytes_collected", atomic_load(&gc->totalCollectedBytes));
  RedisModule_InfoAddFieldLongLong(ctx, "total_ms_run", total_ms);
  RedisModule_InfoAddFieldLongLong(ctx, "total_cycles", cycles);
  RedisModule_InfoAddFieldDouble(ctx, "average_cycle_time_ms",
                                 cycles ? (double)total_ms / (double)cycles : 0.0);
  RedisModule_InfoAddFieldDouble(ctx, "last_run_time_ms", (double)atomic_load(&gc->lastRunTimeMs));
  RedisModule_InfoEndDictField(ctx);
}

static void deleteOrUpdateCb(void *ctx) {
  InProcGC *gc = ctx;
  atomic_fetch_add(&gc->deletedOrUpdatedDocsFromLastRun, 1);
  IndexsGlobalStats_IncreaseLogicallyDeleted(1);
}

static void getStatsCb(void *gcCtx, InfoGCStats *out) {
  InProcGC *gc = gcCtx;
  out->totalCollectedBytes = atomic_load(&gc->totalCollectedBytes);
  out->totalCycles = atomic_load(&gc->totalCycles);
  out->totalTime = atomic_load(&gc->totalTimeMs);
  out->lastRunTimeMs = (long long)atomic_load(&gc->lastRunTimeMs);
}

static struct timespec getIntervalCb(void *ctx) {
  InProcGC *gc = ctx;
  return (struct timespec){.tv_sec = gc->intervalSec, .tv_nsec = 0};
}

InProcGC *InProcGC_Create(StrongRef spec_ref, GCCallbacks *callbacks) {
  InProcGC *gc = rm_calloc(1, sizeof(*gc));
  gc->index = StrongRef_Demote(spec_ref);
  gc->ctx = RedisModule_GetDetachedThreadSafeContext(RSDummyContext);
  gc->intervalSec = RSGlobalConfig.gcConfigParams.gcSettings.forkGcRunIntervalSec;

  callbacks->onTerm = onTerminateCb;
  callbacks->periodicCallback = periodicCb;
  callbacks->renderStats = statsCb;
  callbacks->renderStatsForInfo = statsForInfoCb;
  callbacks->getInterval = getIntervalCb;
  callbacks->onDelete = deleteOrUpdateCb;
  callbacks->onWrite = NULL;  // writes are not tracked (as with the fork GC)
  callbacks->onUpdate = deleteOrUpdateCb;
  callbacks->getStats = getStatsCb;

  return gc;
}
