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
#include "tag_index.h"
#include "triemap_ffi.h"
#include "numeric_range_tree_ffi.h"
#include "iterators_ffi.h"
#include "util/arr.h"
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

// Reopen the live inverted index for `key` under whatever spec lock the caller
// (gcOneInvertedIndex) currently holds; return NULL if it no longer exists.
typedef InvertedIndex *(*GcReopenFn)(RedisSearchCtx *sctx, void *key);
// Remove the container entry for `key` when its index became empty after apply.
// Called under the spec write lock with `idx` still valid (sample it before any
// destructive delete). NULL when the container has no per-index entry to remove
// (e.g. the single existing-docs index).
typedef void (*GcOnEmptyFn)(RedisSearchCtx *sctx, void *key, InvertedIndex *idx,
                            II_GCScanStats *info);

// Scan + apply GC for one inverted index. The snapshot is taken under a brief read
// lock, scanned lock-free, then the delta is applied under the write lock (see the
// file header for the locking rationale). `reopen` re-resolves the live index for
// `key` (the pointer may change between scan and apply); `on_empty` removes the
// container entry if the index emptied. Returns bytes freed.
static size_t gcOneInvertedIndex(InProcGC *gc, RedisSearchCtx *sctx, void *key, GcReopenFn reopen,
                                 GcOnEmptyFn on_empty, size_t *entriesRemoved) {
  RedisSearchCtx_LockSpecRead(sctx);
  InvertedIndex *idx = reopen(sctx, key);
  struct OwnedGcSnapshot *snap = idx ? InvertedIndex_TakeGcSnapshot(idx) : NULL;
  RedisSearchCtx_UnlockSpec(sctx);

  if (!snap) {
    return 0;  // index gone, or nothing indexed
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

  RedisSearchCtx_LockSpecWrite(sctx);
  idx = reopen(sctx, key);
  if (!idx) {
    // Index vanished between scan and apply. Drop the delta without applying.
    InvertedIndex_GcDelta_Free(delta);
    RedisSearchCtx_UnlockSpec(sctx);
    return 0;
  }

  II_GCScanStats info = {0};
  InvertedIndex_ApplyGCDelta(idx, delta, &info);  // takes ownership of `delta`
  IndexStats_BlockCountAdd(&sctx->spec->stats, info.block_count_delta);

  if (on_empty && InvertedIndex_NumDocs(idx) == 0) {
    on_empty(sctx, key, idx, &info);
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

// ---- Terms (spec->terms trie + keysDict) -----------------------------------
static InvertedIndex *reopenTerm(RedisSearchCtx *sctx, void *key) {
  const CollectedTerm *ct = key;
  return Redis_OpenInvertedIndex(sctx->spec, ct->term, ct->len, DONT_CREATE_INDEX, NULL);
}

static void onEmptyTerm(RedisSearchCtx *sctx, void *key, InvertedIndex *idx, II_GCScanStats *info) {
  deleteEmptyTerm(sctx, idx, (const CollectedTerm *)key, info);
}

// ---- Missing-field docs (spec->missingFieldDict: HiddenString* -> II) ------
// Mirrors FGC_parentHandleMissingDocs. `key` is a HiddenString* dict key.
static InvertedIndex *reopenMissing(RedisSearchCtx *sctx, void *key) {
  if (!sctx->spec->missingFieldDict) {
    return NULL;
  }
  return dictFetchValue(sctx->spec->missingFieldDict, key);
}

static void onEmptyMissing(RedisSearchCtx *sctx, void *key, InvertedIndex *idx,
                           II_GCScanStats *info) {
  // Sample memory/blocks before the dict destructor (InvIndFreeCb) frees the index.
  info->bytes_freed += InvertedIndex_MemUsage(idx);
  IndexStats_BlockCountAdd(&sctx->spec->stats, -(int64_t)InvertedIndex_NumBlocks(idx));
  dictDelete(sctx->spec->missingFieldDict, key);
}

// ---- Existing docs (spec->existingDocs: single II for the wildcard/INDEXALL) --
static InvertedIndex *reopenExisting(RedisSearchCtx *sctx, void *key) {
  (void)key;
  return sctx->spec->existingDocs;
}

// ---- Tag indexes (per TAG field: a TrieMap of tag value -> II) --------------
// A collected (field, tag-value) pair. Copied out under a brief read lock so no
// live TrieMap iterator is held across the per-index lock cycles.
typedef struct {
  char *field;
  size_t fieldLen;
  char *tagVal;
  size_t tagLen;
} TagKey;

typedef struct {
  TagKey *items;
  size_t count;
  size_t cap;
} TagKeyList;

static void TagKeyList_Push(TagKeyList *tl, const char *field, size_t fieldLen, const char *tagVal,
                            size_t tagLen) {
  if (tl->count == tl->cap) {
    tl->cap = tl->cap ? tl->cap * 2 : 16;
    tl->items = rm_realloc(tl->items, tl->cap * sizeof(*tl->items));
  }
  TagKey *k = &tl->items[tl->count++];
  k->field = rm_malloc(fieldLen + 1);
  memcpy(k->field, field, fieldLen);
  k->field[fieldLen] = '\0';
  k->fieldLen = fieldLen;
  k->tagVal = rm_malloc(tagLen + 1);
  memcpy(k->tagVal, tagVal, tagLen);
  k->tagVal[tagLen] = '\0';
  k->tagLen = tagLen;
}

static void TagKeyList_Free(TagKeyList *tl) {
  for (size_t i = 0; i < tl->count; ++i) {
    rm_free(tl->items[i].field);
    rm_free(tl->items[i].tagVal);
  }
  rm_free(tl->items);
}

// Re-resolve the tag field -> TagIndex from the field name each time, rather than
// caching the TagIndex* across lock cycles.
static TagIndex *tagIndexForKey(RedisSearchCtx *sctx, const TagKey *k) {
  const FieldSpec *fs = IndexSpec_GetFieldWithLength(sctx->spec, k->field, k->fieldLen);
  if (!fs) {
    return NULL;
  }
  return TagIndex_Open(fs);
}

static InvertedIndex *reopenTag(RedisSearchCtx *sctx, void *key) {
  const TagKey *k = key;
  TagIndex *tagIdx = tagIndexForKey(sctx, k);
  if (!tagIdx) {
    return NULL;
  }
  size_t sz = 0;
  InvertedIndex *idx = TagIndex_OpenIndex(tagIdx, k->tagVal, k->tagLen, DONT_CREATE_INDEX, &sz);
  return idx == TRIEMAP_NOTFOUND ? NULL : idx;
}

static void onEmptyTag(RedisSearchCtx *sctx, void *key, InvertedIndex *idx, II_GCScanStats *info) {
  const TagKey *k = key;
  // Sample memory/blocks before the TrieMap destructor frees the index.
  info->bytes_freed += InvertedIndex_MemUsage(idx);
  IndexStats_BlockCountAdd(&sctx->spec->stats, -(int64_t)InvertedIndex_NumBlocks(idx));
  TagIndex *tagIdx = tagIndexForKey(sctx, k);
  if (!tagIdx) {
    return;
  }
  TrieMap_Delete(tagIdx->values, k->tagVal, k->tagLen, (void (*)(void *))InvertedIndex_Free);
  // Empty values (INDEXEMPTY) are never inserted into the suffix triemap; skip.
  if (tagIdx->suffix && k->tagLen) {
    deleteSuffixTrieMap(tagIdx->suffix, k->tagVal, k->tagLen);
  }
}

// Walk every TAG field's value TrieMap under a brief read lock and copy out the
// (field, tag-value) pairs.
static void collectTagKeys(RedisSearchCtx *sctx, TagKeyList *out) {
  RedisSearchCtx_LockSpecRead(sctx);
  arrayof(FieldSpec *) tagFields = getFieldsByType(sctx->spec, INDEXFLD_T_TAG);
  for (int i = 0; i < array_len(tagFields); ++i) {
    const FieldSpec *fs = tagFields[i];
    size_t fieldLen = 0;
    const char *fieldName = HiddenString_GetUnsafe(fs->fieldName, &fieldLen);
    TagIndex *tagIdx = TagIndex_Open(fs);
    if (!tagIdx) {
      continue;
    }
    TrieMapIterator *iter = TrieMap_Iterate(tagIdx->values);
    char *ptr = NULL;
    tm_len_t len = 0;
    void *value = NULL;
    while (TrieMapIterator_Next(iter, &ptr, &len, &value)) {
      TagKeyList_Push(out, fieldName, fieldLen, ptr, len);
    }
    TrieMapIterator_Free(iter);
  }
  array_free(tagFields);
  RedisSearchCtx_UnlockSpec(sctx);
}

// ---- Numeric / geo range trees ---------------------------------------------
// Numeric GC does not fit the shared per-II helper: it walks a range tree via a
// streaming scanner, applies one node's delta at a time keyed by a slab
// position+generation (which detects nodes that changed since the scan), and
// finally trims empty leaves. The range tree is not Arc-snapshot-based, so the
// scan runs under the spec READ lock (not lock-free like terms/tags); the apply
// + compaction run under the spec WRITE lock. Mirrors FGC_child/parentNumeric.
typedef struct {
  uint32_t pos;
  uint32_t gen;
  uint8_t *data;
  size_t len;
} NumEntry;

typedef struct {
  NumEntry *items;
  size_t count;
  size_t cap;
} NumEntryList;

static void NumEntryList_Push(NumEntryList *nl, uint32_t pos, uint32_t gen, const uint8_t *data,
                              size_t len) {
  if (nl->count == nl->cap) {
    nl->cap = nl->cap ? nl->cap * 2 : 16;
    nl->items = rm_realloc(nl->items, nl->cap * sizeof(*nl->items));
  }
  NumEntry *e = &nl->items[nl->count++];
  e->pos = pos;
  e->gen = gen;
  e->data = rm_malloc(len ? len : 1);
  memcpy(e->data, data, len);
  e->len = len;
}

static void NumEntryList_Free(NumEntryList *nl) {
  for (size_t i = 0; i < nl->count; ++i) {
    rm_free(nl->items[i].data);
  }
  rm_free(nl->items);
}

static void gcOneNumericField(InProcGC *gc, RedisSearchCtx *sctx, const char *fieldName,
                              size_t fieldLen, size_t *entriesRemoved) {
  // Collect one delta per node under the read lock (the scanner reads the live
  // tree; copy the serialized data since the scanner reuses its buffer).
  NumEntryList entries = {0};
  uint64_t uniqueId = 0;
  bool found = false;
  RedisSearchCtx_LockSpecRead(sctx);
  const FieldSpec *fs = IndexSpec_GetFieldWithLength(sctx->spec, fieldName, fieldLen);
  NumericRangeTree *rt =
      fs ? openNumericOrGeoIndex(sctx->spec, (FieldSpec *)fs, DONT_CREATE_INDEX) : NULL;
  if (rt) {
    found = true;
    uniqueId = NumericRangeTree_GetUniqueId(rt);
    NumericGcScanner *scanner = NumericGcScanner_New(sctx, rt);
    NumericGcNodeEntry e;
    while (NumericGcScanner_Next(scanner, &e)) {
      NumEntryList_Push(&entries, e.node_position, e.node_generation, e.data, e.data_len);
    }
    NumericGcScanner_Free(scanner);
  }
  RedisSearchCtx_UnlockSpec(sctx);

  bool cleanEmpty = RSGlobalConfig.gcConfigParams.gcSettings.forkGCCleanNumericEmptyNodes;
  if (!found || (entries.count == 0 && !cleanEmpty)) {
    NumEntryList_Free(&entries);
    return;
  }

  // Apply the collected deltas + optional compaction under one write-lock section.
  // Node staleness (a node that split/changed since the scan) is detected by
  // NumericRangeTree_ApplyGcEntry via the slab position+generation.
  RedisSearchCtx_LockSpecWrite(sctx);
  fs = IndexSpec_GetFieldWithLength(sctx->spec, fieldName, fieldLen);
  rt = fs ? openNumericOrGeoIndex(sctx->spec, (FieldSpec *)fs, DONT_CREATE_INDEX) : NULL;
  if (rt && NumericRangeTree_GetUniqueId(rt) == uniqueId) {
    for (size_t i = 0; i < entries.count; ++i) {
      ApplyGcEntryResult r =
          NumericRangeTree_ApplyGcEntry(rt, entries.items[i].pos, entries.items[i].gen,
                                        entries.items[i].data, entries.items[i].len);
      if (r.status != Ok) {
        continue;  // NodeNotFound (stale node) or DeserializationError: skip
      }
      II_GCScanStats gi = r.gc_result.index_gc_info;
      sctx->spec->stats.numRecords -= gi.entries_removed;
      sctx->spec->stats.invertedSize += gi.bytes_allocated;
      sctx->spec->stats.invertedSize -= gi.bytes_freed;
      IndexStats_BlockCountAdd(&sctx->spec->stats, gi.block_count_delta);
      atomic_fetch_add(&gc->totalCollectedBytes,
                       (ssize_t)gi.bytes_freed - (ssize_t)gi.bytes_allocated);
      *entriesRemoved += gi.entries_removed;
    }
    if (cleanEmpty) {
      CompactIfSparseResult cr = NumericRangeTree_CompactIfSparse(rt);
      if (cr.inverted_index_size_delta < 0) {
        size_t freed = (size_t)(-cr.inverted_index_size_delta);
        sctx->spec->stats.invertedSize -= freed;
        atomic_fetch_add(&gc->totalCollectedBytes, (ssize_t)freed);
      }
      IndexStats_BlockCountAdd(&sctx->spec->stats, cr.block_count_delta);
    }
  }
  RedisSearchCtx_UnlockSpec(sctx);
  NumEntryList_Free(&entries);
}

// Collect the numeric/geo field names under a brief read lock, then GC each.
static void gcNumericFields(InProcGC *gc, RedisSearchCtx *sctx, size_t *entriesRemoved) {
  TermList fields = {0};
  RedisSearchCtx_LockSpecRead(sctx);
  arrayof(FieldSpec *) nf = getFieldsByType(sctx->spec, INDEXFLD_T_NUMERIC | INDEXFLD_T_GEO);
  for (int i = 0; i < array_len(nf); ++i) {
    size_t l = 0;
    const char *n = HiddenString_GetUnsafe(nf[i]->fieldName, &l);
    char *c = rm_malloc(l + 1);
    memcpy(c, n, l);
    c[l] = '\0';
    TermList_Push(&fields, c, l);
  }
  array_free(nf);
  RedisSearchCtx_UnlockSpec(sctx);

  for (size_t i = 0; i < fields.count; ++i) {
    gcOneNumericField(gc, sctx, fields.items[i].term, fields.items[i].len, entriesRemoved);
  }
  TermList_Free(&fields);
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

  // Terms (text inverted indexes).
  TermList terms = {0};
  collectTermNames(&sctx, &terms);
  for (size_t i = 0; i < terms.count; ++i) {
    gcOneInvertedIndex(gc, &sctx, &terms.items[i], reopenTerm, onEmptyTerm, &entriesRemoved);
  }
  TermList_Free(&terms);

  // Missing-field indexes (spec->missingFieldDict: HiddenString* -> II). Collect
  // the dict keys under a brief read lock so no live dict iterator is held across
  // the per-index lock cycles. Only GC deletes these entries (and it is
  // single-threaded), so the collected key pointers stay valid while we process.
  {
    void **keys = NULL;
    size_t nkeys = 0, cap = 0;
    RedisSearchCtx_LockSpecRead(&sctx);
    if (sctx.spec->missingFieldDict) {
      dictIterator *it = dictGetIterator(sctx.spec->missingFieldDict);
      dictEntry *e;
      while ((e = dictNext(it))) {
        if (nkeys == cap) {
          cap = cap ? cap * 2 : 8;
          keys = rm_realloc(keys, cap * sizeof(*keys));
        }
        keys[nkeys++] = dictGetKey(e);
      }
      dictReleaseIterator(it);
    }
    RedisSearchCtx_UnlockSpec(&sctx);
    for (size_t i = 0; i < nkeys; ++i) {
      gcOneInvertedIndex(gc, &sctx, keys[i], reopenMissing, onEmptyMissing, &entriesRemoved);
    }
    rm_free(keys);
  }

  // Existing-docs index (single II; never removed on empty).
  gcOneInvertedIndex(gc, &sctx, NULL, reopenExisting, NULL, &entriesRemoved);

  // Tag indexes (per TAG field: a TrieMap of tag value -> II).
  TagKeyList tags = {0};
  collectTagKeys(&sctx, &tags);
  for (size_t i = 0; i < tags.count; ++i) {
    gcOneInvertedIndex(gc, &sctx, &tags.items[i], reopenTag, onEmptyTag, &entriesRemoved);
  }
  TagKeyList_Free(&tags);

  // Numeric / geo range trees (streaming scan + generational per-node apply).
  gcNumericFields(gc, &sctx, &entriesRemoved);

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
