/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#include "aggregate/aggregate.h"
#include "result_processor.h"
#include "query.h"
#include "extension.h"
#include <util/minmax_heap.h>
#include "ext/default.h"
#include "rmutil/rm_assert.h"
#include "util/timeout.h"
#include "util/arr.h"
#include "iterators/empty_iterator.h"
#include "rs_wall_clock.h"
#include <stdatomic.h>
#include <pthread.h>
#include <unistd.h>
#include <time.h>
#include "util/references.h"
#include "hybrid/hybrid_scoring.h"
#include "hybrid/hybrid_search_result.h"
#include "config.h"
#include "module.h"
#include "search_disk.h"
#include "debug_commands.h"
#include "search_result.h"

/*******************************************************************************************************************
 *  Base Result Processor - this processor is the topmost processor of every processing chain.
 *
 * It takes the raw index results from the index, and builds the search result to be sent
 * downstream.
 *******************************************************************************************************************/

static int UnlockSpec_and_ReturnRPResult(RedisSearchCtx *sctx, int result_status) {
  RedisSearchCtx_UnlockSpec(sctx);
  return result_status;
}
typedef struct {
  ResultProcessor base;
  QueryIterator *iterator;
  RedisSearchCtx *sctx;
  uint32_t timeoutLimiter;                      // counter to limit number of calls to TimedOut_WithCounter()
  uint32_t slotsVersion;                        // version of the slot ranges used for filtering
  const RedisModuleSlotRangeArray *querySlots;  // Query slots info, may be used for filtering
  const SharedSlotRangeArray *slotRanges;       // Owned slot ranges info, may be used for filtering. TODO ASM: remove
} RPQueryIterator;


/****
 * getDocumentMetadata - get the document metadata for the current document from the iterator.
 * If the document is deleted or expired, return false.
 * If the document is not deleted or expired, return true.
 * If the document is not deleted or expired, and dmd is not NULL, set *dmd to the document metadata.
 * @param spec The index spec
 * @param docs The document table
 * @param sctx The search context
 * @param it The query iterator
 * @param dmd The document metadata pointer to set
 * @return true if the document is not deleted or expired, false otherwise.
 */
static bool getDocumentMetadata(IndexSpec* spec, DocTable* docs, RedisSearchCtx *sctx, const QueryIterator *it, const RSDocumentMetadata **dmd) {
  if (spec->diskSpec) {
    RSDocumentMetadata* diskDmd = (RSDocumentMetadata *)rm_calloc(1, sizeof(RSDocumentMetadata));
    diskDmd->ref_count = 1;
    // Start from checking the deleted-ids (in memory), then perform IO
    const bool foundDocument = !SearchDisk_DocIdDeleted(spec->diskSpec, it->current->docId) && SearchDisk_GetDocumentMetadata(spec->diskSpec, it->current->docId, diskDmd);
    if (!foundDocument) {
      DMD_Return(diskDmd);
      return false;
    }
    *dmd = diskDmd;
  } else {
    if (it->current->dmd) {
      *dmd = it->current->dmd;
    } else {
      *dmd = DocTable_Borrow(docs, it->lastDocId);
    }
    if (!*dmd || (*dmd)->flags & Document_Deleted || DocTable_IsDocExpired(docs, *dmd, &sctx->time.current)) {
      DMD_Return(*dmd);
      return false;;
    }
  }
  return true;
}

// TODO ASM: use this to decide if we need to filter by slots
extern atomic_uint key_space_version;
atomic_uint key_space_version = 0;

/* Next implementation */
static int rpQueryItNext(ResultProcessor *base, SearchResult *res) {
  RPQueryIterator *self = (RPQueryIterator *)base;
  QueryIterator *it = self->iterator;
  RedisSearchCtx *sctx = self->sctx;
  DocTable* docs = &self->sctx->spec->docs;
  const RSDocumentMetadata *dmd;
  if (sctx->flags == RS_CTX_UNSET) {
    // If we need to read the iterators and we didn't lock the spec yet, lock it now
    // and reopen the keys in the concurrent search context (iterators' validation)
    RedisSearchCtx_LockSpecRead(sctx);
    ValidateStatus rc = it->Revalidate(it);
    if (rc == VALIDATE_ABORTED) {
      // The iterator is no longer valid, we should not use it.
      self->iterator->Free(self->iterator);
      it = self->iterator = NewEmptyIterator(); // Replace with a new empty iterator
    } else if (rc == VALIDATE_MOVED && !it->atEOF) {
      // The iterator is still valid, but the current result has changed, or we are at EOF.
      // If we are at EOF, we can enter the loop and let it handle it. (reading again should be safe)
      goto validate_current;
    }
  }

  // Read from the root filter until we have a valid result
  while (1) {
    // check for timeout in case we are encountering a lot of deleted documents
    if (TimedOut_WithCounter(&sctx->time.timeout, &self->timeoutLimiter) == TIMED_OUT) {
      return UnlockSpec_and_ReturnRPResult(sctx, RS_RESULT_TIMEDOUT);
    }
    IteratorStatus rc = it->Read(it);
    switch (rc) {
    case ITERATOR_EOF:
      // This means we are done!
      return UnlockSpec_and_ReturnRPResult(sctx, RS_RESULT_EOF);
    case ITERATOR_TIMEOUT:
      return UnlockSpec_and_ReturnRPResult(sctx, RS_RESULT_TIMEDOUT);
    default:
      RS_ASSERT(rc == ITERATOR_OK);
    }

validate_current:
    IndexSpec* spec = self->sctx->spec;
    if (!getDocumentMetadata(spec, docs, sctx, it, &dmd)) {
      continue;
    }

    if (isTrimming && RedisModule_ShardingGetKeySlot) {
      RedisModuleString *key = RedisModule_CreateString(NULL, dmd->keyPtr, sdslen(dmd->keyPtr));
      int slot = RedisModule_ShardingGetKeySlot(key);
      RedisModule_FreeString(NULL, key);
      int firstSlot, lastSlot;
      RedisModule_ShardingGetSlotRange(&firstSlot, &lastSlot);
      if (firstSlot > slot || lastSlot < slot) {
        DMD_Return(dmd);
        continue;
      }
    }
    if (should_filter_slots) {
      RS_ASSERT(self->slotRanges != NULL);
      int slot = RedisModule_ClusterKeySlotC(dmd->keyPtr, sdslen(dmd->keyPtr));
      if (!Slots_CanAccessKeysInSlot(self->slotRanges, slot)) {
        DMD_Return(dmd);
        continue;
      }
    }

    // Increment the total results barring deleted results
    base->parent->totalResults++;
    break;
  }

  // set the result data
  SearchResult_SetDocId(res, it->lastDocId);
  SearchResult_SetIndexResult(res, it->current);
  SearchResult_SetScore(res, 0);
  SearchResult_SetDocumentMetadata(res, dmd);
  RLookupRow_SetSortingVector(SearchResult_GetRowDataMut(res), dmd->sortVector);
  return RS_RESULT_OK;
}

static void rpQueryItFree(ResultProcessor *iter) {
  RPQueryIterator *self = (RPQueryIterator *)iter;
  self->iterator->Free(self->iterator);
  rm_free((void *)self->querySlots);
  Slots_FreeLocalSlots(self->slotRanges);
  rm_free(iter);
}

ResultProcessor *RPQueryIterator_New(QueryIterator *root, const SharedSlotRangeArray *slotRanges, const RedisModuleSlotRangeArray *querySlots, uint32_t slotsVersion, RedisSearchCtx *sctx) {
  RS_ASSERT(root != NULL);
  RPQueryIterator *ret = rm_calloc(1, sizeof(*ret));
  ret->iterator = root;
  ret->slotRanges = slotRanges;
  ret->querySlots = querySlots;
  ret->slotsVersion = slotsVersion;
  ret->base.Next = rpQueryItNext;
  ret->base.Free = rpQueryItFree;
  ret->sctx = sctx;
  ret->base.type = RP_INDEX;
  return &ret->base;
}

QueryIterator *QITR_GetRootFilter(QueryProcessingCtx *it) {
  /* On coordinator, the root result processor will be a network result processor and we should ignore it */
  if (it->rootProc && it->rootProc->type == RP_INDEX) {
    return ((RPQueryIterator *)it->rootProc)->iterator;
  }
  return NULL;
}

void QITR_PushRP(QueryProcessingCtx *it, ResultProcessor *rp) {
  rp->parent = it;
  if (!it->rootProc) {
    it->endProc = it->rootProc = rp;
    rp->upstream = NULL;
    return;
  }
  rp->upstream = it->endProc;
  it->endProc = rp;
}

void QITR_FreeChain(QueryProcessingCtx *qitr) {
  ResultProcessor *rp = qitr->endProc;
  while (rp) {
    ResultProcessor *next = rp->upstream;
    rp->Free(rp);
    rp = next;
  }
}

/*******************************************************************************************************************
 *  Scoring Processor
 *
 * It takes results from upstream, and using a scoring function applies the score to each one.
 *
 * It may not be invoked if we are working in SORTBY mode (or later on in aggregations)
 *******************************************************************************************************************/

typedef struct {
  ResultProcessor base;
  RSScoringFunction scorer;
  RSFreeFunction scorerFree;
  ScoringFunctionArgs scorerCtx;
  const RLookupKey *scoreKey;
} RPScorer;

static int rpscoreNext(ResultProcessor *base, SearchResult *res) {
  int rc;
  RPScorer *self = (RPScorer *)base;

  do {
    rc = base->upstream->Next(base->upstream, res);
    if (rc != RS_RESULT_OK) {
      return rc;
    }

    // Apply the scoring function
    SearchResult_SetScore(res, self->scorer(&self->scorerCtx, SearchResult_GetIndexResult(res), SearchResult_GetDocumentMetadata(res), base->parent->minScore));
    if (self->scorerCtx.scrExp) {
      SearchResult_SetScoreExplain(res, (RSScoreExplain *)self->scorerCtx.scrExp);
      self->scorerCtx.scrExp = rm_calloc(1, sizeof(RSScoreExplain));
    }
    // If we got the special score RS_SCORE_FILTEROUT - disregard the result and decrease the total
    // number of results (it's been increased by the upstream processor)
    if (SearchResult_GetScore(res) == RS_SCORE_FILTEROUT) {
      base->parent->totalResults--;
      SearchResult_Clear(res);
      // continue and loop to the next result, since this is excluded by the
      // scorer.
      continue;
    }
    if (self->scoreKey) {
      RLookup_WriteOwnKey(self->scoreKey, SearchResult_GetRowDataMut(res), RSValue_NewNumber(SearchResult_GetScore(res)));
    }

    break;
  } while (1);

  return rc;
}

/* Free impl. for scorer - frees up the scorer privdata if needed */
static void rpscoreFree(ResultProcessor *rp) {
  RPScorer *self = (RPScorer *)rp;
  if (self->scorerFree) {
    self->scorerFree(self->scorerCtx.extdata);
  }
  rm_free(self->scorerCtx.scrExp);
  self->scorerCtx.scrExp = NULL;
  rm_free(self);
}

/* Create a new scorer by name. If the name is not found in the scorer registry, we use the default
 * scorer */
ResultProcessor *RPScorer_New(const ExtScoringFunctionCtx *funcs,
                              const ScoringFunctionArgs *fnargs,
                              const RLookupKey *rlk) {
  RPScorer *ret = rm_calloc(1, sizeof(*ret));
  ret->scorer = funcs->sf;
  ret->scorerFree = funcs->ff;
  ret->scorerCtx = *fnargs;
  ret->scoreKey = rlk;
  ret->base.Next = rpscoreNext;
  ret->base.Free = rpscoreFree;
  ret->base.type = RP_SCORER;
  return &ret->base;
}

/*******************************************************************************************************************
 *  Additional Values Loader Result Processor
 *
 * It takes results from upstream (should be Index iterator or close; before any RP that need these field),
 * and add their additional value to the right score field before sending them downstream.
 *******************************************************************************************************************/

typedef struct {
  ResultProcessor base;
} RPMetrics;

static int rpMetricsNext(ResultProcessor *base, SearchResult *res) {
  int rc;

  rc = base->upstream->Next(base->upstream, res);
  if (rc != RS_RESULT_OK) {
    return rc;
  }

  arrayof(RSYieldableMetric) arr = SearchResult_GetIndexResult(res)->metrics;
  for (size_t i = 0; i < array_len(arr); i++) {
    RLookup_WriteKey(arr[i].key, SearchResult_GetRowDataMut(res), arr[i].value);
  }

  return rc;
}

/* Free implementation for RPMetrics */
static void rpMetricsFree(ResultProcessor *rp) {
  RPMetrics *self = (RPMetrics *)rp;
  rm_free(self);
}

ResultProcessor *RPMetricsLoader_New() {
  RPMetrics *ret = rm_calloc(1, sizeof(*ret));
  ret->base.Next = rpMetricsNext;
  ret->base.Free = rpMetricsFree;
  ret->base.type = RP_METRICS;
  return &ret->base;
}

/*******************************************************************************************************************
 *  Sorting Processor
 *
 * This is where things become a bit complex...
 *
 * The sorter takes scored results from the scorer (or in the case of SORTBY, the raw results), and
 * maintains a heap of the top N results.
 *
 * Since we need it to be thread safe, every result that's put on the heap is copied, including its
 * index result tree.
 *
 * This means that from here down-stream, everything is thread safe, but we also need to properly
 * free discarded results.
 *
 * The sorter is actually a reducer - it returns RS_RESULT_QUEUED until its upstream parent returns
 * EOF. then it starts yielding results one by one by popping from the top of the heap.
 *
 * Note: We use a min-max heap to simplify maintaining a max heap where we can pop from the bottom
 * while finding the top N results
 *******************************************************************************************************************/

typedef int (*RPSorterCompareFunc)(const void *e1, const void *e2, const void *udata);

typedef struct {
  ResultProcessor base;

  // The heap. We use a min-max heap here
  mm_heap_t *pq;

  // the compare function for the heap. We use it to test if a result needs to be added to the heap
  RPSorterCompareFunc cmp;

  // private data for the compare function
  void *cmpCtx;

  // pooled result - we recycle it to avoid allocations
  SearchResult *pooledResult;

  struct {
    const RLookupKey **keys;
    size_t nkeys;
    uint64_t ascendMap;
  } fieldcmp;

  // Whether a timeout warning needs to be propagated down the downstream
  bool timedOut;
} RPSorter;

/* Yield - pops the current top result from the heap */
static int rpsortNext_Yield(ResultProcessor *rp, SearchResult *r) {
  RPSorter *self = (RPSorter *)rp;
  SearchResult *cur_best = mmh_pop_max(self->pq);

  if (cur_best) {
    SearchResult_Override(r, cur_best);
    rm_free(cur_best);
    return RS_RESULT_OK;
  }
  int ret = self->timedOut ? RS_RESULT_TIMEDOUT : RS_RESULT_EOF;
  self->timedOut = false;
  return ret;
}

static void rpsortFree(ResultProcessor *rp) {
  RPSorter *self = (RPSorter *)rp;

  SearchResult_Destroy(self->pooledResult);
  rm_free(self->pooledResult);

  // calling mmh_free will free all the remaining results in the heap, if any
  mmh_free(self->pq);
  rm_free(rp);
}

#define RESULT_QUEUED RS_RESULT_MAX + 1

static int rpsortNext_innerLoop(ResultProcessor *rp, SearchResult *r) {
  RPSorter *self = (RPSorter *)rp;

  // get the next result from upstream. `self->pooledResult` is expected to be empty and allocated.
  int rc = rp->upstream->Next(rp->upstream, self->pooledResult);

  // if our upstream has finished - just change the state to not accumulating, and yield
  if (rc == RS_RESULT_EOF) {
    rp->Next = rpsortNext_Yield;
    return rpsortNext_Yield(rp, r);
  } else if (rc == RS_RESULT_TIMEDOUT && (rp->parent->timeoutPolicy == TimeoutPolicy_Return)) {
    self->timedOut = true;
    rp->Next = rpsortNext_Yield;
    return rpsortNext_Yield(rp, r);
  } else if (rc != RS_RESULT_OK) {
    // whoops!
    return rc;
  }

  // If the queue is not full - we just push the result into it
  if (self->pq->count < self->pq->size) {

    // copy the index result to make it thread safe - but only if it is pushed to the heap
    SearchResult_SetIndexResult(self->pooledResult, NULL);
    mmh_insert(self->pq, self->pooledResult);
    if (SearchResult_GetScore(self->pooledResult) < rp->parent->minScore) {
      rp->parent->minScore = SearchResult_GetScore(self->pooledResult);
    }
    // we need to allocate a new result for the next iteration
    self->pooledResult = rm_calloc(1, sizeof(*self->pooledResult));
  } else {
    // find the min result
    SearchResult *minh = mmh_peek_min(self->pq);

    // update the min score. Irrelevant to SORTBY mode but hardly costs anything...
    if (SearchResult_GetScore(minh) > rp->parent->minScore) {
      rp->parent->minScore = SearchResult_GetScore(minh);
    }

    // if needed - pop it and insert a new result
    if (self->cmp(self->pooledResult, minh, self->cmpCtx) > 0) {
      SearchResult_SetIndexResult(self->pooledResult, NULL);
      self->pooledResult = mmh_exchange_min(self->pq, self->pooledResult);
    }
    // clear the result in preparation for the next iteration
    SearchResult_Clear(self->pooledResult);
  }
  return RESULT_QUEUED;
}

static int rpsortNext_Accum(ResultProcessor *rp, SearchResult *r) {
  uint32_t chunkLimit = rp->parent->resultLimit;
  rp->parent->resultLimit = UINT32_MAX; // we want to accumulate all results
  int rc;
  while ((rc = rpsortNext_innerLoop(rp, r)) == RESULT_QUEUED) {
    // Do nothing.
  }
  rp->parent->resultLimit = chunkLimit; // restore the limit
  return rc;
}

/* Compare results for the heap by score */
static inline int cmpByScore(const void *e1, const void *e2, const void *udata) {
  const SearchResult *h1 = e1, *h2 = e2;

  if (SearchResult_GetScore(h1) < SearchResult_GetScore(h2)) {
    return -1;
  } else if (SearchResult_GetScore(h1) > SearchResult_GetScore(h2)) {
    return 1;
  }
  return SearchResult_GetDocId(h1) > SearchResult_GetDocId(h2) ? -1 : 1;
}

/* Compare results for the heap by sorting key */
static int cmpByFields(const void *e1, const void *e2, const void *udata) {
  const RPSorter *self = udata;
  const SearchResult *h1 = e1, *h2 = e2;
  int ascending = 0;

  QueryError *qerr = NULL;
  if (self && self->base.parent && self->base.parent->err) {
    qerr = self->base.parent->err;
  }

  for (size_t i = 0; i < self->fieldcmp.nkeys && i < SORTASCMAP_MAXFIELDS; i++) {
    const RSValue *v1 = RLookup_GetItem(self->fieldcmp.keys[i], SearchResult_GetRowData(h1));
    const RSValue *v2 = RLookup_GetItem(self->fieldcmp.keys[i], SearchResult_GetRowData(h2));
    // take the ascending bit for this property from the ascending bitmap
    ascending = SORTASCMAP_GETASC(self->fieldcmp.ascendMap, i);
    if (!v1 || !v2) {
      // If at least one of these has no sort key, it gets high value regardless of asc/desc
      if (v1) {
        return 1;
      } else if (v2) {
        return -1;
      } else {
        // Both have no sort key, so they are equal. Continue to next sort key
        continue;
      }
    }

    int rc = RSValue_Cmp(v1, v2, qerr);
    if (rc != 0) return ascending ? -rc : rc;
  }

  int rc = SearchResult_GetDocId(h1) < SearchResult_GetDocId(h2) ? -1 : 1;
  return ascending ? -rc : rc;
}

static void srDtor(void *p) {
  if (p) {
    SearchResult_Destroy(p);
    rm_free(p);
  }
}

ResultProcessor *RPSorter_NewByFields(size_t maxresults, const RLookupKey **keys, size_t nkeys, uint64_t ascmap) {

  RPSorter *ret = rm_calloc(1, sizeof(*ret));
  ret->cmp = nkeys ? cmpByFields : cmpByScore;
  ret->cmpCtx = ret;
  ret->fieldcmp.ascendMap = ascmap;
  ret->fieldcmp.keys = keys;
  ret->fieldcmp.nkeys = nkeys;

  ret->pq = mmh_init_with_size(maxresults, ret->cmp, ret->cmpCtx, srDtor);
  ret->pooledResult = rm_calloc(1, sizeof(*ret->pooledResult));
  ret->base.Next = rpsortNext_Accum;
  ret->base.Free = rpsortFree;
  ret->base.type = RP_SORTER;
  return &ret->base;
}

ResultProcessor *RPSorter_NewByScore(size_t maxresults) {
  return RPSorter_NewByFields(maxresults, NULL, 0, 0);
}

/*******************************************************************************************************************
 *  Paging Processor
 *
 * The sorter builds a heap of size N, but the pager is responsible for taking result
 * FIRST...FIRST+NUM from it.
 *
 * For example, if we want to get results 40-50, we build a heap of size 50 on the sorter, and
 *the pager is responsible for discarding the first 40 results and returning just 10
 *
 * They are separated so that later on we can cache the sorter's heap, and continue paging it
 * without re-executing the entire query
 *******************************************************************************************************************/

typedef struct {
  ResultProcessor base;
  uint32_t offset;
  uint32_t remaining;
} RPPager;

static int rppagerNext_Limit(ResultProcessor *base, SearchResult *r) {
  RPPager *self = (RPPager *)base;

  // If we've reached LIMIT:
  if (!self->remaining) {
    return RS_RESULT_EOF;
  }

  int ret = base->upstream->Next(base->upstream, r);
  // Account for the result only if we got one.
  if (ret == RS_RESULT_OK) self->remaining--;
  return ret;
}

static int rppagerNext_Skip(ResultProcessor *base, SearchResult *r) {
  RPPager *self = (RPPager *)base;

  // Currently a pager is never called more than offset+limit times.
  // We limit the entire pipeline to offset+limit (upstream and downstream).
  uint32_t limit = MIN(self->remaining, base->parent->resultLimit);
  // Save the previous limit, so that it will seem untouched to the downstream
  uint32_t downstreamLimit = base->parent->resultLimit;
  base->parent->resultLimit = self->offset + limit;

  // If we've not reached the offset
  while (self->offset) {
    int rc = base->upstream->Next(base->upstream, r);
    if (rc != RS_RESULT_OK) {
      return rc;
    }
    base->parent->resultLimit--;
    self->offset--;
    SearchResult_Clear(r);
  }

  base->parent->resultLimit = downstreamLimit;

  base->Next = rppagerNext_Limit; // switch to second phase
  return base->Next(base, r);
}

static void rppagerFree(ResultProcessor *base) {
  rm_free(base);
}

/* Create a new pager. The offset and limit are taken from the user request */
ResultProcessor *RPPager_New(size_t offset, size_t limit) {
  RPPager *ret = rm_calloc(1, sizeof(*ret));
  ret->offset = offset;
  ret->remaining = limit;

  ret->base.type = RP_PAGER_LIMITER;
  ret->base.Next = rppagerNext_Skip;
  ret->base.Free = rppagerFree;

  return &ret->base;
}

////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////
/// Value Loader                                                             ///
////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////

typedef struct {
  ResultProcessor base;
  RLookup *lk;
  RLookupLoadOptions loadopts;
  QueryError status;
} RPLoader;

/***
 * isDocumentStillValid - check if the document is still valid for loading.
 * @param self The loader
 * @param r The search result
 * @return true if the document is still valid, false otherwise.
 */

static bool isDocumentStillValid(const RPLoader *self, SearchResult *r) {
  if (self->loadopts.sctx->spec->diskSpec) {
    // The Document_Deleted and Document_FailedToOpen flags are not used on disk and are not updated after we take the GIL, so we check the disk directly.
    if (SearchDisk_DocIdDeleted(self->loadopts.sctx->spec->diskSpec, SearchResult_GetDocumentMetadata(r)->id)) {
      SearchResult_SetFlags(r, SearchResult_GetFlags(r) | Result_ExpiredDoc);
      return false;
    }
  } else {
    if ((SearchResult_GetDocumentMetadata(r)->flags & Document_FailedToOpen) || (SearchResult_GetDocumentMetadata(r)->flags & Document_Deleted)) {
        SearchResult_SetFlags(r, SearchResult_GetFlags(r) | Result_ExpiredDoc);
      return false;
    }
  }
  return true;
}

static void rpLoader_loadDocument(RPLoader *self, SearchResult *r) {
  // If the document was modified or deleted, we don't load it, and we need to mark
  // the result as expired.
  if (!isDocumentStillValid(self, r)) {
    return;
  }

  self->loadopts.dmd = SearchResult_GetDocumentMetadata(r);
  // if loading the document has failed, we keep the row as it was.
  // Error code and message are ignored.
  if (RLookup_LoadDocument(self->lk, SearchResult_GetRowDataMut(r), &self->loadopts) != REDISMODULE_OK) {
    // mark the document as "failed to open" for later loaders or other threads (optimization)
    ((RSDocumentMetadata *)(SearchResult_GetDocumentMetadata(r)))->flags |= Document_FailedToOpen;
    // The result contains an expired document.
    SearchResult_SetFlags(r, SearchResult_GetFlags(r) | Result_ExpiredDoc);
    QueryError_ClearError(&self->status);
  }
}

static int rploaderNext(ResultProcessor *base, SearchResult *r) {
  RPLoader *lc = (RPLoader *)base;
  int rc = base->upstream->Next(base->upstream, r);
  if (rc != RS_RESULT_OK) {
    return rc;
  }

  rpLoader_loadDocument(lc, r);
  return RS_RESULT_OK;
}

static void rploaderFreeInternal(ResultProcessor *base) {
  RPLoader *lc = (RPLoader *)base;
  QueryError_ClearError(&lc->status);
  rm_free(lc->loadopts.keys);
}

static void rploaderFree(ResultProcessor *base) {
  rploaderFreeInternal(base);
  rm_free(base);
}

static void rploaderNew_setLoadOpts(RPLoader *self, RedisSearchCtx *sctx, RLookup *lk, const RLookupKey **keys, size_t nkeys, bool forceLoad) {
  self->loadopts.forceString = 1; // used in `LOAD_ALLKEYS` mode.
  self->loadopts.forceLoad = forceLoad;
  self->loadopts.status = &self->status;
  self->loadopts.sctx = sctx;
  self->loadopts.dmd = NULL;
  self->loadopts.keys = rm_malloc(sizeof(*keys) * nkeys);
  memcpy(self->loadopts.keys, keys, sizeof(*keys) * nkeys);
  self->loadopts.nkeys = nkeys;
  if (nkeys) {
    self->loadopts.mode = RLOOKUP_LOAD_KEYLIST;
  } else {
    self->loadopts.mode = RLOOKUP_LOAD_ALLKEYS;
    lk->options |= RLOOKUP_OPT_ALL_LOADED; // TODO: turn on only for HASH specs
  }

  self->lk = lk;
}

static ResultProcessor *RPPlainLoader_New(RedisSearchCtx *sctx, RLookup *lk, const RLookupKey **keys, size_t nkeys, bool forceLoad) {
  RPLoader *self = rm_calloc(1, sizeof(*self));

  rploaderNew_setLoadOpts(self, sctx, lk, keys, nkeys, forceLoad);

  self->base.Next = rploaderNext;
  self->base.Free = rploaderFree;
  self->base.type = RP_LOADER;
  return &self->base;
}

/*******************************************************************************************************************
 *  Safe Loader Results Processor
 *
 * This component should be added to the query's execution pipeline INSTEAD OF a loader, if a loader is needed.
 *
 * The RP has few phases:
 * 1. Buffering phase - the RP will buffer the results from the upstream.
 * 2. Loading phase:
 *   a. Verify that the spec is unlocked, and lock the Redis keyspace.
 *   b. Load the needed data for each buffered result.
 *   c. Unlock the Redis keyspace.
 * 3. Yielding phase - the RP will yield the buffered results.
 *******************************************************************************************************************/

#define DEFAULT_BUFFER_BLOCK_SIZE 1024

typedef struct RPSafeLoader {
  // Loading context
  RPLoader base_loader;

  // Buffer management
  SearchResult **BufferBlocks;
  size_t buffer_results_count;

  // Results iterator
  size_t curr_result_index;

  // Last buffered result code. To know weather to return OK or EOF.
  char last_buffered_rc;

  // If true, the loader will become a plain loader after the buffer is empty.
  // Used when changing the MT mode through a cursor execution session (e.g. FT.CURSOR READ)
  bool becomePlainLoader;

  // Search context
  RedisSearchCtx *sctx;
} RPSafeLoader;

/************************* Safe Loader private functions *************************/

static void SetResult(SearchResult *buffered_result,  SearchResult *result_output) {
  // Free the RLookup row before overriding it.
  RLookupRow_Reset(SearchResult_GetRowDataMut(result_output));
  *result_output = *buffered_result;
}

static SearchResult *GetResultsBlock(RPSafeLoader *self, size_t idx) {
  // Get a pointer to the block at the given index
  SearchResult **ret = array_ensure_at(&self->BufferBlocks, idx, SearchResult*);

  // If the block is not allocated, allocate it
  if (!*ret) {
    *ret = array_new(SearchResult, DEFAULT_BUFFER_BLOCK_SIZE);
  }

  return *ret;

}

// If @param currBlock is full we add a new block and return it, otherwise returns @param CurrBlock.
static SearchResult *InsertResult(RPSafeLoader *self, SearchResult *resToBuffer, SearchResult *currBlock) {
  size_t idx_in_curr_block = self->buffer_results_count % DEFAULT_BUFFER_BLOCK_SIZE;
  // if the block is full, allocate a new one
  if (idx_in_curr_block == 0) {
    // get the curr block, allocate new block if needed
    currBlock = GetResultsBlock(self, self->buffer_results_count / DEFAULT_BUFFER_BLOCK_SIZE);
  }
  // append the result to the current block at rp->curr_idx_at_block
  // this operation takes ownership of the result's allocated data
  currBlock[idx_in_curr_block] = *resToBuffer;
  ++self->buffer_results_count;
  return currBlock;
}

static bool IsBufferEmpty(RPSafeLoader *self) {
  return self->buffer_results_count == 0;
}

static SearchResult *GetNextResult(RPSafeLoader *self) {
  size_t curr_elem_index = self->curr_result_index;

  // if we reached to the end of the buffer return NULL
  if (curr_elem_index >= self->buffer_results_count) {
    return NULL;
  }

  // get current block
  SearchResult *curr_block = self->BufferBlocks[curr_elem_index / DEFAULT_BUFFER_BLOCK_SIZE];

  // get the result in the block
  SearchResult* ret = curr_block + (curr_elem_index % DEFAULT_BUFFER_BLOCK_SIZE);

  // Increase result's index
  ++self->curr_result_index;

  // return result
  return ret;
}

static int rpSafeLoaderNext_Accumulate(ResultProcessor *rp, SearchResult *res);  // Forward declaration

static int rpSafeLoader_ResetAndReturnLastCode(RPSafeLoader *self, SearchResult *res) {
  // Reset the next function, in case we are in cursor mode
  if (self->becomePlainLoader) {
    self->base_loader.base.Next = rploaderNext;
  } else {
    self->base_loader.base.Next = rpSafeLoaderNext_Accumulate;
  }
  self->buffer_results_count = 0;
  self->curr_result_index = 0;

  int rc = self->last_buffered_rc;
  self->last_buffered_rc = RS_RESULT_OK;
  // We CANNOT return `RS_RESULT_OK` HERE, since it will be interpreted as a
  // success while no population of the result was done.
  // So if the last rc was `RS_RESULT_OK`, we need to continue activating the
  // pipeline.
  if (rc == RS_RESULT_OK) {
    return self->base_loader.base.Next(&self->base_loader.base, res);
  }
  return rc;
}

/*********************************************************************************/

static void rpSafeLoader_Load(RPSafeLoader *self) {
  SearchResult *curr_res;

  // iterate the buffer.
  // TODO: implement `GetNextResult` that gets the current block to save calculation time.
  while ((curr_res = GetNextResult(self))) {
    rpLoader_loadDocument(&self->base_loader, curr_res);
  }

  // Reset the iterator
  self->curr_result_index = 0;
}

static int rpSafeLoaderNext_Yield(ResultProcessor *rp, SearchResult *result_output) {
  RPSafeLoader *self = (RPSafeLoader *)rp;
  SearchResult *curr_res = GetNextResult(self);

  if (curr_res) {
    SetResult(curr_res, result_output);
    return RS_RESULT_OK;
  } else {
    return rpSafeLoader_ResetAndReturnLastCode(self, result_output);
  }
}

/*********************************************************************************/

static int rpSafeLoaderNext_Accumulate(ResultProcessor *rp, SearchResult *res) {
  RS_LOG_ASSERT(rp->parent->resultLimit > 0, "Result limit should be greater than 0");
  RPSafeLoader *self = (RPSafeLoader *)rp;

  // Keep fetching results from the upstream result processor until EOF is reached
  RedisSearchCtx *sctx = self->sctx;
  int result_status;
  uint32_t bufferLimit = rp->parent->resultLimit;
  SearchResult resToBuffer = {0};
  SearchResult *currBlock = NULL;
  // Get the next result and save it in the buffer
  while (rp->parent->resultLimit && ((result_status = rp->upstream->Next(rp->upstream, &resToBuffer)) == RS_RESULT_OK)) {
    // Decrease the result limit after getting a result from the upstream
    rp->parent->resultLimit--;
    // Buffer the result.
    currBlock = InsertResult(self, &resToBuffer, currBlock);

    memset(&resToBuffer, 0, sizeof(SearchResult));

  }
  rp->parent->resultLimit = bufferLimit; // Restore the result limit

  // If we exit the loop because we got an error, or we have zero result, return without locking Redis.
  if ((result_status != RS_RESULT_EOF && result_status != RS_RESULT_OK &&
      !(result_status == RS_RESULT_TIMEDOUT && rp->parent->timeoutPolicy == TimeoutPolicy_Return)) ||
      IsBufferEmpty(self)) {
    return result_status;
  }
  // save the last buffered result code to return when we done yielding the buffered results.
  self->last_buffered_rc = result_status;

  // Now we have the data of all documents that pass the query filters,
  // let's lock Redis to provide safe access to Redis keyspace

  // First, we verify that we unlocked the spec before we lock Redis.
  RedisSearchCtx_UnlockSpec(sctx);

  bool isQueryProfile = rp->parent->isProfile;
  rs_wall_clock rpStartTime;
  if (isQueryProfile) rs_wall_clock_init(&rpStartTime);
  // Then, lock Redis to guarantee safe access to Redis keyspace
  RedisModule_ThreadSafeContextLock(sctx->redisCtx);

  rpSafeLoader_Load(self);

  // Done loading. Unlock Redis
  RedisModule_ThreadSafeContextUnlock(sctx->redisCtx);

  if (isQueryProfile) {
    // GIL time is time passed since rpStartTime combined with the time we already accumulated in the rp->GILTime
    rp->parent->GILTime += rs_wall_clock_elapsed_ns(&rpStartTime);
  }

  // Move to the yielding phase
  rp->Next = rpSafeLoaderNext_Yield;
  return rp->Next(rp, res);
}

static void rpSafeLoaderFree(ResultProcessor *base) {
  RPSafeLoader *sl = (RPSafeLoader *)base;

  // Free leftover results in the buffer (if any)
  SearchResult *cur;
  while ((cur = GetNextResult(sl))) {
    SearchResult_Destroy(cur);
  }

  // Free buffer memory blocks
  array_foreach(sl->BufferBlocks, SearchResultsBlock, array_free(SearchResultsBlock));
  array_free(sl->BufferBlocks);

  rploaderFreeInternal(base);

  rm_free(sl);
}

static ResultProcessor *RPSafeLoader_New(RedisSearchCtx *sctx, RLookup *lk, const RLookupKey **keys, size_t nkeys, bool forceLoad) {
  RPSafeLoader *sl = rm_calloc(1, sizeof(*sl));

  rploaderNew_setLoadOpts(&sl->base_loader, sctx, lk, keys, nkeys, forceLoad);

  sl->BufferBlocks = NULL;
  sl->buffer_results_count = 0;
  sl->curr_result_index = 0;

  sl->last_buffered_rc = RS_RESULT_OK;
  sl->sctx = sctx;

  sl->base_loader.base.Next = rpSafeLoaderNext_Accumulate;
  sl->base_loader.base.Free = rpSafeLoaderFree;
  sl->base_loader.base.type = RP_SAFE_LOADER;
  return &sl->base_loader.base;
}

/*********************************************************************************/

typedef struct {
  ResultProcessor base;
  const RLookupKey *out;
} RPKeyNameLoader;

static inline void RPKeyNameLoader_Free(ResultProcessor *self) {
  rm_free(self);
}

static int RPKeyNameLoader_Next(ResultProcessor *base, SearchResult *res) {
  int rc = base->upstream->Next(base->upstream, res);
  if (RS_RESULT_OK == rc) {
    RPKeyNameLoader *nl = (RPKeyNameLoader *)base;
    size_t keyLen = sdslen(SearchResult_GetDocumentMetadata(res)->keyPtr); // keyPtr is an sds
    RLookup_WriteOwnKey(nl->out, SearchResult_GetRowDataMut(res), RSValue_NewCopiedString(SearchResult_GetDocumentMetadata(res)->keyPtr, keyLen));
  }
  return rc;
}

static ResultProcessor *RPKeyNameLoader_New(const RLookupKey *key) {
  RPKeyNameLoader *rp = rm_calloc(1, sizeof(*rp));
  rp->out = key;

  ResultProcessor *base = &rp->base;
  base->Free = RPKeyNameLoader_Free;
  base->Next = RPKeyNameLoader_Next;
  base->type = RP_KEY_NAME_LOADER;
  return base;
}

/*********************************************************************************/

ResultProcessor *RPLoader_New(RedisSearchCtx *sctx, uint32_t reqflags, RLookup *lk, const RLookupKey **keys, size_t nkeys, bool forceLoad, uint32_t *outStateflags) {
  if (RSGlobalConfig.enableUnstableFeatures) {
    if (nkeys == 1 && !strcmp(keys[0]->path, UNDERSCORE_KEY)) {
      // Return a thin RP that doesn't actually loads anything or access to the key space
      // Returning without turning on the `QEXEC_S_HAS_LOAD` flag
      return RPKeyNameLoader_New(keys[0]);
    }
  }
  *outStateflags |= QEXEC_S_HAS_LOAD;
  if (reqflags & QEXEC_F_RUN_IN_BACKGROUND) {
    // Assumes that Redis is *NOT* locked while executing the loader
    return RPSafeLoader_New(sctx, lk, keys, nkeys, forceLoad);
  } else {
    // Assumes that Redis *IS* locked while executing the loader
    return RPPlainLoader_New(sctx, lk, keys, nkeys, forceLoad);
  }
}

// Consumes the input loader and returns a new safe loader that wraps it.
static ResultProcessor *RPSafeLoader_New_FromPlainLoader(RPLoader *loader) {
  RPSafeLoader *sl = rm_new(RPSafeLoader);

  // Copy the loader, move ownership of the keys
  sl->base_loader = *loader;
  sl->sctx = loader->loadopts.sctx;
  rm_free(loader);

  // Reset the loader's buffer and state
  sl->BufferBlocks = NULL;
  sl->buffer_results_count = 0;
  sl->curr_result_index = 0;

  sl->last_buffered_rc = RS_RESULT_OK;

  sl->base_loader.base.Next = rpSafeLoaderNext_Accumulate;
  sl->base_loader.base.Free = rpSafeLoaderFree;
  sl->base_loader.base.type = RP_SAFE_LOADER;
  return &sl->base_loader.base;
}

void SetLoadersForBG(QueryProcessingCtx *qctx) {
  ResultProcessor *cur = qctx->endProc;
  ResultProcessor dummyHead = { .upstream = cur };
  ResultProcessor *downstream = &dummyHead;
  while (cur) {
    if (cur->type == RP_LOADER) {
      cur = RPSafeLoader_New_FromPlainLoader((RPLoader *)cur);
      downstream->upstream = cur;
    } else if (cur->type == RP_SAFE_LOADER) {
      // If the pipeline was originally built with a safe loader and later got set to run on
      // the main thread, we keep the safe loader and only change the next function.
      // Now we need to change the next function back to the safe loader's next function.
      RS_ASSERT(cur->Next == rploaderNext);
      cur->Next = rpSafeLoaderNext_Accumulate;
      ((RPSafeLoader *)cur)->becomePlainLoader = false;
    }
    downstream = cur;
    cur = cur->upstream;
  }
  // Update the endProc to the new head in case it was changed
  qctx->endProc = dummyHead.upstream;
}

void SetLoadersForMainThread(QueryProcessingCtx *qctx) {
  ResultProcessor *rp = qctx->endProc;
  while (rp) {
    if (rp->type == RP_SAFE_LOADER) {
      // If the `Next` function is `rpSafeLoaderNext_Accumulate`, it means that the loader didn't
      // buffer any result yet (or was reset), so we can safely change it to `rploaderNext`.
      // Otherwise, we keep the `Next` function as is (rpSafeLoaderNext_Yield) and set the flag
      // `becomePlainLoader` to true, so the loader will become a plain loader after the buffer is
      // empty.
      if (rp->Next == rpSafeLoaderNext_Accumulate) {
        rp->Next = rploaderNext;
      }
      ((RPSafeLoader *)rp)->becomePlainLoader = true;
    }
    rp = rp->upstream;
  }
}

/*********************************************************************************/

static char *RPTypeLookup[RP_MAX] = {"Index",   "Loader",    "Threadsafe-Loader", "Scorer",
                                     "Sorter",  "Counter",   "Pager/Limiter",     "Highlighter",
                                     "Grouper", "Projector", "Filter",            "Profile",
                                     "Network", "Metrics Applier", "Key Name Loader", "Score Max Normalizer",
                                     "Vector Normalizer", "Hybrid Merger", "Depleter"};

const char *RPTypeToString(ResultProcessorType type) {
  RS_LOG_ASSERT(type >= 0 && type < RP_MAX, "enum is out of range");
  return RPTypeLookup[type];
}

ResultProcessorType StringToRPType(const char *str) {
  for (int i = 0; i < RP_MAX; i++) {
    if (!strcmp(str, RPTypeLookup[i])) {
      return i;
    }
  }
  return RP_MAX;
}

////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////
/// Profile RP                                                               ///
////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////

typedef struct {
  ResultProcessor base;
  rs_wall_clock_ns_t profileTime;
  uint64_t profileCount;
} RPProfile;

static int rpprofileNext(ResultProcessor *base, SearchResult *r) {
  RPProfile *self = (RPProfile *)base;

  rs_wall_clock start;
  rs_wall_clock_init(&start);
  int rc = base->upstream->Next(base->upstream, r);
  self->profileTime += rs_wall_clock_elapsed_ns(&start);
  self->profileCount++;
  return rc;
}

static void rpProfileFree(ResultProcessor *base) {
  RPProfile *rp = (RPProfile *)base;
  rm_free(rp);
}

ResultProcessor *RPProfile_New(ResultProcessor *rp, QueryProcessingCtx *qctx) {
  RPProfile *rpp = rm_calloc(1, sizeof(*rpp));

  rpp->profileCount = 0;
  rpp->base.upstream = rp;
  rpp->base.parent = qctx;
  rpp->base.Next = rpprofileNext;
  rpp->base.Free = rpProfileFree;
  rpp->base.type = RP_PROFILE;

  return &rpp->base;
}

rs_wall_clock_ns_t RPProfile_GetClock(ResultProcessor *rp) {
  RPProfile *self = (RPProfile *)rp;
  return self->profileTime;
}

uint64_t RPProfile_GetCount(ResultProcessor *rp) {
  RPProfile *self = (RPProfile *)rp;
  return self->profileCount;
}

void RPProfile_IncrementCount(ResultProcessor *rp) {
  RPProfile *self = (RPProfile *)rp;
  self->profileCount++;
}

void Profile_AddRPs(QueryProcessingCtx *qctx) {
  ResultProcessor *cur = qctx->endProc = RPProfile_New(qctx->endProc, qctx);
  while (cur && cur->upstream && cur->upstream->upstream) {
    cur = cur->upstream;
    cur->upstream = RPProfile_New(cur->upstream, qctx);
    cur = cur->upstream;
  }
}

 /*******************************************************************************************************************
   *  Max Score Normalizer Result Processor
   *
   * This result processor normalizes the scores of search results using division by
   * the max score. It gathers all results from the upstream processor, finds the
   * maximum score, and divides each score by the maximum. This ensures that all scores
   * fall within the range [0, 1].
   *
   * The processor works in two phases:
   * 1. Accumulation: Gather all results from upstream and find the max score.
   * 2. Yield: Normalize each result's score by division with the max score, then pass
   *    it downstream.
  *******************************************************************************************************************/
 typedef struct {
   ResultProcessor base;
   // Stores the max value found (if needed in the future)
   double maxValue;
   const RLookupKey *scoreKey;
   SearchResult *pooledResult;
   arrayof(SearchResult *) pool;
   bool timedOut;
 } RPMaxScoreNormalizer;


 static void RPMaxScoreNormalizer_Free(ResultProcessor *base) {
   RPMaxScoreNormalizer *self = (RPMaxScoreNormalizer *)base;
   array_free_ex(self->pool, srDtor(*(char **)ptr));
   srDtor(self->pooledResult);
   rm_free(self);
 }

 static int RPMaxScoreNormalizer_Yield(ResultProcessor *rp, SearchResult *r){
   RPMaxScoreNormalizer* self = (RPMaxScoreNormalizer*)rp;
   size_t length = array_len(self->pool);
   if (length == 0) {
    // We've already yielded all results, return EOF
    int ret = self->timedOut ? RS_RESULT_TIMEDOUT : RS_RESULT_EOF;
    self->timedOut = false;
    return ret;
   }
  SearchResult *poppedResult = array_pop(self->pool);
  SearchResult_Override(r, poppedResult);
  rm_free(poppedResult);
  double oldScore = SearchResult_GetScore(r);
  if (self->maxValue != 0) {
    SearchResult_SetScore(r, SearchResult_GetScore(r) / self->maxValue);
  }
  if (self->scoreKey) {
    RLookup_WriteOwnKey(self->scoreKey, SearchResult_GetRowDataMut(r), RSValue_NewNumber(SearchResult_GetScore(r)));
  }
  EXPLAIN(SearchResult_GetScoreExplainMut(r),
        "Final BM25STD.NORM: %.2f = Original Score: %.2f / Max Score: %.2f",
        SearchResult_GetScore(r), oldScore, self->maxValue);
  return RS_RESULT_OK;
 }

static int RPMaxScoreNormalizerNext_innerLoop(ResultProcessor *rp, SearchResult *r) {
  RPMaxScoreNormalizer *self = (RPMaxScoreNormalizer *)rp;
  // get the next result from upstream. `self->pooledResult` is expected to be empty and allocated.
  int rc = rp->upstream->Next(rp->upstream, self->pooledResult);
  // if our upstream has finished - just change the state to not accumulating, and yield
  if (rc == RS_RESULT_EOF) {
    rp->Next = RPMaxScoreNormalizer_Yield;
    return rp->Next(rp, r);
  } else if (rc == RS_RESULT_TIMEDOUT && (rp->parent->timeoutPolicy == TimeoutPolicy_Return)) {
    self->timedOut = true;
    rp->Next = RPMaxScoreNormalizer_Yield;
    return rp->Next(rp, r);
  } else if (rc != RS_RESULT_OK) {
    return rc;
  }

  self->maxValue = MAX(self->maxValue, SearchResult_GetScore(self->pooledResult));
  // copy the index result to make it thread safe - but only if it is pushed to the heap
  SearchResult_SetIndexResult(self->pooledResult, NULL);
  array_ensure_append_1(self->pool, self->pooledResult);

  // we need to allocate a new result for the next iteration
  self->pooledResult = rm_calloc(1, sizeof(*self->pooledResult));
  return RESULT_QUEUED;
}

static int RPMaxScoreNormalizer_Accum(ResultProcessor *rp, SearchResult *r) {
  RPMaxScoreNormalizer *self = (RPMaxScoreNormalizer *)rp;
  uint32_t chunkLimit = rp->parent->resultLimit;
  rp->parent->resultLimit = UINT32_MAX; // we want to accumulate all results
  int rc;
  while ((rc = RPMaxScoreNormalizerNext_innerLoop(rp, r)) == RESULT_QUEUED) {};
  rp->parent->resultLimit = chunkLimit; // restore the limit
  return rc;
}

 /* Create a new Max Collector processor */
 ResultProcessor *RPMaxScoreNormalizer_New(const RLookupKey *rlk) {
  RPMaxScoreNormalizer *ret = rm_calloc(1, sizeof(*ret));
  ret->pooledResult = rm_calloc(1, sizeof(*ret->pooledResult));
  ret->pool = array_new(SearchResult*, 0);
  ret->base.Next = RPMaxScoreNormalizer_Accum;
  ret->base.Free = RPMaxScoreNormalizer_Free;
  ret->base.type = RP_MAX_SCORE_NORMALIZER;
  ret->scoreKey = rlk;
  return &ret->base;
}

/*******************************************************************************************************************
 *  Vector Normalizer Result Processor
 *
 * Normalizes vector distance scores using a provided normalization function.
 * Processes results immediately without accumulation, unlike RPMaxScoreNormalizer.
 * The normalization function is provided during construction by pipeline construction logic.
 *******************************************************************************************************************/

typedef struct {
  ResultProcessor base;
  VectorNormFunction normFunc;
  const RLookupKey *scoreKey;   // score field
} RPVectorNormalizer;

static int RPVectorNormalizer_Next(ResultProcessor *rp, SearchResult *r) {
  RPVectorNormalizer *self = (RPVectorNormalizer *)rp;

  // Get next result from upstream
  int rc = rp->upstream->Next(rp->upstream, r);
  if (rc != RS_RESULT_OK) {
    return rc;
  }

  // Apply normalization to the score
  double normalizedScore = 0.0;
  RSValue *distanceValue = RLookup_GetItem(self->scoreKey, SearchResult_GetRowData(r));
  if (distanceValue) {
    double originalScore = 0.0;
    if (RSValue_ToNumber(distanceValue, &originalScore)) {
      normalizedScore = self->normFunc(originalScore);
    }
  }
  SearchResult_SetScore(r, normalizedScore);

  // Update distance field
  if (self->scoreKey) {
    RLookup_WriteOwnKey(self->scoreKey, SearchResult_GetRowDataMut(r), RSValue_NewNumber(normalizedScore));
  }
  return RS_RESULT_OK;
}

static void RPVectorNormalizer_Free(ResultProcessor *rp) {
  RPVectorNormalizer *self = (RPVectorNormalizer *)rp;
  rm_free(self);
}

/* Create a new Vector Normalizer processor */
ResultProcessor *RPVectorNormalizer_New(VectorNormFunction normFunc, const RLookupKey *scoreKey) {
  RPVectorNormalizer *ret = rm_calloc(1, sizeof(*ret));

  ret->normFunc = normFunc;
  ret->base.Next = RPVectorNormalizer_Next;
  ret->base.Free = RPVectorNormalizer_Free;
  ret->base.type = RP_VECTOR_NORMALIZER;
  ret->scoreKey = scoreKey;

  return &ret->base;
}

/*******************************************************************************************************************
 *  Depleter Result Processor
 *
 *  The RPDepleter result processor offloads the task of consuming all results from
 *  its upstream processor into a background thread, storing them in an internal
 *  array. While the background thread is running, calls to Next() wait on a shared
 *  condition variable and return RS_RESULT_DEPLETING. The thread can be awakened
 *  either by its own depleting thread completing or by another RPDepleter's thread
 *  signaling completion. Once depleting is complete for this processor, Next()
 *  yields results one by one from the internal array, and finally returns the last
 *  return code from the upstream.
 *  NOTE: Currently the recommended number of upstreams is 2. Using more may
 *  induce performance issues, until a more robust mechanism is implemented.
 *******************************************************************************************************************/
typedef struct {
  ResultProcessor base;                // Base result processor struct
  // We require separate contexts because we have different threads.
  // Each thread may use the redis context in the search context and in order for things to be thread safe we need a context for each thread
  RedisSearchCtx *depletingThreadCtx;  // Upstream Search context - used by the depleting thread
  RedisSearchCtx *nextThreadCtx;       // Downstream search context - used by the thread calling Next
  arrayof(SearchResult *) results;     // Array of pointers to SearchResult, filled by the depleting thread
  bool done_depleting;                 // Set to `true` when depleting is finished (under lock)
  uint cur_idx;                        // Current index for yielding results
  RPStatus last_rc;                    // Last return code from upstream
  bool first_call;                     // Whether the first call to Next has been made
  StrongRef sync_ref;                  // Reference to shared synchronization object (DepleterSync)
} RPDepleter;

/*
 * Shared synchronization object for all RPDepleter instances of a pipeline.
 * We have two main synchronization fronts:
 * 1. The pipeline thread should wake up once ANY depleter finishes depleting.
 *    For this, we have the shared condition variable `cond` and mutex `mutex`.
 * 2. The pipeline thread should release the index lock only after ALL depleters
 *    have locked the index for read.
 *    It is critical that it releases it at the point and not sooner or later,
 *    since sooner may cause an inconsistent view of the index among the subqueries,
 *    and later may cause performance issues (and deadlock if not released at all)
 *    as the GIL may not be released due to the main-thread waiting on the index-lock.
 */
typedef struct {
  pthread_cond_t cond;
  pthread_mutex_t mutex;
  uint num_depleters;      // Number of depleters to sync
  atomic_int num_locked;   // Number of depleters that have locked the index
  bool index_released;     // Whether or not the index-spec has been released by the pipeline thread yet
  bool take_index_lock;    // Whether or not the depleter should take the index lock
} DepleterSync;

// Free function for DepleterSync
static void DepleterSync_Free(void *obj) {
  DepleterSync *sync = (DepleterSync *)obj;
  pthread_cond_destroy(&sync->cond);
  pthread_mutex_destroy(&sync->mutex);
  rm_free(sync);
}

// Create a new shared sync object for a pipeline
StrongRef DepleterSync_New(uint num_depleters, bool take_index_lock) {
  DepleterSync *sync = rm_calloc(1, sizeof(DepleterSync));
  pthread_cond_init(&sync->cond, NULL);
  pthread_mutex_init(&sync->mutex, NULL);
  sync->num_depleters = num_depleters;
  sync->take_index_lock = take_index_lock;
  return StrongRef_New(sync, DepleterSync_Free);
}

/**
 * Destructor
 */
static void RPDepleter_Free(ResultProcessor *base) {
  RPDepleter *self = (RPDepleter *)base;
  array_free_ex(self->results, srDtor(*(char **)ptr));
  StrongRef_Release(self->sync_ref);
  rm_free(self);
}

/**
 * Background thread function: consumes all results from upstream and stores them in the results array.
 * Signals completion by setting done_depleting to `true` and broadcasting to condition variable.
 */
static void RPDepleter_Deplete(void *arg) {
  RPDepleter *self = (RPDepleter *)arg;
  RPStatus rc;

  DepleterSync *sync = (DepleterSync *)StrongRef_Get(self->sync_ref);

  if (sync->take_index_lock) {
    // Lock the index for read
    RedisSearchCtx_LockSpecRead(self->depletingThreadCtx);
    // Increment the counter
    atomic_fetch_add(&sync->num_locked, 1);
  }

  // Deplete the pipeline into the `self->results` array.
  SearchResult *r = rm_calloc(1, sizeof(*r));
  while ((rc = self->base.upstream->Next(self->base.upstream, r)) == RS_RESULT_OK) {
    array_append(self->results, r);
    r = rm_calloc(1, sizeof(*r));
  }

  // Clean up the last allocated SearchResult that wasn't used
  SearchResult_Destroy(r);
  rm_free(r);

  // Save the last return code from the upstream.
  self->last_rc = rc;

  // Verify the index is unlocked (in case the pipeline did not release the lock,
  // e.g., limit + no Loader)
  if (sync->take_index_lock) {
    RedisSearchCtx_UnlockSpec(self->depletingThreadCtx);
  }

  // Signal completion under mutex protection
  pthread_mutex_lock(&sync->mutex);
  self->done_depleting = true;
  pthread_cond_broadcast(&sync->cond);  // Wake up all waiting depleters
  pthread_mutex_unlock(&sync->mutex);
}

/**
 * Next function for RPDepleter.
 */
static int RPDepleter_Next_Yield(ResultProcessor *base, SearchResult *r) {
  RPDepleter *self = (RPDepleter *)base;

  // Depleting thread is done, it's safe to return the results.
  if (self->cur_idx == array_len(self->results)) {
    // We've reached the end of the array, return the last code from the upstream.
    int rc = self->last_rc;
    self->last_rc = RS_RESULT_EOF;
    return rc;
  }
  // Return the next result in the array.
  SearchResult *current = self->results[self->cur_idx];
  SearchResult_Override(r, current);    // Copy result data to output
  rm_free(current);
  self->results[self->cur_idx] = NULL;
  self->cur_idx++;
  return RS_RESULT_OK;
}

// Adds a depletion job to the depleters thread pool
static inline void RPDepleter_StartDepletionThread(RPDepleter *self) {
    redisearch_thpool_add_work(depleterPool, RPDepleter_Deplete, self, THPOOL_PRIORITY_HIGH);
}

// Can only succeed once, if called after RE_RESULT_OK was returned an error will be returned
// Waits for all the depletion threads to take a read lock
// After all of them took a lock it will release its own read lock which was previously obtained in the main query thread
// This ensures all the depleters see a consistent index state across the board for their lifetime
static inline int RPDepleter_WaitForDepletionToStart(DepleterSync *sync, RedisSearchCtx *nextThreadCtx) {
  if (sync->take_index_lock && !sync->index_released) {
    // Load the atomic counter
    int num_locked = atomic_load(&sync->num_locked);
    RS_ASSERT(num_locked <= sync->num_depleters);
    if (num_locked == sync->num_depleters) {
      // Release the index
      RedisSearchCtx_UnlockSpec(nextThreadCtx);
      // Mark the index as released
      sync->index_released = true;
      return RS_RESULT_OK;
    } else {
      // Not all depleter threads have taken the index lock yet. Wait for them
      return RS_RESULT_DEPLETING;
    }
  }
  // Depleting already started
  return RS_RESULT_ERROR;
}

// Must be called after sync->mutex was locked by the thread
static inline int RPDepleter_WaitForDepletionToComplete(RPDepleter *self, DepleterSync *sync) {
  // Check if depleting is already done.
  // We do this while holding the mutex so that we don't miss a signal.
  if (self->done_depleting == true) {
    self->base.Next = RPDepleter_Next_Yield;
    return RS_RESULT_OK;
  }

  // Wait on condition variable for any depleter to signal completion
  pthread_cond_wait(&sync->cond, &sync->mutex);

  // Check if our specific thread is done after being woken up
  if (self->done_depleting == true) {
    // Our thread is done, switch to yield mode
    self->base.Next = RPDepleter_Next_Yield;
    return RS_RESULT_OK;
  }

  // Our thread is not done yet, but another depleter signaled completion
  // Return DEPLETING so downstream can check other depleters
  return RS_RESULT_DEPLETING;
}

/**
 * Next function for RPDepleter.
 * First call: starts background thread and returns `RS_RESULT_DEPLETING`.
 * Subsequent calls: wait on condition variable for any depleter to complete.
 * When woken up, checks if this depleter is done. If so, switches to yield mode.
 * If not, returns `RS_RESULT_DEPLETING` to allow downstream to check other depleters.
 *
 * A dedicated thread-pool `depleterPool` is used, such that there are no
 * contentions with the `_workers_thpool` thread-pool, such as adding a new job
 * to its queue after `WORKERS` has been set to `0`.
 */
static int RPDepleter_Next_Dispatch(ResultProcessor *base, SearchResult *r) {
  RPDepleter *self = (RPDepleter *)base;

  // The first call to next will start the depleting thread, and return `RS_RESULT_DEPLETING`.
  if (self->first_call) {
    self->first_call = false;
    RPDepleter_StartDepletionThread(self);
    return RS_RESULT_DEPLETING;
  }

  DepleterSync *sync = (DepleterSync *)StrongRef_Get(self->sync_ref);
  if (RPDepleter_WaitForDepletionToStart(sync, self->nextThreadCtx) == RS_RESULT_DEPLETING) {
    return RS_RESULT_DEPLETING;
  }

  pthread_mutex_lock(&sync->mutex);
  const int rc = RPDepleter_WaitForDepletionToComplete(self, sync);
  pthread_mutex_unlock(&sync->mutex);
  if (rc == RS_RESULT_OK) {
    return base->Next(base, r);
  }
  return rc;
}

/**
 * Synchronous Next function for RPDepleter.
 * On first call: depletes all results synchronously in the current thread.
 * Subsequent calls: yields results one by one from the internal array.
 *
 * This is used for single-pipeline scenarios where we want to avoid
 * the overhead of threading and ensure totalResults is populated
 * before returning.
 */
static int RPDepleter_Next_Sync(ResultProcessor *base, SearchResult *r) {
  RPDepleter *self = (RPDepleter *)base;

  // First call: deplete synchronously
  if (self->first_call) {
    self->first_call = false;

    // Call the depletion function directly (no thread pool)
    RPDepleter_Deplete(self);

    // Switch to yield mode
    self->base.Next = RPDepleter_Next_Yield;

    // Now yield the first result
    return RPDepleter_Next_Yield(base, r);
  }

  // Should never reach here since we switch to yield mode on first call
  RS_LOG_ASSERT(0, "Unreachable code");
  return RS_RESULT_ERROR;
}

/**
 * Constructs a new RPDepleter processor. Consumes the StrongRef given.
 */
ResultProcessor *RPDepleter_New(StrongRef sync_ref, RedisSearchCtx *depletingThreadCtx, RedisSearchCtx *nextThreadCtx) {
  RPDepleter *ret = rm_calloc(1, sizeof(*ret));
  ret->results = array_new(SearchResult*, 0);
  ret->base.Next = RPDepleter_Next_Dispatch;
  ret->base.Free = RPDepleter_Free;
  ret->base.type = RP_DEPLETER;
  ret->first_call = true;
  ret->sync_ref = sync_ref;
  ret->depletingThreadCtx = depletingThreadCtx;
  ret->nextThreadCtx = nextThreadCtx;
  // Make sure the sync reference is valid
  RS_LOG_ASSERT(StrongRef_Get(sync_ref), "Invalid sync reference");
  return &ret->base;
}

/**
 * Constructs a new RPDepleter processor that runs synchronously (no background thread).
 * This is useful for single-pipeline scenarios where you want to avoid threading overhead
 * and ensure totalResults is fully populated before yielding results.
 * Consumes the StrongRef given.
 */
ResultProcessor *RPDepleter_NewSync(StrongRef sync_ref, RedisSearchCtx *sctx) {
  RPDepleter *ret = rm_calloc(1, sizeof(*ret));
  ret->results = array_new(SearchResult*, 0);
  ret->base.Next = RPDepleter_Next_Sync;  // Use synchronous version
  ret->base.Free = RPDepleter_Free;
  ret->base.type = RP_DEPLETER;
  ret->first_call = true;
  ret->sync_ref = sync_ref;
  ret->depletingThreadCtx = sctx;
  ret->nextThreadCtx = sctx;
  // Make sure the sync reference is valid
  RS_LOG_ASSERT(StrongRef_Get(sync_ref), "Invalid sync reference");
  return &ret->base;
}

static inline bool verifyInvariants(arrayof(ResultProcessor*) depleters, DepleterSync** outSync, RedisSearchCtx** outSearchCtx) {
  DepleterSync *sync = NULL;
  RedisSearchCtx *searchCtx = NULL;
  size_t count = array_len(depleters);
  for (size_t i = 0; i < count; i++) {
    RPDepleter *depleter = (RPDepleter*)depleters[i];
    DepleterSync *depleterSync = (DepleterSync *)StrongRef_Get(depleter->sync_ref);
    if (sync && sync != depleterSync) {
      return false;
    }
    if (searchCtx && searchCtx != depleter->nextThreadCtx) {
      return false;
    }
    if (depleter->first_call == false) {
      return false;
    }
    sync = depleterSync;
    searchCtx = depleter->nextThreadCtx;
  }
  if (sync->num_depleters != count) {
    return false;
  }
  *outSync = sync;
  *outSearchCtx = searchCtx;
  return true;
}

/*
* This function will trigger the depeletion process for the depleters group
* 0. Some sanity checks, will return an error if it detected an invalid state
* 1. It will start a thread for every depleter
* 2. Wait for all the threads to take their own read lock and then unlock the lock it held - we assume the lock was taken in the query thread
* 3. Wait for the depletion to complete in all the depleters, there is no timeout handling here - we rely on each depleter to handle timeout and stop depleting.
* 4. The function must return only after all the depletion threads finished running
*/
int RPDepleter_DepleteAll(arrayof(ResultProcessor*) depleters) {
  DepleterSync *sync = NULL;
  RedisSearchCtx *searchCtx = NULL;
  // Verify we are in a sane state before starting the depletion process
  if (!verifyInvariants(depleters, &sync, &searchCtx)) {
    return RS_RESULT_ERROR;
  }

  const size_t count = array_len(depleters);
  // Start all depleting threads
  for (size_t i = 0; i < count; i++) {
    RPDepleter* depleter = (RPDepleter*)depleters[i];
    depleter->first_call = false;
    RPDepleter_StartDepletionThread(depleter);
  }

  // Wait for depleting to start with configurable interval and timeout
  while (RPDepleter_WaitForDepletionToStart(sync, searchCtx) == RS_RESULT_DEPLETING) {
    usleep(1000);
  }

  for (size_t numDone = 0; numDone < count; ) {
    pthread_mutex_lock(&sync->mutex);
    for (size_t i = 0; i < count; i++) {
      RPDepleter *depleter = (RPDepleter*)depleters[i];
      // Can't rely on done_depleting since it is set by thread and it doesn't change its own Next function
      // This way the behaviour is more predictable
      if (depleter->base.Next == RPDepleter_Next_Yield) {
        continue;
      }
      // Will internally wait on a condition variable until the depleter finishes depleting
      if (RPDepleter_WaitForDepletionToComplete(depleter, sync) == RS_RESULT_OK) {
        ++numDone;
      }
    }
    pthread_mutex_unlock(&sync->mutex);
    // Only sleep if we haven't completed all depleters
    if (numDone < count) {
      usleep(1000);
    }
  }
  return RS_RESULT_OK;
}

// Wrapper for HybridSearchResult destructor to match dictionary value destructor signature
static void hybridSearchResultValueDestructor(void *privdata, void *obj) {
  HybridSearchResult_Free((HybridSearchResult*)obj);
}

// Dictionary type for keyPtr -> HybridSearchResult mapping
dictType dictTypeHybridSearchResult = {
  .hashFunction = stringsHashFunction,
  .keyDup = stringsKeyDup,
  .valDup = NULL,
  .keyCompare = stringsKeyCompare,
  .keyDestructor = stringsKeyDestructor,
  .valDestructor = hybridSearchResultValueDestructor,
};

 /*******************************************************************************************************************
  *  Hybrid Merger Result Processor
  *
  * This result processor merges results from two upstream processors using a hybrid scoring function.
  * It takes results from both upstreams and applies the provided function to combine their scores.
  *******************************************************************************************************************/
 typedef struct {
 ResultProcessor base;
 HybridScoringContext *hybridScoringCtx;  // Store by pointer - RPHybridMerger is responsible for freeing it
 ResultProcessor **upstreams;     // Dynamic array of upstream processors
 size_t numUpstreams;             // Number of upstream processors
 dict *hybridResults;             // keyPtr -> HybridSearchResult mapping
 dictIterator *iterator;          // Iterator for yielding results
 const RLookupKey *scoreKey;      // Key for writing score as field when YIELD_SCORE_AS is specified
 const RLookupKey *docKey;        // Key for reading document key when dmd is not available
 RPStatus* upstreamReturnCodes;   // Final return codes from each upstream
 HybridLookupContext *lookupCtx;  // Lookup context for field merging

} RPHybridMerger;

/* Generic helper function to check if any upstream has a specific return code */
static bool RPHybridMerger_HasReturnCode(const RPHybridMerger *self, int returnCode) {
  for (size_t i = 0; i < self->numUpstreams; i++) {
    if (self->upstreamReturnCodes[i] == returnCode) {
      return true;
    }
  }
  return false;
}

/* Helper function to check if any upstream timed out */
static inline bool RPHybridMerger_TimedOut(const RPHybridMerger *self) {
  return RPHybridMerger_HasReturnCode(self, RS_RESULT_TIMEDOUT);
}

/* Helper function to check if any upstream errored */
static inline bool RPHybridMerger_Error(const RPHybridMerger *self) {
  return RPHybridMerger_HasReturnCode(self, RS_RESULT_ERROR);
}

/* Helper function to store a result from an upstream into the hybrid merger's dictionary
  * @param r - the result to store
  * @param hybridResults - the dictionary to store the result in
  * @param upstreamIndex - the index of the upstream that provided the result
  * @param numUpstreams - the number of upstreams
  * @param score - used to override the result's score
 */
 static bool hybridMergerStoreUpstreamResult(RPHybridMerger* self, SearchResult *r, size_t upstreamIndex, double score) {
  // Single shard case - use dmd->keyPtr
  RLookupRow translated = {0};
  RLookupRow_WriteFieldsFrom(&r->rowdata, self->lookupCtx->sourceLookups[upstreamIndex], &translated, self->lookupCtx->tailLookup);
  RLookupRow_Reset(&r->rowdata);
  r->rowdata = translated;

  const RSDocumentMetadata *dmd = SearchResult_GetDocumentMetadata(r);
  const char *keyPtr = dmd ? dmd->keyPtr : NULL;
  // Coordinator case - no dmd - use docKey in rlookup
  const bool fallbackToLookup = !keyPtr && self->docKey;
  if (fallbackToLookup) {
    RSValue *docKeyValue = RLookup_GetItem(self->docKey, &r->rowdata);
    if (docKeyValue != NULL) {
      keyPtr = RSValue_StringPtrLen(docKeyValue, NULL);
    }
  }
  if (!keyPtr) {
    return false;
  }

  // Check if we've seen this document before
  HybridSearchResult *hybridResult = (HybridSearchResult*)dictFetchValue(self->hybridResults, keyPtr);

  if (!hybridResult) {
    // First time seeing this document - create new hybrid result
    hybridResult = HybridSearchResult_New(self->numUpstreams);
    dictAdd(self->hybridResults, (void*)keyPtr, hybridResult);
  }

   SearchResult_SetScore(r, score);
   HybridSearchResult_StoreResult(hybridResult, r, upstreamIndex);
   return true;
 }

 /* Helper function to consume results from a single upstream */
 static int hybridMergerConsumeFromUpstream(RPHybridMerger *self, size_t maxResults, size_t upstreamIndex) {
   size_t consumed = 0;
   int rc = RS_RESULT_OK;
   SearchResult *r = rm_calloc(1, sizeof(*r));
   ResultProcessor *upstream = self->upstreams[upstreamIndex];
   while (consumed < maxResults && (rc = upstream->Next(upstream, r)) == RS_RESULT_OK) {
       double score = SearchResult_GetScore(r);
       consumed++;
       if (self->hybridScoringCtx->scoringType == HYBRID_SCORING_RRF) {
         score = consumed;
       }
       if (hybridMergerStoreUpstreamResult(self, r, upstreamIndex, score)) {
         r = rm_calloc(1, sizeof(*r));
       } else {
         SearchResult_Clear(r);
         --consumed; // avoid wrong rank in RRF
       }
   }
   rm_free(r);
   return rc;
 }

 /* Yield phase - iterate through results and apply hybrid scoring */
static int RPHybridMerger_Yield(ResultProcessor *rp, SearchResult *r) {
  RPHybridMerger *self = (RPHybridMerger *)rp;

  RS_ASSERT(self->iterator);
  // Get next entry from iterator
  dictEntry *entry = dictNext(self->iterator);
  if (!entry) {
    // No more results to yield
    int ret = RPHybridMerger_TimedOut(self) ? RS_RESULT_TIMEDOUT : RS_RESULT_EOF;
    return ret;
  }

  // Get the key and value before removing the entry
  void *key = dictGetKey(entry);
  HybridSearchResult *hybridResult = (HybridSearchResult*)dictGetVal(entry);
  RS_ASSERT(hybridResult);

  SearchResult *mergedResult = mergeSearchResults(hybridResult, self->hybridScoringCtx, self->lookupCtx);
  if (!mergedResult) {
    return RS_RESULT_ERROR;
  }

  // Override the output result with merged data
  SearchResult_Override(r, mergedResult);
  rm_free(mergedResult);

  // Add score as field if scoreKey is provided
  if (self->scoreKey) {
    RLookup_WriteOwnKey(self->scoreKey, SearchResult_GetRowDataMut(r), RSValue_NewNumber(SearchResult_GetScore(r)));
  }

  return RS_RESULT_OK;
 }

 /* Accumulation phase - consume window results from all upstreams */
 static int RPHybridMerger_Accum(ResultProcessor *rp, SearchResult *r) {
  RPHybridMerger *self = (RPHybridMerger *)rp;

  size_t window;
  if (self->hybridScoringCtx->scoringType == HYBRID_SCORING_RRF) {
    window = self->hybridScoringCtx->rrfCtx.window;
  } else {
    window = self->hybridScoringCtx->linearCtx.window;
  }

  bool *consumed = rm_calloc(self->numUpstreams, sizeof(bool));
  size_t numConsumed = 0;
  // Continuously try to consume from upstreams until all are consumed
  while (numConsumed < self->numUpstreams) {
    for (size_t i = 0; i < self->numUpstreams; i++) {
      if (consumed[i]) {
        continue;
      }
      int rc = hybridMergerConsumeFromUpstream(self, window, i);

      if (rc == RS_RESULT_DEPLETING) {
        // Upstream is still active but not ready to provide results. Skip to the next.
        continue;
      }

      // Store the final return code for this upstream
      self->upstreamReturnCodes[i] = rc;
      // Currently continues processing other upstreams.
      // TODO: Update logic to stop processing further results — we want to return immediately on timeout or error : MOD-11004
      // Note: This processor might have rp_depleter as an upstream, which currently lacks a mechanism to stop its spawned thread before completion.
      consumed[i] = true;
      numConsumed++;
    }
  }

  // Free the consumed tracking array
  rm_free(consumed);

  if (RPHybridMerger_Error(self)) {
    return RS_RESULT_ERROR;
  } else if (RPHybridMerger_TimedOut(self) && rp->parent->timeoutPolicy == TimeoutPolicy_Fail) {
    return RS_RESULT_TIMEDOUT;
  }

  // Initialize iterator for yield phase
  self->iterator = dictGetIterator(self->hybridResults);

  // Update total results to reflect the number of unique documents we'll yield
  rp->parent->totalResults = dictSize(self->hybridResults);

  // Switch to yield phase
  rp->Next = RPHybridMerger_Yield;
  return rp->Next(rp, r);
 }

 /* Free function for RPHybridMerger */
 static void RPHybridMerger_Free(ResultProcessor *rp) {
   RPHybridMerger *self = (RPHybridMerger *)rp;

   // Free the iterator
   if (self->iterator) {
    dictReleaseIterator(self->iterator);
   }

   HybridScoringContext_Free(self->hybridScoringCtx);

   // Free the hybrid results dictionary (HybridSearchResult values automatically freed by destructor)
   dictRelease(self->hybridResults);

   // Free the upstreams array, the upstreams themselves are freed by the pipeline(e.g as a result of AREQ_Free)
   array_free(self->upstreams);

   // Free lookup context if it exists
   if (self->lookupCtx) {
     HybridLookupContext_Free(self->lookupCtx);
   }

   // Free the processor itself
   rm_free(self);
 }

 const RLookupKey *RPHybridMerger_GetScoreKey(ResultProcessor *rp) {
   if (!rp || rp->type != RP_HYBRID_MERGER) {
     return NULL;
   }
   RPHybridMerger *self = (RPHybridMerger *)rp;
   return self->scoreKey;
 }

 /* Create a new Hybrid Merger processor */
ResultProcessor *RPHybridMerger_New(HybridScoringContext *hybridScoringCtx,
                                    ResultProcessor **upstreams,
                                    size_t numUpstreams,
                                    const RLookupKey *docKey,
                                    const RLookupKey *scoreKey,
                                    RPStatus *subqueriesReturnCodes,
                                    HybridLookupContext *lookupCtx) {
  RPHybridMerger *ret = rm_calloc(1, sizeof(*ret));

  RS_ASSERT(numUpstreams > 0);
  ret->numUpstreams = numUpstreams;

  // Store the context by pointer - RPHybridMerger takes ownership and is responsible for freeing it
  RS_ASSERT(hybridScoringCtx);
  ret->hybridScoringCtx = hybridScoringCtx;

  // Store the scoreKey for writing scores as fields when YIELD_SCORE_AS is specified or __score otherwise
  ret->scoreKey = scoreKey;

  ret->docKey = docKey;

  // Store reference to the hybrid request's subqueries return codes array
  RS_ASSERT(subqueriesReturnCodes);
  ret->upstreamReturnCodes = subqueriesReturnCodes;

  // Store lookup context for field merging (takes ownership)
  ret->lookupCtx = lookupCtx;

   // Since we're storing by pointer, the caller is responsible for memory management
   ret->upstreams = upstreams;
   ret->hybridResults = dictCreate(&dictTypeHybridSearchResult, NULL);

   // Calculate maximal dictionary size based on scoring type
   size_t maximalSize;
   if (hybridScoringCtx->scoringType == HYBRID_SCORING_RRF) {
     maximalSize = hybridScoringCtx->rrfCtx.window * numUpstreams;
   } else {
     maximalSize = hybridScoringCtx->linearCtx.window * numUpstreams;
   }
   // Pre-size the dictionary to avoid multiple resizes during accumulation
   dictExpand(ret->hybridResults, maximalSize);

   ret->iterator = NULL;

   ret->base.type = RP_HYBRID_MERGER;
   ret->base.Next = RPHybridMerger_Accum;
   ret->base.Free = RPHybridMerger_Free;

   return &ret->base;
 }

 /*******************************************************************************************************************
 *  Debug only result processors
 *
 * *******************************************************************************************************************/

// Insert the result processor between the last result processor and its downstream result processor
static void addResultProcessor(QueryProcessingCtx *qctx, ResultProcessor *rp) {
  ResultProcessor *cur = qctx->endProc;
  ResultProcessor dummyHead = { .upstream = cur };
  ResultProcessor *downstream = &dummyHead;

  // Search for the last result processor
  while (cur) {
    if (!cur->upstream) {
      rp->parent = qctx;
      downstream->upstream = rp;
      rp->upstream = cur;
      break;
    }
    downstream = cur;
    cur = cur->upstream;
  }
  // Update the endProc to the new head in case it was changed
  qctx->endProc = dummyHead.upstream;
}

// Insert the result processor before the first occurrence of a specific RP type in the upstream
static bool addResultProcessorBeforeType(QueryProcessingCtx *qctx, ResultProcessor *rp, ResultProcessorType target_type) {
  ResultProcessor *cur = qctx->endProc;
  ResultProcessor *downstream = NULL;

  // Search for the target result processor type
  while (cur) {
    // Change downstream -> cur(type) -> cur->upstream
    // To: downstream -> rp -> cur(type) -> cur->upstream

    if (cur->type == target_type) {
      rp->parent = qctx;
      rp->upstream = cur;
      // Checking edge case: we are the first RP in the stream
      if (cur == qctx->endProc) {
        qctx->endProc = rp;
      } else {
        downstream->upstream = rp;
      }
      return true;
    }

    downstream = cur;
    cur = cur->upstream;
  }

  return false;
}

// Insert the result processor after the first occurrence of a specific RP type in the upstream
// Cannot be the last RP in the stream
static bool addResultProcessorAfterType(QueryProcessingCtx *qctx, ResultProcessor *rp, ResultProcessorType target_type) {
  ResultProcessor *cur = qctx->endProc;
  ResultProcessor *downstream = cur;

  bool found = false;

  // Search for the target result processor type
  while (cur) {
    // Change downstream -> cur(type) -> cur->upstream
    // To: downstream -> cur(type) -> rp-> cur->upstream
    if (cur->type == target_type) {
      if (!cur->upstream) {
        return false;
      }
      rp->upstream = cur->upstream;
      cur->upstream = rp;
      rp->parent = qctx;
      return true;
    }
    downstream = cur;
    cur = cur->upstream;
  }

  return false;
}

/*******************************************************************************************************************
 *  Timeout Processor - DEBUG ONLY
 *
 * returns timeout after N results, N >= 0.
 * If N is larger than the actual results, EOF is returned.
 *******************************************************************************************************************/

typedef struct {
  ResultProcessor base;
  uint32_t count;
  uint32_t remaining;
  RedisSearchCtx *sctx;
} RPTimeoutAfterCount;

/** For debugging purposes
 * Will add a result processor that will return timeout according to the results count specified.
 * @param results_count: number of results to return. should be greater equal 0.
 * The result processor will also change the query timing so further checks down the pipeline will also result in timeout.
 */
void PipelineAddTimeoutAfterCount(QueryProcessingCtx *qctx, RedisSearchCtx *sctx, size_t results_count) {
  ResultProcessor *RPTimeoutAfterCount = RPTimeoutAfterCount_New(results_count, sctx);
  addResultProcessor(qctx, RPTimeoutAfterCount);
}

static void RPTimeoutAfterCount_SimulateTimeout(ResultProcessor *rp_timeout, RedisSearchCtx *sctx) {
    // set timeout to now for the RP up the chain to handle
    static struct timespec now;
    clock_gettime(CLOCK_MONOTONIC_RAW, &now);
    sctx->time.timeout = now;

    // search upstream for rpQueryItNext to set timeout limiter
    ResultProcessor *cur = rp_timeout->upstream;
    while (cur && cur->type != RP_INDEX) {
        cur = cur->upstream;
    }

    if (cur) { // This is a shard pipeline
      RPQueryIterator *rp_index = (RPQueryIterator *)cur;
      rp_index->timeoutLimiter = TIMEOUT_COUNTER_LIMIT - 1;
    }
}

static int RPTimeoutAfterCount_Next(ResultProcessor *base, SearchResult *r) {
  RPTimeoutAfterCount *self = (RPTimeoutAfterCount *)base;

  // If we've reached COUNT:
  if (!self->remaining) {

    RPTimeoutAfterCount_SimulateTimeout(base, self->sctx);

    int rc = base->upstream->Next(base->upstream, r);
    if (rc == RS_RESULT_TIMEDOUT) {
      // reset the counter for the next run in cursor mode
      self->remaining = self->count;
    }

    return rc;
  }

  self->remaining--;
  return base->upstream->Next(base->upstream, r);
}

static void RPTimeoutAfterCount_Free(ResultProcessor *base) {
  rm_free(base);
}

ResultProcessor *RPTimeoutAfterCount_New(size_t count, RedisSearchCtx *sctx) {
  RPTimeoutAfterCount *ret = rm_calloc(1, sizeof(RPTimeoutAfterCount));
  ret->count = count;
  ret->remaining = count;
  ret->sctx = sctx;
  ret->base.type = RP_TIMEOUT;
  ret->base.Next = RPTimeoutAfterCount_Next;
  ret->base.Free = RPTimeoutAfterCount_Free;

  return &ret->base;
}

typedef struct {
  ResultProcessor base;
} RPCrash;

static void RPCrash_Free(ResultProcessor *base) {
  rm_free(base);
}

static int RPCrash_Next(ResultProcessor *base, SearchResult *r) {
  RPCrash *self = (RPCrash *)base;
  abort();
  return base->upstream->Next(base->upstream, r);
}

ResultProcessor *RPCrash_New() {
  RPCrash *ret = rm_calloc(1, sizeof(RPCrash));
  ret->base.type = RP_CRASH;
  ret->base.Next = RPCrash_Next;
  ret->base.Free = RPCrash_Free;
  return &ret->base;
}

void PipelineAddCrash(struct AREQ *r) {
  ResultProcessor *crash = RPCrash_New();
  addResultProcessor(AREQ_QueryProcessingCtx(r), crash);
}

/*******************************************************************************************************************
 *  Pause Processor - DEBUG ONLY
 *
 * Pauses the query after N results, N >= 0.
 *******************************************************************************************************************/
typedef struct {
  ResultProcessor base;
  uint32_t count;
  uint32_t remaining;
} RPPauseAfterCount;

bool PipelineAddPauseRPcount(QueryProcessingCtx *qctx, size_t results_count, bool before, ResultProcessorType rp_type, QueryError *status) {
  ResultProcessor *RPPauseAfterCount = RPPauseAfterCount_New(results_count);

  if (!RPPauseAfterCount) {
    // Set query error
    QueryError_SetError(status, QUERY_ERROR_CODE_GENERIC, "Failed to create pause RP or another debug RP is already set");
    return false;
  }

  bool success = false;
  if (before) {
    success = addResultProcessorBeforeType(qctx, RPPauseAfterCount, rp_type);
  } else {
    success = addResultProcessorAfterType(qctx, RPPauseAfterCount, rp_type);
  }
  // Free if failed
  if (!success) {
    RPPauseAfterCount->Free(RPPauseAfterCount);
    QueryError_SetWithoutUserDataFmt(status, QUERY_ERROR_CODE_GENERIC, "%s RP type not found in stream or tried to insert after last RP", RPTypeToString(rp_type));
  }
  return success;

}

static void RPPauseAfterCount_Pause(RPPauseAfterCount *self) {

  QueryDebugCtx_SetPause(true);
  while (QueryDebugCtx_IsPaused()) { // volatile variable
    usleep(1000);
  }
}

static int RPPauseAfterCount_Next(ResultProcessor *base, SearchResult *r) {
  RPPauseAfterCount *self = (RPPauseAfterCount *)base;

  // If we've reached COUNT:
  if (!self->remaining) {
    RPPauseAfterCount_Pause(self);
  }

  self->remaining--;
  return base->upstream->Next(base->upstream, r);
}

static void RPPauseAfterCount_Free(ResultProcessor *base) {
  RS_LOG_ASSERT(QueryDebugCtx_GetDebugRP() == base, "Freed debug RP tried to change DebugCTX debugRP but it's not the current debug RP");
  rm_free(base);
  QueryDebugCtx_SetDebugRP(NULL);
}

ResultProcessor *RPPauseAfterCount_New(size_t count) {

  // Validate no other debug RP is set
  // If so, don't set it and return NULL
  if (QueryDebugCtx_HasDebugRP()) {
    return NULL;
  }

  RPPauseAfterCount *ret = rm_calloc(1, sizeof(RPPauseAfterCount));
  ret->count = count;
  ret->remaining = count;
  ret->base.type = RP_PAUSE;
  ret->base.Next = RPPauseAfterCount_Next;
  ret->base.Free = RPPauseAfterCount_Free;

  QueryDebugCtx_SetDebugRP(&ret->base);

  return &ret->base;
}
