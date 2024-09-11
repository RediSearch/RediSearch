/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
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

/*******************************************************************************************************************
 *  General Result Processor Helper functions
 *******************************************************************************************************************/

void QITR_Cleanup(QueryIterator *qitr) {
  ResultProcessor *p = qitr->rootProc;
  while (p) {
    ResultProcessor *next = p->upstream;
    if (p->Free) {
      p->Free(p);
    }
    p = next;
  }
}

// Allocates a new SearchResult, and populates it with `r`'s data (takes
// ownership as well)
SearchResult *SearchResult_Copy(SearchResult *r) {
  SearchResult *ret = rm_malloc(sizeof(*ret));
  *ret = *r;
  return ret;
}

void SearchResult_Clear(SearchResult *r) {
  // This won't affect anything if the result is null
  r->score = 0;
  if (r->scoreExplain) {
    SEDestroy(r->scoreExplain);
    r->scoreExplain = NULL;
  }
  if (r->indexResult) {
    // IndexResult_Free(r->indexResult);
    r->indexResult = NULL;
  }

  r->flags = 0;
  RLookupRow_Wipe(&r->rowdata);
  if (r->dmd) {
    DMD_Return(r->dmd);
    r->dmd = NULL;
  }
}

/* Free the search result object including the object itself */
void SearchResult_Destroy(SearchResult *r) {
  SearchResult_Clear(r);
  RLookupRow_Cleanup(&r->rowdata);
}


/*******************************************************************************************************************
 *  Base Result Processor - this processor is the topmost processor of every processing chain.
 *
 * It takes the raw index results from the index, and builds the search result to be sent
 * downstream.
 *******************************************************************************************************************/

// Get the index spec from the result processor - this should be used only if the spec
// can be accessed safely.
#define RP_SPEC(rpctx) (RP_SCTX(rpctx)->spec)

static int UnlockSpec_and_ReturnRPResult(ResultProcessor *base, int result_status) {
  RedisSearchCtx_UnlockSpec(RP_SCTX(base));
  return result_status;
}
typedef struct {
  ResultProcessor base;
  IndexIterator *iiter;
  size_t timeoutLimiter;    // counter to limit number of calls to TimedOut_WithCounter()
} RPIndexIterator;

/* Next implementation */
static int rpidxNext(ResultProcessor *base, SearchResult *res) {
  RPIndexIterator *self = (RPIndexIterator *)base;
  IndexIterator *it = self->iiter;

  if (TimedOut_WithCounter(&RP_SCTX(base)->timeout, &self->timeoutLimiter) == TIMED_OUT) {
    return UnlockSpec_and_ReturnRPResult(base, RS_RESULT_TIMEDOUT);
  }

  if (RP_SCTX(base)->flags == RS_CTX_UNSET) {
    // If we need to read the iterators and we didn't lock the spec yet, lock it now
    // and reopen the keys in the concurrent search context (iterators' validation)
    RedisSearchCtx_LockSpecRead(RP_SCTX(base));
    ConcurrentSearchCtx_ReopenKeys(base->parent->conc);
  }

  RSIndexResult *r;
  const RSDocumentMetadata *dmd;
  int rc;

  // Read from the root filter until we have a valid result
  while (1) {
    rc = it->Read(it->ctx, &r);
    switch (rc) {
    case INDEXREAD_EOF:
      // This means we are done!
      return UnlockSpec_and_ReturnRPResult(base, RS_RESULT_EOF);
    case INDEXREAD_TIMEOUT:
      return UnlockSpec_and_ReturnRPResult(base, RS_RESULT_TIMEDOUT);
    case INDEXREAD_NOTFOUND:
      continue;
    default: // INDEXREAD_OK
      if (!r)
        continue;
    }

    if (r->dmd) {
      dmd = r->dmd;
    } else {
      dmd = DocTable_Borrow(&RP_SPEC(base)->docs, r->docId);
    }
    if (!dmd || (dmd->flags & Document_Deleted)) {
      DMD_Return(dmd);
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

    // Increment the total results barring deleted results
    base->parent->totalResults++;
    break;
  }

  // set the result data
  res->docId = r->docId;
  res->indexResult = r;
  res->score = 0;
  res->dmd = dmd;
  res->rowdata.sv = dmd->sortVector;
  return RS_RESULT_OK;
}

static void rpidxFree(ResultProcessor *iter) {
  rm_free(iter);
}

ResultProcessor *RPIndexIterator_New(IndexIterator *root) {
  RPIndexIterator *ret = rm_calloc(1, sizeof(*ret));
  ret->iiter = root;
  ret->base.Next = rpidxNext;
  ret->base.Free = rpidxFree;
  ret->base.type = RP_INDEX;
  return &ret->base;
}

IndexIterator *QITR_GetRootFilter(QueryIterator *it) {
  /* On coordinator, the root result processor will be a network result processor and we should ignore it */
  if (it->rootProc->type == RP_INDEX) {
      return ((RPIndexIterator *)it->rootProc)->iiter;
  }
  return NULL;
}

void QITR_PushRP(QueryIterator *it, ResultProcessor *rp) {
  rp->parent = it;
  if (!it->rootProc) {
    it->endProc = it->rootProc = rp;
    rp->upstream = NULL;
    return;
  }
  rp->upstream = it->endProc;
  it->endProc = rp;
}

void QITR_FreeChain(QueryIterator *qitr) {
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
    res->score = self->scorer(&self->scorerCtx, res->indexResult, res->dmd, base->parent->minScore);
    if (self->scorerCtx.scrExp) {
      res->scoreExplain = (RSScoreExplain *)self->scorerCtx.scrExp;
      self->scorerCtx.scrExp = rm_calloc(1, sizeof(RSScoreExplain));
    }
    // If we got the special score RS_SCORE_FILTEROUT - disregard the result and decrease the total
    // number of results (it's been increased by the upstream processor)
    if (res->score == RS_SCORE_FILTEROUT) {
      base->parent->totalResults--;
      SearchResult_Clear(res);
      // continue and loop to the next result, since this is excluded by the
      // scorer.
      continue;
    }
    if (self->scoreKey) {
      RLookup_WriteOwnKey(self->scoreKey, &res->rowdata, RS_NumVal(res->score));
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

/* Create a new scorer by name. If the name is not found in the scorer registry, we use the defalt
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

  arrayof(RSYieldableMetric) arr = res->indexResult->metrics;
  for (size_t i = 0; i < array_len(arr); i++) {
    RLookup_WriteKey(arr[i].key, &(res->rowdata), arr[i].value);
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
    RLookupRow oldrow = r->rowdata;
    *r = *cur_best;

    rm_free(cur_best);
    RLookupRow_Cleanup(&oldrow);
    return RS_RESULT_OK;
  }
  return self->timedOut ? RS_RESULT_TIMEDOUT : RS_RESULT_EOF;
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
    self->pooledResult->indexResult = NULL;
    mmh_insert(self->pq, self->pooledResult);
    if (self->pooledResult->score < rp->parent->minScore) {
      rp->parent->minScore = self->pooledResult->score;
    }
    // we need to allocate a new result for the next iteration
    self->pooledResult = rm_calloc(1, sizeof(*self->pooledResult));
  } else {
    // find the min result
    SearchResult *minh = mmh_peek_min(self->pq);

    // update the min score. Irrelevant to SORTBY mode but hardly costs anything...
    if (minh->score > rp->parent->minScore) {
      rp->parent->minScore = minh->score;
    }

    // if needed - pop it and insert a new result
    if (self->cmp(self->pooledResult, minh, self->cmpCtx) > 0) {
      self->pooledResult->indexResult = NULL;
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

  if (h1->score < h2->score) {
    return -1;
  } else if (h1->score > h2->score) {
    return 1;
  }
  return h1->docId > h2->docId ? -1 : 1;
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
    const RSValue *v1 = RLookup_GetItem(self->fieldcmp.keys[i], &h1->rowdata);
    const RSValue *v2 = RLookup_GetItem(self->fieldcmp.keys[i], &h2->rowdata);
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
    // printf("asc? %d Compare: \n", ascending);
    // RSValue_Print(v1);
    // printf(" <=> ");
    // RSValue_Print(v2);
    // printf("\n");

    if (rc != 0) return ascending ? -rc : rc;
  }

  int rc = h1->docId < h2->docId ? -1 : 1;
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

void SortAscMap_Dump(uint64_t tt, size_t n) {
  for (size_t ii = 0; ii < n; ++ii) {
    if (SORTASCMAP_GETASC(tt, ii)) {
      printf("%lu=(A), ", ii);
    } else {
      printf("%lu=(D)", ii);
    }
  }
  printf("\n");
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

  self->remaining--;
  return base->upstream->Next(base->upstream, r);
}

static int rppagerNext_Skip(ResultProcessor *base, SearchResult *r) {
  RPPager *self = (RPPager *)base;

  // Currently a pager is never called more than offset+limit times.
  // We limit the entire pipeline to offset+limit (upstream and downstream).
  uint32_t limit = MIN(self->remaining, base->parent->resultLimit);
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

static void rpLoader_loadDocument(RPLoader *self, SearchResult *r) {
  // If the document was modified or deleted, we don't load it, and we need to mark
  // the result as expired.
  if ((r->dmd->flags & Document_FailedToOpen) || (r->dmd->flags & Document_Deleted)) {
    r->flags |= Result_ExpiredDoc;
    return;
  }

  self->loadopts.dmd = r->dmd;
  // if loading the document has failed, we keep the row as it was.
  // Error code and message are ignored.
  if (RLookup_LoadDocument(self->lk, &r->rowdata, &self->loadopts) != REDISMODULE_OK) {
    // mark the document as "failed to open" for later loaders or other threads (optimization)
    ((RSDocumentMetadata *)(r->dmd))->flags |= Document_FailedToOpen;
    // The result contains an expired document.
    r->flags |= Result_ExpiredDoc;
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
} RPSafeLoader;

/************************* Safe Loader private functions *************************/

static void SetResult(SearchResult *buffered_result,  SearchResult *result_output) {
  // Free the RLookup row before overriding it.
  RLookupRow_Cleanup(&result_output->rowdata);
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

static int rpSafeLoader_ResetAndReturnLastCode(RPSafeLoader *self) {
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
  }
  if (!curr_res || rp->parent->resultLimit <= 1) {
    return rpSafeLoader_ResetAndReturnLastCode(self);
  }
  return RS_RESULT_OK;
}

/*********************************************************************************/

static int rpSafeLoaderNext_Accumulate(ResultProcessor *rp, SearchResult *res) {
  RPSafeLoader *self = (RPSafeLoader *)rp;

  // Keep fetching results from the upstream result processor until EOF is reached
  RedisSearchCtx *sctx = RP_SCTX(rp);
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

  // Then, lock Redis to guarantee safe access to Redis keyspace
  RedisModule_ThreadSafeContextLock(sctx->redisCtx);

  rpSafeLoader_Load(self);

  // Done loading. Unlock Redis
  RedisModule_ThreadSafeContextUnlock(sctx->redisCtx);

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

  sl->base_loader.base.Next = rpSafeLoaderNext_Accumulate;
  sl->base_loader.base.Free = rpSafeLoaderFree;
  sl->base_loader.base.type = RP_SAFE_LOADER;
  return &sl->base_loader.base;
}

/*********************************************************************************/

ResultProcessor *RPLoader_New(AREQ *r, RLookup *lk, const RLookupKey **keys, size_t nkeys, bool forceLoad) {
  r->stateflags |= QEXEC_S_HAS_LOAD;
  if (r->reqflags & QEXEC_F_RUN_IN_BACKGROUND) {
    // Assumes that Redis is *NOT* locked while executing the loader
    return RPSafeLoader_New(r->sctx, lk, keys, nkeys, forceLoad);
  } else {
    // Assumes that Redis *IS* locked while executing the loader
    return RPPlainLoader_New(r->sctx, lk, keys, nkeys, forceLoad);
  }
}

// Consumes the input loader and returns a new safe loader that wraps it.
static ResultProcessor *RPSafeLoader_New_FromPlainLoader(RPLoader *loader) {
  RPSafeLoader *sl = rm_new(RPSafeLoader);

  // Copy the loader, move ownership of the keys
  sl->base_loader = *loader;
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

void SetLoadersForBG(AREQ *r) {
  ResultProcessor *cur = r->qiter.endProc;
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
      assert(cur->Next == rploaderNext);
      cur->Next = rpSafeLoaderNext_Accumulate;
      ((RPSafeLoader *)cur)->becomePlainLoader = false;
    }
    downstream = cur;
    cur = cur->upstream;
  }
  // Update the endProc to the new head in case it was changed
  r->qiter.endProc = dummyHead.upstream;
}

void SetLoadersForMainThread(AREQ *r) {
  ResultProcessor *rp = r->qiter.endProc;
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
                                     "Network", "Metrics Applier"};

const char *RPTypeToString(ResultProcessorType type) {
  RS_LOG_ASSERT(type >= 0 && type < RP_MAX, "enum is out of range");
  return RPTypeLookup[type];
}

void RP_DumpChain(const ResultProcessor *rp) {
  for (; rp; rp = rp->upstream) {
    printf("RP(%s) @%p\n", RPTypeToString(rp->type), rp);
    RS_LOG_ASSERT(rp->upstream != rp, "ResultProcessor should be different then upstream");
  }
}

////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////
/// Profile RP                                                               ///
////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////

typedef struct {
  ResultProcessor base;
  clock_t profileTime;
  uint64_t profileCount;
} RPProfile;

static int rpprofileNext(ResultProcessor *base, SearchResult *r) {
  RPProfile *self = (RPProfile *)base;

  clock_t rpStartTime = clock();
  int rc = base->upstream->Next(base->upstream, r);
  self->profileTime += clock() - rpStartTime;
  self->profileCount++;
  return rc;
}

static void rpProfileFree(ResultProcessor *base) {
  RPProfile *rp = (RPProfile *)base;
  rm_free(rp);
}

ResultProcessor *RPProfile_New(ResultProcessor *rp, QueryIterator *qiter) {
  RPProfile *rpp = rm_calloc(1, sizeof(*rpp));

  rpp->profileCount = 0;
  rpp->base.upstream = rp;
  rpp->base.parent = qiter;
  rpp->base.Next = rpprofileNext;
  rpp->base.Free = rpProfileFree;
  rpp->base.type = RP_PROFILE;

  return &rpp->base;
}

clock_t RPProfile_GetClock(ResultProcessor *rp) {
  RPProfile *self = (RPProfile *)rp;
  return self->profileTime;
}

uint64_t RPProfile_GetCount(ResultProcessor *rp) {
  RPProfile *self = (RPProfile *)rp;
  return self->profileCount;
}

void Profile_AddRPs(QueryIterator *qiter) {
  ResultProcessor *cur = qiter->endProc = RPProfile_New(qiter->endProc, qiter);
  while (cur && cur->upstream && cur->upstream->upstream) {
    cur = cur->upstream;
    cur->upstream = RPProfile_New(cur->upstream, qiter);
    cur = cur->upstream;
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
  size_t count;
} RPCounter;

static int rpcountNext(ResultProcessor *base, SearchResult *res) {
  int rc;
  RPCounter *self = (RPCounter *)base;

  while ((rc = base->upstream->Next(base->upstream, res)) == RS_RESULT_OK) {
    self->count += 1;
    SearchResult_Clear(res);
  }

  // Since this never returns RM_OK, in profile mode, count should be increased
  // to compensate for EOF
  if (base->upstream->type == RP_PROFILE) {
    ((RPProfile *)base->parent->endProc)->profileCount++;
  }

  return rc;
}

/* Free impl. for scorer - frees up the scorer privdata if needed */
static void rpcountFree(ResultProcessor *rp) {
  RPScorer *self = (RPScorer *)rp;
  rm_free(self);
}

/* Create a new counter. */
ResultProcessor *RPCounter_New() {
  RPCounter *ret = rm_calloc(1, sizeof(*ret));
  ret->count = 0;
  ret->base.Next = rpcountNext;
  ret->base.Free = rpcountFree;
  ret->base.type = RP_COUNTER;
  return &ret->base;
}
