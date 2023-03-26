/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "result_processor.h"
#include "query.h"
#include "extension.h"
#include <util/minmax_heap.h>
#include "ext/default.h"
#include "rmutil/rm_assert.h"
#include "rmutil/cxx/chrono-clock.h"
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

// Get the index search context from the result processor
#define RP_SCTX(rpctx) ((rpctx)->parent->sctx)
// Get the index spec from the result processor - this should be used only if the spec
// can be accessed safely.
#define RP_SPEC(rpctx) (RP_SCTX(rpctx)->spec)

static int UnlockSpec_and_ReturnRPResult(ResultProcessor *base, int result_status) {
  if(RP_SCTX(base)) {
    RedisSearchCtx_UnlockSpec(RP_SCTX(base));
  }
  return result_status;
}
typedef struct {
  ResultProcessor base;
  IndexIterator *iiter;
  struct timespec timeout;  // milliseconds until timeout
  size_t timeoutLimiter;    // counter to limit number of calls to TimedOut_WithCounter()
} RPIndexIterator;

/* Next implementation */
static int rpidxNext(ResultProcessor *base, SearchResult *res) {
  RPIndexIterator *self = (RPIndexIterator *)base;
  IndexIterator *it = self->iiter;

  if (TimedOut_WithCounter(&self->timeout, &self->timeoutLimiter) == TIMED_OUT) {
    return UnlockSpec_and_ReturnRPResult(base, RS_RESULT_TIMEDOUT);
  }

  // No root filter - the query has 0 results
  if (self->iiter == NULL) {
    return UnlockSpec_and_ReturnRPResult(base, RS_RESULT_EOF);
  }

  RSIndexResult *r;
  const RSDocumentMetadata *dmd;
  int rc;

  // Read from the root filter until we have a valid result
  while (1) {
    rc = it->Read(it->ctx, &r);
    // This means we are done!
    switch (rc) {
    case INDEXREAD_EOF:
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

ResultProcessor *RPIndexIterator_New(IndexIterator *root, struct timespec timeout) {
  RPIndexIterator *ret = rm_calloc(1, sizeof(*ret));
  ret->iiter = root;
  ret->timeout = timeout;
  ret->base.Next = rpidxNext;
  ret->base.Free = rpidxFree;
  ret->base.type = RP_INDEX;
  return &ret->base;
}

void updateRPIndexTimeout(ResultProcessor *base, struct timespec timeout) {
  RPIndexIterator *self = (RPIndexIterator *)base;
  self->timeout = timeout;
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
                              const ScoringFunctionArgs *fnargs) {
  RPScorer *ret = rm_calloc(1, sizeof(*ret));
  ret->scorer = funcs->sf;
  ret->scorerFree = funcs->ff;
  ret->scorerCtx = *fnargs;
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

  // The desired size of the heap - top N results
  uint32_t size;

  // The heap. We use a min-max heap here
  heap_t *pq;

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

  // return as soon as heap is full
  bool quickExit;
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
  return RS_RESULT_EOF;
}

static void rpsortFree(ResultProcessor *rp) {
  RPSorter *self = (RPSorter *)rp;
  if (self->pooledResult) {
    SearchResult_Destroy(self->pooledResult);
    rm_free(self->pooledResult);
  }

  // calling mmh_free will free all the remaining results in the heap, if any
  mmh_free(self->pq);
  rm_free(rp);
}

#define RESULT_QUEUED RS_RESULT_MAX + 1

static int rpsortNext_innerLoop(ResultProcessor *rp, SearchResult *r) {
  RPSorter *self = (RPSorter *)rp;

  if (self->pooledResult == NULL) {
    self->pooledResult = rm_calloc(1, sizeof(*self->pooledResult));
  } else {
    RLookupRow_Wipe(&self->pooledResult->rowdata);
  }

  SearchResult *h = self->pooledResult;
  int rc = rp->upstream->Next(rp->upstream, h);

  // if our upstream has finished - just change the state to not accumulating, and yield
  if (rc == RS_RESULT_EOF || (rc == RS_RESULT_TIMEDOUT && rp->parent->timeoutPolicy == TimeoutPolicy_Return)) {
    // Transition state:
    rp->Next = rpsortNext_Yield;
    return rpsortNext_Yield(rp, r);
  } else if (rc != RS_RESULT_OK) {
    // whoops!
    return rc;
  }

  // If the queue is not full - we just push the result into it
  if (self->pq->count < self->size) {

    // copy the index result to make it thread safe - but only if it is pushed to the heap
    h->indexResult = NULL;
    mmh_insert(self->pq, h);
    self->pooledResult = NULL;
    if (h->score < rp->parent->minScore) {
      rp->parent->minScore = h->score;
    }
    // collected `limit` results. No need to continue.
    if (self->quickExit && self->pq->count + 1 == self->pq->size) {
      rp->Next = rpsortNext_Yield;
      return rpsortNext_Yield(rp, r);
    }
  } else {
    // find the min result
    SearchResult *minh = mmh_peek_min(self->pq);

    // update the min score. Irrelevant to SORTBY mode but hardly costs anything...
    if (minh->score > rp->parent->minScore) {
      rp->parent->minScore = minh->score;
    }

    // if needed - pop it and insert a new result
    if (self->cmp(h, minh, self->cmpCtx) > 0) {
      h->indexResult = NULL;
      self->pooledResult = mmh_exchange_min(self->pq, h);
      SearchResult_Clear(self->pooledResult);
    } else {
      // The current should not enter the pool, so just leave it as is
      self->pooledResult = h;
      SearchResult_Clear(self->pooledResult);
    }
  }
  return RESULT_QUEUED;
}

static int rpsortNext_Accum(ResultProcessor *rp, SearchResult *r) {
  int rc;
  while ((rc = rpsortNext_innerLoop(rp, r)) == RESULT_QUEUED) {
    // Do nothing.
  }
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
        break; // following the existing logic. TODO: if both are NULL, should we continue to the next field?
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

ResultProcessor *RPSorter_NewByFields(size_t maxresults, const RLookupKey **keys, size_t nkeys,
                                      uint64_t ascmap, bool quickExit) {

  RPSorter *ret = rm_calloc(1, sizeof(*ret));
  ret->cmp = nkeys ? cmpByFields : cmpByScore;
  ret->cmpCtx = ret;
  ret->quickExit = quickExit;
  ret->fieldcmp.ascendMap = ascmap;
  ret->fieldcmp.keys = keys;
  ret->fieldcmp.nkeys = nkeys;

  ret->pq = mmh_init_with_size(maxresults, ret->cmp, ret->cmpCtx, srDtor);
  ret->size = maxresults;
  ret->pooledResult = NULL;
  ret->base.Next = rpsortNext_Accum;
  ret->base.Free = rpsortFree;
  ret->base.type = RP_SORTER;
  ret->base.behavior = RESULT_PROCESSOR_B_ACCUMULATOR;
  return &ret->base;
}

ResultProcessor *RPSorter_NewByScore(size_t maxresults, bool quickExit) {
  return RPSorter_NewByFields(maxresults, NULL, 0, 0, quickExit);
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
  uint32_t limit;
  uint32_t count;
} RPPager;

static int rppagerNext(ResultProcessor *base, SearchResult *r) {
  RPPager *self = (RPPager *)base;
  int rc;

  // If we've not reached the offset
  while (self->count < self->offset) {
    int rc = base->upstream->Next(base->upstream, r);
    if (rc != RS_RESULT_OK) {
      return rc;
    }
    self->count++;
    SearchResult_Clear(r);
  }

  // If we've reached LIMIT:
  if (self->count >= self->limit + self->offset) {
    return UnlockSpec_and_ReturnRPResult(base, RS_RESULT_EOF);
  }

  self->count++;
  rc = base->upstream->Next(base->upstream, r);
  return rc;
}

static void rppagerFree(ResultProcessor *base) {
  rm_free(base);
}

/* Create a new pager. The offset and limit are taken from the user request */
ResultProcessor *RPPager_New(size_t offset, size_t limit) {
  RPPager *ret = rm_calloc(1, sizeof(*ret));
  ret->offset = offset;
  ret->limit = limit;
  ret->base.type = RP_PAGER_LIMITER;
  ret->base.Next = rppagerNext;
  ret->base.Free = rppagerFree;

  // If the pager reaches the limit, it will declare EOF, without an additional call to its upstream.next.
  ret->base.behavior = RESULT_PROCESSOR_B_ABORTER;
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
  const RLookupKey **fields;
  size_t nfields;
  RLookupLoadOptions loadopts;
  QueryError status;
} RPLoader;

static int rploaderNext(ResultProcessor *base, SearchResult *r) {
  RPLoader *lc = (RPLoader *)base;
  int rc = base->upstream->Next(base->upstream, r);
  if (rc != RS_RESULT_OK) {
    return rc;
  }

  // Current behavior skips entire result if document does not exist.
  // I'm unsure if that's intentional or an oversight.
  if (r->dmd == NULL || (r->dmd->flags & Document_Deleted)) {
    return RS_RESULT_OK;
  }

  lc->loadopts.sctx = lc->base.parent->sctx; // TODO: can be set in the constructor
  lc->loadopts.dmd = r->dmd;

  // if loading the document has failed, we return an empty array.
  // Error code and message are ignored.
  if (RLookup_LoadDocument(lc->lk, &r->rowdata, &lc->loadopts) != REDISMODULE_OK) {
    r->flags |= SEARCHRESULT_VAL_IS_NULL;
    QueryError_ClearError(&lc->status);
  }
  return RS_RESULT_OK;
}

static void rploaderFree(ResultProcessor *base) {
  RPLoader *lc = (RPLoader *)base;
  QueryError_ClearError(&lc->status);
  rm_free(lc->fields);
  rm_free(lc);
}

ResultProcessor *RPLoader_New(RLookup *lk, const RLookupKey **keys, size_t nkeys) {
  RPLoader *sc = rm_calloc(1, sizeof(*sc));
  sc->nfields = nkeys;
  sc->fields = rm_calloc(nkeys, sizeof(*sc->fields));
  memcpy(sc->fields, keys, sizeof(*keys) * nkeys);

  sc->loadopts.forceString = 1; // used in `LOAD_ALLKEYS` mode.
  sc->loadopts.forceLoad = 1;   // used in `LOAD_ALLKEYS` mode. TODO: use only with JSON specs and DIALECT<3
  sc->loadopts.status = &sc->status;
  sc->loadopts.sctx = NULL; // TODO: can be set in the constructor
  sc->loadopts.dmd = NULL;
  sc->loadopts.keys = sc->fields;
  sc->loadopts.nkeys = sc->nfields;
  if (nkeys) {
    sc->loadopts.mode = RLOOKUP_LOAD_KEYLIST;
  } else {
    sc->loadopts.mode = RLOOKUP_LOAD_ALLKEYS;
    lk->options |= RLOOKUP_OPT_ALL_LOADED; // TODO: turn on only for HASH specs
  }

  sc->lk = lk;
  sc->base.Next = rploaderNext;
  sc->base.Free = rploaderFree;
  sc->base.type = RP_LOADER;
  sc->base.behavior = RESULT_PROCESSOR_B_ACCESS_REDIS;
  return &sc->base;
}

static char *RPTypeLookup[RP_MAX] = {"Index",       "Loader",  "Buffer and Locker", "Unlocker",
                                     "Scorer",      "Sorter",  "Counter",           "Pager/Limiter",
                                     "Highlighter", "Grouper", "Projector",         "Filter",
                                     "Profile",     "Network", "Metrics Applier"};

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
  double profileTime;
  uint64_t profileCount;
} RPProfile;

static int rpprofileNext(ResultProcessor *base, SearchResult *r) {
  RPProfile *self = (RPProfile *)base;

  hires_clock_t t0;
  hires_clock_get(&t0);
  int rc = base->upstream->Next(base->upstream, r);
  self->profileTime += hires_clock_since_msec(&t0);
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

double RPProfile_GetDurationMSec(ResultProcessor *rp) {
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

/*******************************************************************************************************************
 *  Buffer and Locker Results Processor
 *
 * This component should be added to the query's execution pipeline if a thread safe access to
 * Redis keyspace is required.
 *
 * The buffer is responsible for buffering the document that pass the query filters and lock the access
 * to Redis keyspace to allow the downstream result processor a thread safe access to it.
 *
 * Unlocking Redis should be done only by the Unlocker result processor.
 *******************************************************************************************************************/

struct RPBufferAndLocker{
  ResultProcessor base;

  // Buffer management
  SearchResult **BufferBlocks;
  size_t BlockSize;
  size_t buffer_results_count;

  // Results iterator
  size_t curr_result_index;

  // Redis's lock status
  bool isRedisLocked;

  // Spec version before unlocking the spec.
  size_t spec_version;
};
/*********** Buffered and locker functions declarations ***********/

// Destroy
static void RPBufferAndLocker_Free(ResultProcessor *base);

// Buffer phase
static int rpbufferNext_bufferDocs(ResultProcessor *rp, SearchResult *res);

// Yeild results phase functions
static int rpbufferNext_Yield(ResultProcessor *rp, SearchResult *result_output);
static int rpbufferNext_ValidateAndYield(ResultProcessor *rp, SearchResult *result_output);
static bool isResultValid(const SearchResult *res);

// Redis lock management
static bool isRedisLocked(RPBufferAndLocker *bufferAndLocker);
static void LockRedis(RPBufferAndLocker *rpBufferAndLocker, RedisModuleCtx* redisCtx);
static void UnLockRedis(RPBufferAndLocker *rpBufferAndLocker, RedisModuleCtx* redisCtx);

/*********** Buffered results blocks management functions declarations ***********/
static SearchResult *NewResultsBlock(RPBufferAndLocker *rpPufferAndLocker);

// Insert result to the buffer.
//If @param CurrBlock is full we add a new block and return it, otherwise returns @param CurrBlock.
static SearchResult *InsertResult(RPBufferAndLocker *rpPufferAndLocker, SearchResult *resToBuffer, SearchResult *CurrBlock);
static bool IsBufferEmpty(RPBufferAndLocker *rpPufferAndLocker);

static SearchResult *GetNextResult(RPBufferAndLocker *rpPufferAndLocker);
/*******************************************************************************/

ResultProcessor *RPBufferAndLocker_New(size_t BlockSize, size_t spec_version) {
  RPBufferAndLocker *ret = rm_calloc(1, sizeof(RPBufferAndLocker));

  ret->base.Next = rpbufferNext_bufferDocs;
  ret->base.Free = RPBufferAndLocker_Free;
  ret->base.type = RP_BUFFER_AND_LOCKER;

  // Initialize the blocks' array to contain memory for one block pointer.
  ret->BufferBlocks = array_new(SearchResult *, 1);
  ret->BlockSize = BlockSize;

  ret->buffer_results_count = 0;
  ret->curr_result_index = 0;

  ret->isRedisLocked = false;

  ret->spec_version = spec_version;
  return &ret->base;
}


void RPBufferAndLocker_Free(ResultProcessor *base) {
  RPBufferAndLocker *bufferAndLocker = (RPBufferAndLocker *)base;

  assert(!isRedisLocked(bufferAndLocker));

  // Free buffer memory blocks
  array_foreach(bufferAndLocker->BufferBlocks, SearchResultsBlock, array_free(SearchResultsBlock));
  array_free(bufferAndLocker->BufferBlocks);

  rm_free(bufferAndLocker);
}

int rpbufferNext_bufferDocs(ResultProcessor *rp, SearchResult *res) {
  RPBufferAndLocker *rpPufferAndLocker = (RPBufferAndLocker *)rp;

  // Keep fetching results from the upstream result processor until EOF is reached
  int result_status;
  SearchResult resToBuffer = {0};
  SearchResult *CurrBlock = NULL;
  // Get the next result and save it in the buffer
  while((result_status = rp->upstream->Next(rp->upstream, &resToBuffer)) == RS_RESULT_OK) {

    // Buffer the result.
    CurrBlock = InsertResult(rpPufferAndLocker, &resToBuffer, CurrBlock);

    memset(&resToBuffer, 0, sizeof(SearchResult));

  }

  // If we exit the loop because we got an error, or we have zero result, return without locking Redis.
  if ((result_status != RS_RESULT_EOF &&
      !(result_status == RS_RESULT_TIMEDOUT && rp->parent->timeoutPolicy == TimeoutPolicy_Return)) ||
      IsBufferEmpty(rpPufferAndLocker)) {
    return result_status;
  }

  // Now we have the data of all documents that pass the query filters,
  // let's lock Redis to provide safe access to Redis keyspace

  RedisSearchCtx *Sctx = RP_SCTX(rp);

  // Lock Redis to gurentee safe access to Redis keyspace
  LockRedis(rpPufferAndLocker, Sctx->redisCtx);

  // If the spec has been changed since we released the spec lock,
  // we need to validate every buffered result
  if (rpPufferAndLocker->spec_version != IndexSpec_GetVersion(Sctx->spec)) {
    rp->Next = rpbufferNext_ValidateAndYield;
  } else { // Else we just return the results one by one
    rp->Next = rpbufferNext_Yield;
  }

  // We don't lock the index spec because we assume that there
  // are no more access to the index down the pipeline and the data
  // we buffered remains valid.
  return rp->Next(rp, res);
}

bool isResultValid(const SearchResult *res) {
  // check if the doc is not marked deleted in the spec
  return !(res->dmd->flags & Document_Deleted);
}
static void InvalidateBufferedResult(SearchResult *buffered_result) {
  buffered_result->indexResult = NULL;
  buffered_result->dmd = NULL;
  buffered_result->scoreExplain = NULL;
  memset(&buffered_result->rowdata, 0, sizeof(RLookupRow));
}
static void SetResult(SearchResult *buffered_result,  SearchResult *result_output) {
  // Free the RLookup row before overriding it.
  RLookupRow_Cleanup(&result_output->rowdata);
  *result_output = *buffered_result;

  InvalidateBufferedResult(buffered_result);
}
/*********** Redis lock management ***********/
bool isRedisLocked(RPBufferAndLocker *bufferAndLocker) {
  return bufferAndLocker->isRedisLocked;
}

void LockRedis(RPBufferAndLocker *rpBufferAndLocker, RedisModuleCtx* redisCtx) {
  RedisModule_ThreadSafeContextLock(redisCtx);

  rpBufferAndLocker->isRedisLocked = true;
}

void UnLockRedis(RPBufferAndLocker *rpBufferAndLocker, RedisModuleCtx* redisCtx) {
  RedisModule_ThreadSafeContextUnlock(redisCtx);

  rpBufferAndLocker->isRedisLocked = false;
}
/*********** Yeild results phase functions ***********/

int rpbufferNext_Yield(ResultProcessor *rp, SearchResult *result_output) {
  RPBufferAndLocker *RPBuffer = (RPBufferAndLocker *)rp;
  SearchResult *curr_res = GetNextResult(RPBuffer);

  if(!curr_res) {
    return RS_RESULT_EOF;
  }
 SetResult(curr_res, result_output);
 return RS_RESULT_OK;

}

int rpbufferNext_ValidateAndYield(ResultProcessor *rp, SearchResult *result_output) {
  RPBufferAndLocker *RPBuffer = (RPBufferAndLocker *)rp;
  SearchResult *curr_res;

  // iterate the buffer.
  while((curr_res = GetNextResult(RPBuffer))) {
    // Skip invalid results
    if (isResultValid(curr_res)) {
      SetResult(curr_res, result_output);
      return RS_RESULT_OK;
    }

    // If the result is invalid discard it.
    SearchResult_Destroy(curr_res);
    rp->parent->totalResults--;

  }

  return RS_RESULT_EOF;
}

/*********** Buffered and locker functions ***********/
SearchResult *NewResultsBlock(RPBufferAndLocker *rpPufferAndLocker) {
  // Get new results block
  SearchResult *ret = array_new(SearchResult, rpPufferAndLocker->BlockSize);
  // Append the block to the blocks array and update the pointer that might have benn
  // reallocated.
  rpPufferAndLocker->BufferBlocks = array_ensure_append_1(rpPufferAndLocker->BufferBlocks, ret);
  return ret;

}

// If @param CurrBlock is full we add a new block and return it, otherwise returns @param CurrBlock.
SearchResult *InsertResult(RPBufferAndLocker *rpPufferAndLocker, SearchResult *resToBuffer, SearchResult *CurrBlock) {
  size_t idx_in_curr_block = rpPufferAndLocker->buffer_results_count % rpPufferAndLocker->BlockSize;
  // if the block is full, allocate a new one
  if(idx_in_curr_block  == 0) {
    // alocate new block
    CurrBlock = NewResultsBlock(rpPufferAndLocker);

  }
  // append the result to the current block at rp->curr_idx_at_blocck
  // this operation takes ownership of the result's allocated data
  CurrBlock[idx_in_curr_block] = *resToBuffer;
  ++rpPufferAndLocker->buffer_results_count;
  return CurrBlock;
}

bool IsBufferEmpty(RPBufferAndLocker *rpPufferAndLocker) {
  return rpPufferAndLocker->buffer_results_count == 0;
}

SearchResult *GetNextResult(RPBufferAndLocker *rpPufferAndLocker) {
  size_t curr_elem_index = rpPufferAndLocker->curr_result_index;
  size_t blockSize = rpPufferAndLocker->BlockSize;

  assert(curr_elem_index <= rpPufferAndLocker->buffer_results_count);

  // if we reached to the end of the buffer return NULL
  if(curr_elem_index == rpPufferAndLocker->buffer_results_count) {
    return NULL;
  }

  // get current block
  SearchResult *curr_block = rpPufferAndLocker->BufferBlocks[curr_elem_index / blockSize];

  // get the result in the block
  SearchResult* ret = curr_block + (curr_elem_index % blockSize);


  // Increase result's index
  ++rpPufferAndLocker->curr_result_index;

  // return result
  return ret;
}

/*******************************************************************************************************************
 *  UnLocker Results Processor
 *
 * This component should be added to the query's execution pipeline if a thread safe access to
 * Redis keyspace is required.
 *
 * @param rpBufferAndLocker is a pointer to the buffer and locker result processor
 * that locked the GIL to be released.
 *
 * It is responsible for unlocking Redis keyspace lock.
 *
 *******************************************************************************************************************/

typedef struct {
  ResultProcessor base;
  RPBufferAndLocker* rpBufferAndLocker;

} RPUnlocker;

static int RPUnlocker_Next(ResultProcessor *rp, SearchResult *res) {
  // call the next result processor
  int result_status = rp->upstream->Next(rp->upstream, res);

  // Finish the search
  if(result_status != REDISMODULE_OK) {
    RPUnlocker *unlocker = (RPUnlocker *)rp;

    // Unlock Redis if it was locked
    if(isRedisLocked(unlocker->rpBufferAndLocker)){
      UnLockRedis(unlocker->rpBufferAndLocker, unlocker->base.parent->sctx->redisCtx);
    }

  }
  return result_status;
}

static void RPUnlocker_Free(ResultProcessor *base) {
  RPUnlocker *unlocker = (RPUnlocker *)base;
  assert(!isRedisLocked(unlocker->rpBufferAndLocker));

  rm_free(base);
}

ResultProcessor *RPUnlocker_New(RPBufferAndLocker *rpBufferAndLocker) {
  RPUnlocker *ret = rm_calloc(1, sizeof(RPUnlocker));

  ret->base.Next = RPUnlocker_Next;
  ret->base.Free = RPUnlocker_Free;
  ret->base.type = RP_UNLOCKER;

  ret->rpBufferAndLocker = rpBufferAndLocker;
  return &ret->base;
}
