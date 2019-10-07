#include "result_processor.h"
#include "query.h"
#include "extension.h"
#include <util/minmax_heap.h>
#include "ext/default.h"

/*******************************************************************************************************************
 *  General Result Processor Helper functions
 ********************************************************************************************************************/

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
    DMD_Decref(r->dmd);
    r->dmd = NULL;
  }
}

/* Free the search result object including the object itself */
void SearchResult_Destroy(SearchResult *r) {
  SearchResult_Clear(r);
  RLookupRow_Cleanup(&r->rowdata);
}

static int RPGeneric_NextEOF(ResultProcessor *rp, SearchResult *res) {
  return RS_RESULT_EOF;
}

/*******************************************************************************************************************
 *  Base Result Processor - this processor is the topmost processor of every processing chain.
 *
 * It takes the raw index results from the index, and builds the search result to be sent
 * downstream.
 ********************************************************************************************************************/

typedef struct {
  ResultProcessor base;
  IndexIterator *iiter;
} RPIndexIterator;

/* Next implementation */
static int rpidxNext(ResultProcessor *base, SearchResult *res) {
  RPIndexIterator *self = (RPIndexIterator *)base;
  IndexIterator *it = self->iiter;

  // No root filter - the query has 0 results
  if (self->iiter == NULL) {
    return RS_RESULT_EOF;
  }

  RSIndexResult *r;
  RSDocumentMetadata *dmd;
  int rc;

  // Read from the root filter until we have a valid result
  while (1) {
    rc = it->Read(it->ctx, &r);
    // This means we are done!
    if (rc == INDEXREAD_EOF) {
      return RS_RESULT_EOF;
    } else if (!r || rc == INDEXREAD_NOTFOUND) {
      continue;
    }

    dmd = DocTable_Get(&RP_SPEC(base)->docs, r->docId);
    if (!dmd || (dmd->flags & Document_Deleted)) {
      continue;
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
  DMD_Incref(dmd);
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
  ret->base.name = "Index";
  return &ret->base;
}

IndexIterator *QITR_GetRootFilter(QueryIterator *it) {
  return ((RPIndexIterator *)it->rootProc)->iiter;
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
 ********************************************************************************************************************/

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
  ret->base.name = "Scorer";
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
 * while
 * finding the top N results
 ********************************************************************************************************************/

typedef int (*RPSorterCompareFunc)(const void *e1, const void *e2, const void *udata);

typedef struct {
  ResultProcessor base;

  // The desired size of the heap - top N results
  // If set to 0 this is a growing heap
  uint32_t size;

  // The offset - used when popping result after we're done
  uint32_t offset;

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

} RPSorter;

/* Yield - pops the current top result from the heap */
static int rpsortNext_Yield(ResultProcessor *rp, SearchResult *r) {
  RPSorter *self = (RPSorter *)rp;
  // make sure we don't overshoot the heap size, unless the heap size is dynamic
  if (self->pq->count > 0 && (!self->size || self->offset++ < self->size)) {
    SearchResult *sr = mmh_pop_max(self->pq);
    RLookupRow oldrow = r->rowdata;
    *r = *sr;

    rm_free(sr);
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
  if (rc == RS_RESULT_EOF) {
    // Transition state:
    rp->Next = rpsortNext_Yield;
    return rpsortNext_Yield(rp, r);
  } else if (rc != RS_RESULT_OK) {
    // whoops!
    return rc;
  }

  // If the queue is not full - we just push the result into it
  // If the pool size is 0 we always do that, letting the heap grow dynamically
  if (!self->size || self->pq->count + 1 < self->pq->size) {

    // copy the index result to make it thread safe - but only if it is pushed to the heap
    h->indexResult = NULL;
    mmh_insert(self->pq, h);
    self->pooledResult = NULL;
    if (h->score < rp->parent->minScore) {
      rp->parent->minScore = h->score;
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
      self->pooledResult = mmh_pop_min(self->pq);
      mmh_insert(self->pq, h);
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
  return h1->docId < h2->docId ? -1 : 1;
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
      int rc;
      if (v1) {
        rc = 1;
      } else if (v2) {
        rc = -1;
      } else {
        rc = h1->docId < h2->docId ? -1 : 1;
      }
      return ascending ? -rc : rc;
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
                                      uint64_t ascmap) {
  RPSorter *ret = rm_calloc(1, sizeof(*ret));
  ret->cmp = nkeys ? cmpByFields : cmpByScore;
  ret->cmpCtx = ret;
  ret->fieldcmp.ascendMap = ascmap;
  ret->fieldcmp.keys = keys;
  ret->fieldcmp.nkeys = nkeys;

  ret->pq = mmh_init_with_size(maxresults + 1, ret->cmp, ret->cmpCtx, srDtor);
  ret->size = maxresults;
  ret->offset = 0;
  ret->pooledResult = NULL;
  ret->base.Next = rpsortNext_Accum;
  ret->base.Free = rpsortFree;
  ret->base.name = "Sorter";
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
    return RS_RESULT_EOF;
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
  ret->base.name = "Pager/Limiter";
  ret->base.Next = rppagerNext;
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
  const RLookupKey **fields;
  size_t nfields;
} RPLoader;

static int rploaderNext(ResultProcessor *base, SearchResult *r) {
  RPLoader *lc = (RPLoader *)base;
  int rc = base->upstream->Next(base->upstream, r);
  if (rc != RS_RESULT_OK) {
    return rc;
  }

  int isExplicitReturn = !!lc->nfields;

  // Current behavior skips entire result if document does not exist.
  // I'm unusre if that's intentional or an oversight.
  if (r->dmd == NULL || (r->dmd->flags & Document_Deleted)) {
    return RS_RESULT_OK;
  }
  RedisSearchCtx *sctx = lc->base.parent->sctx;

  QueryError status = {0};
  RLookupLoadOptions loadopts = {.sctx = lc->base.parent->sctx,  // lb
                                 .dmd = r->dmd,
                                 .noSortables = 1,
                                 .status = &status,
                                 .keys = lc->fields,
                                 .nkeys = lc->nfields};
  if (isExplicitReturn) {
    loadopts.mode |= RLOOKUP_LOAD_KEYLIST;
  } else {
    loadopts.mode |= RLOOKUP_LOAD_ALLKEYS;
  }
  RLookup_LoadDocument(lc->lk, &r->rowdata, &loadopts);
  return RS_RESULT_OK;
}

static void rploaderFree(ResultProcessor *base) {
  RPLoader *lc = (RPLoader *)base;
  rm_free(lc->fields);
  rm_free(lc);
}

ResultProcessor *RPLoader_New(RLookup *lk, const RLookupKey **keys, size_t nkeys) {
  RPLoader *sc = rm_calloc(1, sizeof(*sc));
  sc->nfields = nkeys;
  sc->fields = rm_calloc(nkeys, sizeof(*sc->fields));
  memcpy(sc->fields, keys, sizeof(*keys) * nkeys);

  sc->lk = lk;
  sc->base.Next = rploaderNext;
  sc->base.Free = rploaderFree;
  sc->base.name = "Loader";
  return &sc->base;
}

void RP_DumpChain(const ResultProcessor *rp) {
  for (; rp; rp = rp->upstream) {
    printf("RP(%s) @%p\n", rp->name, rp);
    assert(rp->upstream != rp);
  }
}
