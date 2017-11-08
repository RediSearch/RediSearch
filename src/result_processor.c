#include "result_processor.h"
#include "query.h"
#include "extension.h"
#include "util/minmax_heap.h"
#include "ext/default.h"
#include "util/array.h"

/*******************************************************************************************************************
 *  General Result Processor Helper functions
 ********************************************************************************************************************/

/* Create a new result processor with a given upstream processor, and context private data */
ResultProcessor *NewResultProcessor(ResultProcessor *upstream, void *privdata) {
  ResultProcessor *p = calloc(1, sizeof(ResultProcessor));
  p->ctx = (ResultProcessorCtx){
      .privdata = privdata, .upstream = upstream, .qxc = upstream ? upstream->ctx.qxc : NULL,
  };

  return p;
}

/* Safely call Next on an upstream processor, putting the result into res. If allowSwitching is 1,
 * we check the concurrent context and perhaps switch if needed.
 *
 * Note 1: Do not call processors' Next() directly, ONLY USE THIS FUNCTION
 *
 * Note 2: this function will not return RS_RESULT_QUEUED, but only OK or EOF. Any queued events
 * will be handled by this function
 * */
inline int ResultProcessor_Next(ResultProcessor *rp, SearchResult *res, int allowSwitching) {
  int rc;
  ConcurrentSearchCtx *cxc = rp->ctx.qxc->conc;

  do {

    // If we can switch - we check the concurrent context switch BEFORE calling the upstream
    if (allowSwitching) {
      CONCURRENT_CTX_TICK(cxc);
      // need to abort - return EOF
      if (rp->ctx.qxc->state == QueryState_Aborted) {
        return RS_RESULT_EOF;
      }
    }
    rc = rp->Next(&rp->ctx, res);

  } while (rc == RS_RESULT_QUEUED);
  return rc;
}

/* Helper function - get the total from a processor, and if the Total callback is NULL, climb up
 * the
 * chain until we find a processor with a Total callback. This allows processors to avoid
 * implementing it if they have no calculations to add to Total (such as deeted/ignored results)
 * */
size_t ResultProcessor_Total(ResultProcessor *rp) {
  return rp->ctx.qxc->totalResults;
}

/* Free a result processor - recursively freeing its upstream as well. If the processor does not
 * implement Free - we just call free() on the processor object itself.
 *
 * Do NOT call Free() callbacks on processors directly! */
void ResultProcessor_Free(ResultProcessor *rp) {
  ResultProcessor *upstream = rp->ctx.upstream;
  if (rp->Free) {
    rp->Free(rp);
  } else {
    // For processors that did not bother to define a special Free - we just call free()
    free(rp);
  }
  // continue to the upstream processor
  if (upstream) ResultProcessor_Free(upstream);
}

SearchResult *NewSearchResult() {
  SearchResult *ret = calloc(1, sizeof(*ret));
  ret->fields = RS_NewFieldMap(4);
  ret->score = 0;
  return ret;
}

/* Free the search result's internal data but not the result itself - it may be allocated on the
 * stack */
inline void SearchResult_FreeInternal(SearchResult *r) {

  if (!r) return;
  // This won't affect anything if the result is null
  IndexResult_Free(r->indexResult);

  RSFieldMap_Free(r->fields, 0);
}

/* Free the search result object including the object itself */
void SearchResult_Free(void *p) {
  SearchResult_FreeInternal((SearchResult *)p);
  free(p);
}

/* Generic free function for result processors that just need to free their private data with free()
 */
void ResultProcessor_GenericFree(ResultProcessor *rp) {
  free(rp->ctx.privdata);
  free(rp);
}

/*******************************************************************************************************************
 *  Base Result Processor - this processor is the topmost processor of every processing chain.
 *
 * It takes the raw index results from the index, and builds the search result to be sent
 * downstream.
 ********************************************************************************************************************/

/* Next implementation */
int baseResultProcessor_Next(ResultProcessorCtx *ctx, SearchResult *res) {
  QueryPlan *q = ctx->privdata;

  // No root filter - the query has 0 results
  if (q->rootFilter == NULL) {
    return RS_RESULT_EOF;
  }
  // if we've timed out - abort the root processor and return EOF
  if (q->execCtx.state == QueryState_TimedOut) {
    q->rootFilter->Abort(q->rootFilter->ctx);
    return RS_RESULT_EOF;
  }
  RSIndexResult *r;
  RSDocumentMetadata *dmd;
  int rc;
  // Read from the root filter until we have a valid result
  do {
    rc = q->rootFilter->Read(q->rootFilter->ctx, &r);

    // This means we are done!
    if (rc == INDEXREAD_EOF) {
      return RS_RESULT_EOF;
    } else if (!r || rc == INDEXREAD_NOTFOUND) {
      continue;
    }

    dmd = DocTable_Get(&RP_SPEC(ctx)->docs, r->docId);

    // skip deleted documents
    if (!dmd || (dmd->flags & Document_Deleted)) {
      continue;
    }

    // Increment the total results barring deleted results
    ++ctx->qxc->totalResults;
    // valid result!

    break;
  } while (1);

  // set the result data
  res->docId = r->docId;

  // the index result of the search result is not thread safe. It will be copied by the sorter later
  // on if we need it to be thread safe
  res->indexResult = r;
  res->score = 0;
  res->sv = dmd->sortVector;
  res->md = dmd;

  return RS_RESULT_OK;
}

/* Createa a new base processor */
ResultProcessor *NewBaseProcessor(QueryPlan *q, QueryProcessingCtx *xc) {
  ResultProcessor *rp = NewResultProcessor(NULL, q);
  rp->ctx.qxc = xc;
  rp->Next = baseResultProcessor_Next;
  return rp;
}

/*******************************************************************************************************************
 *  Scoring Processor
 *
 * It takes results from upstream, and using a scoring function applies the score to each one.
 *
 * It may not be invoked if we are working in SORTBY mode (or later on in aggregations)
 ********************************************************************************************************************/

/* The scorer context - basically the scoring function and its private data */
struct scorerCtx {
  RSScoringFunction scorer;
  RSFreeFunction scorerFree;
  RSScoringFunctionCtx scorerCtx;
};

int scorerProcessor_Next(ResultProcessorCtx *ctx, SearchResult *res) {
  struct scorerCtx *sc = ctx->privdata;
  int rc;

  do {

    if (RS_RESULT_EOF == (rc = ResultProcessor_Next(ctx->upstream, res, 0))) return rc;

    // Apply the scoring function
    res->score = sc->scorer(&sc->scorerCtx, res->indexResult, res->md, ctx->qxc->minScore);

    // If we got the special score RS_SCORE_FILTEROUT - disregard the result and decrease the total
    // number of results (it's been increased by the upstream processor)
    if (res->score == RS_SCORE_FILTEROUT) ctx->qxc->totalResults--;

    break;
  } while (1);

  return rc;
}

/* Free impl. for scorer - frees up the scorer privdata if needed */
static void scorer_Free(ResultProcessor *rp) {
  struct scorerCtx *sc = rp->ctx.privdata;
  if (sc->scorerFree) {
    sc->scorerFree(sc->scorerCtx.privdata);
  }
  ResultProcessor_GenericFree(rp);
}

/* Create a new scorer by name. If the name is not found in the scorer registry, we use the defalt
 * scorer */
ResultProcessor *NewScorer(const char *scorer, ResultProcessor *upstream, RSSearchRequest *req) {
  struct scorerCtx *sc = malloc(sizeof(*sc));
  ExtScoringFunctionCtx *scx =
      Extensions_GetScoringFunction(&sc->scorerCtx, scorer ? scorer : DEFAULT_SCORER_NAME);
  if (!scx) {
    scx = Extensions_GetScoringFunction(&sc->scorerCtx, DEFAULT_SCORER_NAME);
  }

  sc->scorer = scx->sf;
  sc->scorerFree = scx->ff;
  sc->scorerCtx.payload = req->payload;
  // Initialize scorer stats
  IndexSpec_GetStats(req->sctx->spec, &sc->scorerCtx.indexStats);

  ResultProcessor *rp = NewResultProcessor(upstream, sc);
  rp->Next = scorerProcessor_Next;
  rp->Free = scorer_Free;
  return rp;
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

/* Sorter's private context */
struct sorterCtx {

  // The desired size of the heap - top N results
  uint32_t size;

  // The offset - used when popping result after we're done
  uint32_t offset;

  // The heap. We use a min-max heap here
  heap_t *pq;

  // the compare function for the heap. We use it to test if a result needs to be added to the heap
  int (*cmp)(const void *e1, const void *e2, const void *udata);
  // private data for the compare function
  void *cmpCtx;

  // pooled result - we recycle it to avoid allocations
  SearchResult *pooledResult;

  // accumulation state - while this is true, any call to next() will yield QUEUED
  int accumulating;

  int saveIndexResults;
};

/* Yield - pops the current top result from the heap */
int sorter_Yield(struct sorterCtx *sc, SearchResult *r) {

  // make sure we don't overshoot the heap size
  if (sc->pq->count > 0 && sc->offset++ < sc->size) {
    SearchResult *sr = mmh_pop_max(sc->pq);
    *r = *sr;
    free(sr);
    return RS_RESULT_OK;
  }
  return RS_RESULT_EOF;
}

void sorter_Free(ResultProcessor *rp) {
  struct sorterCtx *sc = rp->ctx.privdata;
  if (sc->pooledResult) {
    SearchResult_Free(sc->pooledResult);
  }
  // calling mmh_free will free all the remaining results in the heap, if any
  mmh_free(sc->pq);
  free(sc);
  free(rp);
}

int sorter_Next(ResultProcessorCtx *ctx, SearchResult *r) {
  struct sorterCtx *sc = ctx->privdata;
  // if we're not accumulating anymore - yield the top result
  if (!sc->accumulating) {
    return sorter_Yield(sc, r);
  }

  if (sc->pooledResult == NULL) {
    sc->pooledResult = NewSearchResult();
  } else {
    sc->pooledResult->fields->len = 0;
  }
  SearchResult *h = sc->pooledResult;

  int rc = ResultProcessor_Next(ctx->upstream, h, 0);
  // if our upstream has finished - just change the state to not accumulating, and yield
  if (rc == RS_RESULT_EOF) {
    sc->accumulating = 0;
    return sorter_Yield(sc, r);
  }

  // If the queue is not full - we just push the result into it
  if (sc->pq->count + 1 < sc->pq->size) {

    // copy the index result to make it thread safe - but only if it is pushed to the heap
    h->indexResult = NULL;
    mmh_insert(sc->pq, h);
    sc->pooledResult = NULL;
    if (h->score < ctx->qxc->minScore) {
      ctx->qxc->minScore = h->score;
    }

  } else {
    // find the min result
    SearchResult *minh = mmh_peek_min(sc->pq);

    // update the min score. Irrelevant to SORTBY mode but hardly costs anything...
    if (minh->score > ctx->qxc->minScore) {
      ctx->qxc->minScore = minh->score;
    }

    // if needed - pop it and insert a new result
    if (sc->cmp(h, minh, sc->cmpCtx) > 0) {
      // copy the index result to make it thread safe - but only if it is pushed to the heap
      h->indexResult = NULL;

      sc->pooledResult = mmh_pop_min(sc->pq);
      sc->pooledResult->indexResult = NULL;
      mmh_insert(sc->pq, h);
    } else {
      // The current should not enter the pool, so just leave it as is
      sc->pooledResult = h;
      // make sure we will not try to free the index result of the pooled result at the end
      sc->pooledResult->indexResult = NULL;
    }
  }

  return RS_RESULT_QUEUED;
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
static int cmpBySortKey(const void *e1, const void *e2, const void *udata) {
  const RSSortingKey *sk = udata;
  const SearchResult *h1 = e1, *h2 = e2;
  if (!h1->sv || !h2->sv) {
    return h1->docId < h2->docId ? -1 : 1;
  }
  return -RSSortingVector_Cmp(h1->sv, h2->sv, (RSSortingKey *)sk);
}

ResultProcessor *NewSorter(RSSortingKey *sk, uint32_t size, ResultProcessor *upstream,
                           int copyIndexResults) {

  struct sorterCtx *sc = malloc(sizeof(*sc));

  if (sk) {
    sc->cmp = cmpBySortKey;
    sc->cmpCtx = sk;
  } else {
    sc->cmp = cmpByScore;
    sc->cmpCtx = NULL;
  }

  sc->pq = mmh_init_with_size(size + 1, sc->cmp, sc->cmpCtx, SearchResult_Free);
  sc->size = size;
  sc->offset = 0;
  sc->pooledResult = NULL;
  sc->accumulating = 1;
  sc->saveIndexResults = copyIndexResults;

  ResultProcessor *rp = NewResultProcessor(upstream, sc);
  rp->Next = sorter_Next;
  rp->Free = sorter_Free;
  return rp;
}

/*******************************************************************************************************************
 *  Paging Processor
 *
 * The sorter builds a heap of size N, but the pager is responsible for taking result
 * FIRST...FIRST+NUM from it.
 *
 * For example, if we want to get results 40-50, we build a heap of size 50 on the sorter, and the
 * pager is responsible for discarding the first 40 results and returning just 10
 *
 * They are separated so that later on we can cache the sorter's heap, and continue paging it
 * without re-executing the entire query
 *******************************************************************************************************************/

struct pagerCtx {
  uint32_t offset;
  uint32_t limit;
  uint32_t count;
};

int pager_Next(ResultProcessorCtx *ctx, SearchResult *r) {
  struct pagerCtx *pc = ctx->privdata;

  int rc = ResultProcessor_Next(ctx->upstream, r, 1);
  // if our upstream has finished - just change the state to not accumulating, and yield
  if (rc == RS_RESULT_EOF) {
    return rc;
  }

  // not reached beginning of results
  if (pc->count < pc->offset) {

    IndexResult_Free(r->indexResult);
    free(r->fields);
    pc->count++;
    return RS_RESULT_QUEUED;
  }
  // overshoot the count
  if (pc->count >= pc->limit + pc->offset) {
    return RS_RESULT_EOF;
  }
  pc->count++;
  return RS_RESULT_OK;
}

/* Create a new pager. The offset and limit are taken from the user request */
ResultProcessor *NewPager(ResultProcessor *upstream, uint32_t offset, uint32_t limit) {
  struct pagerCtx *pc = malloc(sizeof(*pc));
  pc->offset = offset;
  pc->limit = limit;
  pc->count = 0;
  ResultProcessor *rp = NewResultProcessor(upstream, pc);

  rp->Next = pager_Next;
  // no need for a special free function
  rp->Free = ResultProcessor_GenericFree;
  return rp;
}

/*******************************************************************************************************************
 *  Loading Processor
 *
 * This processor simply takes the search results, and based on the request parameters, loads the
 * relevant fields for the results that need to be displayed to the user, from redis.
 *
 * It fills the result objects' field map with values corresponding to the requested return fields
 *
 *******************************************************************************************************************/
struct loaderCtx {
  RedisSearchCtx *ctx;
  FieldList *fields;
};

int loader_Next(ResultProcessorCtx *ctx, SearchResult *r) {
  struct loaderCtx *lc = ctx->privdata;

  int rc = ResultProcessor_Next(ctx->upstream, r, 1);
  // END - let's write the total processed size
  if (rc == RS_RESULT_EOF) {
    return rc;
  }
  Document doc = {NULL};
  RedisModuleKey *rkey = NULL;

  // Current behavior skips entire result if document does not exist.
  // I'm unusre if that's intentional or an oversight.
  RedisModuleString *idstr =
      RedisModule_CreateString(lc->ctx->redisCtx, r->md->key, strlen(r->md->key));
  if (!lc->fields->explicitReturn) {
    Redis_LoadDocument(lc->ctx, idstr, &doc);
  } else {
    Array fieldList;
    Array_Init(&fieldList);
    for (size_t ii = 0; ii < lc->fields->numFields; ++ii) {
      Array_Write(&fieldList, &lc->fields->fields[ii].name, sizeof(char *));
    }

    Redis_LoadDocumentEx(lc->ctx, idstr, (const char **)fieldList.data, lc->fields->numFields, &doc,
                         &rkey);
    RedisModule_FreeString(lc->ctx->redisCtx, idstr);
    Array_Free(&fieldList);
  }
  // TODO: load should return strings, not redis strings
  for (int i = 0; i < doc.numFields; i++) {
    RSFieldMap_Add(&r->fields, doc.fields[i].name,
                   doc.fields[i].text ? RS_RedisStringVal(doc.fields[i].text) : RS_NullVal());
  }
  Document_Free(&doc);

  return RS_RESULT_OK;
}

ResultProcessor *NewLoader(ResultProcessor *upstream, RSSearchRequest *r) {
  struct loaderCtx *sc = malloc(sizeof(*sc));
  sc->ctx = r->sctx;
  sc->fields = &r->fields;

  ResultProcessor *rp = NewResultProcessor(upstream, sc);

  rp->Next = loader_Next;
  rp->Free = ResultProcessor_GenericFree;
  return rp;
}

/*******************************************************************************************************************
 * Building the processor chaing based on the processors available and the request parameters
 *******************************************************************************************************************/

ResultProcessor *Query_BuildProcessorChain(QueryPlan *q, RSSearchRequest *req) {

  // The base processor translates index results into search results
  ResultProcessor *next = NewBaseProcessor(q, &q->execCtx);

  // If we are not in SORTBY mode - add a scorer to the chain
  if (req->sortBy == NULL) {
    next = NewScorer(req->scorer, next, req);
  }

  // The sorter sorts the top-N results
  next = NewSorter(req->sortBy, req->offset + req->num, next, req->fields.wantSummaries);

  // The pager pages over the results of the sorter
  next = NewPager(next, req->offset, req->num);

  // The loader loads the documents from redis
  // If we do not need to return any fields - we do not need the loader in the loop
  if (!(req->flags & Search_NoContent)) {
    next = NewLoader(next, req);
    if (req->fields.wantSummaries && (req->sctx->spec->flags & Index_StoreTermOffsets) != 0) {
      next = NewHighlightProcessor(next, req);
    }
  }

  return next;
}