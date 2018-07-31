#include "result_processor.h"
#include "query.h"
#include "extension.h"
#include "util/minmax_heap.h"
#include "ext/default.h"
#include "query_plan.h"
#include "highlight.h"

/*******************************************************************************************************************
 *  General Result Processor Helper functions
 ********************************************************************************************************************/

/* Create a new result processor with a given upstream processor, and context private data */
ResultProcessor *NewResultProcessor(ResultProcessor *upstream, void *privdata) {
  ResultProcessor *p = calloc(1, sizeof(ResultProcessor));
  p->ctx = (ResultProcessorCtx){
      .privdata = privdata,
      .upstream = upstream,
      .qxc = upstream ? upstream->ctx.qxc : NULL,
  };

  return p;
}

/* Helper function - get the total from a processor, and if the Total callback is NULL, climb up
 * the
 * chain until we find a processor with a Total callback. This allows processors to avoid
 * implementing it if they have no calculations to add to Total (such as deeted/ignored results)
 * */
size_t ResultProcessor_Total(ResultProcessor *rp) {
  return rp->ctx.qxc ? rp->ctx.qxc->totalResults : 0;
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
void SearchResult_FreeInternal(SearchResult *r) {

  if (!r) return;
  // This won't affect anything if the result is null
  if (r->indexResult) {
    // IndexResult_Free(r->indexResult);
    r->indexResult = NULL;
  }
  if (r->fields) {
    RSFieldMap_Free(r->fields);
    r->fields = NULL;
  }
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
  if (q->execCtx.state == QPState_TimedOut) {
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

    // printf("%d => '%s'\n", r->docId, dmd->key);
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
  res->indexResult = r;  // q->opts.needIndexResult ? r : NULL;

  res->score = 0;
  res->sorterPrivateData = dmd->sortVector;
  res->scorerPrivateData = dmd;
  if (res->fields != NULL) {
    res->fields->len = 0;
  }

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
    res->score =
        sc->scorer(&sc->scorerCtx, res->indexResult, res->scorerPrivateData, ctx->qxc->minScore);

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
  IndexSpec_GetStats(upstream->ctx.qxc->sctx->spec, &sc->scorerCtx.indexStats);

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

typedef enum {
  Sort_ByScore,
  Sort_BySortKey,
  Sort_ByFields,
} SortMode;
/* Sorter's private context */
struct sorterCtx {

  // The desired size of the heap - top N results
  // If set to 0 this is a growing heap
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

  SortMode sortMode;
};

struct fieldCmpCtx {
  RSMultiKey *keys;

  // a bitmap where each bit corresponds to a variable in the keymap, specifying ascending (1) or
  // descending (0)
  uint64_t ascendMap;
};

/* Yield - pops the current top result from the heap */
int sorter_Yield(struct sorterCtx *sc, SearchResult *r) {

  // make sure we don't overshoot the heap size, unless the heap size is dynamic
  if (sc->pq->count > 0 && (!sc->size || sc->offset++ < sc->size)) {
    SearchResult *sr = mmh_pop_max(sc->pq);
    *r = *sr;
    DMD_Decref(r->scorerPrivateData);
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
  if (sc->cmpCtx) {
    if (sc->sortMode == Sort_ByFields) {
      struct fieldCmpCtx *fcc = sc->cmpCtx;
      RSMultiKey_Free(fcc->keys);
      free(fcc);
    }
  }

  // calling mmh_free will free all the remaining results in the heap, if any
  mmh_free(sc->pq);
  free(sc);
  free(rp);
}

static void keepResult(struct sorterCtx *sctx, SearchResult *r) {
  DMD_Incref(r->scorerPrivateData);
  if (sctx->sortMode == Sort_ByFields && r->fields) {
    for (size_t ii = 0; ii < r->fields->len; ++ii) {
      r->fields->fields[ii].val = RSValue_MakePersistent(r->fields->fields[ii].val);
      r->fields->fields[ii].key = strdup(r->fields->fields[ii].key);
      r->fields->isKeyAlloc = 1;
    }
  }
}

int sorter_Next(ResultProcessorCtx *ctx, SearchResult *r) {
  struct sorterCtx *sc = ctx->privdata;
  // if we're not accumulating anymore - yield the top result
  if (!sc->accumulating) {
    return sorter_Yield(sc, r);
  }

  if (sc->pooledResult == NULL) {
    sc->pooledResult = NewSearchResult();
  } else if (sc->pooledResult->fields) {
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
  // If the pool size is 0 we always do that, letting the heap grow dynamically
  if (!sc->size || sc->pq->count + 1 < sc->pq->size) {

    // copy the index result to make it thread safe - but only if it is pushed to the heap
    h->indexResult = NULL;

    keepResult(sc, h);
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
      SearchResult_FreeInternal(sc->pooledResult);

      keepResult(sc, h);
      mmh_insert(sc->pq, h);
    } else {
      // The current should not enter the pool, so just leave it as is
      h->indexResult = NULL;
      sc->pooledResult = h;
      // make sure we will not try to free the index result of the pooled result at the end
      SearchResult_FreeInternal(sc->pooledResult);
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
  if (!h1->sorterPrivateData || !h2->sorterPrivateData) {
    return h1->docId < h2->docId ? -1 : 1;
  }
  return -RSSortingVector_Cmp(h1->sorterPrivateData, h2->sorterPrivateData, (RSSortingKey *)sk);
}

/* Compare results for the heap by sorting key */
static int cmpByFields(const void *e1, const void *e2, const void *udata) {
  const struct fieldCmpCtx *cc = udata;

  const SearchResult *h1 = e1, *h2 = e2;
  int ascending = 0;

  for (size_t i = 0; i < cc->keys->len && i < sizeof(cc->ascendMap) * 8; i++) {
    RSValue *v1 = RSFieldMap_GetByKey(h1->fields, &cc->keys->keys[i]);
    RSValue *v2 = RSFieldMap_GetByKey(h2->fields, &cc->keys->keys[i]);
    if (!v1 || !v2) {
      break;
    }

    int rc = RSValue_Cmp(v1, v2);
    // take the ascending bit for this property from the ascending bitmap
    ascending = cc->ascendMap & (1 << i) ? 1 : 0;
    if (rc != 0) return ascending ? -rc : rc;
  }

  int rc = h1->docId < h2->docId ? -1 : 1;
  return ascending ? -rc : rc;
}

ResultProcessor *NewSorter(SortMode sortMode, void *sortCtx, uint32_t size,
                           ResultProcessor *upstream, int copyIndexResults) {

  struct sorterCtx *sc = malloc(sizeof(*sc));
  // select the sorting function by the sort mode
  switch (sortMode) {
    case Sort_ByScore:
      sc->cmp = cmpByScore;
      break;
    case Sort_ByFields:
      sc->cmp = cmpByFields;
      break;
    case Sort_BySortKey:
      sc->cmp = cmpBySortKey;
      break;
  }
  sc->cmpCtx = sortCtx;

  sc->pq = mmh_init_with_size(size + 1, sc->cmp, sc->cmpCtx, SearchResult_Free);
  sc->size = size;
  sc->offset = 0;
  sc->pooledResult = NULL;
  sc->accumulating = 1;
  sc->saveIndexResults = copyIndexResults;
  sc->sortMode = sortMode;

  ResultProcessor *rp = NewResultProcessor(upstream, sc);
  rp->Next = sorter_Next;
  rp->Free = sorter_Free;
  return rp;
}

ResultProcessor *NewSorterByFields(RSMultiKey *mk, uint64_t ascendingMap, uint32_t size,
                                   ResultProcessor *upstream) {
  struct fieldCmpCtx *c = malloc(sizeof(*c));
  c->ascendMap = ascendingMap;
  c->keys = mk;

  return NewSorter(Sort_ByFields, c, size, upstream, 0);
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

    // IndexResult_Free(r->indexResult);
    RSFieldMap_Free(r->fields);
    r->fields = NULL;

    pc->count++;
    return RS_RESULT_QUEUED;
  }
  // overshoot the count
  if (pc->count >= pc->limit + pc->offset) {
    // IndexResult_Free(r->indexResult);
    RSFieldMap_Free(r->fields);
    r->fields = NULL;
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
typedef struct {
  const char *name;  // Key to use on output
  int sortIndex;     // If sortable, sort index; otherwise -1
  int type;          // Type, if in field spec, otherwise -1
} LoadedField;

struct loaderCtx {
  RedisSearchCtx *ctx;
  LoadedField *fields;
  size_t numFields;
  int explicitReturn;
};

static RSValue *getValueFromField(RedisModuleString *origval, int typeCode) {
  switch (typeCode) {
    case FIELD_NUMERIC: {
      double d;
      if (RedisModule_StringToDouble(origval, &d) == REDISMODULE_OK) {
        return RS_NumVal(d);
        break;
      }
    }
    case FIELD_FULLTEXT:
    case FIELD_TAG:
    case FIELD_GEO:
    default:
      return RS_RedisStringVal(origval);
      // Just store as a string
  }
}

static void loadExplicitFields(struct loaderCtx *lc, RedisSearchCtx *sctx, RedisModuleString *idstr,
                               const RSDocumentMetadata *dmd, SearchResult *r) {

  RedisModuleKey *k = NULL;
  int triedOpen = 0;

  for (size_t ii = 0; ii < lc->numFields; ++ii) {
    const LoadedField *field = lc->fields + ii;

    // NOTE: Text fulltext fields are normalized
    if (field->type == FIELD_NUMERIC && field->sortIndex > -1 && dmd->sortVector) {
      RSSortingKey k = {.index = field->sortIndex};
      RSValue *v = RSSortingVector_Get(dmd->sortVector, &k);
      if (v) {
        RSFieldMap_Set(&r->fields, field->name, RSValue_IncrRef(v));
        continue;
      }
    }
    // Otherwise, we need to load from the fieldspec
    if (triedOpen && !k) {
      continue;  // not gonna open the key
    }
    k = RedisModule_OpenKey(sctx->redisCtx, idstr, REDISMODULE_READ);
    triedOpen = 1;
    if (!k || RedisModule_KeyType(k) != REDISMODULE_KEYTYPE_HASH) {
      continue;
    }

    // Try to get the field
    RedisModuleString *v = NULL;
    int rv = RedisModule_HashGet(k, REDISMODULE_HASH_CFIELDS, field->name, &v, NULL);
    if (rv == REDISMODULE_OK && v) {
      RSFieldMap_Set(&r->fields, field->name, getValueFromField(v, field->type));
    } else {
      RSFieldMap_Set(&r->fields, field->name, RS_NullVal());
    }
  }

  if (k) {
    RedisModule_CloseKey(k);
  }
}

int loader_Next(ResultProcessorCtx *ctx, SearchResult *r) {
  struct loaderCtx *lc = ctx->privdata;

  int rc = ResultProcessor_Next(ctx->upstream, r, 1);
  // END - let's write the total processed size
  if (rc == RS_RESULT_EOF) {
    return rc;
  }
  Document doc = {NULL};

  // Current behavior skips entire result if document does not exist.
  // I'm unusre if that's intentional or an oversight.
  const RSDocumentMetadata *dmd = DocTable_Get(&lc->ctx->spec->docs, r->docId);
  if (dmd == NULL || (dmd->flags & Document_Deleted)) {
    return RS_RESULT_OK;
  }

  RedisModuleString *idstr = DMD_CreateKeyString(dmd, lc->ctx->redisCtx);

  if (!lc->explicitReturn) {
    Redis_LoadDocument(lc->ctx, idstr, &doc);
    for (int i = 0; i < doc.numFields; i++) {
      if (doc.fields[i].text) {
        RSFieldMap_Set(&r->fields, doc.fields[i].name, RS_RedisStringVal(doc.fields[i].text));
      } else {
        RSFieldMap_Set(&r->fields, doc.fields[i].name, RS_NullVal());
      }
    }
  } else {
    // Figure out if we need to load the document at all; or maybe a simple
    // load from sortables is sufficient?
    loadExplicitFields(lc, lc->ctx, idstr, dmd, r);
  }

  // TODO: load should return strings, not redis strings
  RedisModule_FreeString(lc->ctx->redisCtx, idstr);
  Document_Free(&doc);

  return RS_RESULT_OK;
}

void loader_Free(ResultProcessor *rp) {
  struct loaderCtx *lc = rp->ctx.privdata;
  free(lc->fields);
  free(lc);
  free(rp);
}
ResultProcessor *NewLoader(ResultProcessor *upstream, RedisSearchCtx *sctx, FieldList *fields) {
  struct loaderCtx *sc = malloc(sizeof(*sc));

  sc->ctx = sctx;
  sc->numFields = fields->numFields;
  sc->fields = calloc(fields->numFields, sizeof(*sc->fields));

  for (size_t ii = 0; ii < fields->numFields; ++ii) {
    const char *name = fields->fields[ii].name;
    LoadedField *lf = sc->fields + ii;
    lf->name = name;
    // Find the fieldspec
    const FieldSpec *fs = IndexSpec_GetField(sctx->spec, name, strlen(name));
    if (fs) {
      lf->type = fs->type;
      if (FieldSpec_IsSortable(fs)) {
        lf->sortIndex = fs->sortIdx;
      } else {
        lf->sortIndex = -1;
      }
    } else {
      lf->sortIndex = -1;
      lf->type = -1;
    }
  }
  sc->explicitReturn = fields->explicitReturn;

  ResultProcessor *rp = NewResultProcessor(upstream, sc);

  rp->Next = loader_Next;
  rp->Free = loader_Free;
  return rp;
}

/*******************************************************************************************************************
 * Building the processor chaing based on the processors available and the request parameters
 *******************************************************************************************************************/
ResultProcessor *Query_BuildProcessorChain(QueryPlan *q, void *privdata, QueryError *status) {
  RSSearchRequest *req = privdata;
  // The base processor translates index results into search results
  ResultProcessor *next = NewBaseProcessor(q, &q->execCtx);

  // If we are not in SORTBY mode - add a scorer to the chain
  if (q->opts.sortBy == NULL) {
    next = NewScorer(q->opts.scorer, next, req);
    // Scorers usually need the index results, let's tell the query plan that
    q->opts.needIndexResult = 1;
  }

  // The sorter sorts the top-N results
  next = NewSorter(q->opts.sortBy ? Sort_BySortKey : Sort_ByScore, q->opts.sortBy,
                   q->opts.offset + q->opts.num, next, req->opts.fields.wantSummaries);

  // The pager pages over the results of the sorter
  next = NewPager(next, q->opts.offset, q->opts.num);

  // The loader loads the documents from redis
  // If we do not need to return any fields - we do not need the loader in the loop
  if (!(q->opts.flags & Search_NoContent)) {
    next = NewLoader(next, q->ctx, &req->opts.fields);
    if (req->opts.fields.wantSummaries && (q->ctx->spec->flags & Index_StoreTermOffsets) != 0) {
      next = NewHighlightProcessor(next, req);
    }
  }

  return next;
}
