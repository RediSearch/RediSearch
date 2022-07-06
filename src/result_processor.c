
#include "result_processor.h"
#include "query.h"
#include "extension.h"
#include "ext/default.h"
#include "rmutil/rm_assert.h"

///////////////////////////////////////////////////////////////////////////////////////////////

// General Result Processor Helper functions

void QueryIterator::Cleanup() {
  ResultProcessor *p = rootProc;
  while (p) {
    ResultProcessor *next = p->upstream;
    delete p;
    p = next;
  }
}

//---------------------------------------------------------------------------------------------

void SearchResult::Clear() {
  // This won't affect anything if the result is null
  score = 0;
  if (scoreExplain) {
    scoreExplain->SEDestroy();
    scoreExplain = NULL;
  }
  if (indexResult) {
    delete indexResult;
    indexResult = NULL;
  }

  rowdata.Wipe();
  if (dmd) {
    dmd->Decref();
    dmd = NULL;
  }
}

//---------------------------------------------------------------------------------------------

SearchResult::SearchResult() {
}

//---------------------------------------------------------------------------------------------

// Free the search result object including the object itself

SearchResult::~SearchResult() {
  Clear();
  rowdata.Cleanup();
}

//---------------------------------------------------------------------------------------------

static int RPGeneric_NextEOF(ResultProcessor *rp, SearchResult *res) {
  return RS_RESULT_EOF;
}

//---------------------------------------------------------------------------------------------

// Next implementation
int RPIndexIterator::Next(SearchResult *res) {
  IndexIterator *it = iiter;

  // No root filter - the query has 0 results
  if (iiter == NULL) {
    return RS_RESULT_EOF;
  }

  IndexResult *r;
  RSDocumentMetadata *dmd;
  int rc;

  // Read from the root filter until we have a valid result
  while (true) {
    rc = it->Read(it->ctx, &r); //@@ What should replace it->ctx?
    // This means we are done!
    if (rc == INDEXREAD_EOF) {
      return RS_RESULT_EOF;
    } else if (!r || rc == INDEXREAD_NOTFOUND) {
      continue;
    }

    dmd = RP_SPEC(this)->docs.Get(r->docId);
    if (!dmd || (dmd->flags & Document_Deleted)) {
      continue;
    }

    // Increment the total results barring deleted results
    parent->totalResults++;
    break;
  }

  // set the result data
  res->docId = r->docId;
  res->indexResult = r;
  res->score = 0;
  res->dmd = dmd;
  res->rowdata.sv = dmd->sortVector;
  dmd->Incref();
  return RS_RESULT_OK;
}

//---------------------------------------------------------------------------------------------

RPIndexIterator::RPIndexIterator(IndexIterator *root) : ResultProcessor("Index") {
  iiter = root;
}

//---------------------------------------------------------------------------------------------

IndexIterator *QueryIterator::GetRootFilter() {
  return ((RPIndexIterator *)rootProc)->iiter;
}

//---------------------------------------------------------------------------------------------

void QueryIterator::PushRP(ResultProcessor *rp) {
  rp->parent = this;
  if (!rootProc) {
    endProc = rootProc = rp;
    rp->upstream = NULL;
    return;
  }
  rp->upstream = endProc;
  endProc = rp;
}

//---------------------------------------------------------------------------------------------

void QueryIterator::FreeChain() {
  ResultProcessor *rp = endProc;
  while (rp) {
    ResultProcessor *next = rp->upstream;
    delete rp;
    rp = next;
  }
}

//---------------------------------------------------------------------------------------------

int RPScorer::Next(SearchResult *res) {
  int rc;

  do {
    rc = upstream->Next(res);
    if (rc != RS_RESULT_OK) {
      return rc;
    }

    // Apply the scoring function
    res->score = scorer(&scorerCtx, res->indexResult, res->dmd, parent->minScore);
    if (scorerCtx.scrExp) {
      res->scoreExplain = (RSScoreExplain *)scorerCtx.scrExp;
      scorerCtx.scrExp = rm_calloc(1, sizeof(RSScoreExplain));
    }
    // If we got the special score RS_SCORE_FILTEROUT - disregard the result and decrease the total
    // number of results (it's been increased by the upstream processor)
    if (res->score == RS_SCORE_FILTEROUT) {
      parent->totalResults--;
      res->Clear();
      // continue and loop to the next result, since this is excluded by the
      // scorer.
      continue;
    }

    break;
  } while (1);

  return rc;
}

//---------------------------------------------------------------------------------------------

// Free impl. for scorer - frees up the scorer privdata if needed

RPScorer::~RPScorer() {
  if (scorerFree) {
    scorerFree(scorerCtx.extdata);
  }
  rm_free(scorerCtx.scrExp);
  scorerCtx.scrExp = NULL;
}

//---------------------------------------------------------------------------------------------

// Create a new scorer by name. If the name is not found in the scorer registry, we use the
// defalt scorer

RPScorer::RPScorer(const ExtScoringFunction *funcs, const ScoringFunctionArgs *fnargs) : ResultProcessor("Scorer") {
  scorer = funcs->sf;
  scorerFree = funcs->ff;
  scorerCtx = *fnargs;
}

//---------------------------------------------------------------------------------------------

// Next - pops the current top result from the heap

int RPSorter::Next(SearchResult *r) {
  // make sure we don't overshoot the heap size, unless the heap size is dynamic
  if (pq->count > 0 && (!size || offset++ < size)) {
    SearchResult *sr = mmh_pop_max(pq);
    RLookupRow oldrow = r->rowdata;
    *r = *sr;

    rm_free(sr);
    oldrow.Cleanup();
    return RS_RESULT_OK;
  }
  return RS_RESULT_EOF;
}

//---------------------------------------------------------------------------------------------

RPSorter::~RPSorter() {
  if (pooledResult) {
    delete pooledResult;
  }

  // calling mmh_free will free all the remaining results in the heap, if any
  mmh_free(pq);
}

//---------------------------------------------------------------------------------------------

#define RESULT_QUEUED RS_RESULT_MAX + 1

int RPSorter::innerLoop(SearchResult *r) {
  if (pooledResult == NULL) {
    pooledResult = rm_calloc(1, sizeof(*pooledResult));
  } else {
    pooledResult->rowdata.Wipe();
  }

  SearchResult *h = pooledResult;
  int rc = upstream->Next(h);

  // if our upstream has finished - just change the state to not accumulating, and Next
  if (rc == RS_RESULT_EOF) {
    // Transition state:
    return Next(r);
  } else if (rc != RS_RESULT_OK) {
    // whoops!
    return rc;
  }

  // If the queue is not full - we just push the result into it
  // If the pool size is 0 we always do that, letting the heap grow dynamically
  if (!size || pq->count + 1 < pq->size) {

    // copy the index result to make it thread safe - but only if it is pushed to the heap
    h->indexResult = NULL;
    mmh_insert(pq, h);
    pooledResult = NULL;
    if (h->score < parent->minScore) {
      parent->minScore = h->score;
    }

  } else {
    // find the min result
    SearchResult *minh = mmh_peek_min(pq);

    // update the min score. Irrelevant to SORTBY mode but hardly costs anything...
    if (minh->score > parent->minScore) {
      parent->minScore = minh->score;
    }

    // if needed - pop it and insert a new result
    if (cmp(h, minh, cmpCtx) > 0) {
      h->indexResult = NULL;
      pooledResult = mmh_pop_min(pq);
      mmh_insert(pq, h);
      pooledResult->Clear();
    } else {
      // The current should not enter the pool, so just leave it as is
      pooledResult = h;
      pooledResult->Clear();
    }
  }
  return RESULT_QUEUED;
}

//---------------------------------------------------------------------------------------------

int RPSorter::Accum(SearchResult *r) {
  int rc;
  while ((rc = innerLoop(r)) == RESULT_QUEUED) {
    // Do nothing.
  }
  return rc;
}

//---------------------------------------------------------------------------------------------

// Compare results for the heap by score
static inline int cmpByScore(const void *e1, const void *e2, const void *udata) {
  const SearchResult *h1 = e1, *h2 = e2;

  if (h1->score < h2->score) {
    return -1;
  } else if (h1->score > h2->score) {
    return 1;
  }
  return h1->docId < h2->docId ? -1 : 1;
}

//---------------------------------------------------------------------------------------------

// Compare results for the heap by sorting key
static int cmpByFields(const void *e1, const void *e2, const void *udata) {
  const RPSorter *self = udata;
  const SearchResult *h1 = e1, *h2 = e2;
  int ascending = 0;

  QueryError *qerr = NULL;
  if (self && self->parent && self->parent->err) {
    qerr = self->parent->err;
  }

  for (size_t i = 0; i < self->fieldcmp.nkeys && i < SORTASCMAP_MAXFIELDS; i++) {
    const RSValue *v1 = h1->rowdata.GetItem(self->fieldcmp.keys[i]);
    const RSValue *v2 = h2->rowdata.GetItem(self->fieldcmp.keys[i]);
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

    int rc = RSValue::Cmp(v1, v2, qerr);
    // printf("asc? %d Compare: \n", ascending);
    // v1->Print();
    // printf(" <=> ");
    // v2->Print();
    // printf("\n");

    if (rc != 0) return ascending ? -rc : rc;
  }

  int rc = h1->docId < h2->docId ? -1 : 1;
  return ascending ? -rc : rc;
}

//---------------------------------------------------------------------------------------------

static void srDtor(void *p) {
  if (p) {
    SearchResult_Destroy(p);
    rm_free(p);
  }
}

//---------------------------------------------------------------------------------------------

void RPSorter::ctor(size_t maxresults, const RLookupKey **keys, size_t nkeys, uint64_t ascmap) {
  cmp = nkeys ? cmpByFields : cmpByScore;
  cmpCtx = this;
  fieldcmp.ascendMap = ascmap;
  fieldcmp.keys = keys;
  fieldcmp.nkeys = nkeys;

  pq = mmh_init_with_size(maxresults + 1, cmp, cmpCtx, srDtor);
  size = maxresults;
  offset = 0;
  pooledResult = NULL;
  name = "Sorter";
}

//---------------------------------------------------------------------------------------------

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

//---------------------------------------------------------------------------------------------

int RPPager::Next(SearchResult *r) {
  int rc;

  // If we've not reached the offset
  while (count < offset) {
    int rc = upstream->Next(r);
    if (rc != RS_RESULT_OK) {
      return rc;
    }
    count++;
    r->Clear();
  }

  // If we've reached LIMIT:
  if (count >= limit + offset) {
    return RS_RESULT_EOF;
  }

  count++;
  rc = upstream->Next(r);
  return rc;
}

//---------------------------------------------------------------------------------------------

// Create a new pager. The offset and limit are taken from the user request

RPPager::RPPager(size_t offset, size_t limit) :
  ResultProcessor("Pager/Limiter"), offset(offset), limit(limit) {}

//---------------------------------------------------------------------------------------------

int ResultsLoader::Next(SearchResult *r) {
  int rc = upstream->Next(r);
  if (rc != RS_RESULT_OK) {
    return rc;
  }

  int isExplicitReturn = !!nfields;

  // Current behavior skips entire result if document does not exist.
  // I'm unusre if that's intentional or an oversight.
  if (r->dmd == NULL || (r->dmd->flags & Document_Deleted)) {
    return RS_RESULT_OK;
  }

  RLookupLoadOptions loadopts(parent->sctx, r->dmd, new QueryError());
  loadopts.noSortables = true;
  loadopts.forceString = true;
  loadopts.keys = fields;
  loadopts.nkeys = nfields;

  if (isExplicitReturn) {
    loadopts.mode |= RLOOKUP_LOAD_KEYLIST;
  } else {
    loadopts.mode |= RLOOKUP_LOAD_ALLKEYS;
  }
  lk->LoadDocument(&r->rowdata, &loadopts);
  return RS_RESULT_OK;
}

//---------------------------------------------------------------------------------------------

ResultsLoader::~ResultsLoader() {
  rm_free(fields);
}

//---------------------------------------------------------------------------------------------

ResultsLoader::ResultsLoader(RLookup *lk, const RLookupKey **keys, size_t nkeys) :
  ResultProcessor("Loader"), nfields(nkeys), lk(lk) {
  fields = rm_calloc(nkeys, sizeof(*fields));
  memcpy(fields, keys, sizeof(*keys) * nkeys);
}

//---------------------------------------------------------------------------------------------

void ResultProcessor::DumpChain() const {
  ResultProcessor *rp = this;
  for (; rp; rp = rp->upstream) {
    printf("RP(%s) @%p\n", rp->name, rp);
    RS_LOG_ASSERT(rp->upstream != rp, "ResultProcessor should be different then upstream");
  }
}

///////////////////////////////////////////////////////////////////////////////////////////////
