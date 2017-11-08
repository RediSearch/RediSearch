#ifndef RS_RESULT_PROCESSOR_H_
#define RS_RESULT_PROCESSOR_H_

#include "redisearch.h"
#include "sortable.h"
#include "value.h"
#include "concurrent_ctx.h"
#include "search_request.h"

/********************************************************************************
 * Result Processor Chain
 *
 * We use a chain of result processors to sort, score, filter and page the results coming from the
 * index.
 *
 * The index iterator tree is responsible for extracting results from the index, and the processor
 * chain is responsible for processing those and preparing them for the users.
 * The processors are exposing an iterator interface, adding values to SearchResult objects.
 *
 * SearchResult objects contain all the data needed for a search result - from docId and score, to
 * the actual fields loaded from redis.
 *
 * Processors can add more fields, rewrite them, change the score, etc.
 * The query plan builds the chain based on the request, and then the chain just processes the
 * results.
 *
 ********************************************************************************/

/* Query processing state */
typedef enum {
  QueryState_OK,
  QueryState_Aborted,
  QueryState_Error,
  // TimedOut state differs from aborted in that it lets the processors drain their accumulated
  // results instead of stopping in our tracks and returning nothing.
  QueryState_TimedOut,
} QueryState;

/* Query processing context. It is shared by all result processors */
typedef struct {
  // Concurrent search context for thread switching
  ConcurrentSearchCtx *conc;
  // Contains our spec
  RedisSearchCtx *sctx;
  // the minimal score applicable for a result. It can be used to optimize the scorers
  double minScore;
  // the total results found in the query, incremented by the root processors and decremented by
  // others who might disqualify results
  uint32_t totalResults;
  // an optional error string if something went wrong - currently not used
  char *errorString;
  // the state - used for aborting queries
  QueryState state;

  IndexIterator *rootFilter;

  struct timespec startTime;

} QueryProcessingCtx;

/*
 * SearchResult - the object all the processing chain is working on.
 * It has the indexResult which is what the index scan brought - scores, vectors, flags, etc.
 *
 * And a list of fields loaded by the chain - currenly only by the loader, but possibly by
 * aggregators later on
 */
typedef struct {
  t_docId docId;

  // not all results have score - TBD
  double score;

  // The sorting vector of the result.
  RSSortingVector *sv;

  // The entire document metadata. Guaranteed not to be NULL
  // TODO: Check thread safety of it. Might be deleted on context switches
  RSDocumentMetadata *md;

  // index result should cover what you need for highlighting,
  // but we will add a method to duplicate index results to make
  // them thread safe
  RSIndexResult *indexResult;

  // dynamic fields
  RSFieldMap *fields;
} SearchResult;

/* Result processor return codes */

// OK - we have a valid result
#define RS_RESULT_OK 0
// Queued - nothing yet, this is a reducer in accumulation state
#define RS_RESULT_QUEUED 1
// EOF - no results from this processor
#define RS_RESULT_EOF 2

/* Context for a single result processor, including global shared state, upstream processor and
 * private data */
typedef struct {
  // The processor's own private data. It's the processor's responsibility to free it
  void *privdata;

  // The upstream processor from which we read results
  struct resultProcessor *upstream;

  // The global state of the query processing chain
  QueryProcessingCtx *qxc;
} ResultProcessorCtx;

typedef struct resultProcessor {
  // the context should contain a pointer to the upstream step
  // like the index iterator does
  ResultProcessorCtx ctx;

  // Next is called by the downstream processor, and should return either:
  // * RS_RESULT_OK -> means we put something in the result pointer and it can be processed
  // * RS_RESULT_QUEUED -> no result yet, we're waiting for more results upstream. Caller should
  //   return QUEUED as well
  // * RS_RESULT_EOF -> finished, nothing more from this processor
  int (*Next)(ResultProcessorCtx *ctx, SearchResult *res);

  // Free just frees up the processor. If left as NULL we simply use free()
  void (*Free)(struct resultProcessor *p);
} ResultProcessor;

/* Create a raw result processor object with no callbacks, just the upstream and privdata */
ResultProcessor *NewResultProcessor(ResultProcessor *upstream, void *privdata);

/* Safely call Next on an upstream processor, putting the result into res. If allowSwitching is 1,
 * we check the concurrent context and perhaps switch if needed.
 *
 * Note 1: Do not call processors' Next() directly, ONLY USE THIS FUNCTION
 *
 * Note 2: this function will not return RS_RESULT_QUEUED, but only OK or EOF. Any queued events
 * will be handled by this function
 * */
int ResultProcessor_Next(ResultProcessor *rp, SearchResult *res, int allowSwitching);

/* Helper function - get the total from a processor, and if the Total callback is NULL, climb up
 * the
 * chain until we find a processor with a Total callback. This allows processors to avoid
 * implementing it if they have no calculations to add to Total (such as deeted/ignored results)
 * */
size_t ResultProcessor_Total(ResultProcessor *rp);

/* Free a result processor - recursively freeing its upstream as well. If the processor does not
 * implement Free - we just call free() on the processor object itself.
 *
 * Do NOT call Free() callbacks on processors directly! */
void ResultProcessor_Free(ResultProcessor *rp);

/** Frees the processor and privdata with `free()` */
void ResultProcessor_GenericFree(ResultProcessor *rp);

// Get the index spec from the result processor
#define RP_SPEC(rpctx) ((rpctx)->qxc->sctx->spec)

SearchResult *NewSearchResult();

void SearchResult_FreeInternal(SearchResult *r);
void SearchResult_Free(void *p);

ResultProcessor *NewHighlightProcessor(ResultProcessor *upstream, RSSearchRequest *req);
#endif  // !RS_RESULT_PROCESSOR_H_
