/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */


#pragma once

#include "redisearch.h"
#include "sortable.h"
#include "value.h"
#include "concurrent_ctx.h"
#include "search_ctx.h"
#include "index_iterator.h"
#include "search_options.h"
#include "rlookup.h"
#include "extension.h"
#include "score_explain.h"

#ifdef __cplusplus
extern "C" {
#endif

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

typedef enum {
  RP_INDEX,
  RP_LOADER,
  RP_SAFE_LOADER,
  RP_SCORER,
  RP_SORTER,
  RP_COUNTER,
  RP_PAGER_LIMITER,
  RP_HIGHLIGHTER,
  RP_GROUP,
  RP_PROJECTOR,
  RP_FILTER,
  RP_PROFILE,
  RP_NETWORK,
  RP_METRICS,
  RP_MAX,
} ResultProcessorType;

struct ResultProcessor;
struct RLookup;

typedef struct {
  // First processor
  struct ResultProcessor *rootProc;

  // Last processor
  struct ResultProcessor *endProc;

  // Concurrent search context for thread switching
  ConcurrentSearchCtx *conc;

  // Contains our spec
  RedisSearchCtx *sctx;

  // the minimal score applicable for a result. It can be used to optimize the scorers
  double minScore;

  // the total results found in the query, incremented by the root processors and decremented by
  // others who might disqualify results
  uint32_t totalResults;

  // the number of results we requested to return at the current chunk. This value may be used by
  // processors to optimize their work and to signal RP in the upstream their limit.
  uint32_t resultLimit;

  // Object which contains the error
  QueryError *err;

  RSTimeoutPolicy timeoutPolicy;
} QueryIterator, QueryProcessingCtx;

IndexIterator *QITR_GetRootFilter(QueryIterator *it);
void QITR_PushRP(QueryIterator *it, struct ResultProcessor *rp);
void QITR_FreeChain(QueryIterator *qitr);

/*
 * SearchResult - the object all the processing chain is working on.
 * It has the indexResult which is what the index scan brought - scores, vectors, flags, etc,
 * and a list of fields loaded by the chain
 */
typedef struct {
  t_docId docId;

  // not all results have score - TBD
  double score;
  RSScoreExplain *scoreExplain;

  const RSDocumentMetadata *dmd;

  // index result should cover what you need for highlighting,
  // but we will add a method to duplicate index results to make
  // them thread safe
  RSIndexResult *indexResult;

  // Row data. Use RLookup_* functions to access
  RLookupRow rowdata;
} SearchResult;

/* Result processor return codes */

/** Possible return values from Next() */
typedef enum {
  // Result is filled with valid data
  RS_RESULT_OK = 0,
  // Result is empty, and the last result has already been returned.
  RS_RESULT_EOF,
  // Execution paused due to rate limiting (or manual pause from ext. thread??)
  RS_RESULT_PAUSED,
  // Execution halted because of timeout
  RS_RESULT_TIMEDOUT,
  // Aborted because of error. The QueryState (parent->status) should have
  // more information.
  RS_RESULT_ERROR,
  // Not a return code per se, but a marker signifying the end of the 'public'
  // return codes. Implementations can use this for extensions.
  RS_RESULT_MAX
} RPStatus;

/**
 * Result processor structure. This should be "Subclassed" by the actual
 * implementations
 */
typedef struct ResultProcessor {
  // Reference to the parent structure
  QueryIterator *parent;

  // Previous result processor in the chain
  struct ResultProcessor *upstream;

  // Type of result processor
  ResultProcessorType type;

  /**
   * Populates the result pointed to by `res`. The existing data of `res` is
   * not read, so it is the responsibility of the caller to ensure that there
   * are no refcount leaks in the structure.
   *
   * Users can use SearchResult_Clear() to reset the structure without freeing
   * it.
   *
   * The populated structure (if RS_RESULT_OK is returned) does contain references
   * to document data. Callers *MUST* ensure they are eventually freed.
   */
  int (*Next)(struct ResultProcessor *self, SearchResult *res);

  /** Frees the processor and any internal data related to it. */
  void (*Free)(struct ResultProcessor *self);
} ResultProcessor;

/**
 * This function allocates a new SearchResult, copies the data from `src` to it,
 * and returns it.
*/
SearchResult *SearchResult_Copy(SearchResult *r);

/**
 * This function resets the search result, so that it may be reused again.
 * Internal caches are reset but not freed
 */
void SearchResult_Clear(SearchResult *r);

/**
 * This function clears the search result, also freeing its internals. Internal
 * caches are freed. Use this function if `r` will not be used again.
 */
void SearchResult_Destroy(SearchResult *r);

ResultProcessor *RPIndexIterator_New(IndexIterator *itr);

ResultProcessor *RPScorer_New(const ExtScoringFunctionCtx *funcs,
                              const ScoringFunctionArgs *fnargs);

ResultProcessor *RPMetricsLoader_New();

/** Functions abstracting the sortmap. Hides the bitwise logic */
#define SORTASCMAP_INIT 0xFFFFFFFFFFFFFFFF
#define SORTASCMAP_MAXFIELDS 8
#define SORTASCMAP_SETASC(mm, pos) ((mm) |= (1LLU << (pos)))
#define SORTASCMAP_SETDESC(mm, pos) ((mm) &= ~(1LLU << (pos)))
#define SORTASCMAP_GETASC(mm, pos) ((mm) & (1LLU << (pos)))
void SortAscMap_Dump(uint64_t v, size_t n);

/**
 * Creates a sorter result processor.
 * @param keys is an array of RLookupkeys to sort by them,
 * @param nkeys is the number of keys.
 * keys will be freed by the arrange step dtor.
 */
ResultProcessor *RPSorter_NewByFields(size_t maxresults, const RLookupKey **keys, size_t nkeys, uint64_t ascendingMap);

ResultProcessor *RPSorter_NewByScore(size_t maxresults);

ResultProcessor *RPPager_New(size_t offset, size_t limit);

/*******************************************************************************************************************
 *  Loading Processor
 *
 * This processor simply takes the search results, and based on the request parameters, loads the
 * relevant fields for the results that need to be displayed to the user, from redis.
 *
 * It fills the result objects' field map with values corresponding to the requested return fields
 *
 * On thread safe mode, the loader will buffer results, in an internal phase will lock redis and load the requested
 * fields and then unlock redis, and then will yield the results to the next processor in the chain.
 * On non thread safe mode (running the query from the main thread), the loader will load the requested fields
 * for each result, one result at a time, and yield it to the next processor in the chain.
 *
 *******************************************************************************************************************/
struct AREQ;
ResultProcessor *RPLoader_New(struct AREQ *r, RLookup *lk, const RLookupKey **keys, size_t nkeys);

/** Creates a new Highlight processor */
ResultProcessor *RPHighlighter_New(const RSSearchOptions *searchopts, const FieldList *fields,
                                   const RLookup *lookup);

void RP_DumpChain(const ResultProcessor *rp);

/*******************************************************************************************************************
 *  Profiling Processor
 *
 * This processor collects time and count info about the performance of its upstream RP.
 *
 *******************************************************************************************************************/
ResultProcessor *RPProfile_New(ResultProcessor *rp, QueryIterator *qiter);


/*******************************************************************************************************************
 *  Counter Processor
 *
 * This processor counts the search results.
 *
 *******************************************************************************************************************/
ResultProcessor *RPCounter_New();

double RPProfile_GetDurationMSec(ResultProcessor *rp);
uint64_t RPProfile_GetCount(ResultProcessor *rp);

void Profile_AddRPs(QueryIterator *qiter);

// Return string for RPType
const char *RPTypeToString(ResultProcessorType type);

#ifdef __cplusplus
}
#endif
