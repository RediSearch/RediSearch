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

/* Query processing state */
typedef enum {
  QITR_S_RUNNING,
  QITR_S_ABORTED,

  // TimedOut state differs from aborted in that it lets the processors drain their accumulated
  // results instead of stopping in our tracks and returning nothing.
  QITR_S_TIMEDOUT
} QITRState;

typedef enum {
  RP_INDEX,
  RP_LOADER,
  RP_BUFFER_AND_LOCKER,
  RP_UNLOCKER,
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

  // Object which contains the error
  QueryError *err;

  // the state - used for aborting queries
  QITRState state;

  struct timespec startTime;

  RSTimeoutPolicy timeoutPolicy;
} QueryIterator, QueryProcessingCtx;

IndexIterator *QITR_GetRootFilter(QueryIterator *it);
void QITR_PushRP(QueryIterator *it, struct ResultProcessor *rp);
void QITR_FreeChain(QueryIterator *qitr);


/*
 * Flags related to the search results.
 */

#define SEARCHRESULT_VAL_IS_NULL 0x01

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
  RSScoreExplain *scoreExplain;

  const RSDocumentMetadata *dmd;

  // index result should cover what you need for highlighting,
  // but we will add a method to duplicate index results to make
  // them thread safe
  RSIndexResult *indexResult;

  // Row data. Use RLookup_* functions to access
  RLookupRow rowdata;

  // Result flags.
  uint8_t flags;
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

typedef enum {
  RESULT_PROCESSOR_F_ACCESS_REDIS = 0x01,  // The result processor requires access to redis keyspace.

  // The result processor might break the pipeline by changing RPStatus.
  // Note that this kind of rp is also responsible to release the spec lock when it breaks the pipeline
  // (declaring EOF or TIMEOUT), by calling UnlockSpec_and_ReturnRPResult.
  RESULT_PROCESSOR_F_BREAKS_PIPELINE = 0x02
} BaseRPFlags;

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

  uint32_t flags;

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
 * This function resets the search result, so that it may be reused again.
 * Internal caches are reset but not freed
 */
void SearchResult_Clear(SearchResult *r);

/**
 * This function clears the search result, also freeing its internals. Internal
 * caches are freed. Use this function if `r` will not be used again.
 */
void SearchResult_Destroy(SearchResult *r);

ResultProcessor *RPIndexIterator_New(IndexIterator *itr, struct timespec timeoutTime);

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
 * @param loadKeys is an array of RLookupkeys that their value needs to be loaded from Redis keyspace.
 * @param nLoadKeys is the length of loadKeys.
 * If keys and loadKeys doesn't point to the same address, loadKeys will be freed in the sorter dtor.
 */
ResultProcessor *RPSorter_NewByFields(size_t maxresults, const RLookupKey **keys, size_t nkeys,
                                      uint64_t ascendingMap, bool quickExit);

ResultProcessor *RPSorter_NewByScore(size_t maxresults, bool quickExit);

ResultProcessor *RPPager_New(size_t offset, size_t limit);

/*******************************************************************************************************************
 *  Loading Processor
 *
 * This processor simply takes the search results, and based on the request parameters, loads the
 * relevant fields for the results that need to be displayed to the user, from redis.
 *
 * It fills the result objects' field map with values corresponding to the requested return fields
 *
 *******************************************************************************************************************/
ResultProcessor *RPLoader_New(RLookup *lk, const RLookupKey **keys, size_t nkeys);

/** Creates a new Highlight processor */
ResultProcessor *RPHighlighter_New(const RSSearchOptions *searchopts, const FieldList *fields,
                                   const RLookup *lookup);

void RP_DumpChain(const ResultProcessor *rp);

/*******************************************************************************************************************
 *  Buffer and Locker Results Processor
 *
 * This component should be added to the query's execution pipeline if a thread safe access to
 * Redis keyspace is required.
 *
 * The buffer is responsible for buffering the document that pass the query filters and lock the access
 * to Redis key-space to allow the downstream result processor a thread safe access to it.
 *
 * Unlocking Redis should be done only by the Unlocker result processor that should be added as well.
 *
 * @param BlockSize is the number of results in each buffer block.
 * @param spec_version is the version of the spec during pipeline construction. This version will be compared
 * to the spec version after we unlock the spec, to decide if results' validation is needed.
 *******************************************************************************************************************/
typedef struct RPBufferAndLocker RPBufferAndLocker;
ResultProcessor *RPBufferAndLocker_New(size_t BlockSize, size_t spec_version);

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

ResultProcessor *RPUnlocker_New(RPBufferAndLocker *rpBufferAndLocker);

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

void updateRPIndexTimeout(ResultProcessor *base, struct timespec timeout);

double RPProfile_GetDurationMSec(ResultProcessor *rp);
uint64_t RPProfile_GetCount(ResultProcessor *rp);

void Profile_AddRPs(QueryIterator *qiter);

// Return string for RPType
const char *RPTypeToString(ResultProcessorType type);

#ifdef __cplusplus
}
#endif
