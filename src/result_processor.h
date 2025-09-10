/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#pragma once

#include "redisearch.h"
#include "sortable.h"
#include "value.h"
#include "concurrent_ctx.h"
#include "search_ctx.h"
#include "iterators/iterator_api.h"
#include "search_options.h"
#include "rlookup.h"
#include "extension.h"
#include "score_explain.h"
#include "rs_wall_clock.h"
#include "util/references.h"
#include "hybrid/hybrid_scoring.h"
#include "hybrid/hybrid_lookup_context.h"
#include "vector_normalization.h"
#include "result_processor_rs.h"
#include "search_result_rs.h"
#include "slot_ranges.h"

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
  RP_KEY_NAME_LOADER,
  RP_MAX_SCORE_NORMALIZER,
  RP_VECTOR_NORMALIZER,
  RP_HYBRID_MERGER,
  RP_DEPLETER,
  RP_MAX, // Marks the last non-debug RP type
  // Debug only result processors
  RP_TIMEOUT,
  RP_CRASH,
  RP_PAUSE,
} ResultProcessorType;

struct ResultProcessor;
struct RLookup;

// Define our own structures to avoid conflicts with the iterator_api.h QueryIterator
/// <div rustbindgen hide></div>
typedef struct QueryProcessingCtx {
  // First processor
  struct ResultProcessor *rootProc;

  // Last processor
  struct ResultProcessor *endProc;

  rs_wall_clock initTime; //used with clock_gettime(CLOCK_MONOTONIC, ...)
  rs_wall_clock_ns_t GILTime;  //Time accumulated in nanoseconds

  // the minimal score applicable for a result. It can be used to optimize the
  // scorers
  double minScore;

  // the total results found in the query, incremented by the root processors
  // and decremented by others who might disqualify results
  uint32_t totalResults;

  // the number of results we requested to return at the current chunk.
  // This value is meant to be used by the RP to limit the number of results
  // returned by its upstream RP ONLY.
  // It should be restored after using it for local aggregation etc., as done in
  // the Safe-Loader, Sorter, and Pager.
  uint32_t resultLimit;

  // Object which contains the error
  QueryError *err;

  // Background indexing OOM warning
  bool bgScanOOM;

  bool isProfile;
  RSTimeoutPolicy timeoutPolicy;
} QueryProcessingCtx;

QueryIterator *QITR_GetRootFilter(QueryProcessingCtx *it);
void QITR_PushRP(QueryProcessingCtx *it, struct ResultProcessor *rp);
void QITR_FreeChain(QueryProcessingCtx *qitr);

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
  // Depleting process has begun.
  RS_RESULT_DEPLETING,
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
  QueryProcessingCtx *parent;

  // Previous result processor in the chain
  struct ResultProcessor *upstream;

  // Type of result processor
  ResultProcessorType type;

  struct timespec GILTime;
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

ResultProcessor *RPQueryIterator_New(QueryIterator *itr, const SharedSlotRangeArray *slotRanges, const RedisModuleSlotRangeArray *querySlots, uint32_t slotsVersion, RedisSearchCtx *sctx);

ResultProcessor *RPScorer_New(const ExtScoringFunctionCtx *funcs,
                              const ScoringFunctionArgs *fnargs,
                              const RLookupKey *rlk);

ResultProcessor *RPMetricsLoader_New();

/** Functions abstracting the sortmap. Hides the bitwise logic */
#define SORTASCMAP_INIT 0xFFFFFFFFFFFFFFFF
#define SORTASCMAP_MAXFIELDS 8
#define SORTASCMAP_SETASC(mm, pos) ((mm) |= (1LLU << (pos)))
#define SORTASCMAP_SETDESC(mm, pos) ((mm) &= ~(1LLU << (pos)))
#define SORTASCMAP_GETASC(mm, pos) ((mm) & (1LLU << (pos)))

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
ResultProcessor *RPLoader_New(RedisSearchCtx *sctx, uint32_t reqflags, RLookup *lk, const RLookupKey **keys, size_t nkeys, bool forceLoad, uint32_t *outStateflags);

void SetLoadersForBG(QueryProcessingCtx *qctx);
void SetLoadersForMainThread(QueryProcessingCtx *qctx);

/** Creates a new Highlight processor */
ResultProcessor *RPHighlighter_New(RSLanguage language, const FieldList *fields,
                                   const RLookup *lookup);

/*******************************************************************************************************************
 *  Profiling Processor
 *
 * This processor collects time and count info about the performance of its upstream RP.
 *
 *******************************************************************************************************************/
ResultProcessor *RPProfile_New(ResultProcessor *rp, QueryProcessingCtx *qctx);

rs_wall_clock_ns_t RPProfile_GetClock(ResultProcessor *rp);
uint64_t RPProfile_GetCount(ResultProcessor *rp);
void RPProfile_IncrementCount(ResultProcessor *rp);

void Profile_AddRPs(QueryProcessingCtx *qctx);

/*******************************************************************************************************************
 *  Normalizer Result Processor
 *
 * Normalizes search result scores to [0, 1] range by dividing each score by the maximum score.
 * First accumulates all results from the upstream, then normalizes and yields them.
 *******************************************************************************************************************/
ResultProcessor *RPMaxScoreNormalizer_New(const RLookupKey *rlk);

/*******************************************************************************************************************
 *  Vector Normalizer Result Processor
 *
 * Normalizes vector distance scores using a provided normalization function.
 * Processes results immediately without accumulation.
 * The normalization function is provided by pipeline construction logic.
 *******************************************************************************************************************/
ResultProcessor *RPVectorNormalizer_New(VectorNormFunction normFunc, const RLookupKey *scoreKey);

/*******************************************************************************
* Depleter Result Processor
*
*  The RPDepleter result processor offloads the task of consuming all results from
*  its upstream processor into a background thread, storing them in an internal
*  array. While the background thread is running, calls to Next() wait on a shared
*  condition variable and return RS_RESULT_DEPLETING. The thread can be awakened
*  either by its own depleting thread completing or by another RPDepleter's thread
*  signaling completion. Once depleting is complete for this processor, Next()
*  yields results one by one from the internal array, and finally returns the last
*  return code from the upstream.
*/

/**
* Constructs a new RPDepleter processor that offloads result consumption to a background thread.
* The returned processor takes ownership of result depleting and yielding.
* @param sync_ref Reference to shared synchronization object for coordinating multiple depleters
* @param depletingThreadCtx Search context for the upstream processor being wrapped
* @param nextThreadCtx Search context for the downstream processor that will receive results
*/
ResultProcessor *RPDepleter_New(StrongRef sync_ref, RedisSearchCtx *depletingThreadCtx, RedisSearchCtx *nextThreadCtx);

/**
* Starts the depletion for all the depleters in the array, waits until all finished depleting, and returns.
* @param depleters Array of depleter processors
* @param count Number of depleter processors in the array
* @return RS_RESULT_OK if all depleters completed successfully, otherwise an error code
*/
int RPDepleter_DepleteAll(arrayof(ResultProcessor*) depleters);

/**
* Creates a new shared synchronization object for coordinating multiple RPDepleter processors.
* This is used during pipeline construction to create sync objects that allow multiple
* depleters to coordinate their background threads and wake each other when depleting completes.
* @param num_depleters Number of RPDepleter processors that will share this sync object
* @param take_index_lock Whether the depleters should participate in index locking coordination
*/
StrongRef DepleterSync_New(unsigned int num_depleters, bool take_index_lock);

/*******************************************************************************************************************
 *  Hybrid Merger Result Processor
 *
 * Merges results from multiple upstream processors using a hybrid scoring function.
 * Takes results from all upstreams and applies the provided function to combine their scores.
 *******************************************************************************************************************/
/*
 * Creates a new Hybrid Merger processor.
 * Note: RPHybridMerger takes ownership of hybridScoringCtx and is responsible for freeing it.
 * @param scoreKey Optional key for writing scores as fields when no LOAD step is provided
 */
ResultProcessor *RPHybridMerger_New(HybridScoringContext *hybridScoringCtx,
                                    ResultProcessor **upstreams,
                                    size_t numUpstreams,
                                    const RLookupKey *docKey,
                                    const RLookupKey *scoreKey,
                                    RPStatus *subqueriesReturnCodes,
                                    HybridLookupContext *lookupCtx);

/*
 * Returns NULL if the processor is not a HybridMerger or if scoreKey is NULL.
 */
const RLookupKey *RPHybridMerger_GetScoreKey(ResultProcessor *rp);

// Return string for RPType
const char *RPTypeToString(ResultProcessorType type);

// Return RPType for string
ResultProcessorType StringToRPType(const char *str);


/*******************************************************************************************************************
 *  Debug only result processors
 *
 * *******************************************************************************************************************/

/*******************************************************************************************************************
 *  Timeout Processor - DEBUG ONLY
 *
 * returns timeout after N results, N >= 0.
 *******************************************************************************************************************/
ResultProcessor *RPTimeoutAfterCount_New(size_t count, RedisSearchCtx *sctx);
void PipelineAddTimeoutAfterCount(QueryProcessingCtx *qctx, RedisSearchCtx *sctx, size_t results_count);

/*******************************************************************************************************************
 *  Crash Processor - DEBUG ONLY
 *
 * crash the at the start of the query
 *******************************************************************************************************************/
ResultProcessor *RPCrash_New();
void PipelineAddCrash(struct AREQ *r);

/*******************************************************************************************************************
 *  Pause Processor - DEBUG ONLY
 *
 * Pauses the query after N results, N >= 0.
 *******************************************************************************************************************/
ResultProcessor *RPPauseAfterCount_New(size_t count);

// Adds a pause processor after N results, before/after a specific RP type
bool PipelineAddPauseRPcount(QueryProcessingCtx *qctx, size_t results_count, bool before, ResultProcessorType rp_type, QueryError *status);

#ifdef __cplusplus
}
#endif
