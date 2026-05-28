/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#ifndef RS_AGGREGATE_H__
#define RS_AGGREGATE_H__

#include "query_flags.h"
#include "value_ffi.h"
#include "query.h"
#include "reducer.h"
#include "result_processor.h"
#include "expr/expression.h"
#include "aggregate_plan.h"
#include "pipeline/pipeline.h"
#include "pipeline/pipeline_construction.h"
#include "reply.h"
#include "vector_index.h"
#include "hybrid/vector_query_utils.h"
#include "slot_ranges.h"
#include "profile/profile.h"
#include "rs_wall_clock.h"

#include "rmutil/rm_assert.h"

#ifdef __cplusplus
#include <atomic>
#define RS_Atomic(T) std::atomic<T>
#define RS_AtomicBoolLoadRelaxed(p)     (((std::atomic<bool> *)(p))->load(std::memory_order_relaxed))
#define RS_AtomicBoolStoreRelaxed(p, v) (((std::atomic<bool> *)(p))->store((v), std::memory_order_relaxed))
extern "C" {
#else
#define RS_Atomic(T) _Atomic(T)
#define RS_AtomicBoolLoadRelaxed(p)     __atomic_load_n((bool *)(p), __ATOMIC_RELAXED)
#define RS_AtomicBoolStoreRelaxed(p, v) __atomic_store_n((bool *)(p), (v), __ATOMIC_RELAXED)
#endif

#define DEFAULT_LIMIT 10

// Forward declaration for cursor
struct Cursor;

// Forward declaration for the MR channel used by the abort-wake path.
struct MRChannel;

/** Cached variables to avoid serializeResult retrieving these each time */
typedef struct {
  RLookup *lastLookup;
  const PLN_ArrangeStep *lastAstp;
} cachedVars;

/**
 * State needed for reply serialization in reply_callback path.
 * When using FAIL policy with workers, the background thread stores results here,
 * then calls UnblockClient. The reply_callback reads from here to build the reply.
 *
 * ## Cursor ↔ AREQ Ownership
 *
 * **Cursor owns AREQ** (not vice versa):
 * - cursor->execState points to the AREQ
 * - Cursor_FreeInternal calls AREQ_DecrRef(cur->execState)
 *
 * **AREQ does NOT own Cursor**:
 * - The `cursor` field below is a NON-OWNING handle.
 * - It exists solely so QueryReplyCallback knows which cursor to pause/free after
 *   finishSendChunk completes.
 * - In normal flow, QueryReplyCallback calls Cursor_Free/Cursor_Pause and clears this field.
 */
typedef struct {
  SearchResult **results;  // Aggregated results array (NULL if not aggregated yet)
  int rc;                  // Pipeline return code (RS_RESULT_OK, RS_RESULT_EOF, etc.)
  bool hasStoredResults;   // Flag to indicate results were stored for reply_callback
  QueryError err;          // Query error state (copied from qctx->err after pipeline execution)
  cachedVars cv;           // Cached lookup variables for result serialization
  /**
   * NON-OWNING cursor handle for reply_callback path.
   * See ownership model above. This is set in runCursor() when useReplyCallback is true,
   * and cleared by QueryReplyCallback after it handles cursor pause/free.
   * If timeout fires first, ChunkReplyState_Destroy cleans this up.
   */
  struct Cursor *cursor;
  size_t limit;            // Original limit passed to sendChunk (for RESP2 resultsLen calculation)
} ChunkReplyState;

/**
 * Clean up all resources held by a ChunkReplyState.
 * Handles the cursor ownership edge case (see struct documentation above).
 */
void ChunkReplyState_Destroy(ChunkReplyState *state);

typedef struct Grouper Grouper;
struct QOptimizer;

/*
 * A query can be of one type. So QEXEC_F_IS_AGGREGATE, QEXEC_F_IS_SEARCH, QEXEC_F_IS_HYBRID_TAIL,
 * QEXEC_F_IS_HYBRID_SEARCH_SUBQUERY, QEXEC_F_IS_HYBRID_VECTOR_AGGREGATE_SUBQUERY are mutually exclusive (Only one can be set).
 */

// Configuration parameters for cursor behavior
typedef struct {
  uint32_t maxIdle;     // Maximum idle time for the cursor (from MAXIDLE parameter)
  uint32_t chunkSize;   // Number of results per cursor read (from COUNT parameter)
} CursorConfig;

// Context structure for parseAggPlan to reduce parameter count
typedef struct {
  AGGPlan *plan;                    // Aggregation plan
  QEFlags *reqflags;                // Request flags
  RequestConfig *reqConfig;         // Request configuration
  RSSearchOptions *searchopts;      // Search options
  size_t *prefixesOffset;           // Prefixes offset
  CursorConfig *cursorConfig;       // Cursor configuration
  const char ***requiredFields;     // Required fields
  size_t *maxSearchResults;         // Maximum search results
  size_t *maxAggregateResults;      // Maximum aggregate results
  const RedisModuleSlotRangeArray **querySlots; // Slots requested (referenced from AREQ)
  uint32_t *keySpaceVersion;        // Version given by the slots tracker
  rs_wall_clock_ns_t *coordDispatchTime; // Coordinator dispatch time in ns (for internal commands)
} ParseAggPlanContext;

#define IsCount(r) ((r)->reqflags & QEXEC_F_NOROWS)
#define IsSearch(r) ((r)->reqflags & QEXEC_F_IS_SEARCH)
#define IsAggregate(r) ((r)->reqflags & QEXEC_F_IS_AGGREGATE)
#define IsHybridTail(r) ((r)->reqflags & QEXEC_F_IS_HYBRID_TAIL)
#define IsHybridSearchSubquery(r) ((r)->reqflags & QEXEC_F_IS_HYBRID_SEARCH_SUBQUERY)
#define IsHybridVectorSubquery(r) ((r)->reqflags & QEXEC_F_IS_HYBRID_VECTOR_AGGREGATE_SUBQUERY)
#define IsHybrid(r) (IsHybridTail(r) || IsHybridSearchSubquery(r) || IsHybridVectorSubquery(r))
#define IsProfile(r) ((r)->reqflags & QEXEC_F_PROFILE)
#define IsOptimized(r) ((r)->reqflags & QEXEC_OPTIMIZE)
#define HasDepleter(r) ((r)->reqflags & QEXEC_F_HAS_DEPLETER)
#define HasWithCount(r) ((r)->reqflags & QEXEC_F_HAS_WITHCOUNT)
#define IsFormatExpand(r) ((r)->reqflags & QEXEC_FORMAT_EXPAND)
#define IsWildcard(r) ((r)->ast.root->type == QN_WILDCARD)
#define IsCursor(r) ((r)->reqflags & QEXEC_F_IS_CURSOR)
#define HasScorer(r) ((r)->optimizer && (r)->optimizer->scorerType != SCORER_TYPE_NONE)
#define HasLoader(r) ((r)->stateflags & QEXEC_S_HAS_LOAD)
#define IsScorerNeeded(r) ((r)->reqflags & (QEXEC_F_SEND_SCORES | QEXEC_F_SEND_SCORES_AS_FIELD))
#define HasScoreInPipeline(r) ((r)->reqflags & QEXEC_F_SEND_SCORES_AS_FIELD)
#define HasSortBy(r) ((r)->reqflags & QEXEC_F_HAS_SORTBY)
#define HasGroupBy(r) ((r)->reqflags & QEXEC_F_HAS_GROUPBY)
#define IsInternal(r) ((r)->reqflags & QEXEC_F_INTERNAL)
#define IsDebug(r) ((r)->reqflags & QEXEC_F_DEBUG)

// Indicates whether a query should run in the background.
// Requires context to check if the client can be blocked.
bool RunInThread(RedisModuleCtx *ctx);

typedef void (*profiler_func)(RedisModule_Reply *reply, void *ctx);

typedef enum {
  /* Pipeline has a loader */
  QEXEC_S_HAS_LOAD = 0x01,
  /* Received EOF from iterator */
  QEXEC_S_ITERDONE = 0x02,
  /* ASM trimming delay timeout */
  QEXEC_S_ASM_TRIMMING_DELAY_TIMEOUT = 0x04,
  /* A shard reply carried a TIMEDOUT warning. Set by the coord-side RPNet
   * when it observes a shard's TIMEDOUT warning meta entry; the coord pipeline
   * keeps draining other shards (the coord has its own deadline check) and the
   * reply emitters surface the TIMEOUT warning to the user via this flag. */
  QEXEC_S_SHARD_TIMED_OUT_WARNING = 0x08,
} QEStateFlags;


typedef enum { COMMAND_AGGREGATE, COMMAND_SEARCH, COMMAND_EXPLAIN, COMMAND_HYBRID } CommandType;

/**
 * Common synchronization context for request types (AREQ, HybridRequest).
 * This context is used for timeout handling and synchronization between the main thread and the background thread.
 */
typedef struct RequestSyncCtx {
  // Timeout signaling flag set by timeout callback on main thread
  RS_Atomic(bool) timedOut;
  // Reference count for shared ownership between timeout callback (main thread) and background thread
  uint8_t refcount;

  /* Partial-timeout coordination. The CAS claim grants exclusive ownership of
   * the result-production phase: the BG-thread winner runs AggregateResults
   * and stores results, while the timeout-callback winner preempts BG (BG
   * bails at its post-claim check) and replies empty without running the
   * pipeline. The loser waits for the winner's completion signal.
   * Gated by `requiresAggregateResultsSync`. */
  bool requiresAggregateResultsSync;     // Enable CAS/Signal/Wait around AggregateResults
  RS_Atomic(bool) aggregatingResults;    // CAS claim: BG winner runs the pipeline; timeout-callback winner skips it and replies empty
  bool aggregateResultsDone;             // Set at completion; guarded by aggregateResultsLock
  pthread_mutex_t aggregateResultsLock;
  pthread_cond_t aggregateResultsCond;

  /* Abort-wake registration (single-slot). BG reader registers its blocking MR
   * channel; timeout callback broadcasts on it after flipping `timedOut`.
   * `abortWakeLock` serializes register/unregister/wake. */
  struct MRChannel *abortWakeChannel;
  pthread_mutex_t abortWakeLock;
} RequestSyncCtx;

// Initialize a RequestSyncCtx with default values
static inline void RequestSyncCtx_Init(RequestSyncCtx *ctx) {
  ctx->timedOut = false;
  ctx->refcount = 1;
  ctx->requiresAggregateResultsSync = false;
  ctx->aggregatingResults = false;
  ctx->aggregateResultsDone = false;
  pthread_mutex_init(&ctx->aggregateResultsLock, NULL);
  pthread_cond_init(&ctx->aggregateResultsCond, NULL);
  ctx->abortWakeChannel = NULL;
  pthread_mutex_init(&ctx->abortWakeLock, NULL);
}

// Release resources owned by a RequestSyncCtx. Must be called exactly once
// per successful Init, from the request's free path.
static inline void RequestSyncCtx_Destroy(RequestSyncCtx *ctx) {
  pthread_mutex_destroy(&ctx->aggregateResultsLock);
  pthread_cond_destroy(&ctx->aggregateResultsCond);
  pthread_mutex_destroy(&ctx->abortWakeLock);
}

typedef struct AREQ {
  /* Arguments converted to sds. Received on input */
  sds *args;
  size_t nargs;

  /** Search query string */
  const char *query;

  /** For hybrid queries: contains parsed vector data and partially constructed query node */
  ParsedVectorData *parsedVectorData;

  /** Fields to be output and otherwise processed */
  FieldList outFields;

  /** Options controlling search behavior */
  RSSearchOptions searchopts;

  /** Parsed query tree */
  QueryAST ast;

  /** Root iterator. This is owned by the request */
  QueryIterator *rootiter;

  /** Context, owned by request */
  RedisSearchCtx *sctx;

  /** Local slots info for this request */
  const RedisModuleSlotRangeArray *querySlots;
  uint32_t keySpaceVersion;

  /** Context for iterating over the queries themselves */
  QueryProcessingCtx qiter;

    /** The pipeline for this request */
  Pipeline pipeline;

  /** Flags controlling query output */
  QEFlags reqflags;

  /** Flags indicating current execution state */
  uint32_t stateflags;

  int protocol; // RESP2/3

  /*
  // Dialect version used on this request
  unsigned int dialectVersion;
  // Query timeout in milliseconds
  long long reqTimeout;
  RSTimeoutPolicy timeoutPolicy;
  // reply with time on profile
  int printProfileClock;
  uint64_t BM25STD_TanhFactor;
  */

  RequestConfig reqConfig;

  /** Cursor configuration */
  CursorConfig cursorConfig;

  /** Profile variables */
  ProfileClocks profileClocks;

  const char** requiredFields;

  struct QOptimizer *optimizer;        // Hold parameters for query optimizer

  // Currently we need both because maxSearchResults limits the OFFSET also in
  // FT.AGGREGATE execution.
  size_t maxSearchResults;
  size_t maxAggregateResults;

  // Cursor id, if this is a cursor
  uint64_t cursor_id;

  // Profiling function
  profiler_func profile;

  // The offset of the prefixes in the command
  size_t prefixesOffset;

  ProfilePrinterCtx profileCtx;

  // Synchronization context for timeout/reply callbacks
  RequestSyncCtx syncCtx;

  // Flag to indicate whether to skip timeout checks using clock checks
  bool skipTimeoutChecks;

  bool useReplyCallback;

  // State for reply_callback path (FAIL policy with workers)
  // Background thread stores results here, then calls UnblockClient.
  // The reply_callback reads from here to build the reply on the main thread.
  ChunkReplyState storedReplyState;
} AREQ;

/**
 * Create a new aggregate request. The request's lifecycle consists of several
 * stages:
 *
 * 1) New - creates a blank request
 *
 * 2) Compile - this gathers the request options from the commandline, creates
 *    the basic abstract plan.
 *
 * 3) ApplyContext - This is the second stage of Compile, and applies
 *    a stateful context. The reason for this state remaining separate is
 *    the ability to test parsing and option logic without having to worry
 *    that something might touch the underlying index.
 *    Compile also provides a place to optimize or otherwise rework the plan
 *    based on information known only within the query itself
 *
 * 4) BuildPipeline: This lines up all the iterators so that it can be
 *    read from.
 *
 * 5) Execute: This step is optional, and iterates through the result iterator,
 *    formatting the output and sending it to the network client. This step is
 *    optional, since the iterator can be obtained directly via AREQ_RP and
 *    processed directly
 *
 * 6) Free: This releases all resources consumed by the request
 */

AREQ *AREQ_New(void);

/**
 * Compile the request given the arguments. This does not rely on
 * Redis-specific states and may be unit-tested. This largely just
 * compiles the options and parses the commands..
 */
int AREQ_Compile(AREQ *req, RedisModuleCtx *ctx, RedisModuleString **argv, int argc, bool isDiskIndex, QueryError *status);

/**
 * Parse aggregate plan arguments (GROUPBY, APPLY, LOAD, FILTER) from an ArgsCursor.
 * This function extracts the aggregate-specific parsing logic that was previously
 * part of AREQ_Compile, allowing it to be reused for merge plans in hybrid queries.
 */
int parseAggPlan(ParseAggPlanContext *ctx, ArgsCursor *ac, bool isDiskIndex, QueryError *status);

/**
 * Initialize basic AREQ structure with search options and aggregation plan.
 */
void initializeAREQ(AREQ *req);

/**
 * This stage will apply the context to the request. During this phase, the
 * query will be parsed (and matched according to the schema), and the reducers
 * will be loaded and analyzed.
 *
 * Can be called from the main thread or from a background thread. (Note: access RSGlobalConfig which is not thread safe)
 *
 * This consumes a refcount of the context used.
 *
 * Note that this function consumes a refcount even if it fails!
 */
int AREQ_ApplyContext(AREQ *req, RedisSearchCtx *sctx, QueryError *status);

/**
 * Constructs the pipeline objects needed to actually start processing
 * the requests. This does not yet start iterating over the objects
 */
void AREQ_BuildAggregationPipelineParams(AREQ *req, AggregationPipelineParams *params);
int AREQ_BuildPipelineWithAggregationParams(AREQ *req,
                                            const AggregationPipelineParams *aggregationParams,
                                            QueryError *status);
int AREQ_BuildPipeline(AREQ *req, QueryError *status);

/**
 * Classify the request's (already-built) pipeline as yielding a valid partial
 * answer on RETURN-STRICT timeout and store the result on the request's
 * QueryProcessingCtx. Writes `false` if the pipeline has no end/root processor
 * yet (e.g. called before the pipeline is built).
 */
void AREQ_SetCanYieldPartialResults(AREQ *req);

static inline QEFlags AREQ_RequestFlags(const AREQ *req) {
  return (QEFlags)req->reqflags;
}

static inline void AREQ_AddRequestFlags(AREQ *req, QEFlags flags) {
  req->reqflags = (QEFlags)(req->reqflags | flags);
}

static inline void AREQ_RemoveRequestFlags(AREQ *req, QEFlags flags) {
  req->reqflags = (QEFlags)(req->reqflags & ~flags);
}

/**
 * Macro to directly set flags on a uint32_t *reqflags pointer.
 * This is used when we don't have access to an AREQ structure
 * but need to set flags directly on the reqflags pointer.
 */
#define REQFLAGS_AddFlags(reqflags, flags) (*(reqflags) |= (flags))

#define REQFLAGS_RemoveFlags(reqflags, flags) (*(reqflags) &= ~(flags))

static inline QueryProcessingCtx *AREQ_QueryProcessingCtx(AREQ *req) {
  return &req->pipeline.qctx;
}

static inline ProfilePrinterCtx *AREQ_ProfilePrinterCtx(AREQ *req) {
  return &req->profileCtx;
}

static inline RedisSearchCtx *AREQ_SearchCtx(AREQ *req) {
  return req->sctx;
}

static inline AGGPlan *AREQ_AGGPlan(AREQ *req) {
  return &req->pipeline.ap;
}

/******************************************************************************
 ******************************************************************************
 ** Grouper Functions                                                        **
 ******************************************************************************
 ******************************************************************************/

/**
 * Creates a new grouper object. This is equivalent to a GROUPBY clause.
 * A `Grouper` object contains at the minimum, the keys on which it groups
 * (indicated by the srckeys) and the keys on which it outputs (indicated by
 * dstkeys).
 *
 * The Grouper will create a new group for each unique cartesian of values found
 * in srckeys within each row, and invoke associated Reducers (can be added via
 * @ref Grouper_AddReducer()) within that context.
 *
 * The srckeys and dstkeys parameters are mirror images of one another, but are
 * necessary because a reducer function will convert and reduce one or more
 * source rows into a single destination row. The srckeys are the values to
 * group by within the source rows, and the dstkeys are the values as they are
 * stored within the destination rows. It is assumed that two RLookups are used
 * like so:
 *
 * @code {.c}
 * RLookup lksrc = RLookup_New();
 * RLookup lkdst = RLookup_New();
 * const char *kname[] = {"foo", "bar", "baz"};
 * RLookupKey *srckeys[3];
 * RLookupKey *dstkeys[3];
 * for (size_t ii = 0; ii < 3; ++ii) {
 *  srckeys[ii] = RLookup_GetKey(&lksrc, kname[ii], RLOOKUP_F_OCREAT);
 *  dstkeys[ii] = RLookup_GetKey(&lkdst, kname[ii], RLOOKUP_F_OCREAT);
 * }
 * @endcode
 *
 * ResultProcessors (and a grouper is a ResultProcessor) before the grouper
 * should write their data using `lksrc` as a reference point.
 */
Grouper *Grouper_New(const RLookupKey **srckeys, const RLookupKey **dstkeys, size_t n,
                     GroupByLimits groupByLimits);

void Grouper_Free(Grouper *g);

/**
 * Gets the result processor associated with the grouper.
 * This is used for building the query pipeline
 */
ResultProcessor *Grouper_GetRP(Grouper *gr);

/**
 * Adds a reducer to the grouper. This must be called before any results are
 * processed by the grouper.
 */
void Grouper_AddReducer(Grouper *g, Reducer *r, RLookupKey *dst);

void AREQ_Execute(AREQ *req, RedisModuleCtx *outctx);
void sendChunk(AREQ *req, RedisModule_Reply *reply, size_t limit);
void sendChunk_ReplyOnly_EmptyResults(RedisModuleCtx *ctx, AREQ *req);

/**
 * Increment the reference count of the AREQ.
 * @param req the request to increment
 * @return the request (for chaining)
 */
AREQ *AREQ_IncrRef(AREQ *req);

/**
 * Decrement the reference count of the AREQ.
 * If the reference count reaches 0, the request is freed.
 * @param req the request to decrement
 */
void AREQ_DecrRef(AREQ *req);

/**
 * Free a cursor parked in `req->storedReplyState.cursor`, if any.
 * Used by cleanup paths to release a cursor left behind when the
 * blocked-client timeout fires before the reply callback runs and
 * drains it via `AREQ_ReplyWithStoredResults`.
 * No-op when `storedReplyState.cursor` is NULL.
 */
void AREQ_CleanUpStoredCursor(AREQ *req);

/**
 * Start the cursor on the current request
 * @param r the request
 * @param reply the context used for replies (only used in current command)
 * @param spec_ref a strong reference to the spec. The cursor saves a weak reference to the spec
 * to be promoted when cursor read is called.
 * @param status if this function errors, this contains the message
 * @param coord if true, this is a coordinator cursor
 * @return REDISMODULE_OK or REDISMODULE_ERR
 *
 * If this function returns REDISMODULE_OK then the cursor might have been
 * freed. If it returns REDISMODULE_ERR, then the cursor is still valid
 * and must be freed manually.
 */
int AREQ_StartCursor(AREQ *r, RedisModule_Reply *reply, StrongRef spec_ref, QueryError *status, bool coord);

int RSCursorReadCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int RSCursorProfileCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int RSCursorDelCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int RSCursorGCCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);

/**
 * @brief Parse a dialect version from var args
 *
 * @param dialect pointer to unsigned int to store the parsed value
 * @param ac ArgsCruser set to point on the dialect version position in the var args list
 * @param status QueryError struct to contain error messages
 * @return int REDISMODULE_OK in case of successful parsing, REDISMODULE_ERR otherwise
 */
int parseDialect(unsigned int *dialect, ArgsCursor *ac, QueryError *status);


int parseValueFormat(uint32_t *flags, ArgsCursor *ac, QueryError *status);
int parseTimeout(size_t *timeout, ArgsCursor *ac, QueryError *status);
int SetValueFormat(bool is_resp3, bool is_json, uint32_t *flags, QueryError *status);
void SetSearchCtx(RedisSearchCtx *sctx, const AREQ *req);

// From dist_aggregate.c
// Allows calling parseProfileArgs from reply_empty.c
int parseProfileArgs(RedisModuleString **argv, int argc, AREQ *r);

static inline bool AREQ_TimedOut(AREQ *req) {
  return RS_AtomicBoolLoadRelaxed(&req->syncCtx.timedOut);
}
static inline void AREQ_SetTimedOut(AREQ *req) {
  RS_AtomicBoolStoreRelaxed(&req->syncCtx.timedOut, true);
}
#ifdef ENABLE_ASSERT
// SyncPointStopFn predicate adapter for AREQ_TimedOut. Pass the AREQ as `arg`
// to SyncPoint_WaitUntil to release the wait when the request is timed out.
bool areq_timed_out(void *arg);
#endif

/* Non-inline named bridge over AREQ_TimedOut, invoked by Rust query
 * iterators on the Blocked Client Timeout path. The named extern is a
 * stable symbol that LTO can inline through. */
bool AREQ_CheckTimedOut(AREQ *areq);

/* True when this AREQ uses the BG-thread / timeout-callback claim handshake
 * around AggregateResults (TryClaim/Signal/Wait). Currently set only on the
 * coordinator AREQ under RETURN-STRICT; all other paths skip the protocol. */
static inline bool AREQ_RequiresThreadsSyncResults(const AREQ *req) {
  return req->syncCtx.requiresAggregateResultsSync;
}

/* TryClaim: atomic CAS on `aggregatingResults`; winner runs AggregateResults.
 * Signal: called by winner at completion. Wait: called by loser, blocks until Signal.
 * Exactly one of {BG thread, timeout callback} wins. */
bool AREQ_TryClaimAggregateResults(AREQ *req);
void AREQ_SignalAggregateResultsComplete(AREQ *req);
void AREQ_WaitForAggregateResultsComplete(AREQ *req);

/* Reset the per-cursor-read sync state on a coordinator RETURN_STRICT cursor
 * read so the next chunk starts from a clean slate. Resets:
 *   - syncCtx.aggregatingResults (CAS claim)
 *   - syncCtx.aggregateResultsDone (signal latch)
 *   - syncCtx.timedOut (timer latch from the previous chunk's timer)
 *   - RPNet::drainOnly on the root proc when it is RP_NETWORK (so the next
 *     read does not short-circuit to EOF on the first empty-channel observation).
 * Caller MUST hold the per-request setRequestLock so the timer cannot publish a
 * fresh TimedOut between the reset and SetRequest. Does NOT reset
 * RPSorter::base.Next: the Yield latch is load-bearing across reads. */
void AREQ_ResetForCursorReadReturnStrict(AREQ *req);

/* Abort-wake registration (single-slot). BG reader registers its blocking channel
 * before reading; timeout callback flips `timedOut` then broadcasts to wake it.
 * Operates on RequestSyncCtx so AREQ and HybridRequest can share. */
void RequestSyncCtx_RegisterAbortWakeChannel(RequestSyncCtx *ctx, struct MRChannel *chan);
void RequestSyncCtx_UnregisterAbortWakeChannel(RequestSyncCtx *ctx);
void RequestSyncCtx_WakeAbortChannel(RequestSyncCtx *ctx);

static inline bool AREQ_ShouldCheckTimeout(AREQ *req) {
  return !req->skipTimeoutChecks;
}

static inline void AREQ_SetSkipTimeoutChecks(AREQ *req, bool skipTimeoutChecks) {
  req->skipTimeoutChecks = skipTimeoutChecks;
  // Also propagate to the SearchCtx's SearchTime for timeout functions that access it directly
  if (req->sctx) {
    req->sctx->time.skipTimeoutChecks = skipTimeoutChecks;
  }
}

// Returns the AREQ that iterator constructors should use to wire the
// Blocked Client Timeout, or NULL if iterators should fall back to the
// in-pipeline clock-based timeout. `skipTimeoutChecks` is set by
// `AREQ_ApplyContext` exactly when the BC callback is the active source.
static inline AREQ *AREQ_TimeoutAreqOrNull(AREQ *req) {
  return (req && req->skipTimeoutChecks) ? req : NULL;
}

static inline bool RequestConfig_ApplyCoordinatorElapsedTime(RequestConfig *reqConfig,
                                                             rs_wall_clock_ns_t coordinatorElapsedTime) {
  // Only adjust the timeout for 'fail' and 'return-strict' policies.
  // 'return' policy keeps the original timeout for backwards compatibility.
  if (reqConfig->timeoutPolicy == TimeoutPolicy_Return) {
    return false;
  }

  if (reqConfig->queryTimeoutMS == 0) {
    return false;
  }

  const rs_wall_clock_ms_t elapsedMS = rs_wall_clock_convert_ns_to_ms(coordinatorElapsedTime);

  if (elapsedMS >= (rs_wall_clock_ms_t)reqConfig->queryTimeoutMS) {
    reqConfig->queryTimeoutMS = 1; // Avoid underflow, and reserved 0 for "no timeout"
    return true;
  }

  reqConfig->queryTimeoutMS -= (long long)elapsedMS;
  return false;
}

void AREQ_ReplyOrStoreError(AREQ *req, RedisModuleCtx *ctx, QueryError *status);
void AREQ_ReplyWithStoredResults(RedisModuleCtx *ctx, AREQ *req);

#define AREQ_RP(req) AREQ_QueryProcessingCtx(req)->endProc

#undef RS_Atomic

#ifdef __cplusplus
}
#endif
#endif
