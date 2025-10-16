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

#include <stdbool.h>
#include "value.h"
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

#include "rmutil/rm_assert.h"

#ifdef __cplusplus
extern "C" {
#endif

#define DEFAULT_LIMIT 10

/** Cached variables to avoid serializeResult retrieving these each time */
typedef struct {
  RLookup *lastLookup;
  const PLN_ArrangeStep *lastAstp;
} cachedVars;

typedef struct Grouper Grouper;
struct QOptimizer;

/*
 * A query can be of one type. So QEXEC_F_IS_AGGREGATE, QEXEC_F_IS_SEARCH, QEXEC_F_IS_HYBRID_TAIL,
 * QEXEC_F_IS_HYBRID_SEARCH_SUBQUERY, QEXEC_F_IS_HYBRID_VECTOR_AGGREGATE_SUBQUERY are mutually exclusive (Only one can be set).
 */
typedef enum {
  QEXEC_F_IS_AGGREGATE = 0x01,    // Is an aggregate command
  QEXEC_F_SEND_SCORES = 0x02,     // Output: Send scores with each result
  QEXEC_F_SEND_SORTKEYS = 0x04,   // Sent the key used for sorting, for each result
  QEXEC_F_SEND_NOFIELDS = 0x08,   // Don't send the contents of the fields
  QEXEC_F_SEND_PAYLOADS = 0x10,   // Sent the payload set with ADD
  QEXEC_F_IS_CURSOR = 0x20,       // Is a cursor-type query
  QEXEC_F_REQUIRED_FIELDS = 0x40, // Send multiple required fields

  /**
   * Do not create the root result processor. Only process those components
   * which process fully-formed, fully-scored results. This also means
   * that a scorer is not created. It will also not initialize the
   * first step or the initial lookup table
   */
  QEXEC_F_BUILDPIPELINE_NO_ROOT = 0x80,

  /**
   * Add the ability to run the query in a multi threaded environment
   */
  QEXEC_F_RUN_IN_BACKGROUND = 0x100,

  /* The query is a search command */
  QEXEC_F_IS_SEARCH = 0x200,

  /* Highlight/summarize options are active */
  QEXEC_F_SEND_HIGHLIGHT = 0x400,

  /* Do not emit any rows, only the number of query results */
  QEXEC_F_NOROWS = 0x800,

  /* Do not stringify result values. Send them in their proper types */
  QEXEC_F_TYPED = 0x1000,

  /* Send raw document IDs alongside key names. Used for debugging */
  QEXEC_F_SENDRAWIDS = 0x2000,

  /* Flag for scorer function to create explanation strings */
  QEXEC_F_SEND_SCOREEXPLAIN = 0x4000,

  /* Profile command */
  QEXEC_F_PROFILE = 0x8000,
  QEXEC_F_PROFILE_LIMITED = 0x10000,

  /* FT.AGGREGATE load all fields */
  QEXEC_AGG_LOAD_ALL = 0x20000,

  /* Optimize query */
  QEXEC_OPTIMIZE = 0x40000,

  // Compound values are expanded (RESP3 w/JSON)
  QEXEC_FORMAT_EXPAND = 0x80000,

  // Compound values are returned serialized (RESP2 or HASH) or expanded (RESP3 w/JSON)
  QEXEC_FORMAT_DEFAULT = 0x100000,

  // Set the score of the doc to an RLookupKey in the result
  QEXEC_F_SEND_SCORES_AS_FIELD = 0x200000,

  // The query is internal (responding to a command from the coordinator)
  QEXEC_F_INTERNAL = 0x400000,

  // The query is a Hybrid Request
  QEXEC_F_IS_HYBRID_TAIL = 0x800000,

  // The query is a Search Subquery of a Hybrid Request
  QEXEC_F_IS_HYBRID_SEARCH_SUBQUERY = 0x1000000,

  // The query is a Vector Subquery of a Hybrid Request (aggregate equivalent)
  QEXEC_F_IS_HYBRID_VECTOR_AGGREGATE_SUBQUERY = 0x2000000,

  // The query has an explicit SORT BY 0 step - no sorting at all
  // Currently only used in when QEXEC_F_IS_HYBRID_TAIL is set - i.e this is the tail part
  QEXEC_F_NO_SORT = 0x4000000,

  // The query is for debugging. Note that this is the last bit of uint32_t
  QEXEC_F_DEBUG = 0x80000000,

} QEFlags;

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
} ParseAggPlanContext;

#define IsCount(r) ((r)->reqflags & QEXEC_F_NOROWS)
#define IsSearch(r) ((r)->reqflags & QEXEC_F_IS_SEARCH)
#define IsHybridTail(r) ((r)->reqflags & QEXEC_F_IS_HYBRID_TAIL)
#define IsHybridSearchSubquery(r) ((r)->reqflags & QEXEC_F_IS_HYBRID_SEARCH_SUBQUERY)
#define IsHybridVectorSubquery(r) ((r)->reqflags & QEXEC_F_IS_HYBRID_VECTOR_AGGREGATE_SUBQUERY)
#define IsHybrid(r) (IsHybridTail(r) || IsHybridSearchSubquery(r) || IsHybridVectorSubquery(r))
#define IsProfile(r) ((r)->reqflags & QEXEC_F_PROFILE)
#define IsOptimized(r) ((r)->reqflags & QEXEC_OPTIMIZE)
#define IsFormatExpand(r) ((r)->reqflags & QEXEC_FORMAT_EXPAND)
#define IsWildcard(r) ((r)->ast.root->type == QN_WILDCARD)
#define HasScorer(r) ((r)->optimizer && (r)->optimizer->scorerType != SCORER_TYPE_NONE)
#define HasLoader(r) ((r)->stateflags & QEXEC_S_HAS_LOAD)
#define IsScorerNeeded(r) ((r)->reqflags & (QEXEC_F_SEND_SCORES | QEXEC_F_SEND_SCORES_AS_FIELD))
#define HasScoreInPipeline(r) ((r)->reqflags & QEXEC_F_SEND_SCORES_AS_FIELD)
#define IsInternal(r) ((r)->reqflags & QEXEC_F_INTERNAL)
#define IsDebug(r) ((r)->reqflags & QEXEC_F_DEBUG)

// Indicates whether a query should run in the background. This
// will also guarantee that there is a running thread pool with al least 1 thread.
#define RunInThread() (RSGlobalConfig.numWorkerThreads)

typedef void (*profiler_func)(RedisModule_Reply *reply, void *ctx);

typedef enum {
  /* Pipeline has a loader */
  QEXEC_S_HAS_LOAD = 0x01,
  /* Received EOF from iterator */
  QEXEC_S_ITERDONE = 0x02,
} QEStateFlags;


typedef enum { COMMAND_AGGREGATE, COMMAND_SEARCH, COMMAND_EXPLAIN } CommandType;
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
  rs_wall_clock initClock;                      // Time of start. Reset for each cursor call
  rs_wall_clock_ns_t profileTotalTime;          // Total time. Used to accumulate cursors times
  rs_wall_clock_ns_t profileParseTime;          // Time for parsing the query
  rs_wall_clock_ns_t profilePipelineBuildTime;  // Time for creating the pipeline

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
int AREQ_Compile(AREQ *req, RedisModuleString **argv, int argc, QueryError *status);

/**
 * Parse aggregate plan arguments (GROUPBY, APPLY, LOAD, FILTER) from an ArgsCursor.
 * This function extracts the aggregate-specific parsing logic that was previously
 * part of AREQ_Compile, allowing it to be reused for merge plans in hybrid queries.
 */
int parseAggPlan(ParseAggPlanContext *ctx, ArgsCursor *ac, QueryError *status);

/**
 * Initialize basic AREQ structure with search options and aggregation plan.
 */
void initializeAREQ(AREQ *req);

/**
 * This stage will apply the context to the request. During this phase, the
 * query will be parsed (and matched according to the schema), and the reducers
 * will be loaded and analyzed.
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
int AREQ_BuildPipeline(AREQ *req, QueryError *status);

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

static inline QueryProcessingCtx *AREQ_QueryProcessingCtx(AREQ *req) {
  return &req->pipeline.qctx;
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
 * RLookup lksrc;
 * RLookup lkdst;
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
Grouper *Grouper_New(const RLookupKey **srckeys, const RLookupKey **dstkeys, size_t n);

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
int prepareExecutionPlan(AREQ *req, QueryError *status);
void sendChunk(AREQ *req, RedisModule_Reply *reply, size_t limit);
void AREQ_Free(AREQ *req);

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

int RSCursorCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);

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
int parseTimeout(long long *timeout, ArgsCursor *ac, QueryError *status);
int SetValueFormat(bool is_resp3, bool is_json, uint32_t *flags, QueryError *status);
void SetSearchCtx(RedisSearchCtx *sctx, const AREQ *req);
int prepareRequest(AREQ **r_ptr, RedisModuleCtx *ctx, RedisModuleString **argv, int argc, CommandType type, int execOptions, QueryError *status);


#define AREQ_RP(req) AREQ_QueryProcessingCtx(req)->endProc

#ifdef __cplusplus
}
#endif
#endif
