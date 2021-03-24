#ifndef RS_AGGREGATE_H__
#define RS_AGGREGATE_H__
#include "value.h"
#include "query.h"
#include "reducer.h"
#include "result_processor.h"
#include "expr/expression.h"
#include "aggregate_plan.h"
#include "rmutil/rm_assert.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct Grouper Grouper;

typedef enum {
  QEXEC_F_IS_EXTENDED = 0x01,    // Contains aggregations or projections
  QEXEC_F_SEND_SCORES = 0x02,    // Output: Send scores with each result
  QEXEC_F_SEND_SORTKEYS = 0x04,  // Sent the key used for sorting, for each result
  QEXEC_F_SEND_NOFIELDS = 0x08,  // Don't send the contents of the fields
  QEXEC_F_SEND_PAYLOADS = 0x10,  // Sent the payload set with ADD
  QEXEC_F_IS_CURSOR = 0x20,      // Is a cursor-type query

  /** Don't use concurrent execution */
  QEXEC_F_SAFEMODE = 0x100,

  /* The inverse of IS_EXTENDED. The two cannot coexist together */
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

} QEFlags;

#define IsCount(r) ((r)->reqflags & QEXEC_F_NOROWS)
#define IsSearch(r) ((r)->reqflags & QEXEC_F_IS_SEARCH)
#define IsProfile(r) ((r)->reqflags & QEXEC_F_PROFILE)

typedef enum {
  /* Received EOF from iterator */
  QEXEC_S_ITERDONE = 0x02,
} QEStateFlags;

typedef struct {
  /* plan containing the logical sequence of steps */
  AGGPlan ap;

  /* Arguments converted to sds. Received on input */
  sds *args;
  size_t nargs;

  /** Search query string */
  const char *query;

  /** Fields to be output and otherwise processed */
  FieldList outFields;

  /** Options controlling search behavior */
  RSSearchOptions searchopts;

  /** Parsed query tree */
  QueryAST ast;

  /** Root iterator. This is owned by the request */
  IndexIterator *rootiter;

  /** Context, owned by request */
  RedisSearchCtx *sctx;

  /** Resumable context */
  ConcurrentSearchCtx conc;

  /** Context for iterating over the queries themselves */
  QueryIterator qiter;

  /** Flags controlling query output */
  uint32_t reqflags;

  /** Flags indicating current execution state */
  uint32_t stateflags;

  /** Query timeout in milliseconds */
  int32_t reqTimeout;
  struct timespec timeoutTime;

  /** Cursor settings */
  unsigned cursorMaxIdle;
  unsigned cursorChunkSize;

  /** Profile variables */
  clock_t initClock;          // Time of start. Reset for each cursor call
  clock_t totalTime;          // Total time. Used to accimulate cursors times
  clock_t parseTime;          // Time for parsing the query
  clock_t pipelineBuildTime;  // Time for creating the pipeline
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
 * Do not create the root result processor. Only process those components
 * which process fully-formed, fully-scored results. This also means
 * that a scorer is not created. It will also not initialize the
 * first step or the initial lookup table
 */
#define AREQ_BUILDPIPELINE_NO_ROOT 0x01
/**
 * Constructs the pipeline objects needed to actually start processing
 * the requests. This does not yet start iterating over the objects
 */
int AREQ_BuildPipeline(AREQ *req, int options, QueryError *status);

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
void sendChunk(AREQ *req, RedisModuleCtx *outctx, size_t limit);
void AREQ_Free(AREQ *req);

/**
 * Start the cursor on the current request
 * @param r the request
 * @param outctx the context used for replies (only used in current command)
 * @param lookupName the name of the index used for the cursor reservation
 * @param status if this function errors, this contains the message
 * @return REDISMODULE_OK or REDISMODULE_ERR
 *
 * If this function returns REDISMODULE_OK then the cursor might have been
 * freed. If it returns REDISMODULE_ERR, then the cursor is still valid
 * and must be freed manually.
 */
int AREQ_StartCursor(AREQ *r, RedisModuleCtx *outctx, const char *lookupName, QueryError *status);

int RSCursorCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);

#define AREQ_RP(req) (req)->qiter.endProc

#ifdef __cplusplus
}
#endif
#endif
