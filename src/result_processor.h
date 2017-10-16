#ifndef RS_RESULT_PROCESSOR_H_
#define RS_RESULT_PROCESSOR_H_

#include "redisearch.h"
#include "sortable.h"
#include "value.h"
#include "concurrent_ctx.h"

typedef enum {
  QueryState_OK,
  QueryState_Aborted,
  QueryState_Error,
} QueryState;

typedef struct {
  ConcurrentSearchCtx *conc;
  double minScore;
  uint32_t totalResults;
  char *errorString;
  QueryState state;
} QueryProcessingCtx;

/******************************************************************************************************
 *   Result Processor Definitions
 ******************************************************************************************************/
typedef struct {
  t_docId docId;

  // not all results have score - TBD
  double score;

  RSSortingVector *sv;

  RSDocumentMetadata *md;

  // index result should cover what you need for highlighting,
  // but we will add a method to duplicate index results to make
  // them thread safe
  RSIndexResult *indexResult;

  // dynamic fields
  RSFieldMap *fields;
} SearchResult;

#define RS_RESULT_OK 0
#define RS_RESULT_QUEUED 1
#define RS_RESULT_EOF 2

typedef struct {
  void *privdata;
  struct resultProcessor *upstream;
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

#endif  // !RS_RESULT_PROCESSOR_H_
