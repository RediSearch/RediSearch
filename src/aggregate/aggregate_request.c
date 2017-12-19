#include "aggregate.h"
#include "reducer.h"
#include <result_processor.h>
#include <rmutil/cmdparse.h>

static CmdSchemaNode *requestSchema = NULL;

typedef struct {
  const char *indexName;
  RedisSearchCtx *sctx;

  CmdArg *parsedReqeust;

} AggregateRequest;

ResultProcessor *AggregateRequest_BuildPipeline(AggregateRequest *req) {
}