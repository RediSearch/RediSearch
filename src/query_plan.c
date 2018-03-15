#include "query_plan.h"
#include "config.h"
#include "value.h"
#include "aggregate/aggregate.h"

/******************************************************************************************************
 *   Query Plan - the actual binding context of the whole execution plan - from filters to
 *   processors
 ******************************************************************************************************/

static size_t serializeResult(QueryPlan *qex, SearchResult *r, RSSearchFlags flags) {
  size_t count = 0;

  RedisModuleCtx *ctx = qex->ctx->redisCtx;
  if (r->md && !(qex->opts.flags & Search_AggregationQuery)) {
    size_t klen;
    const char *k = DMD_KeyPtrLen(r->md, &klen);
    count += 1;
    RedisModule_ReplyWithStringBuffer(ctx, k, klen);
  }

  if (flags & Search_WithScores) {
    RedisModule_ReplyWithDouble(ctx, r->score);
    count++;
  }

  if (flags & Search_WithPayloads) {
    ++count;
    const RSPayload *payload = r->md ? r->md->payload : NULL;
    if (payload) {
      RedisModule_ReplyWithStringBuffer(ctx, payload->data, payload->len);
    } else {
      RedisModule_ReplyWithNull(ctx);
    }
  }

  if (flags & Search_WithSortKeys) {
    ++count;
    const RSValue *sortkey = RSSortingVector_Get(r->sv, qex->opts.sortBy);
    if (sortkey) {
      switch (sortkey->t) {
        case RSValue_Number:
          /* Serialize double - by prepending "%" to the number, so the coordinator/client can tell
           * it's a double and not just a numeric string value */
          RedisModule_ReplyWithString(
              ctx, RedisModule_CreateStringPrintf(ctx, "#%.17g", sortkey->numval));
          break;
        case RSValue_String:
          /* Serialize string - by prepending "$" to it */

          RedisModule_ReplyWithString(ctx,
                                      RedisModule_CreateStringPrintf(ctx, "$%s", sortkey->strval));
          break;
        case RSValue_RedisString:
          RedisModule_ReplyWithString(
              ctx, RedisModule_CreateStringPrintf(
                       ctx, "$%s", RedisModule_StringPtrLen(sortkey->rstrval, NULL)));
          break;
        default:
          // NIL, or any other type:
          RedisModule_ReplyWithNull(ctx);
      }
    }

    else {
      RedisModule_ReplyWithNull(ctx);
    }
  }

  if (!(flags & Search_NoContent)) {
    count++;
    size_t fieldCount = r->fields ? r->fields->len : 0;
    RedisModule_ReplyWithArray(ctx, fieldCount * 2);
    for (int i = 0; i < fieldCount; i++) {
      RedisModule_ReplyWithStringBuffer(ctx, r->fields->fields[i].key,
                                        strlen(r->fields->fields[i].key));
      RSValue_SendReply(ctx, RSFieldMap_Item(r->fields, i));
    }
  }
  return count;
}

/**
 * Returns true if the query has timed out and the user has requested
 * that we do not drain partial results.
 */
#define HAS_TIMEOUT_FAILURE(qex) \
  ((qex)->execCtx.state == QueryState_TimedOut && (qex)->opts.timeoutPolicy == TimeoutPolicy_Fail)

int Query_SerializeResults(QueryPlan *qex) {
  int rc;
  int count = 0;
  RedisModuleCtx *ctx = qex->ctx->redisCtx;

  do {
    SearchResult r = SEARCH_RESULT_INIT;
    rc = ResultProcessor_Next(qex->rootProcessor, &r, 1);

    if (rc == RS_RESULT_EOF) {
      break;
    }

    if (HAS_TIMEOUT_FAILURE(qex)) {
      RSFieldMap_Free(r.fields, 0);
      break;
    }

    // printf("Read result %d, rc %d\n", r.docId, rc);
    if (count == 0) {
      RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
      RedisModule_ReplyWithLongLong(ctx, ResultProcessor_Total(qex->rootProcessor));
      count++;
    }
    count += serializeResult(qex, &r, qex->opts.flags);

    // IndexResult_Free(r.indexResult);
    RSFieldMap_Free(r.fields, 0);
    r.fields = NULL;
  } while (rc != RS_RESULT_EOF);

  if (count == 0) {
    if (HAS_TIMEOUT_FAILURE(qex)) {
      return RedisModule_ReplyWithError(ctx, "Command timed out");
    }

    RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
    RedisModule_ReplyWithLongLong(ctx, ResultProcessor_Total(qex->rootProcessor));
    count++;
  }

  RedisModule_ReplySetArrayLength(ctx, count);
  return REDISMODULE_OK;
}

/* A callback called when we regain concurrent execution context, and the index spec key is
 * reopened. We protect against the case that the spec has been deleted during query execution
 */
void Query_OnReopen(RedisModuleKey *k, void *privdata) {

  IndexSpec *sp = RedisModule_ModuleTypeGetValue(k);
  QueryPlan *q = privdata;

  // If we don't have a spec or key - we abort the query
  if (k == NULL || sp == NULL) {

    q->execCtx.state = QueryState_Aborted;
    q->ctx->spec = NULL;
    return;
  }

  // The spec might have changed while we were sleeping - for example a realloc of the doc table
  q->ctx->spec = sp;

  if (RSGlobalConfig.queryTimeoutMS > 0) {
    // Check the elapsed processing time
    static struct timespec now;
    clock_gettime(CLOCK_MONOTONIC_RAW, &now);

    long long durationNS = (long long)1000000000 * (now.tv_sec - q->execCtx.startTime.tv_sec) +
                           (now.tv_nsec - q->execCtx.startTime.tv_nsec);
    // printf("Elapsed: %zdms\n", durationNS / 1000000);
    // Abort on timeout
    if (durationNS > q->opts.timeoutMS * 1000000) {
      q->execCtx.state = QueryState_TimedOut;
    }
  }
  // q->docTable = &sp->docs;
}

void QueryPlan_Free(QueryPlan *plan) {
  if (plan->rootProcessor) {
    ResultProcessor_Free(plan->rootProcessor);
  }
  if (plan->rootFilter) {
    plan->rootFilter->Free(plan->rootFilter);
  }
  if (plan->conc) {
    ConcurrentSearchCtx_Free(plan->conc);
    free(plan->conc);
  }

  free(plan);
}

/* Evaluate the query, and return 1 on success */
static int queryPlan_EvalQuery(QueryPlan *plan, QueryParseCtx *parsedQuery, RSSearchOptions *opts) {
  QueryEvalCtx ev = {.docTable = plan->ctx && plan->ctx->spec ? &plan->ctx->spec->docs : NULL,
                     .conc = plan->conc,
                     .numTokens = parsedQuery->numTokens,
                     .tokenId = 1,
                     .sctx = plan->ctx,
                     .opts = opts};

  plan->rootFilter = Query_EvalNode(&ev, parsedQuery->root);
  return plan->rootFilter ? 1 : 0;
}

QueryPlan *Query_BuildPlan(RedisSearchCtx *ctx, QueryParseCtx *parsedQuery, RSSearchOptions *opts,
                           ProcessorChainBuilder pcb, void *chainBuilderContext, char **err) {
  QueryPlan *plan = calloc(1, sizeof(*plan));
  plan->ctx = ctx;
  plan->conc = opts->concurrentMode ? malloc(sizeof(*plan->conc)) : NULL;
  plan->opts = opts ? *opts : RS_DEFAULT_SEARCHOPTS;
  if (plan->opts.timeoutMS == 0) {
    plan->opts.timeoutMS = RSGlobalConfig.queryTimeoutMS;
  }
  if (plan->opts.timeoutPolicy == TimeoutPolicy_Default) {
    plan->opts.timeoutPolicy = RSGlobalConfig.timeoutPolicy;
  }

  plan->execCtx = (QueryProcessingCtx){
      .errorString = NULL,
      .minScore = 0,
      .totalResults = 0,
      .state = QueryState_OK,
      .sctx = plan->ctx,
      .conc = plan->conc,
  };
  clock_gettime(CLOCK_MONOTONIC_RAW, &plan->execCtx.startTime);
  if (plan->conc) {
    ConcurrentSearchCtx_Init(ctx->redisCtx, plan->conc);
    ConcurrentSearch_AddKey(plan->conc, plan->ctx->key, REDISMODULE_READ, plan->ctx->keyName,
                            Query_OnReopen, plan, NULL, ConcurrentKey_SharedKeyString);
  }
  if (!queryPlan_EvalQuery(plan, parsedQuery, opts)) {
    QueryPlan_Free(plan);
    return NULL;
  }
  plan->execCtx.rootFilter = plan->rootFilter;
  plan->rootProcessor = pcb(plan, chainBuilderContext, err);
  if (!plan->rootProcessor) {
    QueryPlan_Free(plan);
    return NULL;
  }
  return plan;
}

int QueryPlan_Run(QueryPlan *plan, char **err) {
  plan->bc = NULL;

  *err = NULL;
  RedisModuleCtx *ctx = plan->ctx->redisCtx;
  int rc = Query_SerializeResults(plan);
  *err = plan->execCtx.errorString;
  return rc;
}
