#include "query_plan.h"
#include "config.h"
#include "aggregate/aggregate.h"

/******************************************************************************************************
 *   Query Plan - the actual binding context of the whole execution plan - from filters to
 *   processors
 ******************************************************************************************************/

static size_t serializeResult(QueryPlan *qex, SearchResult *r, RSSearchFlags flags) {
  size_t count = 0;

  RedisModuleCtx *ctx = qex->ctx->redisCtx;
  if (r->md) {
    count += 1;
    RedisModule_ReplyWithStringBuffer(ctx, r->md->key, strlen(r->md->key));
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
    const RSSortableValue *sortkey = RSSortingVector_Get(r->sv, qex->opts.sortBy);
    if (sortkey) {
      if (sortkey->type == RS_SORTABLE_NUM) {
        RedisModule_ReplyWithDouble(ctx, sortkey->num);
      } else {
        RedisModule_ReplyWithStringBuffer(ctx, sortkey->str, strlen(sortkey->str));
      }
    } else {
      RedisModule_ReplyWithNull(ctx);
    }
  }

  if (!(flags & Search_NoContent)) {
    count++;
    RedisModule_ReplyWithArray(ctx, r->fields->len * 2);
    for (int i = 0; i < r->fields->len; i++) {
      RedisModule_ReplyWithStringBuffer(ctx, r->fields->fields[i].key,
                                        strlen(r->fields->fields[i].key));
      RSValue_SendReply(ctx, RSFieldMap_Item(r->fields, i));
    }
  }
  return count;
}

int Query_SerializeResults(QueryPlan *qex) {
  int rc;
  int count = 0;
  RedisModuleCtx *ctx = qex->ctx->redisCtx;
  RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);

  do {
    SearchResult r = SEARCH_RESULT_INIT;
    rc = ResultProcessor_Next(qex->rootProcessor, &r, 1);

    if (rc == RS_RESULT_EOF) break;
    // printf("Read result %d, rc %d\n", r.docId, rc);
    if (count == 0) {
      RedisModule_ReplyWithLongLong(ctx, ResultProcessor_Total(qex->rootProcessor));
      count++;
    }
    count += serializeResult(qex, &r, qex->opts.flags);

    // IndexResult_Free(r.indexResult);
    // RSFieldMap_Free(r.fields, 0);
  } while (rc != RS_RESULT_EOF);
  if (count == 0) {
    RedisModule_ReplyWithLongLong(ctx, ResultProcessor_Total(qex->rootProcessor));
    count++;
  }
  RedisModule_ReplySetArrayLength(ctx, count);
  return REDISMODULE_OK;
}

/* A callback called when we regain concurrent execution context, and the index spec key is
 * reopened. We protect against the case that the spec has been deleted during query execution */
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
    if (durationNS > RSGlobalConfig.queryTimeoutMS * 1000000) {
      q->execCtx.state = QueryState_TimedOut;
    }
  }
  // q->docTable = &sp->docs;
}

int QueryPlan_Execute(QueryPlan *plan, const char **err) {
  int rc = Query_SerializeResults(plan);
  if (err) *err = plan->execCtx.errorString;
  return rc;
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
                           ProcessorChainBuilder pcb, void *chainBuilderContext) {
  QueryPlan *plan = calloc(1, sizeof(*plan));
  plan->ctx = ctx;
  plan->conc = opts->concurrentMode ? malloc(sizeof(*plan->conc)) : NULL;
  plan->opts = opts ? *opts : RS_DEFAULT_SEARCHOPTS;

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
                            Query_OnReopen, plan, NULL, 0);
  }
  queryPlan_EvalQuery(plan, parsedQuery, opts);
  plan->execCtx.rootFilter = plan->rootFilter;
  plan->rootProcessor = pcb(plan, chainBuilderContext);
  return plan;
}

int runQueryPlan(QueryPlan *plan) {

  // Execute the query
  char *err;
  RedisModuleCtx *ctx = plan->ctx->redisCtx;
  int rc = QueryPlan_Execute(plan, (const char **)&err);
  if (rc == REDISMODULE_ERR) {
    RedisModule_ReplyWithError(ctx, QUERY_ERROR_INTERNAL_STR);
  }
  QueryPlan_Free(plan);
  return rc;
}

// process the query in the thread pool - thread pool callback
void threadProcessPlan(void *p) {
  QueryPlan *plan = p;
  RedisModuleCtx *ctx = RedisModule_GetThreadSafeContext(plan->bc);
  RedisModule_AutoMemory(ctx);

  RedisModule_ThreadSafeContextLock(ctx);
  plan->ctx = NewSearchCtx(
      ctx, RedisModule_CreateString(ctx, plan->opts.indexName, strlen(plan->opts.indexName)));

  if (!plan->ctx) {
    RedisModule_ReplyWithError(ctx, "Unknown Index name");
  } else {
    runQueryPlan(plan);
  }

  RedisModule_ThreadSafeContextUnlock(ctx);
  RedisModule_UnblockClient(plan->bc, NULL);
  QueryPlan_Free(plan);
  RedisModule_FreeThreadSafeContext(ctx);

  return;
  //  return REDISMODULE_OK;
}

int QueryPlan_ProcessInThreadpool(RedisModuleCtx *ctx, QueryPlan *plan) {
  plan->bc = RedisModule_BlockClient(ctx, NULL, NULL, NULL, 0);
  ConcurrentSearch_ThreadPoolRun(threadProcessPlan, plan, CONCURRENT_POOL_SEARCH);
  return REDISMODULE_OK;
}

int QueryPlan_ProcessMainThread(RedisSearchCtx *sctx, QueryPlan *plan) {
  plan->ctx = sctx;
  plan->bc = NULL;

  int rc = runQueryPlan(plan);
  QueryPlan_Free(plan);
  return rc;
}
