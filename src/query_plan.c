#include "query_plan.h"
#include "config.h"
#include "value.h"
#include "aggregate/aggregate.h"
#include "rmalloc.h"

/******************************************************************************************************
 *   Query Plan - the actual binding context of the whole execution plan - from filters to
 *   processors
 ******************************************************************************************************/

static size_t serializeResult(QueryPlan *qex, SearchResult *r, RSSearchFlags flags,
                              RedisModuleCtx *ctx) {
  size_t count = 0;
  RSDocumentMetadata *dmd = DocTable_Get(&qex->ctx->spec->docs, r->docId);
  if (!dmd && !(qex->opts.flags & Search_AggregationQuery)) {
    return 0;
  }

  if (!(qex->opts.flags & Search_AggregationQuery) && dmd) {
    size_t klen;
    const char *k = DMD_KeyPtrLen(dmd, &klen);
    count += 1;
    RedisModule_ReplyWithStringBuffer(ctx, k, klen);
  }

  if (flags & Search_WithScores) {
    RedisModule_ReplyWithDouble(ctx, r->score);
    count++;
  }

  if (flags & Search_WithPayloads) {
    ++count;
    if (dmd && dmd->payload) {
      RedisModule_ReplyWithStringBuffer(ctx, dmd->payload->data, dmd->payload->len);
    } else {
      RedisModule_ReplyWithNull(ctx);
    }
  }

  if ((flags & Search_WithSortKeys) && dmd) {
    ++count;
    const RSValue *sortkey = RSSortingVector_Get(dmd->sortVector, qex->opts.sortBy);
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
  ((qex)->execCtx.state == QPState_TimedOut && (qex)->opts.timeoutPolicy == TimeoutPolicy_Fail)

static void Query_SerializeResults(QueryPlan *qex, RedisModuleCtx *output) {
  int rc;
  int count = 0;
  // this just means it's a cursored request
  const int isCursor = qex->opts.flags & Search_IsCursor;
  // this means this is the first cursor call
  const int firstCursor = qex->execCtx.totalResults == 0;
  size_t limit = 0, nrows = 0;

  if (isCursor) {
    if ((limit = qex->opts.chunksize) == 0) {
      /* Always set limit if we're a cursor */
      limit = -1;
    }

    /* Reset per-chunk variables */
    qex->pause = 0;
    qex->count = 0;
    qex->outputFlags = 0;
  }

  do {
    SearchResult r = SEARCH_RESULT_INIT;
    rc = ResultProcessor_Next(qex->rootProcessor, &r, 1);

    if (rc == RS_RESULT_EOF) {
      qex->outputFlags |= QP_OUTPUT_FLAG_DONE;
      break;
    }

    if (HAS_TIMEOUT_FAILURE(qex)) {
      RSFieldMap_Free(r.fields);
      qex->outputFlags |= QP_OUTPUT_FLAG_DONE;
      break;
    }

    // First result!
    if (count == 0) {
      RedisModule_ReplyWithArray(output, REDISMODULE_POSTPONED_ARRAY_LEN);
      // call pre hook if needed
      if (qex->preHook.callback && firstCursor) {
        count += qex->preHook.callback(output, &qex->execCtx, qex->preHook.privdata);
      }
      RedisModule_ReplyWithLongLong(output, ResultProcessor_Total(qex->rootProcessor));
      count++;
    }
    count += serializeResult(qex, &r, qex->opts.flags, output);

    // IndexResult_Free(r.indexResult);
    RSFieldMap_Free(r.fields);
    r.fields = NULL;

    if (limit) {
      if (++nrows >= limit || qex->pause) {
        break;
      }
    }
  } while (1);

  if (count == 0) {
    if (HAS_TIMEOUT_FAILURE(qex)) {
      RedisModule_ReplyWithError(output, "Command timed out");
      qex->outputFlags |= QP_OUTPUT_FLAG_ERROR;
      return;
    }

    RedisModule_ReplyWithArray(output, REDISMODULE_POSTPONED_ARRAY_LEN);
    RedisModule_ReplyWithLongLong(output, ResultProcessor_Total(qex->rootProcessor));
    count++;
  }

  if (qex->postHook.callback && firstCursor) {
    count += qex->postHook.callback(output, &qex->execCtx, qex->postHook.privdata);
  }
  RedisModule_ReplySetArrayLength(output, count);
}

/* A callback called when we regain concurrent execution context, and the index spec key is
 * reopened. We protect against the case that the spec has been deleted during query execution
 */
void Query_OnReopen(RedisModuleKey *k, void *privdata) {

  IndexSpec *sp = RedisModule_ModuleTypeGetValue(k);
  QueryPlan *q = privdata;

  // If we don't have a spec or key - we abort the query
  if (k == NULL || sp == NULL) {

    q->execCtx.state = QPState_Aborted;
    q->ctx->spec = NULL;
    return;
  }

  // The spec might have changed while we were sleeping - for example a realloc of the doc table
  q->ctx->spec = sp;

  // FIXME: Per-query!!
  if (RSGlobalConfig.queryTimeoutMS > 0) {
    // Check the elapsed processing time
    static struct timespec now;
    clock_gettime(CLOCK_MONOTONIC_RAW, &now);

    long long durationNS = (long long)1000000000 * (now.tv_sec - q->execCtx.startTime.tv_sec) +
                           (now.tv_nsec - q->execCtx.startTime.tv_nsec);
    // printf("Elapsed: %zdms\n", durationNS / 1000000);
    // Abort on timeout
    if (durationNS > q->opts.timeoutMS * 1000000) {
      if (q->opts.flags & Search_IsCursor) {
        q->pause = 1;
      } else {
        q->execCtx.state = QPState_TimedOut;
      }
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
    rm_free(plan->conc);
  }
  if (plan->preHook.privdata) {
    if (plan->preHook.free) plan->preHook.free(plan->preHook.privdata);
  }
  if (plan->postHook.privdata) {
    if (plan->postHook.free) plan->postHook.free(plan->postHook.privdata);
  }

  rm_free(plan);
}

static int queryPlan_ValidateNode(QueryNode *node, QueryParseCtx *q, void *ctx) {
#define PHONETIC_ERR_STR "Phonetic requested but field is not declared phonetic"
  QueryError *status = ctx;
  if (node->type != QN_TOKEN) {
    // currently only tokens needs validation
    // maybe in the future we will add more validations to other nodes types
    return 1;
  }
  if (node->opts.fieldMask != RS_FIELDMASK_ALL && node->opts.phonetic != PHONETIC_DEFAULT) {
    char *fieldName = GetFieldNameByBit(q->sctx->spec, node->opts.fieldMask);
    if (!fieldName) {
      QueryError_SetError(status, QUERY_EBADATTR, PHONETIC_ERR_STR);
      return 0;
    }
    FieldSpec *fieldSpec = IndexSpec_GetField(q->sctx->spec, fieldName, strlen(fieldName));
    if (!(fieldSpec->options & FieldSpec_Phonetics)) {
      QueryError_SetError(status, QUERY_EBADATTR, PHONETIC_ERR_STR);
      return 0;
    }
  }
  return 1;
}

/* Evaluate the query, and return 1 on success */
static int queryPlan_ValidateQuery(QueryParseCtx *parsedQuery, void *ctx) {
  return Query_NodeForEach(parsedQuery, queryPlan_ValidateNode, ctx);
}

static int queryPlan_EvalQuery(QueryPlan *plan, QueryParseCtx *parsedQuery, RSSearchOptions *opts,
                               QueryError *status) {
  QueryEvalCtx ev = {.docTable = plan->ctx && plan->ctx->spec ? &plan->ctx->spec->docs : NULL,
                     .conc = plan->conc,
                     .numTokens = parsedQuery->numTokens,
                     .tokenId = 1,
                     .sctx = plan->ctx,
                     .opts = opts};

  plan->rootFilter = Query_EvalNode(&ev, parsedQuery->root);
  if (!plan->rootFilter) {
    QueryError_SetError(status, QUERY_ENORESULTS, NULL);
  }
  return status->code == QUERY_OK ? 1 : 0;
}

QueryPlan *Query_BuildPlan(RedisSearchCtx *ctx, QueryParseCtx *parsedQuery, RSSearchOptions *opts,
                           ProcessorChainBuilder pcb, void *chainBuilderContext,
                           QueryError *status) {
  QueryPlan *plan = rm_calloc(1, sizeof(*plan));
  plan->ctx = ctx;
  // we need to always create the concurent ctx for cursors but we will not switch on safemode
  plan->conc = rm_malloc(sizeof(*plan->conc));
  plan->conc->allowSwitching = opts->concurrentMode;
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
      .state = QPState_Running,
      .sctx = plan->ctx,
      .conc = plan->conc,
  };
  clock_gettime(CLOCK_MONOTONIC_RAW, &plan->execCtx.startTime);
  if (plan->conc) {
    ConcurrentSearchCtx_Init(ctx->redisCtx, plan->conc);
    if (plan->ctx->key) {
      ConcurrentSearch_AddKey(plan->conc, plan->ctx->key, REDISMODULE_READ, plan->ctx->keyName,
                              Query_OnReopen, plan, NULL, ConcurrentKey_SharedKeyString);
    }
  }

  if (parsedQuery) {
    // Is there ever a time when this is not NULL?
    if (!queryPlan_ValidateQuery(parsedQuery, status)) {
      QueryPlan_Free(plan);
      QueryError_SetError(status, QUERY_EINVAL, NULL);
      return NULL;
    }
    if (!queryPlan_EvalQuery(plan, parsedQuery, opts, status)) {
      QueryPlan_Free(plan);
      return NULL;
    }
  }

  plan->execCtx.rootFilter = plan->rootFilter;
  plan->rootProcessor = pcb(plan, chainBuilderContext, status);
  if (!plan->rootProcessor) {
    QueryError_SetError(status, QUERY_ECONSTRUCT_PIPELINE, NULL);
    QueryPlan_Free(plan);
    return NULL;
  }
  return plan;
}

void QueryPlan_Run(QueryPlan *plan, RedisModuleCtx *outputCtx) {
  Query_SerializeResults(plan, outputCtx);
}
void QueryPlan_SetHook(QueryPlan *plan, QueryPlanHookType ht, QueryHookCallback cb, void *privdata,
                       void (*freefn)(void *)) {
  if (ht == QueryPlanHook_Pre) {
    plan->preHook = (QueryPlanHook){.callback = cb, .privdata = privdata, .free = freefn};

  } else {
    plan->postHook = (QueryPlanHook){.callback = cb, .privdata = privdata, .free = freefn};
  }
}
