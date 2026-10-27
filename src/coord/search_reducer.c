/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#include "search_reducer.h"
#include "result_processor.h"
#include "search_result_ffi.h"
#include "aggregate/aggregate.h"
#include "aggregate/aggregate_debug.h"
#include "concurrent_ctx.h"
#include "debug_commands.h"
#include "dist_profile.h"
#include "fast_float/fast_float_strtod.h"
#include "info/global_stats.h"
#include "query_error_ffi.h"
#include "reply.h"
#include "reply_value.h"
#include "score_explain_mr.h"
#include "resp3.h"
#include "rmr/reply.h"
#include "util/arr/arr.h"
#include <math.h>
#include <string.h>
#ifdef ENABLE_ASSERT
#include <unistd.h>
#endif

typedef struct {
  char *id;
  size_t idLen;
  double score;
  MRReply *explainScores;
  const char *sortKey;
  size_t sortKeyLen;
  double sortKeyNum;
} searchResult;

typedef struct {
  int step;  // offset for next reply
  int score;
  int firstField;
  int payload;
  int sortKey;
} searchReplyOffsets;

typedef struct searchReducerCtx {
  MRReply *fieldNames;
  searchRequestCtx *searchCtx;
  size_t totalReplies;
  searchReplyOffsets offsets;

  specialCaseCtx *reduceSpecialCaseCtxKnn;
  specialCaseCtx *reduceSpecialCaseCtxSortby;

  MRReply *warning;
  QueryProcessingCtx qctx;
  RLookup lookup;
  const RLookupKey *sortKeys[2];
  const RLookupKey *knnKeys[2];
  const RLookupKey *fieldsKey;
  const RLookupKey *payloadKey;
  const RLookupKey *wireSortKey;
  MRReply **replies;
} searchReducerCtx;

static int searchResultReducer(searchRequestCtx *req, bool fromTimeout);

static searchResult *newResult_resp2(searchResult *res, MRReply *arr, size_t j,
                                     searchReplyOffsets *offsets, int explainScores) {
  int scoreOffset = offsets->score;
  int sortKeyOffset = offsets->sortKey;
  res->sortKey = NULL;
  res->sortKeyNum = HUGE_VAL;
  if (MRReply_Type(MRReply_ArrayElement(arr, j)) != MR_REPLY_STRING) {
    res->id = NULL;
    return res;
  }
  res->id = (char *)MRReply_String(MRReply_ArrayElement(arr, j), &res->idLen);
  if (!res->id) {
    return res;
  }
  // parse score
  if (explainScores) {
    MRReply *scoreReply = MRReply_ArrayElement(arr, j + scoreOffset);
    if (MRReply_Type(scoreReply) != MR_REPLY_ARRAY) {
      res->id = NULL;
      return res;
    }
    if (MRReply_Length(scoreReply) != 2) {
      res->id = NULL;
      return res;
    }
    if (!MRReply_ToDouble(MRReply_ArrayElement(scoreReply, 0), &res->score)) {
      res->id = NULL;
      return res;
    }
    res->explainScores = MRReply_ArrayElement(scoreReply, 1);
    // Parse scores only if they were are part of the shard's response.
  } else if (scoreOffset > 0 &&
             !MRReply_ToDouble(MRReply_ArrayElement(arr, j + scoreOffset), &res->score)) {
    res->id = NULL;
    return res;
  }
  if (sortKeyOffset > 0) {
    res->sortKey = MRReply_String(MRReply_ArrayElement(arr, j + sortKeyOffset), &res->sortKeyLen);
  }
  if (res->sortKey) {
    if (res->sortKey[0] == '#') {
      char *endptr;
      res->sortKeyNum = fast_float_strtod(res->sortKey + 1, &endptr);
      RS_ASSERT(endptr == res->sortKey + res->sortKeyLen);
    }
  }
  return res;
}

static searchResult *newResult_resp3(searchResult *res, MRReply *results, size_t j,
                                     searchReplyOffsets *offsets, bool explainScores,
                                     specialCaseCtx *reduceSpecialCaseCtxSortBy) {
  res->sortKey = NULL;
  res->sortKeyNum = HUGE_VAL;

  MRReply *result_j = MRReply_ArrayElement(results, j);
  if (MRReply_Type(result_j) != MR_REPLY_MAP) {
    res->id = NULL;
    return res;
  }

  MRReply *result_id = MRReply_MapElement(result_j, "id");
  if (!result_id || MRReply_Type(result_id) != MR_REPLY_STRING) {
    // We crash in development env, and return NULL (such that an error is raised)
    // in production.
    RS_LOG_ASSERT_FMT(false, "Expected id %zu to exist, and be a string", j);
    res->id = NULL;
    return res;
  }
  res->id = (char *)MRReply_String(result_id, &res->idLen);
  if (!res->id) {
    return res;
  }

  // parse score
  MRReply *score = MRReply_MapElement(result_j, "score");
  if (explainScores) {
    if (MRReply_Type(score) != MR_REPLY_ARRAY) {
      res->id = NULL;
      return res;
    }
    if (!MRReply_ToDouble(MRReply_ArrayElement(score, 0), &res->score)) {
      res->id = NULL;
      return res;
    }
    res->explainScores = MRReply_ArrayElement(score, 1);

  } else if (offsets->score > 0 && !MRReply_ToDouble(score, &res->score)) {
    res->id = NULL;
    return res;
  }

  if (offsets->sortKey > 0) {
    MRReply *sortkey = NULL;
    if (reduceSpecialCaseCtxSortBy) {
      MRReply *require_fields = MRReply_MapElement(result_j, "required_fields");
      if (require_fields) {
        sortkey = MRReply_MapElement(require_fields, reduceSpecialCaseCtxSortBy->sortby.sortKey);
      }
    }
    if (!sortkey) {
      // If sortkey is the only special case, it will not be in the required_fields map
      sortkey = MRReply_MapElement(result_j, "sortkey");
    }
    if (!sortkey) {
      // Fail if sortkey is required but not found
      res->id = NULL;
      return res;
    }
    if (sortkey) {
      res->sortKey = MRReply_String(sortkey, &res->sortKeyLen);
      if (res->sortKey) {
        if (res->sortKey[0] == '#') {
          char *endptr;
          res->sortKeyNum = fast_float_strtod(res->sortKey + 1, &endptr);
          RS_ASSERT(endptr == res->sortKey + res->sortKeyLen);
        }
      }
    }
  }

  return res;
}

static void getReplyOffsets(const searchRequestCtx *ctx, searchReplyOffsets *offsets) {

  /**
   * Reply format
   *
   * ID
   * SCORE         ---| optional - only if WITHSCORES was given, or SORTBY section was not given.
   * Payload
   * Sort field    ---|
   * ...              | special cases - SORTBY, TOPK. Sort key is always first for backwards
   * compatibility.
   * ...           ---|
   * First field
   *
   *
   */

  if (ctx->withScores || !ctx->withSortby) {
    offsets->step = 3;  // 1 for key, 1 for score, 1 for fields
    offsets->score = 1;
    offsets->firstField = 2;
  } else {
    offsets->score = -1;
    offsets->step = 2;  // 1 for key, 1 for fields
    offsets->firstField = 1;
  }
  offsets->payload = -1;
  offsets->sortKey = -1;

  if (ctx->withPayload) {  // save an extra step for payloads
    offsets->step++;
    offsets->payload = offsets->firstField;
    offsets->firstField++;
  }

  // Update the offsets for the special case after determining score, payload, field.
  size_t specialCaseStartOffset = offsets->firstField;
  size_t specialCasesMaxOffset = 0;
  if (ctx->specialCases) {
    size_t nSpecialCases = array_len(ctx->specialCases);
    for (size_t i = 0; i < nSpecialCases; i++) {
      switch (ctx->specialCases[i]->specialCaseType) {
        case SPECIAL_CASE_KNN: {
          ctx->specialCases[i]->knn.offset += specialCaseStartOffset;
          specialCasesMaxOffset = MAX(specialCasesMaxOffset, ctx->specialCases[i]->knn.offset);
          break;
        }
        case SPECIAL_CASE_SORTBY: {
          ctx->specialCases[i]->sortby.offset += specialCaseStartOffset;
          offsets->sortKey = ctx->specialCases[i]->sortby.offset;
          specialCasesMaxOffset = MAX(specialCasesMaxOffset, ctx->specialCases[i]->sortby.offset);
          break;
        }
        case SPECIAL_CASE_NONE:
        default:
          break;
      }
    }
  }

  if (specialCasesMaxOffset > 0) {
    offsets->firstField = specialCasesMaxOffset + 1;
    offsets->step = offsets->firstField + 1;
  } else if (ctx->withSortingKeys) {
    offsets->step++;
    offsets->sortKey = offsets->firstField++;
  }

  // nocontent - one less field, and the offset is -1 to avoid parsing it
  if (ctx->noContent) {
    offsets->step--;
    offsets->firstField = -1;
  }
}

static double parseNumeric(const char *str) {
  RS_ASSERT(str[0] == '#');
  char *eptr;
  double d = fast_float_strtod(str + 1, &eptr);
  RS_ASSERT(eptr != str + 1 && *eptr == 0);
  return d;
}

#ifdef ENABLE_ASSERT
// Helper function to check and pause before reducing a result (for testing coordinator timeout
// during reduce)
static void debugCheckAndPauseBeforeReduce(searchReducerCtx *rCtx) {
  int currentCount = CoordReduceDebugCtx_GetReduceCount();
  int pauseBeforeN = CoordReduceDebugCtx_GetPauseBeforeN();
  if (pauseBeforeN > 0 && currentCount == pauseBeforeN - 1) {
    // Pause before the Nth result (1-based index)
    CoordReduceDebugCtx_SetPause(true);
    while (CoordReduceDebugCtx_IsPaused()) {
      // Check if timed out - break to avoid deadlock with timeout callback
      // (timeout callback waits for reducer to complete, but we're paused)
      if (QueryRequestTimeout_IsBlockedClientTimedOut(&rCtx->searchCtx->base.timeout)) {
        CoordReduceDebugCtx_SetPause(false);
        break;
      }
      usleep(1000);  // Spin-wait with 1ms sleep
    }
  }
}
#endif

typedef struct RPSearchReply {
  ResultProcessor base;
  searchReducerCtx *ctx;
  _Atomic int publishedCount;
  size_t capacity;
  MRReply **replies;
  int snapshotCount;
  uint32_t shard;
  size_t row;
  MRReply *current;
  bool fromTimeout;
} RPSearchReply;

static MRReply *searchReplyBody(const searchRequestCtx *req, MRReply *reply) {
  if (!reply || MRReply_Type(reply) == MR_REPLY_ERROR || !req->profileArgs) return reply;
  return MRSearchCtx_GetCommandProtocol(req->fanout) == 3 ? MRReply_MapElement(reply, "Results")
                                                          : MRReply_ArrayElement(reply, 0);
}

static bool needsKnnSelection(const searchReducerCtx *ctx) {
  return ctx->reduceSpecialCaseCtxKnn && ctx->reduceSpecialCaseCtxKnn->knn.shouldSort;
}

static bool searchRowDistance(searchReducerCtx *ctx, MRReply *body, size_t ordinal,
                              const searchResult *view, double *distance) {
  MRReply *value;
  knnContext *knn = &ctx->reduceSpecialCaseCtxKnn->knn;
  // WITHSORTKEYS can move the shared distance field out of required_fields.
  // The normal sort-key decoder already handles both wire representations.
  if (ctx->reduceSpecialCaseCtxSortby && view->sortKey &&
      !strcmp(ctx->reduceSpecialCaseCtxSortby->sortby.sortKey, knn->fieldName)) {
    *distance = view->sortKeyNum;
    return true;
  }
  if (MRReply_Type(body) == MR_REPLY_MAP) {
    MRReply *fields = MRReply_MapElement(
        MRReply_ArrayElement(MRReply_MapElement(body, "results"), ordinal), "required_fields");
    if (!fields || !(value = MRReply_MapElement(fields, knn->fieldName))) return false;
  } else {
    value = MRReply_ArrayElement(body, 1 + ordinal * ctx->offsets.step + knn->offset);
  }
  *distance = parseNumeric(MRReply_String(value, NULL));
  return true;
}

static int searchReplyParseError(searchReducerCtx *ctx) {
  QueryError_SetError(ctx->qctx.err, QUERY_ERROR_CODE_GENERIC,
                      "could not parse redisearch results");
  return RS_RESULT_ERROR;
}

static MRReply *takeSearchField(MRReply *body, size_t offset, int fieldOffset, const char *name) {
  if (MRReply_Type(body) == MR_REPLY_MAP) {
    return MRReply_TakeMapElement(MRReply_ArrayElement(MRReply_MapElement(body, "results"), offset),
                                  name);
  }
  return fieldOffset > 0 ? MRReply_TakeArrayElement(body, offset + fieldOffset) : NULL;
}

static int searchReplyNext(ResultProcessor *base, SearchResult *row) {
  RPSearchReply *source = (RPSearchReply *)base;
  searchReducerCtx *ctx = source->ctx;
  while (source->shard < (uint32_t)source->snapshotCount) {
    {
      if (!source->current) {
        if (ctx->searchCtx->profileArgs &&
            MRReply_Type(ctx->replies[source->shard]) == MR_REPLY_ERROR) {
          ++source->shard;
          continue;
        }
        MRReply *body = searchReplyBody(ctx->searchCtx, ctx->replies[source->shard]);
        if (!body || MRReply_Type(body) == MR_REPLY_ERROR ||
            (MRReply_Type(body) != MR_REPLY_MAP &&
             (MRReply_Type(body) != MR_REPLY_ARRAY || !MRReply_Length(body)))) {
          goto next_shard;
        }
        source->current = body;
        if (MRReply_Type(body) == MR_REPLY_MAP) {
          MRReply *warning = MRReply_MapElement(body, "warning");
          RS_LOG_ASSERT(warning && MRReply_Type(warning) == MR_REPLY_ARRAY,
                        "invalid warning record");
          if (!ctx->warning && warning && MRReply_Length(warning)) ctx->warning = warning;
          MRReply *total = MRReply_MapElement(body, "total_results");
          if ((!total && !needsKnnSelection(ctx)) || !MRReply_MapElement(body, "results")) {
            return searchReplyParseError(ctx);
          }
          if (!needsKnnSelection(ctx)) ctx->totalReplies += MRReply_Integer(total);
          source->row = 0;
        } else {
          if (!needsKnnSelection(ctx))
            ctx->totalReplies += MRReply_Integer(MRReply_ArrayElement(body, 0));
          source->row = 0;
        }
      }
      MRReply *body = source->current;
      bool resp3 = MRReply_Type(body) == MR_REPLY_MAP;
      MRReply *rows = resp3 ? MRReply_MapElement(body, "results") : body;
      size_t len = MRReply_Length(rows);
      size_t step = resp3 ? 1 : ctx->offsets.step;
      size_t offset = resp3 ? source->row : 1 + source->row * step;
      if (offset < len) {
        if (step > len - offset) return searchReplyParseError(ctx);
        searchResult view = {0};
        if (resp3) {
          newResult_resp3(&view, rows, source->row, &ctx->offsets,
                          ctx->searchCtx->withExplainScores, ctx->reduceSpecialCaseCtxSortby);
        } else {
          newResult_resp2(&view, body, offset, &ctx->offsets, ctx->searchCtx->withExplainScores);
        }
#ifdef ENABLE_ASSERT
        debugCheckAndPauseBeforeReduce(ctx);
#endif
        if (!view.id) return searchReplyParseError(ctx);
        RLookupRow *values = SearchResult_GetRowDataMut(row);
        if (needsKnnSelection(ctx)) {
          double distance;
          if (!searchRowDistance(ctx, body, source->row, &view, &distance)) {
            return searchReplyParseError(ctx);
          }
          RLookup_WriteOwnKey(ctx->knnKeys[0], values, RSValue_NewNumber(distance));
          ctx->totalReplies = MIN(ctx->reduceSpecialCaseCtxKnn->knn.k, ctx->totalReplies + 1);
        }
        // The sort projection borrows bytes owned by the wire sort key in this same row.
        if (view.sortKey) {
          RSValue *sortValue = view.sortKey[0] == '#' ? RSValue_NewNumber(view.sortKeyNum)
                                                      : RSValue_NewBorrowedString(
                                                            view.sortKey + 1, view.sortKeyLen - 1);
          RLookup_WriteOwnKey(ctx->sortKeys[0], values, sortValue);
        }
        MRReply *record = resp3 ? MRReply_ArrayElement(rows, source->row) : body;
        RLookup_WriteOwnKey(ctx->sortKeys[1], values,
                            MRReply_ToValue(resp3 ? MRReply_TakeMapElement(record, "id")
                                                  : MRReply_TakeArrayElement(body, offset)));
        RLookup_WriteOwnKey(ctx->fieldsKey, values,
                            MRReply_ToValue(takeSearchField(body, offset, ctx->offsets.firstField,
                                                            "extra_attributes")));
        RLookup_WriteOwnKey(
            ctx->payloadKey, values,
            MRReply_ToValue(takeSearchField(body, offset, ctx->offsets.payload, "payload")));
        if (view.explainScores && MRReply_Type(view.explainScores) != MR_REPLY_NIL) {
          SearchResult_SetScoreExplain(row, SE_FromMRReply(view.explainScores));
        }
        if (view.sortKey) {
          MRReply *key = NULL;
          if (resp3 && ctx->reduceSpecialCaseCtxSortby) {
            MRReply *required = MRReply_MapElement(record, "required_fields");
            if (required)
              key =
                  MRReply_TakeMapElement(required, ctx->reduceSpecialCaseCtxSortby->sortby.sortKey);
          }
          if (!key) key = takeSearchField(body, offset, ctx->offsets.sortKey, "sortkey");
          RLookup_WriteOwnKey(ctx->wireSortKey, values, MRReply_ToValue(key));
        } else {
          RLookup_WriteOwnKey(ctx->wireSortKey, values, RSValue_NullStatic());
        }
        SearchResult_SetScore(row, view.score);
        ++source->row;
#ifdef ENABLE_ASSERT
        CoordReduceDebugCtx_IncrementReduceCount();
#endif
        return RS_RESULT_OK;
      }
      if (resp3) processResultFormat(&ctx->searchCtx->format, body);
    }
  next_shard:
    source->current = NULL;
    ++source->shard;
    // Preserve the reducer's shard-boundary cancellation point during migration.
    if (!source->fromTimeout &&
        QueryRequestTimeout_IsBlockedClientTimedOut(&ctx->searchCtx->base.timeout)) {
      return RS_RESULT_EOF;
    }
  }
  return RS_RESULT_EOF;
}

static void searchReplyFree(ResultProcessor *base) {
  RPSearchReply *source = (RPSearchReply *)base;
  int published = atomic_load_explicit(&source->publishedCount, memory_order_acquire);
  for (int i = 0; i < published; ++i) MRReply_Free(source->replies[i]);
  rm_free(source->replies);
  rm_free(source);
}

void CoordSearchReply_Init(searchRequestCtx *request) {
  RPSearchReply *source = rm_calloc(1, sizeof(*source));
  source->base =
      (ResultProcessor){.Next = searchReplyNext, .Free = searchReplyFree, .type = RP_SEARCH_REPLY};
  atomic_init(&source->publishedCount, 0);
  request->replySource = source;
}

void CoordSearchReply_Prepare(searchRequestCtx *request, size_t capacity) {
  RPSearchReply *source = request->replySource;
  RS_ASSERT(!source->replies);
  source->capacity = capacity;
  source->replies = rm_calloc(capacity, sizeof(*source->replies));
}

void CoordSearchReply_Accept(searchRequestCtx *request, MRReply *reply) {
  RPSearchReply *source = request->replySource;
#ifdef ENABLE_ASSERT
  SyncPoint_Wait("BeforeCoordSearchReplyAccept");
#endif
  if (!QueryRequestTimeout_IsBlockedClientTimedOut(&request->base.timeout)) {
    // Each request has one IO-thread producer. Publish only fully initialized slots.
    int published = atomic_load_explicit(&source->publishedCount, memory_order_relaxed);
    RS_ASSERT((size_t)published < source->capacity);
    source->replies[published] = reply;
#ifdef ENABLE_ASSERT
    SyncPoint_Wait(published == 0 ? "BeforeCoordSearchReplyPublish"
                                  : "BeforeCoordSearchNextReplyPublish");
#endif
    atomic_store_explicit(&source->publishedCount, published + 1, memory_order_release);
  } else {
    MRReply_Free(reply);
  }
#ifdef ENABLE_ASSERT
  SyncPoint_Wait("AfterCoordSearchReplyAccept");
#endif
}

void CoordSearchReducer_Free(searchRequestCtx *request) {
  // Release stored rows and buffered candidates through the common row lifecycle.
  QueryRequest_ResetReply(&request->base);
  searchReducerCtx *ctx = request->rctx;
  if (ctx) {
    bool sourceInChain = ctx->qctx.rootProc != NULL;
    QITR_FreeChain(&ctx->qctx);
    if (sourceInChain) request->replySource = NULL;
    QueryRequest_SetEndProcRef(&request->base, NULL);
    RLookup_Cleanup(&ctx->lookup);
    rm_free(ctx);
  }
  if (request->replySource) searchReplyFree(&request->replySource->base);
}

static void reduceSearchPipeline(searchReducerCtx *ctx, int count, MRReply **replies,
                                 bool fromTimeout) {
  ctx->replies = replies;
  ctx->qctx.err = &ctx->searchCtx->base.reply.err;
  ctx->qctx.timeoutPolicy = ctx->searchCtx->base.timeout.config.timeoutPolicy;
  ctx->qctx.resultLimit = UINT32_MAX;
  ctx->qctx.skipIndexResultDeepCopy = true;
  RPSearchReply *source = ctx->searchCtx->replySource;
  source->ctx = ctx;
  RS_ASSERT(source->snapshotCount == count);
  source->fromTimeout = fromTimeout;
  QITR_PushRP(&ctx->qctx, &source->base);
  ctx->sortKeys[0] = RLookup_GetKey_Write(&ctx->lookup, "sortkey", RLOOKUP_F_NOFLAGS);
  ctx->sortKeys[1] = RLookup_GetKey_Write(&ctx->lookup, "document_key", RLOOKUP_F_NOFLAGS);
  ctx->fieldsKey = RLookup_GetKey_Write(&ctx->lookup, "fields", RLOOKUP_F_NOFLAGS);
  ctx->payloadKey = RLookup_GetKey_Write(&ctx->lookup, "payload", RLOOKUP_F_NOFLAGS);
  ctx->wireSortKey = RLookup_GetKey_Write(&ctx->lookup, "wire_sortkey", RLOOKUP_F_NOFLAGS);
  if (needsKnnSelection(ctx)) {
    ctx->knnKeys[0] = RLookup_GetKey_Write(&ctx->lookup, "distance", RLOOKUP_F_NOFLAGS);
    ctx->knnKeys[1] = ctx->sortKeys[1];
    QITR_PushRP(&ctx->qctx,
                RPSorter_NewByFields(ctx->reduceSpecialCaseCtxKnn->knn.k, ctx->knnKeys, 2, 3));
  }
  ResultProcessor *sorter =
      ctx->searchCtx->withSortby
          ? RPSorter_NewByFields(ctx->searchCtx->requestedResultsCount, ctx->sortKeys, 2,
                                 ctx->searchCtx->sortAscending ? 3 : 0)
          : RPSorter_NewByScore(ctx->searchCtx->requestedResultsCount, ctx->sortKeys[1]);
  QITR_PushRP(&ctx->qctx, sorter);
  if (ctx->searchCtx->limit != 0) {
    QITR_PushRP(&ctx->qctx, RPPager_New(ctx->searchCtx->offset, ctx->searchCtx->limit));
  }
  QueryRequest_SetEndProcRef(&ctx->searchCtx->base, &ctx->qctx.endProc);
  ChunkReplyState *reply = &ctx->searchCtx->base.reply;
  reply->results = array_new(SearchResult *, 8);
  reply->limit = ctx->searchCtx->limit;
  SearchResult row = SearchResult_New();
  // A count-only request still consumes envelopes but has no candidates to sort.
  ResultProcessor *end = ctx->qctx.endProc;
  if (ctx->searchCtx->limit == 0) end = ctx->qctx.rootProc;
  while ((reply->rc = end->Next(end, &row)) == RS_RESULT_OK) {
    if (ctx->searchCtx->limit != 0) {
      array_append(reply->results, SearchResult_AllocateMove(&row));
      row = SearchResult_New();
    } else {
      SearchResult_Clear(&row);
    }
  }
  SearchResult_Destroy(&row);
  reply->hasStoredResults = true;
  if (ctx->reduceSpecialCaseCtxKnn) {
    ctx->totalReplies = MIN(ctx->reduceSpecialCaseCtxKnn->knn.k, ctx->totalReplies);
  }
}

static inline QueryTimeoutStage searchReqCtx_GetExecutionStage(searchRequestCtx *req) {
  return (QueryTimeoutStage)QueryRequest_GetExecutionPhase(&req->base);
}

// Record an FT.SEARCH-coordinator per-stage timeout, reading the search request's
// own marker (QUEUE -> PIPELINE at dequeue -> REPLY after reduction). Coord side.
static inline void recordSearchTimeoutStage(searchRequestCtx *req, bool isError) {
  QueryTimeoutStage stage = req ? searchReqCtx_GetExecutionStage(req) : QUERY_TIMEOUT_STAGE_QUEUE;
  QueryTimeoutStageStats_Record(stage, isError, COORD_ERR_WARN);
}

static void sendSearchResults(RedisModule_Reply *reply, searchReducerCtx *rCtx) {
  searchRequestCtx *req = rCtx->searchCtx;

  SearchResult **results = req->base.reply.results;
  size_t qlen = results ? array_len(results) : 0;

  //-------------------------------------------------------------------------------------------
  // RESP2 is a flat array: total, then each result's fields in sequence.
  RedisModule_Reply_MapOrArray(reply);
  if (reply->resp3)  // RESP3
  {
    RedisModule_Reply_SimpleString(reply, "attributes");
    if (rCtx->fieldNames) {
      MR_ReplyWithMRReply(reply, rCtx->fieldNames);
    } else {
      RedisModule_Reply_EmptyArray(reply);
    }

    RedisModule_Reply_SimpleString(reply, "warning");  // >warning
    if (rCtx->warning) {
      size_t len = MRReply_Length(rCtx->warning);
      RedisModule_Reply_ArrayWithLen(reply, len);
      // Iterate over warning array and track warnings
      for (size_t i = 0; i < len; ++i) {
        // Extract warning string and track it
        MRReply *currentWarning = MRReply_ArrayElement(rCtx->warning, i);
        const char *warning_str = MRReply_String(currentWarning, NULL);
        QueryWarningCode warningCode = QueryWarningCode_GetCodeFromMessage(warning_str);
        QueryWarningsGlobalStats_UpdateWarning(warningCode, 1, COORD_ERR_WARN);

        // Reply warning
        MR_ReplyWithMRReply(reply, currentWarning);
      }
    } else if (req->queryOOM) {
      QueryWarningsGlobalStats_UpdateWarning(QUERY_WARNING_CODE_OUT_OF_MEMORY_COORD, 1,
                                             COORD_ERR_WARN);
      // We use the cluster warning since shard level warning sent via empty reply bailout
      RedisModule_Reply_ArrayWithLen(reply, 1);
      RedisModule_Reply_SimpleString(reply, QUERY_WOOM_COORD);
    } else if (req->timedOut) {
      QueryWarningsGlobalStats_UpdateWarning(QUERY_WARNING_CODE_TIMED_OUT, 1, COORD_ERR_WARN);
      RedisModule_Reply_ArrayWithLen(reply, 1);
      RedisModule_Reply_SimpleString(reply, QueryWarning_Strwarning(QUERY_WARNING_CODE_TIMED_OUT));
    } else {
      RedisModule_Reply_EmptyArray(reply);
    }

    RedisModule_ReplyKV_LongLong(reply, "total_results", rCtx->totalReplies);

    if (rCtx->searchCtx->format & QEXEC_FORMAT_EXPAND) {
      RedisModule_ReplyKV_SimpleString(reply, "format", "EXPAND");  // >format
    } else {
      RedisModule_ReplyKV_SimpleString(reply, "format", "STRING");  // >format
    }

    RedisModule_ReplyKV_Array(reply, "results");  // >results

    // id, the optional sections below, and the trailing "values" placeholder.
    const size_t entries = 2 + req->withScores + req->withPayload +
                           (req->withSortingKeys && req->withSortby) + !req->noContent;
    for (size_t i = 0; i < qlen; ++i) {
      RedisModule_Reply_MapWithLen(reply, entries);  // >> result
      const SearchResult *res = results[i];
      const RLookupRow *values = SearchResult_GetRowData(res);
      RedisModule_Reply_SimpleString(reply, "id");
      RedisModule_Reply_RSValue(reply, RLookupRow_Get(rCtx->sortKeys[1], values), 0);

      if (req->withScores) {
        RedisModule_Reply_SimpleString(reply, "score");

        if (req->withExplainScores) {
          RedisModule_Reply_ArrayWithLen(reply, SCORE_WITH_EXPLAIN_REPLY_LEN);
          RedisModule_Reply_Double(reply, SearchResult_GetScore(res));
          SEReply(reply, SearchResult_GetScoreExplain(res));
        } else {
          RedisModule_Reply_Double(reply, SearchResult_GetScore(res));
        }
      }

      if (req->withPayload) {
        RedisModule_Reply_SimpleString(reply, "payload");
        RedisModule_Reply_RSValue(reply, RLookupRow_Get(rCtx->payloadKey, values), 0);
      }

      if (req->withSortingKeys && req->withSortby) {
        RedisModule_Reply_SimpleString(reply, "sortkey");
        RedisModule_Reply_RSValue(reply, RLookupRow_Get(rCtx->wireSortKey, values), 0);
      }
      if (!req->noContent) {
        RedisModule_Reply_SimpleString(reply, "extra_attributes");
        RedisModule_Reply_RSValue(
            reply, RLookupRow_Get(rCtx->fieldsKey, values),
            req->format & QEXEC_FORMAT_EXPAND ? SENDREPLY_FLAG_EXPAND : 0);  // >> extra_attributes
      }

      RedisModule_Reply_SimpleString(reply, "values");
      RedisModule_Reply_EmptyArray(reply);
    }

    RedisModule_Reply_ArrayEnd(reply);  // >results
  }
  //-------------------------------------------------------------------------------------------
  else  // RESP2
  {
    RedisModule_Reply_LongLong(reply, rCtx->totalReplies);

    for (size_t pos = 0; pos < qlen; pos++) {
      const SearchResult *res = results[pos];
      const RLookupRow *values = SearchResult_GetRowData(res);
      RedisModule_Reply_RSValue(reply, RLookupRow_Get(rCtx->sortKeys[1], values), 0);
      if (req->withScores) {
        if (req->withExplainScores) {
          RedisModule_Reply_ArrayWithLen(reply, SCORE_WITH_EXPLAIN_REPLY_LEN);
          RedisModule_Reply_Double(reply, SearchResult_GetScore(res));
          SEReply(reply, SearchResult_GetScoreExplain(res));
        } else {
          RedisModule_Reply_Double(reply, SearchResult_GetScore(res));
        }
      }
      if (req->withPayload) {
        RedisModule_Reply_RSValue(reply, RLookupRow_Get(rCtx->payloadKey, values), 0);
      }
      if (req->withSortingKeys && req->withSortby) {
        RedisModule_Reply_RSValue(reply, RLookupRow_Get(rCtx->wireSortKey, values), 0);
      }
      if (!req->noContent) {
        RedisModule_Reply_RSValue(reply, RLookupRow_Get(rCtx->fieldsKey, values), 0);
      }
    }
  }
  RedisModule_Reply_MapOrArrayEnd(reply);

  if (req->queryOOM) {
    QueryWarningsGlobalStats_UpdateWarning(QUERY_WARNING_CODE_OUT_OF_MEMORY_COORD, 1,
                                           COORD_ERR_WARN);
  }
  //-------------------------------------------------------------------------------------------
}

struct PrintCoordProfile_ctx {
  rs_wall_clock *totalTime;
  rs_wall_clock_ns_t postProcessTime;
  rs_wall_clock_ns_t coordQueueTime;  // Time spent waiting in coordinator thread pool queue
};
static void profileSearchReplyCoordinator(RedisModule_Reply *reply, void *ctx) {
  struct PrintCoordProfile_ctx *pCtx = ctx;
  RedisModule_Reply_Map(reply);
  RedisModule_ReplyKV_Double(
      reply, "Total Coordinator time",
      rs_wall_clock_convert_ns_to_ms_d(rs_wall_clock_elapsed_ns(pCtx->totalTime)));
  RedisModule_ReplyKV_Double(
      reply, "Post Processing time",
      rs_wall_clock_convert_ns_to_ms_d(rs_wall_clock_now_ns() - pCtx->postProcessTime));
  RedisModule_ReplyKV_Double(reply, "Coordinator queue time",
                             rs_wall_clock_convert_ns_to_ms_d(pCtx->coordQueueTime));
  RedisModule_Reply_MapEnd(reply);
}

static void profileSearchReply(RedisModule_Reply *reply, searchReducerCtx *rCtx, int count,
                               MRReply **replies, rs_wall_clock *totalTime,
                               rs_wall_clock_ns_t postProcessTime) {
  bool has_map = RedisModule_IsRESP3(reply);
  RedisModule_Reply_MapOrArray(reply);  // root; RESP2: results, then the profile sections
  if (has_map) {
    RedisModule_Reply_SimpleString(reply, "Results");  // >results
  }
  sendSearchResults(reply, rCtx);

  // print profile of shards & coordinator
  PrintShardProfile_ctx shardsCtx = {
      .count = count,
      .replies = replies,
      .isSearch = true,
  };
  struct PrintCoordProfile_ctx coordCtx = {
      .totalTime = totalTime,
      .postProcessTime = postProcessTime,
      .coordQueueTime = rCtx->searchCtx->coordQueueTime,
  };
  Profile_PrintInFormat(reply, PrintShardProfile, &shardsCtx, profileSearchReplyCoordinator,
                        &coordCtx);

  RedisModule_Reply_MapOrArrayEnd(reply);  // >root
}

void sendSearchResults_EmptyResults(RedisModule_Reply *reply, searchRequestCtx *req) {
  searchReducerCtx ctx = {.searchCtx = req};
  if (req->profileArgs > 0) {
    profileSearchReply(reply, &ctx, 0, NULL, &req->profileClock, rs_wall_clock_now_ns());
  } else {
    sendSearchResults(reply, &ctx);
  }
}

static void searchResultReducer_wrapper(void *arg) {
  searchRequestCtx *req = arg;
  searchResultReducer(req, false);
  // Unblock is the final access: it can free the request, pipeline and transport.
  RedisModule_UnblockClient(MRSearchCtx_GetBlockedClient(req->fanout), &req->base);
}

void CoordSearchReducer_Schedule(searchRequestCtx *request) {
  ConcurrentSearch_ThreadPoolRun(searchResultReducer_wrapper, request);
}

static bool should_return_error(const searchRequestCtx *req, QueryErrorCode errCode) {
  // Check if this is a timeout error with non-fail policy
  if (errCode == QUERY_ERROR_CODE_TIMED_OUT) {
    return req->base.timeout.config.timeoutPolicy == TimeoutPolicy_Fail;
  }
  // Check if this is an OOM error with non-fail policy
  if (errCode == QUERY_ERROR_CODE_OUT_OF_MEMORY) {
    return RSGlobalConfig.requestConfigParams.oomPolicy == OomPolicy_Fail;
  }

  // For any other error, return it
  return true;
}

static int searchResultReducer(searchRequestCtx *req, bool fromTimeout) {
  RedisModuleBlockedClient *bc = NULL;
  searchReducerCtx *rCtx = NULL;
  RPSearchReply *source = req->replySource;
  int count = 0;
  MRReply **replies = NULL;

#ifdef ENABLE_ASSERT
  if (!fromTimeout) SyncPoint_Wait("BeforeCoordReducerClaim");
#endif
  // If called from timeout callback, we already own reducing (claimed before calling).
  bool ownsReducing = fromTimeout || QueryRequest_TryClaimResults(&req->base);
  if (!ownsReducing) {
    return REDISMODULE_OK;
  }

#ifdef ENABLE_ASSERT
  if (!fromTimeout) SyncPoint_Wait("CoordSearchReducerClaimed");
  // Debug-only hook to pause after claiming reducing but before reducer setup.
  // This lets tests force the timeout race where the background reducer exits
  // before initializing req->rctx.
  if (CoordReduceDebugCtx_GetPauseBeforeN() == COORD_REDUCE_PAUSE_BEFORE_REDUCER_INIT) {
    CoordReduceDebugCtx_SetPause(true);
    while (CoordReduceDebugCtx_IsPaused()) {
      if (QueryRequestTimeout_IsBlockedClientTimedOut(&req->base.timeout)) {
        CoordReduceDebugCtx_SetPause(false);
        break;
      }
      usleep(1000);  // Spin-wait with 1ms sleep
    }
  }
#endif

  // No reduction is needed if the timeout callback already replied.
  if (!fromTimeout && QueryRequestTimeout_IsBlockedClientTimedOut(&req->base.timeout)) {
    goto cleanup;
  }

  if (!fromTimeout) bc = MRSearchCtx_GetBlockedClient(req->fanout);

  rCtx = rm_calloc(1, sizeof(searchReducerCtx));
  rCtx->lookup = RLookup_New();

  // Save the reducer state in the request so it is available to the
  // blocked-client reply callback after the normal unblock path.
  req->rctx = rCtx;
  // Set searchCtx early so it's available even if we bail out early
  rCtx->searchCtx = req;
  count = atomic_load_explicit(&source->publishedCount, memory_order_acquire);
  // Zero may precede IO-thread allocation; do not read the unpublished array pointer.
  replies = count ? source->replies : NULL;
  source->snapshotCount = count;
  rCtx->replies = replies;

  // got no replies
  if (!fromTimeout && (count == 0 || req->limit < 0)) {
    QueryError_SetError(&req->base.reply.err, QUERY_ERROR_CODE_GENERIC,
                        "Could not send query to cluster");
    goto cleanup;
  }

  // Traverse the replies, check for early bail-out which we want for all errors
  // but timeout+non-strict timeout policy.
  for (int i = 0; i < count; i++) {
    MRReply *curr_rep = replies[i];
    if (MRReply_Type(curr_rep) == MR_REPLY_ERROR) {
      const char *errStr = MRReply_String(curr_rep, NULL);
      QueryErrorCode errCode = QueryError_GetCodeFromMessage(errStr);
      if (should_return_error(req, errCode)) {
        // Shard reply already contains the prefixed error string — set directly.
        QueryError_SetCode(&req->base.reply.err, errCode);
        QueryError_SetDetail(&req->base.reply.err, errStr);
        goto cleanup;
      }
    }
  }

  // Get reply offsets
  getReplyOffsets(rCtx->searchCtx, &rCtx->offsets);

  if (req->specialCases) {
    for (size_t i = 0; i < array_len(req->specialCases); ++i) {
      specialCaseCtx *special = req->specialCases[i];
      if (special->specialCaseType == SPECIAL_CASE_KNN)
        rCtx->reduceSpecialCaseCtxKnn = special;
      else if (special->specialCaseType == SPECIAL_CASE_SORTBY)
        rCtx->reduceSpecialCaseCtxSortby = special;
    }
  }
  reduceSearchPipeline(rCtx, count, replies, fromTimeout);
  if (!fromTimeout && QueryRequestTimeout_IsBlockedClientTimedOut(&req->base.timeout)) {
    goto cleanup;
  }

#ifdef ENABLE_ASSERT
  // Handle N=-1: pause after the last result is reduced
  if (CoordReduceDebugCtx_GetPauseBeforeN() == COORD_REDUCE_PAUSE_AFTER_LAST_RESULT) {
    CoordReduceDebugCtx_SetPause(true);
    while (CoordReduceDebugCtx_IsPaused()) {
      // Check if timed out - break to avoid deadlock with timeout callback
      if (QueryRequestTimeout_IsBlockedClientTimedOut(&req->base.timeout)) {
        CoordReduceDebugCtx_SetPause(false);
        break;
      }
      usleep(1000);  // Spin-wait with 1ms sleep
    }
  }
#endif

cleanup:
  // Reduction/post-processing done, about to hand off the reply: advance the marker
  // so a timeout from here on is attributed to REPLY (frozen once already timed out).
  if (!QueryRequestTimeout_IsBlockedClientTimedOut(&req->base.timeout)) {
    searchReqCtx_SetExecutionStage(req, QUERY_TIMEOUT_STAGE_REPLY);
  }

  if (bc && !fromTimeout && !QueryRequestTimeout_IsBlockedClientTimedOut(&req->base.timeout)) {
    RedisModule_BlockedClientMeasureTimeEnd(bc);
  }
  QueryRequest_SignalResultsComplete(&req->base);

  return REDISMODULE_OK;
}

// Reply callback for distributed search.
// Called on the main thread when the client is unblocked.
// QueryRequest_OnFree ends the blocked-client cycle after this callback.
#ifdef ENABLE_ASSERT
RLookup *CoordSearchReducer_DebugReduce(searchRequestCtx *request) {
  searchResultReducer(request, true);
  return &request->rctx->lookup;
}
#endif

int CoordSearchReducer_Reply(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  UNUSED(argv);
  UNUSED(argc);
  QueryRequest *request = RedisModule_GetBlockedClientPrivateData(ctx);
  if (request) {
    searchRequestCtx *req = QueryRequest_GetSearch(request);
    RPSearchReply *source = req->replySource;

    // Check if we have an error and return it
    if (QueryError_HasError(&request->reply.err)) {
      // Track error in global statistics
      QueryErrorsGlobalStats_UpdateError(QueryError_GetCode(&request->reply.err), 1,
                                         COORD_ERR_WARN);
      QueryError_ReplyAndClear(ctx, &request->reply.err);
      return REDISMODULE_OK;
    }

    if (source->snapshotCount == 0) {
      // Can happen in a topology error, before or after we sent the command to the cluster
      RedisModule_ReplyWithError(ctx, "Could not send query to cluster");
      return REDISMODULE_OK;
    }

    searchReducerCtx *rCtx = req->rctx;
    // If NumReplied > 0 we expect ReducerCtx to be initialized
    RS_ASSERT(rCtx)

    RedisModule_Reply _reply = RedisModule_NewReply(ctx), *reply = &_reply;

    if (req->profileArgs > 0) {
      // Profile command
      profileSearchReply(reply, rCtx, source->snapshotCount, rCtx->replies, &req->profileClock,
                         rs_wall_clock_now_ns());
    } else {
      // Non-profile command
      sendSearchResults(reply, rCtx);
    }

    RedisModule_EndReply(reply);

    rs_wall_clock_ns_t duration = rs_wall_clock_elapsed_ns(&req->initClock);
    TotalGlobalStats_CountQuery(QEXEC_F_IS_SEARCH, duration);
  }
  return REDISMODULE_OK;
}

// Timeout callback for FT.SEARCH in coordinator mode.
// Called on the main thread when the blocking client times out.
// For FAIL policy
int CoordSearchReducer_TimeoutFail(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  UNUSED(argv);
  UNUSED(argc);

  QueryRequest *request = RedisModule_GetBlockedClientPrivateData(ctx);
  if (request) {
    searchRequestCtx *req = QueryRequest_GetSearch(request);
    QueryRequestTimeout_MarkTimedOut(&req->base.timeout);
    recordSearchTimeoutStage(req, /*isError=*/true);
  }

  QueryErrorsGlobalStats_UpdateError(QUERY_ERROR_CODE_TIMED_OUT, 1, COORD_ERR_WARN);
  RedisModule_ReplyWithError(ctx, QueryError_Strerror(QUERY_ERROR_CODE_TIMED_OUT));

  return REDISMODULE_OK;
}

// Timeout callback for FT.SEARCH in coordinator mode.
// Called on the main thread when the blocking client times out.
// Used for RETURN-STRICT policy - returns partial results using the blocked client timeout
// mechanism instead of error
int CoordSearchReducer_TimeoutPartial(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {

  QueryRequest *request = RedisModule_GetBlockedClientPrivateData(ctx);
  if (!request) {
    // This shouldn't happen but handle gracefully
    return RedisModule_ReplyWithError(ctx, "ERR timeout with no context");
  }

  searchRequestCtx *req = QueryRequest_GetSearch(request);
  RPSearchReply *source = req->replySource;

  QueryRequestTimeout_MarkTimedOut(&req->base.timeout);

  recordSearchTimeoutStage(req, /*isError=*/false);

  // Try to claim reducing - if we get it, run reducer on main thread
  // If we don't get it, reducer is already running (or bailout claimed it) - wait for it
  if (QueryRequest_TryClaimResults(&req->base)) {
    // We claimed reducing - run reducer on main thread with current replies
    searchResultReducer(req, true);
  } else {
    // Reducer already running or bailout claimed it - wait for completion
    QueryRequest_WaitForResultsComplete(&req->base);

    // A background reducer may have claimed reducing, observed the timeout,
    // and exited before initializing req->rctx. In that case adopt the
    // timeout-owned reduction path now that the competing reducer has finished.
    if (!QueryError_HasError(&request->reply.err) && req->rctx == NULL) {
      searchResultReducer(req, true);
    }
  }

  // Check if bailout set an error (e.g., index dropped before fanout)
  // In this case, reply with the error instead of partial results
  if (QueryError_HasError(&request->reply.err)) {
    QueryErrorsGlobalStats_UpdateError(QueryError_GetCode(&request->reply.err), 1, COORD_ERR_WARN);
    QueryError_ReplyAndClear(ctx, &request->reply.err);
    return REDISMODULE_OK;
  }

  // Reply with results from reducer
  searchReducerCtx *rCtx = req->rctx;
  // rCtx must be set - either we ran the reducer or waited for it to complete
  RS_ASSERT(rCtx);
  req->timedOut = true;

  RedisModule_Reply _reply = RedisModule_NewReply(ctx), *reply = &_reply;
  if (req->profileArgs > 0) {
    profileSearchReply(reply, rCtx, source->snapshotCount, rCtx->replies, &req->profileClock,
                       rs_wall_clock_now_ns());
  } else {
    sendSearchResults(reply, rCtx);
  }
  RedisModule_EndReply(reply);

  return REDISMODULE_OK;
}
