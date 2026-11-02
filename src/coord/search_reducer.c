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
#include "resp3.h"
#include "rmr/reply.h"
#include "util/arr/arr.h"
#include "util/heap.h"
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
  MRReply *fields;
  MRReply *payload;
  const char *sortKey;
  size_t sortKeyLen;
  double sortKeyNum;
} searchResult;

struct searchReducerCtx;  // Predecleration

typedef void (*processReplyCB)(MRReply *arr, struct searchReducerCtx *rCtx);
typedef void (*postProcessReplyCB)(struct searchReducerCtx *rCtx);

typedef struct {
  int step;  // offset for next reply
  int score;
  int firstField;
  int payload;
  int sortKey;
} searchReplyOffsets;

typedef struct searchReducerCtx {
  MRReply *fieldNames;
  MRReply *lastError;
  searchResult *cachedResult;
  searchRequestCtx *searchCtx;
  heap_t *pq;
  size_t totalReplies;
  bool errorOccurred;
  searchReplyOffsets offsets;

  processReplyCB processReply;
  postProcessReplyCB postProcess;
  specialCaseCtx *reduceSpecialCaseCtxKnn;
  specialCaseCtx *reduceSpecialCaseCtxSortby;

  MRReply *warning;
  QueryProcessingCtx qctx;
  arrayof(SearchResult *) results;
  MRReply **replies;
  bool pipeline;
} searchReducerCtx;

typedef struct {
  searchResult *result;
  double score;
} scoredSearchResultWrapper;

static int searchResultReducer(searchRequestCtx *req, int count, MRReply **replies,
                               bool fromTimeout);

void CoordSearchReducer_Free(searchRequestCtx *r) {
  searchReducerCtx *rctx = r->rctx;
  if (rctx) {
    if (rctx->pq) {
      heap_destroy(rctx->pq);
    }
    if (rctx->reduceSpecialCaseCtxKnn && rctx->reduceSpecialCaseCtxKnn->knn.pq) {
      heap_destroy(rctx->reduceSpecialCaseCtxKnn->knn.pq);
    }
    QITR_FreeChain(&rctx->qctx);
    if (rctx->results) {
      for (size_t i = 0; i < array_len(rctx->results); ++i) {
        SearchResult_Destroy(rctx->results[i]);
        rm_free(rctx->results[i]);
      }
      array_free(rctx->results);
    }
    rm_free(rctx);
  }
}

static int cmpStrings(const char *s1, size_t l1, const char *s2, size_t l2) {
  int cmp = memcmp(s1, s2, MIN(l1, l2));
  if (l1 == l2) {
    // if the strings are the same length, just return the result of strcmp
    return cmp;
  }

  // if the strings are identical but the lengths aren't, return the longer string
  if (cmp == 0) {
    return l1 > l2 ? 1 : -1;
  } else {  // the strings are lexically different, just return that
    return cmp;
  }
}

static int cmp_results(const void *p1, const void *p2, const void *udata) {

  const searchResult *r1 = p1, *r2 = p2;
  const searchRequestCtx *req = udata;
  // Compary by sorting keys
  if (req->withSortby) {
    int cmp = 0;
    if ((r1->sortKey || r2->sortKey)) {
      // Sort by numeric sorting keys
      if (r1->sortKeyNum != HUGE_VAL && r2->sortKeyNum != HUGE_VAL) {
        double diff = r2->sortKeyNum - r1->sortKeyNum;
        cmp = diff < 0 ? -1 : (diff > 0 ? 1 : 0);
      } else if (r1->sortKey && r2->sortKey) {

        // Sort by string sort keys
        cmp = cmpStrings(r2->sortKey, r2->sortKeyLen, r1->sortKey, r1->sortKeyLen);
      } else {
        // If at least one of these has no sort key, it gets high value regardless of asc/desc
        return r2->sortKey ? 1 : -1;
      }
    }
    // in case of a tie or missing both sorting keys - compare ids
    if (!cmp) {
      cmp = cmpStrings(r2->id, r2->idLen, r1->id, r1->idLen);
    }
    return (req->sortAscending ? -cmp : cmp);
  }

  double s1 = r1->score, s2 = r2->score;
  if (s1 < s2) {
    return 1;
  } else if (s1 > s2) {
    return -1;
  } else {
    // This was reversed to be more compatible with OSS version where tie breaker was changed
    // to return the lower doc ID to reduce sorting heap work. Doc name might not be ascending
    // or descending but this still may reduce heap work.
    // Our tests are usually ascending so this will create similarity between RS and RSC.
    int rv = -cmpStrings(r2->id, r2->idLen, r1->id, r1->idLen);
    return rv;
  }
}

searchResult *newResult_resp2(searchResult *cached, MRReply *arr, size_t j,
                              searchReplyOffsets *offsets, int explainScores) {
  int scoreOffset = offsets->score;
  int fieldsOffset = offsets->firstField;
  int payloadOffset = offsets->payload;
  int sortKeyOffset = offsets->sortKey;
  searchResult *res = cached ? cached : rm_malloc(sizeof *res);
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
  // get fields
  res->fields = fieldsOffset > 0 ? MRReply_ArrayElement(arr, j + fieldsOffset) : NULL;
  // get payloads
  res->payload = payloadOffset > 0 ? MRReply_ArrayElement(arr, j + payloadOffset) : NULL;
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

searchResult *newResult_resp3(searchResult *cached, MRReply *results, size_t j,
                              searchReplyOffsets *offsets, bool explainScores,
                              specialCaseCtx *reduceSpecialCaseCtxSortBy) {
  searchResult *res = cached ? cached : rm_malloc(sizeof *res);
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

  // get fields
  res->fields = MRReply_MapElement(result_j, "extra_attributes");

  // get payloads
  res->payload = MRReply_MapElement(result_j, "payload");

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

/************************** Result processing callbacks **********************/

static int cmp_scored_results(const void *p1, const void *p2, const void *udata) {
  const scoredSearchResultWrapper *s1 = p1;
  const scoredSearchResultWrapper *s2 = p2;
  double score1 = s1->score;
  double score2 = s2->score;
  if (score1 < score2) {
    return -1;
  } else if (score1 > score2) {
    return 1;
  }
  return cmpStrings(s1->result->id, s1->result->idLen, s2->result->id, s2->result->idLen);
}

static double parseNumeric(const char *str, const char *sortKey) {
  RS_ASSERT(str[0] == '#');
  char *eptr;
  double d = fast_float_strtod(str + 1, &eptr);
  RS_ASSERT(eptr != sortKey + 1 && *eptr == 0);
  return d;
}

#define GET_NUMERIC_SCORE(d, searchResult_var, score_exp) \
  do {                                                    \
    if (res->sortKeyNum != HUGE_VAL) {                    \
      d = searchResult_var->sortKeyNum;                   \
    } else {                                              \
      const char *score = (score_exp);                    \
      d = parseNumeric(score, res->sortKey);              \
    }                                                     \
  } while (0);

static void ProcessKNNSearchResult(searchResult *res, searchReducerCtx *rCtx, double score,
                                   knnContext *knnCtx) {
  // As long as we don't have k results, keep insert
  if (heap_count(knnCtx->pq) < knnCtx->k) {
    scoredSearchResultWrapper *resWrapper = rm_malloc(sizeof(scoredSearchResultWrapper));
    resWrapper->result = res;
    resWrapper->score = score;
    heap_offerx(knnCtx->pq, resWrapper);
  } else {
    // Check for upper bound
    scoredSearchResultWrapper tmpWrapper;
    tmpWrapper.result = res;
    tmpWrapper.score = score;
    scoredSearchResultWrapper *largest = heap_peek(knnCtx->pq);
    int c = cmp_scored_results(&tmpWrapper, largest, rCtx->searchCtx);
    if (c < 0) {
      scoredSearchResultWrapper *resWrapper = rm_malloc(sizeof(scoredSearchResultWrapper));
      resWrapper->result = res;
      resWrapper->score = score;
      // Current result is smaller then upper bound, replace them.
      largest = heap_poll(knnCtx->pq);
      heap_offerx(knnCtx->pq, resWrapper);
      rCtx->cachedResult = largest->result;
      rm_free(largest);
    } else {
      rCtx->cachedResult = res;
    }
  }
}

static void ProcessKNNSearchReply(MRReply *arr, searchReducerCtx *rCtx) {
  if (arr == NULL) {
    return;
  }
  if (MRReply_Type(arr) == MR_REPLY_ERROR) {
    return;
  }

  bool resp3 = MRReply_Type(arr) == MR_REPLY_MAP;
  if (!resp3 && (MRReply_Type(arr) != MR_REPLY_ARRAY || MRReply_Length(arr) == 0)) {
    // Empty reply??
    return;
  }

  searchRequestCtx *req = rCtx->searchCtx;
  specialCaseCtx *reduceSpecialCaseCtxKnn = rCtx->reduceSpecialCaseCtxKnn;
  specialCaseCtx *reduceSpecialCaseCtxSortBy = rCtx->reduceSpecialCaseCtxSortby;
  searchResult *res;
  if (resp3) {
    // Check for a warning
    MRReply *warning = MRReply_MapElement(arr, "warning");
    RS_LOG_ASSERT(warning && MRReply_Type(warning) == MR_REPLY_ARRAY, "invalid warning record");
    if (!rCtx->warning && MRReply_Length(warning) > 0) {
      rCtx->warning = warning;
    }

    MRReply *results = MRReply_MapElement(arr, "results");
    RS_LOG_ASSERT(results && MRReply_Type(results) == MR_REPLY_ARRAY, "invalid results record");
    size_t len = MRReply_Length(results);
    for (int j = 0; j < len; ++j) {
      res = newResult_resp3(rCtx->cachedResult, results, j, &rCtx->offsets,
                            rCtx->searchCtx->withExplainScores, reduceSpecialCaseCtxSortBy);
      if (res && res->id) {
        rCtx->cachedResult = NULL;
      } else {
        RedisModule_Log(RSDummyContext, "warning",
                        "missing required_field when parsing redisearch results");
        goto error;
      }
      MRReply *require_fields =
          MRReply_MapElement(MRReply_ArrayElement(results, j), "required_fields");
      if (!require_fields) {
        RedisModule_Log(RSDummyContext, "warning",
                        "missing required_fields when parsing redisearch results");
        goto error;
      }
      MRReply *score_value =
          MRReply_MapElement(require_fields, reduceSpecialCaseCtxKnn->knn.fieldName);
      if (!score_value) {
        RedisModule_Log(RSDummyContext, "warning",
                        "missing knn required_field when parsing redisearch results");
        goto error;
      }
      double d;
      GET_NUMERIC_SCORE(d, res, MRReply_String(score_value, NULL));
      ProcessKNNSearchResult(res, rCtx, d, &reduceSpecialCaseCtxKnn->knn);
    }
    processResultFormat(&req->format, arr);

  } else {
    size_t len = MRReply_Length(arr);

    int step = rCtx->offsets.step;
    int scoreOffset = reduceSpecialCaseCtxKnn->knn.offset;
    for (int j = 1; j < len; j += step) {
      if (j + step > len) {
        RedisModule_Log(
            RSDummyContext, "warning",
            "got a bad reply from redisearch, reply contains less parameters then expected");
        rCtx->errorOccurred = true;
        break;
      }
      res = newResult_resp2(rCtx->cachedResult, arr, j, &rCtx->offsets,
                            rCtx->searchCtx->withExplainScores);
      if (res && res->id) {
        rCtx->cachedResult = NULL;
      } else {
        RedisModule_Log(RSDummyContext, "warning",
                        "missing required_field when parsing redisearch results");
        goto error;
      }

      double d;
      GET_NUMERIC_SCORE(d, res, MRReply_String(MRReply_ArrayElement(arr, j + scoreOffset), NULL));
      ProcessKNNSearchResult(res, rCtx, d, &reduceSpecialCaseCtxKnn->knn);
    }
  }
  return;

error:
  rCtx->errorOccurred = true;
  // invalid result - usually means something is off with the response, and we should just
  // quit this response
  rCtx->cachedResult = res;
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

static void processSearchReplyResult(searchResult *res, searchReducerCtx *rCtx) {
#ifdef ENABLE_ASSERT
  debugCheckAndPauseBeforeReduce(rCtx);
#endif

  if (!res || !res->id) {
    RedisModule_Log(RSDummyContext, "warning",
                    "got an unexpected argument when parsing redisearch results");
    rCtx->errorOccurred = true;
    // invalid result - usually means something is off with the response, and we should just
    // quit this response
    rCtx->cachedResult = res;
    return;
  }

  rCtx->cachedResult = NULL;

  // TODO: minmax_heap?
  if (heap_count(rCtx->pq) < heap_size(rCtx->pq)) {
    heap_offerx(rCtx->pq, res);
  } else {
    searchResult *smallest = heap_peek(rCtx->pq);
    int c = cmp_results(res, smallest, rCtx->searchCtx);
    if (c < 0) {
      smallest = heap_poll(rCtx->pq);
      heap_offerx(rCtx->pq, res);
      rCtx->cachedResult = smallest;
    } else {
      rCtx->cachedResult = res;
      if (rCtx->searchCtx->withSortby) {
        // If the result is lower than the last result in the heap,
        // AND there is a user-defined sort order - we can stop now
#ifdef ENABLE_ASSERT
        CoordReduceDebugCtx_IncrementReduceCount();
#endif
        return;
      }
    }
  }

#ifdef ENABLE_ASSERT
  CoordReduceDebugCtx_IncrementReduceCount();
#endif
}

static void processSearchReply(MRReply *arr, searchReducerCtx *rCtx) {
  if (arr == NULL) {
    return;
  }
  if (MRReply_Type(arr) == MR_REPLY_ERROR) {
    return;
  }

  bool resp3 = MRReply_Type(arr) == MR_REPLY_MAP;
  if (!resp3 && (MRReply_Type(arr) != MR_REPLY_ARRAY || MRReply_Length(arr) == 0)) {
    // Empty reply??
    return;
  }

  searchRequestCtx *req = rCtx->searchCtx;

  if (resp3)  // RESP3
  {
    // Check for a warning
    MRReply *warning = MRReply_MapElement(arr, "warning");
    RS_LOG_ASSERT(warning && MRReply_Type(warning) == MR_REPLY_ARRAY, "invalid warning record");
    if (!rCtx->warning && MRReply_Length(warning) > 0) {
      rCtx->warning = warning;
    }

    MRReply *total_results = MRReply_MapElement(arr, "total_results");
    if (!total_results) {
      rCtx->errorOccurred = true;
      return;
    }
    rCtx->totalReplies += MRReply_Integer(total_results);
    MRReply *results = MRReply_MapElement(arr, "results");
    if (!results) {
      rCtx->errorOccurred = true;
      return;
    }
    size_t len = MRReply_Length(results);

    bool needScore = rCtx->offsets.score > 0;
    for (int i = 0; i < len; ++i) {
      searchResult *res =
          newResult_resp3(rCtx->cachedResult, results, i, &rCtx->offsets,
                          rCtx->searchCtx->withExplainScores, rCtx->reduceSpecialCaseCtxSortby);
      processSearchReplyResult(res, rCtx);
    }
    processResultFormat(&rCtx->searchCtx->format, arr);
  } else  // RESP2
  {
    size_t len = MRReply_Length(arr);

    // first element is the total count
    rCtx->totalReplies += MRReply_Integer(MRReply_ArrayElement(arr, 0));

    int step = rCtx->offsets.step;

    for (int j = 1; j < len; j += step) {
      if (j + step > len) {
        RedisModule_Log(
            RSDummyContext, "warning",
            "got a bad reply from redisearch, reply contains less parameters then expected");
        rCtx->errorOccurred = true;
        break;
      }
      searchResult *res = newResult_resp2(rCtx->cachedResult, arr, j, &rCtx->offsets,
                                          rCtx->searchCtx->withExplainScores);
      processSearchReplyResult(res, rCtx);
    }
  }
}

/* MRCtx owns the immutable reply backing until request cleanup. A coordinator
 * docId is a (reply index, row ordinal) locator, never a local document-table id.
 * This keeps payloads, explanations and nested RESP values lossless without
 * copying every candidate's output fields into a second representation. */
typedef struct {
  ResultProcessor base;
  searchReducerCtx *ctx;
  int count;
  uint32_t shard;
  size_t row;
  MRReply *current;
  bool fromTimeout;
} RPSearchReply;

static MRReply *searchReplyBody(const searchRequestCtx *req, MRReply *reply) {
  if (!reply || MRReply_Type(reply) == MR_REPLY_ERROR || !req->profileArgs) return reply;
  return MRCtx_GetCommandProtocol(req->mrctx) == 3 ? MRReply_MapElement(reply, "Results")
                                                   : MRReply_ArrayElement(reply, 0);
}

static void searchResultView(searchReducerCtx *ctx, const SearchResult *row, searchResult *view) {
  uint64_t locator = SearchResult_GetDocId(row);
  MRReply *body = searchReplyBody(ctx->searchCtx, ctx->replies[locator >> 32]);
  uint32_t offset = (uint32_t)locator;
  *view = (searchResult){0};
  if (MRReply_Type(body) == MR_REPLY_MAP) {
    newResult_resp3(view, MRReply_MapElement(body, "results"), offset, &ctx->offsets,
                    ctx->searchCtx->withExplainScores, ctx->reduceSpecialCaseCtxSortby);
  } else {
    newResult_resp2(view, body, 1 + (size_t)offset * ctx->offsets.step, &ctx->offsets,
                    ctx->searchCtx->withExplainScores);
  }
}

static int compareSearchRows(const void *a, const void *b, const void *context) {
  searchReducerCtx *ctx = (searchReducerCtx *)context;
  searchResult left, right;
  searchResultView(ctx, a, &left);
  searchResultView(ctx, b, &right);
  return -cmp_results(&left, &right, ctx->searchCtx);
}

static int searchReplyNext(ResultProcessor *base, SearchResult *row) {
  RPSearchReply *source = (RPSearchReply *)base;
  searchReducerCtx *ctx = source->ctx;
  while (source->shard < (uint32_t)source->count) {
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
          if (!total || !MRReply_MapElement(body, "results")) {
            ctx->errorOccurred = true;
            goto next_shard;
          }
          ctx->totalReplies += MRReply_Integer(total);
          source->row = 0;
        } else {
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
        // Shard row counts are bounded by MAX_SEARCH_REQUEST_RESULTS.
        if (source->row > UINT32_MAX) {
          ctx->errorOccurred = true;
          goto next_shard;
        }
        if (step > len - offset) {
          ctx->errorOccurred = true;
          goto next_shard;
        }
        SearchResult_SetDocId(row, ((uint64_t)source->shard << 32) | source->row++);
        searchResult view;
        searchResultView(ctx, row, &view);
#ifdef ENABLE_ASSERT
        debugCheckAndPauseBeforeReduce(ctx);
#endif
        if (!view.id) {
          ctx->errorOccurred = true;
          continue;
        }
        SearchResult_SetScore(row, view.score);
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
  rm_free(base);
}

static void reduceSearchPipeline(searchReducerCtx *ctx, int count, MRReply **replies,
                                 bool fromTimeout) {
  ctx->pipeline = true;
  ctx->replies = replies;
  ctx->qctx.err = &ctx->searchCtx->base.reply.err;
  ctx->qctx.timeoutPolicy = ctx->searchCtx->base.timeout.config.timeoutPolicy;
  ctx->qctx.resultLimit = UINT32_MAX;
  ctx->qctx.skipIndexResultDeepCopy = true;
  RPSearchReply *source = rm_calloc(1, sizeof(*source));
  source->base =
      (ResultProcessor){.Next = searchReplyNext, .Free = searchReplyFree, .type = RP_SEARCH_REPLY};
  source->ctx = ctx;
  source->count = count;
  source->fromTimeout = fromTimeout;
  QITR_PushRP(&ctx->qctx, &source->base);
  QITR_PushRP(&ctx->qctx, RPSorter_NewByComparator(ctx->searchCtx->requestedResultsCount,
                                                   compareSearchRows, ctx));
  if (ctx->searchCtx->limit != 0) {
    QITR_PushRP(&ctx->qctx, RPPager_New(ctx->searchCtx->offset, ctx->searchCtx->limit));
  }
  QueryRequest_SetEndProcRef(&ctx->searchCtx->base, &ctx->qctx.endProc);
  ctx->results = array_new(SearchResult *, 8);
  SearchResult row = SearchResult_New();
  // A count-only request still consumes envelopes but has no candidates to sort.
  ResultProcessor *end = ctx->qctx.endProc;
  if (ctx->searchCtx->limit == 0) end = ctx->qctx.rootProc;
  while (end->Next(end, &row) == RS_RESULT_OK) {
    if (ctx->searchCtx->limit != 0) {
      array_append(ctx->results, SearchResult_AllocateMove(&row));
      row = SearchResult_New();
    } else {
      SearchResult_Clear(&row);
    }
  }
  SearchResult_Destroy(&row);
}

/************************ Result post processing callbacks ********************/

static void noOpPostProcess(searchReducerCtx *rCtx) {
  return;
}

static void knnPostProcess(searchReducerCtx *rCtx) {
  specialCaseCtx *reducerSpecialCaseCtx = rCtx->reduceSpecialCaseCtxKnn;
  RS_ASSERT(reducerSpecialCaseCtx->specialCaseType == SPECIAL_CASE_KNN);
  if (reducerSpecialCaseCtx->knn.pq) {
    size_t numberOfResults = heap_count(reducerSpecialCaseCtx->knn.pq);
    for (size_t i = 0; i < numberOfResults; i++) {
      scoredSearchResultWrapper *wrappedResult = heap_poll(reducerSpecialCaseCtx->knn.pq);
      searchResult *res = wrappedResult->result;
      rm_free(wrappedResult);
      if (heap_count(rCtx->pq) < heap_size(rCtx->pq)) {
        heap_offerx(rCtx->pq, res);
      } else {
        searchResult *smallest = heap_peek(rCtx->pq);
        int c = cmp_results(res, smallest, rCtx->searchCtx);
        if (c < 0) {
          smallest = heap_poll(rCtx->pq);
          heap_offerx(rCtx->pq, res);
          rm_free(smallest);
        } else {
          rm_free(res);
        }
      }
    }
    // `shouldSort`: ProcessKNNSearchReply never accumulated the shards'
    // total_results, it only fed the K-bounded knn.pq. rCtx->pq is sized
    // MAX(K, offset+count) on this path (see setKNNSpecialCase), so the merge
    // above dropped nothing and its count is exactly the number of matching
    // documents, min(K, global matches).
    rCtx->totalReplies = heap_count(rCtx->pq);
  } else {
    // SORTBY by the distance field: processSearchReply already summed each
    // shard's total_results, i.e. sum_i min(K, matches_i). rCtx->pq here is only
    // a response window sized MIN(K, offset+count), so its count is not a hit
    // count. Clamping the sum to K is exact: if every shard has at most K
    // matches the sum *is* the global match count, and otherwise both the sum
    // and the global match count are >= K.
    rCtx->totalReplies = MIN(reducerSpecialCaseCtx->knn.k, rCtx->totalReplies);
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

  // Number of results to actually return
  size_t num = req->offset + req->limit;

  size_t qlen = rCtx->pipeline ? array_len(rCtx->results) : heap_count(rCtx->pq);
  size_t pos = qlen;
  size_t first = rCtx->pipeline ? 0 : req->offset;

  // Load the results from the heap into a sorted array. Free the items in
  // the heap one-by-one so that we don't have to go through them again
  searchResult **results = rm_malloc(sizeof(*results) * qlen);
  if (rCtx->pipeline) {
    for (size_t i = 0; i < qlen; ++i) {
      results[i] = rm_calloc(1, sizeof(*results[i]));
      searchResultView(rCtx, rCtx->results[i], results[i]);
    }
  } else {
    while (pos) results[--pos] = heap_poll(rCtx->pq);
    heap_free(rCtx->pq);
    rCtx->pq = NULL;
  }

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
    for (size_t i = first; i < qlen && i < num; ++i) {
      RedisModule_Reply_MapWithLen(reply, entries);  // >> result
      searchResult *res = results[i];

      RedisModule_ReplyKV_StringBuffer(reply, "id", res->id, res->idLen);

      if (req->withScores) {
        RedisModule_Reply_SimpleString(reply, "score");

        if (req->withExplainScores) {
          RedisModule_Reply_ArrayWithLen(reply, SCORE_WITH_EXPLAIN_REPLY_LEN);
          RedisModule_Reply_Double(reply, res->score);
          MR_ReplyWithMRReply(reply, res->explainScores);
        } else {
          RedisModule_Reply_Double(reply, res->score);
        }
      }

      if (req->withPayload) {
        RedisModule_Reply_SimpleString(reply, "payload");
        MR_ReplyWithMRReply(reply, res->payload);
      }

      if (req->withSortingKeys && req->withSortby) {
        RedisModule_Reply_SimpleString(reply, "sortkey");
        if (res->sortKey) {
          RedisModule_Reply_StringBuffer(reply, res->sortKey, res->sortKeyLen);
        } else {
          RedisModule_Reply_Null(reply);
        }
      }
      if (!req->noContent) {
        RedisModule_ReplyKV_MRReply(reply, "extra_attributes", res->fields);  // >> extra_attributes
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

    for (pos = first; pos < qlen && pos < num; pos++) {
      searchResult *res = results[pos];
      RedisModule_Reply_StringBuffer(reply, res->id, res->idLen);
      if (req->withScores) {
        if (req->withExplainScores) {
          RedisModule_Reply_ArrayWithLen(reply, SCORE_WITH_EXPLAIN_REPLY_LEN);
          RedisModule_Reply_Double(reply, res->score);
          MR_ReplyWithMRReply(reply, res->explainScores);
        } else {
          RedisModule_Reply_Double(reply, res->score);
        }
      }
      if (req->withPayload) {
        MR_ReplyWithMRReply(reply, res->payload);
      }
      if (req->withSortingKeys && req->withSortby) {
        if (res->sortKey) {
          RedisModule_Reply_StringBuffer(reply, res->sortKey, res->sortKeyLen);
        } else {
          RedisModule_Reply_Null(reply);
        }
      }
      if (!req->noContent) {
        MR_ReplyWithMRReply(reply, res->fields);
      }
    }
  }
  RedisModule_Reply_MapOrArrayEnd(reply);

  if (req->queryOOM) {
    QueryWarningsGlobalStats_UpdateWarning(QUERY_WARNING_CODE_OUT_OF_MEMORY_COORD, 1,
                                           COORD_ERR_WARN);
  }
  //-------------------------------------------------------------------------------------------

  // Free the sorted results
  for (pos = 0; pos < qlen; pos++) {
    rm_free(results[pos]);
  }
  rm_free(results);
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

// Coordinator reply with empty search results for FT.SEARCH command.
// Creates a dummy searchReducerCtx with empty heap to use existing sendSearchResults logic.
// Handles RESP2/RESP3 protocol and formatting.
// Currently used during OOM conditions for early bailout and return empty results instead of
// failing.
void sendSearchResults_EmptyResults(RedisModule_Reply *reply, searchRequestCtx *req) {
  // Setup a dummy searchReducerCtx that will be used by sendSearchResults
  searchReducerCtx rCtx = {NULL};
  rCtx.searchCtx = req;
  // Create empty heap (dynamic allocation is necessary for heap_free in sendSearchResults)
  heap_t *emptyHeap = heap_new(cmp_results, req);
  // The empty heap will result in an empty reply
  rCtx.pq = emptyHeap;

  if (req->profileArgs > 0) {
    profileSearchReply(reply, &rCtx, 0, NULL, &req->profileClock, rs_wall_clock_now_ns());
  } else {
    sendSearchResults(reply, &rCtx);
  }
}

static void searchResultReducer_wrapper(void *mc_v) {
  struct MRCtx *mc = mc_v;
  searchRequestCtx *req = MRCtx_GetPrivData(mc);
  searchResultReducer(req, MRCtx_GetNumReplied(mc), MRCtx_GetReplies(mc), false);
  // The worker owns blocked-client completion even when timeout owns the reply.
  RedisModule_UnblockClient(MRCtx_GetBlockedClient(mc), &req->base);
  MRCtx_DecrRef(mc);
}

int CoordSearchReducer_Schedule(struct MRCtx *mc, int count, MRReply **replies) {
  MRCtx_IncrRef(mc);
  ConcurrentSearch_ThreadPoolRun(searchResultReducer_wrapper, mc);
  return REDISMODULE_OK;
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

static int searchResultReducer(searchRequestCtx *req, int count, MRReply **replies,
                               bool fromTimeout) {
  RedisModuleBlockedClient *bc = NULL;
  searchReducerCtx *rCtx = NULL;
  int profile = 0;
  size_t num = 0;

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

  bc = MRCtx_GetBlockedClient(req->mrctx);

  rCtx = rm_calloc(1, sizeof(searchReducerCtx));

  // Save the reducer state in the request so it is available to the
  // blocked-client reply callback after the normal unblock path.
  req->rctx = rCtx;
  // Set searchCtx early so it's available even if we bail out early
  rCtx->searchCtx = req;

  profile = req->profileArgs > 0;

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
      rCtx->errorOccurred = true;
      rCtx->lastError = curr_rep;
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

  // Init results heap.
  num = req->requestedResultsCount;
  rCtx->pq = rm_malloc(heap_sizeof(num));
  heap_init(rCtx->pq, cmp_results, req, num);

  // Default result process and post process operations
  rCtx->processReply = (processReplyCB)processSearchReply;
  rCtx->postProcess = (postProcessReplyCB)noOpPostProcess;

  if (req->specialCases) {
    size_t nSpecialCases = array_len(req->specialCases);
    for (size_t i = 0; i < nSpecialCases; ++i) {
      if (req->specialCases[i]->specialCaseType == SPECIAL_CASE_KNN) {
        specialCaseCtx *knnCtx = req->specialCases[i];
        rCtx->postProcess = (postProcessReplyCB)knnPostProcess;
        rCtx->reduceSpecialCaseCtxKnn = knnCtx;
        if (knnCtx->knn.shouldSort) {
          knnCtx->knn.pq = rm_malloc(heap_sizeof(knnCtx->knn.k));
          heap_init(knnCtx->knn.pq, cmp_scored_results, NULL, knnCtx->knn.k);
          rCtx->processReply = (processReplyCB)ProcessKNNSearchReply;
          break;
        }
      } else if (req->specialCases[i]->specialCaseType == SPECIAL_CASE_SORTBY) {
        rCtx->reduceSpecialCaseCtxSortby = req->specialCases[i];
      }
    }
  }

  if (!rCtx->reduceSpecialCaseCtxKnn) {
    heap_free(rCtx->pq);
    rCtx->pq = NULL;
    reduceSearchPipeline(rCtx, count, replies, fromTimeout);
    if (!fromTimeout && QueryRequestTimeout_IsBlockedClientTimedOut(&req->base.timeout)) {
      goto cleanup;
    }
  } else if (!profile) {
    for (int i = 0; i < count; ++i) {
      rCtx->processReply(replies[i], rCtx);
      if (!fromTimeout && QueryRequestTimeout_IsBlockedClientTimedOut(&req->base.timeout)) {
        goto cleanup;
      }
    }
  } else {
    const bool resp3 = MRCtx_GetCommandProtocol(req->mrctx) == 3;
    for (int i = 0; i < count; ++i) {
      MRReply *mr_reply;

      // Check that the reply is not an error, can be caused if a shard failed to execute the query
      if (MRReply_Type(replies[i]) == MR_REPLY_ERROR) {
        continue;
      }

      if (resp3) {
        mr_reply = MRReply_MapElement(replies[i], "Results");
      } else {
        mr_reply = MRReply_ArrayElement(replies[i], 0);
      }
      rCtx->processReply(mr_reply, rCtx);
      if (!fromTimeout && QueryRequestTimeout_IsBlockedClientTimedOut(&req->base.timeout)) {
        goto cleanup;
      }
    }
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

  if (rCtx->errorOccurred && !rCtx->lastError) {
    QueryError_SetError(&req->base.reply.err, QUERY_ERROR_CODE_GENERIC,
                        "could not parse redisearch results");
    goto cleanup;
  }

cleanup:
  if (rCtx) {
    // Call postProcess even on early exits (e.g. timeouts) so that partially
    // processed special cases like KNN can flush their internal queues into
    // the final result heap.
    if (rCtx->postProcess) {
      rCtx->postProcess(rCtx);
      rCtx->postProcess = NULL;
    }
    rm_free(rCtx->cachedResult);
    rCtx->cachedResult = NULL;
  }

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
int CoordSearchReducer_Reply(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  UNUSED(argv);
  UNUSED(argc);
  QueryRequest *request = RedisModule_GetBlockedClientPrivateData(ctx);
  if (request) {
    searchRequestCtx *req = QueryRequest_GetSearch(request);
    struct MRCtx *mrctx = req->mrctx;

    // Check if we have an error and return it
    if (QueryError_HasError(&request->reply.err)) {
      // Track error in global statistics
      QueryErrorsGlobalStats_UpdateError(QueryError_GetCode(&request->reply.err), 1,
                                         COORD_ERR_WARN);
      QueryError_ReplyAndClear(ctx, &request->reply.err);
      return REDISMODULE_OK;
    }

    if (MRCtx_GetNumReplied(mrctx) == 0) {
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
      profileSearchReply(reply, rCtx, MRCtx_GetNumReplied(mrctx), MRCtx_GetReplies(mrctx),
                         &req->profileClock, rs_wall_clock_now_ns());
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
  struct MRCtx *mrctx = req->mrctx;

  // Signal timeout to stop accepting new replies in searchFanoutCallback
  QueryRequestTimeout_MarkTimedOut(&req->base.timeout);

  recordSearchTimeoutStage(req, /*isError=*/false);

  // Try to claim reducing - if we get it, run reducer on main thread
  // If we don't get it, reducer is already running (or bailout claimed it) - wait for it
  if (QueryRequest_TryClaimResults(&req->base)) {
    // We claimed reducing - run reducer on main thread with current replies
    searchResultReducer(req, MRCtx_GetNumReplied(mrctx), MRCtx_GetReplies(mrctx), true);
  } else {
    // Reducer already running or bailout claimed it - wait for completion
    QueryRequest_WaitForResultsComplete(&req->base);

    // A background reducer may have claimed reducing, observed the timeout,
    // and exited before initializing req->rctx. In that case adopt the
    // timeout-owned reduction path now that the competing reducer has finished.
    if (!QueryError_HasError(&request->reply.err) && req->rctx == NULL) {
      searchResultReducer(req, MRCtx_GetNumReplied(mrctx), MRCtx_GetReplies(mrctx), true);
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
    profileSearchReply(reply, rCtx, MRCtx_GetNumReplied(mrctx), MRCtx_GetReplies(mrctx),
                       &req->profileClock, rs_wall_clock_now_ns());
  } else {
    sendSearchResults(reply, rCtx);
  }
  RedisModule_EndReply(reply);

  return REDISMODULE_OK;
}
