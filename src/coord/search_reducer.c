/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#include "search_reducer.h"
#include "aggregate/aggregate.h"
#include "aggregate/aggregate_debug.h"
#include "concurrent_ctx.h"
#include "debug_commands.h"
#include "dist_profile.h"
#include "fast_float/fast_float_strtod.h"
#include "info/global_stats.h"
#include "query_error_ffi.h"
#include "query_memory.h"
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

// More distinct warnings than the shards can send.
#define SEARCH_REDUCER_MAX_WARNINGS 8

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

  // Distinct warnings of the shards' replies, in arrival order. Each points into a reply that
  // outlives the reducer.
  MRReply *warnings[SEARCH_REDUCER_MAX_WARNINGS];
  size_t numWarnings;

  // Whether merging checks for a stop per row (mergeRowShouldStop).
  bool rowStopChecks;
  uint32_t rowsSinceMemoryRead;
  // The OOM error in the request's reply is the coordinator's own OOM stop, not a shard's.
  bool oomErrorMapped;
} searchReducerCtx;

// Per merged row: the stop flag, and memory every QUERY_MEMORY_RATIO_READ_INTERVAL rows, so one
// large shard reply cannot run far past the limit before the reducer's read after that reply.
// Calls QueryMemory_CheckpointExact only on those rows, which keeps short replies out of the hook's
// AT_CALL count.
static bool mergeRowShouldStop(searchReducerCtx *rCtx) {
  if (!rCtx->rowStopChecks) {
    return false;
  }
  QueryRequestLimits *timeout = &rCtx->searchCtx->base.limits;
  if (QueryRequestLimits_IsMemoryStopped(timeout) ||
      QueryRequestLimits_IsBlockedClientTimedOut(timeout)) {
    return true;
  }
  if (++rCtx->rowsSinceMemoryRead < QueryMemory_GetRatioReadInterval()) {
    return false;
  }
  rCtx->rowsSinceMemoryRead = 0;
  return QueryMemory_CheckpointExact(timeout);
}

typedef struct {
  searchResult *result;
  double score;
} scoredSearchResultWrapper;

static int searchResultReducer(searchRequestCtx *req, int count, MRReply **replies,
                               bool fromTimeout);

static void searchReducerCtx_Free(searchReducerCtx *rctx) {
  if (!rctx) {
    return;
  }
  if (rctx->pq) {
    heap_destroy(rctx->pq);
  }
  if (rctx->reduceSpecialCaseCtxKnn && rctx->reduceSpecialCaseCtxKnn->knn.pq) {
    heap_destroy(rctx->reduceSpecialCaseCtxKnn->knn.pq);
    rctx->reduceSpecialCaseCtxKnn->knn.pq = NULL;
  }
  rm_free(rctx);
}

void CoordSearchReducer_Free(searchRequestCtx *r) {
  searchReducerCtx_Free(r->rctx);
  r->rctx = NULL;
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

searchResult *newResult_resp2(searchResult *cached, MRReply *arr, int j,
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

searchResult *newResult_resp3(searchResult *cached, MRReply *results, int j,
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
    RS_LOG_ASSERT_FMT(false, "Expected id %d to exist, and be a string", j);
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
  if (heap_count(knnCtx->pq) < knnCtx->pqBound) {
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

// Keeps each distinct warning once, so that shards stopping for different reasons are all
// reported.
static void collectShardWarnings(searchReducerCtx *rCtx, MRReply *warnings) {
  const size_t len = MRReply_Length(warnings);
  for (size_t i = 0; i < len; ++i) {
    MRReply *warning = MRReply_ArrayElement(warnings, i);
    size_t warningLen;
    const char *str = MRReply_String(warning, &warningLen);
    bool known = false;
    for (size_t j = 0; j < rCtx->numWarnings && !known; ++j) {
      size_t knownLen;
      const char *knownStr = MRReply_String(rCtx->warnings[j], &knownLen);
      known = knownLen == warningLen && !memcmp(str, knownStr, warningLen);
    }
    if (!known && rCtx->numWarnings < SEARCH_REDUCER_MAX_WARNINGS) {
      rCtx->warnings[rCtx->numWarnings++] = warning;
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
    collectShardWarnings(rCtx, warning);

    MRReply *results = MRReply_MapElement(arr, "results");
    RS_LOG_ASSERT(results && MRReply_Type(results) == MR_REPLY_ARRAY, "invalid results record");
    size_t len = MRReply_Length(results);
    for (int j = 0; j < len; ++j) {
      if (mergeRowShouldStop(rCtx)) {
        break;
      }
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
      if (mergeRowShouldStop(rCtx)) {
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
      if (QueryRequestLimits_IsBlockedClientTimedOut(&rCtx->searchCtx->base.limits)) {
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
    collectShardWarnings(rCtx, warning);

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
      if (mergeRowShouldStop(rCtx)) {
        break;
      }
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
      if (mergeRowShouldStop(rCtx)) {
        break;
      }
      searchResult *res = newResult_resp2(rCtx->cachedResult, arr, j, &rCtx->offsets,
                                          rCtx->searchCtx->withExplainScores);
      processSearchReplyResult(res, rCtx);
    }
  }
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

// The reply has started, so a stop here can only truncate it, with the OOM warning: ON_OOM RETURN
// only. ON_OOM FAIL was decided before the first reply byte (reducerShouldStop, then
// CoordSearchReducer_Reply).
// With `exact` it reads memory now.
static bool replyShouldStop(searchRequestCtx *req, bool stoppedBefore, bool exact) {
  QueryRequestLimits *timeout = &req->base.limits;
  if (stoppedBefore || req->base.reqConfig.oomPolicy == OomPolicy_Fail ||
      !(exact ? QueryMemory_CheckpointExact(timeout) : QueryMemory_Checkpoint(timeout))) {
    return false;
  }
  req->queryOOM = true;
  return true;
}

static void sendSearchResults(RedisModule_Reply *reply, searchReducerCtx *rCtx) {
  searchRequestCtx *req = rCtx->searchCtx;

  // Number of results to actually return
  size_t num = req->offset + req->limit;
  // The reply of an already stopped or timed-out query is the partial one; do not cut it further.
  const bool stoppedBefore = QueryRequestLimits_IsMemoryStopped(&req->base.limits) ||
                             QueryRequestLimits_IsBlockedClientTimedOut(&req->base.limits);
  // Under ON_OOM RETURN the reducer does not read after its last merge, so the reply reads once
  // before its rows: a reply shorter than the read interval, or with no row, would never read.
  const bool stopBeforeRows = replyShouldStop(req, stoppedBefore, true);

  if (stopBeforeRows && rCtx->pq) {
    // No row is replied: free the merged results rather than sort them into a new array.
    while (heap_count(rCtx->pq)) {
      rm_free(heap_poll(rCtx->pq));
    }
  }
  // A reduction that exited before building its heap replies no rows.
  size_t qlen = rCtx->pq ? heap_count(rCtx->pq) : 0;
  size_t pos = qlen;

  // Load the results from the heap into a sorted array. Free the items in
  // the heap one-by-one so that we don't have to go through them again
  searchResult **results = rm_malloc(sizeof(*results) * qlen);
  while (pos) {
    results[--pos] = heap_poll(rCtx->pq);
  }
  if (rCtx->pq) {
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
    for (size_t i = rCtx->searchCtx->offset; i < qlen && i < num; ++i) {
      if (stopBeforeRows || replyShouldStop(req, stoppedBefore, false)) {
        break;
      }
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

    // After the results, so a reply cut short for memory still carries its warning.
    // Each distinct warning once: the shards', then the coordinator's own stops. Without a memory
    // stop, shard warnings stand in for the timeout warning.
    RedisModule_Reply_SimpleString(reply, "warning");  // >warning
    const char *timeoutWarning = QueryWarning_Strwarning(QUERY_WARNING_CODE_TIMED_OUT);
    bool addTimeoutWarning =
        req->timedOut &&
        (rCtx->numWarnings == 0 || QueryRequestLimits_IsMemoryStopped(&req->base.limits));
    for (size_t i = 0; i < rCtx->numWarnings && addTimeoutWarning; ++i) {
      addTimeoutWarning = strcmp(MRReply_String(rCtx->warnings[i], NULL), timeoutWarning) != 0;
    }
    RedisModule_Reply_ArrayWithLen(reply, rCtx->numWarnings + req->queryOOM + addTimeoutWarning);
    for (size_t i = 0; i < rCtx->numWarnings; ++i) {
      MRReply *currentWarning = rCtx->warnings[i];
      const char *warning_str = MRReply_String(currentWarning, NULL);
      QueryWarningCode warningCode = QueryWarningCode_GetCodeFromMessage(warning_str);
      QueryWarningsGlobalStats_UpdateWarning(warningCode, 1, COORD_ERR_WARN);
      MR_ReplyWithMRReply(reply, currentWarning);
    }
    if (req->queryOOM) {
      QueryWarningsGlobalStats_UpdateWarning(QUERY_WARNING_CODE_OUT_OF_MEMORY_COORD, 1,
                                             COORD_ERR_WARN);
      RedisModule_Reply_SimpleString(reply, QUERY_WOOM_COORD);
    }
    if (addTimeoutWarning) {
      QueryWarningsGlobalStats_UpdateWarning(QUERY_WARNING_CODE_TIMED_OUT, 1, COORD_ERR_WARN);
      RedisModule_Reply_SimpleString(reply, timeoutWarning);
    }
  }
  //-------------------------------------------------------------------------------------------
  else  // RESP2
  {
    RedisModule_Reply_LongLong(reply, rCtx->totalReplies);

    for (pos = rCtx->searchCtx->offset; pos < qlen && pos < num; pos++) {
      if (stopBeforeRows || replyShouldStop(req, stoppedBefore, false)) {
        break;
      }
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

  // RESP3 counted it with its warning section; RESP2 replies no warning but the stop still counts.
  if (req->queryOOM && !reply->resp3) {
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
    return req->base.limits.timeout.config.timeoutPolicy == TimeoutPolicy_Fail;
  }
  // Check if this is an OOM error with non-fail policy
  if (errCode == QUERY_ERROR_CODE_OUT_OF_MEMORY) {
    return req->base.reqConfig.oomPolicy == OomPolicy_Fail;
  }

  // For any other error, return it
  return true;
}

static bool searchStoppedForOom(const searchRequestCtx *req) {
  return QueryRequestLimits_IsMemoryStopped(&req->base.limits);
}

// An upper bound on the rows the kept replies hold, cheap enough to size the heap by.
static size_t searchRepliesRowBound(const searchRequestCtx *req, int count, MRReply **replies) {
  const bool profile = req->profileArgs > 0;
  size_t rows = 0;
  for (int i = 0; i < count; i++) {
    MRReply *r = replies[i];
    if (profile && r && MRReply_Type(r) == MR_REPLY_MAP) {
      r = MRReply_MapElement(r, "Results");
    } else if (profile && r && MRReply_Type(r) == MR_REPLY_ARRAY && MRReply_Length(r) > 0) {
      r = MRReply_ArrayElement(r, 0);
    }
    if (!r) {
      continue;
    }
    if (MRReply_Type(r) == MR_REPLY_MAP) {
      rows += MRReply_Length(MRReply_MapElement(r, "results"));
    } else if (MRReply_Type(r) == MR_REPLY_ARRAY) {
      rows += MRReply_Length(r);
    }
  }
  return rows;
}

// Stop merging on a blocked-client timeout, or when memory runs short while merging (read once per
// shard reply merged). A stop requested before, by the fan-out, already discarded the replies that
// landed after it, so the ones kept are merged. After the last reply only ON_OOM FAIL reads: that
// read is FAIL's last chance to fail before the reply starts (the reply loop cannot).
// The timeout callback owns the reply (`fromTimeout`), so a fired timeout does not stop its own
// merge and memory is not read again, but a memory stop already requested still does: it must not
// leave the main thread merging every queued shard reply.
static bool reducerShouldStop(searchRequestCtx *req, bool stoppedBeforeReducing, bool lastReply,
                              bool fromTimeout) {
  QueryRequestLimits *timeout = &req->base.limits;
  if (fromTimeout) {
    return !stoppedBeforeReducing && QueryRequestLimits_IsMemoryStopped(timeout);
  }
  if (QueryRequestLimits_IsBlockedClientTimedOut(timeout)) {
    return true;
  }
  if (stoppedBeforeReducing || (lastReply && req->base.reqConfig.oomPolicy != OomPolicy_Fail)) {
    return false;
  }
  return QueryMemory_CheckpointExact(timeout);
}

static int searchResultReducer(searchRequestCtx *req, int count, MRReply **replies,
                               bool fromTimeout) {
  RedisModuleBlockedClient *bc = NULL;
  searchReducerCtx *rCtx = NULL;
  int profile = 0;
  size_t num = 0;
  size_t keptRowsBound = SIZE_MAX;
  MRReply *returnedReply = NULL;
  QueryErrorCode returnedCode = QUERY_ERROR_CODE_OK;
  // A stop requested before reducing (in the fan-out) leaves the replies kept so far to merge.
  const bool stoppedBeforeReducing = QueryRequestLimits_IsMemoryStopped(&req->base.limits);

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
      if (QueryRequestLimits_IsBlockedClientTimedOut(&req->base.limits)) {
        CoordReduceDebugCtx_SetPause(false);
        break;
      }
      usleep(1000);  // Spin-wait with 1ms sleep
    }
  }
#endif

  // No reduction is needed if the timeout callback already replied.
  if (!fromTimeout && QueryRequestLimits_IsBlockedClientTimedOut(&req->base.limits)) {
    goto cleanup;
  }

  bc = MRCtx_GetBlockedClient(req->mrctx);

  rCtx = rm_calloc(1, sizeof(searchReducerCtx));

  // Save the reducer state in the request so it is available to the
  // blocked-client reply callback after the normal unblock path.
  req->rctx = rCtx;
  // Set searchCtx early so it's available even if we bail out early
  rCtx->searchCtx = req;
  // The timeout callback's reducer merges what landed; a fan-out stop already kept only the
  // replies to merge.
  rCtx->rowStopChecks = !fromTimeout && !stoppedBeforeReducing;

  profile = req->profileArgs > 0;

  // got no replies; a fan-out stopped for OOM before keeping any reply replies empty instead
  if (!fromTimeout && ((count == 0 && !searchStoppedForOom(req)) || req->limit < 0)) {
    QueryError_SetError(&req->base.reply.err, QUERY_ERROR_CODE_GENERIC,
                        "Could not send query to cluster");
    goto cleanup;
  }

  // Traverse the replies, check for early bail-out which we want for all errors
  // but timeout+non-strict timeout policy. Every reply is scanned: among the errors that must be
  // returned the first wins, except that an OOM error replaces a timeout one.
  for (int i = 0; i < count; i++) {
    MRReply *curr_rep = replies[i];
    if (MRReply_Type(curr_rep) == MR_REPLY_ERROR) {
      rCtx->errorOccurred = true;
      rCtx->lastError = curr_rep;
      const char *errStr = MRReply_String(curr_rep, NULL);
      QueryErrorCode errCode = QueryError_GetCodeFromMessage(errStr);
      if (should_return_error(req, errCode) &&
          (!returnedReply || (returnedCode == QUERY_ERROR_CODE_TIMED_OUT &&
                              errCode == QUERY_ERROR_CODE_OUT_OF_MEMORY))) {
        returnedReply = curr_rep;
        returnedCode = errCode;
      }
    }
  }
  if (returnedReply) {
    // Shard reply already contains the prefixed error string — set directly.
    rCtx->lastError = returnedReply;
    QueryError_SetCode(&req->base.reply.err, returnedCode);
    QueryError_SetDetail(&req->base.reply.err, MRReply_String(returnedReply, NULL));
    goto cleanup;
  }

  // The fan-out stopped for OOM under ON_OOM FAIL: cleanup replies the OOM error, so the kept
  // replies cannot be returned. Retained shard errors above keep their precedence. The heap stays
  // NULL: a timeout callback that takes over the reply reruns the reduction.
  if (!fromTimeout && stoppedBeforeReducing && searchStoppedForOom(req) &&
      req->base.reqConfig.oomPolicy == OomPolicy_Fail) {
    goto cleanup;
  }

  // Get reply offsets
  getReplyOffsets(rCtx->searchCtx, &rCtx->offsets);

  // Init results heap. After an OOM stop only the kept rows can fill it, so do not allocate for
  // the requested limit under memory pressure.
  num = req->requestedResultsCount;
  if (searchStoppedForOom(req)) {
    keptRowsBound = searchRepliesRowBound(req, count, replies);
    if (keptRowsBound < num) {
      num = keptRowsBound > 0 ? keptRowsBound : 1;
    }
  }
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
          // The K heap gets the same bound: K is user supplied and may be huge. The stored bound
          // decides when the heap is full, never its capacity, which may start smaller and grow;
          // a bound below K only drops rows no reply holds.
          size_t knnCap = knnCtx->knn.k;
          if (keptRowsBound < knnCap) {
            knnCap = keptRowsBound > 0 ? keptRowsBound : 1;
          }
          knnCtx->knn.pqBound = knnCap;
          knnCtx->knn.pq = rm_malloc(heap_sizeof(knnCap));
          heap_init(knnCtx->knn.pq, cmp_scored_results, NULL, knnCap);
          rCtx->processReply = (processReplyCB)ProcessKNNSearchReply;
          break;
        }
      } else if (req->specialCases[i]->specialCaseType == SPECIAL_CASE_SORTBY) {
        rCtx->reduceSpecialCaseCtxSortby = req->specialCases[i];
      }
    }
  }

  if (!profile) {
    for (int i = 0; i < count; ++i) {
      rCtx->processReply(replies[i], rCtx);
      if (reducerShouldStop(req, stoppedBeforeReducing, i == count - 1, fromTimeout)) {
        goto cleanup;
      }
    }
  } else {
    const bool resp3 = MRCtx_GetCommandProtocol(req->mrctx) == 3;
    for (int i = 0; i < count; ++i) {
      MRReply *mr_reply;

      // Check that the reply is not an error, can be caused if a shard failed to execute the query
      if (MRReply_Type(replies[i]) != MR_REPLY_ERROR) {
        if (resp3) {
          mr_reply = MRReply_MapElement(replies[i], "Results");
        } else {
          mr_reply = MRReply_ArrayElement(replies[i], 0);
        }
        rCtx->processReply(mr_reply, rCtx);
      }
      // Also after an error reply, so the read after the last reply always runs.
      if (reducerShouldStop(req, stoppedBeforeReducing, i == count - 1, fromTimeout)) {
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
      if (QueryRequestLimits_IsBlockedClientTimedOut(&req->base.limits)) {
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
  // A merge loop that stopped for OOM replies what the heap holds with the OOM warning, or the
  // OOM error under ON_OOM FAIL; the reply callback reads both. Not once the timeout callback
  // owns the reply: its timeout reply wins over an OOM stop.
  if (!fromTimeout && !QueryRequestLimits_IsBlockedClientTimedOut(&req->base.limits) &&
      QueryRequestLimits_IsMemoryStopped(&req->base.limits)) {
    if (req->base.reqConfig.oomPolicy != OomPolicy_Fail) {
      req->queryOOM = true;
    } else if (QueryError_IsOk(&req->base.reply.err)) {
      QueryError_SetCode(&req->base.reply.err, QUERY_ERROR_CODE_OUT_OF_MEMORY);
      if (rCtx) {
        rCtx->oomErrorMapped = true;
      }
    }
  }
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
  if (!QueryRequestLimits_IsBlockedClientTimedOut(&req->base.limits)) {
    searchReqCtx_SetExecutionStage(req, QUERY_TIMEOUT_STAGE_REPLY);
  }

  if (bc && !fromTimeout && !QueryRequestLimits_IsBlockedClientTimedOut(&req->base.limits)) {
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

    if (MRCtx_GetNumReplied(mrctx) == 0 && !(req->rctx && searchStoppedForOom(req))) {
      // Can happen in a topology error, before or after we sent the command to the cluster
      RedisModule_ReplyWithError(ctx, "Could not send query to cluster");
      return REDISMODULE_OK;
    }

    searchReducerCtx *rCtx = req->rctx;
    // If NumReplied > 0 we expect ReducerCtx to be initialized
    RS_ASSERT(rCtx)

    // Pressure can begin after the reducer's last read and before this callback runs. ON_OOM FAIL
    // cannot fail once the first reply byte is out, so this is its last chance.
    if (req->base.reqConfig.oomPolicy == OomPolicy_Fail &&
        QueryMemory_CheckBeforeExecution(&req->base.limits, OomPolicy_Fail, &request->reply.err) ==
            REDISMODULE_ERR) {
      QueryErrorsGlobalStats_UpdateError(QUERY_ERROR_CODE_OUT_OF_MEMORY, 1, COORD_ERR_WARN);
      QueryError_ReplyAndClear(ctx, &request->reply.err);
      return REDISMODULE_OK;
    }

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
    QueryRequestLimits_MarkTimedOut(&req->base.limits);
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
  QueryRequestLimits_MarkTimedOut(&req->base.limits);

  recordSearchTimeoutStage(req, /*isError=*/false);

  // Try to claim reducing - if we get it, run reducer on main thread
  // If we don't get it, reducer is already running (or bailout claimed it) - wait for it
  if (QueryRequest_TryClaimResults(&req->base)) {
    // We claimed reducing - run reducer on main thread with current replies
    searchResultReducer(req, MRCtx_GetNumReplied(mrctx), MRCtx_GetReplies(mrctx), true);
  } else {
    // Reducer already running or bailout claimed it - wait for completion
    QueryRequest_WaitForResultsComplete(&req->base);

    // The reducer finished before this timeout and mapped its own OOM stop into an
    // error; this callback owns the reply, so the timeout reply wins as when it claims first.
    if (req->rctx && req->rctx->oomErrorMapped) {
      QueryError_ClearError(&request->reply.err);
      req->rctx->oomErrorMapped = false;
    }

    // A background reducer may have claimed reducing, observed the timeout, and exited before
    // initializing req->rctx or its heap. In that case adopt the timeout-owned reduction path
    // now that the competing reducer has finished.
    if (!QueryError_HasError(&request->reply.err) && (req->rctx == NULL || req->rctx->pq == NULL)) {
      searchReducerCtx_Free(req->rctx);
      req->rctx = NULL;
      searchResultReducer(req, MRCtx_GetNumReplied(mrctx), MRCtx_GetReplies(mrctx), true);
    }
  }

  // This callback owns the reply, so an OOM stop the background reducer mapped to the OOM error
  // under ON_OOM FAIL yields to the timeout. Errors, a shard's included, stay. Under ON_OOM RETURN
  // the stop keeps its warning next to the timeout's. A timer-owned reduction never sets
  // queryOOM, so read the stop itself too.
  req->queryOOM = (req->queryOOM || searchStoppedForOom(req)) &&
                  req->base.reqConfig.oomPolicy != OomPolicy_Fail;

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
