/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */


#define REDISMODULE_MAIN
#include "redismodule.h"

#include "rmr/rmr.h"
#include "hiredis/async.h"
#include "rmr/reply.h"
#include "rmutil/util.h"
#include "rmutil/strings.h"
#include "rmr/redis_cluster.h"
#include "rmr/redise.h"
#include "config.h"
#include "coord_module.h"
#include "info_command.h"
#include "version.h"
#include "cursor.h"
#include "build-info/info.h"
#include "aggregate/aggregate.h"
#include "value.h"
#include "cluster_spell_check.h"
#include "profile.h"
#include "resp3.h"
#include "dist_profile.h"
#include "debug_commands.h"

#include "libuv/include/uv.h"

#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <sys/param.h>
#include <pthread.h>
#include <stdbool.h>
#include "query.h"

#define CLUSTERDOWN_ERR "ERRCLUSTER Uninitialized cluster state, could not perform command"

extern RSConfig RSGlobalConfig;

extern RedisModuleCtx *RSDummyContext;

static int DIST_AGG_THREADPOOL = -1;

// Number of shards in the cluster. Hint we can read and modify from the main thread
size_t NumShards = 0;

static inline bool SearchCluster_Ready() {
  return NumShards != 0;
}

// A reducer that just merges N sets of strings by chaining them into one big array with no
// duplicates

int uniqueStringsReducer(struct MRCtx *mc, int count, MRReply **replies) {
  RedisModuleCtx *ctx = MRCtx_GetRedisCtx(mc);
  RedisModule_Reply _reply = RedisModule_NewReply(ctx), *reply = &_reply;

  MRReply *err = NULL;

  TrieMap *dict = NewTrieMap();
  int nArrs = 0;
  // Add all the set elements into the dedup dict
  for (int i = 0; i < count; i++) {
    if (replies[i] && (MRReply_Type(replies[i]) == MR_REPLY_ARRAY
    || MRReply_Type(replies[i]) == MR_REPLY_SET)) {
      nArrs++;
      for (size_t j = 0; j < MRReply_Length(replies[i]); j++) {
        size_t sl = 0;
        const char *s = MRReply_String(MRReply_ArrayElement(replies[i], j), &sl);
        if (s && sl) {
          TrieMap_Add(dict, s, sl, NULL, NULL);
        }
      }
    } else if (MRReply_Type(replies[i]) == MR_REPLY_ERROR && err == NULL) {
      err = replies[i];
    }
  }

  // if there are no values - either reply with an empty set or an error
  if (dict->cardinality == 0) {

    if (nArrs > 0) {
      // the sets were empty - return an empty set
      RedisModule_Reply_Set(reply);
      RedisModule_Reply_SetEnd(reply);
    } else {
      RedisModule_ReplyWithError(ctx, err ? (const char *)err : "Could not perform query");
    }
    goto cleanup;
  }

  // Iterate the dict and reply with all values
  RedisModule_Reply_Set(reply);
    char *s;
    tm_len_t sl;
    void *p;
    TrieMapIterator *it = TrieMap_Iterate(dict, "", 0);
    while (TrieMapIterator_Next(it, &s, &sl, &p)) {
      RedisModule_Reply_StringBuffer(reply, s, sl);
    }
    TrieMapIterator_Free(it);
  RedisModule_Reply_SetEnd(reply);

cleanup:
  TrieMap_Free(dict, NULL);
  RedisModule_EndReply(reply);

  return REDISMODULE_OK;
}

// A reducer that just merges N arrays of the same length, selecting the first non NULL reply from
// each

int mergeArraysReducer(struct MRCtx *mc, int count, MRReply **replies) {
  RedisModuleCtx *ctx = MRCtx_GetRedisCtx(mc);
  RedisModule_Reply _reply = RedisModule_NewReply(ctx), *reply = &_reply;

  for (size_t i = 0; i < count; ++i) {
    if (MRReply_Type(replies[i]) == MR_REPLY_ERROR) {
      // we got an error reply, something goes wrong so we return the error to the user.
      int rc = MR_ReplyWithMRReply(reply, replies[i]);
      RedisModule_EndReply(reply);
      return rc;
    }
  }

  int j = 0;
  int stillValid;
  do {
    // the number of still valid arrays in the response
    stillValid = 0;

    for (int i = 0; i < count; i++) {
      // if this is not an array - ignore it
      if (MRReply_Type(replies[i]) != MR_REPLY_ARRAY) continue;
      // if we've overshot the array length - ignore this one
      if (MRReply_Length(replies[i]) <= j) continue;
      // increase the number of valid replies
      stillValid++;

      // get the j element of array i
      MRReply *ele = MRReply_ArrayElement(replies[i], j);
      // if it's a valid response OR this is the last array we are scanning -
      // add this element to the merged array
      if (MRReply_Type(ele) != MR_REPLY_NIL || i + 1 == count) {
        // if this is the first reply - we need to crack open a new array reply
        if (j == 0) {
          RedisModule_Reply_Array(reply);
        }

        MR_ReplyWithMRReply(reply, ele);
        j++;
        break;
      }
    }
  } while (stillValid > 0);

  // j 0 means we could not process a single reply element from any reply
  if (j == 0) {
    int rc = RedisModule_Reply_Error(reply, "Could not process replies");
    RedisModule_EndReply(reply);
    return rc;
  }
  RedisModule_Reply_ArrayEnd(reply);

  RedisModule_EndReply(reply);
  return REDISMODULE_OK;
}

int singleReplyReducer(struct MRCtx *mc, int count, MRReply **replies) {
  RedisModuleCtx *ctx = MRCtx_GetRedisCtx(mc);
  RedisModule_Reply _reply = RedisModule_NewReply(ctx), *reply = &_reply;

  if (count == 0) {
    RedisModule_Reply_Null(reply);
  } else {
    MR_ReplyWithMRReply(reply, replies[0]);
  }

  RedisModule_EndReply(reply);
  return REDISMODULE_OK;
}

// a reducer that expects "OK" reply for all replies, and stops at the first error and returns it
int allOKReducer(struct MRCtx *mc, int count, MRReply **replies) {
  RedisModuleCtx *ctx = MRCtx_GetRedisCtx(mc);
  RedisModule_Reply _reply = RedisModule_NewReply(ctx), *reply = &_reply;

  if (count == 0) {
    RedisModule_Reply_Error(reply, "Could not distribute command");
    goto end;
  }

  bool isIntegerReply = false, isDoubleReply = false;
  long long integerReply = 0;
  double doubleReply = 0;
  for (int i = 0; i < count; i++) {
    if (MRReply_Type(replies[i]) == MR_REPLY_ERROR) {
      MR_ReplyWithMRReply(reply, replies[i]);
      goto end;
    }
    if (MRReply_Type(replies[i]) == MR_REPLY_INTEGER) {
      long long n = MRReply_Integer(replies[i]);
      if (!isIntegerReply) {
        integerReply = n;
        isIntegerReply = true;
      } else if (n != integerReply) {
        RedisModule_Reply_SimpleString(reply, "not all results are the same");
        goto end;
      }
    } else if (MRReply_Type(replies[i]) == MR_REPLY_DOUBLE) {
      double n = MRReply_Double(replies[i]);
      if (!isDoubleReply) {
        doubleReply = n;
        isDoubleReply = true;
      } else if (n != doubleReply) {
        RedisModule_Reply_SimpleString(reply, "not all results are the same");
        goto end;
      }
    }
  }

  if (isIntegerReply) {
    RedisModule_Reply_LongLong(reply, integerReply);
  } else if (isDoubleReply) {
    RedisModule_Reply_Double(reply, doubleReply);
  } else {
    RedisModule_Reply_SimpleString(reply, "OK");
  }

end:
  RedisModule_EndReply(reply);
  return REDISMODULE_OK;
}

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

struct searchReducerCtx; // Predecleration
typedef void (*processReplyCB)(MRReply *arr, struct searchReducerCtx *rCtx, RedisModuleCtx *ctx);
typedef void (*postProcessReplyCB)( struct searchReducerCtx *rCtx);

typedef struct {
  int step;  // offset for next reply
  int score;
  int firstField;
  int payload;
  int sortKey;
} searchReplyOffsets;

typedef struct{
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
  specialCaseCtx* reduceSpecialCaseCtxKnn;
  specialCaseCtx* reduceSpecialCaseCtxSortby;

  MRReply *warning;
} searchReducerCtx;

typedef struct {
  searchResult* result;
  double score;
} scoredSearchResultWrapper;

specialCaseCtx* SpecialCaseCtx_New() {
  specialCaseCtx* ctx = rm_calloc(1, sizeof(specialCaseCtx));
  return ctx;
}

void SpecialCaseCtx_Free(specialCaseCtx* ctx) {
  if (!ctx) return;
  if(ctx->specialCaseType == SPECIAL_CASE_KNN) {
    QueryNode_Free(ctx->knn.queryNode);
  } else if(ctx->specialCaseType == SPECIAL_CASE_SORTBY) {
    rm_free((void*)ctx->sortby.sortKey);
  }
  rm_free(ctx);
}

static searchRequestCtx* searchRequestCtx_New(void) {
  return rm_calloc(1, sizeof(searchRequestCtx));
}

static void searchRequestCtx_Free(searchRequestCtx *r) {
  if(r->queryString) {
    rm_free(r->queryString);
  }
  if(r->specialCases) {
    size_t specialCasesLen = array_len(r->specialCases);
    for(size_t i = 0; i< specialCasesLen; i ++) {
      specialCaseCtx* ctx = r->specialCases[i];
      SpecialCaseCtx_Free(ctx);
    }
    array_free(r->specialCases);
  }
  if(r->requiredFields) {
    array_free(r->requiredFields);
  }
  rm_free(r);
}

static int searchResultReducer(struct MRCtx *mc, int count, MRReply **replies);

static int rscParseProfile(searchRequestCtx *req, RedisModuleString **argv) {
  req->profileArgs = 0;
  if (RMUtil_ArgIndex("FT.PROFILE", argv, 1) != -1) {
    req->profileArgs += 2;
    req->profileClock = clock();
    if (RMUtil_ArgIndex("LIMITED", argv + 3, 1) != -1) {
      req->profileLimited = 1;
      req->profileArgs++;
    }
    if (RMUtil_ArgIndex("QUERY", argv + 3, 2) == -1) {
      return REDISMODULE_ERR;
    }
  }
  return REDISMODULE_OK;
}


void setKNNSpecialCase(searchRequestCtx *req, specialCaseCtx *knn_ctx) {
  if(!req->specialCases) {
    req->specialCases = array_new(specialCaseCtx*, 1);
  }
  array_append(req->specialCases, knn_ctx);
  // Default: No SORTBY is given, or SORTBY is given by other field
  // When first sorting by different field, the topk vectors should be passed to the coordinator heap
  knn_ctx->knn.shouldSort = true;
  // We need to get K results from the shards
  // For example the command request SORTBY text_field LIMIT 2 3
  // In this case the top 5 results relevant for this sort might be the in the last 5 results of the TOPK
  long long requestedResultsCount = req->requestedResultsCount;
  req->requestedResultsCount = MAX(knn_ctx->knn.k, requestedResultsCount);
  if(array_len(req->specialCases) > 1) {
    specialCaseCtx* optionalSortCtx = req->specialCases[0];
    if(optionalSortCtx->specialCaseType == SPECIAL_CASE_SORTBY) {
      if(strcmp(optionalSortCtx->sortby.sortKey, knn_ctx->knn.fieldName) == 0){
        // If SORTBY is done by the vector score field, the coordinator will do it and no special operation is needed.
        knn_ctx->knn.shouldSort = false;
        // The requested results should be at most K
        req->requestedResultsCount = MIN(knn_ctx->knn.k, requestedResultsCount);
      }
    }
  }
}


// Prepare a TOPK special case, return a context with the required KNN fields if query is
// valid and contains KNN section, NULL otherwise (and set proper error in *status* if error
// was found).
specialCaseCtx *prepareOptionalTopKCase(const char *query_string, RedisModuleString **argv, int argc,
                                        QueryError *status) {

  // First, parse the query params if exists, to set the params in the query parser ctx.
  dict *params = NULL;
  QueryNode* queryNode = NULL;
  int paramsOffset = RMUtil_ArgExists("PARAMS", argv, argc, 1);
  if (paramsOffset > 0) {
    ArgsCursor ac;
    ArgsCursor_InitRString(&ac, argv+paramsOffset+1, argc-(paramsOffset+1));
    if (parseParams(&params, &ac, status) != REDISMODULE_OK) {
        return NULL;
    }
  }
  RedisSearchCtx sctx = {0};
  RSSearchOptions opts = {0};
  opts.params = params;
  QueryParseCtx qpCtx = {
      .raw = query_string,
      .len = strlen(query_string),
      .sctx = &sctx,
      .opts = &opts,
      .status = status,
#ifdef PARSER_DEBUG
      .trace_log = NULL
#endif
  };

  // KNN queries are parsed only on dialect versions >=2
  queryNode = RSQuery_ParseRaw_v2(&qpCtx);
  if (QueryError_GetCode(status) != QUERY_OK || queryNode == NULL) {
    // Query parsing failed.
    goto cleanup;
  }
  if (QueryNode_NumParams(queryNode) > 0 && paramsOffset == 0) {
    // Query expects params, but no params were given.
    goto cleanup;
  }
  if (QueryNode_NumParams(queryNode) > 0) {
      int ret = QueryNode_EvalParamsCommon(params, queryNode, status);
      if (ret != REDISMODULE_OK || QueryError_GetCode(status) != QUERY_OK) {
        // Params evaluation failed.
        goto cleanup;
      }
      Param_DictFree(params);
  }

  if (queryNode->type == QN_VECTOR) {
    QueryVectorNode queryVectorNode = queryNode->vn;
    size_t k = queryVectorNode.vq->knn.k;
    specialCaseCtx *ctx = SpecialCaseCtx_New();
    ctx->knn.k = k;
    ctx->knn.fieldName = queryNode->opts.distField ? queryNode->opts.distField : queryVectorNode.vq->scoreField;
    ctx->knn.pq = NULL;
    ctx->knn.queryNode = queryNode;  // take ownership
    ctx->specialCaseType = SPECIAL_CASE_KNN;
    return ctx;
  }

cleanup:
  if (params) {
    Param_DictFree(params);
  }
  if (queryNode) {
    QueryNode_Free(queryNode);
  }
  return NULL;
}

// Prepare a sortby special case.
void prepareSortbyCase(searchRequestCtx *req, RedisModuleString **argv, int argc, int sortByIndex) {
  const char* sortkey = RedisModule_StringPtrLen(argv[sortByIndex + 1], NULL);
  specialCaseCtx *ctx = SpecialCaseCtx_New();
  ctx->specialCaseType = SPECIAL_CASE_SORTBY;
  ctx->sortby.sortKey = rm_strdup(sortkey);
  ctx->sortby.asc = true;
  req->sortAscending = true;
  if (req->withSortby && sortByIndex + 2 < argc) {
    if (RMUtil_StringEqualsCaseC(argv[sortByIndex + 2], "DESC")) {
      ctx->sortby.asc = false;
      req->sortAscending = false;
    }
  }
  if(!req->specialCases) {
      req->specialCases = array_new(specialCaseCtx*, 1);
    }
  array_append(req->specialCases, ctx);
}

searchRequestCtx *rscParseRequest(RedisModuleString **argv, int argc, QueryError* status) {

  searchRequestCtx *req = searchRequestCtx_New();

  req->initClock = clock();

  if (rscParseProfile(req, argv) != REDISMODULE_OK) {
    searchRequestCtx_Free(req);
    return NULL;
  }

  int argvOffset = 2 + req->profileArgs;
  req->queryString = rm_strdup(RedisModule_StringPtrLen(argv[argvOffset++], NULL));
  req->limit = 10;
  req->offset = 0;
  // marks the user set WITHSCORES. internally it's always set
  req->withScores = RMUtil_ArgExists("WITHSCORES", argv, argc, argvOffset) != 0;
  req->withExplainScores = RMUtil_ArgExists("EXPLAINSCORE", argv, argc, argvOffset) != 0;
  req->specialCases = NULL;
  req->requiredFields = NULL;

  req->withSortingKeys = RMUtil_ArgExists("WITHSORTKEYS", argv, argc, argvOffset) != 0;
  // fprintf(stderr, "Sortby: %d, asc: %d withsort: %d\n", req->withSortby, req->sortAscending,
  //         req->withSortingKeys);

  // Detect "NOCONTENT"
  req->noContent = RMUtil_ArgExists("NOCONTENT", argv, argc, argvOffset) != 0;

  // if RETURN exists - make sure we don't have RETURN 0
  if (!req->noContent && RMUtil_ArgExists("RETURN", argv, argc, argvOffset)) {
    long long numReturns = -1;
    RMUtil_ParseArgsAfter("RETURN", argv, argc, "l", &numReturns);
    // RETURN 0 equals NOCONTENT
    if (numReturns <= 0) {
      req->noContent = 1;
    }
  }

  req->withPayload = RMUtil_ArgExists("WITHPAYLOADS", argv, argc, argvOffset) != 0;

  // Parse LIMIT argument
  RMUtil_ParseArgsAfter("LIMIT", argv + argvOffset, argc - argvOffset, "ll", &req->offset, &req->limit);
  if (req->limit < 0 || req->offset < 0) {
    searchRequestCtx_Free(req);
    return NULL;
  }
  req->requestedResultsCount = req->limit + req->offset;

  // Handle special cases
  // Parse SORTBY ... ASC.
  // Parse it ALWAYS first so the sortkey will be send first
  int sortByIndex = RMUtil_ArgIndex("SORTBY", argv, argc);
  if (sortByIndex > 2) {
    req->withSortby = true;
    // Check for command error where no sortkey is given.
    if(sortByIndex + 1 >= argc) {
      searchRequestCtx_Free(req);
      return NULL;
    }
    prepareSortbyCase(req, argv, argc, sortByIndex);
  } else {
    req->withSortby = false;
  }

  unsigned int dialect = RSGlobalConfig.requestConfigParams.dialectVersion;
  int argIndex = RMUtil_ArgExists("DIALECT", argv, argc, argvOffset);
  if(argIndex > 0) {
      argIndex++;
      ArgsCursor ac;
      ArgsCursor_InitRString(&ac, argv+argIndex, argc-argIndex);
      if (parseDialect(&dialect, &ac, status) != REDISMODULE_OK) {
        searchRequestCtx_Free(req);
        return NULL;
      }
  }

  if(dialect >= 2) {
    // Note: currently there is only one single case. For extending those cases we should use a trie here.
    if(strcasestr(req->queryString, "KNN")) {
      specialCaseCtx *knnCtx = prepareOptionalTopKCase(req->queryString, argv, argc, status);
      if (QueryError_HasError(status)) {
        searchRequestCtx_Free(req);
        return NULL;
      }
      if (knnCtx != NULL) {
        setKNNSpecialCase(req, knnCtx);
      }
    }
  }

  req->format = QEXEC_FORMAT_DEFAULT;
  argIndex = RMUtil_ArgExists("FORMAT", argv, argc, argvOffset);
  if(argIndex > 0) {
    argIndex++;
    ArgsCursor ac;
    ArgsCursor_InitRString(&ac, argv+argIndex, argc-argIndex);
    if (parseValueFormat(&req->format, &ac, status) != REDISMODULE_OK) {
      searchRequestCtx_Free(req);
      return NULL;
    }
  }

  // Get timeout parameter, if set in the command
  argIndex = RMUtil_ArgIndex("TIMEOUT", argv, argc);
  if (argIndex > -1) {
    argIndex++;
    ArgsCursor ac;
    ArgsCursor_InitRString(&ac, argv+argIndex, argc-argIndex);
    if (parseTimeout(&req->timeout, &ac, status)) {
      searchRequestCtx_Free(req);
      return NULL;
    }
  } else {
    req->timeout = RSGlobalConfig.requestConfigParams.queryTimeoutMS;
  }

  return req;
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
        // printf("Using sortKey!! <N=%lu> %.*s vs <N=%lu> %.*s. Result=%d\n", r2->sortKeyLen,
        //        (int)r2->sortKeyLen, r2->sortKey, r1->sortKeyLen, (int)r1->sortKeyLen, r1->sortKey,
        //        cmp);
      } else {
        // If at least one of these has no sort key, it gets high value regardless of asc/desc
        return r2->sortKey ? 1 : -1;
      }
    }
    // in case of a tie or missing both sorting keys - compare ids
    if (!cmp) {
      // printf("It's a tie! Comparing <N=%lu> %.*s vs <N=%lu> %.*s\n", r2->idLen, (int)r2->idLen,
      //        r2->id, r1->idLen, (int)r1->idLen, r1->id);
      cmp = cmpStrings(r2->id, r2->idLen, r1->id, r1->idLen);
    }
    return (req->sortAscending ? -cmp : cmp);
  }

  double s1 = r1->score, s2 = r2->score;
  // printf("Scores: %lf vs %lf. WithSortBy: %d. SK1=%p. SK2=%p\n", s1, s2, req->withSortby,
  //        r1->sortKey, r2->sortKey);
  if (s1 < s2) {
    return 1;
  } else if (s1 > s2) {
    return -1;
  } else {
    // printf("Scores are tied. Will compare ID Strings instead\n");

    // This was reversed to be more compatible with OSS version where tie breaker was changed
    // to return the lower doc ID to reduce sorting heap work. Doc name might not be ascending
    // or decending but this still may reduce heap work.
    // Our tests are usually ascending so this will create similarity between RS and RSC.
    int rv = -cmpStrings(r2->id, r2->idLen, r1->id, r1->idLen);

    // printf("ID Strings: Comparing <N=%lu> %.*s vs <N=%lu> %.*s => %d\n", r2->idLen,
    // (int)r2->idLen,
    //        r2->id, r1->idLen, (int)r1->idLen, r1->id, rv);
    return rv;
  }
}

searchResult *newResult_resp2(searchResult *cached, MRReply *arr, int j, searchReplyOffsets* offsets, int explainScores) {
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
  res->id = (char*)MRReply_String(MRReply_ArrayElement(arr, j), &res->idLen);
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
      res->sortKeyNum = strtod(res->sortKey + 1, &endptr);
      RedisModule_Assert(endptr == res->sortKey + res->sortKeyLen);
    }
    // fprintf(stderr, "Sort key string '%s', num '%f\n", res->sortKey, res->sortKeyNum);
  }
  return res;
}

searchResult *newResult_resp3(searchResult *cached, MRReply *results, int j, searchReplyOffsets* offsets, bool explainScores, specialCaseCtx *reduceSpecialCaseCtxSortBy) {
  searchResult *res = cached ? cached : rm_malloc(sizeof *res);
  res->sortKey = NULL;
  res->sortKeyNum = HUGE_VAL;

  MRReply *result_j = MRReply_ArrayElement(results, j);
  if (MRReply_Type(result_j) != MR_REPLY_MAP) {
    res->id = NULL;
    return res;
  }

  MRReply *result_id = MRReply_MapElement(result_j, "id");
  res->id = (char*)MRReply_String(result_id, &res->idLen);
  if (!res->id) {
    return res;
  }

  // parse socre
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
          res->sortKeyNum = strtod(res->sortKey + 1, &endptr);
          RedisModule_Assert(endptr == res->sortKey + res->sortKeyLen);
        }
        // fprintf(stderr, "Sort key string '%s', num '%f\n", res->sortKey, res->sortKeyNum);
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
   * ...              | special cases - SORTBY, TOPK. Sort key is always first for backwards comptability.
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
    for(size_t i = 0; i < nSpecialCases; i++) {
      switch (ctx->specialCases[i]->specialCaseType)
      {
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

  if(specialCasesMaxOffset > 0) {
    offsets->firstField=specialCasesMaxOffset+1;
    offsets->step=offsets->firstField+1;
  }
  else if(ctx->withSortingKeys) {
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
  const scoredSearchResultWrapper* s1= p1;
  const scoredSearchResultWrapper* s2 = p2;
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
    RedisModule_Assert(str[0] == '#');
    char *eptr;
    double d = strtod(str + 1, &eptr);
    RedisModule_Assert(eptr != sortKey + 1 && *eptr == 0);
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

static void ProcessKNNSearchResult(searchResult *res, searchReducerCtx *rCtx, double score, knnContext *knnCtx) {
  // As long as we don't have k results, keep insert
    if (heap_count(knnCtx->pq) < knnCtx->k) {
      scoredSearchResultWrapper* resWrapper = rm_malloc(sizeof(scoredSearchResultWrapper));
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
        scoredSearchResultWrapper* resWrapper = rm_malloc(sizeof(scoredSearchResultWrapper));
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

static void ProcessKNNSearchReply(MRReply *arr, searchReducerCtx *rCtx, RedisModuleCtx *ctx) {
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
  specialCaseCtx* reduceSpecialCaseCtxKnn = rCtx->reduceSpecialCaseCtxKnn;
  specialCaseCtx* reduceSpecialCaseCtxSortBy = rCtx->reduceSpecialCaseCtxSortby;
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
      res = newResult_resp3(rCtx->cachedResult, results, j, &rCtx->offsets, rCtx->searchCtx->withExplainScores, reduceSpecialCaseCtxSortBy);
      if (res && res->id) {
        rCtx->cachedResult = NULL;
      } else {
        RedisModule_Log(ctx, "warning", "missing required_field when parsing redisearch results");
        goto error;
      }
      MRReply *require_fields = MRReply_MapElement(MRReply_ArrayElement(results, j), "required_fields");
      if (!require_fields) {
        RedisModule_Log(ctx, "warning", "missing required_fields when parsing redisearch results");
        goto error;
      }
      MRReply *score_value = MRReply_MapElement(require_fields, reduceSpecialCaseCtxKnn->knn.fieldName);
      if (!score_value) {
        RedisModule_Log(ctx, "warning", "missing knn required_field when parsing redisearch results");
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
            ctx, "warning",
            "got a bad reply from redisearch, reply contains less parameters then expected");
        rCtx->errorOccurred = true;
        break;
      }
      res = newResult_resp2(rCtx->cachedResult, arr, j, &rCtx->offsets, rCtx->searchCtx->withExplainScores);
      if (res && res->id) {
        rCtx->cachedResult = NULL;
      } else {
        RedisModule_Log(ctx, "warning", "missing required_field when parsing redisearch results");
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

static void processSearchReplyResult(searchResult *res, searchReducerCtx *rCtx, RedisModuleCtx *ctx) {
  if (!res || !res->id) {
    RedisModule_Log(ctx, "warning", "got an unexpected argument when parsing redisearch results");
    rCtx->errorOccurred = true;
    // invalid result - usually means something is off with the response, and we should just
    // quit this response
    rCtx->cachedResult = res;
    return;
  }

  rCtx->cachedResult = NULL;

  // fprintf(stderr, "Result %d Reply docId %s score: %f sortkey %f\n", i, res->id, res->score, res->sortKeyNum);

  // TODO: minmax_heap?
  if (heap_count(rCtx->pq) < heap_size(rCtx->pq)) {
    // printf("Offering result score %f\n", res->score);
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
        return;
      }
    }
  }
}

static void processSearchReply(MRReply *arr, searchReducerCtx *rCtx, RedisModuleCtx *ctx) {
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

  if (resp3) // RESP3
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
      searchResult *res = newResult_resp3(rCtx->cachedResult, results, i, &rCtx->offsets, rCtx->searchCtx->withExplainScores, rCtx->reduceSpecialCaseCtxSortby);
      processSearchReplyResult(res, rCtx, ctx);
    }
    processResultFormat(&rCtx->searchCtx->format, arr);
  }
  else // RESP2
  {
    size_t len = MRReply_Length(arr);

    // first element is the total count
    rCtx->totalReplies += MRReply_Integer(MRReply_ArrayElement(arr, 0));

    int step = rCtx->offsets.step;
    // fprintf(stderr, "Step %d, scoreOffset %d, fieldsOffset %d, sortKeyOffset %d\n", step,
    //         scoreOffset, fieldsOffset, sortKeyOffset);

    for (int j = 1; j < len; j += step) {
      if (j + step > len) {
        RedisModule_Log(ctx, "warning",
          "got a bad reply from redisearch, reply contains less parameters then expected");
        rCtx->errorOccurred = true;
        break;
      }
      searchResult *res = newResult_resp2(rCtx->cachedResult, arr, j, &rCtx->offsets , rCtx->searchCtx->withExplainScores);
      processSearchReplyResult(res, rCtx, ctx);
    }
  }
}

/************************ Result post processing callbacks ********************/


static void noOpPostProcess(searchReducerCtx *rCtx){
  return;
}

static void knnPostProcess(searchReducerCtx *rCtx) {
  specialCaseCtx* reducerSpecialCaseCtx = rCtx->reduceSpecialCaseCtxKnn;
  RedisModule_Assert(reducerSpecialCaseCtx->specialCaseType == SPECIAL_CASE_KNN);
  if(reducerSpecialCaseCtx->knn.pq) {
    size_t numberOfResults = heap_count(reducerSpecialCaseCtx->knn.pq);
    for (size_t i = 0; i < numberOfResults; i++) {
      scoredSearchResultWrapper* wrappedResult = heap_poll(reducerSpecialCaseCtx->knn.pq);
      searchResult* res = wrappedResult->result;
      rm_free(wrappedResult);
      if(heap_count(rCtx->pq) < heap_size(rCtx->pq)) {
        heap_offerx(rCtx->pq, res);
      }
      else {
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
  }
  // We can always get at most K results
  rCtx->totalReplies = heap_count(rCtx->pq);

}

static void sendSearchResults(RedisModule_Reply *reply, searchReducerCtx *rCtx) {
  // Reverse the top N results

  rCtx->postProcess((struct searchReducerCtx *)rCtx);

  searchRequestCtx *req = rCtx->searchCtx;

  // Number of results to actually return
  size_t num = req->requestedResultsCount;

  size_t qlen = heap_count(rCtx->pq);
  size_t pos = qlen;

  // Load the results from the heap into a sorted array. Free the items in
  // the heap one-by-one so that we don't have to go through them again
  searchResult **results = rm_malloc(sizeof(*results) * qlen);
  while (pos) {
    results[--pos] = heap_poll(rCtx->pq);
  }
  heap_free(rCtx->pq);
  rCtx->pq = NULL;

  //-------------------------------------------------------------------------------------------
  RedisModule_Reply_Map(reply);
  if (reply->resp3) // RESP3
  {
    RedisModule_Reply_SimpleString(reply, "attributes");
    if (rCtx->fieldNames) {
      MR_ReplyWithMRReply(reply, rCtx->fieldNames);
    } else {
      RedisModule_Reply_EmptyArray(reply);
    }

    RedisModule_Reply_SimpleString(reply, "warning"); // >warning
    if (rCtx->warning) {
      MR_ReplyWithMRReply(reply, rCtx->warning);
    } else {
      RedisModule_Reply_EmptyArray(reply);
    }

    RedisModule_ReplyKV_LongLong(reply, "total_results", rCtx->totalReplies);

    if (rCtx->searchCtx->format & QEXEC_FORMAT_EXPAND) {
      RedisModule_ReplyKV_SimpleString(reply, "format", "EXPAND"); // >format
    } else {
      RedisModule_ReplyKV_SimpleString(reply, "format", "STRING"); // >format
    }

    RedisModule_ReplyKV_Array(reply, "results"); // >results

    for (int i = 0; i < qlen && i < num; ++i) {
      RedisModule_Reply_Map(reply); // >> result
        searchResult *res = results[i];

        RedisModule_ReplyKV_StringBuffer(reply, "id", res->id, res->idLen);

        if (req->withScores) {
          RedisModule_Reply_SimpleString(reply, "score");

          if (req->withExplainScores) {
            RedisModule_Reply_Array(reply);
              RedisModule_Reply_Double(reply, res->score);
              MR_ReplyWithMRReply(reply, res->explainScores);
            RedisModule_Reply_ArrayEnd(reply);
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
          RedisModule_ReplyKV_MRReply(reply, "extra_attributes", res->fields); // >> extra_attributes
        }

        RedisModule_Reply_SimpleString(reply, "values");
        RedisModule_Reply_EmptyArray(reply);
      RedisModule_Reply_MapEnd(reply); // >>result
    }

    RedisModule_Reply_ArrayEnd(reply); // >results
  }
  //-------------------------------------------------------------------------------------------
  else // RESP2
  {
    RedisModule_Reply_LongLong(reply, rCtx->totalReplies);

    for (pos = rCtx->searchCtx->offset; pos < qlen && pos < num; pos++) {
      searchResult *res = results[pos];
      RedisModule_Reply_StringBuffer(reply, res->id, res->idLen);
      if (req->withScores) {
        if (req->withExplainScores) {
          RedisModule_Reply_Array(reply);
            RedisModule_Reply_Double(reply, res->score);
            MR_ReplyWithMRReply(reply, res->explainScores);
          RedisModule_Reply_ArrayEnd(reply);
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
  RedisModule_Reply_MapEnd(reply);
  //-------------------------------------------------------------------------------------------

  // Free the sorted results
  for (pos = 0; pos < qlen; pos++) {
    rm_free(results[pos]);
  }
  rm_free(results);
}

/**
 * This function is used to print profiles received from the shards.
 * It is used by both SEARCH and AGGREGATE.
 */
static void PrintShardProfile_resp2(RedisModule_Reply *reply, int count, MRReply **replies, bool isSearch) {
  // The 1st location always stores the results. On FT.AGGREGATE, the next place stores the
  // cursor ID. The last location (2nd for FT.SEARCH and 3rd for FT.AGGREGATE) stores the
  // profile information of the shard.
  const int profile_data_idx = isSearch ? 1 : 2;
  for (int i = 0; i < count; ++i) {
    MRReply *shards_reply = MRReply_ArrayElement(replies[i], profile_data_idx);
    MRReply *shards_array_profile = MRReply_ArrayElement(shards_reply, 1);
    MRReply *shard_profile = MRReply_ArrayElement(shards_array_profile, 0);
    MR_ReplyWithMRReply(reply, shard_profile);
  }
}

static void PrintShardProfile_resp3(RedisModule_Reply *reply, int count, MRReply **replies, bool isSearch) {
  for (int i = 0; i < count; ++i) {
    MRReply *profile;
    if (!isSearch) {
      // On aggregate commands, take the results from the response (second component is the cursor-id)
      MRReply *results = MRReply_ArrayElement(replies[i], 0);
      profile = MRReply_MapElement(results, PROFILE_STR);
    } else {
      profile = MRReply_MapElement(replies[i], PROFILE_STR);
    }
    MRReply *shards = MRReply_MapElement(profile, PROFILE_SHARDS_STR);
    MRReply *shard = MRReply_ArrayElement(shards, 0);

    MR_ReplyWithMRReply(reply, shard);
  }
}

void PrintShardProfile(RedisModule_Reply *reply, void *ctx) {
  PrintShardProfile_ctx *pCtx = ctx;
  if (reply->resp3) {
    PrintShardProfile_resp3(reply, pCtx->count, pCtx->replies, pCtx->isSearch);
  } else {
    PrintShardProfile_resp2(reply, pCtx->count, pCtx->replies, pCtx->isSearch);
  }
}

struct PrintCoordProfile_ctx {
  clock_t totalTime;
  clock_t postProcessTime;
};
static void profileSearchReplyCoordinator(RedisModule_Reply *reply, void *ctx) {
  struct PrintCoordProfile_ctx *pCtx = ctx;
  RedisModule_Reply_Map(reply);
  RedisModule_ReplyKV_Double(reply, "Total Coordinator time", (double)(clock() - pCtx->totalTime) / CLOCKS_PER_MILLISEC);
  RedisModule_ReplyKV_Double(reply, "Post Processing time", (double)(clock() - pCtx->postProcessTime) / CLOCKS_PER_MILLISEC);
  RedisModule_Reply_MapEnd(reply);
}

static void profileSearchReply(RedisModule_Reply *reply, searchReducerCtx *rCtx,
                               int count, MRReply **replies,
                               clock_t totalTime, clock_t postProcessTime) {
  bool has_map = RedisModule_HasMap(reply);
  RedisModule_Reply_Map(reply); // root
    // Have a named map for the results for RESP3
    if (has_map) {
      RedisModule_Reply_SimpleString(reply, "Results"); // >results
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
    };
    Profile_PrintInFormat(reply, PrintShardProfile, &shardsCtx, profileSearchReplyCoordinator, &coordCtx);

    RedisModule_Reply_MapEnd(reply); // >root
}

static void searchResultReducer_wrapper(void *mc_v) {
  struct MRCtx *mc = mc_v;
  searchResultReducer(mc, MRCtx_GetNumReplied(mc), MRCtx_GetReplies(mc));
}

static int searchResultReducer_background(struct MRCtx *mc, int count, MRReply **replies) {
  ConcurrentSearch_ThreadPoolRun(searchResultReducer_wrapper, mc, DIST_AGG_THREADPOOL);
  return REDISMODULE_OK;
}

static bool should_return_error(MRReply *reply) {
  // TODO: Replace third condition with a var instead of hard-coded string
  const char *errStr = MRReply_String(reply, NULL);
  return (!errStr
          || RSGlobalConfig.requestConfigParams.timeoutPolicy == TimeoutPolicy_Fail
          || strcmp(errStr, "Timeout limit was reached"));
}

static bool should_return_timeout_error(searchRequestCtx *req) {
  return RSGlobalConfig.requestConfigParams.timeoutPolicy == TimeoutPolicy_Fail
         && req->timeout != 0
         && ((double)(clock() - req->initClock) / CLOCKS_PER_MILLISEC) > req->timeout;
}

static int searchResultReducer(struct MRCtx *mc, int count, MRReply **replies) {
  clock_t postProcessTime;
  RedisModuleBlockedClient *bc = MRCtx_GetBlockedClient(mc);
  RedisModuleCtx *ctx = RedisModule_GetThreadSafeContext(bc);
  searchRequestCtx *req = MRCtx_GetPrivData(mc);
  searchReducerCtx rCtx = {NULL};
  int profile = req->profileArgs > 0;
  RedisModule_Reply _reply = RedisModule_NewReply(ctx), *reply = &_reply;

  int res = REDISMODULE_OK;
  // got no replies - this means timeout
  if (count == 0 || req->limit < 0) {
    res = RedisModule_Reply_Error(reply, "Could not send query to cluster");
    goto cleanup;
  }

  // Traverse the replies, check for early bail-out which we want for all errors
  // but timeout+non-strict timeout policy.
  for (int i = 0; i < count; i++) {
    MRReply *curr_rep = replies[i];
    if (MRReply_Type(curr_rep) == MR_REPLY_ERROR) {
      rCtx.errorOccurred = true;
      rCtx.lastError = curr_rep;
      if (should_return_error(curr_rep)) {
        res = MR_ReplyWithMRReply(reply, curr_rep);
        goto cleanup;
      }
    }
  }

  rCtx.searchCtx = req;

  // Get reply offsets
  getReplyOffsets(rCtx.searchCtx, &rCtx.offsets);

  // Init results heap.
  size_t num = req->requestedResultsCount;
  rCtx.pq = rm_malloc(heap_sizeof(num));
  heap_init(rCtx.pq, cmp_results, req, num);

  // Default result process and post process operations
  rCtx.processReply = (processReplyCB) processSearchReply;
  rCtx.postProcess = (postProcessReplyCB) noOpPostProcess;

  if (req->specialCases) {
    size_t nSpecialCases = array_len(req->specialCases);
    for (size_t i = 0; i < nSpecialCases; ++i) {
      if (req->specialCases[i]->specialCaseType == SPECIAL_CASE_KNN) {
        specialCaseCtx* knnCtx = req->specialCases[i];
        rCtx.postProcess = (postProcessReplyCB) knnPostProcess;
        rCtx.reduceSpecialCaseCtxKnn = knnCtx;
        if (knnCtx->knn.shouldSort) {
          knnCtx->knn.pq = rm_malloc(heap_sizeof(knnCtx->knn.k));
          heap_init(knnCtx->knn.pq, cmp_scored_results, NULL, knnCtx->knn.k);
          rCtx.processReply = (processReplyCB) ProcessKNNSearchReply;
          break;
        }
      } else if (req->specialCases[i]->specialCaseType == SPECIAL_CASE_SORTBY) {
        rCtx.reduceSpecialCaseCtxSortby = req->specialCases[i];
      }
    }
  }

  if (!profile) {
    for (int i = 0; i < count; ++i) {
      rCtx.processReply(replies[i], (struct searchReducerCtx *)&rCtx, ctx);

      // If we timed out on strict timeout policy, return a timeout error
      if (should_return_timeout_error(req)) {
        RedisModule_Reply_Error(reply, QueryError_Strerror(QUERY_ETIMEDOUT));
        goto cleanup;
      }
    }
  } else {
    for (int i = 0; i < count; ++i) {
      MRReply *mr_reply;
      if (reply->resp3) {
        mr_reply = MRReply_MapElement(replies[i], "Results");
      } else {
        mr_reply = MRReply_ArrayElement(replies[i], 0);
      }
      rCtx.processReply(mr_reply, (struct searchReducerCtx *)&rCtx, ctx);

      // If we timed out on strict timeout policy, return a timeout error
      if (should_return_timeout_error(req)) {
        RedisModule_Reply_Error(reply, QueryError_Strerror(QUERY_ETIMEDOUT));
        goto cleanup;
      }
    }
  }

  if (rCtx.cachedResult) {
    rm_free(rCtx.cachedResult);
  }

  if (rCtx.errorOccurred && !rCtx.lastError) {
    RedisModule_Reply_Error(reply, "could not parse redisearch results");
    goto cleanup;
  }

  if (!profile) {
    sendSearchResults(reply, &rCtx);
  } else {
    profileSearchReply(reply, &rCtx, count, replies, req->profileClock, clock());
  }

cleanup:
  RedisModule_EndReply(reply);

  if (rCtx.pq) {
    heap_destroy(rCtx.pq);
  }
  if (rCtx.reduceSpecialCaseCtxKnn &&
      rCtx.reduceSpecialCaseCtxKnn->knn.pq) {
    heap_destroy(rCtx.reduceSpecialCaseCtxKnn->knn.pq);
  }

  RedisModule_BlockedClientMeasureTimeEnd(bc);
  RedisModule_UnblockClient(bc, NULL);
  RedisModule_FreeThreadSafeContext(ctx);
  // We could pass `mc` to the unblock function to perform the next 3 cleanup steps, but
  // this way we free the memory from the background after the client is unblocked,
  // which is a bit more efficient.
  // The unblocking callback also replies with error if there was 0 replies from the shards,
  // and since we already replied with error in this case (in the beginning of this function),
  // we can't pass `mc` to the unblock function.
  searchRequestCtx_Free(req);
  MR_requestCompleted();
  MRCtx_Free(mc);
  return res;
}

static inline bool cannotBlockCtx(RedisModuleCtx *ctx) {
  return RedisModule_GetContextFlags(ctx) & REDISMODULE_CTX_FLAGS_DENY_BLOCKING;
}

static inline int ReplyBlockDeny(RedisModuleCtx *ctx, const RedisModuleString *cmd) {
  return RMUtil_ReplyWithErrorFmt(ctx, "Cannot perform `%s`: Cannot block", RedisModule_StringPtrLen(cmd, NULL));
}

static int genericCallUnderscoreVariant(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  size_t len;
  const char *cmd = RedisModule_StringPtrLen(argv[0], &len);
  RedisModule_Assert(!strncasecmp(cmd, "FT.", 3));
  char *localCmd;
  rm_asprintf(&localCmd, "_%.*s", len, cmd);
  /*
   * v - argv input array of RedisModuleString
   * C - same client
   * M - respect OOM
   * 0 - same RESP protocol
   * ! - replicate the command if needed (allows for replication)
   */
  RedisModuleCallReply *r = RedisModule_Call(ctx, localCmd, "vCM0!", argv + 1, argc - 1);
  RedisModule_ReplyWithCallReply(ctx, r); // Pass the reply to the client
  rm_free(localCmd);
  RedisModule_FreeCallReply(r);
  return REDISMODULE_OK;
}

// Supports FT.ADD, FT.DEL, FT.GET, FT.SUGADD, FT.SUGGET, FT.SUGDEL, FT.SUGLEN.
// If needed for more commands, make sure `MRCommand_GetShardingKey` is implemented for them.
// Notice that only OSS cluster should deal with such redirections.
int SingleShardCommandHandler(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {

  if (argc < 2) {
    return RedisModule_WrongArity(ctx);
  }
  if (!SearchCluster_Ready()) {
    return RedisModule_ReplyWithError(ctx, CLUSTERDOWN_ERR);
  } else if (NumShards == 1) {
    // There is only one shard in the cluster. We can handle the command locally.
    return genericCallUnderscoreVariant(ctx, argv, argc);
  }
  if (cannotBlockCtx(ctx)) {
    return ReplyBlockDeny(ctx, argv[0]);
  }
  RS_AutoMemory(ctx);

  MRCommand cmd = MR_NewCommandFromRedisStrings(argc, argv);
  MRCommand_SetProtocol(&cmd, ctx);
  /* Replace our own FT command with _FT. command */
  MRCommand_SetPrefix(&cmd, "_FT");

  MR_MapSingle(MR_CreateCtx(ctx, 0, NULL, NumShards), singleReplyReducer, cmd);

  return REDISMODULE_OK;
}

/* FT.MGET {idx} {key} ... */
int MGetCommandHandler(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {

  if (argc < 3) {
    return RedisModule_WrongArity(ctx);
  }
  // Check that the cluster state is valid
  if (!SearchCluster_Ready()) {
    return RedisModule_ReplyWithError(ctx, CLUSTERDOWN_ERR);
  } else if (NumShards == 1) {
    return genericCallUnderscoreVariant(ctx, argv, argc);
  }
  if (cannotBlockCtx(ctx)) {
    return ReplyBlockDeny(ctx, argv[0]);
  }
  RS_AutoMemory(ctx);

  MRCommand cmd = MR_NewCommandFromRedisStrings(argc, argv);
  MRCommand_SetProtocol(&cmd, ctx);
  /* Replace our own FT command with _FT. command */
  MRCommand_SetPrefix(&cmd, "_FT");

  struct MRCtx *mrctx = MR_CreateCtx(ctx, 0, NULL, NumShards);
  MR_Fanout(mrctx, mergeArraysReducer, cmd, true);
  return REDISMODULE_OK;
}

int SpellCheckCommandHandler(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  // Check that the cluster state is valid
  if (NumShards == 0) {
    // Cluster state is not ready
    return RedisModule_ReplyWithError(ctx, CLUSTERDOWN_ERR);
  } else if (NumShards == 1) {
    // There is only one shard in the cluster. We can handle the command locally.
    return SpellCheckCommand(ctx, argv, argc);
  }
  if (argc < 3) {
    return RedisModule_WrongArity(ctx);
  }
  if (cannotBlockCtx(ctx)) {
    return ReplyBlockDeny(ctx, argv[0]);
  }
  RS_AutoMemory(ctx);

  MRCommand cmd = MR_NewCommandFromRedisStrings(argc, argv);
  MRCommand_SetProtocol(&cmd, ctx);
  /* Replace our own FT command with _FT. command */
  MRCommand_SetPrefix(&cmd, "_FT");

  MRCommand_Insert(&cmd, 3, "FULLSCOREINFO", sizeof("FULLSCOREINFO") - 1);

  struct MRCtx *mrctx = MR_CreateCtx(ctx, 0, NULL, NumShards);
  MR_Fanout(mrctx, is_resp3(ctx) ? spellCheckReducer_resp3 : spellCheckReducer_resp2, cmd, true);
  return REDISMODULE_OK;
}

static int MastersFanoutCommandHandler(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  if (argc < 2) {
    return RedisModule_WrongArity(ctx);
  }

  // Check that the cluster state is valid
  if (!SearchCluster_Ready()) {
    return RedisModule_ReplyWithError(ctx, CLUSTERDOWN_ERR);
  } else if (NumShards == 1) {
    // There is only one shard in the cluster. We can handle the command locally.
    return genericCallUnderscoreVariant(ctx, argv, argc);
  }
  if (cannotBlockCtx(ctx)) {
    return ReplyBlockDeny(ctx, argv[0]);
  }
  RS_AutoMemory(ctx);

  MRCommand cmd = MR_NewCommandFromRedisStrings(argc, argv);
  MRCommand_SetProtocol(&cmd, ctx);
  /* Replace our own FT command with _FT. command */
  MRCommand_SetPrefix(&cmd, "_FT");
  struct MRCtx *mrctx = MR_CreateCtx(ctx, 0, NULL, NumShards);

  MR_Fanout(mrctx, allOKReducer, cmd, true);
  return REDISMODULE_OK;
}

void RSExecDistAggregate(RedisModuleCtx *ctx, RedisModuleString **argv, int argc,
                         struct ConcurrentCmdCtx *cmdCtx);
int RSAggregateCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);

static int DistAggregateCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  if (NumShards == 0) {
    return RedisModule_ReplyWithError(ctx, CLUSTERDOWN_ERR);
  } else if (NumShards == 1) {
    // There is only one shard in the cluster. We can handle the command locally.
    return RSAggregateCommand(ctx, argv, argc);
  }

  if (argc < 3) {
    return RedisModule_WrongArity(ctx);
  }
  if (cannotBlockCtx(ctx)) {
    return ReplyBlockDeny(ctx, argv[0]);
  }
  return ConcurrentSearch_HandleRedisCommandEx(DIST_AGG_THREADPOOL, CMDCTX_NO_GIL,
                                               RSExecDistAggregate, ctx, argv, argc);
}

static void CursorCommandInternal(RedisModuleCtx *ctx, RedisModuleString **argv, int argc, struct ConcurrentCmdCtx *cmdCtx) {
  RSCursorCommand(ctx, argv, argc);
}

static int CursorCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  if (argc < 4) {
    return RedisModule_WrongArity(ctx);
  }
  if (!SearchCluster_Ready()) {
    return RedisModule_ReplyWithError(ctx, CLUSTERDOWN_ERR);
  } else if (NumShards == 1) {
    // There is only one shard in the cluster. We can handle the command locally.
    return RSCursorCommand(ctx, argv, argc);
  }
  if (cannotBlockCtx(ctx)) {
    return ReplyBlockDeny(ctx, argv[0]);
  }
  return ConcurrentSearch_HandleRedisCommandEx(DIST_AGG_THREADPOOL, CMDCTX_NO_GIL,
                                               CursorCommandInternal, ctx, argv, argc);
}

int TagValsCommandHandler(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  if (argc < 3) {
    return RedisModule_WrongArity(ctx);
  }
  // Check that the cluster state is valid
  if (!SearchCluster_Ready()) {
    return RedisModule_ReplyWithError(ctx, CLUSTERDOWN_ERR);
  } else if (NumShards == 1) {
    return genericCallUnderscoreVariant(ctx, argv, argc);
  }
  if (cannotBlockCtx(ctx)) {
    return ReplyBlockDeny(ctx, argv[0]);
  }
  RS_AutoMemory(ctx);

  MRCommand cmd = MR_NewCommandFromRedisStrings(argc, argv);
  MRCommand_SetProtocol(&cmd, ctx);
  /* Replace our own FT command with _FT. command */
  MRCommand_SetPrefix(&cmd, "_FT");

  MR_Fanout(MR_CreateCtx(ctx, 0, NULL, NumShards), uniqueStringsReducer, cmd, true);
  return REDISMODULE_OK;
}

int InfoCommandHandler(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  if (argc != 2) {
    // FT.INFO {index}
    return RedisModule_WrongArity(ctx);
  }
  // Check that the cluster state is valid
  if (!SearchCluster_Ready()) {
    return RedisModule_ReplyWithError(ctx, CLUSTERDOWN_ERR);
  } else if (NumShards == 1) {
    // There is only one shard in the cluster. We can handle the command locally.
    return IndexInfoCommand(ctx, argv, argc);
  }
  if (cannotBlockCtx(ctx)) {
    return ReplyBlockDeny(ctx, argv[0]);
  }
  RS_AutoMemory(ctx);
  MRCommand cmd = MR_NewCommandFromRedisStrings(argc, argv);
  MRCommand_Append(&cmd, WITH_INDEX_ERROR_TIME, strlen(WITH_INDEX_ERROR_TIME));
  MRCommand_SetProtocol(&cmd, ctx);
  MRCommand_SetPrefix(&cmd, "_FT");

  struct MRCtx *mctx = MR_CreateCtx(ctx, 0, NULL, NumShards);
  MR_SetCoordinationStrategy(mctx, false); // send to all shards (not just the masters)
  MR_Fanout(mctx, InfoReplyReducer, cmd, true);
  return REDISMODULE_OK;
}

void sendRequiredFields(searchRequestCtx *req, MRCommand *cmd) {
  size_t specialCasesLen = array_len(req->specialCases);
  size_t offset = 0;
  for(size_t i=0; i < specialCasesLen; i++) {
    specialCaseCtx* ctx = req->specialCases[i];
    switch (ctx->specialCaseType) {
      // Handle sortby
      case SPECIAL_CASE_SORTBY: {
        // Sort by is always the first case.
        RedisModule_Assert(i==0);
        if(req->requiredFields == NULL) {
          req->requiredFields = array_new(const char*, 1);
        }
        array_append(req->requiredFields, ctx->sortby.sortKey);
        // Sortkey is the first required key value to return
        ctx->sortby.offset = 0;
        offset++;
        break;
      }
      case SPECIAL_CASE_KNN: {
        // Before requesting for a new field, see if it is not the sortkey.
        if(!ctx->knn.shouldSort) {
            // We have already requested this field, we will not append it.
            ctx->knn.offset = 0;
            break;;
        }
        // Fall back into appending new required field.
        if(req->requiredFields == NULL) {
          req->requiredFields = array_new(const char*, 1);
        }
        array_append(req->requiredFields, ctx->knn.fieldName);
        ctx->knn.offset = offset++;
        break;
      }
      default:
        break;
    }
  }

  if(req->requiredFields) {
    MRCommand_Append(cmd, "_REQUIRED_FIELDS", strlen("_REQUIRED_FIELDS"));
    int numberOfFields = array_len(req->requiredFields);
    char snum[8];
    int len = sprintf(snum, "%d", numberOfFields);
    MRCommand_Append(cmd, snum, len);
    for(size_t i = 0; i < numberOfFields; i++) {
        MRCommand_Append(cmd, req->requiredFields[i], strlen(req->requiredFields[i]));
    }
  }
}

int FlatSearchCommandHandler(RedisModuleBlockedClient *bc, int protocol, RedisModuleString **argv, int argc) {
  QueryError status = {0};
  searchRequestCtx *req = rscParseRequest(argv, argc, &status);

  if (!req) {
    RedisModuleCtx* clientCtx = RedisModule_GetThreadSafeContext(bc);
    RedisModule_ReplyWithError(clientCtx, QueryError_GetError(&status));
    QueryError_ClearError(&status);
    RedisModule_BlockedClientMeasureTimeEnd(bc);
    RedisModule_UnblockClient(bc, NULL);
    RedisModule_FreeThreadSafeContext(clientCtx);
    return REDISMODULE_OK;
  }

  MRCommand cmd = MR_NewCommandFromRedisStrings(argc, argv);
  cmd.protocol = protocol;

  // replace the LIMIT {offset} {limit} with LIMIT 0 {limit}, because we need all top N to merge
  int limitIndex = RMUtil_ArgExists("LIMIT", argv, argc, 3);
  if (limitIndex && req->limit > 0 && limitIndex < argc - 2) {
    size_t k =0;
    MRCommand_ReplaceArg(&cmd, limitIndex + 1, "0", 1);
    char buf[32];
    snprintf(buf, sizeof(buf), "%lld", req->requestedResultsCount);
    MRCommand_ReplaceArg(&cmd, limitIndex + 2, buf, strlen(buf));
  }

  /* Replace our own FT command with _FT. command */
  if (req->profileArgs == 0) {
    MRCommand_ReplaceArg(&cmd, 0, "_FT.SEARCH", sizeof("_FT.SEARCH") - 1);
  } else {
    MRCommand_ReplaceArg(&cmd, 0, "_FT.PROFILE", sizeof("_FT.PROFILE") - 1);
  }

  // adding the WITHSCORES option only if there is no SORTBY (hence the score is the default sort key)
  if (!req->withSortby) {
    MRCommand_Insert(&cmd, 3 + req->profileArgs, "WITHSCORES", sizeof("WITHSCORES") - 1);
  }

  if(req->specialCases) {
    sendRequiredFields(req, &cmd);
  }

  // Here we have an unsafe read of `NumShards`. This is fine because its just a hint.
  struct MRCtx *mrctx = MR_CreateCtx(0, bc, req, NumShards);

  MRCtx_SetReduceFunction(mrctx, searchResultReducer_background);
  MR_Fanout(mrctx, NULL, cmd, false);
  return REDISMODULE_OK;
}

typedef struct SearchCmdCtx {
  RedisModuleString **argv;
  int argc;
  RedisModuleBlockedClient* bc;
  int protocol;
}SearchCmdCtx;

static void DistSearchCommandHandler(void* pd) {
  SearchCmdCtx* sCmdCtx = pd;
  FlatSearchCommandHandler(sCmdCtx->bc, sCmdCtx->protocol, sCmdCtx->argv, sCmdCtx->argc);
  for (size_t i = 0 ; i < sCmdCtx->argc ; ++i) {
    RedisModule_FreeString(NULL, sCmdCtx->argv[i]);
  }
  rm_free(sCmdCtx->argv);
  rm_free(sCmdCtx);
}

// If the client is unblocked with a private data, we have to free it.
// This currently happens only when the client is unblocked without calling its reduce function,
// because we expect 0 replies. This function handles this case as well.
static int DistSearchUnblockClient(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  struct MRCtx *mrctx = RedisModule_GetBlockedClientPrivateData(ctx);
  if (mrctx) {
    if (MRCtx_GetNumReplied(mrctx) == 0) {
      RedisModule_ReplyWithError(ctx, "Could not send query to cluster");
    }
    searchRequestCtx_Free(MRCtx_GetPrivData(mrctx));
    MR_requestCompleted();
    MRCtx_Free(mrctx);
  }
}

int RSSearchCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);

static int DistSearchCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  if (NumShards == 0) {
    return RedisModule_ReplyWithError(ctx, CLUSTERDOWN_ERR);
  } else if (NumShards == 1) {
    // There is only one shard in the cluster. We can handle the command locally.
    return RSSearchCommand(ctx, argv, argc);
  }
  if (argc < 3) {
    return RedisModule_WrongArity(ctx);
  }
  if (cannotBlockCtx(ctx)) {
    return ReplyBlockDeny(ctx, argv[0]);
  }
  RedisModuleBlockedClient* bc = RedisModule_BlockClient(ctx, DistSearchUnblockClient, NULL, NULL, 0);
  SearchCmdCtx* sCmdCtx = rm_malloc(sizeof(*sCmdCtx));
  sCmdCtx->argv = rm_malloc(sizeof(RedisModuleString*) * argc);
  for (size_t i = 0 ; i < argc ; ++i) {
    // We need to copy the argv because it will be freed in the callback (from another thread).
    sCmdCtx->argv[i] = RedisModule_CreateStringFromString(ctx, argv[i]);
  }
  sCmdCtx->argc = argc;
  sCmdCtx->bc = bc;
  sCmdCtx->protocol = is_resp3(ctx) ? 3 : 2;
  RedisModule_BlockedClientMeasureTimeStart(bc);
  ConcurrentSearch_ThreadPoolRun(DistSearchCommandHandler, sCmdCtx, DIST_AGG_THREADPOOL);

  return REDISMODULE_OK;
}

int RSProfileCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int ProfileCommandHandler(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  if (argc < 5) {
    return RedisModule_WrongArity(ctx);
  }

  if (RMUtil_ArgExists("WITHCURSOR", argv, argc, 3)) {
    return RedisModule_ReplyWithError(ctx, "FT.PROFILE does not support cursor");
  }
  if (NumShards == 1) {
    // There is only one shard in the cluster. We can handle the command locally.
    // We must first check that we don't have a cursor, as the local command handler allows cursors
    // for multi-shard clusters support.
    return RSProfileCommand(ctx, argv, argc);
  }

  if (RMUtil_ArgExists("SEARCH", argv, 3, 2)) {
    return DistSearchCommand(ctx, argv, argc);
  }
  if (RMUtil_ArgExists("AGGREGATE", argv, 3, 2)) {
    return DistAggregateCommand(ctx, argv, argc);
  }
  return RedisModule_ReplyWithError(ctx, "No `SEARCH` or `AGGREGATE` provided");
}

int ClusterInfoCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  if (MR_CurrentTopologyExists()) {
    // If we have a topology, we must read it from the uv thread
    MR_uvReplyClusterInfo(ctx);
  } else {
    // If we don't have a topology, we can reply immediately
    MR_ReplyClusterInfo(ctx, NULL);
  }
  return REDISMODULE_OK;
}

// A special command for redis cluster OSS, that refreshes the cluster state
int RefreshClusterCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  UpdateTopology(ctx);
  return RedisModule_ReplyWithSimpleString(ctx, "OK");
}

int SetClusterCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  MRClusterTopology *topo = RedisEnterprise_ParseTopology(ctx, argv, argc);
  // this means a parsing error, the parser already sent the explicit error to the client
  if (!topo) {
    return REDISMODULE_ERR;
  }

  RedisModule_Log(ctx, "debug", "Setting number of partitions to %ld", topo->numShards);
  NumShards = topo->numShards;

  // send the topology to the cluster
  MR_UpdateTopology(topo);
  return RedisModule_ReplyWithSimpleString(ctx, "OK");
}

/* Perform basic configurations and init all threads and global structures */
static int initSearchCluster(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  RedisModule_Log(ctx, "notice",
                  "Cluster configuration: AUTO partitions, type: %d, coordinator timeout: %dms",
                  clusterConfig.type, clusterConfig.timeoutMS);

  if (clusterConfig.type == ClusterType_RedisOSS) {
    // Check if we are actually in cluster mode
    RedisModuleCallReply *rep = RedisModule_Call(ctx, "CONFIG", "cc", "GET", "cluster-enabled");
    RedisModule_Assert(rep && RedisModule_CallReplyType(rep) == REDISMODULE_REPLY_ARRAY &&
                       RedisModule_CallReplyLength(rep) == 2);
    size_t len;
    const char *isCluster = RedisModule_CallReplyStringPtr(RedisModule_CallReplyArrayElement(rep, 1), &len);
    if (STR_EQCASE(isCluster, len, "yes")) {
      // Init the topology updater cron loop.
      InitRedisTopologyUpdater(ctx);
    } else {
      // We are not in cluster mode. No need to init the topology updater cron loop.
      // Set the number of shards to 1 to indicate the topology is "set"
      NumShards = 1;
    }
    RedisModule_FreeCallReply(rep);
  }

  size_t num_connections_per_shard;
  if (clusterConfig.connPerShard) {
    num_connections_per_shard = clusterConfig.connPerShard;
  } else {
    // default
    #ifdef MT_BUILD
    num_connections_per_shard = RSGlobalConfig.numWorkerThreads + 1;
    #else
    num_connections_per_shard = 1;
    #endif
  }

  MRCluster *cl = MR_NewCluster(NULL, num_connections_per_shard);
  MR_Init(cl, clusterConfig.timeoutMS);

  return REDISMODULE_OK;
}

/** A dummy command handler, for commands that are disabled when running the module in OSS
 * clusters
 * when it is not an internal OSS build. */
int DisabledCommandHandler(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  return RedisModule_ReplyWithError(ctx, "Module Disabled in Open Source Redis");
}

/** A wrapper function that safely checks whether we are running in OSS cluster when registering
 * commands.
 * If we are, and the module was not compiled for oss clusters, this wrapper will return a pointer
 * to a dummy function disabling the actual handler.
 *
 * If we are running in RLEC or in a special OSS build - we simply return the original command.
 *
 * All coordinator handlers must be wrapped in this decorator.
 */
static RedisModuleCmdFunc SafeCmd(RedisModuleCmdFunc f) {
  if (RSBuildType_g == RSBuildType_Enterprise && clusterConfig.type != ClusterType_RedisLabs) {
    /* If we are running inside OSS cluster and not built for oss, we return the dummy handler */
    return DisabledCommandHandler;
  }

  /* Valid - we return the original function */
  return f;
}


#define RM_TRY(expr)                                                  \
  if (expr == REDISMODULE_ERR) {                                      \
    RedisModule_Log(ctx, "warning", "Could not run " __STRING(expr)); \
    return REDISMODULE_ERR;                                           \
  }

/**
 * A wrapper function to override hiredis allocators with redis allocators.
 * It should be called after RedisModule_Init.
 */
void setHiredisAllocators(){
  hiredisAllocFuncs ha = {
    .mallocFn = rm_malloc,
    .callocFn = rm_calloc,
    .reallocFn = rm_realloc,
    .strdupFn = rm_strdup,
    .freeFn = rm_free,
  };

  hiredisSetAllocators(&ha);
}

void Coordinator_ShutdownEvent(RedisModuleCtx *ctx, RedisModuleEvent eid, uint64_t subevent, void *data) {
  RedisModule_Log(ctx, "notice", "%s", "Begin releasing RediSearch resources on shutdown");
  RediSearch_CleanupModule();
  RedisModule_Log(ctx, "notice", "%s", "End releasing RediSearch resources");
}

void Initialize_CoordKeyspaceNotifications(RedisModuleCtx *ctx) {
  // To be called after `Initialize_KeyspaceNotifications` as callbacks are overridden.
  if (RedisModule_SubscribeToServerEvent && getenv("RS_GLOBAL_DTORS")) {
    // clear resources when the server exits
    // used only with sanitizer or valgrind
    RedisModule_Log(ctx, "notice", "%s", "Subscribe to clear resources on shutdown");
    RedisModule_SubscribeToServerEvent(ctx, RedisModuleEvent_Shutdown, Coordinator_ShutdownEvent);
  }
}

int __attribute__((visibility("default")))
RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {

  if (RedisModule_Init(ctx, REDISEARCH_MODULE_NAME, REDISEARCH_MODULE_VERSION,
                       REDISMODULE_APIVER_1) == REDISMODULE_ERR) {
    return REDISMODULE_ERR;
  }

  setHiredisAllocators();
  uv_replace_allocator(rm_malloc, rm_realloc, rm_calloc, rm_free);

  if (!RSDummyContext) {
    RSDummyContext = RedisModule_GetDetachedThreadSafeContext(ctx);
  }

  // Chain the config into RediSearch's global config and set the default values
  clusterConfig = DEFAULT_CLUSTER_CONFIG;
  RSConfigOptions_AddConfigs(&RSGlobalConfigOptions, GetClusterConfigOptions());
  ClusterConfig_RegisterTriggers();

  // Init RediSearch internal search
  if (RediSearch_InitModuleInternal(ctx, argv, argc) == REDISMODULE_ERR) {
    RedisModule_Log(ctx, "warning", "Could not init search library...");
    return REDISMODULE_ERR;
  }

  // Init the global cluster structs
  if (initSearchCluster(ctx, argv, argc) == REDISMODULE_ERR) {
    RedisModule_Log(ctx, "warning", "Could not init MR search cluster");
    return REDISMODULE_ERR;
  }

  // Init the aggregation thread pool
  DIST_AGG_THREADPOOL = ConcurrentSearch_CreatePool(clusterConfig.coordinatorPoolSize);

  Initialize_CoordKeyspaceNotifications(ctx);

  // read commands
  if (clusterConfig.type == ClusterType_RedisLabs) {
    RM_TRY(RedisModule_CreateCommand(ctx, "FT.AGGREGATE", SafeCmd(DistAggregateCommand), "readonly", 0, 1, -2));
  } else {
    RM_TRY(RedisModule_CreateCommand(ctx, "FT.AGGREGATE", SafeCmd(DistAggregateCommand), "readonly", 0, 0, -1));
  }
  RM_TRY(RedisModule_CreateCommand(ctx, "FT.INFO", SafeCmd(InfoCommandHandler), "readonly", 0, 0, -1));
  RM_TRY(RedisModule_CreateCommand(ctx, "FT.SEARCH", SafeCmd(DistSearchCommand), "readonly", 0, 0, -1));
  RM_TRY(RedisModule_CreateCommand(ctx, "FT.PROFILE", SafeCmd(ProfileCommandHandler), "readonly", 0, 0, -1));
  if (clusterConfig.type == ClusterType_RedisLabs) {
    RM_TRY(RedisModule_CreateCommand(ctx, "FT.CURSOR", SafeCmd(CursorCommand), "readonly", 3, 1, -3));
  } else {
    RM_TRY(RedisModule_CreateCommand(ctx, "FT.CURSOR", SafeCmd(CursorCommand), "readonly", 0, 0, -1));
  }
  RM_TRY(RedisModule_CreateCommand(ctx, "FT.SPELLCHECK", SafeCmd(SpellCheckCommandHandler), "readonly", 0, 0, -1));
  // Assumes "_FT.DEBUG" is registered (from `RediSearch_InitModuleInternal`)
  RM_TRY(RegisterCoordDebugCommands(RedisModule_GetCommand(ctx, "_FT.DEBUG")));

  if (RSBuildType_g == RSBuildType_OSS) {
    RedisModule_Log(ctx, "notice", "Register write commands");
    // suggestion commands
    RM_TRY(RedisModule_CreateCommand(ctx, "FT.SUGADD", SafeCmd(SingleShardCommandHandler), "readonly", 0, 0, -1));
    RM_TRY(RedisModule_CreateCommand(ctx, "FT.SUGGET", SafeCmd(SingleShardCommandHandler), "readonly", 0, 0, -1));
    RM_TRY(RedisModule_CreateCommand(ctx, "FT.SUGDEL", SafeCmd(SingleShardCommandHandler), "readonly", 0, 0, -1));
    RM_TRY(RedisModule_CreateCommand(ctx, "FT.SUGLEN", SafeCmd(SingleShardCommandHandler), "readonly", 0, 0, -1));
    // write commands (on enterprise we do not define them, the dmc take care of them)
    RM_TRY(RedisModule_CreateCommand(ctx, "FT.CREATE", SafeCmd(MastersFanoutCommandHandler), "readonly", 0, 0, -1));
    RM_TRY(RedisModule_CreateCommand(ctx, "FT._CREATEIFNX", SafeCmd(MastersFanoutCommandHandler), "readonly", 0, 0, -1));
    RM_TRY(RedisModule_CreateCommand(ctx, "FT.ALTER", SafeCmd(MastersFanoutCommandHandler), "readonly", 0, 0, -1));
    RM_TRY(RedisModule_CreateCommand(ctx, "FT._ALTERIFNX", SafeCmd(MastersFanoutCommandHandler), "readonly", 0, 0, -1));
    RM_TRY(RedisModule_CreateCommand(ctx, "FT.DROPINDEX", SafeCmd(MastersFanoutCommandHandler), "readonly",0, 0, -1));
    RM_TRY(RedisModule_CreateCommand(ctx, "FT._DROPINDEXIFX", SafeCmd(MastersFanoutCommandHandler), "readonly",0, 0, -1));
    RM_TRY(RedisModule_CreateCommand(ctx, "FT.DICTADD", SafeCmd(MastersFanoutCommandHandler), "readonly", 0, 0, -1));
    RM_TRY(RedisModule_CreateCommand(ctx, "FT.DICTDEL", SafeCmd(MastersFanoutCommandHandler), "readonly", 0, 0, -1));
    RM_TRY(RedisModule_CreateCommand(ctx, "FT.ALIASADD", SafeCmd(MastersFanoutCommandHandler), "readonly", 0, 0, -1));
    RM_TRY(RedisModule_CreateCommand(ctx, "FT._ALIASADDIFNX", SafeCmd(MastersFanoutCommandHandler), "readonly", 0, 0, -1));
    RM_TRY(RedisModule_CreateCommand(ctx, "FT.ALIASDEL", SafeCmd(MastersFanoutCommandHandler), "readonly", 0, 0, -1));
    RM_TRY(RedisModule_CreateCommand(ctx, "FT._ALIASDELIFX", SafeCmd(MastersFanoutCommandHandler), "readonly", 0, 0, -1));
    RM_TRY(RedisModule_CreateCommand(ctx, "FT.ALIASUPDATE", SafeCmd(MastersFanoutCommandHandler), "readonly", 0, 0, -1));
    RM_TRY(RedisModule_CreateCommand(ctx, "FT.SYNUPDATE", SafeCmd(MastersFanoutCommandHandler),"readonly", 0, 0, -1));
    RM_TRY(RedisModule_CreateCommand(ctx, "FT.SYNFORCEUPDATE", SafeCmd(MastersFanoutCommandHandler),"readonly", 0, 0, -1));
  }

  // cluster set commands
  RM_TRY(RedisModule_CreateCommand(ctx, REDISEARCH_MODULE_NAME".CLUSTERSET", SafeCmd(SetClusterCommand), "readonly allow-loading deny-script", 0,0, -1));
  RM_TRY(RedisModule_CreateCommand(ctx, REDISEARCH_MODULE_NAME".CLUSTERREFRESH", SafeCmd(RefreshClusterCommand),"readonly deny-script", 0, 0, -1));
  RM_TRY(RedisModule_CreateCommand(ctx, REDISEARCH_MODULE_NAME".CLUSTERINFO", SafeCmd(ClusterInfoCommand), "readonly allow-loading deny-script",0, 0, -1));

  // Deprecated commands. Grouped here for easy tracking
  RM_TRY(RedisModule_CreateCommand(ctx, "FT.MGET", SafeCmd(MGetCommandHandler), "readonly", 0, 0, -1));
  RM_TRY(RedisModule_CreateCommand(ctx, "FT.TAGVALS", SafeCmd(TagValsCommandHandler), "readonly", 0, 0, -1));
  if (RSBuildType_g == RSBuildType_OSS) {
    RM_TRY(RedisModule_CreateCommand(ctx, "FT.GET", SafeCmd(SingleShardCommandHandler), "readonly", 0, 0, -1));
    RM_TRY(RedisModule_CreateCommand(ctx, "FT.ADD", SafeCmd(SingleShardCommandHandler), "readonly", 0, 0, -1));
    RM_TRY(RedisModule_CreateCommand(ctx, "FT.DEL", SafeCmd(SingleShardCommandHandler), "readonly", 0, 0, -1));
    RM_TRY(RedisModule_CreateCommand(ctx, "FT.DROP", SafeCmd(MastersFanoutCommandHandler), "readonly",0, 0, -1));
    RM_TRY(RedisModule_CreateCommand(ctx, "FT._DROPIFX", SafeCmd(MastersFanoutCommandHandler), "readonly",0, 0, -1));
  }

  return REDISMODULE_OK;
}
