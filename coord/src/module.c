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
#include "crc16_tags.h"
#include "crc12_tags.h"
#include "rmr/redis_cluster.h"
#include "rmr/redise.h"
#include "fnv32.h"
#include "util/heap.h"
#include "search_cluster.h"
#include "config.h"
#include "module.h"
#include "info_command.h"
#include "version.h"
#include "cursor.h"
#include "build-info/info.h"
#include "aggregate/aggregate.h"
#include "value.h"
#include "cluster_spell_check.h"
#include "profile.h"

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

int redisMajorVesion = 0;
int redisMinorVesion = 0;
int redisPatchVesion = 0;
//REDISMODULE_INIT_SYMBOLS();

extern RedisModuleCtx *RSDummyContext;

// forward declaration
int allOKReducer(struct MRCtx *mc, int count, MRReply **replies);
RSValue *MRReply_ToValue(MRReply *r, RSValueType convertType);

/* A reducer that just chains the replies from a map request */
int chainReplyReducer(struct MRCtx *mc, int count, MRReply **replies) {

  RedisModuleCtx *ctx = MRCtx_GetRedisCtx(mc);

  RedisModule_ReplyWithArray(ctx, count);
  for (int i = 0; i < count; i++) {
    MR_ReplyWithMRReply(ctx, replies[i]);
  }
  // RedisModule_ReplySetArrayLength(ctx, x);
  return REDISMODULE_OK;
}

/* A reducer that just merges N arrays of strings by chaining them into one big array with no
 * duplicates */
int uniqueStringsReducer(struct MRCtx *mc, int count, MRReply **replies) {
  RedisModuleCtx *ctx = MRCtx_GetRedisCtx(mc);

  MRReply *err = NULL;

  TrieMap *dict = NewTrieMap();
  int nArrs = 0;
  // Add all the array elements into the dedup dict
  for (int i = 0; i < count; i++) {
    if (replies[i] && MRReply_Type(replies[i]) == MR_REPLY_ARRAY) {
      nArrs++;
      for (size_t j = 0; j < MRReply_Length(replies[i]); j++) {
        size_t sl = 0;
        char *s = MRReply_String(MRReply_ArrayElement(replies[i], j), &sl);
        if (s && sl) {
          TrieMap_Add(dict, s, sl, NULL, NULL);
        }
      }
    } else if (MRReply_Type(replies[i]) == MR_REPLY_ERROR && err == NULL) {
      err = replies[i];
    }
  }

  // if there are no values - either reply with an empty array or an error
  if (dict->cardinality == 0) {

    if (nArrs > 0) {
      // the arrays were empty - return an empty array
      RedisModule_ReplyWithArray(ctx, 0);
    } else {
      TrieMap_Free(dict, NULL);
      return RedisModule_ReplyWithError(ctx, err ? (const char *)err : "Could not perfrom query");
    }
    goto cleanup;
  }

  char *s;
  tm_len_t sl;
  void *p;
  // Iterate the dict and reply with all values
  TrieMapIterator *it = TrieMap_Iterate(dict, "", 0);
  RedisModule_ReplyWithArray(ctx, dict->cardinality);
  while (TrieMapIterator_Next(it, &s, &sl, &p)) {
    RedisModule_ReplyWithStringBuffer(ctx, s, sl);
  }

  TrieMapIterator_Free(it);

cleanup:
  TrieMap_Free(dict, NULL);

  return REDISMODULE_OK;
}
/* A reducer that just merges N arrays of the same length, selecting the first non NULL reply from
 * each */
int mergeArraysReducer(struct MRCtx *mc, int count, MRReply **replies) {

  RedisModuleCtx *ctx = MRCtx_GetRedisCtx(mc);

  for (size_t i = 0; i < count; ++i) {
    if (MRReply_Type(replies[i]) == MR_REPLY_ERROR) {
      // we got an error reply, something goes wrong so we return the error to the user.
      return MR_ReplyWithMRReply(ctx, replies[i]);
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
        if (j == 0) RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);

        MR_ReplyWithMRReply(ctx, ele);
        j++;
        break;
      }
    }
  } while (stillValid > 0);

  // j 0 means we could not process a single reply element from any reply
  if (j == 0) {
    return RedisModule_ReplyWithError(ctx, "Could not process replies");
  }
  RedisModule_ReplySetArrayLength(ctx, j);

  return REDISMODULE_OK;
}

int synonymAddFailedReducer(struct MRCtx *mc, int count, MRReply **replies) {
  RedisModuleCtx *ctx = MRCtx_GetRedisCtx(mc);
  if (count == 0) {
    return RedisModule_ReplyWithNull(ctx);
  }

  MR_ReplyWithMRReply(ctx, replies[0]);

  return REDISMODULE_OK;
}

int synonymAllOKReducer(struct MRCtx *mc, int count, MRReply **replies) {
  RedisModuleCtx *ctx = MRCtx_GetRedisCtx(mc);
  if (count == 0) {
    RedisModule_ReplyWithError(ctx, "Could not distribute comand");
    return REDISMODULE_OK;
  }
  for (int i = 0; i < count; i++) {
    if (MRReply_Type(replies[i]) == MR_REPLY_ERROR) {
      MR_ReplyWithMRReply(ctx, replies[i]);
      return REDISMODULE_OK;
    }
  }

  assert(MRCtx_GetCmdsSize(mc) >= 1);
  assert(MRCtx_GetCmds(mc)[0].num > 3);
  size_t groupLen;
  const char *groupStr = MRCommand_ArgStringPtrLen(&MRCtx_GetCmds(mc)[0], 2, &groupLen);
  RedisModuleString *synonymGroupIdStr = RedisModule_CreateString(ctx, groupStr, groupLen);
  long long synonymGroupId = 0;
  int rv = RedisModule_StringToLongLong(synonymGroupIdStr, &synonymGroupId);
  assert(rv == REDIS_OK);

  RedisModule_ReplyWithLongLong(ctx, synonymGroupId);

  RedisModule_FreeString(ctx, synonymGroupIdStr);
  return REDISMODULE_OK;
}

int synonymUpdateFanOutReducer(struct MRCtx *mc, int count, MRReply **replies) {
  RedisModuleCtx *ctx = MRCtx_GetRedisCtx(mc);
  if (count != 1 || MRReply_Type(replies[0]) != MR_REPLY_INTEGER) {
    RedisModuleBlockedClient *bc = (RedisModuleBlockedClient *)ctx;
    RS_CHECK_FUNC(RedisModule_BlockedClientMeasureTimeEnd, bc);
    RedisModule_UnblockClient(bc, mc);
    return REDISMODULE_OK;
  }
  assert(MRCtx_GetCmdsSize(mc) == 1);
  MRCommand updateCommand = {NULL};
  const MRCommand *srcCmd = &MRCtx_GetCmds(mc)[0];
  for (size_t ii = 0; ii < 2; ++ii) {
    MRCommand_AppendFrom(&updateCommand, srcCmd, ii);
  }

  // MRCommand updateCommand = MR_NewCommandFromStrings(2, MRCtx_GetCmds(mc)[0].args);
  double d = 0;
  MRReply_ToDouble(replies[0], &d);
  char buf[128] = {0};
  size_t nbuf = sprintf(buf, "%lu", (unsigned long)d);
  MRCommand_Append(&updateCommand, buf, nbuf);

  for (size_t ii = 2; ii < srcCmd->num; ++ii) {
    MRCommand_AppendFrom(&updateCommand, srcCmd, ii);
  }

  const char *cmdName = "_FT.SYNFORCEUPDATE";
  MRCommand_ReplaceArg(&updateCommand, 0, cmdName, strlen(cmdName));

  size_t idLen = 0;
  const char *idStr = MRCommand_ArgStringPtrLen(&updateCommand, 1, &idLen);
  MRKey key = {0};
  MRKey_Parse(&key, idStr, idLen);

  // reseting the tag
  MRCommand_ReplaceArg(&updateCommand, 1, key.base, key.baseLen);

  MRCommandGenerator cg = SearchCluster_MultiplexCommand(GetSearchCluster(), &updateCommand);
  struct MRCtx *mrctx = MR_CreateCtx(ctx, NULL);
  MR_SetCoordinationStrategy(mrctx, MRCluster_MastersOnly);
  MR_Map(mrctx, synonymAllOKReducer, cg, false);
  cg.Free(cg.ctx);

  // we need to call request complete here manualy since we did not unblocked the client
  MR_requestCompleted();
  return REDISMODULE_OK;
}

int singleReplyReducer(struct MRCtx *mc, int count, MRReply **replies) {

  RedisModuleCtx *ctx = MRCtx_GetRedisCtx(mc);
  if (count == 0) {
    return RedisModule_ReplyWithNull(ctx);
  }

  MR_ReplyWithMRReply(ctx, replies[0]);

  return REDISMODULE_OK;
}
// a reducer that expects "OK" reply for all replies, and stops at the first error and returns it
int allOKReducer(struct MRCtx *mc, int count, MRReply **replies) {
  RedisModuleCtx *ctx = MRCtx_GetRedisCtx(mc);
  if (count == 0) {
    RedisModule_ReplyWithError(ctx, "Could not distribute comand");
    return REDISMODULE_OK;
  }
  bool isIntegerReply = false;
  long long integerReply = 0;
  for (int i = 0; i < count; i++) {
    if (MRReply_Type(replies[i]) == MR_REPLY_ERROR) {
      MR_ReplyWithMRReply(ctx, replies[i]);
      return REDISMODULE_OK;
    }
    if (MRReply_Type(replies[i]) == MR_REPLY_INTEGER) {
      long long currIntegerReply = MRReply_Integer(replies[i]);
      if (!isIntegerReply) {
        integerReply = currIntegerReply;
        isIntegerReply = true;
      } else if (currIntegerReply != integerReply) {
        RedisModule_ReplyWithSimpleString(ctx, "not all results are the same");
        return REDISMODULE_OK;
      }
    }
  }

  if (isIntegerReply) {
    RedisModule_ReplyWithLongLong(ctx, integerReply);
  } else {
    RedisModule_ReplyWithSimpleString(ctx, "OK");
  }
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

typedef enum {
  SPECIAL_CASE_NONE,
  SPECIAL_CASE_KNN,
  SPECIAL_CASE_SORTBY
} searchRequestSpecialCase;


typedef struct {
      size_t k;               // K value
      const char* fieldName;  // Field name
      bool shouldSort;        // Should run presort before the coordinator sort
      size_t offset;          // Reply offset
      heap_t *pq;             // Priority queue
      QueryNode* queryNode;   // Query node
} knnContext;

typedef struct {
  const char* sortKey;  // SortKey name;
  bool asc;             // Sort order ASC/DESC
  size_t offset;        // SortKey reply offset
} sortbyContext;

typedef struct {
  union {
    knnContext knn;
    sortbyContext sortby;
  };
  searchRequestSpecialCase specialCaseType;
} specialCaseCtx;

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

typedef struct {
  char *queryString;
  long long offset;
  long long limit;
  long long requestedResultsCount;
  int withScores;
  int withExplainScores;
  int withPayload;
  int withSortby;
  int sortAscending;
  int withSortingKeys;
  int noContent;

  specialCaseCtx** specialCases;
  const char** requiredFields;
  // used to signal profile flag and count related args
  int profileArgs;
  int profileLimited;
  clock_t profileClock;
  void *reducer;
} searchRequestCtx;

typedef struct{
  MRReply *lastError;
  searchResult *cachedResult;
  searchRequestCtx *searchCtx;
  heap_t *pq;
  size_t totalReplies;
  bool errorOccured;
  searchReplyOffsets offsets;
  processReplyCB processReply;
  postProcessReplyCB postProcess;
  specialCaseCtx* reduceSpecialCaseCtx;
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
  rm_free(ctx);
}

void searchRequestCtx_Free(searchRequestCtx *r) {
  rm_free(r->queryString);
  if(r->specialCases) {
    size_t specialCasesLen = array_len(r->specialCases);
    for(size_t i = 0; i< specialCasesLen; i ++) {
      specialCaseCtx* ctx = r->specialCases[i];
      if(ctx->specialCaseType == SPECIAL_CASE_KNN) {
        QueryNode_Free(ctx->knn.queryNode);
      }
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
static int profileSearchResultReducer(struct MRCtx *mc, int count, MRReply **replies);

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

// Prepare a TOPK special case.
void prepareOptionalTopKCase(searchRequestCtx *req, RedisModuleString **argv, int argc, QueryError *status) {

  // First, parse the query params if exists, to set the params in the query parser ctx.
  dict *params = NULL;
  int paramsOffset = RMUtil_ArgExists("PARAMS", argv, argc, 1)+1;
  if(paramsOffset > 1) {
    ArgsCursor ac;
    ArgsCursor_InitRString(&ac, argv+paramsOffset, argc-paramsOffset);
    if (parseParams(&params, &ac, status) != REDISMODULE_OK) {
        return;
    }
  }
  RedisSearchCtx sctx = {0};
  RSSearchOptions opts = {0};
  opts.params = params;
  QueryParseCtx qpCtx = {
      .raw = req->queryString,
      .len = strlen(req->queryString),
      .sctx = &sctx,
      .opts = &opts,
      .status = status,
#ifdef PARSER_DEBUG
      .trace_log = NULL
#endif
  };

  // KNN queries are parsed only on dialect versions >=2
  QueryNode* queryNode = RSQuery_ParseRaw_v2(&qpCtx);
  if(status->code != 0 ) {
    //fail.
  }
  if (queryNode!= NULL && QueryNode_NumParams(queryNode)>0) {
    if (paramsOffset != 0) {
      QueryNode_EvalParamsCommon(params, queryNode, status);
    } else {
      // fail
    }
  }
  if (params) {
    Param_DictFree(params);
  }

  if(queryNode!= NULL && queryNode->type == QN_VECTOR) {
    QueryVectorNode queryVectorNode = queryNode->vn;
    size_t k = queryVectorNode.vq->knn.k;
    specialCaseCtx *ctx = SpecialCaseCtx_New();
    ctx->knn.k = k;
    ctx->knn.fieldName = queryNode->opts.distField ? queryNode->opts.distField : queryVectorNode.vq->scoreField;
    ctx->knn.pq = NULL;
    ctx->knn.queryNode = queryNode;
    ctx->specialCaseType = SPECIAL_CASE_KNN;
    if(!req->specialCases) {
      req->specialCases = array_new(specialCaseCtx*, 1);
    }
    req->specialCases = array_append(req->specialCases, ctx);
    // Default: No SORTBY is given, or SORTBY is given by other field
    // When first sorting by different field, the topk vectors should be passed to the coordinator heap
    ctx->knn.shouldSort = true;
    // We need to get K results from the shards
    // For example the command request SORTBY text_field LIMIT 2 3
    // In this case the top 5 results relevant for this sort might be the in the last 5 results of the TOPK
    long long requestedResultsCount = req->requestedResultsCount;
    req->requestedResultsCount = MAX(k, requestedResultsCount);
    if(array_len(req->specialCases) > 1) {
      specialCaseCtx* optionalSortCtx = req->specialCases[0];
      if(optionalSortCtx->specialCaseType == SPECIAL_CASE_SORTBY) {
        if(strcmp(optionalSortCtx->sortby.sortKey, ctx->knn.fieldName) == 0){
          // If SORTBY is done by the vector score field, the coordinator will do it and no special operation is needed.
          ctx->knn.shouldSort = false;
          // The requested results should be at most K
          req->requestedResultsCount = MIN(k, requestedResultsCount);
        }
      }
    }
  }
}

// Prepare a sortby special case.
void prepareSortbyCase(searchRequestCtx *req, RedisModuleString **argv, int argc, int sortByIndex) {
  const char* sortkey = RedisModule_StringPtrLen(argv[sortByIndex + 1], NULL);
  specialCaseCtx *ctx = SpecialCaseCtx_New();
  ctx->specialCaseType = SPECIAL_CASE_SORTBY;
  ctx->sortby.sortKey = sortkey;
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
  req->specialCases = array_append(req->specialCases, ctx);
}

searchRequestCtx *rscParseRequest(RedisModuleString **argv, int argc, QueryError* status) {
  /* A search request must have at least 3 args */
  if (argc < 3) {
    return NULL;
  }

  searchRequestCtx *req = rm_malloc(sizeof *req);

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
  if(sortByIndex > 2) {
    req->withSortby = true;
    // Check for command error where no sortkey is given.
    if(sortByIndex + 1 >= argc) {
      searchRequestCtx_Free(req);
      return NULL;
    }
    prepareSortbyCase(req, argv, argc, sortByIndex);
  }
  else {
    req->withSortby = false;
  }

  unsigned int dialect = RSGlobalConfig.defaultDialectVersion;
  int dialectArgIndex = RMUtil_ArgExists("DIALECT", argv, argc, argvOffset);
  if(dialectArgIndex > 0) {
      dialectArgIndex++;
      ArgsCursor ac;
      ArgsCursor_InitRString(&ac, argv+dialectArgIndex, argc-dialectArgIndex);
      if (parseDialect(&dialect, &ac, status) != REDISMODULE_OK) {
        searchRequestCtx_Free(req);
        return NULL;
      }
  }

  if(dialect >= 2) {
    // Note: currently there is only one single case. For extending those cases we should use a trie here.
    if(strcasestr(req->queryString, "KNN")) {
      prepareOptionalTopKCase(req, argv, argc, status);
      if (QueryError_HasError(status)) {
        searchRequestCtx_Free(req);
        return NULL;
      }
    }
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

searchResult *newResult(searchResult *cached, MRReply *arr, int j, searchReplyOffsets* offsets, int explainScores) {
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
  res->id = MRReply_String(MRReply_ArrayElement(arr, j), &res->idLen);
  if (!res->id) {
    return res;
  }
  // parse socre
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
  } else {
    res->sortKey = NULL;
  }
  if (res->sortKey) {
    if (res->sortKey[0] == '#') {
      char *eptr;
      double d = strtod(res->sortKey + 1, &eptr);
      if (eptr != res->sortKey + 1 && *eptr == 0) {
        res->sortKeyNum = d;
      }
    } else if (!strncmp(res->sortKey, "none", 4)) {
      res->sortKey = NULL;
    }
    // fprintf(stderr, "Sort key string '%s', num '%f\n", res->sortKey, res->sortKeyNum);
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
   * ...              | special cases - SORTBY, TOPK. Sort key is always first for backwords comptability.
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
  if(ctx->specialCases) {
    size_t nSpecialCases = array_len(ctx->specialCases);
    for(size_t i =0; i < nSpecialCases; i++) {
      switch (ctx->specialCases[i]->specialCaseType)
      {
      case SPECIAL_CASE_KNN: {
        ctx->specialCases[i]->knn.offset+=specialCaseStartOffset;
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

static void proccessKNNSearchReply(MRReply *arr, searchReducerCtx *rCtx, RedisModuleCtx *ctx) {
  if (arr == NULL) {
    return;
  }
  if (MRReply_Type(arr) == MR_REPLY_ERROR) {
    rCtx->lastError = arr;
    return;
  }
  if (MRReply_Type(arr) != MR_REPLY_ARRAY || MRReply_Length(arr) == 0) {
    // Empty reply??
    return;
  }

  searchRequestCtx *req = rCtx->searchCtx;

  size_t len = MRReply_Length(arr);

  int step = rCtx->offsets.step;
  specialCaseCtx* reduceSpecialCaseCtx = rCtx->reduceSpecialCaseCtx;
  int scoreOffset = reduceSpecialCaseCtx->knn.offset;
  for (int j = 1; j < len; j += step) {
    if (j + step > len) {
      RedisModule_Log(
          ctx, "warning",
          "got a bad reply from redisearch, reply contains less parameters then expected");
      rCtx->errorOccured = true;
      break;
    }
    searchResult *res = newResult(rCtx->cachedResult, arr, j, &rCtx->offsets , rCtx->searchCtx->withExplainScores);
    if (!res || !res->id) {
      RedisModule_Log(ctx, "warning", "got an unexpected argument when parsing redisearch results");
      rCtx->errorOccured = true;
      // invalid result - usually means something is off with the response, and we should just
      // quit this response
      rCtx->cachedResult = res;
      break;
    } else {
      rCtx->cachedResult = NULL;
    }

    size_t len;
    char* score = MRReply_String(MRReply_ArrayElement(arr, j + scoreOffset), &len);
    RedisModule_Assert(score[0] == '#');
    char *eptr;
    double d = strtod(score + 1, &eptr);
    RedisModule_Assert(eptr != res->sortKey + 1 && *eptr == 0);

    // As long as we don't have k results, keep insert
    if (heap_count(reduceSpecialCaseCtx->knn.pq) < reduceSpecialCaseCtx->knn.k) {
      scoredSearchResultWrapper* resWrapper = rm_malloc(sizeof(scoredSearchResultWrapper));
      resWrapper->result = res;
      resWrapper->score = d;
      heap_offerx(reduceSpecialCaseCtx->knn.pq, resWrapper);
    } else {
      // Check for upper bound
      scoredSearchResultWrapper tmpWrapper;
      tmpWrapper.result = res;
      tmpWrapper.score = d;
      scoredSearchResultWrapper *largest = heap_peek(reduceSpecialCaseCtx->knn.pq);
      int c = cmp_scored_results(&tmpWrapper, largest, rCtx->searchCtx);
      if (c < 0) {
        scoredSearchResultWrapper* resWrapper = rm_malloc(sizeof(scoredSearchResultWrapper));
        resWrapper->result = res;
        resWrapper->score = d;
        // Current result is smaller then upper bound, replace them.
        largest = heap_poll(reduceSpecialCaseCtx->knn.pq);
        heap_offerx(reduceSpecialCaseCtx->knn.pq, resWrapper);
        rCtx->cachedResult = largest->result;
        rm_free(largest);
      } else {
        rCtx->cachedResult = res;
      }
    }
  }
}

static void processSearchReply(MRReply *arr, searchReducerCtx *rCtx, RedisModuleCtx *ctx) {
  if (arr == NULL) {
    return;
  }
  if (MRReply_Type(arr) == MR_REPLY_ERROR) {
    rCtx->lastError = arr;
    return;
  }
  if (MRReply_Type(arr) != MR_REPLY_ARRAY || MRReply_Length(arr) == 0) {
    // Empty reply??
    return;
  }

  searchRequestCtx *req = rCtx->searchCtx;

  // first element is always the total count
  rCtx->totalReplies += MRReply_Integer(MRReply_ArrayElement(arr, 0));
  size_t len = MRReply_Length(arr);

  int step = rCtx->offsets.step;
  // fprintf(stderr, "Step %d, scoreOffset %d, fieldsOffset %d, sortKeyOffset %d\n", step,
  //         scoreOffset, fieldsOffset, sortKeyOffset);
  for (int j = 1; j < len; j += step) {
    if (j + step > len) {
      RedisModule_Log(
          ctx, "warning",
          "got a bad reply from redisearch, reply contains less parameters then expected");
      rCtx->errorOccured = true;
      break;
    }
    searchResult *res = newResult(rCtx->cachedResult, arr, j, &rCtx->offsets , rCtx->searchCtx->withExplainScores);
    if (!res || !res->id) {
      RedisModule_Log(ctx, "warning", "got an unexpected argument when parsing redisearch results");
      rCtx->errorOccured = true;
      // invalid result - usually means something is off with the response, and we should just
      // quit this response
      rCtx->cachedResult = res;
      break;
    } else {
      rCtx->cachedResult = NULL;
    }

    // fprintf(stderr, "Response %d result %d Reply docId %s score: %f sortkey %f\n", i, j,
    //         res->id, res->score, res->sortKeyNum);

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
          break;
        }
      }
    }
  }
}

/************************ Result post processing callbacks ********************/


static void noOpPostProcess(searchReducerCtx *rCtx){
  return;
}

static void knnPostProcess(searchReducerCtx *rCtx) {
  specialCaseCtx* reducerSpecialCaseCtx = rCtx->reduceSpecialCaseCtx;
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

static void sendSearchResults(RedisModuleCtx *ctx, searchReducerCtx *rCtx) {
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

  RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
  RedisModule_ReplyWithLongLong(ctx, rCtx->totalReplies);
  size_t len = 1;

  for (pos = rCtx->searchCtx->offset; pos < qlen && pos < num; pos++) {
    searchResult *res = results[pos];
    RedisModule_ReplyWithStringBuffer(ctx, res->id, res->idLen);
    len++;
    if (req->withScores) {
      if (req->withExplainScores) {
        RedisModule_ReplyWithArray(ctx, 2);
      }
      RedisModule_ReplyWithDouble(ctx, res->score);
      if (req->withExplainScores) {
          MR_ReplyWithMRReply(ctx, res->explainScores);
      }
      len++;
    }
    if (req->withPayload) {

      MR_ReplyWithMRReply(ctx, res->payload);
      len++;
    }
    if (req->withSortingKeys && req->withSortby) {
      len++;
      if (res->sortKey) {
        RedisModule_ReplyWithStringBuffer(ctx, res->sortKey, res->sortKeyLen);
      } else {
        RedisModule_ReplyWithNull(ctx);
      }
    }
    if (!req->noContent) {
      MR_ReplyWithMRReply(ctx, res->fields);
      len++;
    }
  }
  RedisModule_ReplySetArrayLength(ctx, len);

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
size_t PrintShardProfile(RedisModuleCtx *ctx, int count, MRReply **replies, int isSearch) {
  size_t retLen = 0;
  // Print information for each shard
  for (int i = 0; i < count; ++i) {
    RedisModule_ReplyWithPrintf(ctx, "Shard #%d", i + 1);
    retLen++;
    // The 1st location always stores the results. On FT.AGGREGATE, the next place stores the
    // cursor ID. The last location (2nd for FT.SEARCH and 3rd for FT.AGGREGATE) stores the
    // profile information of the shard.
    int idx = isSearch ? 1 : 2;
    MRReply *reply = MRReply_ArrayElement(replies[i], idx);
    int len = MRReply_Length(reply);
    for (int j = 0; j < len; ++j) {
      MR_ReplyWithMRReply(ctx, MRReply_ArrayElement(reply, j));
    }
    retLen += len;
  }
  return retLen;
}

static void profileSearchReply(RedisModuleCtx *ctx, searchReducerCtx *rCtx,
                               int count, MRReply **replies,
                               clock_t totalTime, clock_t postProccesTime) {
  RedisModule_ReplyWithArray(ctx, 2);
  // print results
  sendSearchResults(ctx, rCtx);

  // print profile of shards
  int arrLen = 0;
  RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);

  arrLen += PrintShardProfile(ctx, count, replies, 1);

  // print coordinator stats
  RedisModule_ReplyWithSimpleString(ctx, "Coordinator");
  arrLen++;
  // search cmd only do the heap so there is no parsing time
  RedisModule_ReplyWithArray(ctx, 2);
  RedisModule_ReplyWithSimpleString(ctx, "Total Coordinator time");
  RedisModule_ReplyWithDouble(ctx, (double)(clock() - totalTime) / CLOCKS_PER_MILLISEC);
  arrLen++;

  RedisModule_ReplyWithArray(ctx, 2);
  RedisModule_ReplyWithSimpleString(ctx, "Post Proccessing time");
  RedisModule_ReplyWithDouble(ctx, (double)(clock() - postProccesTime) / CLOCKS_PER_MILLISEC);
  arrLen++;

  RedisModule_ReplySetArrayLength(ctx, arrLen);
}

static int searchResultReducer(struct MRCtx *mc, int count, MRReply **replies) {
  clock_t postProccesTime;
  RedisModuleBlockedClient *bc = (RedisModuleBlockedClient *)MRCtx_GetRedisCtx(mc);
  RedisModuleCtx *ctx = RedisModule_GetThreadSafeContext(bc);
  searchRequestCtx *req = MRCtx_GetPrivdata(mc);
  searchReducerCtx rCtx = {NULL};
  int profile = (req->profileArgs > 0);

  int res = REDISMODULE_OK;
  // got no replies - this means timeout
  if (count == 0 || req->limit < 0) {
    res = RedisModule_ReplyWithError(ctx, "Could not send query to cluster");
    goto cleanup;
  }

  if (MRReply_Type(*replies) == MR_REPLY_ERROR) {
    res = MR_ReplyWithMRReply(ctx, *replies);
    goto cleanup;
  }

  rCtx.searchCtx = req;

  // Get reply offsets
  getReplyOffsets(rCtx.searchCtx, &rCtx.offsets);

  // Init results heap.
  size_t num = req->requestedResultsCount;
  rCtx.pq = rm_malloc(heap_sizeof(num));
  heap_init(rCtx.pq, cmp_results, req, num);

  // Default result process and post process operations
  rCtx.processReply = (void (*)(struct redisReply *, struct searchReducerCtx *, RedisModuleCtx *))processSearchReply;
  rCtx.postProcess = (void (*)(struct searchReducerCtx *))noOpPostProcess;


  if(req->specialCases) {
    size_t nSpecialCases = array_len(req->specialCases);
    for(size_t i =0; i < nSpecialCases; i++) {
      if(req->specialCases[i]->specialCaseType == SPECIAL_CASE_KNN) {
        specialCaseCtx* knnCtx = req->specialCases[i];
        rCtx.postProcess = (void (*)(struct searchReducerCtx *))knnPostProcess;
        rCtx.reduceSpecialCaseCtx = knnCtx;
        if(knnCtx->knn.shouldSort) {
          knnCtx->knn.pq = rm_malloc(heap_sizeof(knnCtx->knn.k));
          heap_init(knnCtx->knn.pq, cmp_scored_results, NULL, knnCtx->knn.k);
          rCtx.processReply =(void (*)(struct redisReply *, struct searchReducerCtx *, RedisModuleCtx *))proccessKNNSearchReply;
          rCtx.reduceSpecialCaseCtx = knnCtx;
          break;
        }
      }
    }
  }

  for (int i = 0; i < count; i++) {
    MRReply *reply = (!profile) ? replies[i] : MRReply_ArrayElement(replies[i], 0);
    rCtx.processReply(reply, (struct searchReducerCtx *)&rCtx, ctx);
  }
  if (rCtx.cachedResult) {
    rm_free(rCtx.cachedResult);
  }
  // If we didn't get any results and we got an error - return it.
  // If some shards returned results and some errors - we prefer to show the results we got an not
  // return an error. This might change in the future
  if ((rCtx.totalReplies == 0 && rCtx.lastError != NULL) || rCtx.errorOccured) {
    if (rCtx.lastError) {
      MR_ReplyWithMRReply(ctx, rCtx.lastError);
    } else {
      RedisModule_ReplyWithError(ctx, "could not parse redisearch results");
    }
    goto cleanup;
  }

  if (!profile) {
    sendSearchResults(ctx, &rCtx);
  } else {
    postProccesTime = clock();
    profileSearchReply(ctx, &rCtx, count, replies, req->profileClock, postProccesTime);
  }

cleanup:
  if (rCtx.pq) {
    heap_destroy(rCtx.pq);
  }
  if (rCtx.reduceSpecialCaseCtx &&
      rCtx.reduceSpecialCaseCtx->specialCaseType == SPECIAL_CASE_KNN &&
      rCtx.reduceSpecialCaseCtx->knn.pq) {
    heap_destroy(rCtx.reduceSpecialCaseCtx->knn.pq);
  }

  searchRequestCtx_Free(req);
  RS_CHECK_FUNC(RedisModule_BlockedClientMeasureTimeEnd, bc);
  RedisModule_UnblockClient(bc, mc);
  RedisModule_FreeThreadSafeContext(ctx);
  MR_requestCompleted();
  MRCtx_Free(mc);
  return res;
}

int FirstPartitionCommandHandler(RedisModuleCtx *ctx, RedisModuleString **argv, int argc,
                                 MRReduceFunc reducer, struct MRCtx *mrCtx) {

  MRCommand cmd = MR_NewCommandFromRedisStrings(argc, argv);
  /* Replace our own FT command with _FT. command */
  MRCommand_SetPrefix(&cmd, "_FT");

  /* Rewrite the sharding key based on the partitioning key */
  SearchCluster_RewriteCommandToFirstPartition(GetSearchCluster(), &cmd);

  MR_MapSingle(mrCtx, reducer, cmd);

  return REDISMODULE_OK;
}

int FirstShardCommandHandler(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {

  if (!SearchCluster_Ready(GetSearchCluster())) {
    return RedisModule_ReplyWithError(ctx, CLUSTERDOWN_ERR);
  }

  RS_AutoMemory(ctx);

  struct MRCtx *mrCtx = MR_CreateCtx(ctx, NULL);

  return FirstPartitionCommandHandler(ctx, argv, argc, singleReplyReducer, mrCtx);
}

int SynAddCommandHandler(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  if (argc < 3) {
    return RedisModule_WrongArity(ctx);
  }

  if (!SearchCluster_Ready(GetSearchCluster())) {
    return RedisModule_ReplyWithError(ctx, CLUSTERDOWN_ERR);
  }

  RS_AutoMemory(ctx);

  struct MRCtx *mrCtx = MR_CreateCtx(ctx, NULL);

  // reducer is set here so the client will not be unblocked.
  // we need to send SYNFORCEUPDATE commands to the other
  // shards before unblocking the client.
  MRCtx_SetReduceFunction(mrCtx, synonymUpdateFanOutReducer);

  return FirstPartitionCommandHandler(ctx, argv, argc, synonymAddFailedReducer, mrCtx);
}

/* ft.ADD {index} ... */
int SingleShardCommandHandler(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {

  if (argc < 2) {
    return RedisModule_WrongArity(ctx);
  }
  if (!SearchCluster_Ready(GetSearchCluster())) {
    return RedisModule_ReplyWithError(ctx, CLUSTERDOWN_ERR);
  }
  RS_AutoMemory(ctx);

  MRCommand cmd = MR_NewCommandFromRedisStrings(argc, argv);
  /* Replace our own FT command with _FT. command */
  MRCommand_SetPrefix(&cmd, "_FT");
//  int partPos = MRCommand_GetPartitioningKey(&cmd);
//
//  /* Rewrite the sharding key based on the partitioning key */
//  if (partPos > 0) {
//    SearchCluster_RewriteCommand(GetSearchCluster(), &cmd, partPos);
//  }
//
//  /* Rewrite the partitioning key as well */
//
//  if (MRCommand_GetFlags(&cmd) & MRCommand_MultiKey) {
//    if (partPos > 0) {
//      SearchCluster_RewriteCommandArg(GetSearchCluster(), &cmd, partPos, partPos);
//    }
//  }
  // MRCommand_Print(&cmd);
  MR_MapSingle(MR_CreateCtx(ctx, NULL), singleReplyReducer, cmd);

  return REDISMODULE_OK;
}

/* FT.MGET {idx} {key} ... */
int MGetCommandHandler(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {

  if (argc < 3) {
    return RedisModule_WrongArity(ctx);
  }
  // Check that the cluster state is valid
  if (!SearchCluster_Ready(GetSearchCluster())) {
    return RedisModule_ReplyWithError(ctx, CLUSTERDOWN_ERR);
  }
  RS_AutoMemory(ctx);

  MRCommand cmd = MR_NewCommandFromRedisStrings(argc, argv);
  /* Replace our own FT command with _FT. command */
  MRCommand_SetPrefix(&cmd, "_FT");
//  for (int i = 2; i < argc; i++) {
//    SearchCluster_RewriteCommandArg(GetSearchCluster(), &cmd, i, i);
//  }

  MRCommandGenerator cg = SearchCluster_MultiplexCommand(GetSearchCluster(), &cmd);
  struct MRCtx *mrctx = MR_CreateCtx(ctx, NULL);
  MR_SetCoordinationStrategy(mrctx, MRCluster_MastersOnly | MRCluster_FlatCoordination);
  MR_Map(mrctx, mergeArraysReducer, cg, true);
  cg.Free(cg.ctx);
  return REDISMODULE_OK;
}

int SpellCheckCommandHandler(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  if (argc < 3) {
    return RedisModule_WrongArity(ctx);
  }
  // Check that the cluster state is valid
  if (!SearchCluster_Ready(GetSearchCluster())) {
    return RedisModule_ReplyWithError(ctx, CLUSTERDOWN_ERR);
  }
  RS_AutoMemory(ctx);

  MRCommand cmd = MR_NewCommandFromRedisStrings(argc, argv);
  /* Replace our own FT command with _FT. command */
  MRCommand_SetPrefix(&cmd, "_FT");

  MRCommandGenerator cg = SearchCluster_MultiplexCommand(GetSearchCluster(), &cmd);
  struct MRCtx *mrctx = MR_CreateCtx(ctx, NULL);
  MR_SetCoordinationStrategy(mrctx, MRCluster_MastersOnly | MRCluster_FlatCoordination);
  MR_Map(mrctx, spellCheckReducer, cg, true);
  cg.Free(cg.ctx);
  return REDISMODULE_OK;
}

static int mastersCommandCommon(RedisModuleCtx *ctx, RedisModuleString **argv, int argc,
                                int isSharded) {
  if (argc < 2) {
    return RedisModule_WrongArity(ctx);
  }
  // Check that the cluster state is valid
  if (!SearchCluster_Ready(GetSearchCluster())) {
    return RedisModule_ReplyWithError(ctx, CLUSTERDOWN_ERR);
  }
  RS_AutoMemory(ctx);

  MRCommand cmd = MR_NewCommandFromRedisStrings(argc, argv);
  /* Replace our own FT command with _FT. command */
  MRCommand_SetPrefix(&cmd, "_FT");
  struct MRCtx *mrctx = MR_CreateCtx(ctx, NULL);

  if (isSharded) {
    MRCommandGenerator cg = SearchCluster_MultiplexCommand(GetSearchCluster(), &cmd);
    MR_SetCoordinationStrategy(mrctx, MRCluster_MastersOnly | MRCluster_FlatCoordination);
    MR_Map(mrctx, allOKReducer, cg, true);
    cg.Free(cg.ctx);
  } else {
    MR_Fanout(mrctx, allOKReducer, cmd, true);
  }
  return REDISMODULE_OK;
}

int MastersFanoutCommandHandler(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  return mastersCommandCommon(ctx, argv, argc, 1);
}

static int MastersUnshardedHandler(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  return mastersCommandCommon(ctx, argv, argc, 0);
}

int FanoutCommandHandler(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {

  if (argc < 2) {
    return RedisModule_WrongArity(ctx);
  }

  RS_AutoMemory(ctx);

  MRCommand cmd = MR_NewCommandFromRedisStrings(argc, argv);
  /* Replace our own FT command with _FT. command */
  MRCommand_SetPrefix(&cmd, "_FT");

  MRCommandGenerator cg = SearchCluster_MultiplexCommand(GetSearchCluster(), &cmd);
  MR_Map(MR_CreateCtx(ctx, NULL), allOKReducer, cg, true);
  cg.Free(cg.ctx);
  return REDISMODULE_OK;
}

void RSExecDistAggregate(RedisModuleCtx *ctx, RedisModuleString **argv, int argc,
                         struct ConcurrentCmdCtx *cmdCtx);
static int DIST_AGG_THREADPOOL = -1;

static int DistAggregateCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {

  if (argc < 3) {
    return RedisModule_WrongArity(ctx);
  }
  if (!SearchCluster_Ready(GetSearchCluster())) {
    return RedisModule_ReplyWithError(ctx, CLUSTERDOWN_ERR);
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
  if (!SearchCluster_Ready(GetSearchCluster())) {
    return RedisModule_ReplyWithError(ctx, CLUSTERDOWN_ERR);
  }
  return ConcurrentSearch_HandleRedisCommandEx(DIST_AGG_THREADPOOL, CMDCTX_NO_GIL,
                                               CursorCommandInternal, ctx, argv, argc);
}

int TagValsCommandHandler(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {

  if (argc < 3) {
    return RedisModule_WrongArity(ctx);
  }
  // Check that the cluster state is valid
  if (!SearchCluster_Ready(GetSearchCluster())) {
    return RedisModule_ReplyWithError(ctx, CLUSTERDOWN_ERR);
  }
  RS_AutoMemory(ctx);

  MRCommand cmd = MR_NewCommandFromRedisStrings(argc, argv);
  /* Replace our own FT command with _FT. command */
  MRCommand_SetPrefix(&cmd, "_FT");

  MRCommandGenerator cg = SearchCluster_MultiplexCommand(GetSearchCluster(), &cmd);
  MR_Map(MR_CreateCtx(ctx, NULL), uniqueStringsReducer, cg, true);
  cg.Free(cg.ctx);
  return REDISMODULE_OK;
}

int BroadcastCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {

  if (argc < 2) {
    return RedisModule_WrongArity(ctx);
  }
  // Check that the cluster state is valid
  if (!SearchCluster_Ready(GetSearchCluster())) {
    return RedisModule_ReplyWithError(ctx, CLUSTERDOWN_ERR);
  }
  RS_AutoMemory(ctx);

  MRCommand cmd = MR_NewCommandFromRedisStrings(argc - 1, &argv[1]);
  struct MRCtx *mctx = MR_CreateCtx(ctx, NULL);
  MR_SetCoordinationStrategy(mctx, MRCluster_FlatCoordination);

  if (cmd.num > 1 && MRCommand_GetShardingKey(&cmd) >= 0) {
    MRCommandGenerator cg = SearchCluster_MultiplexCommand(GetSearchCluster(), &cmd);
    MR_Map(mctx, chainReplyReducer, cg, true);
    cg.Free(cg.ctx);
  } else {
    MR_Fanout(mctx, chainReplyReducer, cmd, true);
  }
  return REDISMODULE_OK;
}

int InfoCommandHandler(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  if (argc != 2) {
    // FT.INFO {index}
    return RedisModule_WrongArity(ctx);
  }
  // Check that the cluster state is valid
  if (!SearchCluster_Ready(GetSearchCluster())) {
    return RedisModule_ReplyWithError(ctx, CLUSTERDOWN_ERR);
  }
  RS_AutoMemory(ctx);
  MRCommand cmd = MR_NewCommandFromRedisStrings(argc, argv);
  MRCommand_SetPrefix(&cmd, "_FT");

  struct MRCtx *mctx = MR_CreateCtx(ctx, NULL);
  MRCommandGenerator cg = SearchCluster_MultiplexCommand(GetSearchCluster(), &cmd);
  MR_SetCoordinationStrategy(mctx, MRCluster_FlatCoordination);
  MR_Map(mctx, InfoReplyReducer, cg, true);
  cg.Free(cg.ctx);
  return REDISMODULE_OK;
}

int LocalSearchCommandHandler(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {

  // MR_UpdateTopology(ctx);
  if (argc < 3) {
    return RedisModule_WrongArity(ctx);
  }
  // Check that the cluster state is valid
  if (!SearchCluster_Ready(GetSearchCluster())) {
    return RedisModule_ReplyWithError(ctx, CLUSTERDOWN_ERR);
  }
  RS_AutoMemory(ctx);

  QueryError status = {0};
  searchRequestCtx *req = rscParseRequest(argv, argc, &status);
  if (!req) {
    RedisModule_ReplyWithError(ctx, QueryError_GetError(&status));
    QueryError_ClearError(&status);
    return REDISMODULE_OK;
  }

  MRCommand cmd = MR_NewCommandFromRedisStrings(argc, argv);

  // replace the LIMIT {offset} {limit} with LIMIT 0 {limit}, because we need all top N to merge
  int limitIndex = RMUtil_ArgExists("LIMIT", argv, argc, 3);
  if (limitIndex && req->limit > 0 && limitIndex < argc - 2) {
    MRCommand_ReplaceArg(&cmd, limitIndex + 1, "0", 1);
  }

  /* Replace our own DFT command with FT. command */
  MRCommand_ReplaceArg(&cmd, 0, "_FT.SEARCH", sizeof("_FT.SEARCH") - 1);

  if (req->withSortby) {
    // if sort by requested we adding the WITHSORTKEYS option anyway immediately after the query.
    // Worst case it will appears twice.
    MRCommand_AppendArgsAtPos(&cmd, 3, 1, "WITHSORTKEYS");
  } else {
    // adding the WITHSCORES option anyway immediately after the query if there is no SORTBY.
    // If user added `WITHSCORES`, it will appear twice.
    MRCommand_AppendArgsAtPos(&cmd, 3, 1, "WITHSCORES");
  }

  MRCommandGenerator cg = SearchCluster_MultiplexCommand(GetSearchCluster(), &cmd);
  struct MRCtx *mrctx = MR_CreateCtx(ctx, req);
  // we prefer the next level to be local - we will only approach nodes on our own shard
  // we also ask only masters to serve the request, to avoid duplications by random
  MR_SetCoordinationStrategy(mrctx, MRCluster_LocalCoordination | MRCluster_MastersOnly);

  MR_Map(mrctx, searchResultReducer, cg, true);
  cg.Free(cg.ctx);
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
        req->requiredFields = array_append(req->requiredFields, ctx->sortby.sortKey);
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
        req->requiredFields = array_append(req->requiredFields, ctx->knn.fieldName);
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
    char snum[8] = {0};
    sprintf(snum, "%d", numberOfFields);
    MRCommand_Append(cmd, snum, strlen(snum));
    for(size_t i = 0; i < numberOfFields; i++) {
        MRCommand_Append(cmd, req->requiredFields[i], strlen(req->requiredFields[i]));
    }
  }
}

int FlatSearchCommandHandler(RedisModuleBlockedClient *bc, RedisModuleString **argv, int argc) {
  QueryError status = {0};
  searchRequestCtx *req = rscParseRequest(argv, argc, &status);

  if (!req) {
    RedisModuleCtx* clientCtx = RedisModule_GetThreadSafeContext(bc);
    RedisModule_ReplyWithError(clientCtx, QueryError_GetError(&status));
    QueryError_ClearError(&status);
    RS_CHECK_FUNC(RedisModule_BlockedClientMeasureTimeEnd, bc);
    RedisModule_UnblockClient(bc, NULL);
    RedisModule_FreeThreadSafeContext(clientCtx);
    return REDISMODULE_OK;
  }

  MRCommand cmd = MR_NewCommandFromRedisStrings(argc, argv);

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
    MRCommand_AppendArgsAtPos(&cmd, 3 + req->profileArgs, 1, "WITHSCORES");
  }

  if(req->specialCases) {
    sendRequiredFields(req, &cmd);
  }

  struct MRCtx *mrctx = MR_CreateCtx((RedisModuleCtx *)bc, req);
  // we prefer the next level to be local - we will only approach nodes on our own shard
  // we also ask only masters to serve the request, to avoid duplications by random
  MR_SetCoordinationStrategy(mrctx, MRCluster_FlatCoordination | MRCluster_MastersOnly);

  MRCtx_SetReduceFunction(mrctx, searchResultReducer);
  MR_Fanout(mrctx, NULL, cmd, false);
  return REDISMODULE_OK;
}

typedef struct SearchCmdCtx {
  RedisModuleString **argv;
  int argc;
  RedisModuleBlockedClient* bc;
}SearchCmdCtx;

static void DistSearchCommandHandler(void* pd) {
  SearchCmdCtx* sCmdCtx = pd;
  FlatSearchCommandHandler(sCmdCtx->bc, sCmdCtx->argv, sCmdCtx->argc);
  for (size_t i = 0 ; i < sCmdCtx->argc ; ++i) {\
    RedisModule_FreeString(NULL, sCmdCtx->argv[i]);
  }
  rm_free(sCmdCtx->argv);
  rm_free(sCmdCtx);
}

static int DistSearchCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {

  if (argc < 3) {
    return RedisModule_WrongArity(ctx);
  }
  if (!SearchCluster_Ready(GetSearchCluster())) {
    return RedisModule_ReplyWithError(ctx, CLUSTERDOWN_ERR);
  }
  RedisModuleBlockedClient* bc = RedisModule_BlockClient(ctx, NULL, NULL, NULL, 0);
  SearchCmdCtx* sCmdCtx = rm_malloc(sizeof(*sCmdCtx));
  sCmdCtx->argv = rm_malloc(sizeof(RedisModuleString*) * argc);
  for (size_t i = 0 ; i < argc ; ++i) {
    // We need to copy the argv because it will be freed in the callback (from another thread).
    sCmdCtx->argv[i] = RedisModule_CreateStringFromString(ctx, argv[i]);
  }
  sCmdCtx->argc = argc;
  sCmdCtx->bc = bc;
  RS_CHECK_FUNC(RedisModule_BlockedClientMeasureTimeStart, bc);
  ConcurrentSearch_ThreadPoolRun(DistSearchCommandHandler, sCmdCtx, DIST_AGG_THREADPOOL);

  return REDISMODULE_OK;
}

int ProfileCommandHandler(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  if (argc < 5) {
    return RedisModule_WrongArity(ctx);
  }

  if (RMUtil_ArgExists("WITHCURSOR", argv, argc, 3)) {
    return RedisModule_ReplyWithError(ctx, "FT.PROFILE does not support cursor");
  }

  const char *typeStr = RedisModule_StringPtrLen(argv[2], NULL);
  if (RMUtil_ArgExists("SEARCH", argv, 3, 2)) {
    return DistSearchCommand(ctx, argv, argc);
  }
  if (RMUtil_ArgExists("AGGREGATE", argv, 3, 2)) {
    return DistAggregateCommand(ctx, argv, argc);
  }
  return RedisModule_ReplyWithError(ctx, "No `SEARCH` or `AGGREGATE` provided");
}

int ClusterInfoCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {

  RS_AutoMemory(ctx);

  int n = 0;
  RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);

  RedisModule_ReplyWithSimpleString(ctx, "num_partitions");
  n++;
  RedisModule_ReplyWithLongLong(ctx, GetSearchCluster()->size);
  n++;
  RedisModule_ReplyWithSimpleString(ctx, "cluster_type");
  n++;
  RedisModule_ReplyWithSimpleString(
      ctx, clusterConfig.type == ClusterType_RedisLabs ? "redislabs" : "redis_oss");
  n++;

  // Report hash func
  MRClusterTopology *topo = MR_GetCurrentTopology();
  RedisModule_ReplyWithSimpleString(ctx, "hash_func");
  n++;
  if (topo) {
    RedisModule_ReplyWithSimpleString(
        ctx, topo->hashFunc == MRHashFunc_CRC12
                 ? MRHASHFUNC_CRC12_STR
                 : (topo->hashFunc == MRHashFunc_CRC16 ? MRHASHFUNC_CRC16_STR : "n/a"));
  } else {
    RedisModule_ReplyWithSimpleString(ctx, "n/a");
  }
  n++;

  // Report topology
  RedisModule_ReplyWithSimpleString(ctx, "num_slots");
  n++;
  RedisModule_ReplyWithLongLong(ctx, topo ? (long long)topo->numSlots : 0);
  n++;

  RedisModule_ReplyWithSimpleString(ctx, "slots");
  n++;

  if (!topo) {
    RedisModule_ReplyWithNull(ctx);
    n++;

  } else {

    for (int i = 0; i < topo->numShards; i++) {
      MRClusterShard *sh = &topo->shards[i];
      RedisModule_ReplyWithArray(ctx, 2 + sh->numNodes);
      n++;
      RedisModule_ReplyWithLongLong(ctx, sh->startSlot);
      RedisModule_ReplyWithLongLong(ctx, sh->endSlot);
      for (int j = 0; j < sh->numNodes; j++) {
        MRClusterNode *node = &sh->nodes[j];
        RedisModule_ReplyWithArray(ctx, 4);
        RedisModule_ReplyWithSimpleString(ctx, node->id);
        RedisModule_ReplyWithSimpleString(ctx, node->endpoint.host);
        RedisModule_ReplyWithLongLong(ctx, node->endpoint.port);
        RedisModule_ReplyWithString(
            ctx, RedisModule_CreateStringPrintf(ctx, "%s%s",
                                                node->flags & MRNode_Master ? "master " : "slave ",
                                                node->flags & MRNode_Self ? "self" : ""));
      }
    }
  }

  RedisModule_ReplySetArrayLength(ctx, n);
  return REDISMODULE_OK;
}

int UnsuportedOnCluster(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  return RedisModule_ReplyWithError(ctx, "Command not supported on cluster");
}

// A special command for redis cluster OSS, that refreshes the cluster state
int RefreshClusterCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {

  RS_AutoMemory(ctx);
  MRClusterTopology *topo = RedisCluster_GetTopology(ctx);

  SearchCluster_EnsureSize(ctx, GetSearchCluster(), topo);

  MR_UpdateTopology(topo);
  RedisModule_ReplyWithSimpleString(ctx, "OK");

  return REDISMODULE_OK;
}

int SetClusterCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  RS_AutoMemory(ctx);
  MRClusterTopology *topo = RedisEnterprise_ParseTopology(ctx, argv, argc);
  // this means a parsing error, the parser already sent the explicit error to the client
  if (!topo) {
    return REDISMODULE_ERR;
  }

  SearchCluster_EnsureSize(ctx, GetSearchCluster(), topo);
  // If the cluster hash func or cluster slots has changed, set the new value
  switch (topo->hashFunc) {
    case MRHashFunc_CRC12:
      PartitionCtx_SetSlotTable(&GetSearchCluster()->part, crc12_slot_table,
                                MIN(4096, topo->numSlots));
      break;
    case MRHashFunc_CRC16:
      PartitionCtx_SetSlotTable(&GetSearchCluster()->part, crc16_slot_table,
                                MIN(16384, topo->numSlots));
      break;
    case MRHashFunc_None:
    default:
      // do nothing
      break;
  }

  // send the topology to the cluster
  if (MR_UpdateTopology(topo) != REDISMODULE_OK) {
    // failed update
    MRClusterTopology_Free(topo);
    return RedisModule_ReplyWithError(ctx, "Error updating the topology");
  }

  RedisModule_ReplyWithSimpleString(ctx, "OK");

  return REDISMODULE_OK;
}

/* Perform basic configurations and init all threads and global structures */
static int initSearchCluster(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  RedisModule_Log(ctx, "notice",
                  "Cluster configuration: %ld partitions, type: %d, coordinator timeout: %dms",
                  clusterConfig.numPartitions, clusterConfig.type, clusterConfig.timeoutMS);

  /* Configure cluster injections */
  ShardFunc sf;

  MRClusterTopology *initialTopology = NULL;

  const char **slotTable = NULL;
  size_t tableSize = 0;

  switch (clusterConfig.type) {
    case ClusterType_RedisLabs:
      sf = CRC12ShardFunc;
      slotTable = crc12_slot_table;
      tableSize = 4096;

      break;
    case ClusterType_RedisOSS:
    default:
      // init the redis topology updater loop
      if (InitRedisTopologyUpdater() == REDIS_ERR) {
        RedisModule_Log(ctx, "warning", "Could not init redis cluster topology updater. Aborting");
        return REDISMODULE_ERR;
      }
      sf = CRC16ShardFunc;
      slotTable = crc16_slot_table;
      tableSize = 16384;
  }

  MRCluster *cl = MR_NewCluster(initialTopology, sf, 2);
  MR_Init(cl, clusterConfig.timeoutMS);
  InitGlobalSearchCluster(clusterConfig.numPartitions, slotTable, tableSize);

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

/**
 * Because our indexes are in the form of IDX{something}, a single real index might
 * appear as multiple indexes, using more memory and essentially disabling rate
 * limiting.
 *
 * This works as a hook after every new index is created, to strip the '{' from the
 * cursor name, and use the real name as the entry.
 */
static void addIndexCursor(const IndexSpec *sp) {
  char *s = rm_strdup(sp->name);
  char *end = strchr(s, '{');
  if (end) {
    *end = '\0';
    CursorList_AddSpec(&RSCursorsCoord, s, RSCURSORS_DEFAULT_CAPACITY);
  }
  rm_free(s);
}

#define RM_TRY(expr)                                                  \
  if (expr == REDISMODULE_ERR) {                                      \
    RedisModule_Log(ctx, "warning", "Could not run " __STRING(expr)); \
    return REDISMODULE_ERR;                                           \
  }

static void getRedisVersion() {
  RedisModuleCallReply *reply = RedisModule_Call(RSDummyContext, "info", "c", "server");
  assert(RedisModule_CallReplyType(reply) == REDISMODULE_REPLY_STRING);
  size_t len;
  const char *replyStr = RedisModule_CallReplyStringPtr(reply, &len);

  int n = sscanf(replyStr, "# Server\nredis_version:%d.%d.%d", &redisMajorVesion, &redisMinorVesion,
                 &redisPatchVesion);

  assert(n == 3);

  RedisModule_FreeCallReply(reply);
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


static void Coordinator_CleanupModule(void) {
  MR_Destroy();
  GlobalSearchCluser_Release();
}

void Coordinator_ShutdownEvent(RedisModuleCtx *ctx, RedisModuleEvent eid, uint64_t subevent, void *data) {
  RedisModule_Log(ctx, "notice", "%s", "Begin releasing RediSearch resources on shutdown");
  RediSearch_CleanupModule();
  RedisModule_Log(ctx, "notice", "%s", "End releasing RediSearch resources");
  RedisModule_Log(ctx, "notice", "%s", "Begin releasing Coordinator resources on shutdown");
  Coordinator_CleanupModule();
  RedisModule_Log(ctx, "notice", "%s", "End releasing Coordinator resources");
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
    if (RedisModule_GetDetachedThreadSafeContext) {
      RSDummyContext = RedisModule_GetDetachedThreadSafeContext(ctx);
    } else {
      RSDummyContext = RedisModule_GetThreadSafeContext(NULL);
    }
  }

  getRedisVersion();
  RedisModule_Log(ctx, "notice", "redis version observed by redisearch : %d.%d.%d",
                  redisMajorVesion, redisMinorVesion, redisPatchVesion);

  // Chain the config into RediSearch's global config and set the default values
  clusterConfig = DEFAULT_CLUSTER_CONFIG;
  RSConfigOptions_AddConfigs(&RSGlobalConfigOptions, GetClusterConfigOptions());

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
  DIST_AGG_THREADPOOL = ConcurrentSearch_CreatePool(RSGlobalConfig.coordinatorPoolSize);

  Initialize_CoordKeyspaceNotifications(ctx);

  // suggestion commands
  RM_TRY(RedisModule_CreateCommand(ctx, "FT.SUGADD", SafeCmd(SingleShardCommandHandler), "readonly",
                                     0, 0, -1));
  RM_TRY(RedisModule_CreateCommand(ctx, "FT.SUGGET", SafeCmd(SingleShardCommandHandler), "readonly",
                                   0, 0, -1));
  RM_TRY(RedisModule_CreateCommand(ctx, "FT.SUGDEL", SafeCmd(SingleShardCommandHandler), "readonly",
                                   0, 0, -1));
  RM_TRY(RedisModule_CreateCommand(ctx, "FT.SUGLEN", SafeCmd(SingleShardCommandHandler), "readonly",
                                   0, 0, -1));

  // read commands
  RM_TRY(RedisModule_CreateCommand(ctx, "FT.GET", SafeCmd(SingleShardCommandHandler), "readonly", 0, 0, -1));
  RM_TRY(RedisModule_CreateCommand(ctx, "FT.MGET", SafeCmd(MGetCommandHandler), "readonly", 0, 0, -1));
  RM_TRY(RedisModule_CreateCommand(ctx, "FT.TAGVALS", SafeCmd(TagValsCommandHandler), "readonly", 0, 0, -1));
  RM_TRY(RedisModule_CreateCommand(ctx, "FT.EXPLAIN", SafeCmd(SingleShardCommandHandler), "readonly", 0, 0, -1));
  if (clusterConfig.type == ClusterType_RedisLabs) {
    RM_TRY(RedisModule_CreateCommand(ctx, "FT.AGGREGATE", SafeCmd(DistAggregateCommand), "readonly", 0, 1, -2));
  } else {
    RM_TRY(RedisModule_CreateCommand(ctx, "FT.AGGREGATE", SafeCmd(DistAggregateCommand), "readonly", 0, 0, -1));
  }
  RM_TRY(RedisModule_CreateCommand(ctx, "FT.INFO", SafeCmd(InfoCommandHandler), "readonly", 0, 0, -1));
  RM_TRY(RedisModule_CreateCommand(ctx, "FT.LSEARCH", SafeCmd(LocalSearchCommandHandler), "readonly", 0, 0, -1));
  RM_TRY(RedisModule_CreateCommand(ctx, "FT.FSEARCH", SafeCmd(DistSearchCommand), "readonly", 0, 0, -1));
  RM_TRY(RedisModule_CreateCommand(ctx, "FT.SEARCH", SafeCmd(DistSearchCommand), "readonly", 0, 0, -1));
  RM_TRY(RedisModule_CreateCommand(ctx, "FT.PROFILE", SafeCmd(ProfileCommandHandler), "readonly", 0, 0, -1));
  if (clusterConfig.type == ClusterType_RedisLabs) {
    RM_TRY(RedisModule_CreateCommand(ctx, "FT.CURSOR", SafeCmd(CursorCommand), "readonly", 3, 1, -3));
  } else {
    RM_TRY(RedisModule_CreateCommand(ctx, "FT.CURSOR", SafeCmd(CursorCommand), "readonly", 0, 0, -1));
  }
  RM_TRY(RedisModule_CreateCommand(ctx, "FT.SYNDUMP", SafeCmd(FirstShardCommandHandler), "readonly", 0, 0, -1));
  RM_TRY(RedisModule_CreateCommand(ctx, "FT._LIST", SafeCmd(FirstShardCommandHandler), "readonly",0, 0, -1));
  RM_TRY(RedisModule_CreateCommand(ctx, "FT.DICTDUMP", SafeCmd(FirstShardCommandHandler), "readonly", 0, 0, -1));
  RM_TRY(RedisModule_CreateCommand(ctx, "FT.SPELLCHECK", SafeCmd(SpellCheckCommandHandler), "readonly", 0, 0, -1));

  if (RSBuildType_g == RSBuildType_OSS) {
    RedisModule_Log(ctx, "notice", "Register write commands");
    // write commands (on enterprise we do not define them, the dmc take care of them)
    RM_TRY(RedisModule_CreateCommand(ctx, "FT.ADD", SafeCmd(SingleShardCommandHandler), "readonly", 0, 0, -1));
    RM_TRY(RedisModule_CreateCommand(ctx, "FT.DEL", SafeCmd(SingleShardCommandHandler), "readonly", 0, 0, -1));
    RM_TRY(RedisModule_CreateCommand(ctx, "FT.CREATE", SafeCmd(MastersFanoutCommandHandler), "readonly", 0, 0, -1));
    RM_TRY(RedisModule_CreateCommand(ctx, "FT._CREATEIFNX", SafeCmd(MastersFanoutCommandHandler), "readonly", 0, 0, -1));
    RM_TRY(RedisModule_CreateCommand(ctx, "FT.ALTER", SafeCmd(MastersFanoutCommandHandler), "readonly", 0, 0, -1));
    RM_TRY(RedisModule_CreateCommand(ctx, "FT._ALTERIFNX", SafeCmd(MastersFanoutCommandHandler), "readonly", 0, 0, -1));
    RM_TRY(RedisModule_CreateCommand(ctx, "FT.DROP", SafeCmd(MastersFanoutCommandHandler), "readonly",0, 0, -1));
    RM_TRY(RedisModule_CreateCommand(ctx, "FT._DROPIFX", SafeCmd(MastersFanoutCommandHandler), "readonly",0, 0, -1));
    RM_TRY(RedisModule_CreateCommand(ctx, "FT.DROPINDEX", SafeCmd(MastersFanoutCommandHandler), "readonly",0, 0, -1));
    RM_TRY(RedisModule_CreateCommand(ctx, "FT._DROPINDEXIFX", SafeCmd(MastersFanoutCommandHandler), "readonly",0, 0, -1));
    RM_TRY(RedisModule_CreateCommand(ctx, "FT.DELETE", SafeCmd(MastersFanoutCommandHandler), "readonly",0, 0, -1));
    RM_TRY(RedisModule_CreateCommand(ctx, "FT.BROADCAST", SafeCmd(BroadcastCommand), "readonly", 0, 0, -1));
    RM_TRY(RedisModule_CreateCommand(ctx, "FT.DICTADD", SafeCmd(MastersFanoutCommandHandler), "readonly", 0, 0, -1));
    RM_TRY(RedisModule_CreateCommand(ctx, "FT.DICTDEL", SafeCmd(MastersFanoutCommandHandler), "readonly", 0, 0, -1));
    RM_TRY(RedisModule_CreateCommand(ctx, "FT.ALIASADD", SafeCmd(MastersFanoutCommandHandler), "readonly", 0, 0, -1));
    RM_TRY(RedisModule_CreateCommand(ctx, "FT._ALIASADDIFNX", SafeCmd(MastersFanoutCommandHandler), "readonly", 0, 0, -1));
    RM_TRY(RedisModule_CreateCommand(ctx, "FT.ALIASDEL", SafeCmd(MastersUnshardedHandler), "readonly", 0, 0, -1));
    RM_TRY(RedisModule_CreateCommand(ctx, "FT._ALIASDELIFX", SafeCmd(MastersUnshardedHandler), "readonly", 0, 0, -1));
    RM_TRY(RedisModule_CreateCommand(ctx, "FT.ALIASUPDATE", SafeCmd(MastersFanoutCommandHandler), "readonly", 0, 0, -1));
    // todo : how to handle those
    RM_TRY(RedisModule_CreateCommand(ctx, "FT.SYNADD", SafeCmd(SynAddCommandHandler), "readonly", 0, 0, -1));
    RM_TRY(RedisModule_CreateCommand(ctx, "FT.SYNUPDATE", SafeCmd(MastersFanoutCommandHandler),"readonly", 0, 0, -1));
    RM_TRY(RedisModule_CreateCommand(ctx, "FT.SYNFORCEUPDATE", SafeCmd(MastersFanoutCommandHandler),"readonly", 0, 0, -1));
  }

  // cluster set commands
  RM_TRY(RedisModule_CreateCommand(ctx, REDISEARCH_MODULE_NAME".CLUSTERSET", SafeCmd(SetClusterCommand), "readonly allow-loading", 0,0, -1));
  RM_TRY(RedisModule_CreateCommand(ctx, REDISEARCH_MODULE_NAME".CLUSTERREFRESH", SafeCmd(RefreshClusterCommand),"readonly", 0, 0, -1));
  RM_TRY(RedisModule_CreateCommand(ctx, REDISEARCH_MODULE_NAME".CLUSTERINFO", SafeCmd(ClusterInfoCommand), "readonly allow-loading",0, 0, -1));

  return REDISMODULE_OK;
}
