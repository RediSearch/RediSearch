/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#include "info/info_redis/types/blocked_queries.h"
#include "aggregate/aggregate.h"
#include "hybrid/hybrid_request.h"
#include "rmutil/rm_assert.h"
#include "redismodule.h"
#include <inttypes.h>

static RedisSearchCtx *RSC_GetSearchCtx(RequestSyncCtx *rsc) {
  if (rsc->kind == REQUEST_KIND_AREQ) {
    AREQ *req = RequestSyncCtx_GetAREQ(rsc);
    return req ? AREQ_SearchCtx(req) : NULL;
  }
  HybridRequest *hreq = RequestSyncCtx_GetHybridRequest(rsc);
  return hreq ? hreq->sctx : NULL;
}

static const char *RSC_GetQueryString(RequestSyncCtx *rsc) {
  AREQ *req = rsc->cycleKind == REQUEST_CYCLE_CURSOR
      ? RequestSyncCtx_GetCursorAREQ(rsc, rsc->cycleCursorId)
      : RequestSyncCtx_GetAREQ(rsc);
  if (req) {
    return req->query;
  }

  HybridRequest *hreq = RequestSyncCtx_GetHybridRequest(rsc);
  if (hreq && hreq->nrequests > 0) {
    return hreq->requests[0]->query;
  }
  return NULL;
}

BlockedQueries *BlockedQueries_Init() {
  BlockedQueries* blockedQueries = rm_calloc(1, sizeof(BlockedQueries));
  dllist_init(&blockedQueries->queries);
  dllist_init(&blockedQueries->cursors);
  return blockedQueries;
}

static size_t PrintActiveQueries(BlockedQueries *blockedQueries) {
  size_t count = 0;
  DLLIST_FOREACH(node, &blockedQueries->queries) {
    RequestSyncCtx *rsc = DLLIST_ITEM(node, RequestSyncCtx, blockedNode);
    RedisSearchCtx *sctx = RSC_GetSearchCtx(rsc);
    IndexSpec *sp = sctx ? sctx->spec : NULL;
    ++count; // increment regardless if sp is valid, the fact we have a valid node is problematic
    const char *indexName = sp ? IndexSpec_FormatName(sp, RSGlobalConfig.hideUserDataFromLog) : "<DELETED>";
    const char *query = RSC_GetQueryString(rsc);
    query = query && !RSGlobalConfig.hideUserDataFromLog ? query : "n/a";
    RedisModule_Log(NULL, "warning", "Active query on index %s, query: %s, started at %ld", indexName, query, rsc->cycleStart);
  }
  return count;
}

static size_t PrintActiveCursors(BlockedQueries *blockedQueries) {
  size_t count = 0;
  DLLIST_FOREACH(node, &blockedQueries->cursors) {
    RequestSyncCtx *rsc = DLLIST_ITEM(node, RequestSyncCtx, blockedNode);
    RedisSearchCtx *sctx = RSC_GetSearchCtx(rsc);
    IndexSpec *sp = sctx ? sctx->spec : NULL;
    ++count; // increment regardless if sp is valid, the fact we have a valid node is problematic
    const char *indexName = sp ? IndexSpec_FormatName(sp, RSGlobalConfig.hideUserDataFromLog) : "<DELETED>";
    const char *query = RSC_GetQueryString(rsc);
    query = query && !RSGlobalConfig.hideUserDataFromLog ? query : "n/a";
    RedisModule_Log(NULL, "warning", "Active cursor %" PRIu64 ", on index %s, query: %s, started at %ld", rsc->cycleCursorId, indexName, query, rsc->cycleStart);
  }
  return count;
}

void BlockedQueries_Free(BlockedQueries *blockedQueries) {
  const size_t numQueries = PrintActiveQueries(blockedQueries);
  const size_t numCursors = PrintActiveCursors(blockedQueries);
  RS_LOG_ASSERT_FMT(numQueries == 0 && numCursors == 0,
    "There are %zu active queries and %zu active cursors. This is a bug. Please report it to https://github.com/RediSearch/RediSearch/issues",
    numQueries, numCursors);
  rm_free(blockedQueries);
}

void BlockedQueries_LinkQuery(BlockedQueries *blockedQueries, RequestSyncCtx *rsc) {
  dllist_prepend(&blockedQueries->queries, &rsc->blockedNode);
}

void BlockedQueries_LinkCursor(BlockedQueries *blockedQueries, RequestSyncCtx *rsc) {
  dllist_prepend(&blockedQueries->cursors, &rsc->blockedNode);
}

void BlockedQueries_Unlink(RequestSyncCtx *rsc) {
  dllist_delete(&rsc->blockedNode);
}
