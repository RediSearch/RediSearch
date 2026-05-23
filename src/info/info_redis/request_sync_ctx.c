/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "aggregate/aggregate.h"
#include "coord/rmr/chan.h"
#include "cursor.h"
#include "hybrid/hybrid_request.h"
#include "info/info_redis/types/blocked_queries.h"
#include "search_result_ffi.h"

void ChunkReplyState_Destroy(ChunkReplyState *state) {
  // Free any stored results that weren't consumed
  // (e.g., if timeout occurred before reply_callback ran)
  if (state->results) {
    for (size_t i = 0; i < array_len(state->results); i++) {
      SearchResult_Destroy(state->results[i]);
      rm_free(state->results[i]);
    }
    array_free(state->results);
    state->results = NULL;
  }

  // Timeout edge case: cursor wasn't handled by reply_callback.
  // See ChunkReplyState ownership model in aggregate.h for full explanation.
  // The RSC free path owns the query, so clear cursor->query before freeing
  // the cursor to avoid re-entering RequestSyncCtx_Free from Cursor_Free.
  if (state->cursor) {
    state->cursor->query = NULL;
    Cursor_Free(state->cursor);
    state->cursor = NULL;
  }

  // Clear stored error state
  QueryError_ClearError(&state->err);
}

RequestSyncCtx *RequestSyncCtx_NewAREQ(AREQ *areq) {
  RequestSyncCtx *ctx = rm_new(RequestSyncCtx);
  RequestSyncCtx_Init(ctx, REQUEST_KIND_AREQ, areq);
  return ctx;
}

RequestSyncCtx *RequestSyncCtx_NewHybrid(HybridRequest *hreq) {
  RequestSyncCtx *ctx = rm_new(RequestSyncCtx);
  RequestSyncCtx_Init(ctx, REQUEST_KIND_HYBRID, hreq);
  return ctx;
}

void RequestSyncCtx_Free(RequestSyncCtx *ctx) {
  if (!ctx) {
    return;
  }
  ChunkReplyState_Destroy(&ctx->reply);
  if (ctx->kind == REQUEST_KIND_AREQ) {
    AREQ_FreeFromRequestSyncCtx(ctx->query.areq);
  } else {
    HybridRequest_Free(ctx->query.hreq);
  }
  RequestSyncCtx_Destroy(ctx);
  rm_free(ctx);
}

void RSC_BeginCycle(RequestSyncCtx *ctx, RedisModuleBlockedClient *bc,
                    RedisModuleCmdFunc replyCallback, RequestCycleKind cycleKind,
                    uint64_t cursorId, size_t cursorCount) {
  ctx->bc = bc;
  ctx->replyCallback = replyCallback;
  ctx->cycleKind = cycleKind;
  ctx->cycleStart = time(NULL);
  ctx->cycleCursorId = cursorId;
  ctx->cycleCursorCount = cursorCount;
  ctx->blockedNodeOwns = cycleKind == REQUEST_CYCLE_QUERY;
}

void RSC_EndCycle(RequestSyncCtx *ctx) {
  if (!ctx) {
    return;
  }

  if (ctx->cycleKind != REQUEST_CYCLE_NONE) {
    BlockedQueries_Unlink(ctx);
  }
  ChunkReplyState_Destroy(&ctx->reply);
  ctx->reply = (ChunkReplyState){0};
  ctx->reply.err = QueryError_Default();

  ctx->bc = NULL;
  ctx->replyCallback = NULL;
  ctx->cycleKind = REQUEST_CYCLE_NONE;
  ctx->cycleStart = 0;
  ctx->cycleCursorId = 0;
  ctx->cycleCursorCount = 0;
}

void RequestSyncCtx_OnFree(RedisModuleCtx *ctx, void *privdata) {
  UNUSED(ctx);
  RequestSyncCtx *rsc = privdata;
  RequestCycleKind cycleKind = rsc->cycleKind;

  if (cycleKind == REQUEST_CYCLE_CURSOR) {
    AREQ *req = RequestSyncCtx_GetAREQ(rsc);
    AREQ_CleanUpStoredCursor(req);
    RSC_EndCycle(rsc);
    return;
  }

  bool keepQuery = false;
  if (rsc->kind == REQUEST_KIND_AREQ) {
    AREQ *req = RequestSyncCtx_GetAREQ(rsc);
    keepQuery = req && (AREQ_RequestFlags(req) & QEXEC_F_IS_CURSOR) &&
        req->cursor_id && !(req->stateflags & QEXEC_S_ITERDONE);
  } else {
    HybridRequest *hreq = RequestSyncCtx_GetHybridRequest(rsc);
    if (hreq) {
      for (size_t i = 0; i < hreq->nrequests; i++) {
        if (hreq->requests[i]->cursor_id != 0) {
          keepQuery = true;
          break;
        }
      }
    }
  }

  RSC_EndCycle(rsc);
  if (keepQuery) {
    rsc->blockedNodeOwns = false;
    return;
  }
  RequestSyncCtx_Free(rsc);
}

AREQ *RequestSyncCtx_GetAREQ(RequestSyncCtx *ctx) {
  return (ctx && ctx->kind == REQUEST_KIND_AREQ) ? ctx->query.areq : NULL;
}

HybridRequest *RequestSyncCtx_GetHybridRequest(RequestSyncCtx *ctx) {
  return (ctx && ctx->kind == REQUEST_KIND_HYBRID) ? ctx->query.hreq : NULL;
}

AREQ *RequestSyncCtx_GetCursorAREQ(RequestSyncCtx *ctx, uint64_t cursorId) {
  AREQ *areq = RequestSyncCtx_GetAREQ(ctx);
  if (areq) {
    return areq;
  }

  HybridRequest *hreq = RequestSyncCtx_GetHybridRequest(ctx);
  if (!hreq) {
    return NULL;
  }

  for (size_t i = 0; i < hreq->nrequests; i++) {
    AREQ *subquery = hreq->requests[i];
    if (subquery->cursor_id == cursorId) {
      return subquery;
    }
  }
  return NULL;
}

bool RequestSyncCtx_HasReplyCallback(RequestSyncCtx *ctx) {
  return ctx && ctx->replyCallback != NULL;
}

static int RequestSyncCtx_LegacyDeferredReplyMarker(RedisModuleCtx *ctx,
                                                      RedisModuleString **argv, int argc) {
  UNUSED(ctx);
  UNUSED(argv);
  UNUSED(argc);
  RS_LOG_ASSERT(false, "legacy reply callback sentinel must never be invoked");
  return REDISMODULE_OK;
}

void RequestSyncCtx_SetLegacyDeferredReplyMode(RequestSyncCtx *ctx, bool deferredReply) {
  if (!ctx) {
    return;
  }
  // Temporary coordinator bridge until Step 5 routes coord blocked clients
  // through RSC_BeginCycle with the real reply callback.
  ctx->replyCallback = deferredReply ? RequestSyncCtx_LegacyDeferredReplyMarker : NULL;
}

ChunkReplyState *RequestSyncCtx_GetReplyState(RequestSyncCtx *ctx) {
  return ctx ? &ctx->reply : NULL;
}

void RequestSyncCtx_RegisterAbortWakeChannel(RequestSyncCtx *ctx, struct MRChannel *chan) {
  pthread_mutex_lock(&ctx->abortWakeLock);
  ctx->abortWakeChannel = chan;
  pthread_mutex_unlock(&ctx->abortWakeLock);
}

void RequestSyncCtx_UnregisterAbortWakeChannel(RequestSyncCtx *ctx) {
  pthread_mutex_lock(&ctx->abortWakeLock);
  ctx->abortWakeChannel = NULL;
  pthread_mutex_unlock(&ctx->abortWakeLock);
}

void RequestSyncCtx_WakeAbortChannel(RequestSyncCtx *ctx) {
  pthread_mutex_lock(&ctx->abortWakeLock);
  if (ctx->abortWakeChannel) {
    MRChannel_WakeAbort(ctx->abortWakeChannel);
  }
  pthread_mutex_unlock(&ctx->abortWakeLock);
}
