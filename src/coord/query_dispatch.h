/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#ifndef QUERY_DISPATCH_H__
#define QUERY_DISPATCH_H__

#include "query_request.h"
#include "redismodule.h"
#include "rmalloc.h"
#include "util/references.h"

/* Initial coordinator job only. The blocked-client cycle owns request and its
 * argv; deferred execution takes bc (and, for WITHCOUNT, spec_ref) before this
 * job is destroyed. No request access is allowed after that handoff. */
typedef struct {
  QueryRequest *request;
  RedisModuleBlockedClient *bc;
  WeakRef spec_ref;
  size_t numShards;
} DistQueryDispatchCtx;

static inline void DistQueryDispatchCtx_Finish(DistQueryDispatchCtx *dispatch,
                                               RedisModuleCtx *ctx) {
  RedisModule_FreeThreadSafeContext(ctx);
  if (dispatch->spec_ref.rm) {
    WeakRef_Release(dispatch->spec_ref);
  }
  if (dispatch->bc) {
    RedisModule_BlockedClientMeasureTimeEnd(dispatch->bc);
    RedisModule_UnblockClient(dispatch->bc, dispatch->request);
  }
  rm_free(dispatch);
}

#endif
