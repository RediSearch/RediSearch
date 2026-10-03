/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#include "pipeline_execution.h"
#include "query_request.h"
#include "rmalloc.h"
#include <errno.h>
#include <pthread.h>

struct PipelineExecution {
  pthread_mutex_t gate;
  const QueryRequestTimeout *timeout;
  QueryProcessingCtx *context;
};

struct PipelineAccess {
  PipelineExecution *execution;
  PipelinePending pending;
  bool canSuspend;
};

PipelineExecution *PipelineExecution_New(const QueryRequestTimeout *timeout) {
  RS_ASSERT(timeout && timeout->kind == QUERY_REQUEST_TIMEOUT_BLOCKED_CLIENT &&
            timeout->policy == TimeoutPolicy_ReturnStrict);
  PipelineExecution *execution = rm_calloc(1, sizeof(*execution));
  int rc = pthread_mutex_init(&execution->gate, NULL);
  RS_ASSERT_ALWAYS(rc == 0);
  execution->timeout = timeout;
  return execution;
}

void PipelineExecution_Free(PipelineExecution *execution) {
  if (!execution) return;
  int rc = pthread_mutex_destroy(&execution->gate);
  RS_ASSERT_ALWAYS(rc == 0);
  rm_free(execution);
}

static bool enterNext(PipelineExecution *execution) {
  if (QueryRequestTimeout_IsBlockedClientTimedOut(execution->timeout)) return false;
  int rc = pthread_mutex_trylock(&execution->gate);
  if (rc == EBUSY) return false;
  RS_ASSERT_ALWAYS(rc == 0);
  // Main may already have drained and released the gate. Its prior timeout store
  // is visible after acquisition, even though the timeout flag uses relaxed loads.
  if (QueryRequestTimeout_IsBlockedClientTimedOut(execution->timeout)) {
    rc = pthread_mutex_unlock(&execution->gate);
    RS_ASSERT_ALWAYS(rc == 0);
    return false;
  }
  return true;
}

static void leave(PipelineExecution *execution) {
  if (execution->context) execution->context->executionAccess = NULL;
  int rc = pthread_mutex_unlock(&execution->gate);
  RS_ASSERT_ALWAYS(rc == 0);
}

bool PipelineExecution_RunNext(PipelineExecution *execution, PipelineExecutionStep step,
                               void *data) {
  if (!enterNext(execution)) return false;
  PipelineAccess access = {.execution = execution, .canSuspend = true};
  for (;;) {
    if (execution->context) execution->context->executionAccess = &access;
    step(&access, data);
    PipelinePending pending = access.pending;
    access.pending = (PipelinePending){0};
    leave(execution);
    if (!pending.wait) return true;

    if (QueryRequestTimeout_IsBlockedClientTimedOut(execution->timeout)) {
      pending.destroy(pending.data);
      return false;
    }
    pending.wait(pending.data);
    if (!enterNext(execution)) {
      pending.destroy(pending.data);
      return false;
    }
    // A completion commits while exclusively owned, but cannot itself suspend:
    // the subsequent step is responsible for preparing any further wait.
    access.canSuspend = false;
    if (execution->context) execution->context->executionAccess = &access;
    pending.resume(&access, pending.data);
    pending.destroy(pending.data);
    if (execution->context && execution->context->isProfile) {
      Profile_ResumeRPs(execution->context);
    }
    access.canSuspend = true;
  }
}

void PipelineExecution_RunDrain(PipelineExecution *execution, PipelineExecutionStep step,
                                void *data) {
  RS_ASSERT(QueryRequestTimeout_IsBlockedClientTimedOut(execution->timeout));
  int rc = pthread_mutex_lock(&execution->gate);
  RS_ASSERT_ALWAYS(rc == 0);
  PipelineAccess access = {.execution = execution};
  if (execution->context) execution->context->executionAccess = &access;
  if (execution->context && execution->context->isProfile) {
    Profile_ResumeRPs(execution->context);
  }
  step(&access, data);
  leave(execution);
}

void PipelineAccess_Publish(PipelineAccess *access, QueryProcessingCtx *ctx) {
  RS_ASSERT(access->canSuspend && ctx);
  RS_ASSERT(!access->execution->context || access->execution->context == ctx);
  RS_ASSERT(!ctx->executionAccess || ctx->executionAccess == access);
  access->execution->context = ctx;
  ctx->executionAccess = access;
}

QueryProcessingCtx *PipelineAccess_Context(PipelineAccess *access) {
  return access->execution->context;
}

void PipelineAccess_Suspend(PipelineAccess *access, PipelinePending pending) {
  RS_ASSERT(access->canSuspend && !access->pending.wait);
  RS_ASSERT(pending.wait && pending.resume && pending.destroy);
  access->pending = pending;
}
