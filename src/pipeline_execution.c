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
#ifdef ENABLE_ASSERT
#include "debug_commands.h"
#endif

struct PipelineExecution {
  pthread_mutex_t gate;
  const QueryRequestTimeout *timeout;
  QueryProcessingCtx *context;
};

struct PipelineAccess {
  PipelineExecution *execution;
  bool canWait;
  bool ownsExecution;
};

PipelineExecution *PipelineExecution_New(const QueryRequestTimeout *timeout) {
  RS_ASSERT(timeout && timeout->kind == QUERY_REQUEST_TIMEOUT_BLOCKED_CLIENT &&
            timeout->config.timeoutPolicy == TimeoutPolicy_ReturnStrict);
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
  PipelineAccess access = {.execution = execution, .canWait = true, .ownsExecution = true};
  if (execution->context) execution->context->executionAccess = &access;
  step(&access, data);
  if (!access.ownsExecution) return false;
  leave(execution);
  return true;
}

void PipelineExecution_RunDrain(PipelineExecution *execution, PipelineExecutionStep step,
                                void *data) {
  RS_ASSERT(QueryRequestTimeout_IsBlockedClientTimedOut(execution->timeout));
  int rc = pthread_mutex_lock(&execution->gate);
  RS_ASSERT_ALWAYS(rc == 0);
  PipelineAccess access = {.execution = execution, .ownsExecution = true};
  if (execution->context) execution->context->executionAccess = &access;
  if (execution->context && execution->context->isProfile) {
    Profile_ResumeRPs(execution->context);
  }
  step(&access, data);
  leave(execution);
}

void PipelineAccess_Publish(PipelineAccess *access, QueryProcessingCtx *ctx) {
  RS_ASSERT(access->ownsExecution && access->canWait && ctx);
  RS_ASSERT(!access->execution->context || access->execution->context == ctx);
  RS_ASSERT(!ctx->executionAccess || ctx->executionAccess == access);
  access->execution->context = ctx;
  ctx->executionAccess = access;
}

QueryProcessingCtx *PipelineAccess_Context(PipelineAccess *access) {
  RS_ASSERT(access->ownsExecution);
  return access->execution->context;
}

void PipelineAccess_ReleaseForWait(PipelineAccess *access) {
  RS_ASSERT(access->ownsExecution && access->canWait);
  access->ownsExecution = false;
  leave(access->execution);
}

bool PipelineAccess_ResumeAfterWait(PipelineAccess *access) {
  RS_ASSERT(!access->ownsExecution && access->canWait);
  if (!enterNext(access->execution)) return false;
  access->ownsExecution = true;
  if (access->execution->context) access->execution->context->executionAccess = access;
  return true;
}

bool PipelineAccess_IsOwned(const PipelineAccess *access) {
  return !access || access->ownsExecution;
}

#ifdef ENABLE_ASSERT
bool PipelineAccess_DebugPause(PipelineAccess *access, const char *point) {
  if (!access || !SyncPoint_IsArmed(point)) return false;
  PipelineAccess_ReleaseForWait(access);
  SyncPoint_Wait(point);
  return !PipelineAccess_ResumeAfterWait(access);
}
#endif
