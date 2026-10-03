/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#ifndef PIPELINE_EXECUTION_H__
#define PIPELINE_EXECUTION_H__

#include "result_processor.h"

#ifdef __cplusplus
extern "C" {
#endif

struct QueryRequestTimeout;
typedef struct PipelineExecution PipelineExecution;
typedef struct PipelineAccess PipelineAccess;

// Access exists only during a driver callback. Neither it nor mutable borrows derived
// from it may survive that callback. RPs reborrow the same access, without relocking.
typedef void (*PipelineExecutionStep)(PipelineAccess *access, void *data);

typedef struct PipelinePending {
  void *data;
  // Runs after every pipeline callback/borrow has returned. May block indefinitely.
  void (*wait)(void *data);
  // Called with ownership only if admission succeeds after wait. Never called on timeout.
  void (*resume)(PipelineAccess *access, void *data);
  // Releases private resources, including external locks not transferred by resume.
  // Must also accept cancellation before wait runs. Cannot touch pipeline state.
  void (*destroy)(void *data);
} PipelinePending;

// STRICT-only domain. Create before dispatch, and destroy only after all job borrowers
// end. The timeout object is borrowed for that lifetime and remains a blocked-client source.
PipelineExecution *PipelineExecution_New(const struct QueryRequestTimeout *timeout);
void PipelineExecution_Free(PipelineExecution *execution);

// One callback invocation is an exclusive segment, usually an entire Next loop.
// Suspend schedules a private wait after the callback returns; a successful resume
// restarts step. False means admission was lost; either return relinquishes access.
bool PipelineExecution_RunNext(PipelineExecution *execution, PipelineExecutionStep step,
                               void *data);

// Main sets the existing timeout flag before entry. Waits only for an active segment,
// never for the private wait or a queued worker. Callback owns recovery and reply access.
void PipelineExecution_RunDrain(PipelineExecution *execution, PipelineExecutionStep step,
                                void *data);

// Construction can finish outside the domain while its context is unpublished. Publish
// only a fully initialized pipeline, under admitted execution. NULL means not ready yet.
void PipelineAccess_Publish(PipelineAccess *access, QueryProcessingCtx *ctx);
QueryProcessingCtx *PipelineAccess_Context(PipelineAccess *access);

// Transfers the pending operation to the driver. Caller must unwind the entire Next
// chain immediately with RS_RESULT_SUSPENDED. Only one suspension may be prepared
// by a segment. RPs borrow access from their parent's executionAccess field.
void PipelineAccess_Suspend(PipelineAccess *access, PipelinePending pending);

#ifdef __cplusplus
}
#endif
#endif
