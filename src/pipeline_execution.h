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

// STRICT-only domain. Create before dispatch, and destroy only after all job borrowers
// end. The timeout object is borrowed for that lifetime and remains a blocked-client source.
PipelineExecution *PipelineExecution_New(const struct QueryRequestTimeout *timeout);
void PipelineExecution_Free(PipelineExecution *execution);

// Calls step once under exclusive ownership, usually for an entire Next loop.
// A blocking site releases/reacquires access locally without restarting step.
// False means admission was lost; either return relinquishes access.
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

// Scoped C wait boundary. Before release, publish consistent drainable state and
// capture all wait/cleanup resources privately. Neither this frame nor its callers
// may retain Rust references conflicting with recovery. While released, do not
// access the pipeline. Resume checks timeout even when the worker acquired the GIL:
// main may already have replied before that acquisition.
void PipelineAccess_ReleaseForWait(PipelineAccess *access);
// False leaves ownership released. Clean up private resources and fold with
// RS_RESULT_TIMEDOUT; every surviving caller must avoid pipeline reads/writes.
bool PipelineAccess_ResumeAfterWait(PipelineAccess *access);
// Reads only worker-private bookkeeping, including after denied readmission.
// NULL denotes a legacy caller with no ownership handoff.
bool PipelineAccess_IsOwned(const PipelineAccess *access);

#ifdef ENABLE_ASSERT
// Named debug waits release ownership locally. `point` is a static sync-point
// name; true means readmission failed and requires immediate TIMEDOUT folding.
bool PipelineAccess_DebugPause(PipelineAccess *access, const char *point);
#endif

#ifdef __cplusplus
}
#endif
#endif
