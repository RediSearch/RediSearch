/*
 * Copyright 2018-2022 Redis Labs Ltd. and Contributors
 *
 * This file is available under the Redis Labs Source Available License Agreement
 */
#pragma once
#include "deps/thpool/thpool.h"
#include "references.h"

typedef void (*ThreadPoolAPI_CB)(void *);
typedef struct ThreadPoolAPI_AsyncIndexJob {
  WeakRef spec_ref;             // A reference to the associated spec of the job
  ThreadPoolAPI_CB cb;          // callback to execute (gets the external job context)
  void *arg;                    // The external job context
} ThreadPoolAPI_AsyncIndexJob;

int ThreadPoolAPI_SubmitIndexJobs(void *pool, void *spec_ctx, void **ext_jobs,
                                                         ThreadPoolAPI_CB *cbs,
                                                         size_t n_jobs);

void RS_Threadpools_PauseBeforeDump();
void RS_Threadpools_Resume();
