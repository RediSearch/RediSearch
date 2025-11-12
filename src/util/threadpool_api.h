/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
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
