/*
 * Copyright 2018-2022 Redis Labs Ltd. and Contributors
 *
 * This file is available under the Redis Labs Source Available License Agreement
 */
#pragma once
#include "deps/thpool/thpool.h"
#include "references.h"

typedef void (*ThreadPoolAPI_CB)(void *);
typedef struct ThreadPoolAPI_job {
  WeakRef spec_ref;
  ThreadPoolAPI_CB cb;
  ThreadPoolAPI_CB free_cb;
  void *arg;
} ThreadPoolAPI_job;

int ThreadPoolAPI_SubmitJobs(void *pool, void *spec_ctx, void **ext_jobs,
                                                         ThreadPoolAPI_CB *cbs,
                                                         ThreadPoolAPI_CB* free_cbs, size_t n_jobs);
