/*
 * Copyright 2018-2022 Redis Labs Ltd. and Contributors
 *
 * This file is available under the Redis Labs Source Available License Agreement
 */

#include "threadpool_api.h"
#include "workers_pool.h"
#include "VecSim/vec_sim_common.h"
#include "rmalloc.h"

static void ThreadPoolAPI_Execute(void *ctx) {
  ThreadPoolAPI_job *job = ctx;
  StrongRef spec_ref = WeakRef_Promote(job->spec_ref);

  // If the spec is still alive, execute the callback
  if (StrongRef_Get(spec_ref)) {
    job->cb(job->arg);
    StrongRef_Release(spec_ref);
  }

  // Free the job
  WeakRef_Release(job->spec_ref);
  job->free_cb(job->arg);
  rm_free(job);
}

// For now, we assume that all the jobs that are submitted are low priority jobs (not blocking any client).
// We can add the priority to the `spec_ctx` (and rename it) if needed.
int ThreadPoolAPI_SubmitJobs(void *pool, void *spec_ctx, void **ext_jobs,
                                                         ThreadPoolAPI_CB *cbs,
                                                         ThreadPoolAPI_CB *free_cbs, size_t n_jobs) {
  WeakRef spec_ref = {spec_ctx};

  redisearch_thpool_work_t jobs[n_jobs];
  for (size_t i = 0; i < n_jobs; i++) {
    ThreadPoolAPI_job *job = rm_new(ThreadPoolAPI_job);
    job->spec_ref = WeakRef_Clone(spec_ref);
    job->cb = cbs[i];
    job->free_cb = free_cbs[i];
    job->arg = ext_jobs[i];

    jobs[i].arg_p = job;
    jobs[i].function_p = ThreadPoolAPI_Execute;
  }

  if (redisearch_thpool_add_n_work(pool, jobs, n_jobs, THPOOL_PRIORITY_LOW) == -1) {
    // Failed to add jobs to the thread pool, free all the jobs
    for (size_t i = 0; i < n_jobs; i++) {
      ThreadPoolAPI_job *job = jobs[i].arg_p;
      WeakRef_Release(job->spec_ref);
      job->free_cb(job->arg);
      rm_free(job);
    }
    return REDISMODULE_ERR;
  }
  return REDISMODULE_OK;
}
