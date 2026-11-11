/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include <stdlib.h>
#include <unistd.h>
#include <sys/wait.h>
#include <sys/resource.h>
#include <poll.h>
#include <errno.h>
#include <time.h>

#include "fork_cycle.h"
#include "gc.h"
#include "index_gc_ffi.h"
#include "time_sample.h"
#include "module.h"
#include "info/global_stats.h"
#include "util/redis_mem_info.h"
#include "util/timeout.h"
#include "config.h"
#include "debug_commands.h"
#include "redismodule.h"
#include "spec.h"
#include "util/references.h"
#include "vector_index.h"

#define GC_WRITERFD 1
#define GC_READERFD 0
// Number of attempts to wait for the child to exit gracefully before trying to terminate it
#define GC_WAIT_ATTEMPTS 4

// GIL must be held before calling this function
static inline bool isOutOfMemory(RedisModuleCtx *ctx) {
  // Check if we are a slave/replica
  bool isSlave = RedisModule_GetContextFlags(ctx) & REDISMODULE_CTX_FLAGS_SLAVE;
  float used_memory_ratio = 0;
  if (!isSlave) {
    // On master, use the original unified logic
    used_memory_ratio = RedisMemory_GetUsedMemoryRatioUnified(ctx);
  } else {
    // On slaves, only consider max_process_mem
    RedisModuleServerInfoData *info = RedisModule_GetServerInfo(ctx, "memory");
    size_t used_memory = RedisModule_ServerInfoGetFieldUnsigned(info, "used_memory", NULL);
    size_t max_process_mem = RedisModule_ServerInfoGetFieldUnsigned(info, "max_process_mem", NULL);
    RedisModule_FreeServerInfo(ctx, info);

    used_memory_ratio = max_process_mem ? (float)used_memory / (float)max_process_mem : 0;
  }
  RedisModule_Log(ctx, "debug", "IndexGC - used memory ratio: %f", used_memory_ratio);

  return used_memory_ratio > 1;
}

// Waits up to timeout_sec for cpid to be reaped, polling every 1.5ms (via nanosleep). Called when
// KillForkChild was a no-op, meaning Redis never waited on this pid.
static void reap_child_blocking(RedisModuleCtx *ctx, pid_t cpid, int timeout_sec) {
  struct timespec deadline;
  clock_gettime(CLOCK_MONOTONIC_RAW, &deadline);
  deadline.tv_sec += timeout_sec;

  struct timespec poll_interval = {.tv_sec = 0, .tv_nsec = 1500000};
  while (waitpid(cpid, NULL, WNOHANG) == 0) {
    if (TimedOut(&deadline)) {
      RedisModule_Log(ctx, "warning", "IndexGC - timed out waiting for child %d to exit", cpid);
      break;
    }
    nanosleep(&poll_interval, NULL);
  }
}

// Milliseconds left of a forced run's budget, or -1 when its caller set none.
static long long forkBudgetLeftMs(const GCForcedRun *forced) {
  if (forced->forkWaitMs <= 0) {
    return -1;
  }
  struct timespec now;
  clock_gettime(CLOCK_MONOTONIC_RAW, &now);
  long long left = (forced->forkDeadline.tv_sec - now.tv_sec) * 1000 +
                   (forced->forkDeadline.tv_nsec - now.tv_nsec) / 1000000;
  return left > 0 ? left : 0;
}

// Takes the single child slot Redis allows. A forced cycle retries while a competing BGSAVE,
// AOF rewrite or replication fork holds it (EEXIST), within the budget its caller set; a
// scheduled cycle gets one attempt, since its next run is due anyway. Returns the child pid,
// or -1 having logged why and recorded it on `forced`.
//
// Every attempt is gated on the budget, not just the retries: a job can sit queued behind the
// single GC worker until its deadline has passed, and re-taking the GIL after a wait can overrun
// it too, so forking on the strength of an earlier check could start a cycle for a caller that
// has already been answered.
//
// The GIL must be held on entry and is held on return; it is released only while waiting, both
// because the child being waited on needs the main thread and because its exit is what makes an
// earlier isOutOfMemory() answer stale -- hence the guard inside the loop.
static pid_t forkGCChild(RedisModuleCtx *ctx, GCForcedRun *forced) {
  while (true) {
    long long budget_left = forced ? forkBudgetLeftMs(forced) : -1;
    if (budget_left == 0) {
      RedisModule_Log(ctx, "warning", "GC fork budget spent, aborting fork GC");
      forced->outcome = GC_FORCED_RUN_NO_FORK;
      return -1;
    }

    if (isOutOfMemory(ctx)) {
      RedisModule_Log(ctx, "warning", "Not enough memory for GC fork, skipping GC job");
      return -1;
    }

    pid_t cpid = RedisModule_Fork(NULL, NULL);  // duplicate the current process
    if (cpid != -1) {
      return cpid;
    }
    if (!forced || errno != EEXIST) {
      RedisModule_Log(ctx, "warning", "fork failed - got errno %d, aborting fork GC", errno);
      if (forced) {
        forced->outcome = GC_FORCED_RUN_NO_FORK;
      }
      return -1;
    }

    long long delay_ms = forced->retryIntervalMs;
    if (budget_left > 0 && budget_left < delay_ms) {
      delay_ms = budget_left;  // never sleep past the deadline
    }
    struct timespec delay = {.tv_sec = delay_ms / 1000,
                             .tv_nsec = (delay_ms % 1000) * 1000000};

    GilGate_Unlock(&gcGilGate_g, ctx);

    // Test hook (ENABLE_ASSERT): park after a failed fork, so a test can observe the EEXIST.
#ifdef ENABLE_ASSERT
    SyncPoint_Wait(SYNC_POINT_GC_FORK_SLOT_BUSY);
#endif

    nanosleep(&delay, NULL);
    GilGate_Lock(&gcGilGate_g, ctx);
  }
}

bool ForkCycle_Run(IndexGC *gc, GCForcedRun *forced, const IndexGCHook *hook, StrongRef early_check) {
  RedisModuleCtx *ctx = gc->ctx;
  bool gcrv = true;
  pid_t cpid;
  TimeSample ts;

  TimeSampler_Start(&ts);
  int pipefd[2];
  int rc = pipe(pipefd);  // create the pipe
  if (rc == -1) {
    RedisModule_Log(ctx, "warning", "Couldn't create pipe - got errno %d, aborting fork GC", errno);
    if (forced) {
      forced->outcome = GC_FORCED_RUN_NO_FORK;
    }
    IndexSpecRef_Release(early_check);
    return true;
  }
  gc->pipe_read_fd = pipefd[GC_READERFD];
  gc->pipe_write_fd = pipefd[GC_WRITERFD];
  // initialize the pollfd for the read pipe
  gc->pollfd_read[0].fd = gc->pipe_read_fd;
  gc->pollfd_read[0].events = POLLIN;

  // Test hook (ENABLE_ASSERT): park before the GIL, so a test can take the fork slot first.
#ifdef ENABLE_ASSERT
  SyncPoint_Wait(SYNC_POINT_GC_BEFORE_FORK);
#endif

  // We need to acquire the GIL to use the fork api
  GilGate_Lock(&gcGilGate_g, ctx);

  cpid = forkGCChild(ctx, forced);

  if (cpid == -1) {
    gc->retryIntervalMs = RSGlobalConfig.gcConfigParams.gcSettings.forkGcRetryInterval * 1000LL;
    IndexSpecRef_Release(early_check);

    GilGate_Unlock(&gcGilGate_g, ctx);

    close(gc->pipe_read_fd);
    close(gc->pipe_write_fd);

    return true;
  }

  // Now that we hold the GIL, we can cache this value knowing it won't change by the main thread
  // upon deleting a document (this is the actual number of documents to be cleaned by the fork).
  size_t num_docs_to_clean = gc->deletedOrUpdatedDocsFromLastRun;
  gc->deletedOrUpdatedDocsFromLastRun = 0;

  gc->retryIntervalMs = RSGlobalConfig.gcConfigParams.gcSettings.forkGcRunIntervalSec * 1000LL;

  GilGate_Unlock(&gcGilGate_g, ctx);


  if (cpid == 0) {
    // fork process
    setpriority(PRIO_PROCESS, getpid(), 19);
    close(gc->pipe_read_fd);
    // Pass the index to the child process
    ForkGC_childScanIndexes(gc, StrongRef_Get(early_check));
    close(gc->pipe_write_fd);
    sleep(RSGlobalConfig.gcConfigParams.gcSettings.forkGcSleepBeforeExit);
    RedisModule_ExitFromChild(EXIT_SUCCESS);
  } else {
    // main process
    // release the strong reference to the index for the main process (see comment above)
    IndexSpecRef_Release(early_check);
    close(gc->pipe_write_fd);

    // Test hook: lets tests inject mutations after fork (invisible to child) before results are applied.
    if (hook && hook->beforeApply) hook->beforeApply(hook->privdata);

    gc->cleanNumericEmptyNodes = RSGlobalConfig.gcConfigParams.gcSettings.forkGCCleanNumericEmptyNodes;
    if (ForkGC_parentHandleFromChild(gc) == FORK_GC_SPEC_DELETED) {
      gcrv = false;
    }
    close(gc->pipe_read_fd);
    // give the child some time to exit gracefully
    for (int attempt = 0; attempt < GC_WAIT_ATTEMPTS; ++attempt) {
      if (waitpid(cpid, NULL, WNOHANG) == 0) {
        usleep(500);
      }
    }
    // KillForkChild must be called when holding the GIL
    // otherwise it might cause a pipe leak and eventually run
    // out of file descriptor
    GilGate_Lock(&gcGilGate_g, ctx);
    int kill_rv = RedisModule_KillForkChild(cpid);
    GilGate_Unlock(&gcGilGate_g, ctx);
    if (kill_rv != REDISMODULE_OK) {
      reap_child_blocking(ctx, cpid, RSGlobalConfig.gcConfigParams.gcSettings.forkGcRunIntervalSec);
    }

    if (gcrv) {
      gcrv = VecSim_CallTieredIndexesGC(gc->index);
    }
  }

  IndexsGlobalStats_DecreaseLogicallyDeleted(num_docs_to_clean);
  TimeSampler_End(&ts);
  long long msRun = TimeSampler_DurationMS(&ts);

  gc->stats.numCycles++;
  gc->stats.totalMSRun += msRun;
  gc->stats.lastRunTimeMs = msRun;

  return gcrv;
}
