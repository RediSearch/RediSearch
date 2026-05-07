/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "pipe.h"
#include "time_sample.h"
#include <stdlib.h>
#include <unistd.h>
#include <sys/wait.h>
#include <sys/resource.h>
#include <poll.h>
#include "module.h"
#include "rmutil/rm_assert.h"
#include "resp3.h"
#include "info/global_stats.h"
#include "info/info_redis/threads/current_thread.h"
#include "obfuscation/obfuscation_api.h"
#include "obfuscation/hidden.h"
#include "util/redis_mem_info.h"
#include "util/timeout.h"

#define GC_WRITERFD 1
#define GC_READERFD 0
// Number of attempts to wait for the child to exit gracefully before trying to terminate it
#define GC_WAIT_ATTEMPTS 4

static void FGC_childScanIndexes(ForkGC *gc, IndexSpec *spec) {
  RedisSearchCtx sctx = SEARCH_CTX_STATIC(gc->ctx, spec);
  const char* indexName = IndexSpec_FormatName(spec, RSGlobalConfig.hideUserDataFromLog);
  RedisModule_Log(sctx.redisCtx, "debug", "ForkGC in index %s - child scanning indexes start", indexName);
  FGC_childCollectTerms(gc, &sctx);
  FGC_childCollectNumeric(gc, &sctx);
  FGC_childCollectTags(gc, &sctx);
  FGC_childCollectMissingDocs(gc, &sctx);
  FGC_childCollectExistingDocs(gc, &sctx);
  RedisModule_SendChildHeartbeat(1.0); // final heartbeat
  RedisModule_Log(sctx.redisCtx, "debug", "ForkGC in index %s - child scanning indexes end", indexName);
  // Let the parent wait for the terminal terminator, so we manage to send the heartbeat before exiting
  FGC_sendTerminator(gc);
}

FGCError FGC_parentHandleFromChild(ForkGC *gc) {
  FGCError status = FGC_COLLECTED;
  RedisModule_Log(gc->ctx, "debug", "ForkGC - parent start applying changes");

#define COLLECT_FROM_CHILD(e)               \
  while ((status = (e)) == FGC_COLLECTED) { \
  }                                         \
  if (status != FGC_DONE) {                 \
    return status;                          \
  }

  COLLECT_FROM_CHILD(FGC_parentHandleTerms(gc));
  COLLECT_FROM_CHILD(FGC_parentHandleNumeric(gc));
  COLLECT_FROM_CHILD(FGC_parentHandleTags(gc));
  COLLECT_FROM_CHILD(FGC_parentHandleMissingDocs(gc));
  COLLECT_FROM_CHILD(FGC_parentHandleExistingDocs(gc));

  // Wait for the final terminator from the child, so it can finish post-processing chores before we kill it
  size_t terminator_check;
  int rc = FGC_recvFixed(gc, &terminator_check, sizeof(terminator_check)); // final status from child
  if (rc != REDISMODULE_OK || terminator_check != SIZE_MAX) {
    return FGC_CHILD_ERROR;
  }
  RedisModule_Log(gc->ctx, "debug", "ForkGC - parent ends applying changes");

  return status;
}

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
  RedisModule_Log(ctx, "debug", "ForkGC - used memory ratio: %f", used_memory_ratio);

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
      RedisModule_Log(ctx, "warning", "ForkGC - timed out waiting for child %d to exit", cpid);
      break;
    }
    nanosleep(&poll_interval, NULL);
  }
}

static bool periodicCb(void *privdata, bool force) {
  ForkGC *gc = privdata;
  RedisModuleCtx *ctx = gc->ctx;

  // This check must be done first, because some values (like `deletedDocsFromLastRun`) that are used for
  // early termination might never change after index deletion and will cause periodicCb to always return true,
  // which will cause the GC to never stop rescheduling itself.
  // If the index was deleted, we don't want to reschedule the GC, so we return false.
  // If the index is still valid, we MUST hold the strong reference to it until after the fork, to make sure
  // the child process has a valid reference to the index.
  // If we were to try and revalidate the index after the fork, it might already be dropped and the child
  // will exit before sending any data, and might left the parent waiting for data that will never arrive.
  // Attempting to revalidate the index after the fork is also problematic because the parent and child are
  // not synchronized, and the parent might see the index alive while the child sees it as deleted.
  StrongRef early_check = IndexSpecRef_Promote(gc->index);
  if (!StrongRef_Get(early_check)) {
    // Index was deleted
    return false;
  }

  if (!force && gc->deletedOrUpdatedDocsFromLastRun < RSGlobalConfig.gcConfigParams.gcSettings.forkGcCleanThreshold) {
    IndexSpecRef_Release(early_check);
    return true;
  }

  bool gcrv = true;
  pid_t cpid;
  TimeSample ts;

  while (gc->pauseState == FGC_PAUSED_CHILD) {
    gc->execState = FGC_STATE_WAIT_FORK;
    // spin or sleep
    usleep(500);
  }

  TimeSampler_Start(&ts);
  int pipefd[2];
  int rc = pipe(pipefd);  // create the pipe
  if (rc == -1) {
    RedisModule_Log(ctx, "warning", "Couldn't create pipe - got errno %d, aborting fork GC", errno);
    IndexSpecRef_Release(early_check);
    return true;
  }
  gc->pipe_read_fd = pipefd[GC_READERFD];
  gc->pipe_write_fd = pipefd[GC_WRITERFD];
  // initialize the pollfd for the read pipe
  gc->pollfd_read[0].fd = gc->pipe_read_fd;
  gc->pollfd_read[0].events = POLLIN;

  // We need to acquire the GIL to use the fork api
  RedisModule_ThreadSafeContextLock(ctx);

  // Check if we are out of memory before even trying to fork
  if (isOutOfMemory(ctx)) {
    RedisModule_Log(ctx, "warning", "Not enough memory for GC fork, skipping GC job");
    gc->retryInterval.tv_sec = RSGlobalConfig.gcConfigParams.gcSettings.forkGcRetryInterval;
    IndexSpecRef_Release(early_check);
    RedisModule_ThreadSafeContextUnlock(ctx);
    close(gc->pipe_read_fd);
    close(gc->pipe_write_fd);
    return true;
  }

  gc->execState = FGC_STATE_SCANNING;

  cpid = RedisModule_Fork(NULL, NULL);  // duplicate the current process

  if (cpid == -1) {
    RedisModule_Log(ctx, "warning", "fork failed - got errno %d, aborting fork GC", errno);
    gc->retryInterval.tv_sec = RSGlobalConfig.gcConfigParams.gcSettings.forkGcRetryInterval;
    IndexSpecRef_Release(early_check);

    RedisModule_ThreadSafeContextUnlock(ctx);

    close(gc->pipe_read_fd);
    close(gc->pipe_write_fd);

    return true;
  }

  // Now that we hold the GIL, we can cache this value knowing it won't change by the main thread
  // upon deleting a document (this is the actual number of documents to be cleaned by the fork).
  size_t num_docs_to_clean = gc->deletedOrUpdatedDocsFromLastRun;
  gc->deletedOrUpdatedDocsFromLastRun = 0;

  gc->retryInterval.tv_sec = RSGlobalConfig.gcConfigParams.gcSettings.forkGcRunIntervalSec;

  RedisModule_ThreadSafeContextUnlock(ctx);


  if (cpid == 0) {
    // fork process
    setpriority(PRIO_PROCESS, getpid(), 19);
    close(gc->pipe_read_fd);
    // Pass the index to the child process
    FGC_childScanIndexes(gc, StrongRef_Get(early_check));
    close(gc->pipe_write_fd);
    sleep(RSGlobalConfig.gcConfigParams.gcSettings.forkGcSleepBeforeExit);
    RedisModule_ExitFromChild(EXIT_SUCCESS);
  } else {
    // main process
    // release the strong reference to the index for the main process (see comment above)
    IndexSpecRef_Release(early_check);
    close(gc->pipe_write_fd);
    while (gc->pauseState == FGC_PAUSED_PARENT) {
      gc->execState = FGC_STATE_WAIT_APPLY;
      // spin
      usleep(500);
    }

    gc->execState = FGC_STATE_APPLYING;
    gc->cleanNumericEmptyNodes = RSGlobalConfig.gcConfigParams.gcSettings.forkGCCleanNumericEmptyNodes;
    if (FGC_parentHandleFromChild(gc) == FGC_SPEC_DELETED) {
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
    RedisModule_ThreadSafeContextLock(ctx);
    int kill_rv = RedisModule_KillForkChild(cpid);
    RedisModule_ThreadSafeContextUnlock(ctx);
    if (kill_rv != REDISMODULE_OK) {
      reap_child_blocking(ctx, cpid, RSGlobalConfig.gcConfigParams.gcSettings.forkGcRunIntervalSec);
    }

    if (gcrv) {
      gcrv = VecSim_CallTieredIndexesGC(gc->index);
    }
  }

  IndexsGlobalStats_DecreaseLogicallyDeleted(num_docs_to_clean);
  gc->execState = FGC_STATE_IDLE;
  TimeSampler_End(&ts);
  long long msRun = TimeSampler_DurationMS(&ts);

  gc->stats.numCycles++;
  gc->stats.totalMSRun += msRun;
  gc->stats.lastRunTimeMs = msRun;

  return gcrv;
}

#if defined(__has_feature)
#if __has_feature(thread_sanitizer)
#define NO_TSAN_CHECK __attribute__((no_sanitize("thread")))
#endif
#endif
#ifndef NO_TSAN_CHECK
#define NO_TSAN_CHECK
#endif

void FGC_WaitBeforeFork(ForkGC *gc) NO_TSAN_CHECK {
  RS_LOG_ASSERT(gc->pauseState == 0, "FGC pause state should be 0");
  gc->pauseState = FGC_PAUSED_CHILD;

  while (gc->execState != FGC_STATE_WAIT_FORK) {
    usleep(500);
  }
}

void FGC_ForkAndWaitBeforeApply(ForkGC *gc) NO_TSAN_CHECK {
  // Ensure that we're waiting for the child to begin
  RS_LOG_ASSERT(gc->pauseState == FGC_PAUSED_CHILD, "FGC pause state should be CHILD");
  RS_LOG_ASSERT(gc->execState == FGC_STATE_WAIT_FORK, "FGC exec state should be WAIT_FORK");

  gc->pauseState = FGC_PAUSED_PARENT;
  while (gc->execState != FGC_STATE_WAIT_APPLY) {
    usleep(500);
  }
}

void FGC_Apply(ForkGC *gc) NO_TSAN_CHECK {
  gc->pauseState = FGC_PAUSED_UNPAUSED;
  while (gc->execState != FGC_STATE_IDLE) {
    usleep(500);
  }
}

static void onTerminateCb(void *privdata) {
  ForkGC *gc = privdata;
  size_t deleted_or_updated_docs = atomic_exchange(&gc->deletedOrUpdatedDocsFromLastRun, 0);
  IndexsGlobalStats_DecreaseLogicallyDeleted(deleted_or_updated_docs);
  WeakRef_Release(gc->index);
  RedisModule_FreeThreadSafeContext(gc->ctx);
  rm_free(gc);
}

static void statsCb(RedisModule_Reply *reply, void *gcCtx) {
#define REPLY_KVNUM(k, v) RedisModule_ReplyKV_Double(reply, (k), (v))
  ForkGC *gc = gcCtx;
  if (!gc) return;
  REPLY_KVNUM("bytes_collected", gc->stats.totalCollected);
  REPLY_KVNUM("total_ms_run", gc->stats.totalMSRun);
  REPLY_KVNUM("total_cycles", gc->stats.numCycles);
  REPLY_KVNUM("average_cycle_time_ms", (double)gc->stats.totalMSRun / gc->stats.numCycles);
  REPLY_KVNUM("last_run_time_ms", (double)gc->stats.lastRunTimeMs);
  REPLY_KVNUM("gc_numeric_trees_missed", (double)gc->stats.gcNumericNodesMissed);
  REPLY_KVNUM("gc_blocks_denied", (double)gc->stats.gcBlocksDenied);
}

static void statsForInfoCb(RedisModuleInfoCtx *ctx, void *gcCtx) {
  ForkGC *gc = gcCtx;
  RedisModule_InfoBeginDictField(ctx, "gc_stats");
  RedisModule_InfoAddFieldLongLong(ctx, "bytes_collected", gc->stats.totalCollected);
  RedisModule_InfoAddFieldLongLong(ctx, "total_ms_run", gc->stats.totalMSRun);
  RedisModule_InfoAddFieldLongLong(ctx, "total_cycles", gc->stats.numCycles);
  RedisModule_InfoAddFieldDouble(ctx, "average_cycle_time_ms", (double)gc->stats.totalMSRun / gc->stats.numCycles);
  RedisModule_InfoAddFieldDouble(ctx, "last_run_time_ms", (double)gc->stats.lastRunTimeMs);
  RedisModule_InfoAddFieldDouble(ctx, "gc_numeric_trees_missed", (double)gc->stats.gcNumericNodesMissed);
  RedisModule_InfoAddFieldDouble(ctx, "gc_blocks_denied", (double)gc->stats.gcBlocksDenied);
  RedisModule_InfoEndDictField(ctx);
}

static void deleteOrUpdateCb(void *ctx) {
  ForkGC *gc = ctx;
  ++gc->deletedOrUpdatedDocsFromLastRun;
  IndexsGlobalStats_IncreaseLogicallyDeleted(1);
}

static void getStatsCb(void *gcCtx, InfoGCStats *out) {
  const ForkGC *gc = gcCtx;
  out->totalCollectedBytes = gc->stats.totalCollected;
  out->totalCycles = gc->stats.numCycles;
  out->totalTime = gc->stats.totalMSRun;
  out->lastRunTimeMs = gc->stats.lastRunTimeMs;
}

static struct timespec getIntervalCb(void *ctx) {
  ForkGC *gc = ctx;
  return gc->retryInterval;
}

ForkGC *FGC_Create(StrongRef spec_ref, GCCallbacks *callbacks) {
  ForkGC *forkGc = rm_calloc(1, sizeof(*forkGc));
  *forkGc = (ForkGC){
      .index = StrongRef_Demote(spec_ref),
      .deletedOrUpdatedDocsFromLastRun = 0,
  };
  forkGc->retryInterval.tv_sec = RSGlobalConfig.gcConfigParams.gcSettings.forkGcRunIntervalSec;
  forkGc->retryInterval.tv_nsec = 0;

  forkGc->cleanNumericEmptyNodes = RSGlobalConfig.gcConfigParams.gcSettings.forkGCCleanNumericEmptyNodes;
  forkGc->ctx = RedisModule_GetDetachedThreadSafeContext(RSDummyContext);

  callbacks->onTerm = onTerminateCb;
  callbacks->periodicCallback = periodicCb;
  callbacks->renderStats = statsCb;
  callbacks->renderStatsForInfo = statsForInfoCb;
  callbacks->getInterval = getIntervalCb;
  callbacks->onDelete = deleteOrUpdateCb;
  callbacks->onWrite = NULL; // writes are not tracked for forkGC
  callbacks->onUpdate = deleteOrUpdateCb;
  callbacks->getStats = getStatsCb;

  return forkGc;
}
