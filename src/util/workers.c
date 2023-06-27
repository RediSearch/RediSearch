/*
 * Copyright 2018-2022 Redis Labs Ltd. and Contributors
 *
 * This file is available under the Redis Labs Source Available License Agreement
 */

#include "workers_pool.h"
#include "workers.h"
#include "redismodule.h"
#include "config.h"

#include <pthread.h>

#ifdef MT_BUILD

//------------------------------------------------------------------------------
// Thread pool
//------------------------------------------------------------------------------

redisearch_threadpool _workers_thpool = NULL;
size_t yield_counter = 0;

//TODO: alloc only if _workers_thpool
static AREQ **g_running_requests;

void workersThreadPool_TrackReq(AREQ *req) {
  // get id
  int thread_id = redisearch_thpool_get_thread_idx(_workers_thpool);

  // Save the pointer to the currently executed request.
  g_running_requests[thread_id] = req;
}
void workersThreadPool_UnTrackReq() {
  int thread_id = redisearch_thpool_get_thread_idx(_workers_thpool);

  // Remove the request from the global struct.
  g_running_requests[thread_id] = NULL;
}

static void write_cmd_to_buffer(char *cmd, AREQ *req) {
    // initialize buff = FT.
    if (IsCursorRead(req)) {
      strcat(cmd, "CURSOR READ ");
      return;
    }
    if (IsProfile(req)) {
      // add PROFILE after FT. + space for command type
      strcat(cmd, "PROFILE ");
    } if (IsSearch(req)) {
      strcat(cmd, "SEARCH");
    } else { // Aggregate
      strcat(cmd, "AGGREGATE ");
    }
}

static void workersThreadPool_TrackReq_Reply(int thread_id, RedisModule_Reply *reply) {
  AREQ *req = g_running_requests[thread_id];
  if (req){
    RedisModule_ReplyKV_Map(reply, "command"); // >
    char cmd[100] = "FT.";
    write_cmd_to_buffer(cmd, req);
    RedisModule_ReplyKV_SimpleString(reply, "command", cmd);
    RedisModule_ReplyKV_SimpleString(reply, "idx", AREQ_GetIndexName(req));
    RedisModule_ReplyKV_SimpleString(reply, "query", AREQ_GetQuery(req));
    RedisModule_Reply_MapEnd(reply); // index_definition

  }
}

static void workersThreadPool_TrackReq_Info(int thread_id, RedisModuleInfoCtx *ctx) {
  if (g_running_requests[thread_id]){
    RedisModule_InfoAddSection(ctx, "executing command");
  }
}

static void yieldCallback(void *yieldCtx) {
  yield_counter++;
  if (yield_counter % 10 == 0 || yield_counter == 1) {
    RedisModule_Log(RSDummyContext, "notice", "Yield every 100 ms to allow redis server run while"
                    " waiting for workers to finish: call number %zu", yield_counter);
  }
  RedisModuleCtx *ctx = yieldCtx;
  RedisModule_Yield(ctx, REDISMODULE_YIELD_FLAG_CLIENTS, NULL);
}

// set up workers' thread pool
int workersThreadPool_CreatePool(size_t worker_count) {
  assert(worker_count);
  assert(_workers_thpool == NULL);

  _workers_thpool = redisearch_thpool_create(worker_count, "WORKERS");
  if (_workers_thpool == NULL) return REDISMODULE_ERR;

  g_running_requests = rm_calloc(worker_count, sizeof(AREQ *));

  return REDISMODULE_OK;
}

void workersThreadPool_InitPool() {
  assert(_workers_thpool != NULL);

  redisearch_thpool_init(_workers_thpool);
}

// return number of currently working threads
size_t workersThreadPool_WorkingThreadCount(void) {
  if (!_workers_thpool) {
    return 0;
  }

  return redisearch_thpool_num_threads_working(_workers_thpool);
}

// add task for worker thread
// DvirDu: I think we should add a priority parameter to this function
int workersThreadPool_AddWork(redisearch_thpool_proc function_p, void *arg_p) {
  assert(_workers_thpool != NULL);

  return redisearch_thpool_add_work(_workers_thpool, function_p, arg_p, THPOOL_PRIORITY_HIGH);
}

// Wait until all jobs have finished
void workersThreadPool_Wait(RedisModuleCtx *ctx) {
  if (!_workers_thpool) {
    return;
  }
  if (RedisModule_Yield) {
    // Wait until all the threads in the pool finish the remaining jobs. Periodically return and
    // call RedisModule_Yield even if threads are not done yet, so redis can answer PINGs
    // (and other stuff) so that the node-watch dog won't kill redis, for example.
    redisearch_thpool_timedwait(_workers_thpool, 100, yieldCallback, ctx);
    yield_counter = 0;  // reset
  } else {
    // In Redis versions < 7, RedisModule_Yield doesn't exist. Just wait for without yield.
    redisearch_thpool_wait(_workers_thpool);
  }
}

void workersThreadPool_Terminate(void) {
  redisearch_thpool_terminate_threads(_workers_thpool);
}

void workersThreadPool_Destroy(void) {
  redisearch_thpool_destroy(_workers_thpool);
  rm_free(g_running_requests);
}

void workersThreadPool_InitIfRequired() {
  if(USE_BURST_THREADS()) {
    // Initialize the thread pool temporarily for fast RDB loading of vector index (if needed).
    VecSim_SetWriteMode(VecSim_WriteAsync);
    workersThreadPool_InitPool();
    RedisModule_Log(RSDummyContext, "notice", "Created workers threadpool of size %lu",
                    RSGlobalConfig.numWorkerThreads);
  }
}

void workersThreadPool_waitAndTerminate(RedisModuleCtx *ctx) {
    // Wait until all the threads are finished the jobs currently in the queue. Note that we call
    // RM_Yield periodically while we wait, so we won't block redis for too long
    // (for answering PING etc.)
    if (RSGlobalConfig.numWorkerThreads == 0) return;
    workersThreadPool_Wait(ctx);
    RedisModule_Log(RSDummyContext, "notice",
                    "Done running pending background workers jobs");
    if (USE_BURST_THREADS()) {
    VecSim_SetWriteMode(VecSim_WriteInPlace);
    workersThreadPool_Terminate();
    RedisModule_Log(RSDummyContext, "notice",
                    "Terminated workers threadpool of size %lu",
                    RSGlobalConfig.numWorkerThreads);
  }
}

void workersThreadPool_SetTerminationWhenEmpty() {
  if (RSGlobalConfig.numWorkerThreads == 0) return;

  if (USE_BURST_THREADS()) {
    // Set the library back to in place mode, and let all the async jobs that are still pending run,
    // but after the last job is executed, terminate the running threads.
    VecSim_SetWriteMode(VecSim_WriteInPlace);
    redisearch_thpool_terminate_when_empty(_workers_thpool);
    RedisModule_Log(RSDummyContext, "notice", "Termination of workers threadpool of size %lu is set to occur when all"
                    " pending jobs are done",
                    RSGlobalConfig.numWorkerThreads);
  }
}

void workersThreadPool_PauseBeforeDump() {
  redisearch_thpool_pause_before_dump(_workers_thpool);
}

void workersThreadPool_Resume() {
  redisearch_thpool_resume(_workers_thpool);
}

void workersThreadPool_log_state_to_info(RedisModuleInfoCtx *ctx) {
  redisearch_thpool_log_state_to_info(_workers_thpool, ctx, workersThreadPool_TrackReq_Info);
}

void workersThreadPool_log_state_to_reply(RedisModule_Reply *reply) {
  redisearch_thpool_log_state_to_reply(_workers_thpool, reply, workersThreadPool_TrackReq_Reply);
}

#endif // MT_BUILD
