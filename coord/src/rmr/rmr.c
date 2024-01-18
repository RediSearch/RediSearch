/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#define RMR_C__
#include "rmr.h"
#include "reply.h"
#include "redismodule.h"
#include "cluster.h"
#include "chan.h"
#include "rq.h"
#include "rmutil/rm_assert.h"
#include "resp3.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <time.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <sys/param.h>

#include "hiredis/hiredis.h"
#include "hiredis/async.h"

extern int redisMajorVesion;

/* Currently a single cluster is supported */
static MRCluster *cluster_g = NULL;
static MRWorkQueue *rq_g = NULL;

/* Coordination request timeout */
long long timeout_g = 5000; // unused value. will be set in MR_Init

/* MapReduce context for a specific command's execution */
typedef struct MRCtx {
  struct timespec startTime;
  struct timespec firstRespTime;
  struct timespec endTime;
  int numReplied;
  int numExpected;
  int numErrored;
  MRReply **replies;
  int repliesCap;
  MRReduceFunc reducer;
  void *privdata;
  RedisModuleCtx *redisCtx;
  RedisModuleBlockedClient *bc;
  MRCoordinationStrategy strategy;
  MRCommand *cmds;
  int numCmds;
  int protocol;

  /**
   * This is a reduce function inside the MRCtx.
   * if set when replies will arrive we will not
   * unblock the client and instead the reduce function
   * will be called directly. This mechanism allows us to
   * send commands and base on the response send more commands
   * and do more aggregations. Only the last command/commands sent
   * needs to unblock the client.
   */
  MRReduceFunc fn;
} MRCtx;

/* The request duration in microseconds, relevant only on the reducer */
int64_t MR_RequestDuration(MRCtx *ctx) {
  return ((int64_t)1000000 * ctx->endTime.tv_sec + ctx->endTime.tv_nsec / 1000) -
         ((int64_t)1000000 * ctx->startTime.tv_sec + ctx->startTime.tv_nsec / 1000);
}

void MR_SetCoordinationStrategy(MRCtx *ctx, MRCoordinationStrategy strategy) {
  ctx->strategy = strategy;
}

static int totalAllocd = 0;
/* Create a new MapReduce context */
MRCtx *MR_CreateCtx(RedisModuleCtx *ctx, RedisModuleBlockedClient *bc, void *privdata) {
  MRCtx *ret = rm_malloc(sizeof(MRCtx));
  clock_gettime(CLOCK_REALTIME, &ret->startTime);
  ret->endTime = ret->startTime;
  ret->firstRespTime = ret->startTime;
  ret->numReplied = 0;
  ret->numErrored = 0;
  ret->numExpected = 0;
  ret->repliesCap = MAX(1, MRCluster_NumShards(cluster_g));
  ret->replies = rm_calloc(ret->repliesCap, sizeof(redisReply *));
  ret->reducer = NULL;
  ret->privdata = privdata;
  ret->strategy = MRCluster_FlatCoordination;
  ret->redisCtx = ctx;
  ret->bc = bc;
  RedisModule_Assert(ctx || bc);
  ret->protocol = ctx ? (is_resp3(ctx) ? 3 : 2) : 0;
  ret->fn = NULL;
  totalAllocd++;

  return ret;
}

void MRCtx_Free(MRCtx *ctx) {

  for (int i = 0; i < ctx->numCmds; i++) {
    MRCommand_Free(&ctx->cmds[i]);
  }
  rm_free(ctx->cmds);

  for (int i = 0; i < ctx->numReplied; i++) {
    if (ctx->replies[i] != NULL) {
      MRReply_Free(ctx->replies[i]);
      ctx->replies[i] = NULL;
    }
  }
  rm_free(ctx->replies);

  // free the context
  rm_free(ctx);
}

/* Get the user stored private data from the context */
void *MRCtx_GetPrivData(struct MRCtx *ctx) {
  return ctx->privdata;
}

int MRCtx_GetNumReplied(struct MRCtx *ctx) {
  return ctx->numReplied;
}

MRReply** MRCtx_GetReplies(struct MRCtx *ctx) {
  return ctx->replies;
}

RedisModuleCtx *MRCtx_GetRedisCtx(struct MRCtx *ctx) {
  return ctx->redisCtx;
}

void MRCtx_SetRedisCtx(struct MRCtx *ctx, void* rctx){
  ctx->redisCtx = rctx;
}

RedisModuleBlockedClient *MRCtx_GetBlockedClient(struct MRCtx *ctx) {
  return ctx->bc;
}

int MRCtx_GetProtocol(struct MRCtx *ctx) {
    return ctx->protocol;
}

void MRCtx_SetProtocol(struct MRCtx *ctx, int protocol) {
    ctx->protocol = protocol;
}

MRCommand *MRCtx_GetCmds(struct MRCtx *ctx) {
  return ctx->cmds;
}

int MRCtx_GetCmdsSize(struct MRCtx *ctx) {
  return ctx->numCmds;
}

void MRCtx_SetReduceFunction(struct MRCtx *ctx, MRReduceFunc fn) {
  ctx->fn = fn;
}

static void freePrivDataCB(RedisModuleCtx *ctx, void *p) {
  // printf("FreePrivData called!\n");
  MR_requestCompleted();
  if (p) {
    MRCtx *mc = p;
    MRCtx_Free(mc);
  }
}

static int timeoutHandler(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  RedisModule_Log(ctx, "notice", "Timed out coordination request");
  return RedisModule_ReplyWithError(ctx, "Timeout calling command");
}

/* handler for unblocking redis commands, that calls the actual reducer */
static int unblockHandler(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  RS_AutoMemory(ctx);
  MRCtx *mc = RedisModule_GetBlockedClientPrivateData(ctx);
  clock_gettime(CLOCK_REALTIME, &mc->endTime);

  mc->redisCtx = ctx;

  return mc->reducer(mc, mc->numReplied, mc->replies);
}

/* The callback called from each fanout request to aggregate their replies */
static void fanoutCallback(redisAsyncContext *c, void *r, void *privdata) {
  MRCtx *ctx = privdata;
  struct timespec now;
  clock_gettime(CLOCK_REALTIME, &now);

  if (ctx->numReplied == 0 && ctx->numErrored == 0) {
    clock_gettime(CLOCK_REALTIME, &ctx->firstRespTime);
  }
  if (!r) {
    ctx->numErrored++;

  } else {
    /* If needed - double the capacity for replies */
    if (ctx->numReplied == ctx->repliesCap) {
      ctx->repliesCap *= 2;
      ctx->replies = rm_realloc(ctx->replies, ctx->repliesCap * sizeof(MRReply *));
    }
    ctx->replies[ctx->numReplied++] = r;
  }

  // printf("Unblocking, replied %d, errored %d out of %d\n", ctx->numReplied, ctx->numErrored,
  //        ctx->numExpected);

  // If we've received the last reply - unblock the client
  if (ctx->numReplied + ctx->numErrored == ctx->numExpected) {
    if (ctx->fn) {
      ctx->fn(ctx, ctx->numReplied, ctx->replies);
    } else {
      RedisModuleBlockedClient *bc = ctx->bc;
      RedisModule_Assert(bc);
      RedisModule_BlockedClientMeasureTimeEnd(bc);
      RedisModule_UnblockClient(bc, ctx);
    }
  }
}

// temporary request context to pass to the event loop
struct MRRequestCtx {
  void *ctx;
  MRReduceFunc f;
  MRCommand *cmds;
  int numCmds;
  void (*cb)(struct MRRequestCtx *);
  int protocol;
};

void requestCb(void *p) {
  struct MRRequestCtx *ctx = p;
  ctx->cb(ctx);
}

/* Initialize the MapReduce engine with a node provider */
void MR_Init(MRCluster *cl, long long timeoutMS) {

  cluster_g = cl;
  timeout_g = timeoutMS;
  // `*50` for following the previous behavior
  // #define MAX_CONCURRENT_REQUESTS (MR_CONN_POOL_SIZE * 50)
  rq_g = RQ_New(cl->mgr.nodeConns * 50);
}

MRClusterTopology *MR_GetCurrentTopology() {
  return cluster_g->topo;
}

MRClusterNode *MR_GetMyNode() {
  return cluster_g->myNode;
}

//#ifef DEBUG_MR // @@

static void helloCallback(redisAsyncContext *c, void *r, void *privdata) {
  MRCtx *ctx = privdata;
  MRReply *reply = r;
}

//#endif DEBUG_MG

/* The fanout request received in the event loop in a thread safe manner */
static void uvFanoutRequest(struct MRRequestCtx *mc) {

  MRCtx *mrctx = mc->ctx;
  mrctx->numReplied = 0;
  mrctx->reducer = mc->f;
  mrctx->numExpected = 0;

  mrctx->numCmds = mc->numCmds;
  mrctx->cmds = rm_calloc(mrctx->numCmds, sizeof(MRCommand));

  if (mc->numCmds > 0) {
    // @@TODO: this may not be requires as we're hello-ing before command_send
    int cmd_proto = mc->cmds[0].protocol;
    if (cmd_proto != mc->protocol) {
      MRCommand hello = MR_NewCommand(2, "HELLO", cmd_proto == 3 ? "3" : "2");
      int rc = MRCluster_SendCommand(cluster_g, MRCluster_FlatCoordination, &hello, helloCallback, mrctx);
    }
  }

  if (cluster_g->topo) {
    MRCommand *cmd = &mc->cmds[0];
    mrctx->numExpected =
        MRCluster_FanoutCommand(cluster_g, mrctx->strategy, cmd, fanoutCallback, mrctx);
  }

  for (int i = 0; i < mrctx->numCmds; ++i) {
    mrctx->cmds[i] = mc->cmds[i];
  }

  if (mrctx->numExpected == 0) {
    RedisModuleBlockedClient *bc = mrctx->bc;
    RedisModule_Assert(bc);
    RedisModule_BlockedClientMeasureTimeEnd(bc);
    RedisModule_UnblockClient(bc, mrctx);
    // printf("could not send single command. hande fail please\n");
  }

  rm_free(mc->cmds);
  rm_free(mc);
}

static void uvMapRequest(struct MRRequestCtx *mc) {
  MRCtx *mrctx = mc->ctx;
  mrctx->numReplied = 0;
  mrctx->reducer = mc->f;
  mrctx->numExpected = 0;
  mrctx->numCmds = mc->numCmds;
  mrctx->cmds = rm_calloc(mrctx->numCmds, sizeof(MRCommand));

  if (mc->numCmds > 0) {
    int cmd_proto = mc->cmds[0].protocol;
    // @@TODO: this may not be requires as we're hello-ing before command_send
    if (cmd_proto != mc->protocol) {
      MRCommand hello = MR_NewCommand(2, "HELLO", cmd_proto == 3 ? "3" : "2");
      int rc = MRCluster_SendCommand(cluster_g, MRCluster_FlatCoordination, &hello, helloCallback, mrctx);
    }
  }

  for (int i = 0; i < mc->numCmds; i++) {
    if (!mc->cmds[i].protocol) {
      mc->cmds[i].protocol = mc->protocol; //@@ needed?
    }
    if (MRCluster_SendCommand(cluster_g, mrctx->strategy, &mc->cmds[i], fanoutCallback, mrctx) == REDIS_OK) {
      mrctx->numExpected++;
    }
  }

  for (int i = 0; i < mrctx->numCmds; ++i) {
    mrctx->cmds[i] = mc->cmds[i];
  }

  if (mrctx->numExpected == 0) {
    RedisModuleBlockedClient *bc = mrctx->bc;
    RedisModule_Assert(bc);
    RedisModule_BlockedClientMeasureTimeEnd(bc);
    RedisModule_UnblockClient(bc, mrctx);
    // printf("could not send single command. hande fail please\n");
  }

  rm_free(mc->cmds);
  rm_free(mc);
}

void MR_requestCompleted() {
  RQ_Done(rq_g);
}

/* Fanout map - send the same command to all the shards, sending the collective
 * reply to the reducer callback */
int MR_Fanout(struct MRCtx *mrctx, MRReduceFunc reducer, MRCommand cmd, bool block) {

  struct MRRequestCtx *rc = rm_malloc(sizeof(struct MRRequestCtx));
  rc->protocol = MRCtx_GetProtocol(mrctx);

  if (block) {
    RedisModule_Assert(!mrctx->bc);
    mrctx->bc = RedisModule_BlockClient(
        mrctx->redisCtx, unblockHandler, timeoutHandler, freePrivDataCB, 0); // timeout_g);
    RedisModule_BlockedClientMeasureTimeStart(mrctx->bc);
  }
  rc->ctx = mrctx;
  rc->f = reducer;
  rc->cmds = rm_calloc(1, sizeof(MRCommand));
  rc->numCmds = 1;
  rc->cmds[0] = cmd;
  rc->cb = uvFanoutRequest;
  RQ_Push(rq_g, requestCb, rc);
  return REDIS_OK;
}

int MR_Map(struct MRCtx *ctx, MRReduceFunc reducer, MRCommandGenerator cmds, bool block) {
  struct MRRequestCtx *rc = rm_malloc(sizeof(struct MRRequestCtx));
  rc->ctx = ctx;
  rc->f = reducer;
  rc->cmds = rm_calloc(cmds.Len(cmds.ctx), sizeof(MRCommand));
  rc->numCmds = cmds.Len(cmds.ctx);
  rc->protocol = MRCtx_GetProtocol(ctx);

  // copy the commands from the iterator to the context's array
  for (int i = 0; i < rc->numCmds; i++) {
    if (!cmds.Next(cmds.ctx, &rc->cmds[i])) {
      rc->numCmds = i;
      break;
    }
    rc->cmds[i].protocol = rc->protocol;
  }

  if (block) {
    RedisModule_Assert(!ctx->bc);
    ctx->bc = RedisModule_BlockClient(ctx->redisCtx, unblockHandler, timeoutHandler, freePrivDataCB, 0); // timeout_g);
    RedisModule_BlockedClientMeasureTimeStart(ctx->bc);
  }

  rc->cb = uvMapRequest;
  RQ_Push(rq_g, requestCb, rc);

  return REDIS_OK;
}

int MR_MapSingle(struct MRCtx *ctx, MRReduceFunc reducer, MRCommand cmd) {

  struct MRRequestCtx *rc = rm_malloc(sizeof(struct MRRequestCtx));
  rc->ctx = ctx;
  rc->f = reducer;
  rc->cmds = rm_calloc(1, sizeof(MRCommand));
  rc->numCmds = 1;
  rc->cmds[0] = cmd;
  rc->protocol = MRCtx_GetProtocol(ctx);
  RedisModule_Assert(!ctx->bc);
  ctx->bc = RedisModule_BlockClient(ctx->redisCtx, unblockHandler, timeoutHandler, freePrivDataCB, 0); // timeout_g);
  RedisModule_BlockedClientMeasureTimeStart(ctx->bc);

  rc->cb = uvMapRequest;
  RQ_Push(rq_g, requestCb, rc);
  return REDIS_OK;
}

/* Return the active cluster's host count */
size_t MR_NumHosts() {
  return MRCluster_NumHosts(cluster_g);
}

void SetMyPartition(MRClusterTopology *ct, MRClusterShard *myShard);
/* on-loop update topology request. This can't be done from the main thread */
static void uvUpdateTopologyRequest(void *p) {
  MRClusterTopology *topo = p;
  MRCLuster_UpdateTopology(cluster_g, topo);
  if (cluster_g->myshard) {
    SetMyPartition(topo, cluster_g->myshard);
  }
  RQ_Done(rq_g);
}

/* Set a new topology for the cluster */
void MR_UpdateTopology(MRClusterTopology *newTopo) {
  // enqueue a request on the io thread, this can't be done from the main thread
  RQ_Push_Topology(rq_g, uvUpdateTopologyRequest, newTopo);
}

struct MRIteratorCallbackCtx;

typedef int (*MRIteratorCallback)(struct MRIteratorCallbackCtx *ctx, MRReply *rep);

typedef struct MRIteratorCtx {
  MRCluster *cluster;
  MRChannel *chan;
  MRIteratorCallback cb;
  int pending;    // Number of shards with more results (not depleted)
  int inProcess;  // Number of currently running commands on shards
  int timedOut;   // whether the coordinator experienced a timeout
} MRIteratorCtx;

typedef struct MRIteratorCallbackCtx {
  MRIteratorCtx *ic;
  MRCommand cmd;
} MRIteratorCallbackCtx;

typedef struct MRIterator {
  MRIteratorCtx ctx;
  MRIteratorCallbackCtx *cbxs;
  size_t len;
} MRIterator;

int MRIteratorCallback_Done(MRIteratorCallbackCtx *ctx, int error);
void MRIterator_Free(MRIterator *it);

static void mrIteratorRedisCB(redisAsyncContext *c, void *r, void *privdata) {
  MRIteratorCallbackCtx *ctx = privdata;
  if (!r) {
    MRIteratorCallback_Done(ctx, 1);
    // ctx->numErrored++;
    // TODO: report error
  } else {
    ctx->ic->cb(ctx, r);
  }
}

int MRIteratorCallback_ResendCommand(MRIteratorCallbackCtx *ctx, MRCommand *cmd) {
  ctx->cmd = *cmd;
  return MRCluster_SendCommand(ctx->ic->cluster, MRCluster_MastersOnly, cmd, mrIteratorRedisCB,
                               ctx);
}

// Use after modifying `pending` (or any other variable of the iterator) to make sure it's visible to other threads
void MRIteratorCallback_ProcessDone(MRIteratorCallbackCtx *ctx) {
  unsigned inProcess =  __atomic_sub_fetch(&ctx->ic->inProcess, 1, __ATOMIC_RELEASE);
  if (!inProcess) RQ_Done(rq_g);
}

// Use before obtaining `pending` (or any other variable of the iterator) to make sure it's synchronized with other threads
static int MRIteratorCallback_GetNumInProcess(MRIterator *it) {
  return __atomic_load_n(&it->ctx.inProcess, __ATOMIC_ACQUIRE);
}

bool MRIteratorCallback_GetTimedOut(MRIteratorCtx *ctx) {
  return __atomic_load_n(&ctx->timedOut, __ATOMIC_ACQUIRE);
}

void MRIteratorCallback_SetTimedOut(MRIteratorCtx *ctx) {
  // Atomically set the timedOut field of the ctx
  __atomic_store_n(&ctx->timedOut, 1, __ATOMIC_RELAXED);
}

void MRIteratorCallback_ResetTimedOut(MRIteratorCtx *ctx) {
  // Set the `timedOut` field to 0
  __atomic_store_n(&ctx->timedOut, 0, __ATOMIC_RELAXED);
}

void *MRITERATOR_DONE = "MRITERATOR_DONE";

int MRIteratorCallback_Done(MRIteratorCallbackCtx *ctx, int error) {
  // Mark the command of the context as depleted (so we won't send another command to the shard)
  ctx->cmd.depleted = true;
  int pending = --ctx->ic->pending; // Decrease `pending` before decreasing `inProcess`
  MRIteratorCallback_ProcessDone(ctx);
  if (pending <= 0) {
    RS_LOG_ASSERT(pending >= 0, "Pending should not reach a negative value");
    // fprintf(stderr, "FINISHED iterator, error? %d pending %d\n", error, ctx->ic->pending);
    MRChannel_Close(ctx->ic->chan);
    return 0;
  }
  // fprintf(stderr, "Done iterator, error? %d pending %d\n", error, ctx->ic->pending);

  return 1;
}

MRCommand *MRIteratorCallback_GetCommand(MRIteratorCallbackCtx *ctx) {
  return &ctx->cmd;
}

MRIteratorCtx *MRIteratorCallback_GetCtx(MRIteratorCallbackCtx *ctx) {
  return ctx->ic;
}

int MRIteratorCallback_AddReply(MRIteratorCallbackCtx *ctx, MRReply *rep) {
  return MRChannel_Push(ctx->ic->chan, rep);
}

void iterStartCb(void *p) {
  MRIterator *it = p;
  for (size_t i = 0; i < it->len; i++) {
    if (MRCluster_SendCommand(it->ctx.cluster, MRCluster_MastersOnly, &it->cbxs[i].cmd,
                              mrIteratorRedisCB, &it->cbxs[i]) == REDIS_ERR) {
      // fprintf(stderr, "Could not send command!\n");
      MRIteratorCallback_Done(&it->cbxs[i], 1);
    }
  }
}

void iterManualNextCb(void *p) {
  MRIterator *it = p;
  for (size_t i = 0; i < it->len; i++) {
    if (!it->cbxs[i].cmd.depleted) {
      if (MRCluster_SendCommand(it->ctx.cluster, MRCluster_MastersOnly, &it->cbxs[i].cmd,
                                mrIteratorRedisCB, &it->cbxs[i]) == REDIS_ERR) {
        // fprintf(stderr, "Could not send command!\n");
        MRIteratorCallback_Done(&it->cbxs[i], 1);
      }
    }
  }
}

bool MR_ManuallyTriggerNextIfNeeded(MRIterator *it, size_t channelThreshold) {
  // We currently trigger the next batch of commands only when no commands are in process,
  // regardless of the number of replies we have in the channel.
  // Since we push the triggering job to a single-threaded queue (currently), we can modify the logic here
  // to trigger the next batch when we have no commands in process and no more than channelThreshold replies to process.
  if (MRIteratorCallback_GetNumInProcess(it)) {
    // We have more replies to wait for
    return true;
  }
  size_t channelSize = MRChannel_Size(it->ctx.chan);
  if (channelSize > channelThreshold) {
    // We have more replies to process
    return true;
  }
  // At this point there is no race on the iterator since there are no commands in process.
  // We have <= channelThreshold replies to process, so if there are pending commands we want to trigger them.
  if (it->ctx.pending) {
    // We have more commands to send
    it->ctx.inProcess = it->ctx.pending;
    RQ_Push(rq_g, iterManualNextCb, it);
    return true; // We may have more replies (and we surely will)
  }
  // We have no pending commands and no more than channelThreshold replies to process.
  // If we have more replies we will process them, otherwise we are done.
  return channelSize > 0;
}

MRIterator *MR_Iterate(MRCommandGenerator cg, MRIteratorCallback cb) {

  MRIterator *ret = rm_malloc(sizeof(*ret));
  size_t len = cg.Len(cg.ctx);
  *ret = (MRIterator){
      .ctx =
          {
              .cluster = cluster_g,
              .chan = MR_NewChannel(0),
              .cb = cb,
              .pending = 0,
              .timedOut = 0,
          },
      .cbxs = rm_calloc(len, sizeof(MRIteratorCallbackCtx)),
      .len = len,
  };

  for (size_t i = 0; i < len; i++) {
    ret->cbxs[i].ic = &ret->ctx;
    if (!cg.Next(cg.ctx, &(ret->cbxs[i].cmd))) {
      ret->len = i;
      break;
    }
  }

  // Could not create command, probably invalid cluster
  if (ret->len == 0) {
    MRIterator_Free(ret);
    return NULL;
  }
  ret->ctx.pending = ret->len;
  ret->ctx.inProcess = ret->len; // Initially all commands are in process

  RQ_Push(rq_g, iterStartCb, ret);
  return ret;
}

MRIteratorCtx *MRIterator_GetCtx(MRIterator *it) {
  return &it->ctx;
}

MRReply *MRIterator_Next(MRIterator *it) {
  void *p = MRChannel_Pop(it->ctx.chan);
  // fprintf(stderr, "POP: %s\n", p == MRCHANNEL_CLOSED ? "CLOSED" : "ITER");
  if (p == MRCHANNEL_CLOSED) {
    return MRITERATOR_DONE;
  }
  return p;
}

void MRIterator_WaitDone(MRIterator *it, bool mayBeIdle) {
  if (mayBeIdle) {
    // Wait until all the commands are at least idle (it->ctx.inProcess == 0)
    while (MRIteratorCallback_GetNumInProcess(it)) {
      usleep(1000);
    }
    // If we have no pending shards, we are done.
    if (!it->ctx.pending) return;
    // If we have pending (not depleted) shards, trigger `FT.CURSOR DEL` on them
    it->ctx.inProcess = it->ctx.pending;
    // Change the root command to DEL for each pending shard
    for (size_t i = 0; i < it->len; i++) {
      MRCommand *cmd = &it->cbxs[i].cmd;
      if (!cmd->depleted) {
        // assert(!strcmp(cmd->strs[1], "READ"));
        cmd->rootCommand = C_DEL;
        strcpy(cmd->strs[1], "DEL");
        cmd->lens[1] = 3;
      }
    }
    // Send the DEL commands, and wait for them to be done
    RQ_Push(rq_g, iterManualNextCb, it);
  }
  // Wait until all the commands are done (it->ctx.pending == 0)
  MRChannel_WaitClose(it->ctx.chan);
}

// Assumes no other thread is using the iterator, the channel, or any of the commands and contexts
void MRIterator_Free(MRIterator *it) {
  for (size_t i = 0; i < it->len; i++) {
    MRCommand_Free(&it->cbxs[i].cmd);
  }
  MRReply *reply;
  while ((reply = MRChannel_UnsafeForcePop(it->ctx.chan))) {
    MRReply_Free(reply);
  }
  MRChannel_Free(it->ctx.chan);
  rm_free(it->cbxs);
  rm_free(it);
}
