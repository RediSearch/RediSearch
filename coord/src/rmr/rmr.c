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
#include "coord/src/config.h"

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

/* Currently a single cluster is supported */
static MRCluster *cluster_g = NULL;
static MRWorkQueue *rq_g = NULL;

/* Coordination request timeout */
long long timeout_g = 5000; // unused value. will be set in MR_Init

/* MapReduce context for a specific command's execution */
typedef struct MRCtx {
  int numReplied;
  int numExpected;
  int numErrored;
  int repliesCap;
  MRReply **replies;
  MRReduceFunc reducer;
  void *privdata;
  RedisModuleCtx *redisCtx;
  RedisModuleBlockedClient *bc;
  MRCoordinationStrategy strategy;
  char protocol;
  int numCmds;
  MRCommand *cmds;

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

void MR_SetCoordinationStrategy(MRCtx *ctx, MRCoordinationStrategy strategy) {
  ctx->strategy = strategy;
}

/* Create a new MapReduce context */
MRCtx *MR_CreateCtx(RedisModuleCtx *ctx, RedisModuleBlockedClient *bc, void *privdata) {
  MRCtx *ret = rm_malloc(sizeof(MRCtx));
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

  mc->redisCtx = ctx;

  return mc->reducer(mc, mc->numReplied, mc->replies);
}

/* The callback called from each fanout request to aggregate their replies */
static void fanoutCallback(redisAsyncContext *c, void *r, void *privdata) {
  MRCtx *ctx = privdata;

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
  int protocol;
};

/* Initialize the MapReduce engine with a node provider */
void MR_Init(MRCluster *cl, long long timeoutMS) {

  cluster_g = cl;
  timeout_g = timeoutMS;
  // `*50` for following the previous behavior
  // #define MAX_CONCURRENT_REQUESTS (MR_CONN_POOL_SIZE * 50)
  rq_g = RQ_New(cl->mgr.nodeConns * 50);
}

int MR_CheckTopologyConnections(bool mastersOnly) {
  return MRCluster_CheckConnections(cluster_g, mastersOnly ? MRCluster_MastersOnly : 0);
}

bool MR_CurrentTopologyExists() {
  return cluster_g->topo != NULL;
}

MRClusterNode *MR_GetMyNode() {
  return cluster_g->myNode;
}

/* The fanout request received in the event loop in a thread safe manner */
static void uvFanoutRequest(void *p) {
  struct MRRequestCtx *mc = p;
  MRCtx *mrctx = mc->ctx;
  mrctx->numReplied = 0;
  mrctx->reducer = mc->f;
  mrctx->numExpected = 0;

  mrctx->numCmds = mc->numCmds;
  mrctx->cmds = mc->cmds;

  if (cluster_g->topo) {
    MRCommand *cmd = &mc->cmds[0];
    mrctx->numExpected =
        MRCluster_FanoutCommand(cluster_g, mrctx->strategy, cmd, fanoutCallback, mrctx);
  }

  if (mrctx->numExpected == 0) {
    RedisModuleBlockedClient *bc = mrctx->bc;
    RedisModule_Assert(bc);
    RedisModule_BlockedClientMeasureTimeEnd(bc);
    RedisModule_UnblockClient(bc, mrctx);
  }

  rm_free(mc);
}

static void uvMapRequest(void *p) {
  struct MRRequestCtx *mc = p;
  MRCtx *mrctx = mc->ctx;
  mrctx->numReplied = 0;
  mrctx->reducer = mc->f;
  mrctx->numExpected = 0;
  mrctx->numCmds = mc->numCmds;
  mrctx->cmds = mc->cmds;

  for (int i = 0; i < mc->numCmds; i++) {
    if (MRCluster_SendCommand(cluster_g, mrctx->strategy, &mc->cmds[i], fanoutCallback, mrctx) == REDIS_OK) {
      mrctx->numExpected++;
    }
  }

  if (mrctx->numExpected == 0) {
    RedisModuleBlockedClient *bc = mrctx->bc;
    RedisModule_Assert(bc);
    RedisModule_BlockedClientMeasureTimeEnd(bc);
    RedisModule_UnblockClient(bc, mrctx);
  }

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
  RedisModule_Assert(rc->protocol == cmd.protocol); // TODO: dev-time assert only
  RQ_Push(rq_g, uvFanoutRequest, rc);
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

  RQ_Push(rq_g, uvMapRequest, rc);

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
  RedisModule_Assert(rc->protocol == cmd.protocol); // TODO: dev-time assert only
  RedisModule_Assert(!ctx->bc);
  ctx->bc = RedisModule_BlockClient(ctx->redisCtx, unblockHandler, timeoutHandler, freePrivDataCB, 0); // timeout_g);
  RedisModule_BlockedClientMeasureTimeStart(ctx->bc);

  RQ_Push(rq_g, uvMapRequest, rc);
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
  if (cluster_g->myShard) {
    SetMyPartition(topo, cluster_g->myShard);
  }
}

/* Set a new topology for the cluster */
void MR_UpdateTopology(MRClusterTopology *newTopo) {
  // enqueue a request on the io thread, this can't be done from the main thread
  RQ_Push_Topology(uvUpdateTopologyRequest, newTopo);
}

static void uvReplyClusterInfo(void *p) {
  RedisModuleBlockedClient *bc = p;
  RedisModuleCtx *ctx = RedisModule_GetThreadSafeContext(bc);
  MR_ReplyClusterInfo(ctx, cluster_g->topo);
  RedisModule_FreeThreadSafeContext(ctx);
  RedisModule_BlockedClientMeasureTimeEnd(bc);
  RedisModule_UnblockClient(bc, NULL);
}

void MR_uvReplyClusterInfo(RedisModuleCtx *ctx) {
  RedisModuleBlockedClient *bc = RedisModule_BlockClient(ctx, NULL, NULL, NULL, 0);
  RedisModule_BlockedClientMeasureTimeStart(bc);
  RQ_Push(rq_g, uvReplyClusterInfo, bc);
}

void MR_ReplyClusterInfo(RedisModuleCtx *ctx, MRClusterTopology *topo) {
  RedisModule_Reply _reply = RedisModule_NewReply(ctx), *reply = &_reply;

  const char *hash_func_str;
  switch (topo ? topo->hashFunc : MRHashFunc_None) {
  case MRHashFunc_CRC12:
    hash_func_str = MRHASHFUNC_CRC12_STR;
    break;
  case MRHashFunc_CRC16:
    hash_func_str = MRHASHFUNC_CRC16_STR;
    break;
  default:
    hash_func_str = "n/a";
    break;
  }
  const char *cluster_type_str = clusterConfig.type == ClusterType_RedisOSS ? CLUSTER_TYPE_OSS : CLUSTER_TYPE_RLABS;
  size_t partitions = topo ? topo->numShards : 0;

  //-------------------------------------------------------------------------------------------
  if (reply->resp3) { // RESP3 variant
    RedisModule_Reply_Map(reply); // root

    RedisModule_ReplyKV_LongLong(reply, "num_partitions", partitions);
    RedisModule_ReplyKV_SimpleString(reply, "cluster_type", cluster_type_str);

    RedisModule_ReplyKV_SimpleString(reply, "hash_func", hash_func_str);

    // Report topology
    RedisModule_ReplyKV_LongLong(reply, "num_slots", topo ? (long long)topo->numSlots : 0);

    if (!topo) {
      RedisModule_ReplyKV_Null(reply, "slots");
    } else {
      RedisModule_ReplyKV_Array(reply, "slots"); // >slots
      for (int i = 0; i < topo->numShards; i++) {
        MRClusterShard *sh = &topo->shards[i];

        RedisModule_Reply_Map(reply); // >>(shards)
        RedisModule_ReplyKV_LongLong(reply, "start", sh->startSlot);
        RedisModule_ReplyKV_LongLong(reply, "end", sh->endSlot);

        RedisModule_ReplyKV_Array(reply, "nodes"); // >>>nodes
        for (int j = 0; j < sh->numNodes; j++) {
          MRClusterNode *node = &sh->nodes[j];
          RedisModule_Reply_Map(reply); // >>>>(node)

          RedisModule_ReplyKV_SimpleString(reply, "id", node->id);
          RedisModule_ReplyKV_SimpleString(reply, "host", node->endpoint.host);
          RedisModule_ReplyKV_LongLong(reply, "port", node->endpoint.port);
          RedisModule_ReplyKV_Stringf(reply, "role", "%s%s",
                                      node->flags & MRNode_Master ? "master " : "slave ",
                                      node->flags & MRNode_Self ? "self" : "");

          RedisModule_Reply_MapEnd(reply); // >>>>(node)
        }
        RedisModule_Reply_ArrayEnd(reply); // >>>nodes

        RedisModule_Reply_MapEnd(reply); // >>(shards)
      }
      RedisModule_Reply_ArrayEnd(reply); // >slots
    }

    RedisModule_Reply_MapEnd(reply); // root
  }
  //-------------------------------------------------------------------------------------------
  else // RESP2 variant
  {
    RedisModule_Reply_Array(reply); // root

    RedisModule_ReplyKV_LongLong(reply, "num_partitions", partitions);
    RedisModule_ReplyKV_SimpleString(reply, "cluster_type", cluster_type_str);

    RedisModule_ReplyKV_SimpleString(reply, "hash_func", hash_func_str);

    // Report topology
    RedisModule_ReplyKV_LongLong(reply, "num_slots", topo ? (long long)topo->numSlots : 0);

    RedisModule_Reply_SimpleString(reply, "slots");

    if (!topo) {
      RedisModule_Reply_Null(reply);
    } else {
      for (int i = 0; i < topo->numShards; i++) {
        MRClusterShard *sh = &topo->shards[i];
        RedisModule_Reply_Array(reply); // >shards

        RedisModule_Reply_LongLong(reply, sh->startSlot);
        RedisModule_Reply_LongLong(reply, sh->endSlot);
        for (int j = 0; j < sh->numNodes; j++) {
          MRClusterNode *node = &sh->nodes[j];
          RedisModule_Reply_Array(reply); // >>node
            RedisModule_Reply_SimpleString(reply, node->id);
            RedisModule_Reply_SimpleString(reply, node->endpoint.host);
            RedisModule_Reply_LongLong(reply, node->endpoint.port);
            RedisModule_Reply_Stringf(reply, "%s%s",
                                      node->flags & MRNode_Master ? "master " : "slave ",
                                      node->flags & MRNode_Self ? "self" : "");
          RedisModule_Reply_ArrayEnd(reply); // >>node
        }

        RedisModule_Reply_ArrayEnd(reply); // >shards
      }
    }

    RedisModule_Reply_ArrayEnd(reply); // root
  }
  //-------------------------------------------------------------------------------------------

  RedisModule_EndReply(reply);
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
