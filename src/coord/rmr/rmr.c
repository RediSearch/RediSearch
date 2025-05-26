/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#include "rmr.h"
#include "reply.h"
#include "reply_macros.h"
#include "redismodule.h"
#include "cluster.h"
#include "chan.h"
#include "rq.h"
#include "rmutil/rm_assert.h"
#include "resp3.h"
#include "coord/config.h"

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

#define REFCOUNT_INCR_MSG(caller, refcount) \
  RS_DEBUG_LOG_FMT("%s: increased refCount to == %d", caller, refcount);
#define REFCOUNT_DECR_MSG(caller, refcount) \
  RS_DEBUG_LOG_FMT("%s: decreased refCount to == %d", caller, refcount);

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
  bool mastersOnly;
  MRCommand cmd;

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

void MR_SetCoordinationStrategy(MRCtx *ctx, bool mastersOnly) {
  ctx->mastersOnly = mastersOnly;
}

/* Create a new MapReduce context */
MRCtx *MR_CreateCtx(RedisModuleCtx *ctx, RedisModuleBlockedClient *bc, void *privdata, int replyCap) {
  MRCtx *ret = rm_malloc(sizeof(MRCtx));
  ret->numReplied = 0;
  ret->numErrored = 0;
  ret->numExpected = 0;
  ret->repliesCap = replyCap;
  ret->replies = rm_calloc(ret->repliesCap, sizeof(redisReply *));
  ret->reducer = NULL;
  ret->privdata = privdata;
  ret->mastersOnly = true; // default to masters only
  ret->redisCtx = ctx;
  ret->bc = bc;
  RS_ASSERT(ctx || bc);
  ret->fn = NULL;

  return ret;
}

void MRCtx_Free(MRCtx *ctx) {

  MRCommand_Free(&ctx->cmd);

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

RedisModuleBlockedClient *MRCtx_GetBlockedClient(struct MRCtx *ctx) {
  return ctx->bc;
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
      RS_ASSERT(bc);
      RedisModule_BlockedClientMeasureTimeEnd(bc);
      RedisModule_UnblockClient(bc, ctx);
    }
  }
}

// `*50` for following the previous behavior
// #define MAX_CONCURRENT_REQUESTS (MR_CONN_POOL_SIZE * 50)
#define PENDING_FACTOR 50
/* Initialize the MapReduce engine with a node provider */
void MR_Init(MRCluster *cl, long long timeoutMS) {

  cluster_g = cl;
  timeout_g = timeoutMS;
  rq_g = RQ_New(cl->mgr.nodeConns * PENDING_FACTOR);
}

int MR_CheckTopologyConnections(bool mastersOnly) {
  return MRCluster_CheckConnections(cluster_g, mastersOnly);
}

bool MR_CurrentTopologyExists() {
  return cluster_g->topo != NULL;
}

/* The fanout request received in the event loop in a thread safe manner */
static void uvFanoutRequest(void *p) {
  MRCtx *mrctx = p;

  mrctx->numExpected =
      MRCluster_FanoutCommand(cluster_g, mrctx->mastersOnly, &mrctx->cmd, fanoutCallback, mrctx);

  if (mrctx->numExpected == 0) {
    RedisModuleBlockedClient *bc = mrctx->bc;
    RS_ASSERT(bc);
    RedisModule_BlockedClientMeasureTimeEnd(bc);
    RedisModule_UnblockClient(bc, mrctx);
  }
}

static void uvMapRequest(void *p) {
  MRCtx *mrctx = p;

  int rc = MRCluster_SendCommand(cluster_g, mrctx->mastersOnly, &mrctx->cmd, fanoutCallback, mrctx);
  mrctx->numExpected = (rc == REDIS_OK) ? 1 : 0;

  if (mrctx->numExpected == 0) {
    RedisModuleBlockedClient *bc = mrctx->bc;
    RS_ASSERT(bc);
    RedisModule_BlockedClientMeasureTimeEnd(bc);
    RedisModule_UnblockClient(bc, mrctx);
  }
}

void MR_requestCompleted() {
  RQ_Done(rq_g);
}

/* Fanout map - send the same command to all the shards, sending the collective
 * reply to the reducer callback */
int MR_Fanout(struct MRCtx *mrctx, MRReduceFunc reducer, MRCommand cmd, bool block) {
  if (block) {
    RS_ASSERT(!mrctx->bc);
    mrctx->bc = RedisModule_BlockClient(
        mrctx->redisCtx, unblockHandler, timeoutHandler, freePrivDataCB, 0); // timeout_g);
    RedisModule_BlockedClientMeasureTimeStart(mrctx->bc);
  }
  mrctx->reducer = reducer;
  mrctx->cmd = cmd;
  RQ_Push(rq_g, uvFanoutRequest, mrctx);
  return REDIS_OK;
}

int MR_MapSingle(struct MRCtx *ctx, MRReduceFunc reducer, MRCommand cmd) {
  ctx->reducer = reducer;
  ctx->cmd = cmd;
  RS_ASSERT(!ctx->bc);
  ctx->bc = RedisModule_BlockClient(ctx->redisCtx, unblockHandler, timeoutHandler, freePrivDataCB, 0); // timeout_g);
  RedisModule_BlockedClientMeasureTimeStart(ctx->bc);
  RQ_Push(rq_g, uvMapRequest, ctx);
  return REDIS_OK;
}

/* on-loop update topology request. This can't be done from the main thread */
static void uvUpdateTopologyRequest(void *p) {
  MRCLuster_UpdateTopology(cluster_g, p);
}

/* Set a new topology for the cluster */
void MR_UpdateTopology(MRClusterTopology *newTopo) {
  // enqueue a request on the io thread, this can't be done from the main thread
  RQ_Push_Topology(uvUpdateTopologyRequest, newTopo);
}

/* Modifying the connection pools cannot be done from the main thread */
static void uvUpdateConnPerShard(void *p) {
  size_t connPerShard = (uintptr_t)p;
  MRCluster_UpdateConnPerShard(cluster_g, connPerShard);
  RQ_UpdateMaxPending(rq_g, connPerShard * PENDING_FACTOR);
  MR_requestCompleted();
}

extern size_t NumShards;
void MR_UpdateConnPerShard(size_t connPerShard) {
  if (!rq_g) return; // not initialized yet, we have nothing to update yet.
  if (NumShards == 1) {
    // If we observe that there is only one shard from the main thread,
    // we know the uv thread is not initialized yet (and may never be).
    // We can update the connection pool size directly from the main thread.
    // This is mostly a no-op, as the connection pool is not in use (yet or at all).
    // This call should only update the connection pool `size` for when the connection pool is initialized.
    MRCluster_UpdateConnPerShard(cluster_g, connPerShard);
  } else {
    void *p = (void *)(uintptr_t)connPerShard;
    RQ_Push(rq_g, uvUpdateConnPerShard, p);
  }
}

static void uvGetConnectionPoolState(void *p) {
  RedisModuleBlockedClient *bc = p;
  RedisModuleCtx *ctx = RedisModule_GetThreadSafeContext(bc);
  MRConnManager_ReplyState(&cluster_g->mgr, ctx);
  MR_requestCompleted();
  RedisModule_FreeThreadSafeContext(ctx);
  RedisModule_BlockedClientMeasureTimeEnd(bc);
  RedisModule_UnblockClient(bc, NULL);
}

void MR_GetConnectionPoolState(RedisModuleCtx *ctx) {
  RedisModuleBlockedClient *bc = RedisModule_BlockClient(ctx, NULL, NULL, NULL, 0);
  RedisModule_BlockedClientMeasureTimeStart(bc);
  RQ_Push(rq_g, uvGetConnectionPoolState, bc);
}

static void uvReplyClusterInfo(void *p) {
  RedisModuleBlockedClient *bc = p;
  RedisModuleCtx *ctx = RedisModule_GetThreadSafeContext(bc);
  MR_ReplyClusterInfo(ctx, cluster_g->topo);
  MR_requestCompleted();
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

          REPLY_KVSTR_SAFE("id", node->id);
          REPLY_KVSTR_SAFE("host", node->endpoint.host);
          RedisModule_ReplyKV_LongLong(reply, "port", node->endpoint.port);
          RedisModule_ReplyKV_SimpleStringf(reply, "role", "%s%s",                        // TODO: move the space to "self"
                                      node->flags & MRNode_Master ? "master " : "slave ", // "master" : "slave",
                                      node->flags & MRNode_Self ? "self" : "");           // " self" : ""

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
            REPLY_SIMPLE_SAFE(node->id);
            REPLY_SIMPLE_SAFE(node->endpoint.host);
            RedisModule_Reply_LongLong(reply, node->endpoint.port);
            RedisModule_Reply_SimpleStringf(reply, "%s%s",                                // TODO: move the space to "self"
                                      node->flags & MRNode_Master ? "master " : "slave ", // "master" : "slave",
                                      node->flags & MRNode_Self ? "self" : "");           // " self" : ""
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

struct MRIteratorCtx {
  MRChannel *chan;
  MRIteratorCallback cb;
  short pending;    // Number of shards with more results (not depleted)
  short inProcess;  // Number of currently running commands on shards
  bool timedOut;    // whether the coordinator experienced a timeout
  // reference counter of the iterator.
  // When it reaches 0, both readers and the writer agree that the iterator can be released
  int8_t itRefCount;
};

struct MRIteratorCallbackCtx {
  MRIterator *it;
  MRCommand cmd;
};

struct MRIterator {
  MRIteratorCtx ctx;
  MRIteratorCallbackCtx *cbxs;
  size_t len;
};

static void mrIteratorRedisCB(redisAsyncContext *c, void *r, void *privdata) {
  MRIteratorCallbackCtx *ctx = privdata;
  if (!r) {
    MRIteratorCallback_Done(ctx, 1);
    // ctx->numErrored++;
    // TODO: report error
  } else {
    ctx->it->ctx.cb(ctx, r);
  }
}

int MRIteratorCallback_ResendCommand(MRIteratorCallbackCtx *ctx) {
  return MRCluster_SendCommand(cluster_g, true, &ctx->cmd, mrIteratorRedisCB, ctx);
}

// Use after modifying `pending` (or any other variable of the iterator) to make sure it's visible
// to other threads
void MRIteratorCallback_ProcessDone(MRIteratorCallbackCtx *ctx) {
  short inProcess = __atomic_sub_fetch(&ctx->it->ctx.inProcess, 1, __ATOMIC_RELEASE);
  if (!inProcess) {
    MRChannel_Unblock(ctx->it->ctx.chan);
    RS_DEBUG_LOG("MRIteratorCallback_ProcessDone: calling MRIterator_Release");
    MRIterator_Release(ctx->it);
    RQ_Done(rq_g);
  }
}

// Use before obtaining `pending` (or any other variable of the iterator) to make sure it's synchronized with other threads
static short MRIteratorCallback_GetNumInProcess(MRIterator *it) {
  return __atomic_load_n(&it->ctx.inProcess, __ATOMIC_ACQUIRE);
}

short MRIterator_GetPending(MRIterator *it) {
  return __atomic_load_n(&it->ctx.pending, __ATOMIC_ACQUIRE);
}

bool MRIteratorCallback_GetTimedOut(MRIteratorCtx *ctx) {
  return __atomic_load_n(&ctx->timedOut, __ATOMIC_ACQUIRE);
}

void MRIteratorCallback_SetTimedOut(MRIteratorCtx *ctx) {
  // Atomically set the timedOut field of the ctx
  __atomic_store_n(&ctx->timedOut, true, __ATOMIC_RELAXED);
}

void MRIteratorCallback_ResetTimedOut(MRIteratorCtx *ctx) {
  // Set the `timedOut` field to false
  __atomic_store_n(&ctx->timedOut, false, __ATOMIC_RELAXED);
}

static inline int8_t MRIterator_IncreaseRefCount(MRIterator *it) {
  return  __atomic_add_fetch(&it->ctx.itRefCount, 1, __ATOMIC_ACQUIRE);
}

static inline int8_t MRIterator_DecreaseRefCount(MRIterator *it) {
  return  __atomic_sub_fetch(&it->ctx.itRefCount, 1, __ATOMIC_ACQUIRE);
}

void MRIteratorCallback_Done(MRIteratorCallbackCtx *ctx, int error) {
  // Mark the command of the context as depleted (so we won't send another command to the shard)
  RS_DEBUG_LOG_FMT(
      "depleted(should be false): %d, Pending: (%d), inProcess: %d, itRefCount: %d, channel size: "
      "%zu, shard_slot: %d",
      ctx->cmd.depleted, ctx->it->ctx.pending, ctx->it->ctx.inProcess, ctx->it->ctx.itRefCount,
      MRChannel_Size(ctx->it->ctx.chan), ctx->cmd.targetSlot);
  ctx->cmd.depleted = true;
  short pending = --ctx->it->ctx.pending; // Decrease `pending` before decreasing `inProcess`
  RS_ASSERT(pending >= 0);
  MRIteratorCallback_ProcessDone(ctx);
}

MRCommand *MRIteratorCallback_GetCommand(MRIteratorCallbackCtx *ctx) {
  return &ctx->cmd;
}

MRIteratorCtx *MRIteratorCallback_GetCtx(MRIteratorCallbackCtx *ctx) {
  return &ctx->it->ctx;
}

void MRIteratorCallback_AddReply(MRIteratorCallbackCtx *ctx, MRReply *rep) {
  MRChannel_Push(ctx->it->ctx.chan, rep);
}

void iterStartCb(void *p) {
  MRIterator *it = p;

  size_t len = cluster_g->topo->numShards;
  it->len = len;
  it->ctx.pending = len;
  it->ctx.inProcess = len; // Initially all commands are in process

  it->cbxs = rm_realloc(it->cbxs, len * sizeof(*it->cbxs));
  MRCommand *cmd = &it->cbxs->cmd;
  cmd->targetSlot = cluster_g->topo->shards[0].startSlot; // Set the first command to target the first shard
  for (size_t i = 1; i < len; i++) {
    it->cbxs[i].it = it;
    it->cbxs[i].cmd = MRCommand_Copy(cmd);
    // Set each command to target a different shard
    it->cbxs[i].cmd.targetSlot = cluster_g->topo->shards[i].startSlot;
  }

  for (size_t i = 0; i < it->len; i++) {
    if (MRCluster_SendCommand(cluster_g, true, &it->cbxs[i].cmd,
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
      if (MRCluster_SendCommand(cluster_g, true, &it->cbxs[i].cmd,
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
  // We have <= channelThreshold replies to process, so if there are pending commands we want to trigger them.
  if (it->ctx.pending) {
    // We have more commands to send
    it->ctx.inProcess = it->ctx.pending;
    // All reader have marked that they are done with the current command batch (decreased inProcess)
    // However, they may still hold the iterator reference.
    // We need to take a reference to the iterator for the next batch of commands.
    int8_t refCount = MRIterator_IncreaseRefCount(it);
    REFCOUNT_INCR_MSG("MR_ManuallyTriggerNextIfNeeded", refCount);
    RQ_Push(rq_g, iterManualNextCb, it);
    return true; // We may have more replies (and we surely will)
  }
  // We have no pending commands and no more than channelThreshold replies to process.
  // If we have more replies we will process them, otherwise we are done.
  return channelSize > 0;
}

MRIterator *MR_Iterate(const MRCommand *cmd, MRIteratorCallback cb) {

  MRIterator *ret = rm_new(MRIterator);
  // Initial initialization of the iterator.
  // The rest of the initialization is done in the iterator start callback.
  // We set `pending` and `inProcess` to 1 so we won't get the impression that we are done
  // before the first command is sent. This is also technically correct since we know that we have
  // at least ourselves to wait for.
  // The reference count is set to 2:
  // - one ref for the writers (shards)
  // - one for the reader (the coord)
  *ret = (MRIterator){
    .ctx = {
      .chan = MR_NewChannel(),
      .cb = cb,
      .pending = 1,
      .inProcess = 1,
      .timedOut = false,
      .itRefCount = 2,
    },
    .cbxs = rm_new(MRIteratorCallbackCtx),
  };
  // Initialize the first command
  *ret->cbxs = (MRIteratorCallbackCtx){
    .cmd = MRCommand_Copy(cmd),
    .it = ret,
  };

  RQ_Push(rq_g, iterStartCb, ret);
  return ret;
}

MRIteratorCtx *MRIterator_GetCtx(MRIterator *it) {
  return &it->ctx;
}

MRReply *MRIterator_Next(MRIterator *it) {
  return MRChannel_Pop(it->ctx.chan);
}

// Assumes no other thread is using the iterator, the channel, or any of the commands and contexts
static void MRIterator_Free(MRIterator *it) {
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

void MRIterator_Release(MRIterator *it) {
  int8_t refcount = MRIterator_DecreaseRefCount(it);
  REFCOUNT_DECR_MSG("MRIterator_Release", refcount);
  RS_ASSERT(refcount >= 0);
  if (refcount > 0) return;

  // Both reader and writers are done with the iterator. No writer is in process.
  if (it->ctx.pending) {
    // If we have pending (not depleted) shards, trigger `FT.CURSOR DEL` on them
    it->ctx.inProcess = it->ctx.pending;
    // Change the root command to DEL for each pending shard
    for (size_t i = 0; i < it->len; i++) {
      MRCommand *cmd = &it->cbxs[i].cmd;
      if (!cmd->depleted) {
        RS_DEBUG_LOG_FMT("changing command from %s to DEL for shard: %d", cmd->strs[1], cmd->targetSlot);
        RS_LOG_ASSERT_FMT(cmd->rootCommand != C_DEL, "DEL command should be sent only once to a shard. pending = %d", it->ctx.pending);
        cmd->rootCommand = C_DEL;
        strcpy(cmd->strs[1], "DEL");
        cmd->lens[1] = 3;
      }
    }
    // Take a reference to the iterator for the next batch of commands.
    // The iterator will be released when DEL commands are done.
    refcount = MRIterator_IncreaseRefCount(it);
    REFCOUNT_INCR_MSG("MRIterator_Release: triggering DEL on the shards' cursors", refcount);
    RQ_Push(rq_g, iterManualNextCb, it);
  } else {
    // No pending shards, so no remote resources to free.
    // Free the iterator and we are done.
    RS_DEBUG_LOG("MRIterator_Release: calling MRIterator_Free");
    MRIterator_Free(it);
  }
}
