#define RMR_C__
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
#include "hiredis/adapters/libuv.h"

#include "rmr.h"
#include "reply.h"
#include "redismodule.h"
#include "cluster.h"
#include "chan.h"
#include "rq.h"

extern int redisMajorVesion;

/* Currently a single cluster is supported */
static MRCluster *cluster_g = NULL;
static MRWorkQueue *rq_g = NULL;

#define MAX_CONCURRENT_REQUESTS (MR_CONN_POOL_SIZE * 50)
/* Coordination request timeout */
long long timeout_g = 5000;

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
  void *redisCtx;
  MRCoordinationStrategy strategy;
  MRCommand *cmds;
  int numCmds;

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

/* The request duration in microsecnds, relevant only on the reducer */
int64_t MR_RequestDuration(MRCtx *ctx) {
  return ((int64_t)1000000 * ctx->endTime.tv_sec + ctx->endTime.tv_nsec / 1000) -
         ((int64_t)1000000 * ctx->startTime.tv_sec + ctx->startTime.tv_nsec / 1000);
}

void MR_SetCoordinationStrategy(MRCtx *ctx, MRCoordinationStrategy strategy) {
  ctx->strategy = strategy;
}

static int totalAllocd = 0;
/* Create a new MapReduce context */
MRCtx *MR_CreateCtx(RedisModuleCtx *ctx, void *privdata) {
  MRCtx *ret = malloc(sizeof(MRCtx));
  clock_gettime(CLOCK_REALTIME, &ret->startTime);
  ret->endTime = ret->startTime;
  ret->firstRespTime = ret->startTime;
  ret->numReplied = 0;
  ret->numErrored = 0;
  ret->numExpected = 0;
  ret->repliesCap = MAX(1, MRCluster_NumShards(cluster_g));
  ret->replies = calloc(ret->repliesCap, sizeof(redisReply *));
  ret->reducer = NULL;
  ret->privdata = privdata;
  ret->strategy = MRCluster_FlatCoordination;
  ret->redisCtx = ctx;
  ret->fn = NULL;
  totalAllocd++;

  return ret;
}

void MRCtx_Free(MRCtx *ctx) {

  for (int i = 0; i < ctx->numCmds; i++) {
    MRCommand_Free(&ctx->cmds[i]);
  }
  free(ctx->cmds);

  for (int i = 0; i < ctx->numReplied; i++) {
    if (ctx->replies[i] != NULL) {
      MRReply_Free(ctx->replies[i]);
      ctx->replies[i] = NULL;
    }
  }
  free(ctx->replies);

  // free the context
  free(ctx);
}

/* Get the user stored private data from the context */
void *MRCtx_GetPrivdata(struct MRCtx *ctx) {
  return ctx->privdata;
}

RedisModuleCtx *MRCtx_GetRedisCtx(struct MRCtx *ctx) {
  return ctx->redisCtx;
}

void MRCtx_SetRedisCtx(struct MRCtx *ctx, void* rctx){
  ctx->redisCtx = rctx;
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

static void freePrivDataCB(void *p) {
  // printf("FreePrivData called!\n");
  MR_requestCompleted();
  if (p) {
    MRCtx *mc = p;
    MRCtx_Free(mc);
  }
}

static void freePrivDataCB_V5(RedisModuleCtx *ctx, void *p) {
  freePrivDataCB(p);
}

static int timeoutHandler(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  RedisModule_Log(ctx, "notice", "Timed out coordination request");
  return RedisModule_ReplyWithError(ctx, "Timeout calling command");
}

/* handler for unblocking redis commands, that calls the actual reducer */
static int unblockHandler(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  RedisModule_AutoMemory(ctx);
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
      ctx->replies = realloc(ctx->replies, ctx->repliesCap * sizeof(MRReply *));
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
      RedisModuleBlockedClient *bc = ctx->redisCtx;
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
};

void requestCb(void *p) {
  struct MRRequestCtx *ctx = p;
  ctx->cb(ctx);
}

/* start the event loop side thread */
static void sideThread(void *arg) {

  // uv_loop_configure(uv_default_loop(), UV_LOOP_BLOCK_SIGNAL)
  while (1) {
    if (uv_run(uv_default_loop(), UV_RUN_DEFAULT)) break;
    usleep(1000);
    fprintf(stderr, "restarting loop!\n");
  }
  fprintf(stderr, "Uv loop exited!\n");
}

uv_thread_t loop_th;

/* Initialize the MapReduce engine with a node provider */
void MR_Init(MRCluster *cl, long long timeoutMS) {

  cluster_g = cl;
  timeout_g = timeoutMS;
  rq_g = RQ_New(8, MAX_CONCURRENT_REQUESTS);

  // MRCluster_ConnectAll(cluster_g);
  printf("Creating thread...\n");

  if (uv_thread_create(&loop_th, sideThread, NULL) != 0) {
    perror("thread create");
    exit(-1);
  }
  printf("Thread created\n");
}

MRClusterTopology *MR_GetCurrentTopology() {
  return cluster_g ? cluster_g->topo : NULL;
}

MRClusterNode *MR_GetMyNode() {
  return cluster_g ? cluster_g->myNode : NULL;
}

/* The fanout request received in the event loop in a thread safe manner */
static void uvFanoutRequest(struct MRRequestCtx *mc) {

  MRCtx *mrctx = mc->ctx;
  mrctx->numReplied = 0;
  mrctx->reducer = mc->f;
  mrctx->numExpected = 0;

  mrctx->numCmds = mc->numCmds;
  mrctx->cmds = calloc(mrctx->numCmds, sizeof(MRCommand));
  for (int i = 0; i < mrctx->numCmds; ++i) {
    mrctx->cmds[i] = mc->cmds[i];
  }

  if (cluster_g->topo) {
    MRCommand *cmd = &mc->cmds[0];
    mrctx->numExpected =
        MRCluster_FanoutCommand(cluster_g, mrctx->strategy, cmd, fanoutCallback, mrctx);
  }

  if (mrctx->numExpected == 0) {
    RedisModuleBlockedClient *bc = mrctx->redisCtx;
    RedisModule_UnblockClient(bc, mrctx);
    // printf("could not send single command. hande fail please\n");
  }

  free(mc->cmds);
  free(mc);
}

static void uvMapRequest(struct MRRequestCtx *mc) {
  MRCtx *mrctx = mc->ctx;
  mrctx->numReplied = 0;
  mrctx->reducer = mc->f;
  mrctx->numExpected = 0;

  mrctx->numCmds = mc->numCmds;
  mrctx->cmds = calloc(mrctx->numCmds, sizeof(MRCommand));
  for (int i = 0; i < mrctx->numCmds; ++i) {
    mrctx->cmds[i] = mc->cmds[i];
  }

  for (int i = 0; i < mc->numCmds; i++) {

    if (MRCluster_SendCommand(cluster_g, mrctx->strategy, &mc->cmds[i], fanoutCallback, mrctx) ==
        REDIS_OK) {
      mrctx->numExpected++;
    }
  }

  if (mrctx->numExpected == 0) {
    RedisModuleBlockedClient *bc = mrctx->redisCtx;
    RedisModule_UnblockClient(bc, mrctx);
    // printf("could not send single command. hande fail please\n");
  }

  free(mc->cmds);
  free(mc);

  // return REDIS_OK;
}

void MR_requestCompleted() {
  RQ_Done(rq_g);
}

/* Fanout map - send the same command to all the shards, sending the collective
 * reply to the reducer callback */
int MR_Fanout(struct MRCtx *ctx, MRReduceFunc reducer, MRCommand cmd, bool block) {

  struct MRRequestCtx *rc = malloc(sizeof(struct MRRequestCtx));
  if (block) {
    ctx->redisCtx = RedisModule_BlockClient(
        ctx->redisCtx, unblockHandler, timeoutHandler,
        redisMajorVesion < 5 ? (void (*)(RedisModuleCtx *, void *))freePrivDataCB : freePrivDataCB_V5,
        timeout_g);
  }
  rc->ctx = ctx;
  rc->f = reducer;
  rc->cmds = calloc(1, sizeof(MRCommand));
  rc->numCmds = 1;
  rc->cmds[0] = cmd;
  rc->cb = uvFanoutRequest;
  RQ_Push(rq_g, requestCb, rc);
  return REDIS_OK;
}

int MR_Map(struct MRCtx *ctx, MRReduceFunc reducer, MRCommandGenerator cmds, bool block) {
  struct MRRequestCtx *rc = malloc(sizeof(struct MRRequestCtx));
  rc->ctx = ctx;
  rc->f = reducer;
  rc->cmds = calloc(cmds.Len(cmds.ctx), sizeof(MRCommand));
  rc->numCmds = cmds.Len(cmds.ctx);

  // copy the commands from the iterator to the conext's array
  for (int i = 0; i < rc->numCmds; i++) {
    if (!cmds.Next(cmds.ctx, &rc->cmds[i])) {
      rc->numCmds = i;
      break;
    }
  }

  if (block) {
    ctx->redisCtx = RedisModule_BlockClient(ctx->redisCtx, unblockHandler, timeoutHandler,
                                            redisMajorVesion < 5
                                                ? (void (*)(RedisModuleCtx *, void *))freePrivDataCB
                                                : freePrivDataCB_V5,
                                            timeout_g);
  }

  rc->cb = uvMapRequest;
  RQ_Push(rq_g, requestCb, rc);

  return REDIS_OK;
}

int MR_MapSingle(struct MRCtx *ctx, MRReduceFunc reducer, MRCommand cmd) {

  struct MRRequestCtx *rc = malloc(sizeof(struct MRRequestCtx));
  rc->ctx = ctx;
  rc->f = reducer;
  rc->cmds = calloc(1, sizeof(MRCommand));
  rc->numCmds = 1;
  rc->cmds[0] = cmd;
  ctx->redisCtx = RedisModule_BlockClient(
      ctx->redisCtx, unblockHandler, timeoutHandler,
      redisMajorVesion < 5 ? (void (*)(RedisModuleCtx *, void *))freePrivDataCB : freePrivDataCB_V5,
      timeout_g);

  rc->cb = uvMapRequest;
  RQ_Push(rq_g, requestCb, rc);
  return REDIS_OK;
}

/* Return the active cluster's host count */
size_t MR_NumHosts() {
  return cluster_g ? MRCluster_NumHosts(cluster_g) : 0;
}

void SetMyPartition(MRClusterTopology *ct, MRClusterShard *myShard);
/* on-loop update topology request. This can't be done from the main thread */
static void uvUpdateTopologyRequest(struct MRRequestCtx *mc) {
  MRCLuster_UpdateTopology(cluster_g, (MRClusterTopology *)mc->ctx);
  SetMyPartition((MRClusterTopology *)mc->ctx, cluster_g->myshard);
  RQ_Done(rq_g);
  // fprintf(stderr, "topo update: conc requests: %d\n", concurrentRequests_g);
  free(mc);
}

/* Set a new topology for the cluster */
int MR_UpdateTopology(MRClusterTopology *newTopo) {
  if (cluster_g == NULL) {
    return REDIS_ERR;
  }


  // enqueue a request on the io thread, this can't be done from the main thread
  struct MRRequestCtx *rc = calloc(1, sizeof(*rc));
  rc->ctx = newTopo;
  rc->cb = uvUpdateTopologyRequest;
  RQ_Push(rq_g, requestCb, rc);
  return REDIS_OK;
}

struct MRIteratorCallbackCtx;

typedef int (*MRIteratorCallback)(struct MRIteratorCallbackCtx *ctx, MRReply *rep, MRCommand *cmd);

typedef struct MRIteratorCtx {
  MRCluster *cluster;
  MRChannel *chan;
  void *privdata;
  MRIteratorCallback cb;
  int pending;
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
    ctx->ic->cb(ctx, r, &ctx->cmd);
  }
}

int MRIteratorCallback_ResendCommand(MRIteratorCallbackCtx *ctx, MRCommand *cmd) {
  ctx->cmd = *cmd;
  return MRCluster_SendCommand(ctx->ic->cluster, MRCluster_MastersOnly, cmd, mrIteratorRedisCB,
                               ctx);
}

void *MRITERATOR_DONE = "MRITERATOR_DONE";

int MRIteratorCallback_Done(MRIteratorCallbackCtx *ctx, int error) {
  if (--ctx->ic->pending <= 0) {
    // fprintf(stderr, "FINISHED iterator, error? %d pending %d\n", error, ctx->ic->pending);
    RQ_Done(rq_g);

    MRChannel_Close(ctx->ic->chan);
    return 0;
  }
  // fprintf(stderr, "Done iterator, error? %d pending %d\n", error, ctx->ic->pending);

  return 1;
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

MRIterator *MR_Iterate(MRCommandGenerator cg, MRIteratorCallback cb, void *privdata) {

  MRIterator *ret = malloc(sizeof(*ret));
  size_t len = cg.Len(cg.ctx);
  *ret = (MRIterator){
      .ctx =
          {
              .cluster = cluster_g,
              .chan = MR_NewChannel(0),
              .privdata = privdata,
              .cb = cb,
              .pending = 0,
          },
      .cbxs = calloc(len, sizeof(MRIteratorCallbackCtx)),
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

  RQ_Push(rq_g, iterStartCb, ret);
  return ret;
}

MRReply *MRIterator_Next(MRIterator *it) {

  void *p = MRChannel_Pop(it->ctx.chan);
  // fprintf(stderr, "POP: %s\n", p == MRCHANNEL_CLOSED ? "CLOSED" : "ITER");
  if (p == MRCHANNEL_CLOSED) {
    return MRITERATOR_DONE;
  }
  return p;
}

void MRIterator_WaitDone(MRIterator *it) {
  MRChannel_WaitClose(it->ctx.chan);
}
void MRIterator_Free(MRIterator *it) {
  if (!it) return;
  for (size_t i = 0; i < it->len; i++) {
    MRCommand_Free(&it->cbxs[i].cmd);
  }
  MRReply *reply;
  while((reply = MRChannel_ForcePop(it->ctx.chan))){
      MRReply_Free(reply);
  }
  MRChannel_Free(it->ctx.chan);
  free(it->cbxs);
  free(it);
}
