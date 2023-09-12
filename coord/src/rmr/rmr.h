/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */


#pragma once

#include <stdbool.h>

#include "reply.h"
#include "cluster.h"
#include "command.h"

struct MRCtx;
struct RedisModuleCtx;

/* Prototype for all reduce functions */
typedef int (*MRReduceFunc)(struct MRCtx *ctx, int count, MRReply **replies);

/* Fanout map - send the same command to all the shards, sending the collective
 * reply to the reducer callback */
int MR_Fanout(struct MRCtx *ctx, MRReduceFunc reducer, MRCommand cmd, bool block);

int MR_Map(struct MRCtx *ctx, MRReduceFunc reducer, MRCommandGenerator cmds, bool block);

int MR_MapSingle(struct MRCtx *ctx, MRReduceFunc reducer, MRCommand cmd);

void MR_SetCoordinationStrategy(struct MRCtx *ctx, MRCoordinationStrategy strategy);

/* Initialize the MapReduce engine with a node provider */
void MR_Init(MRCluster *cl, long long timeoutMS);
/* Cleanup used resources at exit */
void MR_Destroy();

/* Set a new topology for the cluster */
int MR_UpdateTopology(MRClusterTopology *newTopology);

/* Get the current cluster topology */
MRClusterTopology *MR_GetCurrentTopology();

/* Return our current node as detected by cluster state calls */
MRClusterNode *MR_GetMyNode();

/* Get the user stored private data from the context */
void *MRCtx_GetPrivData(struct MRCtx *ctx);

/* The request duration in microsecnds, relevant only on the reducer */
int64_t MR_RequestDuration(struct MRCtx *ctx);

struct RedisModuleCtx *MRCtx_GetRedisCtx(struct MRCtx *ctx);
int MRCtx_GetNumReplied(struct MRCtx *ctx);
MRReply** MRCtx_GetReplies(struct MRCtx *ctx);
void MRCtx_SetRedisCtx(struct MRCtx *ctx, void* rctx);
RedisModuleBlockedClient *MRCtx_GetBlockedClient(struct MRCtx *ctx);
int MRCtx_GetProtocol(struct MRCtx *ctx);
void MRCtx_SetProtocol(struct MRCtx *ctx, int protocol);
MRCommand *MRCtx_GetCmds(struct MRCtx *ctx);
int MRCtx_GetCmdsSize(struct MRCtx *ctx);
void MRCtx_SetReduceFunction(struct MRCtx *ctx, MRReduceFunc fn);
void MR_requestCompleted();


/* Free the MapReduce context */
void MRCtx_Free(struct MRCtx *ctx);

/* Create a new MapReduce context with a given private data. In a redis module
 * this should be the RedisModuleCtx */
struct MRCtx *MR_CreateCtx(struct RedisModuleCtx *ctx, struct RedisModuleBlockedClient *bc, void *privdata);

extern void *MRITERATOR_DONE;

#ifndef RMR_C__
typedef struct MRIteratorCallbackCtx MRIteratorCallbackCtx;
typedef struct MRIterator MRIterator;

typedef int (*MRIteratorCallback)(MRIteratorCallbackCtx *ctx, MRReply *rep, MRCommand *cmd);

// Trigger all the commands in the iterator to be sent, and return the number of pending commands
size_t MR_ManuallyTriggerNextIfNeeded(MRIterator *it);

MRReply *MRIterator_Next(MRIterator *it);

MRIterator *MR_Iterate(MRCommandGenerator cg, MRIteratorCallback cb, void *privdata);

int MRIteratorCallback_AddReply(MRIteratorCallbackCtx *ctx, MRReply *rep);

int MRIteratorCallback_Done(MRIteratorCallbackCtx *ctx, int error);

void MRIteratorCallback_ProcessDone(MRIteratorCallbackCtx *ctx);

int MRIteratorCallback_ResendCommand(MRIteratorCallbackCtx *ctx, MRCommand *cmd);

void MRIterator_Free(MRIterator *it);

/* Wait until the iterators producers are all  done */
void MRIterator_WaitDone(MRIterator *it);

#endif // RMR_C__

size_t MR_NumHosts();
