/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#pragma once

#include <stdbool.h>
#include "util/rs_atomic.h"

#ifdef __cplusplus
extern "C" {
#else
#include <stdatomic.h>
#endif

#include "reply.h"
#include "cluster.h"
#include "command.h"
#include "util/references.h"
#include <unistd.h>

typedef struct QueryError QueryError;

// Error detail returned to the client when a query cannot be dispatched to the
// cluster (pre-fanout connection-validation / send failure). Shared by the MR
// iterator no-reply path (rmr.c) and the hybrid cursor-mapping error callback;
// tests assert on this substring, so keep them in sync via this single macro.
#define CLUSTER_QUERY_ERROR "Could not send query to cluster"

struct MRCtx;
struct RedisModuleCtx;

// r/w lock protected wrapper for the local node ID string
typedef struct {
  char *node_id;
  pthread_rwlock_t lock;
} NodeIdRef;

void iterStartCb(void *p);

void iterCursorMappingCb(void *p);

/* Prototype for all reduce functions */
typedef int (*MRReduceFunc)(struct MRCtx *ctx, int count, MRReply **replies);
typedef void (*MRCtxFreePrivDataCB)(struct MRCtx *ctx);

/* Fanout map - send the same command to all the shards, sending the collective
 * reply to the reducer callback */
int MR_Fanout(struct MRCtx *ctx, MRReduceFunc reducer, MRCommand cmd, bool block);

/* Initialize the MapReduce engine with a given number of I/O threads and connections per each node in the Cluster */
void MR_Init(size_t num_io_threads, size_t conn_pool_size, long long timeoutMS);

/* @brief Set a new topology for the cluster and refresh local slots information.
 * @param newTopology The new cluster topology, consumed by this function.
 * @param localSlots The local slots information to refresh. Does NOT take ownership.
 */
void MR_UpdateTopology(MRClusterTopology *newTopology, const RedisModuleSlotRangeArray *localSlots);

/* @brief Initialize the local node ID structure. */
void MR_InitLocalNodeId();

/* @brief Set the local node ID for this shard while holding the write lock.
 * @param node_id The node ID string to set. Will be duplicated internally.
 */
void MR_SetLocalNodeId(const char *node_id);

/* @brief Get the local node ID for this shard.
 * The caller must call MR_ReleaseLocalNodeId() when done using the returned string.
 */
const char* MR_GetLocalNodeId(void);

/* @brief Release the local node ID handle obtained from MR_GetLocalNodeId().
 * Must be called after MR_GetLocalNodeId() to release the read lock.
 */
void MR_ReleaseLocalNodeIdReadLock();

/* @brief Free the local node ID structure. */
void MR_FreeLocalNodeId();

void MR_ReplyClusterInfo(RedisModuleCtx *ctx, MRClusterTopology *topo);

void MR_GetConnectionPoolState(RedisModuleCtx *ctx);

void MR_uvReplyClusterInfo(RedisModuleCtx *ctx);

void MR_UpdateConnPoolSize(size_t conn_pool_size);

void MR_Debug_ClearPendingTopo();

void MR_FreeCluster();

/* Get the user stored private data from the context */
void *MRCtx_GetPrivData(struct MRCtx *ctx);

struct RedisModuleCtx *MRCtx_GetRedisCtx(struct MRCtx *ctx);
int MRCtx_GetNumReplied(struct MRCtx *ctx);
MRReply** MRCtx_GetReplies(struct MRCtx *ctx);
RedisModuleBlockedClient *MRCtx_GetBlockedClient(struct MRCtx *ctx);
void MRCtx_SetReduceFunction(struct MRCtx *ctx, MRReduceFunc fn);

int MRCtx_GetCommandProtocol(struct MRCtx *ctx);

QueryError *MRCtx_GetStatus(struct MRCtx *ctx);
void MRCtx_IncrRef(struct MRCtx *ctx);
void MRCtx_DecrRef(struct MRCtx *ctx);
void MRCtx_SetFreePrivDataCB(struct MRCtx *ctx, MRCtxFreePrivDataCB cb);

/* Set the blocked client for the context (used when MRCtx is created before blocking) */
void MRCtx_SetBlockedClient(struct MRCtx *ctx, RedisModuleBlockedClient *bc);

/* Timeout and reducing state management for partial timeout support */
void MRCtx_SetTimedOut(struct MRCtx *ctx);
bool MRCtx_IsTimedOut(struct MRCtx *ctx);
bool MRCtx_TryClaimReducing(struct MRCtx *ctx);
void MRCtx_SignalReducerComplete(struct MRCtx *ctx);
void MRCtx_WaitForReducerComplete(struct MRCtx *ctx);

void MRCtx_SetValidateConnections(struct MRCtx *ctx, bool validateConnections);
bool MRCtx_GetValidateConnections(struct MRCtx *ctx);

/* Create a new MapReduce context with a given private data. In a redis module
 * this should be the RedisModuleCtx */
struct MRCtx *MR_CreateCtx(struct RedisModuleCtx *ctx, struct RedisModuleBlockedClient *bc, void *privdata, int replyCap);

typedef struct MRIteratorCallbackCtx MRIteratorCallbackCtx;
typedef struct MRIteratorCtx MRIteratorCtx;
typedef struct MRIterator MRIterator;

/**
 * Per-reply callback, invoked on the IO thread for every shard reply.
 * Owns the iterator's completion bookkeeping: it must call
 * MRIteratorCallback_Done once the shard has no more replies to drive the
 * iterator toward depletion. Contrast with MRIteratorErrorCallback, which is
 * notify-only and must not touch the Done state.
 */
typedef void (*MRIteratorCallback)(MRIteratorCallbackCtx *ctx, MRReply *rep);

/**
 * Invoked on the IO thread when a shard command terminates without a reply
 * (NULL async reply or synchronous send failure). Notify-only: must not free
 * the iterator nor call MRIteratorCallback_Done — the MR layer does that next.
 * Optional; NULL preserves the historical depletion-only behavior.
 */
typedef void (*MRIteratorErrorCallback)(MRIteratorCallbackCtx *ctx);

/**
 * Callback type for modifying commands before they are sent to shards.
 * Called from iterStartCb on the IO thread after numShards is known but before
 * commands are sent.
 * This allows calculating values like effectiveK based on the actual topology.
 *
 * @param cmd The command to modify (will be copied for each shard after this callback)
 * @param numShards The actual number of shards from the IO thread's topology
 * @param privateData The private data passed to MR_IterateWithPrivateData
 */
typedef void (*MRCommandModifier)(MRCommand *cmd, size_t numShards, void *privateData);

/**
 * Bundles the callbacks and private data for MR_IterateWithPrivateData.
 * `successCB` and `iterStartCb` are required (MR_IterateWithPrivateData
 * unconditionally schedules `iterStartCb`); every other field may be NULL to
 * opt out of that hook.
 *
 * @param successCB              Per-reply callback (required).
 * @param errorCB                No-reply termination callback (optional).
 * @param cbPrivateData          Private data handed to `successCB` via the callback ctx.
 * @param cbPrivateDataDestructor Frees `cbPrivateData` when the iterator is freed.
 * @param cbPrivateDataInit      Runs once on the IO thread after numShards is known.
 * @param commandModifier        Rewrites the command per-shard before sending.
 * @param iterStartCb            Scheduled on the IO thread to trigger the first send (required).
 * @param iterStartCbPrivateData StrongRef demoted and passed to `iterStartCb`.
 */
typedef struct {
  MRIteratorCallback successCB;
  MRIteratorErrorCallback errorCB;
  void *cbPrivateData;
  void (*cbPrivateDataDestructor)(void *);
  void (*cbPrivateDataInit)(void *, const MRIterator *);
  MRCommandModifier commandModifier;
  void (*iterStartCb)(void *);
  StrongRef *iterStartCbPrivateData;
} MRIteratorConfig;

// Trigger all the commands in the iterator to be sent.
// Returns true if there may be more replies to come, false if we are done.
bool MR_ManuallyTriggerNextIfNeeded(MRIterator *it, size_t channelThreshold);

MRReply *MRIterator_Next(MRIterator *it);

/* Get next reply, with optional CLOCK_MONOTONIC_RAW deadline (`abstime`) and/or
 * abort flag (pair with MRChannel_WakeAbort). `timedOut` set if deadline expired.
 * At least one of `abstime` / `abortFlag` must be non-NULL; for an indefinite
 * blocking next, use MRIterator_Next. */
MRReply *MRIterator_NextWithTimeout(MRIterator *it, const struct timespec *abstime,
                                    RS_Atomic(bool) *abortFlag, bool *timedOut);

/* Return the underlying channel used by the iterator. Intended for callers that need to
 * invoke MRChannel_WakeAbort directly (e.g. from a timeout callback on another thread). */
struct MRChannel *MRIterator_GetChannel(MRIterator *it);

/* Allocate and initialize an iterator without dispatching its fan-out. The
 * iterator is inert until MR_StartIterator schedules its start callback, so the
 * caller can safely publish any state the per-reply callback depends on (e.g.
 * store the iterator pointer, register an abort-wake channel) before any reply
 * can arrive on the IO thread. Pair every MR_CreateIterator with
 * MR_StartIterator. Only the dispatch-related fields of `config` are read here
 * (`successCB`, `errorCB`, `cbPrivateData`, `cbPrivateDataDestructor`,
 * `cbPrivateDataInit`, `commandModifier`); `iterStartCb` /
 * `iterStartCbPrivateData` are consumed by MR_StartIterator. */
MRIterator *MR_CreateIterator(const MRCommand *cmd, const MRIteratorConfig *config);

/* Schedule the iterator's start callback on its IO runtime, kicking off the
 * fan-out to the shards. After this call replies may arrive at any time on the
 * IO thread. */
void MR_StartIterator(MRIterator *it, void (*iterStartCb)(void *),
                      StrongRef *iterStartCbPrivateData);

MRIterator *MR_IterateWithPrivateData(const MRCommand *cmd, const MRIteratorConfig *config);

MRCommand *MRIteratorCallback_GetCommand(MRIteratorCallbackCtx *ctx);

MRIteratorCtx *MRIteratorCallback_GetCtx(MRIteratorCallbackCtx *ctx);

/* Return the iterator that owns this callback context. Intended for per-reply
 * callbacks that need to query iterator-wide state (e.g. the shard count via
 * MRIterator_GetNumShards). */
MRIterator *MRIteratorCallback_GetIterator(MRIteratorCallbackCtx *ctx);

void *MRIteratorCallback_GetPrivateData(MRIteratorCallbackCtx *ctx);

void MRIteratorCallback_AddReply(MRIteratorCallbackCtx *ctx, MRReply *rep);

bool MRIteratorCallback_GetTimedOut(MRIteratorCtx *ctx);

void MRIteratorCallback_SetTimedOut(MRIteratorCtx *ctx);

void MRIteratorCallback_ResetTimedOut(MRIteratorCtx *ctx);

void MRIteratorCallback_Done(MRIteratorCallbackCtx *ctx, int error);

void MRIteratorCallback_ProcessDone(MRIteratorCallbackCtx *ctx);

int MRIteratorCallback_ResendCommand(MRIteratorCallbackCtx *ctx);

MRIteratorCtx *MRIterator_GetCtx(MRIterator *it);

size_t MRIterator_GetChannelSize(const MRIterator *it);

size_t MRIterator_GetNumShards(const MRIterator *it);

short MRIterator_GetPending(MRIterator *it);

void MRIterator_Release(MRIterator *it);

/* Replace the iterator's per-reply successCB and no-reply errorCB. Must only
 * be called from the iterator's own IO thread (the same thread that invokes
 * mrIteratorRedisCB / mrIteratorCallback_Error); no synchronization is applied.
 * Passing NULL for errorCB detaches it. */
void MRIterator_SwapCallbacks(MRIterator *it, MRIteratorCallback successCB,
                              MRIteratorErrorCallback errorCB);

/* Return the privateData stored in the first callback context of the iterator.
 * Valid while the iterator is alive (i.e. before the coord ref is released). */
void *MRIterator_GetPrivateData(const MRIterator *it);

sds MRCommand_SafeToString(const MRCommand *cmd);

#ifdef __cplusplus
}
#endif
