/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#pragma once

#ifdef __cplusplus
extern "C" {
#endif

#include "hiredis/hiredis.h"
#include "hiredis/hiredis_ssl.h"
#include "hiredis/async.h"
#include "endpoint.h"
#include "command.h"
#include "util/dict.h"
#include <uv.h>

/*
 * The state of the connection. Packed so it fits in one byte when embedded
 * in MRConn.
 */
typedef enum __attribute__((packed)) {
  /* Connection is trying to connect (initial state and retry state) */
  MRConn_Connecting,

  /* TCP (and TLS) handshake completed; AUTH command is in flight */
  MRConn_Authenticating,

  /* Back-off before retrying AUTH after a server-side AUTH rejection */
  MRConn_ReAuth,

  /* Connected, authenticated and active */
  MRConn_Connected,

  /* Connection should be freed */
  MRConn_Freeing
} MRConnState;

/*
 * RESP protocol version negotiated on the connection. Packed so it fits in
 * one byte when embedded in MRConn. Values match the HELLO argument.
 */
typedef enum __attribute__((packed)) {
  MRConn_Protocol_Undetermined = 0,
  MRConn_Protocol_RESP2 = 2,
  MRConn_Protocol_RESP3 = 3,
} MRConnProtocol;

static inline const char *MRConnState_Str(MRConnState state) {
  switch (state) {
    case MRConn_Connecting:
      return "Connecting";
    case MRConn_Authenticating:
      return "Authenticating";
    case MRConn_ReAuth:
      return "Re-Authenticating";
    case MRConn_Connected:
      return "Connected";
    case MRConn_Freeing:
      return "Freeing";
    default:
      return "<UNKNOWN STATE (CRASHES AHEAD!!!!)";
  }
}
// opaque type
typedef struct MRConn MRConn;

/* A pool indexes connections by the node id */
typedef struct {
  dict *map;
  int nodeConns;
} MRConnManager;

void MRConnManager_Init(MRConnManager *mgr, int nodeConns);

/*
 * Gets the stateDict filled with connection pool states of different IORuntimes and
 * fills the reply with this stateDict. It fills the Reply for the client.
*/
void MRConnManager_ReplyState(dict *stateDict, RedisModuleCtx *ctx);

/*
 * Fill the state dictionary with the connection pool state.
 * The dictionary is a map of host:port strings to an array of connection states.
 * The array contains the state of each connection in the pool. The stateDict may be empty
 * or already contain information from other ConnManager
*/
void MRConnManager_FillStateDict(MRConnManager *mgr, dict *stateDict);

/* Get the connection for a specific node by id, return NULL if this node is not in the pool */
MRConn *MRConn_Get(MRConnManager *mgr, const char *id);

/* Get the state string of the first connection for a specific node by id.
 * Returns NULL if this node is not in the pool.
 * Must be called from the uv event loop thread, as mgr->map is not thread-safe. */
const char *MRConnManager_GetNodeState(MRConnManager *mgr, const char *id);

int MRConn_SendCommand(MRConn *c, MRCommand *cmd, redisCallbackFn *fn, void *privdata);

/* Add a node to the connection manager and start its connections. Returns
 * true iff the pool for `id` was (re)created; false when an existing pool
 * already matches `ep` and was reused. */
bool MRConnManager_Add(MRConnManager *m, uv_loop_t *loop, const char *id, MREndpoint *ep);

/* Disconnect a node */
int MRConnManager_Disconnect(MRConnManager *m, const char *id);

/*
 * Set number of connections to each node to `num`, disconnect from extras.
 * Assumes that `num` is less than the current number of connections and non-zero
 */
void MRConnManager_Shrink(MRConnManager *m, size_t num);

/*
 * Set number of connections to each node to `num`, connect new connections.
 * Assumes that `num` is greater than the current number of connections
 */
void MRConnManager_Expand(MRConnManager *m, size_t num, uv_loop_t *loop);

/*
 * Disconnect all connections and release the manager's dict. Must be called
 * from the uv thread while the event loop is still alive.
 */
void MRConnManager_Shutdown(MRConnManager *mgr);

#ifdef __cplusplus
}
#endif
