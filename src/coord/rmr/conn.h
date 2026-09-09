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
 * The state of the connection.
 */
typedef enum {
  /* TCP (and TLS) handshake is in flight */
  MRConn_Connecting,

  /* Back-off before retrying connect after a connection failure */
  MRConn_Reconnecting,

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
 * RESP protocol version negotiated on the connection. Values match the
 * HELLO argument.
 */
typedef enum {
  MRConn_Protocol_Undetermined = 0,
  MRConn_Protocol_RESP2 = 2,
  MRConn_Protocol_RESP3 = 3,
} MRConnProtocol;

static inline const char *MRConnState_Str(MRConnState state) {
  switch (state) {
    case MRConn_Connecting:
      return "Connecting";
    case MRConn_Reconnecting:
      return "Reconnecting";
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

/*
 * A node's belief about whether its shard supports a given rolling-upgrade-gated
 * capability (currently just the row-block reply format; see
 * RediSearchCaps_HasRowBlock), learned from the `search` module version advertised
 * in the connection's HELLO reply. Tracked per node/pool rather than per connection
 * because the decision point (building a per-shard command at fan-out time) knows
 * only the target node id, not which of its pool's connections will end up carrying
 * the command - see MRConnPool_GetConn's round-robin selection.
 *
 * Fresh connection => Unknown. Resolved by the first parsed HELLO reply => No or
 * Yes. Reset to Unknown whenever any connection in the pool leaves MRConn_Connected:
 * a process cannot be replaced (rollback, restore, failover onto an older build)
 * without dropping its connections first, so a stale Yes cannot survive one.
 * Nothing here is persisted or survives a restart on either side.
 */
typedef enum {
  MRNodeCap_Unknown,
  MRNodeCap_No,
  MRNodeCap_Yes,
} MRNodeCapState;

static inline const char *MRNodeCapState_Str(MRNodeCapState state) {
  switch (state) {
    case MRNodeCap_Unknown:
      return "Unknown";
    case MRNodeCap_No:
      return "No";
    case MRNodeCap_Yes:
      return "Yes";
    default:
      return "<UNKNOWN CAPABILITY STATE>";
  }
}

/* Get the row-block capability belief for the node's pool, and (for diagnostics
 * only) the last `search` module version parsed from its HELLO reply in
 * *outVersion, or -1 if none was ever parsed. outVersion may be NULL.
 * Returns MRNodeCap_Unknown (with *outVersion == -1) if `id` is not in the pool.
 * Must be called from the uv event loop thread that owns `mgr`, as mgr->map is
 * not thread-safe. */
MRNodeCapState MRConnManager_GetRowBlockCapability(MRConnManager *mgr, const char *id,
                                                    int *outVersion);

/* Defence in depth, on top of (not instead of) RediSearchCaps_HasRowBlock: force a
 * node's row-block capability to No, sticky until its connection pool is rebuilt
 * (i.e. its endpoint changes, see MRConnManager_Add), regardless of what any past
 * or future HELLO reply says. Call this when a shard believed capable rejects
 * `_ROW_BLOCK` with an unknown-argument error - evidence stronger than a version
 * string, covering any hole in the version-to-capability mapping.
 * No-op if `id` is not in the pool. Idempotent. Must be called from the uv event
 * loop thread that owns `mgr`. */
void MRConnManager_DemoteRowBlockCap(MRConnManager *mgr, const char *id);

void MRConnManager_Init(MRConnManager *mgr, int nodeConns);

/*
 * Gets the stateDict filled with connection pool states of different IORuntimes and
 * fills the reply with this stateDict. It fills the Reply for the client.
*/
void MRConnManager_ReplyState(dict *stateDict, RedisModuleCtx *ctx);

/*
 * Fill the state dictionary with the connection pool state.
 * The dictionary is a map of host:port strings to an array of strings: the state of
 * each connection in the pool (see MRConnState_Str), followed by one row-block
 * capability line for the pool as a whole (see MRNodeCapState_Str and
 * MRConnManager_GetRowBlockCapability) - capability is tracked per node/pool, not
 * per connection, so it appears once per pool rather than once per connection.
 * The stateDict may be empty or already contain information from other ConnManagers
 * (one per IO thread; a node's entries from different IO threads can disagree
 * while a rolling capability belief is still converging).
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
void MRConnManager_Shrink(MRConnManager *m, uint32_t num);

/*
 * Set number of connections to each node to `num`, connect new connections.
 * Assumes that `num` is greater than the current number of connections
 */
void MRConnManager_Expand(MRConnManager *m, uint32_t num, uv_loop_t *loop);

/*
 * Disconnect all connections and release the manager's dict. Must be called
 * from the uv thread while the event loop is still alive.
 */
void MRConnManager_Shutdown(MRConnManager *mgr);

#ifdef __cplusplus
}
#endif
