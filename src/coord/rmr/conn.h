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
#include "redisearch_caps.h"
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
 * A node's capabilities are pure functions of one piece of state: the `search`
 * module version it advertised in its connection's HELLO reply (see
 * RediSearchCaps_Supports), plus a per-capability sticky demotion bit. Tracked
 * per node/pool rather than per connection because the decision point (building
 * a per-shard command at fan-out time) knows only the target node id, not which
 * of its pool's connections will end up carrying the command - see
 * MRConnPool_GetConn's round-robin selection.
 *
 * "Unknown" is `moduleVersion < 0`, not a per-capability tri-state: one source of
 * truth, so two capabilities can never disagree about whether the node has been
 * re-probed. Fresh connection => -1. Resolved by the first parsed HELLO reply =>
 * the parsed version (>= 0). Reset to -1 whenever any connection in the pool
 * leaves MRConn_Connected: a process cannot be replaced (rollback, restore,
 * failover onto an older build) without dropping its connections first, so a
 * stale version cannot survive one. Nothing here is persisted or survives a
 * restart on either side.
 *
 * `demoted`, unlike the version, stays per capability (a bitmask of
 * `1u << RSCapability`): a shard rejecting one capability's token says nothing
 * about another capability, so demotion cannot be folded into the single version
 * the way "supported" can. See MRConnManager_NodeSupports for how the two combine
 * and MRConnManager_DemoteCapability for how a bit gets set.
 */

/* Returns whether node `id` currently supports `cap`, applying (in order):
 * 1. The `search-_force-shard-caps` config forcing every shard incapable
 *    (RSForceShardCaps_No in config.h) - a test/ops override, evaluated first so
 *    it can force every other rule's outcome to false without touching state.
 * 2. `id` not in the pool, or its module version unknown (moduleVersion < 0) -
 *    false, same as "no HELLO reply parsed yet".
 * 3. `cap` sticky-demoted for this node (MRConnManager_DemoteCapability) - false,
 *    regardless of what the version predicate would say.
 * 4. Otherwise, RediSearchCaps_Supports(cap, moduleVersion).
 * Must be called from the uv event loop thread that owns `mgr`, as mgr->map is
 * not thread-safe. */
bool MRConnManager_NodeSupports(MRConnManager *mgr, const char *id, RSCapability cap);

/* Defence in depth, on top of (not instead of) RediSearchCaps_Supports: force
 * node `id`'s belief for `cap` to unsupported, sticky until its connection pool
 * is rebuilt (i.e. its endpoint changes, see MRConnManager_Add), regardless of
 * what any past or future HELLO reply says. Call this when a shard believed
 * capable of `cap` rejects its wire token with an unknown-argument error -
 * evidence stronger than a version string, covering any hole in the
 * version-to-capability mapping. Demoting one capability leaves every other
 * capability's belief for the same node untouched.
 * No-op if `id` is not in the pool. Idempotent. Must be called from the uv event
 * loop thread that owns `mgr`. */
void MRConnManager_DemoteCapability(MRConnManager *mgr, const char *id, RSCapability cap);

void MRConnManager_Init(MRConnManager *mgr, int nodeConns);

/*
 * Gets the stateDict filled with connection pool states of different IORuntimes and
 * fills the reply with this stateDict. It fills the Reply for the client.
*/
void MRConnManager_ReplyState(dict *stateDict, RedisModuleCtx *ctx);

/*
 * Fill the state dictionary with the connection pool state.
 * The dictionary is a map of host:port strings to an array of strings: the state of
 * each connection in the pool (see MRConnState_Str), followed by one capabilities
 * line for the pool as a whole (module version plus, for every RSCapability, live
 * vs. demoted - see MRConnManager_NodeSupports) - capability state is tracked per
 * node/pool, not per connection, so it appears once per pool rather than once per
 * connection.
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
