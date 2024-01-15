/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */


#pragma once

#include "hiredis/hiredis.h"
#include "hiredis/hiredis_ssl.h"
#include "hiredis/async.h"
#include "endpoint.h"
#include "command.h"
#include "util/dict.h"

#define MR_CONN_POOL_SIZE 1

/*
 * The state of the connection.
 * TODO: Not all of these are "real" states
 */
typedef enum {
  /* initial state - new connection or disconnected connection due to error */
  MRConn_Disconnected,

  /* Connection is trying to connect */
  MRConn_Connecting,

  MRConn_ReAuth,

  /* Connected, authenticated and active */
  MRConn_Connected,

  /* Connection should be freed */
  MRConn_Freeing
} MRConnState;

static inline const char *MRConnState_Str(MRConnState state) {
  switch (state) {
    case MRConn_Disconnected:
      return "Disconnected";
    case MRConn_Connecting:
      return "Connecting";
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

typedef struct {
  MREndpoint ep;
  redisAsyncContext *conn;
  MRConnState state;
  void *timer;
} MRConn;

/* A pool indexes connections by the node id */
typedef struct {
  dict *map;
  int nodeConns;
} MRConnManager;

void MRConnManager_Init(MRConnManager *mgr, int nodeConns);

/* Get the connection for a specific node by id, return NULL if this node is not in the pool */
MRConn *MRConn_Get(MRConnManager *mgr, const char *id);

int MRConn_SendCommand(MRConn *c, MRCommand *cmd, redisCallbackFn *fn, void *privdata);

/* Add a node to the connection manager */
int MRConnManager_Add(MRConnManager *m, const char *id, MREndpoint *ep, int connect);

/* Connect all nodes to their destinations */
int MRConnManager_ConnectAll(MRConnManager *m);

/* Disconnect a node */
int MRConnManager_Disconnect(MRConnManager *m, const char *id);

void MRConnManager_Free(MRConnManager *m);
