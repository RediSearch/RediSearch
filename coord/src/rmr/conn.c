#include "conn.h"
#include "reply.h"
#include "hiredis/adapters/libuv.h"
#include "search_cluster.h"

#include <uv.h>
#include <signal.h>
#include <sys/param.h>
#include <stdio.h>
#include <assert.h>

static void MRConn_ConnectCallback(const redisAsyncContext *c, int status);
static void MRConn_DisconnectCallback(const redisAsyncContext *, int);
static int MRConn_Connect(MRConn *conn);
static void MRConn_SwitchState(MRConn *conn, MRConnState nextState);
static void MRConn_Free(void *ptr);
static void MRConn_Stop(MRConn *conn);
static MRConn *MR_NewConn(MREndpoint *ep);
static int MRConn_StartNewConnection(MRConn *conn);
static int MRConn_SendAuth(MRConn *conn);

#define RSCONN_RECONNECT_TIMEOUT 250
#define RSCONN_REAUTH_TIMEOUT 1000

#define CONN_LOG(conn, fmt, ...)                                                \
  fprintf(stderr, "[%p %s:%d %s]" fmt "\n", conn, conn->ep.host, conn->ep.port, \
          MRConnState_Str((conn)->state), ##__VA_ARGS__)

/** detaches from our redis context */
static redisAsyncContext *detachFromConn(MRConn *conn, int shouldFree) {
  if (!conn->conn) {
    return NULL;
  }

  redisAsyncContext *ac = conn->conn;
  ac->data = NULL;
  conn->conn = NULL;
  if (shouldFree) {
    redisAsyncFree(ac);
    return NULL;
  } else {
    return ac;
  }
}

typedef struct {
  size_t num;
  size_t rr;  // round robin counter
  MRConn **conns;
} MRConnPool;

static MRConnPool *_MR_NewConnPool(MREndpoint *ep, size_t num) {
  MRConnPool *pool = malloc(sizeof(*pool));
  *pool = (MRConnPool){
      .num = num,
      .rr = 0,
      .conns = calloc(num, sizeof(MRConn *)),
  };

  /* Create the connection */
  for (size_t i = 0; i < num; i++) {
    pool->conns[i] = MR_NewConn(ep);
  }
  return pool;
}

static void MRConnPool_Free(void *p) {
  MRConnPool *pool = p;
  if (!pool) return;
  for (size_t i = 0; i < pool->num; i++) {
    /* We stop the connections and the disconnect callback frees them */
    MRConn_Stop(pool->conns[i]);
  }
  free(pool->conns);
  free(pool);
}

/* Get a connection from the connection pool. We select the next available connected connection with
 * a roundrobin selector */
static MRConn *MRConnPool_Get(MRConnPool *pool) {
  for (size_t i = 0; i < pool->num; i++) {

    MRConn *conn = pool->conns[pool->rr];
    // increase the round-robin counter
    pool->rr = (pool->rr + 1) % pool->num;
    if (conn->state == MRConn_Connected) {
      return conn;
    }
  }
  return NULL;
}

/* Init the connection manager */
void MRConnManager_Init(MRConnManager *mgr, int nodeConns) {
  /* Create the connection map */
  mgr->map = NewTrieMap();
  mgr->nodeConns = nodeConns;
}

/* Free the entire connection manager */
static void MRConnManager_Free(MRConnManager *mgr) {
  TrieMap_Free(mgr->map, MRConnPool_Free);
}

/* Get the connection for a specific node by id, return NULL if this node is not in the pool */
MRConn *MRConn_Get(MRConnManager *mgr, const char *id) {

  void *ptr = TrieMap_Find(mgr->map, (char *)id, strlen(id));
  if (ptr != TRIEMAP_NOTFOUND) {
    MRConnPool *pool = ptr;
    return MRConnPool_Get(pool);
  }
  return NULL;
}

/* Send a command to the connection */
int MRConn_SendCommand(MRConn *c, MRCommand *cmd, redisCallbackFn *fn, void *privdata) {

  /* Only send to connected nodes */
  if (c->state != MRConn_Connected) {
    return REDIS_ERR;
  }
  // printf("Sending to %s:%d\n", c->ep.host, c->ep.port);
  // MRCommand_Print(cmd);
  return redisAsyncCommandArgv(c->conn, fn, privdata, cmd->num, (const char **)cmd->strs,
                               cmd->lens);
}

// replace an existing coonnection pool with a new one
static void *replaceConnPool(void *oldval, void *newval) {
  if (oldval) {
    MRConnPool_Free(oldval);
  }
  return newval;
}
/* Add a node to the connection manager. Return 1 if it's been added or 0 if it hasn't */
int MRConnManager_Add(MRConnManager *m, const char *id, MREndpoint *ep, int connect) {

  /* First try to see if the connection is already in the manager */
  void *ptr = TrieMap_Find(m->map, (char *)id, strlen(id));
  if (ptr != TRIEMAP_NOTFOUND) {
    MRConnPool *pool = ptr;

    MRConn *conn = pool->conns[0];
    // the node hasn't changed address, we don't need to do anything */
    if (!strcmp(conn->ep.host, ep->host) && conn->ep.port == ep->port) {
      // fprintf(stderr, "No need to switch conn pools!\n");
      return 0;
    }

    // if the node has changed, we just replace the pool with a new one automatically
  }

  MRConnPool *pool = _MR_NewConnPool(ep, m->nodeConns);
  if (connect) {
    for (size_t i = 0; i < pool->num; i++) {
      MRConn_Connect(pool->conns[i]);
    }
  }

  return TrieMap_Add(m->map, (char *)id, strlen(id), pool, replaceConnPool);
}

/**
 * Start a new connection. Returns REDISMODULE_ERR if not a new connection
 */
static int MRConn_StartNewConnection(MRConn *conn) {
  if (conn && conn->state == MRConn_Disconnected) {
    if (MRConn_Connect(conn) == REDIS_ERR) {
      MRConn_SwitchState(conn, MRConn_Connecting);
    }
    return REDIS_OK;
  }
  return REDIS_ERR;
}

/* Connect all connections in the manager. Return the number of connections we successfully
 * started.
 * If we cannot connect, we initialize a retry loop */
int MRConnManager_ConnectAll(MRConnManager *m) {

  int n = 0;
  TrieMapIterator *it = TrieMap_Iterate(m->map, "", 0);
  char *key;
  tm_len_t len;
  void *p;
  while (TrieMapIterator_Next(it, &key, &len, &p)) {
    MRConnPool *pool = p;
    if (!pool) continue;
    for (size_t i = 0; i < pool->num; i++) {
      if (MRConn_StartNewConnection(pool->conns[i]) == REDIS_OK) {
        n++;
      }
    }
  }
  TrieMapIterator_Free(it);
  return n;
}

/* Explicitly disconnect a connection and remove it from the connection pool */
int MRConnManager_Disconnect(MRConnManager *m, const char *id) {
  if (TrieMap_Delete(m->map, (char *)id, strlen(id), MRConnPool_Free)) {
    return REDIS_OK;
  }
  return REDIS_ERR;
}

static void MRConn_Stop(MRConn *conn) {
  CONN_LOG(conn, "Requesting to stop");
  MRConn_SwitchState(conn, MRConn_Freeing);
}

static void freeConn(MRConn *conn) {
  MREndpoint_Free(&conn->ep);
  if (conn->timer) {
    if (uv_is_active(conn->timer)) {
      uv_timer_stop(conn->timer);
    }
    uv_close(conn->timer, (uv_close_cb)free);
  }
  free(conn);
}

static void signalCallback(uv_timer_t *tm) {
  MRConn *conn = tm->data;
  if (conn->state == MRConn_Connected) {
    return;  // Nothing to do here!
  }

  if (conn->state == MRConn_Freeing) {
    if (conn->conn) {
      redisAsyncContext *ac = conn->conn;
      // detach the connection
      ac->data = NULL;
      conn->conn = NULL;
      redisAsyncDisconnect(ac);
    }
    freeConn(conn);
    return;
  }

  if (conn->state == MRConn_ReAuth) {
    if (MRConn_SendAuth(conn) != REDIS_OK) {
      detachFromConn(conn, 1);
      MRConn_SwitchState(conn, MRConn_Connecting);
    }
  } else if (conn->state == MRConn_Connecting) {
    if (MRConn_Connect(conn) == REDIS_ERR) {
      detachFromConn(conn, 1);
      MRConn_SwitchState(conn, MRConn_Connecting);
    }
  } else {
    abort();  // Unknown state! - Can't transition
  }
}

/* Safely transition to current state */
static void MRConn_SwitchState(MRConn *conn, MRConnState nextState) {
  if (!conn->timer) {
    conn->timer = malloc(sizeof(uv_timer_t));
    uv_timer_init(uv_default_loop(), conn->timer);
    ((uv_timer_t *)conn->timer)->data = conn;
  }
  CONN_LOG(conn, "Switching state to %s", MRConnState_Str(nextState));

  uint64_t nextTimeout = 0;

  if (nextState == MRConn_Freeing) {
    nextTimeout = 0;
    conn->state = MRConn_Freeing;
    goto activate_timer;
  } else if (conn->state == MRConn_Freeing) {
    return;
  }

  switch (nextState) {
    case MRConn_Disconnected:
      // We should never *switch* to this state
      abort();

    case MRConn_Connecting:
      nextTimeout = RSCONN_RECONNECT_TIMEOUT;
      conn->state = nextState;
      break;

    case MRConn_ReAuth:
      nextTimeout = RSCONN_REAUTH_TIMEOUT;
      conn->state = nextState;
      goto activate_timer;

    case MRConn_Connected:
      // "Dummy" states:
      conn->state = nextState;
      if (uv_is_active(conn->timer)) {
        uv_timer_stop(conn->timer);
      }
      return;
    default:
      // Can't handle this state!
      abort();
  }

activate_timer:
  if (!uv_is_active(conn->timer)) {
    uv_timer_start(conn->timer, signalCallback, nextTimeout, 0);
  }
}

static void MRConn_AuthCallback(redisAsyncContext *c, void *r, void *privdata) {
  MRConn *conn = c->data;
  if (!conn || conn->state == MRConn_Freeing) {
    // Will be picked up by disconnect callback
    return;
  }

  if (c->err || !r) {
    detachFromConn(conn, !!r);
    MRConn_SwitchState(conn, MRConn_Connecting);
    return;
  }

  redisReply *rep = r;
  /* AUTH error */
  if (MRReply_Type(rep) == REDIS_REPLY_ERROR) {
    size_t len;
    const char* s = MRReply_String(rep, &len);
    CONN_LOG(conn, "Error authenticating: %.*s", (int)len, s);
    MRConn_SwitchState(conn, MRConn_ReAuth);
    /*we don't try to reconnect to failed connections */
    return;
  }

  /* Success! we are now connected! */
  // fprintf(stderr, "Connected and authenticated to %s:%d\n", conn->ep.host, conn->ep.port);
  MRConn_SwitchState(conn, MRConn_Connected);
}

static int MRConn_SendAuth(MRConn *conn) {
  CONN_LOG(conn, "Authenticating...");

  // if we failed to send the auth command, start a reconnect loop
  if (redisAsyncCommand(conn->conn, MRConn_AuthCallback, conn, "AUTH %s", conn->ep.auth) ==
      REDIS_ERR) {
    MRConn_SwitchState(conn, MRConn_ReAuth);
    return REDIS_ERR;
  } else {
    return REDIS_OK;
  }
}

/* hiredis async connect callback */
static void MRConn_ConnectCallback(const redisAsyncContext *c, int status) {
  MRConn *conn = c->data;
  if (!conn) {
    if (status == REDIS_OK) {
      // We need to free it here because we will not be getting a disconnect
      // callback.
      redisAsyncFree((redisAsyncContext *)c);
    } else {
      // Will be freed anyway
    }
    return;
  }

  // fprintf(stderr, "Connect callback! status :%d\n", status);
  // if the connection is not stopped - try to reconnect
  if (status != REDIS_OK) {
    CONN_LOG(conn, "Error on connect: %s", c->errstr);
    detachFromConn(conn, 0);  // Free the connection as well - we have an error
    MRConn_SwitchState(conn, MRConn_Connecting);
    return;
  }

  // todo: check if tls is require and if it does initiate a tls connection
  char* client_cert = NULL;
  char* client_key = NULL;
  char* ca_cert = NULL;
  if(checkTLS(&client_key, &client_cert, &ca_cert)){
    redisSSLContextError ssl_error = 0;
    redisSSLContext *ssl_context = redisCreateSSLContext(ca_cert, NULL, client_cert, client_key, NULL, &ssl_error);
    if(ssl_context == NULL || ssl_error != 0) {
      CONN_LOG(conn, "Error on ssl contex creation: %s", (ssl_error != 0) ? redisSSLContextGetError(ssl_error) : "Unknown error");
      detachFromConn(conn, 0);  // Free the connection as well - we have an error
      MRConn_SwitchState(conn, MRConn_Connecting);
      return;
    }
    if (redisInitiateSSLWithContext((redisContext *)(&c->c), ssl_context) != REDIS_OK) {
      CONN_LOG(conn, "Error on tls auth");
      detachFromConn(conn, 0);  // Free the connection as well - we have an error
      MRConn_SwitchState(conn, MRConn_Connecting);
      return;
    }
    rm_free(client_key);
    rm_free(client_cert);
  }



  // If this is an authenticated connection, we need to atu

  if (conn->ep.auth) {
    if (MRConn_SendAuth(conn) != REDIS_OK) {
      detachFromConn(conn, 1);
      MRConn_SwitchState(conn, MRConn_Connecting);
    }
  } else {
    MRConn_SwitchState(conn, MRConn_Connected);
  }
  // fprintf(stderr, "Connected %s:%d...\n", conn->ep.host, conn->ep.port);
}

static void MRConn_DisconnectCallback(const redisAsyncContext *c, int status) {
  MRConn *conn = c->data;
  if (!conn) {
    /* Ignore */
    return;
  }

  // fprintf(stderr, "Disconnected from %s:%d\n", conn->ep.host, conn->ep.port);
  if (conn->state != MRConn_Freeing) {
    detachFromConn(conn, 0);
    MRConn_SwitchState(conn, MRConn_Connecting);
  } else {
    freeConn(conn);
  }
}

static MRConn *MR_NewConn(MREndpoint *ep) {
  MRConn *conn = malloc(sizeof(MRConn));
  *conn = (MRConn){.state = MRConn_Disconnected, .conn = NULL};
  MREndpoint_Copy(&conn->ep, ep);
  return conn;
}

/* Connect to a cluster node. Return REDIS_OK if either connected, or if  */
static int MRConn_Connect(MRConn *conn) {
  assert(!conn->conn);
  // fprintf(stderr, "Connectig to %s:%d\n", conn->ep.host, conn->ep.port);

  redisOptions options = {.type = REDIS_CONN_TCP,
                          .options = REDIS_OPT_NOAUTOFREEREPLIES,
                          .endpoint.tcp = {.ip = conn->ep.host, .port = conn->ep.port}};

  redisAsyncContext *c = redisAsyncConnectWithOptions(&options);
  if (c->err) {
    CONN_LOG(conn, "Could not connect to node: %s", c->errstr);
    redisAsyncFree(c);
    return REDIS_ERR;
  }

  conn->conn = c;
  conn->conn->data = conn;
  conn->state = MRConn_Connecting;

  redisLibuvAttach(conn->conn, uv_default_loop());
  redisAsyncSetConnectCallback(conn->conn, MRConn_ConnectCallback);
  redisAsyncSetDisconnectCallback(conn->conn, MRConn_DisconnectCallback);

  return REDIS_OK;
}
