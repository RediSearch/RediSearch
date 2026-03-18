/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "conn.h"
#include "reply.h"
#include "module.h"
#include "rmutil/rm_assert.h"
#include "hiredis/adapters/libuv.h"
#include "util/config_api.h"

#include <uv.h>
#include <signal.h>
#include <sys/param.h>
#include <stdio.h>
#include <time.h>

#include <openssl/ssl.h>
#include <openssl/err.h>

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
#define UNUSED(x) (void)(x)

/*
 * Enhanced debug logging for MOD-13979 investigation.
 * Logs connection state, timeout configuration, and all callback invocations.
 */
#define CONN_LOG(conn, fmt, ...)                                                      \
  RedisModule_Log(RSDummyContext, "debug", "[CONN %p %s:%d %s] " fmt,                 \
                  conn, conn->ep.host, conn->ep.port, MRConnState_Str((conn)->state), \
                  ##__VA_ARGS__)

/* Log without conn pointer for cases where conn might be NULL */
#define CONN_LOG_RAW(fmt, ...) \
  RedisModule_Log(RSDummyContext, "debug", "[CONN] " fmt, ##__VA_ARGS__)

/* Helper to get current timestamp for logging */
static inline void logTimestamp(char *buf, size_t len) {
  time_t now = time(NULL);
  struct tm *tm_info = localtime(&now);
  strftime(buf, len, "%H:%M:%S", tm_info);
}

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
  MRConnPool *pool = rm_malloc(sizeof(*pool));
  *pool = (MRConnPool){
      .num = num,
      .rr = 0,
      .conns = rm_calloc(num, sizeof(MRConn *)),
  };

  /* Create the connection */
  for (size_t i = 0; i < num; i++) {
    pool->conns[i] = MR_NewConn(ep);
  }
  return pool;
}

static void MRConnPool_Free(void *privdata, void *p) {
  UNUSED(privdata);
  MRConnPool *pool = p;
  if (!pool) return;
  for (size_t i = 0; i < pool->num; i++) {
    /* We stop the connections and the disconnect callback frees them */
    MRConn_Stop(pool->conns[i]);
  }
  rm_free(pool->conns);
  rm_free(pool);
}

/* Get a connection from the connection pool. We select the next available connected connection with
 * a round robin selector */
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

static dictType nodeIdToConnPoolType = {
  .hashFunction = stringsHashFunction,
  .keyDup = stringsKeyDup,
  .valDup = NULL,
  .keyCompare = stringsKeyCompare,
  .keyDestructor = stringsKeyDestructor,
  .valDestructor = MRConnPool_Free,
};

/* Init the connection manager */
void MRConnManager_Init(MRConnManager *mgr, int nodeConns) {
  /* Create the connection map */
  mgr->map = dictCreate(&nodeIdToConnPoolType, NULL);
  mgr->nodeConns = nodeConns;
}

/* Free the entire connection manager */
void MRConnManager_Free(MRConnManager *mgr) {
  dictRelease(mgr->map);
}

void MRConnManager_ReplyState(MRConnManager *mgr, RedisModuleCtx *ctx) {
  RedisModule_ReplyWithMap(ctx, dictSize(mgr->map));
  dictIterator *it = dictGetIterator(mgr->map);
  dictEntry *entry;
  while ((entry = dictNext(it))) {
    MRConnPool *pool = dictGetVal(entry);
    RedisModuleString *key = RedisModule_CreateStringPrintf(ctx, "%s:%d", pool->conns[0]->ep.host,
                                                                          pool->conns[0]->ep.port);
    RedisModule_ReplyWithString(ctx, key);
    RedisModule_FreeString(ctx, key);
    RedisModule_ReplyWithArray(ctx, pool->num);
    for (size_t i = 0; i < pool->num; i++) {
      RedisModule_ReplyWithCString(ctx, MRConnState_Str(pool->conns[i]->state));
    }
  }
  dictReleaseIterator(it);
}

/* Get the connection for a specific node by id, return NULL if this node is not in the pool */
MRConn *MRConn_Get(MRConnManager *mgr, const char *id) {

  dictEntry *ptr = dictFind(mgr->map, id);
  if (ptr) {
    MRConnPool *pool = dictGetVal(ptr);
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

  if (!cmd->cmd) {
    if (redisFormatSdsCommandArgv(&cmd->cmd, cmd->num, (const char **)cmd->strs, cmd->lens) == REDIS_ERR) {
      return REDIS_ERR;
    }
  }
  if (cmd->protocol != 0 && (!c->protocol || c->protocol != cmd->protocol)) {
    int rc = redisAsyncCommand(c->conn, NULL, NULL, "HELLO %d", cmd->protocol);
    c->protocol = cmd->protocol;
  }
  return redisAsyncFormattedCommand(c->conn, fn, privdata, cmd->cmd, sdslen(cmd->cmd));
}

/* Add a node to the connection manager. Return 1 if it's been added or 0 if it hasn't */
int MRConnManager_Add(MRConnManager *m, const char *id, MREndpoint *ep, int connect) {
  /* First try to see if the connection is already in the manager */
  dictEntry *ptr = dictFind(m->map, id);
  if (ptr) {
    MRConnPool *pool = dictGetVal(ptr);

    MRConn *conn = pool->conns[0];
    // the node hasn't changed address, we don't need to do anything */
    if (!strcmp(conn->ep.host, ep->host) && conn->ep.port == ep->port) {
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

  return dictReplace(m->map, (void *)id, pool);
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
  dictIterator *it = dictGetIterator(m->map);
  dictEntry *entry;
  while ((entry = dictNext(it))) {
    MRConnPool *pool = dictGetVal(entry);
    for (size_t i = 0; i < pool->num; i++) {
      if (MRConn_StartNewConnection(pool->conns[i]) == REDIS_OK) {
        n++;
      }
    }
  }
  dictReleaseIterator(it);
  return n;
}

/* Explicitly disconnect a connection and remove it from the connection pool */
int MRConnManager_Disconnect(MRConnManager *m, const char *id) {
  if (dictDelete(m->map, id)) {
    return REDIS_OK;
  }
  return REDIS_ERR;
}

// Shrink the connection pool to the given number of connections
// Assumes that the number of connections is less than the current number of connections,
// and that the new number of connections is greater than 0
void MRConnManager_Shrink(MRConnManager *m, size_t num) {
  dictIterator *it = dictGetIterator(m->map);
  dictEntry *entry;
  while ((entry = dictNext(it))) {
    MRConnPool *pool = dictGetVal(entry);

    for (size_t i = num; i < pool->num; i++) {
      MRConn_Stop(pool->conns[i]);
    }
    pool->num = num;
    pool->rr %= num; // set the round robin counter to the new pool size bound
    pool->conns = rm_realloc(pool->conns, num * sizeof(MRConn *));
  }
  m->nodeConns = num;
  dictReleaseIterator(it);
}

// Expand the connection pool to the given number of connections
// Assumes that the number of connections is greater than the current number of connections
void MRConnManager_Expand(MRConnManager *m, size_t num) {
  dictIterator *it = dictGetIterator(m->map);
  dictEntry *entry;
  while ((entry = dictNext(it))) {
    MRConnPool *pool = dictGetVal(entry);

    pool->conns = rm_realloc(pool->conns, num * sizeof(MRConn *));
    // Use the first connection's endpoint to create new connections
    // There should always be at least one connection in the pool
    MREndpoint *ep = &pool->conns[0]->ep;
    for (size_t i = pool->num; i < num; i++) {
      pool->conns[i] = MR_NewConn(ep);
      MRConn_StartNewConnection(pool->conns[i]);
    }
    pool->num = num;
  }
  m->nodeConns = num;
  dictReleaseIterator(it);
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
    uv_close(conn->timer, (uv_close_cb)rm_free);
  }
  rm_free(conn);
}

/*
 * RediSearch's internal retry timer callback.
 *
 * MOD-13979 INVESTIGATION:
 * This is RediSearch's retry mechanism - it fires every 250ms when state=Connecting.
 * HOWEVER, this timer only fires if MRConn_SwitchState(Connecting) was called.
 *
 * The bug scenario:
 *   1. MRConn_Connect() initiates async connect
 *   2. Hiredis doesn't schedule a timeout (connect_timeout is NULL)
 *   3. MRConn_ConnectCallback is NEVER called (host unreachable, no timeout)
 *   4. MRConn_SwitchState is NEVER called again
 *   5. This timer NEVER fires again
 *   6. Connection stays stuck forever
 */
static void signalCallback(uv_timer_t *tm) {
  char ts[16];
  logTimestamp(ts, sizeof(ts));

  MRConn *conn = tm->data;
  CONN_LOG(conn, "========== signalCallback [%s] ==========", ts);
  CONN_LOG(conn, "RediSearch retry timer fired!");

  if (conn->state == MRConn_Connected) {
    CONN_LOG(conn, "Already connected - nothing to do");
    CONN_LOG(conn, "========== signalCallback END ==========");
    return;
  }

  if (conn->state == MRConn_Freeing) {
    CONN_LOG(conn, "State is Freeing - cleaning up");
    if (conn->conn) {
      redisAsyncContext *ac = conn->conn;
      ac->data = NULL;
      conn->conn = NULL;
      redisAsyncDisconnect(ac);
    }
    freeConn(conn);
    return;
  }

  if (conn->state == MRConn_ReAuth) {
    CONN_LOG(conn, "State is ReAuth - sending auth command");
    if (MRConn_SendAuth(conn) != REDIS_OK) {
      CONN_LOG(conn, "Auth failed - will retry connect");
      detachFromConn(conn, 1);
      MRConn_SwitchState(conn, MRConn_Connecting);
    }
  } else if (conn->state == MRConn_Connecting) {
    CONN_LOG(conn, "State is Connecting - attempting new connection");
    CONN_LOG(conn, "*** This means previous connect attempt completed (success/fail/timeout) ***");
    CONN_LOG(conn, "*** If this NEVER fires after initial connect, the connection is STUCK ***");

    if (MRConn_Connect(conn) == REDIS_ERR) {
      CONN_LOG(conn, "MRConn_Connect returned ERR immediately - scheduling retry");
      detachFromConn(conn, 1);
      MRConn_SwitchState(conn, MRConn_Connecting);
    } else {
      CONN_LOG(conn, "MRConn_Connect returned OK - waiting for async callbacks");
      CONN_LOG(conn, "*** Next log should be MRConn_ConnectCallback ***");
      CONN_LOG(conn, "*** If MRConn_ConnectCallback never fires, connection is STUCK ***");
    }
  } else {
    CONN_LOG(conn, "FATAL: Unknown state %d!", conn->state);
    abort();
  }
  CONN_LOG(conn, "========== signalCallback END ==========");
}

/*
 * Safely transition to a new connection state.
 *
 * MOD-13979 INVESTIGATION:
 * This function schedules the RediSearch retry timer (signalCallback).
 * When switching to Connecting, it schedules a 250ms timer.
 *
 * The bug scenario:
 *   1. MRConn_Connect() succeeds (returns REDIS_OK)
 *   2. State is Connecting, timer NOT re-scheduled here
 *   3. We wait for MRConn_ConnectCallback from hiredis
 *   4. If connect_timeout is NULL, hiredis never fires timeout
 *   5. MRConn_ConnectCallback never fires
 *   6. MRConn_SwitchState never called
 *   7. Timer never fires again
 *   8. Connection stuck forever
 */
static void MRConn_SwitchState(MRConn *conn, MRConnState nextState) {
  char ts[16];
  logTimestamp(ts, sizeof(ts));

  if (!conn->timer) {
    conn->timer = rm_malloc(sizeof(uv_timer_t));
    uv_timer_init(uv_default_loop(), conn->timer);
    ((uv_timer_t *)conn->timer)->data = conn;
  }

  CONN_LOG(conn, "========== MRConn_SwitchState [%s] ==========", ts);
  CONN_LOG(conn, "Transition: %s -> %s",
           MRConnState_Str(conn->state), MRConnState_Str(nextState));

  uint64_t nextTimeout = 0;

  if (nextState == MRConn_Freeing) {
    nextTimeout = 0;
    conn->state = MRConn_Freeing;
    CONN_LOG(conn, "State set to Freeing, timer will fire immediately");
    goto activate_timer;
  } else if (conn->state == MRConn_Freeing) {
    CONN_LOG(conn, "Already Freeing, ignoring transition request");
    CONN_LOG(conn, "========== MRConn_SwitchState END ==========");
    return;
  }

  switch (nextState) {
    case MRConn_Disconnected:
      CONN_LOG(conn, "FATAL: Cannot switch to Disconnected state!");
      abort();

    case MRConn_Connecting:
      nextTimeout = RSCONN_RECONNECT_TIMEOUT;
      conn->state = nextState;
      CONN_LOG(conn, "State set to Connecting, retry timer=%lums",
               (unsigned long)nextTimeout);
      CONN_LOG(conn, "*** signalCallback will fire in %lums to retry connection ***",
               (unsigned long)nextTimeout);
      break;

    case MRConn_ReAuth:
      nextTimeout = RSCONN_REAUTH_TIMEOUT;
      conn->state = nextState;
      CONN_LOG(conn, "State set to ReAuth, auth retry timer=%lums",
               (unsigned long)nextTimeout);
      goto activate_timer;

    case MRConn_Connected:
      conn->state = nextState;
      CONN_LOG(conn, "State set to Connected - connection is fully established!");
      CONN_LOG(conn, "Stopping retry timer (no longer needed)");
      if (uv_is_active(conn->timer)) {
        uv_timer_stop(conn->timer);
      }
      CONN_LOG(conn, "========== MRConn_SwitchState END (success!) ==========");
      return;

    default:
      CONN_LOG(conn, "FATAL: Unknown state %d!", nextState);
      abort();
  }

activate_timer:
  if (!uv_is_active(conn->timer)) {
    CONN_LOG(conn, "Starting timer for %lums", (unsigned long)nextTimeout);
    uv_timer_start(conn->timer, signalCallback, nextTimeout, 0);
  } else {
    CONN_LOG(conn, "Timer already active, not restarting");
  }
  CONN_LOG(conn, "========== MRConn_SwitchState END ==========");
}

static void MRConn_AuthCallback(redisAsyncContext *c, void *r, void *privdata) {
  MRConn *conn = c->data;
  redisReply *rep = r;
  if (!conn || conn->state == MRConn_Freeing) {
    // Will be picked up by disconnect callback
    goto cleanup;
  }

  if (c->err || !r) {
    detachFromConn(conn, !!r);
    MRConn_SwitchState(conn, MRConn_Connecting);
    goto cleanup;
  }

  /* AUTH error */
  if (MRReply_Type(rep) == REDIS_REPLY_ERROR) {
    size_t len;
    const char* s = MRReply_String(rep, &len);
    CONN_LOG(conn, "Error authenticating: %.*s", (int)len, s);
    MRConn_SwitchState(conn, MRConn_ReAuth);
    /*we don't try to reconnect to failed connections */
    goto cleanup;
  }

  /* Success! we are now connected! */
  MRConn_SwitchState(conn, MRConn_Connected);

cleanup:
  // We run with `REDIS_OPT_NOAUTOFREEREPLIES` so we need to free the reply ourselves
  MRReply_Free(rep);
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

/* Callback for passing a keyfile password stored as an sds to OpenSSL */
static int MRConn_TlsPasswordCallback(char *buf, int size, int rwflag, void *u) {
    const char *pass = u;
    size_t pass_len;

    if (!pass) return -1;
    pass_len = strlen(pass);
    if (pass_len > (size_t) size) return -1;
    memcpy(buf, pass, pass_len);

    return (int) pass_len;
}

static SSL_CTX* MRConn_CreateSSLContext(const char *cacert_filename,
				         const char *cert_filename,
					 const char *private_key_filename,
					 const char *private_key_pass,
					 redisSSLContextError *error)
{
    SSL_CTX *ssl_ctx = SSL_CTX_new(SSLv23_client_method());
    if (!ssl_ctx) {
        if (error) *error = REDIS_SSL_CTX_CREATE_FAILED;
        goto error;
    }

    SSL_CTX_set_options(ssl_ctx, SSL_OP_NO_SSLv2 | SSL_OP_NO_SSLv3);
    SSL_CTX_set_verify(ssl_ctx, SSL_VERIFY_PEER, NULL);

    /* always set the callback, otherwise if key is encrypted and password
     * was not given, we will be waiting on stdin. */
    SSL_CTX_set_default_passwd_cb(ssl_ctx, MRConn_TlsPasswordCallback);
    SSL_CTX_set_default_passwd_cb_userdata(ssl_ctx, (void *) private_key_pass);

    if ((cert_filename != NULL && private_key_filename == NULL) ||
            (private_key_filename != NULL && cert_filename == NULL)) {
        if (error) *error = REDIS_SSL_CTX_CERT_KEY_REQUIRED;
        goto error;
    }

    if (cacert_filename) {
        if (!SSL_CTX_load_verify_locations(ssl_ctx, cacert_filename, NULL)) {
            if (error) *error = REDIS_SSL_CTX_CA_CERT_LOAD_FAILED;
            goto error;
        }
    }

    if (cert_filename) {
        if (!SSL_CTX_use_certificate_chain_file(ssl_ctx, cert_filename)) {
            if (error) *error = REDIS_SSL_CTX_CLIENT_CERT_LOAD_FAILED;
            goto error;
        }
        if (!SSL_CTX_use_PrivateKey_file(ssl_ctx, private_key_filename, SSL_FILETYPE_PEM)) {
            if (error) *error = REDIS_SSL_CTX_PRIVATE_KEY_LOAD_FAILED;
            goto error;
        }
    }

    return ssl_ctx;

error:
    if (ssl_ctx) SSL_CTX_free(ssl_ctx);
    return NULL;
}

extern RedisModuleCtx *RSDummyContext;
static int checkTLS(char** client_key, char** client_cert, char** ca_cert, char** key_pass){
  int ret = 1;
  RedisModuleCtx *ctx = RSDummyContext;
  RedisModule_ThreadSafeContextLock(ctx);
  char* clusterTls = NULL;
  char* tlsPort = NULL;

  // If `tls-cluster` is not set to `yes`, we do not connect to the other nodes
  // with TLS on OSS-cluster. On Enterprise, we always want to connect with TLS
  // when the tls-port is set to a non-zero value, since this is the port we
  // get from the proxy.
  clusterTls = getRedisConfigValue(ctx, "tls-cluster");
  if (!clusterTls || strcmp(clusterTls, "yes")) {
    tlsPort = getRedisConfigValue(ctx, "tls-port");
    if (!IsEnterprise() || !tlsPort || !strcmp(tlsPort, "0")) {
      ret = 0;
      goto done;
    }
  }

  *client_key = getRedisConfigValue(ctx, "tls-key-file");
  *client_cert = getRedisConfigValue(ctx, "tls-cert-file");
  *ca_cert = getRedisConfigValue(ctx, "tls-ca-cert-file");
  *key_pass = getRedisConfigValue(ctx, "tls-key-file-pass");

  if (!*client_key || !*client_cert || !*ca_cert){
    ret = 0;
    if(*client_key){
      rm_free(*client_key);
    }
    if(*client_cert){
      rm_free(*client_cert);
    }
    if(*ca_cert){
      rm_free(*client_cert);
    }
  }

done:
  if (clusterTls) {
    rm_free(clusterTls);
  }
  if (tlsPort) {
    rm_free(tlsPort);
  }
  RedisModule_ThreadSafeContextUnlock(ctx);
  return ret;
}

/*
 * hiredis async connect callback.
 *
 * MOD-13979 INVESTIGATION:
 * This callback is the KEY indicator of the bug:
 *   - If called with status=REDIS_OK: Connection succeeded normally
 *   - If called with status=REDIS_ERR + errstr="Timeout": Timeout timer fired (good!)
 *   - If NEVER called: Connection is hung because no timeout was set (BUG!)
 */
static void MRConn_ConnectCallback(const redisAsyncContext *c, int status) {
  char ts[16];
  logTimestamp(ts, sizeof(ts));

  MRConn *conn = c->data;

  if (!conn) {
    CONN_LOG_RAW("========== MRConn_ConnectCallback [%s] ==========", ts);
    CONN_LOG_RAW("conn is NULL (already detached), status=%s",
                 status == REDIS_OK ? "OK" : "ERR");
    CONN_LOG_RAW("c->c.fd=%d, c->err=%d, c->errstr=%s", c->c.fd, c->err, c->errstr);
    if (status == REDIS_OK) {
      CONN_LOG_RAW("Freeing orphaned successful connection");
      redisAsyncFree((redisAsyncContext *)c);
    }
    CONN_LOG_RAW("========== MRConn_ConnectCallback END ==========");
    return;
  }

  CONN_LOG(conn, "========== MRConn_ConnectCallback [%s] ==========", ts);
  CONN_LOG(conn, "*** CALLBACK FIRED! This is CRITICAL for MOD-13979 ***");
  CONN_LOG(conn, "status: %s", status == REDIS_OK ? "REDIS_OK (success!)" : "REDIS_ERR (failure)");
  CONN_LOG(conn, "c->err: %d", c->err);
  CONN_LOG(conn, "c->errstr: %s", c->errstr);
  CONN_LOG(conn, "c->c.fd: %d", c->c.fd);
  CONN_LOG(conn, "c->c.flags: 0x%x (REDIS_CONNECTED=%d)",
           c->c.flags, (c->c.flags & REDIS_CONNECTED) ? 1 : 0);

  if (c->err && strstr(c->errstr, "Timeout") != NULL) {
    CONN_LOG(conn, "*** ERROR IS 'Timeout' - this means hiredis timer worked! ***");
    CONN_LOG(conn, "*** This is the EXPECTED behavior when connect_timeout is set ***");
  }

  if (status != REDIS_OK) {
    CONN_LOG(conn, "Connection FAILED - will trigger retry via MRConn_SwitchState");
    CONN_LOG(conn, "Error details: %s", c->errstr);
    detachFromConn(conn, 0);
    MRConn_SwitchState(conn, MRConn_Connecting);
    CONN_LOG(conn, "========== MRConn_ConnectCallback END (retry scheduled) ==========");
    return;
  }

  CONN_LOG(conn, "Connection SUCCEEDED - TCP handshake complete");
  CONN_LOG(conn, "========== MRConn_ConnectCallback continuing to TLS/Auth ==========");

  // todo: check if tls is require and if it does initiate a tls connection
  char* client_cert = NULL;
  char* client_key = NULL;
  char* ca_cert = NULL;
  char* key_file_pass = NULL;
  if(checkTLS(&client_key, &client_cert, &ca_cert, &key_file_pass)){
    redisSSLContextError ssl_error = 0;
    SSL_CTX *ssl_context = MRConn_CreateSSLContext(ca_cert, client_cert, client_key, key_file_pass, &ssl_error);
    rm_free(client_key);
    rm_free(client_cert);
    rm_free(ca_cert);
    if (key_file_pass) rm_free(key_file_pass);
    if(ssl_context == NULL || ssl_error != 0) {
      CONN_LOG(conn, "Error on ssl context creation: %s", (ssl_error != 0) ? redisSSLContextGetError(ssl_error) : "Unknown error");
      detachFromConn(conn, 0);  // Free the connection as well - we have an error
      MRConn_SwitchState(conn, MRConn_Connecting);
      if (ssl_context) SSL_CTX_free(ssl_context);
      return;
    }
    SSL *ssl = SSL_new(ssl_context);
    const redisContextFuncs *old_callbacks = c->c.funcs;
    if (redisInitiateSSL((redisContext *)(&c->c), ssl) != REDIS_OK) {
      const char *err = c->c.err ? c->c.errstr : "Unknown error";

      // This is a temporary fix to the bug describe on https://github.com/redis/hiredis/issues/1233.
      // In case of SSL initialization failure. We need to reset the callbacks value, as the `redisInitiateSSL`
      // function will not do it for us.
      ((struct redisAsyncContext*)c)->c.funcs = old_callbacks;

      CONN_LOG(conn, "Error on tls auth, %s.", err);
      detachFromConn(conn, 0);  // Free the connection as well - we have an error
      MRConn_SwitchState(conn, MRConn_Connecting);
      if (ssl_context) SSL_CTX_free(ssl_context);
      return;
    }
    if (ssl_context) SSL_CTX_free(ssl_context);
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
  char ts[16];
  logTimestamp(ts, sizeof(ts));

  MRConn *conn = c->data;

  if (!conn) {
    CONN_LOG_RAW("========== MRConn_DisconnectCallback [%s] ==========", ts);
    CONN_LOG_RAW("conn is NULL, status=%d, ignoring", status);
    CONN_LOG_RAW("========== MRConn_DisconnectCallback END ==========");
    return;
  }

  CONN_LOG(conn, "========== MRConn_DisconnectCallback [%s] ==========", ts);
  CONN_LOG(conn, "status=%d, c->err=%d, c->errstr=%s", status, c->err, c->errstr);

  if (conn->state != MRConn_Freeing) {
    CONN_LOG(conn, "Connection lost - scheduling reconnect");
    detachFromConn(conn, 0);
    MRConn_SwitchState(conn, MRConn_Connecting);
    CONN_LOG(conn, "========== MRConn_DisconnectCallback END (reconnect scheduled) ==========");
  } else {
    CONN_LOG(conn, "State is Freeing - cleaning up connection");
    freeConn(conn);
    /* Note: conn is freed, can't log after this */
  }
}

static MRConn *MR_NewConn(MREndpoint *ep) {
  MRConn *conn = rm_malloc(sizeof(MRConn));
  *conn = (MRConn){.state = MRConn_Disconnected, .conn = NULL, .protocol = 0};
  MREndpoint_Copy(&conn->ep, ep);
  return conn;
}

/*
 * Connect to a cluster node.
 *
 * MOD-13979 INVESTIGATION:
 * This function creates a redisOptions struct but does NOT set connect_timeout.
 * As a result, hiredis will NOT schedule a watchdog timer in its libuv adapter.
 * If the target host is unreachable, the connection will hang in "Connecting"
 * state indefinitely because:
 *   1. TCP SYN is sent but no response (network partition)
 *   2. OS retries SYN packets (default ~2 minutes on Linux)
 *   3. Hiredis has no timer to detect the stall
 *   4. Connect callback is NEVER called
 *   5. RediSearch retry logic never triggers
 *
 * Return REDIS_OK if connection started, REDIS_ERR if immediate failure.
 */
static int MRConn_Connect(MRConn *conn) {
  char ts[16];
  logTimestamp(ts, sizeof(ts));

  RS_ASSERT(!conn->conn);

  CONN_LOG(conn, "========== MRConn_Connect START [%s] ==========", ts);
  CONN_LOG(conn, "Target: %s:%d", conn->ep.host, conn->ep.port);

  /*
   * CRITICAL: This is where the bug lives!
   * We create redisOptions but do NOT set connect_timeout.
   * This means hiredis will NOT schedule any timer.
   */
  redisOptions options = {.type = REDIS_CONN_TCP,
                          .options = REDIS_OPT_NOAUTOFREEREPLIES,
                          .endpoint.tcp = {.ip = conn->ep.host, .port = conn->ep.port}};

  /* Log the state of connect_timeout - this is the key evidence */
  CONN_LOG(conn, "redisOptions created:");
  CONN_LOG(conn, "  - type: REDIS_CONN_TCP");
  CONN_LOG(conn, "  - options: REDIS_OPT_NOAUTOFREEREPLIES");
  CONN_LOG(conn, "  - endpoint.tcp.ip: %s", options.endpoint.tcp.ip);
  CONN_LOG(conn, "  - endpoint.tcp.port: %d", options.endpoint.tcp.port);
  CONN_LOG(conn, "  - connect_timeout: %p %s",
           (void*)options.connect_timeout,
           options.connect_timeout ? "SET" : "*** NULL - NO TIMER WILL BE SCHEDULED! ***");
  CONN_LOG(conn, "  - command_timeout: %p", (void*)options.command_timeout);

  if (!options.connect_timeout) {
    CONN_LOG(conn, "!!! WARNING: connect_timeout is NULL !!!");
    CONN_LOG(conn, "!!! hiredis will NOT call scheduleTimer() !!!");
    CONN_LOG(conn, "!!! If host unreachable, connection will hang FOREVER !!!");
  }

  CONN_LOG(conn, "Calling redisAsyncConnectWithOptions()...");
  redisAsyncContext *c = redisAsyncConnectWithOptions(&options);

  if (c->err) {
    CONN_LOG(conn, "redisAsyncConnectWithOptions FAILED immediately: err=%d, errstr=%s",
             c->err, c->errstr);
    CONN_LOG(conn, "This is an immediate failure (DNS, etc), not a timeout issue");
    redisAsyncFree(c);
    CONN_LOG(conn, "========== MRConn_Connect END (REDIS_ERR) ==========");
    return REDIS_ERR;
  }

  /* Connection initiated - now inspect the redisContext */
  CONN_LOG(conn, "redisAsyncConnectWithOptions succeeded (non-blocking connect started)");
  CONN_LOG(conn, "redisAsyncContext state:");
  CONN_LOG(conn, "  - c->c.fd: %d", c->c.fd);
  CONN_LOG(conn, "  - c->c.flags: 0x%x (REDIS_CONNECTED=%d)",
           c->c.flags, (c->c.flags & REDIS_CONNECTED) ? 1 : 0);
  CONN_LOG(conn, "  - c->c.connect_timeout: %p %s",
           (void*)c->c.connect_timeout,
           c->c.connect_timeout ? "SET" : "*** NULL - NO TIMER! ***");

  if (c->c.connect_timeout) {
    CONN_LOG(conn, "  - connect_timeout value: %ld sec, %ld usec",
             (long)c->c.connect_timeout->tv_sec,
             (long)c->c.connect_timeout->tv_usec);
  }

  CONN_LOG(conn, "  - c->c.command_timeout: %p", (void*)c->c.command_timeout);
  CONN_LOG(conn, "  - c->err: %d", c->err);

  conn->conn = c;
  conn->conn->data = conn;
  conn->state = MRConn_Connecting;

  CONN_LOG(conn, "Attaching to libuv event loop...");
  CONN_LOG(conn, "  - When redisLibuvAttach() is called, hiredis will call _EL_ADD_WRITE");
  CONN_LOG(conn, "  - _EL_ADD_WRITE calls refreshTimeout()");
  CONN_LOG(conn, "  - refreshTimeout() checks: REDIS_CONNECTED=%d, connect_timeout=%p",
           (c->c.flags & REDIS_CONNECTED) ? 1 : 0, (void*)c->c.connect_timeout);

  if (!c->c.connect_timeout) {
    CONN_LOG(conn, "  - Since connect_timeout is NULL, scheduleTimer() will NOT be called!");
    CONN_LOG(conn, "  - This means NO timeout will fire if connection hangs!");
  }

  redisLibuvAttach(conn->conn, uv_default_loop());
  CONN_LOG(conn, "libuv attached, ev.scheduleTimer callback is now set");

  CONN_LOG(conn, "Setting connect and disconnect callbacks...");
  redisAsyncSetConnectCallback(conn->conn, MRConn_ConnectCallback);
  redisAsyncSetDisconnectCallback(conn->conn, MRConn_DisconnectCallback);

  CONN_LOG(conn, "Connection setup complete, now waiting for async events:");
  CONN_LOG(conn, "  - If connection succeeds: MRConn_ConnectCallback(status=REDIS_OK)");
  CONN_LOG(conn, "  - If connection fails: MRConn_ConnectCallback(status=REDIS_ERR)");
  CONN_LOG(conn, "  - If host unreachable + NO timeout: *** CALLBACK NEVER FIRES! ***");

  logTimestamp(ts, sizeof(ts));
  CONN_LOG(conn, "========== MRConn_Connect END (REDIS_OK) [%s] ==========", ts);
  return REDIS_OK;
}
