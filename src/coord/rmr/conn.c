/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "conn.h"
#include "reply.h"
#include "hiredis/adapters/libuv.h"

#include <uv.h>
#include <signal.h>
#include <sys/param.h>
#include <stdio.h>
#include <assert.h>

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

#ifdef DEBUG_MR
  fprintf(stderr, "Sending to %s:%d\n", c->ep.host, c->ep.port);
  MRCommand_FPrint(stderr, cmd);
#endif

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
    conn->timer = rm_malloc(sizeof(uv_timer_t));
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
  // fprintf(stderr, "Connected and authenticated to %s:%d\n", conn->ep.host, conn->ep.port);
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


static char* getConfigValue(RedisModuleCtx *ctx, const char* confName){
  RedisModuleCallReply *rep = RedisModule_Call(ctx, "config", "cc", "get", confName);
  RedisModule_Assert(RedisModule_CallReplyType(rep) == REDISMODULE_REPLY_ARRAY);
  if (RedisModule_CallReplyLength(rep) == 0){
    RedisModule_FreeCallReply(rep);
    return NULL;
  }
  RedisModule_Assert(RedisModule_CallReplyLength(rep) == 2);
  RedisModuleCallReply *valueRep = RedisModule_CallReplyArrayElement(rep, 1);
  RedisModule_Assert(RedisModule_CallReplyType(valueRep) == REDISMODULE_REPLY_STRING);
  size_t len;
  const char* valueRepCStr = RedisModule_CallReplyStringPtr(valueRep, &len);

  char* res = rm_calloc(1, len + 1);
  memcpy(res, valueRepCStr, len);

  RedisModule_FreeCallReply(rep);

  return res;
}

extern RedisModuleCtx *RSDummyContext;
static int checkTLS(char** client_key, char** client_cert, char** ca_cert, char** key_pass){
  int ret = 1;
  RedisModuleCtx *ctx = RSDummyContext;
  RedisModule_ThreadSafeContextLock(ctx);
  char* clusterTls = NULL;
  char* tlsPort = NULL;

  clusterTls = getConfigValue(ctx, "tls-cluster");
  if (!clusterTls || strcmp(clusterTls, "yes")) {
    tlsPort = getConfigValue(ctx, "tls-port");
    if (!tlsPort || !strcmp(tlsPort, "0")) {
      ret = 0;
      goto done;
    }
  }

  *client_key = getConfigValue(ctx, "tls-key-file");
  *client_cert = getConfigValue(ctx, "tls-cert-file");
  *ca_cert = getConfigValue(ctx, "tls-ca-cert-file");
  *key_pass = getConfigValue(ctx, "tls-key-file-pass");

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
  MRConn *conn = rm_malloc(sizeof(MRConn));
  *conn = (MRConn){.state = MRConn_Disconnected, .conn = NULL, .protocol = 0};
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
