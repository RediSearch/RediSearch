/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#include "conn.h"
#include "reply.h"
#include "coord/config.h"
#include "module.h"
#include "hiredis/adapters/libuv.h"

#include <uv.h>
#include <signal.h>
#include <sys/param.h>
#include <stdio.h>
#include <assert.h>
#include <stdint.h>

#include <openssl/ssl.h>
#include <openssl/err.h>

// Layout packs the three small fields into the padding slot before the
// inlined timer, keeping ep+conn+loop+state on the first cache line.
struct MRConn {
  MREndpoint ep;
  redisAsyncContext *conn;
  uv_loop_t *loop;
  uint16_t authFailCount;   // consecutive auth failures, for rate-limited logging
  MRConnState state;
  MRConnProtocol protocol;  // Current Redis protocol version in use on this connection
  uv_timer_t timer;         // back-off timer for Connecting/ReAuth
};

static void MRConn_ConnectCallback(const redisAsyncContext *c, int status);
static void MRConn_DisconnectCallback(const redisAsyncContext *, int);
static int MRConn_Connect(MRConn *conn);
static void MRConn_SwitchState(MRConn *conn, MRConnState nextState);
static void MRConn_Disconnect(MRConn *conn);
static MRConn *MR_NewConn(MREndpoint *ep, uv_loop_t *loop);
static int MRConn_SendAuth(MRConn *conn);

#define RECONNECT_MS_DELAY 250
#define REAUTH_MS_DELAY 1000
#define AUTH_FAIL_LOG_INTERVAL 100
#define INTERNALAUTH_USERNAME "internal connection"
#define UNUSED(x) (void)(x)

#define CONN_LOG(conn, fmt, ...)                                                      \
  RedisModule_Log(RSDummyContext, "debug", "[%p %s:%d %s] " fmt,                      \
                  conn, conn->ep.host, conn->ep.port, MRConnState_Str((conn)->state), \
                  ##__VA_ARGS__)

#define CONN_LOG_WARNING(conn, fmt, ...)                                              \
  RedisModule_Log(RSDummyContext, "warning", "[%p %s:%d %s] " fmt,                    \
                  conn, conn->ep.host, conn->ep.port, MRConnState_Str((conn)->state), \
                  ##__VA_ARGS__)

/* Sever the link between conn and its hiredis async context and ask hiredis
 * to tear the ac down. No-op when conn is not attached, and safe to call from
 * inside any hiredis callback: redisAsyncDisconnect defers via the
 * REDIS_IN_CALLBACK flag and lets hiredis's normal post-callback paths run
 * __redisAsyncFree. */
static void detachRedisAsyncContext(MRConn *conn) {
  redisAsyncContext *ac = conn->conn;
  conn->protocol = MRConn_Protocol_Undetermined;
  if (!ac) return;
  ac->data = NULL;
  conn->conn = NULL;
  redisAsyncDisconnect(ac);
}

typedef struct {
  size_t num;
  size_t rr;  // round robin counter
  MRConn **conns;
} MRConnPool;

static MRConnPool *_MR_NewConnPool(MREndpoint *ep, size_t num, uv_loop_t *loop) {
  MRConnPool *pool = rm_malloc(sizeof(*pool));
  *pool = (MRConnPool){
      .num = num,
      .rr = 0,
      .conns = rm_malloc(num * sizeof(MRConn *)),
  };

  /* Create the connection */
  for (size_t i = 0; i < num; i++) {
    pool->conns[i] = MR_NewConn(ep, loop);
  }
  return pool;
}

/* Tear down a connection: log and transition to the terminal Freeing state,
 * which detaches the hiredis ac and schedules the async free of the MRConn
 * struct. Must be called from the uv thread, and exactly once per conn —
 * uv_close inside freeConn is not idempotent. */
static inline void MRConn_Disconnect(MRConn *conn) {
  MRConn_SwitchState(conn, MRConn_Freeing);
}

/* Close callback for the inlined timer handle. libuv has released the handle
 * by the time this fires, so we can free the MRConn itself. */
static void _asyncFreeConn(uv_handle_t *h) {
  MRConn *conn = h->data;
  rm_free(conn);
}

// Free the conn.
static void freeConn(MRConn *conn) {
  MREndpoint_Free(&conn->ep);
  // Some of the teardown is asynchronous because the inlined timer handle
  // must outlive this call until libuv finishes its close sequence; the actual
  // rm_free happens in _asyncFreeConn.
  uv_close((uv_handle_t *)&conn->timer, _asyncFreeConn);
}

/* Dict value destructor: disconnect and free every conn in the pool, then
 * release the pool itself. Invoked by the dict whenever an entry is removed
 * (dictRelease, dictReplace, dictDelete), so pool lifetime is owned entirely
 * by the dict — callers just remove the entry and let this destructor run. */
static void MRConnPool_Free(void *privdata, void *p) {
  UNUSED(privdata);
  MRConnPool *pool = p;
  if (!pool) return;
  for (size_t i = 0; i < pool->num; i++) {
    MRConn_Disconnect(pool->conns[i]);
  }
  rm_free(pool->conns);
  rm_free(pool);
}

/* Get a connection from the connection pool. We select the next available connected connection with
 * a round robin selector */
static MRConn *MRConnPool_GetConn(MRConnPool *pool) {

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

/* Tear down every connection in the manager and release the dict.
 *
 * Must be called from the owning uv thread while the event loop is still alive: the
 * per-conn disconnect path invokes uv_close and redisAsyncDisconnect, both of
 * which require a live loop. After this returns the manager is empty; the
 * MRConnManager struct itself is not freed (it is embedded in IORuntimeCtx). */
void MRConnManager_Shutdown(MRConnManager *mgr) {
  dictRelease(mgr->map);
}

void MRConnManager_ReplyState(dict *stateDict, RedisModuleCtx *ctx) {
  RS_ASSERT(stateDict);
  RedisModule_ReplyWithMap(ctx, dictSize(stateDict));
  dictIterator *it = dictGetIterator(stateDict);
  dictEntry *entry;

  while ((entry = dictNext(it))) {
    // Get the key (host:port string)
    char *key = dictGetKey(entry);
    RedisModule_ReplyWithCString(ctx, key);

    // Get the value (array of connection state strings)
    arrayof(char *) stateList = dictGetVal(entry);

    // Reply with the array of connection states
    RedisModule_ReplyWithArray(ctx, array_len(stateList));
    for (size_t i = 0; i < array_len(stateList); i++) {
      RedisModule_ReplyWithCString(ctx, stateList[i]);
    }
  }

  dictReleaseIterator(it);
}

void MRConnManager_FillStateDict(MRConnManager *mgr, dict *stateDict) {
  RS_ASSERT(stateDict);
  dictIterator *it = dictGetIterator(mgr->map);
  dictEntry *entry;

  while ((entry = dictNext(it))) {
    MRConnPool *pool = dictGetVal(entry);

    // Create the key as "host:port"
    char *key;
    rm_asprintf(&key, "%s:%d", pool->conns[0]->ep.host, pool->conns[0]->ep.port);

    // Get or create an entry in the stateDict
    dictEntry *target_entry = dictAddOrFind(stateDict, key);
    arrayof(char *) stateList = dictGetVal(target_entry) ?: array_new(char *, pool->num);

    // Add connection states from this pool
    for (size_t i = 0; i < pool->num; i++) {
      const char *stateStr = MRConnState_Str(pool->conns[i]->state);
      array_append(stateList, rm_strdup(stateStr));
    }

    dictSetVal(stateDict, target_entry, stateList); // Update the value in case it was reallocated
    rm_free(key);
  }

  dictReleaseIterator(it);
}


/* Get the connection for a specific node by id, return NULL if this node is not in the pool */
MRConn *MRConn_Get(MRConnManager *mgr, const char *id) {
  dictEntry *ptr = dictFind(mgr->map, id);
  if (ptr) {
    MRConnPool *pool = dictGetVal(ptr);
    return MRConnPool_GetConn(pool);
  }
  return NULL;
}

/* Get the state string of the first connection for a specific node by id.
 * Returns NULL if this node is not in the pool.
 * Must be called from the uv event loop thread, as mgr->map is not thread-safe. */
const char *MRConnManager_GetNodeState(MRConnManager *mgr, const char *id) {
  dictEntry *ptr = dictFind(mgr->map, id);
  if (ptr) {
    MRConnPool *pool = dictGetVal(ptr);
    // All connections in the pool share the same endpoint, so any one is representative.
    if (pool->num > 0 && pool->conns[0]) {
      return MRConnState_Str(pool->conns[0]->state);
    }
  }
  return NULL;
}

/* Send a command to the connection */
int MRConn_SendCommand(MRConn *c, MRCommand *cmd, redisCallbackFn *fn, void *privdata) {

  /* Only send to connected nodes */
  if (c->state != MRConn_Connected) {
    CONN_LOG_WARNING(c, "Tried to send command to node in state %s", MRConnState_Str(c->state));
    return REDIS_ERR;
  }

  if (!cmd->cmd) {
    if (redisFormatSdsCommandArgv(&cmd->cmd, cmd->num, (const char **)cmd->strs, cmd->lens) == REDIS_ERR) {
      CONN_LOG_WARNING(c, "Failed to format command");
      return REDIS_ERR;
    }
  }
  MRConnProtocol requiredProtocol = (MRConnProtocol)cmd->protocol;
  if (requiredProtocol != MRConn_Protocol_Undetermined && c->protocol != requiredProtocol) {
    RS_ASSERT(requiredProtocol == MRConn_Protocol_RESP2 || requiredProtocol == MRConn_Protocol_RESP3);
    if (redisAsyncCommand(c->conn, NULL, NULL, "HELLO %d", requiredProtocol) == REDIS_ERR) {
      return REDIS_ERR;
    }
    c->protocol = requiredProtocol;
  }
  return redisAsyncFormattedCommand(c->conn, fn, privdata, cmd->cmd, sdslen(cmd->cmd));
}

/* Add a node to the connection manager and start its connections. Returns
 * true iff the connection pool was (re)created, i.e. the endpoint was new or
 * differs from the one currently registered for `id`; returns false when the
 * existing pool already matches `ep` and was left untouched.
 * Endpoint equality covers host, port, unixSock and password; a password
 * rotation therefore forces a pool rebuild rather than silently reusing
 * connections that would AUTH with stale credentials on reconnect. */
bool MRConnManager_Add(MRConnManager *m, uv_loop_t *loop, const char *id, MREndpoint *ep) {
  /* First try to see if the connection is already in the manager */
  dictEntry *ptr = dictFind(m->map, id);
  if (ptr) {
    MRConnPool *pool = dictGetVal(ptr);

    MRConn *conn = pool->conns[0];
    if (MREndpoint_Equal(&conn->ep, ep)) {
      return false;
    }

    // Endpoint changed - dictReplace below will disconnect+free the old pool
    // via the dict value destructor (MRConnPool_Free).
    RedisModule_Log(RSDummyContext, "notice",
                    "MRConnManager_Add: Node %s endpoint changed from %s:%d to %s:%d, reconnecting (state: %s)",
                    id, conn->ep.host, conn->ep.port, ep->host, ep->port, MRConnState_Str(conn->state));
  }

  MRConnPool *pool = _MR_NewConnPool(ep, m->nodeConns, loop);
  dictReplace(m->map, (void *)id, pool);
  return true;
}

/* Explicitly disconnect a connection and remove it from the connection pool.
 * The dict value destructor (MRConnPool_Free) handles disconnect + free. */
int MRConnManager_Disconnect(MRConnManager *m, const char *id) {
  return dictDelete(m->map, id) == DICT_OK ? REDIS_OK : REDIS_ERR;
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
      MRConn_Disconnect(pool->conns[i]);
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
void MRConnManager_Expand(MRConnManager *m, size_t num, uv_loop_t *loop) {
  dictIterator *it = dictGetIterator(m->map);
  dictEntry *entry;
  while ((entry = dictNext(it))) {
    MRConnPool *pool = dictGetVal(entry);

    pool->conns = rm_realloc(pool->conns, num * sizeof(MRConn *));
    // Use the first connection's endpoint to create new connections
    // There should always be at least one connection in the pool
    MREndpoint *ep = &pool->conns[0]->ep;
    for (size_t i = pool->num; i < num; i++) {
      pool->conns[i] = MR_NewConn(ep, loop);
    }
    pool->num = num;
  }
  m->nodeConns = num;
  dictReleaseIterator(it);
}

static inline void doConnect(MRConn *conn) {
  if (MRConn_Connect(conn) != REDIS_OK) {
    MRConn_SwitchState(conn, MRConn_Reconnecting);
  }
}

static inline void doAuthenticate(MRConn *conn) {
  if (MRConn_SendAuth(conn) != REDIS_OK) {
    if (conn->authFailCount % AUTH_FAIL_LOG_INTERVAL == 0) {
      CONN_LOG_WARNING(conn, "Failed to send AUTH command (%hu consecutive failures)", conn->authFailCount + 1);
    }
    conn->authFailCount++;
    MRConn_SwitchState(conn, MRConn_Reconnecting);
  }
}

/* Timer callback armed while in MRConn_Reconnecting. Re-issues the async
 * connect in-place so the observable state stays Reconnecting for the whole
 * retry cycle (mirrors reauthTimerCallback). */
static void reconnectTimerCallback(uv_timer_t *tm) {
  MRConn *conn = tm->data;
  RS_ASSERT(conn->state == MRConn_Reconnecting);
  doConnect(conn);
}

/* Timer callback armed while in MRConn_ReAuth. Re-sends AUTH after the reauth
 * back-off so the server has time to recover and we don't tight-loop on
 * repeated auth rejections. */
static void reauthTimerCallback(uv_timer_t *tm) {
  MRConn *conn = tm->data;
  RS_ASSERT(conn->state == MRConn_ReAuth);
  doAuthenticate(conn);
}

/* Main state transition function. */
static void MRConn_SwitchState(MRConn *conn, MRConnState nextState) {
  CONN_LOG(conn, "Switching state to %s", MRConnState_Str(nextState));

  // Freeing is terminal: no caller should attempt a second transition. The
  // Freeing case detaches the ac and hands conn off to freeConn; transitioning
  // again would revive a half-torn-down conn or double-close its timer handle.
  RS_ASSERT(conn->state != MRConn_Freeing);

  // Timer-state invariant: the only states we may switch *to* with an armed timer are
  // Freeing (explicit stop) and Reconnecting (from an unexpected disconnect callback).
  // They are the only states that has to handle a timer stop.
  // We reach any other state linearly from the previous one, so no timer should be active on the transition.
  RS_ASSERT(!uv_is_active(&conn->timer) || nextState == MRConn_Reconnecting || nextState == MRConn_Freeing);

  conn->state = nextState;
  switch (nextState) {

    case MRConn_Reconnecting:
      // Detach the ac and arm the back-off timer so we don't spin on a
      // server that just rejected us.
      detachRedisAsyncContext(conn);
      uv_timer_start(&conn->timer, reconnectTimerCallback, RECONNECT_MS_DELAY, 0);
      return;

    case MRConn_ReAuth:
      // Delayed state: the reauth timer gives the server time to recover
      // and avoids a tight AUTH-retry loop on repeated rejections.
      uv_timer_start(&conn->timer, reauthTimerCallback, REAUTH_MS_DELAY, 0);
      return;

    case MRConn_Connecting:
      doConnect(conn);
      return;

    case MRConn_Authenticating:
      doAuthenticate(conn);
      return;

    case MRConn_Connected:
      // Steady state: nothing to do on the transition itself
      return;

    case MRConn_Freeing:
      // Terminal state: detach the ac and hand the conn off to freeConn.
      detachRedisAsyncContext(conn);
      freeConn(conn);
      return;
  }
  RS_ABORT_FMT("MRConn_SwitchState: invalid target state %d", nextState);
}

static void MRConn_AuthCallback(redisAsyncContext *c, void *r, void *privdata) {
  MRConn *conn = c->data;
  redisReply *rep = r;

  if (!conn) {
    // Will be picked up by disconnect callback
    goto cleanup;
  }
  // Entered only while the AUTH we issued is in flight.
  RS_ASSERT(conn->state == MRConn_Authenticating || conn->state == MRConn_ReAuth);

  if (!r || c->err) {
    // ac is being torn down (r==NULL) or in an error state; reconnect.
    MRConn_SwitchState(conn, MRConn_Reconnecting);
    goto cleanup;
  }

  /* AUTH error */
  if (MRReply_Type(rep) == REDIS_REPLY_ERROR) {
    size_t len;
    const char* s = MRReply_String(rep, &len);
    CONN_LOG_WARNING(conn, "Error authenticating: %.*s", (int)len, s);
    MRConn_SwitchState(conn, MRConn_ReAuth);
    /*we don't try to reconnect to failed connections */
    goto cleanup;
  }

  /* Success! we are now connected! */
  conn->authFailCount = 0;
  MRConn_SwitchState(conn, MRConn_Connected);

cleanup:
  // We run with `REDIS_OPT_NOAUTOFREEREPLIES` so we need to free the reply ourselves
  MRReply_Free(rep);
}

/* Issue AUTH on the current ac. Only called from the Authenticating case of
 * MRConn_SwitchState, which handles the REDIS_ERR path by detaching and
 * transitioning back to Connecting. */
static int MRConn_SendAuth(MRConn *conn) {
  RS_ASSERT(conn->state == MRConn_Authenticating || conn->state == MRConn_ReAuth);

  size_t len = 0;

  if (!IsEnterprise()) {
    // Take the GIL before calling the internal function getter
    RedisModule_ThreadSafeContextLock(RSDummyContext);
    const char *internal_secret = RedisModule_GetInternalSecret(RSDummyContext, &len);
    // Create a local copy of the secret so we can release the GIL.
    int status = redisAsyncCommand(conn->conn, MRConn_AuthCallback, NULL,
        "AUTH %s %b", INTERNALAUTH_USERNAME, internal_secret, len);
    RedisModule_ThreadSafeContextUnlock(RSDummyContext);
    return status;
  } else {
    // On Enterprise, we use the password we got from `CLUSTERSET`.
    // If we got here, we know we have a password.
    return redisAsyncCommand(conn->conn, MRConn_AuthCallback, NULL, "AUTH %s",
        conn->ep.password);
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
static int checkTLS(RedisModuleString **client_key, RedisModuleString **client_cert,
                    RedisModuleString **ca_cert, RedisModuleString **key_pass) {
  int ret = 1;
  RedisModuleCtx *ctx = RSDummyContext;
  RedisModule_ThreadSafeContextLock(ctx);

  // If `tls-cluster` is not set to `yes`, and `tls-port` is not set or zero,
  // we do not connect to the other nodes with TLS. We always want to connect with TLS
  // when the tls-port is set to a non-zero value, since this is the port we
  // get from the proxy on Enterprise, and the preferred port on OSS (see RedisCluster_GetTopology).
  if (!getRedisConfigBool(ctx, "tls-cluster", false)) {
    if (getRedisConfigNumeric(ctx, "tls-port", 0) == 0) {
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
      RedisModule_FreeString(ctx, *client_key);
      *client_key = NULL;
    }
    if(*client_cert){
      RedisModule_FreeString(ctx, *client_cert);
      *client_cert = NULL;
    }
    if(*ca_cert){
      RedisModule_FreeString(ctx, *ca_cert);
      *ca_cert = NULL;
    }
    if (*key_pass) {
      RedisModule_FreeString(ctx, *key_pass);
      *key_pass = NULL;
    }
  }

done:
  RedisModule_ThreadSafeContextUnlock(ctx);
  return ret;
}

/* If TLS is configured for the cluster, build an SSL context and bind it to
 * the given hiredis async context. Returns REDIS_OK on success (including
 * "TLS not configured", which is a no-op) and REDIS_ERR on any setup failure;
 * a warning is logged on failure. The caller owns the ac on failure. */
static int MRConn_InitTLS(MRConn *conn, redisAsyncContext *c) {
  RedisModuleString *client_cert = NULL, *client_key = NULL, *ca_cert = NULL, *key_file_pass = NULL;
  if (!checkTLS(&client_key, &client_cert, &ca_cert, &key_file_pass)) {
    return REDIS_OK;
  }

  redisSSLContextError ssl_error = 0;
  SSL_CTX *ssl_context = MRConn_CreateSSLContext(
      RedisModule_StringPtrLen(ca_cert, NULL),
      RedisModule_StringPtrLen(client_cert, NULL),
      RedisModule_StringPtrLen(client_key, NULL),
      key_file_pass ? RedisModule_StringPtrLen(key_file_pass, NULL) : NULL,
      &ssl_error);

  RedisModule_FreeString(RSDummyContext, client_key);
  RedisModule_FreeString(RSDummyContext, client_cert);
  RedisModule_FreeString(RSDummyContext, ca_cert);
  if (key_file_pass) RedisModule_FreeString(RSDummyContext, key_file_pass);

  if (ssl_context == NULL || ssl_error != 0) {
    CONN_LOG_WARNING(conn, "Error on ssl context creation: %s",
                     ssl_error != 0 ? redisSSLContextGetError(ssl_error) : "Unknown error");
    if (ssl_context) SSL_CTX_free(ssl_context);
    return REDIS_ERR;
  }

  SSL *ssl = SSL_new(ssl_context);
  if (!ssl) {
    CONN_LOG_WARNING(conn, "Error creating SSL object");
    SSL_CTX_free(ssl_context);
    return REDIS_ERR;
  }

  int rc = redisInitiateSSL(&c->c, ssl);
  SSL_CTX_free(ssl_context);
  if (rc != REDIS_OK) {
    CONN_LOG_WARNING(conn, "Error on tls auth, %s.", c->c.err ? c->c.errstr : "Unknown error");
    SSL_free(ssl);
    return REDIS_ERR;
  }

  return REDIS_OK;
}

/* hiredis async connect callback.
 * conn (c->data) can be NULL if detachFromConn was called before the connect completed
 * (e.g., MRConn_Freeing with deferred disconnect). Both status values are expected. */
static void MRConn_ConnectCallback(const redisAsyncContext *c, int status) {
  MRConn *conn = c->data;
  if (!conn) {
    // The connection was already freed, we need to clean up the redisAsyncContext
    if (status == REDIS_OK) {
      // We need to free it here because we will not be getting a disconnect
      // callback.
      redisAsyncFree((redisAsyncContext *)c);
    }
    return;
  }

  // Freeing detaches the ac before tearing it down, so we can't be here.
  RS_ASSERT(conn->state != MRConn_Freeing);

  // if the connection is not stopped - try to reconnect
  if (status != REDIS_OK) {
    CONN_LOG_WARNING(conn, "Error on connect: %s", c->errstr);
    MRConn_SwitchState(conn, MRConn_Reconnecting);
    return;
  }

  if (MRConn_InitTLS(conn, (redisAsyncContext *)c) != REDIS_OK) {
    MRConn_SwitchState(conn, MRConn_Reconnecting);
    return;
  }

  // Authenticate on OSS always (as an internal connection), or on Enterprise if
  // a password is set to the `default` ACL user.
  if (!IsEnterprise() || conn->ep.password) {
    MRConn_SwitchState(conn, MRConn_Authenticating);
  } else {
    MRConn_SwitchState(conn, MRConn_Connected);
  }
}

static void MRConn_DisconnectCallback(const redisAsyncContext *c, int status) {
  MRConn *conn = c->data;
  if (!conn) {
    /* Ignore */
    return;
  }
  // Freeing detaches the ac before tearing it down, so we can't be here.
  RS_ASSERT(conn->state != MRConn_Freeing);
  MRConn_SwitchState(conn, MRConn_Reconnecting);
}

/* Create a new MRConn for the given endpoint and kick off its first
 * connection attempt via SwitchState(Connecting), which dispatches the async
 * connect and falls back to Reconnecting on synchronous failure. The initial
 * Reconnecting value is just a placeholder overwritten by SwitchState. */
static MRConn *MR_NewConn(MREndpoint *ep, uv_loop_t *loop) {
  MRConn *conn = rm_new(MRConn);
  *conn = (MRConn){
    .state = MRConn_Connecting,
    .conn = NULL,
    .protocol = MRConn_Protocol_Undetermined,
    .loop = loop,
    .authFailCount = 0,
  };
  uv_timer_init(loop, &conn->timer);
  conn->timer.data = conn;
  MREndpoint_Copy(&conn->ep, ep);

  MRConn_SwitchState(conn, MRConn_Connecting);
  return conn;
}

/* Initiate an async connection attempt. Must be called with `conn->conn ==
 * NULL` (i.e. no ac currently attached) and state in {Connecting, Reconnecting}
 * \u2014 the initial attempt runs in Connecting, retries run in Reconnecting.
 * Returns REDIS_OK if the attempt was dispatched to libuv or REDIS_ERR on
 * synchronous setup failure; on REDIS_ERR `conn->conn` is left NULL. */
static int MRConn_Connect(MRConn *conn) {
  RS_ASSERT(conn->conn == NULL);
  RS_ASSERT(conn->state == MRConn_Connecting || conn->state == MRConn_Reconnecting);
  // Bounds the async TCP+TLS handshake. Without it, a blackholed SYN can leave
  // the ac stuck in SYN-SENT indefinitely, because neither ConnectCallback nor
  // DisconnectCallback will fire and no retry is scheduled. With it, hiredis
  // surfaces a timeout via ConnectCallback(REDIS_ERR), which drops the conn
  // into Reconnecting with the retry timer armed. No command_timeout is set:
  // legitimate queries may run for many seconds.
  const struct timeval *connectTimeout = &clusterConfig.connectTimeout;
  const bool connectTimeoutEnabled = connectTimeout->tv_sec || connectTimeout->tv_usec;
  redisOptions options = {.type = REDIS_CONN_TCP,
                          .options = REDIS_OPT_NOAUTOFREEREPLIES,
                          .connect_timeout = connectTimeoutEnabled ? connectTimeout : NULL,
                          .endpoint.tcp = {.ip = conn->ep.host, .port = conn->ep.port}};

  redisAsyncContext *c = redisAsyncConnectWithOptions(&options);
  if (!c) {
    CONN_LOG(conn, "Could not allocate async context with the given options");
    return REDIS_ERR;
  }
  if (c->err) {
    CONN_LOG_WARNING(conn, "Could not connect to node: %s", c->errstr);
    redisAsyncFree(c);
    return REDIS_ERR;
  }
  if (redisLibuvAttach(c, conn->loop) != REDIS_OK ||
      redisAsyncSetConnectCallback(c, MRConn_ConnectCallback) != REDIS_OK ||
      redisAsyncSetDisconnectCallback(c, MRConn_DisconnectCallback) != REDIS_OK) {
    CONN_LOG_WARNING(conn, "Failed to attach hiredis context to libuv");
    redisAsyncFree(c);
    return REDIS_ERR;
  }

  // All setup succeeded; take ownership of the ac.
  conn->conn = c;
  c->data = conn;
  conn->protocol = MRConn_Protocol_Undetermined;
  return REDIS_OK;
}
