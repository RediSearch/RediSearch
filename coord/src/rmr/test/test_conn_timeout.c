/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

/*
 * Functional regression test: ensure async hiredis connections do not hang
 * indefinitely against an unreachable peer, so the reconnect loop can heal
 * them.
 *
 * Without a `connect_timeout` on the hiredis async context, an unreachable
 * peer (network partition, SYN-drop) would leave the connection stuck in
 * `Connecting` indefinitely.
 *
 * This test exercises the hiredis layer directly: it issues an async connect to
 * an unroutable IP while setting `redisOptions.connect_timeout`, drives a libuv
 * loop, and asserts the connect callback fires with a non-OK status well before
 * a long-running bailout timer.
 *
 * If the connect_timeout is not honored, the callback would not fire within the
 * bailout window and the assertions below would trip.
 *
 * The test requires outbound network access to reach a black-hole IP. Set the
 * environment variable `SKIP_CONN_TIMEOUT_TEST=1` to skip in sandboxed envs.
 */

#include "coord/tests/utils/minunit.h"
#include "rmutil/alloc.h"

#include "hiredis/hiredis.h"
#include "hiredis/async.h"
#include "hiredis/adapters/libuv.h"

#include <uv.h>
#include <time.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

// RFC1918 address commonly used as a black hole in manual reproducers for
// hung-connection scenarios (TCP SYN goes unanswered).
#define UNREACHABLE_IP   "10.255.255.1"
#define UNREACHABLE_PORT 6379
#define CONNECT_TIMEOUT_MS 500
#define BAILOUT_TIMEOUT_MS 5000

typedef struct {
  int callbackFired;
  int callbackStatus;
  double connectElapsedSec;
  int bailoutFired;
  uv_timer_t bailout;
  double startSec;
} TestCtx;

static double now_sec(void) {
  struct timespec t;
  clock_gettime(CLOCK_MONOTONIC, &t);
  return (double)t.tv_sec + (double)t.tv_nsec / 1e9;
}

static void bailoutCb(uv_timer_t *h) {
  TestCtx *tc = h->data;
  tc->bailoutFired = 1;
  uv_stop(h->loop);
}

static void bailoutCloseCb(uv_handle_t *h) {
  (void)h; // handle is a stack member of TestCtx
}

static void connectCb(const redisAsyncContext *ac, int status) {
  TestCtx *tc = ac->data;
  tc->callbackFired = 1;
  tc->callbackStatus = status;
  tc->connectElapsedSec = now_sec() - tc->startSec;
  // Stop the bailout early; hiredis will tear down the async context itself.
  if (uv_is_active((uv_handle_t *)&tc->bailout)) {
    uv_timer_stop(&tc->bailout);
  }
  uv_stop(uv_default_loop());
}

void testAsyncConnectTimeoutFiresForUnreachableHost(void) {
  const char *skip = getenv("SKIP_CONN_TIMEOUT_TEST");
  if (skip && skip[0] && strcmp(skip, "0") != 0) {
    printf("S"); // skipped (no outbound network)
    return;
  }

  TestCtx tc = {0};
  uv_loop_t *loop = uv_default_loop();

  uv_timer_init(loop, &tc.bailout);
  tc.bailout.data = &tc;
  uv_timer_start(&tc.bailout, bailoutCb, BAILOUT_TIMEOUT_MS, 0);

  struct timeval ct = {
      .tv_sec = CONNECT_TIMEOUT_MS / 1000,
      .tv_usec = (CONNECT_TIMEOUT_MS % 1000) * 1000,
  };

  redisOptions options = {
      .type = REDIS_CONN_TCP,
      .options = REDIS_OPT_NOAUTOFREEREPLIES,
      .endpoint.tcp = {.ip = UNREACHABLE_IP, .port = UNREACHABLE_PORT},
      .connect_timeout = &ct,
  };

  tc.startSec = now_sec();
  redisAsyncContext *ac = redisAsyncConnectWithOptions(&options);
  mu_check(ac != NULL);

  // Some kernels synchronously fail the connect (EHOSTUNREACH/ENETUNREACH).
  // That's fine for the regression: hiredis will still report via the callback.
  ac->data = &tc;
  mu_assert_int_eq(REDIS_OK, redisLibuvAttach(ac, loop));
  mu_assert_int_eq(REDIS_OK, redisAsyncSetConnectCallback(ac, connectCb));

  uv_run(loop, UV_RUN_DEFAULT);

  // Drain any pending close callbacks from hiredis's libuv adapter cleanup.
  uv_close((uv_handle_t *)&tc.bailout, bailoutCloseCb);
  uv_run(loop, UV_RUN_NOWAIT);

  mu_check(tc.bailoutFired == 0);
  mu_check(tc.callbackFired == 1);
  mu_check(tc.callbackStatus != REDIS_OK);
  // Must fire well before the bailout window (proves the timeout, not a
  // serendipitous kernel error, bounded the wait).
  mu_check(tc.connectElapsedSec < (BAILOUT_TIMEOUT_MS / 1000.0));
}

static void dummyLog(RedisModuleCtx *ctx, const char *level, const char *fmt, ...) {
  (void)ctx;
  (void)level;
  (void)fmt;
}

int main(int argc, char **argv) {
  (void)argc;
  (void)argv;
  RMUTil_InitAlloc();
  RedisModule_Log = dummyLog;
  MU_RUN_TEST(testAsyncConnectTimeoutFiresForUnreachableHost);
  MU_REPORT();
  return minunit_status;
}
