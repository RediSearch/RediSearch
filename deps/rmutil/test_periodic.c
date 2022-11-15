/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include <stdio.h>
#include <redismodule.h>
#include <unistd.h>
#include "periodic.h"
#include "assert.h"
#include "test.h"
#include "alloc.h"

int timerCb(RedisModuleCtx *ctx, void *p) {
  int *x = p;
  (*x)++;
  return 1;
}

int testPeriodic() {
  volatile int x = 0;
  struct RMUtilTimer *tm = RMUtil_NewPeriodicTimer(
      timerCb, NULL, (void *)&x, (struct timespec){.tv_sec = 0, .tv_nsec = 10000000});
  while (!x) {
    // spin
  }
  ASSERT_EQUAL(0, RMUtilTimer_Terminate(tm));
  ASSERT(x > 0);
  ASSERT(x <= 100);
  return 0;
}

TEST_MAIN({
  RMUTil_InitAlloc();
  TESTFUNC(testPeriodic);
});
