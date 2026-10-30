/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#include "minunit.h"
#include "chan.h"
#include "rmalloc.h"
#include "rmutil/alloc.h"

#include <pthread.h>
#include <sched.h>
#include <stdatomic.h>

void testChan() {
  MRChannel *c = MR_NewChannel();
  mu_check(c != NULL);
  mu_assert_int_eq(0, MRChannel_Size(c));

  for (int i = 0; i < 100; i++) {
    int *ptr = rm_malloc(sizeof(*ptr));
    *ptr = i;
    MRChannel_Push(c, ptr);
    mu_assert_int_eq(i + 1, MRChannel_Size(c));
  }

  int count = 0;
  void *p;
  while (MRChannel_Size(c) && (p = MRChannel_Pop(c))) {
    mu_assert_int_eq(*(int *)p, count);
    count++;
    rm_free(p);
  }
  mu_assert_int_eq(100, count);
  mu_assert_int_eq(0, MRChannel_Size(c));

  MRChannel_Free(c);
}

void testTryPop() {
  MRChannel *c = MR_NewChannel();
  int first = 1, second = 2, late = 3;
  mu_check(MRChannel_TryPop(c) == NULL);
  MRChannel_Push(c, &first);
  MRChannel_Push(c, &second);
  mu_check(MRChannel_TryPop(c) == &first);
  // A reply arriving during the drain remains eligible for collection.
  MRChannel_Push(c, &late);
  mu_check(MRChannel_TryPop(c) == &second);
  mu_check(MRChannel_TryPop(c) == &late);
  mu_check(MRChannel_TryPop(c) == NULL);
  mu_assert_int_eq(0, MRChannel_Size(c));
  MRChannel_Free(c);
}

typedef struct {
  MRChannel *chan;
  atomic_bool *abortFlag;
  atomic_bool *stopFlag;
  atomic_bool parked;
  void *popped;
} popArgs;

static void *popWithFlags(void *arg) {
  popArgs *args = arg;
  atomic_store(&args->parked, true);
  args->popped = MRChannel_PopWithTimeout(args->chan, NULL, args->abortFlag, args->stopFlag, NULL);
  return NULL;
}

// Either flag must abort a parked pop once the channel is woken; a set flag
// does not hide an item that is already queued.
static void testPopAbortsOnEitherFlag(bool viaStopFlag) {
  MRChannel *c = MR_NewChannel();
  atomic_bool abortFlag = false, stopFlag = false;
  int item;
  MRChannel_Push(c, &item);
  mu_check(MRChannel_PopWithTimeout(c, NULL, &abortFlag, &stopFlag, NULL) == &item);

  popArgs args = {.chan = c, .abortFlag = &abortFlag, .stopFlag = &stopFlag, .popped = &item};
  pthread_t consumer;
  mu_check(pthread_create(&consumer, NULL, popWithFlags, &args) == 0);
  while (!atomic_load(&args.parked)) {
    sched_yield();
  }
  atomic_store(viaStopFlag ? &stopFlag : &abortFlag, true);
  MRChannel_WakeAbort(c);
  pthread_join(consumer, NULL);
  mu_check(args.popped == NULL);

  MRChannel_Push(c, &item);
  mu_check(MRChannel_PopWithTimeout(c, NULL, &abortFlag, &stopFlag, NULL) == &item);
  MRChannel_Free(c);
}

void testPopAbortsOnAbortFlag() {
  testPopAbortsOnEitherFlag(false);
}

void testPopAbortsOnStopFlag() {
  testPopAbortsOnEitherFlag(true);
}

int main(int argc, char **argv) {
  RMUTil_InitAlloc();
  MU_RUN_TEST(testChan);
  MU_RUN_TEST(testTryPop);
  MU_RUN_TEST(testPopAbortsOnAbortFlag);
  MU_RUN_TEST(testPopAbortsOnStopFlag);
  MU_REPORT();

  return minunit_status;
}
