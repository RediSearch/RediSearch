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
  mu_check(MRChannel_TryPop(c) == NULL);
  int values[] = {1, 2};
  MRChannel_Push(c, &values[0]);
  MRChannel_Push(c, &values[1]);
  mu_check(MRChannel_TryPop(c) == &values[0]);
  mu_check(MRChannel_TryPop(c) == &values[1]);
  mu_check(MRChannel_TryPop(c) == NULL);

  MRChannel_Unblock(c);
  mu_check(MRChannel_TryPop(c) == NULL);
  // TryPop must not consume the pending unblock notification.
  mu_check(MRChannel_Pop(c) == NULL);
  MRChannel_Free(c);
}

typedef struct {
  MRChannel *channel;
  void *claimed;
} Consumer;

static void *blockingConsumer(void *arg) {
  Consumer *consumer = arg;
  consumer->claimed = MRChannel_Pop(consumer->channel);
  return NULL;
}

void testTryPopWithBlockingConsumer() {
  MRChannel *c = MR_NewChannel();
  int values[] = {1, 2};
  Consumer consumer = {.channel = c};
  pthread_t thread;
  mu_assert_int_eq(0, pthread_create(&thread, NULL, blockingConsumer, &consumer));
  MRChannel_Push(c, &values[0]);
  void *claimed = MRChannel_TryPop(c);
  MRChannel_Push(c, &values[1]);
  mu_assert_int_eq(0, pthread_join(thread, NULL));

  if (!claimed) {
    claimed = MRChannel_TryPop(c);
  }
  mu_check((claimed == &values[0] && consumer.claimed == &values[1]) ||
           (claimed == &values[1] && consumer.claimed == &values[0]));
  mu_check(MRChannel_TryPop(c) == NULL);
  MRChannel_Free(c);
}

int main(int argc, char **argv) {
  RMUTil_InitAlloc();
  MU_RUN_TEST(testChan);
  MU_RUN_TEST(testTryPop);
  MU_RUN_TEST(testTryPopWithBlockingConsumer);
  MU_REPORT();

  return minunit_fail != 0;
}
