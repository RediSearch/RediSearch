/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/


#include "util/references.h"
#include "main_thread.h"
#include "rmalloc.h"

#ifdef __linux__
#include <sys/syscall.h>
#include <unistd.h>
#endif
#include "rmutil/rm_assert.h"

// TLS key for the main thread
static pthread_key_t blockedQueriesKey;

static void __attribute__((constructor)) initializeKeys() {
  int rc = pthread_key_create(&blockedQueriesKey, NULL);
  assert(rc == 0);
}

static void __attribute__((destructor)) destroyKeys() {
  pthread_key_delete(blockedQueriesKey);
}

int MainThread_InitBlockedQueries() {
  // Assumption: the main thread called the Init function
  // If watchdog kills the process it will notify the main thread which will use this list to output useful information
  BlockedQueries *blockedQueries = BlockedQueries_Init();
  return pthread_setspecific(blockedQueriesKey, blockedQueries);
}

void MainThread_DestroyBlockedQueries() {
  BlockedQueries *blockedQueries = pthread_getspecific(blockedQueriesKey);
  BlockedQueries_Free(blockedQueries);
}

BlockedQueries *MainThread_GetBlockedQueries() {
  return pthread_getspecific(blockedQueriesKey);
}