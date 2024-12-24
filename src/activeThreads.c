/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "activeThreads.h"

void activeThreads_Init() {
  // Todo: It may not be ok to have `NULL` dictType. Consider creating dictType for activeThreads.
  activeThreads = rm_calloc(1, sizeof(*activeThreads));
  activeThreads->threadsToSpec = dictCreate(&dictTypeHeapStrings, NULL);
  pthread_mutex_init(&activeThreads->lock, NULL);
}

void activeThreads_Destroy() {
  dictRelease(activeThreads);
  pthread_mutex_destroy(&activeThreads->lock);
}

int activeThreads_AddThread(pthread_t thread, WeakRef spec_ref) {
  pthread_mutex_lock(&activeThreads->lock);
  int rc = dictAdd(activeThreads, &thread, NULL);
  pthread_mutex_unlock(&activeThreads->lock);
  return rc;
}

int activeThreads_RemoveThread(pthread_t thread) {
  pthread_mutex_lock(&activeThreads->lock);
  // TODO: Need to release the weak indexSpec reference (value of this key).
  int rc = dictDelete(activeThreads, &thread);
  pthread_mutex_unlock(&activeThreads->lock);
  return rc;
}
