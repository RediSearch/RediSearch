/*
* Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "util/references.h"
#include "thread_info.h"
#include "rmalloc.h"

#ifdef __linux__
#include <sys/syscall.h>
#include <unistd.h>
#endif

// TLS key for the main thread
pthread_key_t activeQueriesKey;
// TLS key for a query thread
pthread_key_t threadInfoKey;

bool initialized = false;

int ThreadLocalStorage_Init() {
  assert(!initialized);
  int rc = pthread_key_create(&threadInfoKey, NULL);
  if (rc) {
    return rc;
  }
  rc = pthread_key_create(&activeQueriesKey, NULL);
  if (rc) {
    return rc;
  }
  ActiveQueries *activeQueries = ActiveQueries_Init();
  rc = pthread_setspecific(activeQueriesKey, activeQueries);
  if (rc) {
    if (activeQueries) {
      ActiveQueries_Free(activeQueries);
    }
    return rc;
  }
  initialized = true;
  return rc;
}

void ThreadLocalStorage_Destroy() {
  if (!initialized) {
    return;
  }
  pthread_key_delete(threadInfoKey);
  ActiveQueries *activeQueries = pthread_getspecific(activeQueriesKey);
  ActiveQueries_Free(activeQueries);
  pthread_key_delete(activeQueriesKey);
  initialized = false;
}

ActiveQueries *GetActiveQueries() {
  return pthread_getspecific(activeQueriesKey);
}

ThreadInfo *CurrentThread_GetInfo() {
  ThreadInfo *info = pthread_getspecific(threadInfoKey);
  if (!info) {
    info = rm_calloc(1, sizeof(*info));
    info->tid = pthread_self();
#ifdef __linux__
    info->Ltid = syscall(SYS_gettid);
#endif
    int rc = pthread_setspecific(threadInfoKey, info);
    if (rc) {
      rm_free(info);
      info = NULL;
    }
  }
  return info;
}

void CurrentThread_SetIndexSpec(StrongRef specRef) {
  ThreadInfo *info = CurrentThread_GetInfo();
  if (info) {
    info->specRef = StrongRef_Clone(specRef);
    assert(info->specRef.rm != NULL);
  }
}

void CurrentThread_ClearIndexSpec() {
  ThreadInfo *info = pthread_getspecific(threadInfoKey);
  if (info) {
    assert(info->specRef.rm != NULL);
    StrongRef_Release(info->specRef);
    rm_free(info);
    pthread_setspecific(threadInfoKey, NULL);
  }
}