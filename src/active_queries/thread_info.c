/*
* Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "util/references.h"
#include "thread_info.h"
#include "rmalloc.h"

#ifdef __linux__
#include <syscall.h>
#endif

// TLS key for the main thread
pthread_key_t activeQueriesKey;
// TLS key for a query thread
pthread_key_t threadInfoKey;

void ThreadLocalStorage_Init() {
  pthread_key_create(&threadInfoKey, NULL);

  ActiveQueries *activeQueries = ActiveQueries_Init();
  pthread_key_create(&activeQueriesKey, NULL);
  pthread_setspecific(activeQueriesKey, activeQueries);
}

void ThreadLocalStorage_Destroy() {
  pthread_key_delete(threadInfoKey);

  ActiveQueries *activeQueries = pthread_getspecific(activeQueriesKey);
  ActiveQueries_Free(activeQueries);
  pthread_key_delete(activeQueriesKey);
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
    pthread_setspecific(threadInfoKey, info);
  }
  return info;
}

void CurrentThread_SetIndexSpec(StrongRef specRef) {
  ThreadInfo *info = CurrentThread_GetInfo();
  info->specRef = StrongRef_Clone(specRef);
}

void CurrentThread_ClearIndexSpec() {
  ThreadInfo *info = CurrentThread_GetInfo();
  StrongRef_Release(info->specRef);
}