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

bool initialized = false;

// Called when the thread finishes, releases the info
void CurrentThread_Done(void *current_info) {
  ThreadInfo *info = current_info;
  if (info) {
      rm_free(info);
  }
}

void ThreadLocalStorage_Init() {
  assert(!initialized);
  pthread_key_create(&threadInfoKey, CurrentThread_Done);

  ActiveQueries *activeQueries = ActiveQueries_Init();
  pthread_key_create(&activeQueriesKey, NULL);
  printf("ThreadLocalStorage_Init %p\n", activeQueries);
  pthread_setspecific(activeQueriesKey, activeQueries);
  initialized = true;
}

void ThreadLocalStorage_Destroy() {
  assert(initialized);
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
    pthread_setspecific(threadInfoKey, info);
  }
  return info;
}

void CurrentThread_SetIndexSpec(StrongRef specRef) {
  ThreadInfo *info = CurrentThread_GetInfo();
  info->specRef = StrongRef_Clone(specRef);
}

void CurrentThread_ClearIndexSpec() {
  ThreadInfo *info = pthread_getspecific(threadInfoKey);
  assert(info);
  if (info) {
    StrongRef_Release(info->specRef);
  }
}