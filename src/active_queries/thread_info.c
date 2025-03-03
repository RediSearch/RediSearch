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
// TLS key for a spec information
pthread_key_t specInfoKey;

bool initialized = false;

int ThreadLocalStorage_Init() {
  assert(!initialized);
  pthread_key_t *keys[] = { &activeQueriesKey, &specInfoKey };
  for (int i = 0; i < sizeof(keys) / sizeof(keys[0]); i++) {
    int rc = pthread_key_create(&specInfoKey, NULL);
    if (rc != 0) {
      return rc;
    }
  }
  // Assumption: the main thread called the Init function
  // If watchdog kills the process it will notify the main thread which will use this list to output useful information
  ActiveQueries *activeQueries = ActiveQueries_Init();
  int rc = pthread_setspecific(activeQueriesKey, activeQueries);
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
  pthread_key_delete(specInfoKey);
  ActiveQueries *activeQueries = pthread_getspecific(activeQueriesKey);
  ActiveQueries_Free(activeQueries);
  pthread_key_delete(activeQueriesKey);
  initialized = false;
}

ActiveQueries *GetActiveQueries() {
  return pthread_getspecific(activeQueriesKey);
}

SpecInfo *CurrentThread_GetSpecInfo() {
  SpecInfo *info = pthread_getspecific(specInfoKey);
  if (!info) {
    info = rm_calloc(1, sizeof(*info));
    int rc = pthread_setspecific(specInfoKey, info);
    if (rc) {
      rm_free(info);
      info = NULL;
    }
  }
  return info;
}

void CurrentThread_SetIndexSpec(StrongRef specRef) {
  SpecInfo *info = CurrentThread_GetSpecInfo();
  assert(specRef.rm != NULL);
  info->specRef = StrongRef_Demote(specRef);
  // we duplicate the name in case we won't be able to access the weak ref
  const IndexSpec *spec = StrongRef_Get(specRef);
  info->specName = rm_strdup(spec->name);
}

void CurrentThread_ClearIndexSpec() {
  SpecInfo *info = pthread_getspecific(specInfoKey);
  if (info) {
    assert(info->specRef.rm != NULL);
    WeakRef_Release(info->specRef);
    rm_free(info->specName);
    rm_free(info);
    pthread_setspecific(specInfoKey, NULL);
  }
}
