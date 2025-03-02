/*
* Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#pragma once

#include "util/references.h"
#include <pthread.h>
#include "active_queries/active_queries.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
  pthread_t tid;         // pthread thread id
  pid_t Ltid;            // Linux thread id
  WeakRef specRef;       // Weak reference to the IndexSpec
  // WeakRef vs StrongRef consideration
  // If we obtain a strong ref then failure is possible
  // We don't want to fail in
} ThreadInfo;

// Call in module startup, initializes the thread local storage
// 0 - success, otherwise the returned int is a system error code
int ThreadLocalStorage_Init();
// Call in module shutdown, destroys the thread local storage
void ThreadLocalStorage_Destroy();

// Return the active queries list, will return null if called outside the main thread
ActiveQueries *GetActiveQueries();

// Get the thread local info for the current thread
ThreadInfo* CurrentThread_GetInfo();
// Set the current spec the current thread is working on
// If the thread will crash while pointing to this spec then the spec information will be outputted
void CurrentThread_SetIndexSpec(StrongRef specRef);
// Clear the current index spec the thread is working on
void CurrentThread_ClearIndexSpec();

#ifdef __cplusplus
}
#endif