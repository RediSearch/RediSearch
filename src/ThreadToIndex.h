/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include <stdlib.h>
#include <pthread.h>
#include "rmalloc.h"

#include "util/dict.h"

// Array of thread-to-index mappings. Basically, each thread will "register"
// itself with a query that it is working on, and then pop that query upon
// completion.
typedef struct {
  pthread_mutex_t lock;
  // uv_cond_t cond;
  dict *threadToId;   // Maps thread id to index
} ThreadToIndex;

// Global thread-to-index mapping
ThreadToIndex *T2I = NULL;

// ------------------ TODO ------------------
// - Add main-thread to index mapping in every command handler, remove it in the end
// - Free the values of the dict?

// ------------------------------------ API ------------------------------------
void ThreadToIndex_Init() {
  T2I = (ThreadToIndex *)rm_calloc(1, sizeof(ThreadToIndex));
  pthread_mutex_init(&T2I->lock, NULL);
  T2I->threadToId = dictCreate(&dictTypeHeapStrings, NULL);
}

void ThreadToIndex_Destroy() {
  pthread_mutex_destroy(&T2I->lock);
  dictRelease(T2I->threadToId);
  rm_free(T2I);
}


