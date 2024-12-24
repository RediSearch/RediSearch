/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#pragma once

#include <stdlib.h>
#include <pthread.h>

#include "rmalloc.h"
#include "util/dict.h"
#include "util/references.h"

typedef struct {
  dict *threadsToSpec;   // Thread-to-index mapping
  pthread_mutex_t lock;  // Lock for activeThreads
} safeDict;

// Global thread-to-index mapping
extern safeDict *activeThreads;

// ------------------ TODO ------------------
// - Add main-thread to index mapping in every command handler, remove it in the end
// - Add indexing thread to dict?
// - Free the values of the dict?

// ------------------------------------ API ------------------------------------
// Initialize active-threads
void activeThreads_Init();

// Destroy active-threads
void activeThreads_Destroy();

int activeThreads_AddThread(pthread_t thread, WeakRef spec_ref);

int activeThreads_RemoveThread(pthread_t thread);
