/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#pragma once

#include <pthread.h>

#include "util/dllist.h"
#include "util/references.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
  DLLIST_node llnode;
  pthread_t tid;
  StrongRef spec_ref;
} ActiveThread;

typedef struct {
  DLLIST list;
  pthread_mutex_t lock;
} ActiveThreads;

#define ACTIVE_THREADS_NEXT(step) DLLIST_ITEM((at)->llnodePln.next, ActiveThread, llnode)
#define ACTIVE_THREADS_PREV(step) DLLIST_ITEM((at)->llnodePln.prev, ActiveThread, llnode)

// Global active threads list
extern ActiveThreads *activeThreads;

/**
 * @brief Initializes the active threads data structure.
 *
 * This function allocates memory for the `activeThreads` structure and
 * initializes the doubly-linked list for storing `ActiveThread` objects. It
 * also initializes the mutex used for thread-safe operations on the
 * `activeThreads` structure.
 */
void activeThreads_Init();

/**
 * @brief Destroys the active threads data structure.
 *
 * This function releases the doubly-linked list and destroys the mutex used
 * for thread-safe operations on the `activeThreads` structure.
 */
void activeThreads_Destroy();

/**
 * @brief Adds the current thread to the active threads data structure.
 *
 * This function adds the current thread ID (obtained using `pthread_self()`)
 * and its associated `StrongRef` to the doubly-linked list. The function is
 * thread-safe and locks the mutex during the operation.
 *
 * @param spec_ref The `StrongRef` associated with the current thread.
 */
void activeThreads_AddCurrentThread(StrongRef spec_ref);

/**
 * @brief Adds a thread to the active threads data structure.
 *
 * This function adds a thread ID and its associated `StrongRef` (of the
 * `ActiveThread`) to the doubly-linked list. The function is thread-safe and
 * locks the mutex during the operation.
 *
 * @param tid The thread ID to add.
 * @param spec_ref The `StrongRef` associated with the thread.
 */
void activeThreads_AddThread(pthread_t tid, StrongRef spec_ref);

/**
 * @brief Removes the current thread from the active threads data structure.
 *
 * This function removes the current thread ID (obtained using `pthread_self()`)
 * and its associated `StrongRef` from the doubly-linked list. The value
 * destructor will release the `StrongRef`. The function is thread-safe and
 * locks the mutex during the operation.
 */
void activeThreads_RemoveCurrentThread();

/**
 * @brief Removes a thread from the active threads data structure.
 *
 * This function removes a thread ID and its associated `StrongRef` from the
 * doubly-linked list. The value destructor will release the `StrongRef`. The
 * function is thread-safe and locks the mutex during the operation.
 *
 * @param tid The thread ID to remove.
 */
void activeThreads_RemoveThread(pthread_t tid);

#ifdef __cplusplus
}
#endif
