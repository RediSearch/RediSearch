/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "activeThreads.h"
#include "rmutil/rm_assert.h"

ActiveThreads *activeThreads = NULL;

// TLS key for the active thread
pthread_key_t _activeThreadKey;

/**
 * @brief Initializes the active threads data structure.
 *
 * This function allocates memory for the `activeThreads` structure and initializes
 * the dictionary for mapping thread IDs to `StrongRef` objects' `RefManager`. It
 * also initializes the mutex used for thread-safe operations on the
 * `activeThreads` structure.
 */
void activeThreads_Init() {
  activeThreads = rm_calloc(1, sizeof(ActiveThreads));
  dllist_init(&activeThreads->list);
  pthread_mutex_init(&activeThreads->lock, NULL);
  pthread_key_create(&_activeThreadKey, NULL);
}

/**
 * @brief Destroys the active threads data structure.
 *
 * This function releases the dictionary and destroys the mutex used for thread-safe
 * operations on the `activeThreads` structure.
 */
void activeThreads_Destroy() {
  pthread_mutex_destroy(&activeThreads->lock);
  rm_free(activeThreads);
  pthread_key_delete(_activeThreadKey);
}

/**
 * @brief Adds the current thread to the active threads data structure.
 *
 * This function adds the current thread ID (obtained using `pthread_self()`) and its
 * associated `StrongRef` to the dictionary. The function is thread-safe and locks the
 * mutex during the operation.
 *
 * @param spec_ref The `StrongRef` associated with the current thread.
 */
void activeThreads_AddCurrentThread(StrongRef spec_ref) {
  activeThreads_AddThread(pthread_self(), spec_ref);
}

/**
 * @brief Adds a thread to the active threads data structure.
 *
 * This function adds a thread ID and its associated `RefManager` (of the `StrongRef`)
 * to the dictionary.
 * The function is thread-safe and locks the mutex during the operation.
 *
 * @param tid The thread ID to add.
 * @param spec_ref The `StrongRef` associated with the thread.
 */
void activeThreads_AddThread(pthread_t tid, StrongRef spec_ref) {
  ActiveThread *at = rm_calloc(1, sizeof(ActiveThread));
  at->tid = tid;
  at->spec_ref = spec_ref;

  pthread_mutex_lock(&activeThreads->lock);
  dllist_append(&activeThreads->list, &at->llnode);
  pthread_mutex_unlock(&activeThreads->lock);
  
  // Set the thread-specific data
  pthread_setspecific(_activeThreadKey, at);
}

/**
 * @brief Removes the current thread from the active threads data structure.
 *
 * This function removes the current thread ID (obtained using `pthread_self()`) and
 * its associated `StrongRef` from the dictionary. The value destructor will release the
 * `StrongRef`. The function is thread-safe and locks the mutex during the operation.
 */
void activeThreads_RemoveCurrentThread() {
  activeThreads_RemoveThread(pthread_self());
}
 
/**
 * @brief Removes a thread from the active threads data structure.
 *
 * This function removes a thread ID and its associated `StrongRef` from the dictionary.
 * The value destructor will release the `StrongRef`. The function is thread-safe and
 * locks the mutex during the operation.
 *
 * @param tid The thread ID to remove.
 */
void activeThreads_RemoveThread(pthread_t thread) {
  ActiveThread *at = (ActiveThread *)pthread_getspecific(_activeThreadKey);
  RS_LOG_ASSERT(at != NULL, "Active thread not found");

  pthread_mutex_lock(&activeThreads->lock);
  dllist_delete(&at->llnode);
  pthread_mutex_unlock(&activeThreads->lock);

  // The StrongRef is released later by the thread.
}
