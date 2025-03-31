/*
* Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#pragma once

#include "util/references.h"
#include <pthread.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
  char *specName;        // Index name, useful if can't obtain a spec from the weak reference
  WeakRef specRef;       // Weak reference to the IndexSpec
  // WeakRef vs StrongRef consideration
  // If we obtain a strong ref then failure is possible - e.g index was just deleted after strong ref was taken
  // By obtaining a weak ref we avoid the immediate failure - it will be handled in the case we crash
  // By holding a weak ref we ensure we could still access the memory even if the thread forgot to call
  // CurrentThread_ClearIndexSpec
} SpecInfo;

#ifdef __cplusplus
}
#endif