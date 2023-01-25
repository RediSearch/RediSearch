/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#pragma once

#ifdef __cplusplus
extern "C" {
#endif

/**
 * @brief This file defines a set of reference types that can be used to handle references to IndexSpecs.
 * The API mimics some of RUST's reference types. It can be generalized to handle any struct as long as it has
 * a method to get a weak reference and a method to get a strong reference, by passing the appropriate callbacks.
 */

typedef void(*RefManager_Free)(void *obj);
typedef struct RefManager RefManager;

// For LLAPI and wrappers only. DO NOT USE directly.
void *__RefManager_Get_Object(RefManager *rm);

typedef struct StrongRef {
  RefManager *rm;
} StrongRef;

typedef struct WeakRef {
  RefManager *rm;
} WeakRef;

WeakRef WeakRef_Clone(WeakRef ref);
StrongRef WeakRef_Promote(WeakRef w_ref);
void WeakRef_Release(WeakRef w_ref);

StrongRef StrongRef_Clone(StrongRef ref);
WeakRef StrongRef_Demote(StrongRef s_ref);
void StrongRef_Release(StrongRef s_ref);
void *StrongRef_Get(StrongRef s_ref);
void StrongRef_Invalidate(StrongRef s_ref);

StrongRef StrongRef_New(void *obj, RefManager_Free freeCB);

static inline int StrongRef_Equals(StrongRef s_ref, StrongRef other) {
  return s_ref.rm == other.rm;
}

#ifdef __cplusplus
}
#endif
