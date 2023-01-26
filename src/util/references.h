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

#define INVALID_STRONG_REF ((StrongRef){0})

// For LLAPI and wrappers only. DO NOT USE directly.
void *__RefManager_Get_Object(RefManager *rm);

typedef struct StrongRef {
  RefManager *rm;
} StrongRef;

typedef struct WeakRef {
  RefManager *rm;
} WeakRef;

/*************** Weak Ref API ***************/
/**
 * @brief Clone a weak reference
 */
WeakRef WeakRef_Clone(WeakRef ref);
/**
 * @brief Returns a new strong reference from a weak reference.
 * Underlying pointer will be NULL if the object is already freed or marked as invalid.
 * Original weak reference will NOT be invalidated, and still needs to be released.
 */
StrongRef WeakRef_Promote(WeakRef w_ref);
/**
 * @brief Release a weak reference
 */
void WeakRef_Release(WeakRef w_ref);

/************** Strong Ref API **************/
/**
 * @brief Clone a Strong reference.
 * Underlying pointer will be NULL if the object is marked as invalid.
 */
StrongRef StrongRef_Clone(StrongRef ref);
/**
 * @brief Demote a strong reference to a weak reference. Always returns a valid strong reference.
 * Original strong reference will NOT be invalidated, and still needs to be released (if owned).
 */
WeakRef StrongRef_Demote(StrongRef s_ref);
/**
 * @brief Release a strong reference. If the strong reference is the last one, the object will be freed.
 */
void StrongRef_Release(StrongRef s_ref);
/**
 * @brief Get the underlying object from a strong reference. This can be done only by a strong reference.
 * @returns NULL if the object is already freed or marked as invalid.
 * This means that the strong reference is invalid, and it does not need to be released.
 */
void *StrongRef_Get(StrongRef s_ref);
/**
 * @brief Invalidate the underlying object. From this point on, no new strong references can be created.
 * This is useful when the object is being freed, but we still want to keep the strong reference.
 * The strong reference will be invalidated, and it does not need to be released.
 */
void StrongRef_Invalidate(StrongRef s_ref);

/**
 * @brief Create a new weak reference to an object.
 *
 * @param obj - the object to create a reference to
 * @param freeCB - a callback to free the object when the reference count reaches 0
 * @return StrongRef - a strong reference to the object
 */
StrongRef StrongRef_New(void *obj, RefManager_Free freeCB);

static inline int StrongRef_Equals(StrongRef s_ref, StrongRef other) {
  return s_ref.rm == other.rm;
}

#ifdef __cplusplus
}
#endif
