/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "references.h"
#include "rmalloc.h"
#include <stdbool.h>

// This is a weak reference to an object. It is used to prevent using an object that is being freed.
// The object is freed when the strong refcount is 0.

// Promises:
// 1. If the strong refcount gets to 0, it will never be increased again

// By using this functions through the strong and weak refcount API, we can guarantee that
// The spec will be freed before the weak refcount reaches 0.

// The Invalidate function is being used by the drop index command to prevent using the spec after it is dropped.
// It first remove the spec from the specDict_g, then it sets the isInvalid flag to true.
// From that point on, any attempt to get a strong reference will fail, and we can guarantee that the weak refcount
// will only be decreased.

struct RefManager {
  void *obj;
  RefManager_Free freeCB;
  uint16_t strong_refcount;
  uint16_t weak_refcount;
  bool isInvalid;
};

// For tests, LLAPI and strong/weak references only. DO NOT USE DIRECTLY
inline void *__RefManager_Get_Object(RefManager *rm) {
  return rm ? rm->obj : NULL;
}

static RefManager *RefManager_New(void *obj, RefManager_Free freeCB) {
  RefManager *rm = rm_new(*rm);
  rm->obj = obj;
  rm->freeCB = freeCB;
  rm->strong_refcount = 1;
  rm->weak_refcount = 1;
  rm->isInvalid = false;
  return rm;
}

static void RefManager_ReturnStrongReference(RefManager *rm) {
  if (__atomic_sub_fetch(&rm->strong_refcount, 1, __ATOMIC_RELAXED) == 0) {
    rm->freeCB(rm->obj);
  }
}

static void RefManager_ReturnWeakReference(RefManager *rm) {
  if (__atomic_sub_fetch(&rm->weak_refcount, 1, __ATOMIC_RELAXED) == 0) {
    rm_free(rm);
  }
}

static void RefManager_ReturnReferences(RefManager *rm) {
  RefManager_ReturnStrongReference(rm);
  RefManager_ReturnWeakReference(rm);
}

static void RefManager_InvalidateSpec(RefManager *rm) {
  __atomic_store_n(&rm->isInvalid, 1, __ATOMIC_RELEASE);
}

static void RefManager_GetWeakReference(RefManager *rm) {
  __atomic_add_fetch(&rm->weak_refcount, 1, __ATOMIC_RELAXED);
}

// Returns false if the spec is being freed or marked as invalid,
// otherwise increases the strong refcount and returns true.
// Assumes the caller has a weak reference
static bool RefManager_TryGetStrongReference(RefManager *rm) {
  uint16_t cur_ref = -1;
  // Attempt to increase the strong refcount by 1 if it is not 0
  while (!__atomic_compare_exchange_n(&rm->strong_refcount, &cur_ref, cur_ref + 1, 0, 0, 0)) {
    if (cur_ref == 0) {
      // Refcount was 0, so the spec is being freed
      return false;
    }
  }
  // We have a valid strong reference. Check if the spec is invalid before returning it
  if (__atomic_load_n(&rm->isInvalid, __ATOMIC_ACQUIRE)) {
    RefManager_ReturnStrongReference(rm);
    return false;
  } else {
    return true;
  }
}

///////////////////////////////////////////////////////////////////////////////////////////////

StrongRef WeakRef_Promote(WeakRef w_ref) {
  StrongRef s_ref = {0};
  if (RefManager_TryGetStrongReference(w_ref.rm)) {
    RefManager_GetWeakReference(w_ref.rm); // we need to keep the weak reference alive
    s_ref.rm = w_ref.rm;
  }
  return s_ref;
}

WeakRef WeakRef_Clone(WeakRef ref) {
  WeakRef new_ref = {ref.rm};
  RefManager_GetWeakReference(ref.rm);
  return new_ref;
}

void WeakRef_Release(WeakRef w_ref) {
  RefManager_ReturnWeakReference(w_ref.rm);
}

WeakRef StrongRef_Demote(StrongRef s_ref) {
  WeakRef w_ref = {s_ref.rm};
  RefManager_GetWeakReference(s_ref.rm);
  return w_ref;
}

StrongRef StrongRef_Clone(StrongRef ref) {
  StrongRef new_ref = {ref.rm};
  RefManager_TryGetStrongReference(ref.rm); // will succeed since we already have a strong reference
  RefManager_GetWeakReference(ref.rm);
  return new_ref;
}

void StrongRef_Invalidate(StrongRef s_ref) {
  RefManager_InvalidateSpec(s_ref.rm);
}

void StrongRef_Release(StrongRef s_ref) {
  RefManager_ReturnReferences(s_ref.rm);
}

void *StrongRef_Get(StrongRef s_ref) {
  return __RefManager_Get_Object(s_ref.rm);
}

StrongRef StrongRef_New(void *obj, RefManager_Free freeCB) {
  StrongRef s_ref = {RefManager_New(obj, freeCB)};
  return s_ref;
}
