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
// The object will be freed before the weak refcount reaches 0.

struct RefManager {
  void *obj;
  RefManager_Free freeCB;
  uint64_t weak_refcount;
  uint32_t strong_refcount;
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
  rm->weak_refcount = 1;
  rm->strong_refcount = 1;
  rm->isInvalid = false;
  return rm;
}

static void RefManager_ReturnStrongReference(RefManager *rm) {
  if (__atomic_sub_fetch(&rm->strong_refcount, 1, __ATOMIC_SEQ_CST) == 0) {
    rm->freeCB(rm->obj);
  }
}

static void RefManager_ReturnWeakReference(RefManager *rm) {
  if (__atomic_sub_fetch(&rm->weak_refcount, 1, __ATOMIC_SEQ_CST) == 0) {
    rm_free(rm);
  }
}

static void RefManager_ReturnReferences(RefManager *rm) {
  RefManager_ReturnStrongReference(rm);
  RefManager_ReturnWeakReference(rm);
}

static void RefManager_InvalidateObject(RefManager *rm) {
  __atomic_store_n(&rm->isInvalid, 1, __ATOMIC_RELAXED);
}

static void RefManager_GetWeakReference(RefManager *rm) {
  __atomic_add_fetch(&rm->weak_refcount, 1, __ATOMIC_RELAXED);
}

// Returns false if the object is being freed or marked as invalid,
// otherwise increases the strong refcount and returns true.
static bool RefManager_TryGetStrongReference(RefManager *rm) {
  // Attempt to increase the strong refcount by 1 only if it's not 0
  uint32_t cur_ref = __atomic_load_n(&rm->strong_refcount, __ATOMIC_RELAXED);
  // do nothing here
  cur_ref--;
  __atomic_compare_exchange_n(&rm->strong_refcount, &cur_ref, cur_ref + 1, false, 0, 0);
  do {
    if (cur_ref == 0) {
      // Refcount was 0, so the object is being freed
      return false;
    }
  } while (!__atomic_compare_exchange_n(&rm->strong_refcount, &cur_ref, cur_ref + 1, false, 0, 0));

  // We have a valid strong reference. Check if the object is invalid before returning it
  if (__atomic_load_n(&rm->isInvalid, __ATOMIC_RELAXED)) {
    RefManager_ReturnStrongReference(rm);
    return false;
  } else {
    return true;
  }
}

///////////////////////////////////////////////////////////////////////////////////////////////

static StrongRef _Ref_GetStrong(RefManager *rm) {
  StrongRef s_ref = {0};
  if (RefManager_TryGetStrongReference(rm)) {
    // a strong reference also holds a weak reference (reference to the RefManager),
    // so it won't be freed before the object it manages.
    RefManager_GetWeakReference(rm);
    s_ref.rm = rm;
  }
  return s_ref;
}

static WeakRef _Ref_GetWeak(RefManager *rm) {
  WeakRef w_ref = {rm};
  RefManager_GetWeakReference(rm);
  return w_ref;
}

// ------------------------------ Public API --------------------------------------------------

StrongRef WeakRef_Promote(WeakRef w_ref) {
  return _Ref_GetStrong(w_ref.rm);
}

WeakRef WeakRef_Clone(WeakRef ref) {
  return _Ref_GetWeak(ref.rm);
}

void WeakRef_Release(WeakRef w_ref) {
  RefManager_ReturnWeakReference(w_ref.rm);
}

WeakRef StrongRef_Demote(StrongRef s_ref) {
  return _Ref_GetWeak(s_ref.rm);
}

StrongRef StrongRef_Clone(StrongRef ref) {
  return _Ref_GetStrong(ref.rm);
}

void StrongRef_Invalidate(StrongRef s_ref) {
  RefManager_InvalidateObject(s_ref.rm);
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
