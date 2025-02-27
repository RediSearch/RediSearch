/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "references.h"
#include "rmalloc.h"
#include <stdbool.h>

extern RedisModuleCtx *RSDummyContext;

// This is a weak reference to an object. It is used to prevent using an object that is being freed.
// The object is freed when the strong refcount is 0.

// Promises:
// 1. If the strong refcount gets to 0, it will never be increased again

// By using these functions through the strong and weak refcount API, we can guarantee that
// the object will be freed before the weak refcount reaches 0.

enum CountIncrement {
   Weak = 1u,
   Strong = (1u << 31)
};

typedef struct {
  uint32_t weak;
  uint32_t strong;
} RefCount;

typedef struct {
  union {
    RefCount count;
    // need to store in a 64 bit integer to ensure proper atomicity
    uint64_t raw;
  } ref;
  bool isInvalid;
} ControlBlock;

struct RefManager {
  void *obj;
  RefManager_Free freeCB;
  ControlBlock block;
};

// For tests, LLAPI and strong/weak references only. DO NOT USE DIRECTLY
inline void *__RefManager_Get_Object(RefManager *rm) {
  return rm ? rm->obj : NULL;
}

static RefManager *RefManager_New(void *obj, RefManager_Free freeCB) {
  RefManager *rm = rm_new(*rm);
  rm->obj = obj;
  rm->freeCB = freeCB;
  rm->block.ref.count.weak = 1;
  rm->block.ref.count.strong = 1;
  rm->block.isInvalid = false;
  RedisModule_Log(RSDummyContext, REDISMODULE_LOGLEVEL_VERBOSE, "RefManager created: %p", rm);
  return rm;
}

// Strong reference also takes a weak reference to the RefManager
// so it won't be freed before the object it manages.
static void RefManager_ReturnStrongReference(RefManager *rm) {
  ControlBlock block = {0};
  block.ref.raw = __atomic_sub_fetch(&rm->block.ref.raw, Weak | Strong, __ATOMIC_SEQ_CST);
  if (block.ref.count.strong == 0) {
    rm->freeCB(rm->obj);
    RedisModule_Log(RSDummyContext, REDISMODULE_LOGLEVEL_DEBUG, "RefManager's object freed: %p", rm);
  }
  if (block.ref.count.weak == 0) {
    rm_free(rm);
    RedisModule_Log(RSDummyContext, REDISMODULE_LOGLEVEL_DEBUG, "RefManager freed: %p", rm);
  }
}

static void RefManager_ReturnWeakReference(RefManager *rm) {
  ControlBlock block = {0};
  block.ref.raw = __atomic_sub_fetch(&rm->block.ref.raw, Weak, __ATOMIC_SEQ_CST);
  if (block.ref.count.weak == 0) {
    rm_free(rm);
    RedisModule_Log(RSDummyContext, REDISMODULE_LOGLEVEL_DEBUG, "RefManager freed: %p", rm);
  }
}

static void RefManager_ReturnReferences(RefManager *rm) {
  RefManager_ReturnStrongReference(rm);
}

static void RefManager_InvalidateObject(RefManager *rm) {
  __atomic_store_n(&rm->block.isInvalid, 1, __ATOMIC_RELAXED);
}

static void RefManager_GetWeakReference(RefManager *rm) {
  __atomic_add_fetch(&rm->block.ref.raw, Weak, __ATOMIC_RELAXED);
}

// Returns false if the object is being freed or marked as invalid,
// otherwise increases the strong refcount and returns true.
static bool RefManager_TryGetStrongReference(RefManager *rm) {
  // Attempt to increase the strong refcount and weak refcount by 1 only if it's not 0
  uint64_t expected = __atomic_load_n(&rm->block.ref.raw, __ATOMIC_RELAXED);
  uint64_t newValue = 0;
  do {
    ControlBlock newBlock = {0};
    newBlock.ref.raw = expected;
    if (newBlock.ref.count.strong == 0) {
      // Refcount was 0, so the object is being freed
      return false;
    }
    newBlock.ref.count.strong++;
    newBlock.ref.count.weak++;
    newValue = newBlock.ref.raw;
  } while (!__atomic_compare_exchange_n(&rm->block.ref.raw, &expected, newValue, false, 0, 0));

  // We have a valid strong reference. Check if the object is invalid before returning it
  if (__atomic_load_n(&rm->block.isInvalid, __ATOMIC_RELAXED)) {
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
