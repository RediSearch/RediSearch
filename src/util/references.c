/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "references.h"
#include "spec.h"

StrongRef WeakRef_Promote(WeakRef w_ref) {
  StrongRef s_ref = {0};
  if (REDISMODULE_OK == IndexSpecManager_TryGetStrongReference(w_ref.ism)) {
    IndexSpecManager_GetWeakReference(w_ref.ism); // we need to keep the weak reference alive
    s_ref.ism = w_ref.ism;
  }
  return s_ref;
}

WeakRef WeakRef_Clone(WeakRef ref) {
  WeakRef new_ref = {ref.ism};
  IndexSpecManager_GetWeakReference(ref.ism);
  return new_ref;
}

void WeakRef_Release(WeakRef w_ref) {
  IndexSpecManager_ReturnWeakReference(w_ref.ism);
}

WeakRef StrongRef_Demote(StrongRef s_ref) {
  WeakRef w_ref = {s_ref.ism};
  IndexSpecManager_GetWeakReference(s_ref.ism);
  return w_ref;
}

StrongRef StrongRef_Clone(StrongRef ref) {
  StrongRef new_ref = {ref.ism};
  IndexSpecManager_TryGetStrongReference(ref.ism); // will succeed since we already have a strong reference
  IndexSpecManager_GetWeakReference(ref.ism);
  return new_ref;
}

void StrongRef_Release(StrongRef s_ref) {
  IndexSpecManager_ReturnReferences(s_ref.ism);
}

IndexSpec *StrongRef_Get(StrongRef s_ref) {
  return __IndexSpecManager_Get_Spec(s_ref.ism);
}

StrongRef StrongRef_New(IndexSpec *sp) {
  StrongRef s_ref = {IndexSpecManager_New(sp)};
  return s_ref;
}
