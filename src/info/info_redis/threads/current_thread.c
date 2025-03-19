#include "info/info_redis/threads/current_thread.h"
#include "spec.h"

// TLS key for a spec information
static pthread_key_t specInfoKey;

static void __attribute__((constructor)) initializeKeys() {
  int rc = pthread_key_create(&specInfoKey, NULL);
  assert(rc == 0);
}

static void __attribute__((destructor)) destroyKeys() {
  pthread_key_delete(specInfoKey);
}

SpecInfo *CurrentThread_TryGetSpecInfo() {
  SpecInfo *info = pthread_getspecific(specInfoKey);
  return info;
}

void CurrentThread_SetIndexSpec(StrongRef specRef) {
  SpecInfo *info = CurrentThread_TryGetSpecInfo();
  if (!info) {
    info = rm_calloc(1, sizeof(*info));
    int rc = pthread_setspecific(specInfoKey, info);
    if (rc) {
      rm_free(info);
      return;
    }
  }
  RS_ASSERT(specRef.rm != NULL);
  info->specRef = StrongRef_Demote(specRef);
  // we duplicate the name in case we won't be able to access the weak ref
  const IndexSpec *spec = StrongRef_Get(specRef);
  info->specName = rm_strdup(spec->name);
}

void CurrentThread_ClearIndexSpec() {
  SpecInfo *info = CurrentThread_TryGetSpecInfo();
  if (info) {
    RS_ASSERT(info->specRef.rm != NULL);
    WeakRef_Release(info->specRef);
    rm_free(info->specName);
    rm_free(info);
    pthread_setspecific(specInfoKey, NULL);
  }
}
