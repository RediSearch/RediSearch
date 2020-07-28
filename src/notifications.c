
#include "notifications.h"
#include "spec.h"

int HashNotificationCallback(RedisModuleCtx *ctx, int type, const char *event,
                             RedisModuleString *key) {

#define CHECK_CACHED_EVENT(E) \
  if (event == E##_event) {   \
    E = true;                 \
  }

#define CHECK_AND_CACHE_EVENT(E) \
  if (!strcmp(event, #E)) {      \
    E = true;                    \
    E##_event = event;           \
  }

  static const char *hset_event = 0, *hmset_event = 0, *del_event = 0, *hdel_event = 0,
                    *trimmed_event = 0, *restore_event = 0, expired_event = 0;
  bool hset = false, hmset = false, del = false, hdel = false, trimmed = false, restore = false,
       expired = false;

  // todo:
  // we need to also handle changed event comes from crdt, changed means
  // the hash changed, we need to try read it to know if it was deleted or not.

  CHECK_CACHED_EVENT(hset)
  else CHECK_CACHED_EVENT(hmset) else CHECK_CACHED_EVENT(del) else CHECK_CACHED_EVENT(hdel) else CHECK_CACHED_EVENT(
      trimmed) else CHECK_CACHED_EVENT(restore) else CHECK_CACHED_EVENT(expired) else {
    CHECK_AND_CACHE_EVENT(hset)
    else CHECK_AND_CACHE_EVENT(hmset) else CHECK_AND_CACHE_EVENT(del) else CHECK_AND_CACHE_EVENT(
        hdel) else CHECK_AND_CACHE_EVENT(trimmed) else CHECK_AND_CACHE_EVENT(restore) else CHECK_AND_CACHE_EVENT(expired)
  }

  const char *key_cp = RedisModule_StringPtrLen(key, NULL);
  if (hset || hmset || hdel || restore) {
    Indexes_UpdateMatchingWithSchemaRules(ctx, key);
  }
  if (del || trimmed || expired) {
    Indexes_DeleteMatchingWithSchemaRules(ctx, key);
  }

  return REDISMODULE_OK;
}

void Initialize_KeyspaceNotifications(RedisModuleCtx *ctx) {
  RedisModule_SubscribeToKeyspaceEvents(
      ctx, REDISMODULE_NOTIFY_GENERIC | REDISMODULE_NOTIFY_HASH | REDISMODULE_NOTIFY_TRIMMED,
      HashNotificationCallback);
}
