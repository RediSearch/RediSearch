
#include "notifications.h"
#include "spec.h"

RedisModuleString *global_RenameFromKey = NULL;
extern RedisModuleCtx *RSDummyContext;
RedisModuleString **hashFields = NULL;

static void freeHashFields() {
  if (hashFields != NULL) {
    for (size_t i = 0; hashFields[i] != NULL; ++i) {
      RedisModule_FreeString(RSDummyContext, hashFields[i]);
    }
    rm_free(hashFields);
    hashFields = NULL;
  }
}

int HashNotificationCallback(RedisModuleCtx *ctx, int type, const char *event, RedisModuleString *key) {

#define CHECK_CACHED_EVENT(E) \
  if (event == E##_event) { \
    E = true; \
  }

#define CHECK_AND_CACHE_EVENT(E) \
  if (!strcmp(event, #E)) { \
    E = true; \
    E##_event = event; \
  }

  static const char *hset_event = 0, *hmset_event = 0, *del_event = 0,
                    *hdel_event = 0, *set_event = 0,
                    *rename_from_event = 0, *rename_to_event = 0;
  bool hset = false, hmset = false, del = false, hdel = false, set = false,
       rename_from = false, rename_to = false;

       CHECK_CACHED_EVENT(hset)
  else CHECK_CACHED_EVENT(hmset)
  else CHECK_CACHED_EVENT(del)
  else CHECK_CACHED_EVENT(hdel)
  else CHECK_CACHED_EVENT(set)
  else CHECK_CACHED_EVENT(rename_from)
  else CHECK_CACHED_EVENT(rename_to)
  else {
         CHECK_AND_CACHE_EVENT(hset)
    else CHECK_AND_CACHE_EVENT(hmset)
    else CHECK_AND_CACHE_EVENT(del)
    else CHECK_AND_CACHE_EVENT(hdel)
    else CHECK_AND_CACHE_EVENT(set)
    else CHECK_AND_CACHE_EVENT(rename_from)
    else CHECK_AND_CACHE_EVENT(rename_to)
  }

  const char *key_cp = RedisModule_StringPtrLen(key, NULL);
  if (hset || hmset || hdel) {
    Indexes_UpdateMatchingWithSchemaRules(ctx, key, hashFields);
  }
  if (del || set) {
    Indexes_DeleteMatchingWithSchemaRules(ctx, key, hashFields);
  }
  if (rename_from) {
    // Notification rename_to is called right after rename_from so this is safe.  
    global_RenameFromKey = key;
  }
  if (rename_to) {
    Indexes_ReplaceMatchingWithSchemaRules(ctx, global_RenameFromKey, key);
  }

  freeHashFields();

  return REDISMODULE_OK;
}

/*****************************************************************************/

void CommandFilterCallback(RedisModuleCommandFilterCtx *filter) {
  int numArgs = RedisModule_CommandFilterArgsCount(filter);
  if (numArgs < 3) return;

  bool hset = false;

  size_t len;
  const RedisModuleString *cmd = RedisModule_CommandFilterArgGet(filter, 0);
  const char *cmdStr = RedisModule_StringPtrLen(cmd, &len);

  if (!strcasecmp("HSET", cmdStr) || !strcasecmp("HMSET", cmdStr)) {
    if (numArgs % 2 != 0) return;
    hset = true;
  } else if (!strcasecmp("HDEL", cmdStr)) {
    // Nothing to do
  } else {
    return;
  }
  freeHashFields();

  const RedisModuleString *keyStr = RedisModule_CommandFilterArgGet(filter, 1);
  RedisModuleString *copyKeyStr = RedisModule_CreateStringFromString(RSDummyContext, keyStr);

  RedisModuleKey *k = RedisModule_OpenKey(RSDummyContext, copyKeyStr, REDISMODULE_READ);
  if (!k || RedisModule_KeyType(k) != REDISMODULE_KEYTYPE_HASH) {
    // key does not exist or is not a hash, nothing to do
    goto done;
  }

  // HSET receives field&value, HDEL receives field
  int cmdFactor = hset ? 2 : 1;
  int fieldsNum = (numArgs - 2) / cmdFactor;
  hashFields = (RedisModuleString **)rm_calloc(fieldsNum + 1, sizeof(*hashFields));

  for (size_t i = 0; i < fieldsNum; ++i) {
    RedisModuleString *field = (RedisModuleString *)RedisModule_CommandFilterArgGet(filter, 2 + i * cmdFactor);
    RedisModule_RetainString(RSDummyContext, field);
    hashFields[i] = field;
  }

done:
  RedisModule_FreeString(RSDummyContext, copyKeyStr);
  RedisModule_CloseKey(k);
}

void Initialize_KeyspaceNotifications(RedisModuleCtx *ctx) {
  RedisModule_SubscribeToKeyspaceEvents(ctx,
    REDISMODULE_NOTIFY_GENERIC | REDISMODULE_NOTIFY_HASH | REDISMODULE_NOTIFY_STRING,
    HashNotificationCallback);
}

void Initialize_CommandFilter(RedisModuleCtx *ctx) {
  RedisModule_RegisterCommandFilter(ctx, CommandFilterCallback, 0);
}