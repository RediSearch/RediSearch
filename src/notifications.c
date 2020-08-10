
#include "notifications.h"
#include "spec.h"

RedisModuleString *global_RenameFromKey = NULL;
extern RedisModuleCtx *RSDummyContext;
RedisModuleString **hashFields = NULL;

typedef enum {
  hset_cmd,
  hmset_cmd,  
  hsetnx_cmd,
  hincrby_cmd,  
  hincrbyfloat_cmd,  
  hdel_cmd,
  del_cmd,  
  set_cmd,
  rename_from_cmd,  
  rename_to_cmd,
  trimmed_cmd,  
  restore_cmd,
  expire_cmd,  
  change_cmd,
} RedisCmd;

static void freeHashFields() {
  if (hashFields != NULL) {
    for (size_t i = 0; hashFields[i] != NULL; ++i) {
      RedisModule_FreeString(RSDummyContext, hashFields[i]);
    }
    rm_free(hashFields);
    hashFields = NULL;
  }
}

int HashNotificationCallback(RedisModuleCtx *ctx, int type, const char *event,
                             RedisModuleString *key) {

#define CHECK_CACHED_EVENT(E) \
  if (event == E##_event) {   \
    redisCommand = E##_cmd;   \
  }

#define CHECK_AND_CACHE_EVENT(E) \
  if (!strcmp(event, #E)) {      \
    redisCommand = E##_cmd;      \
    E##_event = event;           \
  }

  int redisCommand = 0;
  RedisModuleKey *kp;

  static const char *hset_event = 0, *hmset_event = 0, *hsetnx_event = 0,
                    *hincrby_event = 0, *hincrbyfloat_event = 0, *hdel_event = 0,
                    *del_event = 0, *set_event = 0,
                    *rename_from_event = 0, *rename_to_event = 0,
                    *trimmed_event = 0, *restore_event = 0, *expire_event = 0, *change_event = 0;

  // clang-format off

       CHECK_CACHED_EVENT(hset)
  else CHECK_CACHED_EVENT(hmset)
  else CHECK_CACHED_EVENT(hsetnx)
  else CHECK_CACHED_EVENT(hincrby)
  else CHECK_CACHED_EVENT(hincrbyfloat)
  else CHECK_CACHED_EVENT(hdel)
  else CHECK_CACHED_EVENT(del)
  else CHECK_CACHED_EVENT(set)
  else CHECK_CACHED_EVENT(rename_from)
  else CHECK_CACHED_EVENT(rename_to)
  else CHECK_CACHED_EVENT(trimmed)
  else CHECK_CACHED_EVENT(restore)
  else CHECK_CACHED_EVENT(expire)
  else CHECK_CACHED_EVENT(change)
  else CHECK_CACHED_EVENT(del)
  else CHECK_CACHED_EVENT(set)
  else CHECK_CACHED_EVENT(rename_from)
  else CHECK_CACHED_EVENT(rename_to)
  else {
         CHECK_AND_CACHE_EVENT(hset)
    else CHECK_AND_CACHE_EVENT(hmset)
    else CHECK_AND_CACHE_EVENT(hsetnx)
    else CHECK_AND_CACHE_EVENT(hincrby)
    else CHECK_AND_CACHE_EVENT(hincrbyfloat)
    else CHECK_AND_CACHE_EVENT(hdel)
    else CHECK_AND_CACHE_EVENT(del)
    else CHECK_AND_CACHE_EVENT(set)
    else CHECK_AND_CACHE_EVENT(rename_from)
    else CHECK_AND_CACHE_EVENT(rename_to)
    else CHECK_AND_CACHE_EVENT(trimmed)
    else CHECK_AND_CACHE_EVENT(restore)
    else CHECK_AND_CACHE_EVENT(expire)
    else CHECK_AND_CACHE_EVENT(change)
    else CHECK_AND_CACHE_EVENT(del)
    else CHECK_AND_CACHE_EVENT(set)
    else CHECK_AND_CACHE_EVENT(rename_from)
    else CHECK_AND_CACHE_EVENT(rename_to)
  }

  switch (redisCommand) {
    case hset_cmd:
    case hmset_cmd:
    case hsetnx_cmd:
    case hincrby_cmd:
    case hincrbyfloat_cmd:
    case hdel_cmd:
    case restore_cmd:
      Indexes_UpdateMatchingWithSchemaRules(ctx, key, hashFields);
      break;

    case del_cmd:
    case set_cmd:
    case trimmed_cmd:
    case expire_cmd:
      Indexes_DeleteMatchingWithSchemaRules(ctx, key, hashFields);
      break;

    case change_cmd:
      kp = RedisModule_OpenKey(ctx, key, REDISMODULE_READ);
      if (!kp || RedisModule_KeyType(kp) == REDISMODULE_KEYTYPE_EMPTY) {
        // in crdt empty key means that key was deleted
        Indexes_DeleteMatchingWithSchemaRules(ctx, key, hashFields);
      } else {
        Indexes_UpdateMatchingWithSchemaRules(ctx, key, hashFields);
      }
      RedisModule_CloseKey(kp);
      break;

    case rename_from_cmd:
      // Notification rename_to is called right after rename_from so this is safe.  
      global_RenameFromKey = key;
      break;

    case rename_to_cmd:
      Indexes_ReplaceMatchingWithSchemaRules(ctx, global_RenameFromKey, key);
      break;
  }

  freeHashFields();

  return REDISMODULE_OK;
}

/*****************************************************************************/

void CommandFilterCallback(RedisModuleCommandFilterCtx *filter) {
  size_t len;
  const RedisModuleString *cmd = RedisModule_CommandFilterArgGet(filter, 0);
  const char *cmdStr = RedisModule_StringPtrLen(cmd, &len);
  if (*cmdStr != 'H' && *cmdStr != 'h') {
    return;
  }

  int numArgs = RedisModule_CommandFilterArgsCount(filter);
  if (numArgs < 3) {
    return;
  }
  int cmdFactor = 1;

  // HSETNX does not fire keyspace event if hash exists. No need to keep fields
  if (!strcasecmp("HSET", cmdStr) || !strcasecmp("HMSET", cmdStr) || !strcasecmp("HSETNX", cmdStr) ||
      !strcasecmp("HINCRBY", cmdStr) || !strcasecmp("HINCRBYFLOAT", cmdStr)) {
    if (numArgs % 2 != 0) return;
    // HSET receives field&value, HDEL receives field
    cmdFactor = 2;
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
    REDISMODULE_NOTIFY_GENERIC | REDISMODULE_NOTIFY_HASH |
    REDISMODULE_NOTIFY_TRIMMED | REDISMODULE_NOTIFY_STRING,
    HashNotificationCallback);
}

void Initialize_CommandFilter(RedisModuleCtx *ctx) {
  RedisModule_RegisterCommandFilter(ctx, CommandFilterCallback, 0);
}