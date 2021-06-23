#include "config.h"
#include "notifications.h"
#include "spec.h"
#include "doc_types.h"

#define JSON_LEN 5 // length of string "json."

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
  expired_cmd,
  evicted_cmd,
  change_cmd,
  loaded_cmd,
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

  static const char *hset_event = 0, *hmset_event = 0, *hsetnx_event = 0, *hincrby_event = 0,
                    *hincrbyfloat_event = 0, *hdel_event = 0, *del_event = 0, *set_event = 0,
                    *rename_from_event = 0, *rename_to_event = 0, *trimmed_event = 0,
                    *restore_event = 0, *expired_event = 0, *evicted_event = 0, *change_event = 0,
                    *loaded_event = 0;

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
  else CHECK_CACHED_EVENT(expired)
  else CHECK_CACHED_EVENT(evicted)
  else CHECK_CACHED_EVENT(change)
  else CHECK_CACHED_EVENT(del)
  else CHECK_CACHED_EVENT(set)
  else CHECK_CACHED_EVENT(rename_from)
  else CHECK_CACHED_EVENT(rename_to)
  else CHECK_CACHED_EVENT(loaded)

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
    else CHECK_AND_CACHE_EVENT(expired)
    else CHECK_AND_CACHE_EVENT(evicted)
    else CHECK_AND_CACHE_EVENT(change)
    else CHECK_AND_CACHE_EVENT(del)
    else CHECK_AND_CACHE_EVENT(set)
    else CHECK_AND_CACHE_EVENT(rename_from)
    else CHECK_AND_CACHE_EVENT(rename_to)
    else CHECK_AND_CACHE_EVENT(loaded)
  }

  switch (redisCommand) {
    case loaded_cmd:
      // on loaded event the key is stack allocated so to use it to load the
      // document we must copy it
      key = RedisModule_CreateStringFromString(ctx, key);
      // notice, not break is ok here, we want to continue.
    case hset_cmd:
    case hmset_cmd:
    case hsetnx_cmd:
    case hincrby_cmd:
    case hincrbyfloat_cmd:
    case hdel_cmd:
      Indexes_UpdateMatchingWithSchemaRules(ctx, key, DocumentType_Hash, hashFields);
      if(redisCommand == loaded_cmd){
        RedisModule_FreeString(ctx, key);
      }
      break;

/********************************************************
 *              Handling Redis commands                 * 
 ********************************************************/
    case restore_cmd:
      Indexes_UpdateMatchingWithSchemaRules(ctx, key, getDocTypeFromString(key), hashFields);
      break;

    case del_cmd:
    case set_cmd:
    case trimmed_cmd:
    case expired_cmd:
    case evicted_cmd:
      Indexes_DeleteMatchingWithSchemaRules(ctx, key, hashFields);
      break;

    case change_cmd:
    // TODO: hash/json
      kp = RedisModule_OpenKey(ctx, key, REDISMODULE_READ);
      if (!kp || RedisModule_KeyType(kp) != REDISMODULE_KEYTYPE_HASH) {
        // in crdt empty key means that key was deleted
        // TODO:FIX
        Indexes_DeleteMatchingWithSchemaRules(ctx, key, hashFields);
      } else {
        // todo: here we will open the key again, we can optimize it by
        //       somehow passing the key pointer
        Indexes_UpdateMatchingWithSchemaRules(ctx, key, getDocType(kp), hashFields);
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


/********************************************************
 *              Handling RedisJSON commands             * 
 ********************************************************/
  if (!strncmp(event, "json.", strlen("json."))) {
    if (!strncmp(event + JSON_LEN, "set", strlen("set")) ||
        !strncmp(event + JSON_LEN, "del", strlen("del")) ||
        !strncmp(event + JSON_LEN, "numincrby", strlen("incrby")) ||
        !strncmp(event + JSON_LEN, "nummultby", strlen("nummultby")) ||
        !strncmp(event + JSON_LEN, "strappend", strlen("strappend")) ||
        !strncmp(event + JSON_LEN, "arrappend", strlen("arrappend")) ||
        !strncmp(event + JSON_LEN, "arrinsert", strlen("arrinsert")) ||
        !strncmp(event + JSON_LEN, "arrpop", strlen("arrpop")) ||
        !strncmp(event + JSON_LEN, "arrtrim", strlen("arrtrim")) ||
        !strncmp(event + JSON_LEN, "toggle", strlen("toggle"))) {
      // update index
      Indexes_UpdateMatchingWithSchemaRules(ctx, key, DocumentType_Json, hashFields);
    }
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

void ShardingEvent(RedisModuleCtx *ctx, RedisModuleEvent eid, uint64_t subevent, void *data) {
  /**
   * On sharding event we need to do couple of things depends on the subevent given:
   *
   * 1. REDISMODULE_SUBEVENT_SHARDING_SLOT_RANGE_CHANGED
   *    On this event we know that the slot range changed and we might have data
   *    which are no longer belong to this shard, we must ignore it on searches
   *
   * 2. REDISMODULE_SUBEVENT_SHARDING_TRIMMING_STARTED
   *    This event tells us that the trimming process has started and keys will start to be
   *    deleted, we do not need to do anything on this event
   *
   * 3. REDISMODULE_SUBEVENT_SHARDING_TRIMMING_ENDED
   *    This event tells us that the trimming process has finished, we are not longer
   *    have data that are not belong to us and its safe to stop checking this on searches.
   */
  if (eid.id != REDISMODULE_EVENT_SHARDING) {
    RedisModule_Log(RSDummyContext, "warning", "Bad event given, ignored.");
    return;
  }

  switch (subevent) {
    case REDISMODULE_SUBEVENT_SHARDING_SLOT_RANGE_CHANGED:
      RedisModule_Log(ctx, "notice", "%s", "Got slot range change event, enter trimming phase.");
      isTrimming = true;
      break;
    case REDISMODULE_SUBEVENT_SHARDING_TRIMMING_STARTED:
      RedisModule_Log(ctx, "notice", "%s", "Got trimming started event, enter trimming phase.");
      isTrimming = true;
      break;
    case REDISMODULE_SUBEVENT_SHARDING_TRIMMING_ENDED:
      RedisModule_Log(ctx, "notice", "%s", "Got trimming ended event, exit trimming phase.");
      isTrimming = false;
      break;
    default:
      RedisModule_Log(RSDummyContext, "warning", "Bad subevent given, ignored.");
  }
}

void Initialize_KeyspaceNotifications(RedisModuleCtx *ctx) {
  RedisModule_SubscribeToKeyspaceEvents(ctx,
    REDISMODULE_NOTIFY_GENERIC | REDISMODULE_NOTIFY_HASH |
    REDISMODULE_NOTIFY_TRIMMED | REDISMODULE_NOTIFY_STRING |
    REDISMODULE_NOTIFY_EXPIRED | REDISMODULE_NOTIFY_EVICTED |
    REDISMODULE_NOTIFY_LOADED | REDISMODULE_NOTIFY_MODULE,
    HashNotificationCallback);

  if(CompareVestions(redisVersion, noScanVersion) >= 0){
    // we do not need to scan after rdb load, i.e, there is not danger of losing results
    // after resharding, its safe to filter keys which are not in our slot range.
    if (RedisModule_SubscribeToServerEvent && RedisModule_ShardingGetKeySlot) {
      // we have server events support, lets subscribe to relevan events.
      RedisModule_Log(ctx, "notice", "%s", "Subscribe to sharding events");
      RedisModule_SubscribeToServerEvent(ctx, RedisModuleEvent_Sharding, ShardingEvent);
    }
  }
}

void Initialize_CommandFilter(RedisModuleCtx *ctx) {
  if (RSGlobalConfig.filterCommands) {
    RedisModule_RegisterCommandFilter(ctx, CommandFilterCallback, 0);
  }
}
