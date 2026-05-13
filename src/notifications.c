/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#include "config.h"
#include "notifications.h"
#include "spec.h"
#include "doc_types.h"
#include "redismodule.h"
#include "rdb.h"
#include "module.h"
#include "util/workers.h"
#include "dictionary.h"
#include "slot_ranges.h"
#include "asm_state_machine.h"
#include "coord/rmr/redis_cluster.h"
#include "cursor.h"
#include "search_disk.h"
#include "doc_id_meta.h"
#include "iterators_rs.h"

#define JSON_LEN 5 // length of string "json."
RedisModuleString *global_RenameFromKey = NULL;
extern RedisModuleCtx *RSDummyContext;
RedisModuleString **hashFields = NULL;

// The list of events we handle in the notification callback.
#define REDIS_NOTIFICATION_EVENT_LIST(X)  \
  X(hset)                                 \
  X(hmset)                                \
  X(hsetnx)                               \
  X(hincrby)                              \
  X(hincrbyfloat)                         \
  X(hdel)                                 \
  X(del)                                  \
  X(set)                                  \
  X(rename_from)                          \
  X(rename_to)                            \
  X(trimmed)                              \
  X(key_trimmed)                          \
  X(restore)                              \
  X(hexpire)                              \
  X(hexpired)                             \
  X(expire)                               \
  X(expired)                              \
  X(persist)                              \
  X(hpersist)                             \
  X(evicted)                              \
  X(change)                               \
  X(loaded)                               \
  X(copy_to)

// Define an enum value for each event.
#define DECLARE_EVENT_ENUM(E) E##_cmd,
enum RedisCmd {
  _null_cmd = 0,
  REDIS_NOTIFICATION_EVENT_LIST(DECLARE_EVENT_ENUM)
};
#undef DECLARE_EVENT_ENUM

// Declare a static variable for each event to hold the cached pointer.
// This caches the event string pointer for future comparisons to avoid strcmp in hot paths.
#define DECLARE_REDIS_NOTIFICATION_EVENT_CACHE(E) static const char *E##_event = NULL;
REDIS_NOTIFICATION_EVENT_LIST(DECLARE_REDIS_NOTIFICATION_EVENT_CACHE)
#undef DECLARE_REDIS_NOTIFICATION_EVENT_CACHE

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

#define CHECK_CACHED_EVENT(E)     \
  else if (event == E##_event) {  \
    redisCommand = E##_cmd;       \
  }

#define CHECK_AND_CACHE_EVENT(E)  \
  else if (!strcmp(event, #E)) {  \
    redisCommand = E##_cmd;       \
    E##_event = event;            \
  }

  enum RedisCmd redisCommand;
  RedisModuleKey *kp;
  DocumentType kType;

  // Transform the event string into its corresponding enum value,
  // while caching the event string pointer for future comparisons to avoid strcmp in hot paths.
  // First "iterate" over the cached events, then fall back to strcmp and cache if found.

  if (false) {} // dummy first statement to allow the else-if chain
  REDIS_NOTIFICATION_EVENT_LIST(CHECK_CACHED_EVENT)
  REDIS_NOTIFICATION_EVENT_LIST(CHECK_AND_CACHE_EVENT)
  else redisCommand = _null_cmd;

  switch (redisCommand) {

/********************************************************
 *  GROUP A: Normal operation (same handling in RAM and SearchDisk)
 ********************************************************/
    case loaded_cmd:
      // on loaded event the key is stack allocated so to use it to load the
      // document we must copy it
      if (!IS_SST_RDB_IN_PROCESS(ctx)) {
        key = RedisModule_CreateStringFromString(ctx, key);
        Indexes_UpdateMatchingWithSchemaRules(ctx, key, getDocTypeFromString(key), hashFields); //TODO: avoid getDocTypeFromString ?
        RedisModule_FreeString(ctx, key);
      }
      break;

    case hset_cmd:
    case hmset_cmd:
    case hsetnx_cmd:
    case hincrby_cmd:
    case hincrbyfloat_cmd:
    case hdel_cmd:
      if (!IS_SST_RDB_IN_PROCESS(ctx)) {
        Indexes_UpdateMatchingWithSchemaRules(ctx, key, DocumentType_Hash, hashFields);
      }
      break;
    case hexpired_cmd:
      if (!SearchDisk_IsEnabled()) {
        Indexes_UpdateMatchingWithSchemaRules(ctx, key, DocumentType_Hash, hashFields);
      } else {
        static bool hexpired_warned = false;
        if (!hexpired_warned && Indexes_Count() > 0) {
          RedisModule_Log(ctx, "warning", "HEXPIRED event is not supported on Search when Flex is enabled. Ignoring HEXPIRED on Search");
          hexpired_warned = true;
        }
      }
      break;

    case expire_cmd:
    case persist_cmd:
    case restore_cmd:
    case copy_to_cmd:
      Indexes_UpdateMatchingWithSchemaRules(ctx, key, getDocTypeFromString(key), hashFields);
      break;

    case rename_from_cmd:
      // Notification rename_to is called right after rename_from so this is safe.
      global_RenameFromKey = key;
      break;

    case rename_to_cmd:
      Indexes_ReplaceMatchingWithSchemaRules(ctx, global_RenameFromKey, key);
      break;

/********************************************************
 *  GROUP B: Skip deletion for SearchDisk (Unlink handles it)
 ********************************************************/
    case del_cmd:
    case set_cmd:
      // Deletion handled by keyMetaOnUnlink callback
      if (!SearchDisk_IsEnabled()) {
        Indexes_DeleteMatchingWithSchemaRules(ctx, key, getDocTypeFromString(key), hashFields);
      }
      break;

/********************************************************
 *  GROUP C: Ignore in SearchDisk (field-TTL metadata only)
 ********************************************************/
    case hexpire_cmd:
    case hpersist_cmd:
      // We do not support field-TTL metadata changes in the disk flow.
      if (!SearchDisk_IsEnabled()) {
        Indexes_UpdateMatchingWithSchemaRules(ctx, key, getDocTypeFromString(key), hashFields);
      } else {
        static bool hpexpire_warned = false;
        if (!hpexpire_warned && Indexes_Count() > 0) {
          RedisModule_Log(ctx, "warning", "Field-level expiration is not supported on Search when Flex is enabled. Ignoring HPEXPIRE/HPERSIST on Search");
          hpexpire_warned = true;
        }
      }
      break;

/********************************************************
 *  GROUP D: Has deletion branch to skip for SearchDisk
 ********************************************************/
    case change_cmd:
      kp = RedisModule_OpenKey(ctx, key, REDISMODULE_READ);
      kType = DocumentType_Unsupported;
      if (kp) {
        kType = getDocType(kp);
        RedisModule_CloseKey(kp);
      }
      if (kType == DocumentType_Unsupported) {
        // In CRDT, empty key means key was deleted
        if (SearchDisk_IsEnabled()) {
          // Deletion handled by keyMetaOnUnlink callback
          break;
        }
        Indexes_DeleteMatchingWithSchemaRules(ctx, key, kType, hashFields);
      } else {
        // todo: here we will open the key again, we can optimize it by
        //       somehow passing the key pointer
        Indexes_UpdateMatchingWithSchemaRules(ctx, key, kType, hashFields);
      }
      break;

/********************************************************
 *  GROUP E: Never received with SearchDisk (not subscribed)
 ********************************************************/
    case trimmed_cmd:
    case key_trimmed_cmd:
    case expired_cmd:
    case evicted_cmd:
      RS_ASSERT(!SearchDisk_IsEnabled());
      Indexes_DeleteMatchingWithSchemaRules(ctx, key, getDocTypeFromString(key), hashFields);
      break;
  }


/********************************************************
 *              Handling RedisJSON commands             *
 ********************************************************/
  if (!strncmp(event, "json.", JSON_LEN)) {
    if (!strcmp(event + JSON_LEN, "set") ||
        !strcmp(event + JSON_LEN, "merge") ||
        !strcmp(event + JSON_LEN, "mset") ||
        !strcmp(event + JSON_LEN, "del") ||
        !strcmp(event + JSON_LEN, "numincrby") ||
        !strcmp(event + JSON_LEN, "nummultby") ||
        !strcmp(event + JSON_LEN, "strappend") ||
        !strcmp(event + JSON_LEN, "arrappend") ||
        !strcmp(event + JSON_LEN, "arrinsert") ||
        !strcmp(event + JSON_LEN, "arrpop") ||
        !strcmp(event + JSON_LEN, "arrtrim") ||
        !strcmp(event + JSON_LEN, "toggle")) {
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
  int fieldsNum = 0;

  RedisModuleKey *k = RedisModule_OpenKey(RSDummyContext, copyKeyStr, REDISMODULE_READ);
  if (!k || RedisModule_KeyType(k) != REDISMODULE_KEYTYPE_HASH) {
    // key does not exist or is not a hash, nothing to do
    goto done;
  }

  fieldsNum = (numArgs - 2) / cmdFactor;
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

// These events do not use ASM State Machine
void ShardingEvent(RedisModuleCtx *ctx, RedisModuleEvent eid, uint64_t subevent, void *data) {
  /**
   * On sharding event we need to do couple of things depends on the subevent given:
   *
   * 1. REDISMODULE_SUBEVENT_SHARDING_SLOT_RANGE_CHANGED
   *    On this event we know that the slot range changed and we might have data
   *    which no longer belongs to this shard, we must ignore it on searches
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
      workersThreadPool_OnEventStart();
      break;
    case REDISMODULE_SUBEVENT_SHARDING_TRIMMING_ENDED:
      RedisModule_Log(ctx, "notice", "%s", "Got trimming ended event, exit trimming phase.");
      isTrimming = false;
      // Since trimming is done in a part-time job while redis is running other commands, we notify
      // the thread pool to no longer receive new jobs (in RCE mode), and terminate the threads
      // ONCE ALL PENDING JOBS ARE DONE.
      workersThreadPool_OnEventEnd(false);
      break;
    default:
      RedisModule_Log(RSDummyContext, "warning", "Bad subevent given, ignored.");
  }
}

//We still do not rely on the TIMER_ID being 0 to check initialization state.
#define UNITIALIZED_TIMER_ID 0

struct TrimmingDelayCtx {
  bool checkTrimmingStateTimerIdScheduled;
  bool enableTrimmingTimerIdScheduled;
  RedisModuleTimerID checkTrimmingStateTimerId;
  RedisModuleTimerID enableTrimmingTimerId;
};

static struct TrimmingDelayCtx trimmingDelayCtx = {
  .checkTrimmingStateTimerIdScheduled = false,
  .enableTrimmingTimerIdScheduled = false,
  .checkTrimmingStateTimerId = UNITIALIZED_TIMER_ID,
  .enableTrimmingTimerId = UNITIALIZED_TIMER_ID
};

static void checkTrimmingStateCallback(RedisModuleCtx *ctx, void *privdata) {
  REDISMODULE_NOT_USED(privdata);
  // 1. Check counter of queries with old version
  // 2. If counter is 0, enable trimming and stop enableTrimmingTimer.
  // 3. Otherwise, reschedule the timer after TRIMMING_STATE_CHECK_DELAY.

  RedisModule_Log(ctx, "verbose", "Checking if we can start trimming migrated slots.");
  if (ASM_CanStartTrimming()) {
    RedisModule_Log(ctx, "notice", "No queries using the old version, Enabling trimming.");
    RS_ASSERT(trimmingDelayCtx.enableTrimmingTimerIdScheduled);
    RedisModule_StopTimer(ctx, trimmingDelayCtx.enableTrimmingTimerId, NULL);
    trimmingDelayCtx.enableTrimmingTimerId = UNITIALIZED_TIMER_ID;
    trimmingDelayCtx.enableTrimmingTimerIdScheduled = false;
    trimmingDelayCtx.checkTrimmingStateTimerId = UNITIALIZED_TIMER_ID;
    trimmingDelayCtx.checkTrimmingStateTimerIdScheduled = false;
    RedisModule_ClusterEnableTrim(ctx);
  } else {
    RedisModule_Log(ctx, "verbose", "Queries still using the old version, rescheduling check in %d milliseconds.", RSGlobalConfig.trimmingStateCheckDelayMS);
    trimmingDelayCtx.checkTrimmingStateTimerId = RedisModule_CreateTimer(ctx, RSGlobalConfig.trimmingStateCheckDelayMS, checkTrimmingStateCallback, NULL);
    trimmingDelayCtx.checkTrimmingStateTimerIdScheduled = true;
  }
}

static void enableTrimmingCallback(RedisModuleCtx *ctx, void *privdata) {
  REDISMODULE_NOT_USED(privdata);
  // Cancel the checkTrimmingStateCallback timer (Ignore error if it did not exist it does not matter)
  RedisModule_Log(ctx, "verbose", "Maximum delay reached. Enabling trimming.");
  if (!ASM_CanStartTrimming()) {
    RedisModule_Log(ctx, "warning", "Queries still using the old version, potential result inaccuracy.");
    CursorList_MarkASMInaccuracy();
  }
  RS_ASSERT(trimmingDelayCtx.checkTrimmingStateTimerIdScheduled);
  RedisModule_StopTimer(ctx, trimmingDelayCtx.checkTrimmingStateTimerId, NULL);
  trimmingDelayCtx.checkTrimmingStateTimerId = UNITIALIZED_TIMER_ID;
  trimmingDelayCtx.checkTrimmingStateTimerIdScheduled = false;
  trimmingDelayCtx.enableTrimmingTimerId = UNITIALIZED_TIMER_ID;
  trimmingDelayCtx.enableTrimmingTimerIdScheduled = false;
  RedisModule_ClusterEnableTrim(ctx);
}

void ClusterSlotMigrationEvent(RedisModuleCtx *ctx, RedisModuleEvent eid, uint64_t subevent, void *data) {
  REDISMODULE_NOT_USED(eid);
  RedisModuleClusterSlotMigrationInfo *info = (RedisModuleClusterSlotMigrationInfo *)data;
  RedisModuleSlotRangeArray *slots = info->slots;

  switch (subevent) {

    case REDISMODULE_SUBEVENT_CLUSTER_SLOT_MIGRATION_IMPORT_STARTED:
      RedisModule_Log(RSDummyContext, "notice", "Got ASM import started event.");
      ASM_StateMachine_StartImport(slots);
      workersThreadPool_OnEventStart();
      break;
    case REDISMODULE_SUBEVENT_CLUSTER_SLOT_MIGRATION_IMPORT_COMPLETED:
      ASM_StateMachine_CompleteImport(slots);
    case REDISMODULE_SUBEVENT_CLUSTER_SLOT_MIGRATION_IMPORT_FAILED:
      RedisModule_Log(RSDummyContext, "notice", "Got ASM import %s event.", subevent == REDISMODULE_SUBEVENT_CLUSTER_SLOT_MIGRATION_IMPORT_FAILED ? "failed" : "completed");
      // Since importing is done in a part-time job while redis is running other commands, we notify
      // the thread pool to no longer receive new jobs, and terminate the threads ONCE ALL PENDING JOBS ARE DONE.
      workersThreadPool_OnEventEnd(false);
      if (!IsEnterprise() && subevent == REDISMODULE_SUBEVENT_CLUSTER_SLOT_MIGRATION_IMPORT_COMPLETED) {
        RedisTopologyUpdater_StopAndRescheduleImmediately(ctx);
      }
      break;

    // case REDISMODULE_SUBEVENT_CLUSTER_SLOT_MIGRATION_MIGRATE_STARTED:
    // case REDISMODULE_SUBEVENT_CLUSTER_SLOT_MIGRATION_MIGRATE_FAILED:
    case REDISMODULE_SUBEVENT_CLUSTER_SLOT_MIGRATION_MIGRATE_COMPLETED:
      ASM_StateMachine_CompleteMigration(slots);
      // Start 2 timers. One for the minimal delay, and one for the maximal delay.
      RedisModule_Log(ctx, "notice", "Got ASM migrate completed event.");
      RS_ASSERT(trimmingDelayCtx.enableTrimmingTimerIdScheduled == trimmingDelayCtx.checkTrimmingStateTimerIdScheduled);
      if (trimmingDelayCtx.checkTrimmingStateTimerIdScheduled && trimmingDelayCtx.enableTrimmingTimerIdScheduled) {
        RedisModule_StopTimer(ctx, trimmingDelayCtx.checkTrimmingStateTimerId, NULL);
        trimmingDelayCtx.checkTrimmingStateTimerId = UNITIALIZED_TIMER_ID;
        trimmingDelayCtx.checkTrimmingStateTimerIdScheduled = false;
        RedisModule_StopTimer(ctx, trimmingDelayCtx.enableTrimmingTimerId, NULL);
        trimmingDelayCtx.enableTrimmingTimerId = UNITIALIZED_TIMER_ID;
        trimmingDelayCtx.enableTrimmingTimerIdScheduled = false;
        // This involves that a previous MIGRATION had already completed so we disable trimming, we need to enable trim to avoid a leak in
        // counter of Modules enabling trimming
        RedisModule_Log(ctx, "warning", "A migration completed while waiting to enable trimming from a previous migration");
        RedisModule_ClusterEnableTrim(ctx);
      }

      // Check if number of indices is 0. If so, we can start trimming immediately.
      if (Indexes_Count() == 0) {
        RedisModule_Log(ctx, "notice", "No indices found, allowing trimming immediately.");
        break;
      }

      RedisModule_ClusterDisableTrim(ctx);
      trimmingDelayCtx.checkTrimmingStateTimerId = RedisModule_CreateTimer(ctx, RSGlobalConfig.minTrimDelayMS, checkTrimmingStateCallback, NULL);
      trimmingDelayCtx.enableTrimmingTimerId = RedisModule_CreateTimer(ctx, RSGlobalConfig.maxTrimDelayMS, enableTrimmingCallback, NULL);
      trimmingDelayCtx.checkTrimmingStateTimerIdScheduled = true;
      trimmingDelayCtx.enableTrimmingTimerIdScheduled = true;
      if (!IsEnterprise()) {
        RedisTopologyUpdater_StopAndRescheduleImmediately(ctx);
      }
      break;

    case REDISMODULE_SUBEVENT_CLUSTER_SLOT_MIGRATION_MIGRATE_MODULE_PROPAGATE:
      RedisModule_Log(RSDummyContext, "notice", "Got ASM migrate module propagate event.");
      // We need to propagate all auxiliary data (schemas and dictionaries)
      // If a new type implement `aux_save` and `aux_load` (of any version) we MUST propagate it here too.

      RedisModule_Log(RSDummyContext, "notice", "Propagating %zu schemas.", Indexes_Count());
      Indexes_Propagate(ctx);
      RedisModule_Log(RSDummyContext, "notice", "Finished propagating schemas.");

      RedisModule_Log(RSDummyContext, "notice", "Propagating %zu dictionaries.", Dictionary_Size());
      Dictionary_Propagate(ctx);
      RedisModule_Log(RSDummyContext, "notice", "Finished propagating dictionaries.");
      break;
  }
}


void ClusterSlotMigrationTrimEvent(RedisModuleCtx *ctx, RedisModuleEvent eid, uint64_t subevent, void *data) {
  REDISMODULE_NOT_USED(ctx);
  REDISMODULE_NOT_USED(eid);
  RedisModuleClusterSlotMigrationTrimInfo *info = (RedisModuleClusterSlotMigrationTrimInfo *)data;
  RedisModuleSlotRangeArray *slots = info->slots;

  switch (subevent) {

    case REDISMODULE_SUBEVENT_CLUSTER_SLOT_MIGRATION_TRIM_STARTED:
      RedisModule_Log(RSDummyContext, "notice", "Got ASM trim started event.");
      workersThreadPool_OnEventStart();
      ASM_StateMachine_StartTrim(slots);
      break;
    case REDISMODULE_SUBEVENT_CLUSTER_SLOT_MIGRATION_TRIM_COMPLETED:
      RedisModule_Log(RSDummyContext, "notice", "Got ASM trim completed event.");
      // Since trimming is done in a part-time job while redis is running other commands, we notify
      // the thread pool to no longer receive new jobs, and terminate the threads ONCE ALL PENDING JOBS ARE DONE.
      workersThreadPool_OnEventEnd(false);
      ASM_StateMachine_CompleteTrim(slots);
      break;

    // case REDISMODULE_SUBEVENT_CLUSTER_SLOT_MIGRATION_TRIM_BACKGROUND:
  }
}

static void ServerReadyEvent(RedisModuleCtx *ctx, RedisModuleEvent eid, uint64_t subevent, void *data) {
  REDISMODULE_NOT_USED(eid);
  REDISMODULE_NOT_USED(subevent);
  REDISMODULE_NOT_USED(data);
  RedisModule_Log(ctx, "notice", "Got Server ready event.");
  if (SearchDisk_IsEnabled()) {
    bool disk_initialized = SearchDisk_Initialize(ctx);
    RS_LOG_ASSERT(disk_initialized, "Search Disk is enabled but could not be initialized")
    if (RSGlobalConfig.numWorkerThreads == 0) {
      RSGlobalConfig.numWorkerThreads = DEFAULT_WORKER_THREADS_FLEX;
      workersThreadPool_SetNumWorkers();
      RedisModule_Log(ctx, "notice", "WORKERS set to 1 (Flex mode default)");
    }
  }
}

void ShutdownEvent(RedisModuleCtx *ctx, RedisModuleEvent eid, uint64_t subevent, void *data) {
  RedisModule_Log(ctx, "notice", "%s", "Begin releasing RediSearch resources on shutdown");
  RediSearch_CleanupModule(ctx);
  RedisModule_Log(ctx, "notice", "%s", "End releasing RediSearch resources");
}

void ShutdownDiskClose(RedisModuleCtx *ctx, RedisModuleEvent eid, uint64_t subevent, void *data) {
  RedisModule_Log(ctx, "notice", "%s", "Begin releasing RediSearch DiskAPI resources on shutdown");
  SearchDisk_Close(ctx);
  RedisModule_Log(ctx, "notice", "%s", "End releasing RediSearch DiskAPI resources");
}

#define HIDE_USER_DATA_FROM_LOGS "hide-user-data-from-log"
#define BIGREDIS_MAX_RAM "bigredis-max-ram"

bool getHideUserDataFromLogs() {
  return getRedisConfigBool(RSDummyContext, HIDE_USER_DATA_FROM_LOGS, false);
}

void onUpdatedHideUserDataFromLogs(RedisModuleCtx *ctx) {
  RSGlobalConfig.hideUserDataFromLog = getHideUserDataFromLogs();
  SearchDisk_UpdateLogObfuscation();
  if (RSGlobalConfig.hideUserDataFromLog) {
    RedisModule_Log(ctx, "notice", "Hide user data from search logs is now enabled, "
                   "search entity names (such as indexes and fields) in the logs will now be obfuscated");
  } else {
    RedisModule_Log(ctx, "notice", "Hide user data from search logs is now disabled, "
                   "search entity names (such as indexes and fields) in the logs will now be visible");
  }
}

void ConfigChangedCallback(RedisModuleCtx *ctx, RedisModuleEvent eid, uint64_t event, void *data) {
  if (eid.id != REDISMODULE_EVENT_CONFIG ||
      event != REDISMODULE_SUBEVENT_CONFIG_CHANGE) {
    return;
  }
  RedisModuleConfigChangeV1 *ei = data;
  for (unsigned int i = 0; i < ei->num_changes; i++) {
    const char *conf = ei->config_names[i];
    if (!strcmp(conf, HIDE_USER_DATA_FROM_LOGS)) {
      onUpdatedHideUserDataFromLogs(ctx);
    }
    if (!strcmp(conf, BIGREDIS_MAX_RAM)) {
      RS_ASSERT(SearchDisk_IsInitialized());
      SearchDisk_UpdateBufferBudget(ctx, (int)RSGlobalConfig.diskBufferPercentage);
    }
  }
}

void Initialize_KeyspaceNotifications() {
  static bool RS_KeyspaceEvents_Initialized = false;
  if (!RS_KeyspaceEvents_Initialized) {
    int notifyFlags = 0;
    if (SearchDisk_IsEnabled()) {
      // On Disk we do not listen to notifications that lead to deleting the keys as the unlink callback of DocIDMeta will handle it.
      notifyFlags = REDISMODULE_NOTIFY_GENERIC | REDISMODULE_NOTIFY_HASH | REDISMODULE_NOTIFY_STRING |
      REDISMODULE_NOTIFY_LOADED | REDISMODULE_NOTIFY_MODULE;
    } else {
      notifyFlags = REDISMODULE_NOTIFY_GENERIC | REDISMODULE_NOTIFY_HASH |
      REDISMODULE_NOTIFY_TRIMMED | REDISMODULE_NOTIFY_KEY_TRIMMED | REDISMODULE_NOTIFY_STRING |
      REDISMODULE_NOTIFY_EXPIRED | REDISMODULE_NOTIFY_EVICTED |
      REDISMODULE_NOTIFY_LOADED | REDISMODULE_NOTIFY_MODULE;
    }
    RedisModule_SubscribeToKeyspaceEvents(RSDummyContext, notifyFlags, HashNotificationCallback);
    RS_KeyspaceEvents_Initialized = true;
  }
}

// Persistence event handler.
// Called on BGSAVE/AOF rewrite start and end.
static void PersistenceEvent(RedisModuleCtx *ctx, RedisModuleEvent eid,
                             uint64_t subevent, void *data) {
  REDISMODULE_NOT_USED(eid);
  REDISMODULE_NOT_USED(data);

  switch (subevent) {
  case REDISMODULE_SUBEVENT_PERSISTENCE_RDB_START:
  case REDISMODULE_SUBEVENT_PERSISTENCE_SYNC_RDB_START:
    RedisModule_Log(ctx, "notice", "Persistence started");
    DocIdMeta_SetPersistenceInProgress(true);
    break;
  case REDISMODULE_SUBEVENT_PERSISTENCE_ENDED:
  case REDISMODULE_SUBEVENT_PERSISTENCE_FAILED:
    RedisModule_Log(ctx, "notice", "Persistence ended");
    DocIdMeta_SetPersistenceInProgress(false);
    break;
  }
}

void Initialize_ServerEventNotifications(RedisModuleCtx *ctx) {
  // RedisModule_SubscribeToServerEvent should exist since redis 6.0
  // We can assume it is always present

  // we do not need to scan after rdb load, i.e, there is not danger of losing results
  // after resharding, its safe to filter keys which are not in our slot range.
  if (RedisModule_ShardingGetKeySlot) {
    // we have server events support, lets subscribe to relevant events.
    RedisModule_Log(ctx, "notice", "Subscribe to sharding events");
    RedisModule_SubscribeToServerEvent(ctx, RedisModuleEvent_Sharding, ShardingEvent);
  }
  bool shutdownEventHandled = false;
  if (getenv("RS_GLOBAL_DTORS")) {
    // clear resources when the server exits
    // used only with sanitizer or valgrind
    RedisModule_Log(ctx, "notice", "Subscribe to clear resources on shutdown");
    RedisModule_SubscribeToServerEvent(ctx, RedisModuleEvent_Shutdown, ShutdownEvent);
    shutdownEventHandled = true;
  }

  if (!shutdownEventHandled && SearchDisk_IsEnabled()) {
    RedisModule_SubscribeToServerEvent(ctx, RedisModuleEvent_Shutdown, ShutdownDiskClose);
  }

  RedisModule_Log(ctx, "notice", "Subscribe to config changes");
  RedisModule_SubscribeToServerEvent(ctx, RedisModuleEvent_Config, ConfigChangedCallback);

  RedisModule_Log(ctx, "notice", "Subscribe to cluster slot migration events");
  RedisModule_SubscribeToServerEvent(ctx, RedisModuleEvent_ClusterSlotMigration, ClusterSlotMigrationEvent);
  RedisModule_SubscribeToServerEvent(ctx, RedisModuleEvent_ClusterSlotMigrationTrim, ClusterSlotMigrationTrimEvent);
  if (SearchDisk_IsEnabled()) {
    RedisModule_Log(ctx, "notice", "Subscribe to Server ready event");
    RedisModule_SubscribeToServerEvent(ctx, RedisModuleEvent_ServerReady, ServerReadyEvent);

    RedisModule_Log(ctx, "notice", "Subscribe to persistence events");
    RedisModule_SubscribeToServerEvent(ctx, RedisModuleEvent_Persistence, PersistenceEvent);
  }
}

void Initialize_CommandFilter(RedisModuleCtx *ctx) {
  if (RSGlobalConfig.filterCommands) {
    RedisModule_RegisterCommandFilter(ctx, CommandFilterCallback, 0);
  }
}


void ReplicaBackupCallback(RedisModuleCtx *ctx, RedisModuleEvent eid, uint64_t subevent, void *data) {

  REDISMODULE_NOT_USED(eid);
  switch(subevent) {
  case REDISMODULE_SUBEVENT_REPL_BACKUP_CREATE:
    Backup_Globals();
    break;
  case REDISMODULE_SUBEVENT_REPL_BACKUP_RESTORE:
    Restore_Globals(ctx);
    break;
  case REDISMODULE_SUBEVENT_REPL_BACKUP_DISCARD:
    Discard_Globals_Backup(ctx);
    break;
  }
}

void ReplicaAsyncLoad(RedisModuleCtx *ctx, RedisModuleEvent eid, uint64_t subevent, void *data) {
	REDISMODULE_NOT_USED(eid);
	// Todo: implement callbacks to support async read requests during diskless rdb replication
	//  in "swapdb" mode.
}


int CheckVersionForShortRead() {
  // Minimal versions: 6.2.5
  // (6.0.15 is not supporting the required event notification for modules)
  if (redisVersion.majorVersion >= 7 || (redisVersion.majorVersion == 6 && redisVersion.minorVersion >= 2)) {
	  return REDISMODULE_OK;
  }
  if (redisVersion.majorVersion == 6 && redisVersion.minorVersion == 2) {
      return redisVersion.patchVersion >= 5 ? REDISMODULE_OK : REDISMODULE_ERR;
  } else if (redisVersion.majorVersion == 255 &&
           redisVersion.minorVersion == 255 &&
           redisVersion.patchVersion == 255) {
    // Also supported on master (version=255.255.255)
    return REDISMODULE_OK;
  }
  return REDISMODULE_ERR;
}

void Initialize_RdbNotifications(RedisModuleCtx *ctx) {
  if (CheckVersionForShortRead() == REDISMODULE_OK) {
    int success = RedisModule_SubscribeToServerEvent(ctx, RedisModuleEvent_ReplBackup, ReplicaBackupCallback);
    RS_ASSERT_ALWAYS(success != REDISMODULE_ERR); // should be supported in this redis version/release
    int optionsFlags = SearchDisk_IsEnabled() ? REDISMODULE_OPTIONS_HANDLE_IO_ERRORS | REDISMODULE_OPTIONS_REQUIRE_LOADED_KEYS_IN_RAM : REDISMODULE_OPTIONS_HANDLE_IO_ERRORS;
    RedisModule_SetModuleOptions(ctx, optionsFlags);
    if (redisVersion.majorVersion < 7 || IsEnterprise()) {
      RedisModule_Log(ctx, "notice", "Enabled diskless replication");
      // TODO: in OSS, in redis >= 7, we must set REDISMODULE_OPTIONS_HANDLE_REPL_ASYNC_LOAD as well to allow
      //  diskless replication, as diskless replication occurs only in 'swapdb' mode.
    }
  }
}

void RoleChangeCallback(RedisModuleCtx *ctx, RedisModuleEvent eid, uint64_t subevent, void *data) {
  REDISMODULE_NOT_USED(eid);
  switch(subevent) {
  case REDISMODULE_EVENT_REPLROLECHANGED_NOW_MASTER:
    Indexes_SetTempSpecsTimers(TimerOp_Add);
    break;
  case REDISMODULE_EVENT_REPLROLECHANGED_NOW_REPLICA:
    Indexes_SetTempSpecsTimers(TimerOp_Del);
    break;
  }
}

void Initialize_RoleChangeNotifications(RedisModuleCtx *ctx) {
  int success = RedisModule_SubscribeToServerEvent(ctx, RedisModuleEvent_ReplicationRoleChanged, RoleChangeCallback);
  RS_ASSERT(success != REDISMODULE_ERR); // should be supported in this redis version/release
  RedisModule_Log(ctx, "notice", "Enabled role change notification");
}

// This function is called in case the server is started or
// when the replica is loading the RDB file from the master.
void RDB_LoadingEvent(RedisModuleCtx *ctx, RedisModuleEvent eid, uint64_t subevent, void *data) {
  switch (subevent) {
  case REDISMODULE_SUBEVENT_LOADING_RDB_START:
  case REDISMODULE_SUBEVENT_LOADING_AOF_START:
  case REDISMODULE_SUBEVENT_LOADING_REPL_START:
    Indexes_StartRDBLoadingEvent(ctx);
    workersThreadPool_OnEventStart();
    RedisModule_Log(RSDummyContext, "notice", "Loading event started");
    break;
  case REDISMODULE_SUBEVENT_LOADING_ENDED:
    Indexes_EndRDBLoadingEvent(ctx);
    workersThreadPool_OnEventEnd(true);
    Indexes_EndLoading();
    RedisModule_Log(RSDummyContext, "notice", "Loading event ended successfully");
    break;
  case REDISMODULE_SUBEVENT_LOADING_FAILED:
    workersThreadPool_OnEventEnd(true);
    Indexes_EndLoading();
    RedisModule_Log(RSDummyContext, "notice", "Loading event failed");
    break;
  default:
    RS_LOG_ASSERT_FMT(0, "Unknown sub-event %d", subevent);
    break;
  }
}

void LoadingProgressCallback(RedisModuleCtx *ctx, RedisModuleEvent eid, uint64_t subevent, void *data) {
  RedisModule_Log(RSDummyContext, "debug", "Waiting for background jobs to be executed while loading is in progress (progress is %d)",
  ((RedisModuleLoadingProgress *)data)->progress);
  // Here draining is safe because no read queries are expected to run while loading is in progress.
  workersThreadPool_Drain(ctx, 100);
}
