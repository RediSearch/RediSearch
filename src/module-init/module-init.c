
#ifndef RS_NO_RMAPI
#define REDISMODULE_MAIN
#endif

#include "redismodule.h"

#include "module.h"
#include "version.h"
#include "config.h"
#include "redisearch_api.h"
#include <assert.h>
#include <ctype.h>
#include "concurrent_ctx.h"
#include "cursor.h"
#include "extension.h"
#include "alias.h"
#include "notifications.h"
#include "aggregate/aggregate.h"
#include "ext/default.h"
#include "rwlock.h"
#include "json.h"
#include "VecSim/vec_sim.h"

#ifndef RS_NO_ONLOAD
int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  if (RedisModule_Init(ctx, REDISEARCH_MODULE_NAME, REDISEARCH_MODULE_VERSION,
                       REDISMODULE_APIVER_1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;
  return RediSearch_InitModuleInternal(ctx, argv, argc);
}
#endif

/**
 * Check if we can run under the current AOF configuration. Returns true
 * or false
 */
static int validateAofSettings(RedisModuleCtx *ctx) {
  int rc = 1;

  if (RedisModule_GetContextFlags == NULL) {
    RedisModule_Log(ctx, "warning",
                    "Could not determine if AOF is in use. AOF Rewrite will crash!");
    return 1;
  }

  if ((RedisModule_GetContextFlags(ctx) & REDISMODULE_CTX_FLAGS_AOF) == 0) {
    // AOF disabled. All is OK, and no further checks needed
    return rc;
  }

  // Can't execute commands on the loading context, so make a new one
  RedisModuleCallReply *reply =
      RedisModule_Call(RSDummyContext, "CONFIG", "cc", "GET", "aof-use-rdb-preamble");
  assert(reply);
  assert(RedisModule_CallReplyType(reply) == REDISMODULE_REPLY_ARRAY);
  assert(RedisModule_CallReplyLength(reply) == 2);
  const char *value =
      RedisModule_CallReplyStringPtr(RedisModule_CallReplyArrayElement(reply, 1), NULL);

  // I tried using strcasecmp, but it seems that the yes/no replies have a trailing
  // embedded newline in them
  if (tolower(*value) == 'n') {
    RedisModule_Log(RSDummyContext, "warning", "FATAL: aof-use-rdb-preamble required if AOF is used!");
    rc = 0;
  }
  RedisModule_FreeCallReply(reply);
  return rc;
}

static int initAsModule(RedisModuleCtx *ctx) {
  // Check that redis supports thread safe context. RC3 or below doesn't
  if (RedisModule_GetThreadSafeContext == NULL) {
    RedisModule_Log(ctx, "warning",
                    "***** FATAL: Incompatible version of redis 4.0 detected. *****\n"
                    "\t\t\t\tPlease use Redis 4.0.0 or later from https://redis.io/download\n"
                    "\t\t\t\tRedis will exit now!");
    return REDISMODULE_ERR;
  }

  if (RediSearch_ExportCapi(ctx) != REDISMODULE_OK) {
    RedisModule_Log(ctx, "warning", "Could not initialize low level api");
  } else {
    RedisModule_Log(ctx, "notice", "Low level api version %d initialized successfully",
                    REDISEARCH_CAPI_VERSION);
  }

  if (RedisModule_GetContextFlags == NULL && RSGlobalConfig.concurrentMode) {
    RedisModule_Log(ctx, "warning",
                    "GetContextFlags unsupported (need Redis >= 4.0.6). Commands executed in "
                    "MULTI or LUA will "
                    "malfunction unless 'safe' functions are used or SAFEMODE is enabled.");
  }

  if (!validateAofSettings(ctx)) {
    return REDISMODULE_ERR;
  }

  return REDISMODULE_OK;
}

static int initAsLibrary(RedisModuleCtx *ctx) {
  // Disable concurrent mode:
  RSGlobalConfig.concurrentMode = 0;
  RSGlobalConfig.minTermPrefix = 0;
  RSGlobalConfig.maxPrefixExpansions = LONG_MAX;
  return REDISMODULE_OK;
}

void RS_moduleInfoFunc(RedisModuleInfoCtx *ctx, int for_crash_report) {
  // module version
  RedisModule_InfoAddSection(ctx, "version");
  RedisModuleString *rs_version = RedisModule_CreateStringPrintf(
      NULL, "%d.%d.%d", redisVersion.majorVersion, redisVersion.minorVersion, redisVersion.patchVersion);
  RedisModule_InfoAddFieldString(ctx, "RedisSearch_version", rs_version);
  RedisModule_FreeString(NULL, rs_version);

  // numer of indexes
  RedisModule_InfoAddSection(ctx, "index");
  RedisModule_InfoAddFieldLongLong(ctx, "number_of_indexes", dictSize(specDict_g));

  // // info for each index
  // dictIterator *iter = dictGetIterator(specDict_g);
  // dictEntry *entry;
  // while ((entry = dictNext(iter))) {
  //   IndexSpec *spec = dictGetVal(entry);
  //   size_t failures = spec->stats.indexingFailures;
  //   if (failures > 0) {
  //     RedisModule_InfoAddSection(ctx, spec->name);
  //     RedisModule_InfoAddFieldLongLong(ctx, "number_of_failures", failures);
  //   }
  // }

  // fields statistics
  RedisModule_InfoAddSection(ctx, "fields_statistics");
  // 
  if (RSGlobalConfig.fieldsStats.numTextFields > 0)
    RedisModule_InfoAddFieldLongLong(ctx, "number_of_text_fields", RSGlobalConfig.fieldsStats.numTextFields);
  if (RSGlobalConfig.fieldsStats.numTextFieldsSortable > 0)
    RedisModule_InfoAddFieldLongLong(ctx, "number_of_sortable_text_fields", RSGlobalConfig.fieldsStats.numTextFieldsSortable);
  if (RSGlobalConfig.fieldsStats.numTextFieldsNoIndex > 0)
    RedisModule_InfoAddFieldLongLong(ctx, "number_of_text_fields_no_index", RSGlobalConfig.fieldsStats.numTextFieldsNoIndex);
  if (RSGlobalConfig.fieldsStats.numNumericFields > 0)
    RedisModule_InfoAddFieldLongLong(ctx, "number_of_numeric_fields", RSGlobalConfig.fieldsStats.numNumericFields);
  if (RSGlobalConfig.fieldsStats.numNumericFieldsSortable > 0)
    RedisModule_InfoAddFieldLongLong(ctx, "number_of_sortable_numeric_fields", RSGlobalConfig.fieldsStats.numNumericFieldsSortable);
  if (RSGlobalConfig.fieldsStats.numNumericFieldsNoIndex > 0)
    RedisModule_InfoAddFieldLongLong(ctx, "number_of_numeric_fields_no_index", RSGlobalConfig.fieldsStats.numNumericFieldsNoIndex);
  if (RSGlobalConfig.fieldsStats.numTagFields > 0)
    RedisModule_InfoAddFieldLongLong(ctx, "number_of_tag_fields", RSGlobalConfig.fieldsStats.numTagFields);
  if (RSGlobalConfig.fieldsStats.numTagFieldsCaseSensitive > 0)
    RedisModule_InfoAddFieldLongLong(ctx, "number_of_tag_fields_case_sensitive", RSGlobalConfig.fieldsStats.numTagFieldsCaseSensitive);
  if (RSGlobalConfig.fieldsStats.numTagFieldsSortable > 0)
    RedisModule_InfoAddFieldLongLong(ctx, "number_of_sortable_tag_fields", RSGlobalConfig.fieldsStats.numTagFieldsSortable);
  if (RSGlobalConfig.fieldsStats.numTagFieldsNoIndex > 0)
    RedisModule_InfoAddFieldLongLong(ctx, "number_of_tag_fields_no_index", RSGlobalConfig.fieldsStats.numTagFieldsNoIndex);
  if (RSGlobalConfig.fieldsStats.numGeoFields > 0)
    RedisModule_InfoAddFieldLongLong(ctx, "number_of_geo_fields", RSGlobalConfig.fieldsStats.numGeoFields);
  if (RSGlobalConfig.fieldsStats.numGeoFieldsSortable > 0)
    RedisModule_InfoAddFieldLongLong(ctx, "number_of_sortable_geo_fields", RSGlobalConfig.fieldsStats.numGeoFieldsSortable);
  if (RSGlobalConfig.fieldsStats.numGeoFieldsNoIndex > 0)
    RedisModule_InfoAddFieldLongLong(ctx, "number_of_geo_fields_no_index", RSGlobalConfig.fieldsStats.numGeoFieldsNoIndex);
  if (RSGlobalConfig.fieldsStats.numVectorFieldsFlat > 0)
    RedisModule_InfoAddFieldLongLong(ctx, "number_of_vector_fields_flat", RSGlobalConfig.fieldsStats.numVectorFieldsFlat);
  if (RSGlobalConfig.fieldsStats.numVectorFieldsHSNW > 0)
    RedisModule_InfoAddFieldLongLong(ctx, "number_of_vector_fields_hsnw", RSGlobalConfig.fieldsStats.numVectorFieldsHSNW);

  // load time configuration
  RedisModule_InfoAddSection(ctx, "run_time_configs");
  RedisModule_InfoAddFieldCString(ctx, "concurrent_mode", (char*)RSGlobalConfig.concurrentMode ? "ON" : "OFF");
  if (RSGlobalConfig.extLoad != NULL) {
    RedisModule_InfoAddFieldCString(ctx, "extension_load", (char*)RSGlobalConfig.extLoad);
  }
  if (RSGlobalConfig.frisoIni != NULL) {
    RedisModule_InfoAddFieldCString(ctx, "friso_ini", (char*)RSGlobalConfig.frisoIni);
  }
  RedisModule_InfoAddFieldCString(ctx, "enableGC", (char*)RSGlobalConfig.enableGC ? "ON" : "OFF");
  RedisModule_InfoAddFieldLongLong(ctx, "minimal_term_prefix", RSGlobalConfig.minTermPrefix);
  RedisModule_InfoAddFieldLongLong(ctx, "maximal_prefix_expansions", RSGlobalConfig.maxPrefixExpansions);
  RedisModule_InfoAddFieldLongLong(ctx, "query_timeout_ms", RSGlobalConfig.queryTimeoutMS);
  RedisModule_InfoAddFieldCString(ctx, "timeout_policy", (char*)TimeoutPolicy_ToString(RSGlobalConfig.timeoutPolicy));
  RedisModule_InfoAddFieldLongLong(ctx, "cursor_read_size", RSGlobalConfig.cursorReadSize);
  RedisModule_InfoAddFieldLongLong(ctx, "cursor_max_idle_time", RSGlobalConfig.cursorMaxIdle);

  RedisModule_InfoAddFieldLongLong(ctx, "max_doc_table_size", RSGlobalConfig.maxDocTableSize);
  RedisModule_InfoAddFieldLongLong(ctx, "max_search_results", RSGlobalConfig.maxSearchResults);
  RedisModule_InfoAddFieldLongLong(ctx, "max_aggregate_results", RSGlobalConfig.maxAggregateResults);
  RedisModule_InfoAddFieldLongLong(ctx, "search_pool_size", RSGlobalConfig.searchPoolSize);
  RedisModule_InfoAddFieldLongLong(ctx, "index_pool_size", RSGlobalConfig.indexPoolSize);
  RedisModule_InfoAddFieldLongLong(ctx, "gc_scan_size", RSGlobalConfig.gcScanSize);
  RedisModule_InfoAddFieldLongLong(ctx, "min_phonetic_term_length", RSGlobalConfig.minPhoneticTermLen);
}

static inline const char* RS_GetExtraVersion() {
#ifdef GIT_VERSPEC
  return GIT_VERSPEC;
#else
  return "";
#endif
}

int RS_Initialized = 0;
RedisModuleCtx *RSDummyContext = NULL;

int RediSearch_Init(RedisModuleCtx *ctx, int mode) {
#define DO_LOG(...)                                 \
  do {                                              \
    if (ctx && (mode != REDISEARCH_INIT_LIBRARY)) { \
      RedisModule_Log(ctx, ##__VA_ARGS__);          \
    }                                               \
  } while (false)

  if (RediSearch_LockInit(ctx) != REDISMODULE_OK) {
    return REDISMODULE_ERR;
  }

  // Print version string!
  DO_LOG("notice", "RediSearch version %d.%d.%d (Git=%s)", REDISEARCH_VERSION_MAJOR,
         REDISEARCH_VERSION_MINOR, REDISEARCH_VERSION_PATCH, RS_GetExtraVersion());
  RS_Initialized = 1;

  if (!RSDummyContext) {
    if (RedisModule_GetDetachedThreadSafeContext) {
      RSDummyContext = RedisModule_GetDetachedThreadSafeContext(ctx);
    } else {
      RSDummyContext = RedisModule_GetThreadSafeContext(NULL);
    }
  }

  if (mode == REDISEARCH_INIT_MODULE && initAsModule(ctx) != REDISMODULE_OK) {
    return REDISMODULE_ERR;
  } else if (mode == REDISEARCH_INIT_LIBRARY && initAsLibrary(ctx) != REDISMODULE_OK) {
    return REDISMODULE_ERR;
  }

  sds confstr = RSConfig_GetInfoString(&RSGlobalConfig);
  DO_LOG("notice", "%s", confstr);
  sdsfree(confstr);

  // Init extension mechanism
  Extensions_Init();

  Indexes_Init(ctx);

  if (RSGlobalConfig.concurrentMode) {
    ConcurrentSearch_ThreadPoolStart();
  }

  GC_ThreadPoolStart();

  CleanPool_ThreadPoolStart();
  // Init cursors mechanism
  CursorList_Init(&RSCursors);

  IndexAlias_InitGlobal();

  // Register aggregation functions
  RegisterAllFunctions();

  DO_LOG("notice", "Initialized thread pool!");

  /* Load extensions if needed */
  if (RSGlobalConfig.extLoad != NULL) {

    char *errMsg = NULL;
    // Load the extension so TODO: pass with param
    if (Extension_LoadDynamic(RSGlobalConfig.extLoad, &errMsg) == REDISMODULE_ERR) {
      DO_LOG("warning", "Could not load extension %s: %s", RSGlobalConfig.extLoad, errMsg);
      rm_free(errMsg);
      return REDISMODULE_ERR;
    }
    DO_LOG("notice", "Loaded RediSearch extension '%s'", RSGlobalConfig.extLoad);
  }

  // Register the default hard coded extension
  if (Extension_Load("DEFAULT", DefaultExtensionInit) == REDISEARCH_ERR) {
    DO_LOG("warning", "Could not register default extension");
    return REDISMODULE_ERR;
  }

  // Register Info function
  if (RedisModule_RegisterInfoFunc(ctx, RS_moduleInfoFunc) == REDISMODULE_ERR)
    return REDISMODULE_ERR;

  Initialize_KeyspaceNotifications(ctx);
  Initialize_CommandFilter(ctx);
  GetJSONAPIs(ctx, 1);
  Initialize_RdbNotifications(ctx);

  // Register rm_malloc memory functions as vector similarity memory functions.
  VecSimMemoryFunctions vecsimMemoryFunctions = {.allocFunction = rm_malloc, .callocFunction = rm_calloc, .reallocFunction = rm_realloc, .freeFunction = rm_free};
  VecSim_SetMemoryFunctions(vecsimMemoryFunctions);
  return REDISMODULE_OK;
}
