
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
#include "util/workers.h"
#include "util/array.h"
#include "cursor.h"
#include "fork_gc.h"
#include "info_command.h"
#include "profile.h"

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
  if (RediSearch_ExportCapi(ctx) != REDISMODULE_OK) {
    RedisModule_Log(ctx, "warning", "Could not initialize low level api");
  } else {
    RedisModule_Log(ctx, "notice", "Low level api version %d initialized successfully",
                    REDISEARCH_CAPI_VERSION);
  }

  if (!validateAofSettings(ctx)) {
    return REDISMODULE_ERR;
  }

  GetJSONAPIs(ctx, 1);

  return REDISMODULE_OK;
}

static int initAsLibrary(RedisModuleCtx *ctx) {
  RSGlobalConfig.iteratorsConfigParams.minTermPrefix = 0;
  RSGlobalConfig.iteratorsConfigParams.maxPrefixExpansions = LONG_MAX;
  RSGlobalConfig.iteratorsConfigParams.minStemLength = DEFAULT_MIN_STEM_LENGTH;
  return REDISMODULE_OK;
}

void RS_moduleInfoFunc(RedisModuleInfoCtx *ctx, int for_crash_report) {
  // Module version
  RedisModule_InfoAddSection(ctx, "version");
  char ver[64];
  // RediSearch version
  sprintf(ver, "%d.%d.%d", REDISEARCH_VERSION_MAJOR, REDISEARCH_VERSION_MINOR, REDISEARCH_VERSION_PATCH);
  RedisModule_InfoAddFieldCString(ctx, "version", ver);
  // Redis version
  GetFormattedRedisVersion(ver, sizeof(ver));
  RedisModule_InfoAddFieldCString(ctx, "redis_version", ver);
  // Redis Enterprise version
  if (IsEnterprise()) {
    GetFormattedRedisEnterpriseVersion(ver, sizeof(ver));
    RedisModule_InfoAddFieldCString(ctx, "redis_enterprise_version", ver);
  }

  // Numer of indexes
  RedisModule_InfoAddSection(ctx, "index");
  RedisModule_InfoAddFieldLongLong(ctx, "number_of_indexes", dictSize(specDict_g));

  // Fields statistics
  FieldsGlobalStats_AddToInfo(ctx);

  // Memory
  RedisModule_InfoAddSection(ctx, "memory");
  TotalSpecsInfo total_info = RediSearch_TotalInfo();
  RedisModule_InfoAddFieldDouble(ctx, "used_memory_indexes", total_info.total_mem);
  RedisModule_InfoAddFieldDouble(ctx, "used_memory_indexes_human", total_info.total_mem / (float)0x100000);
  RedisModule_InfoAddFieldDouble(ctx, "total_indexing_time", total_info.indexing_time / (float)CLOCKS_PER_MILLISEC);

  // Cursors
  RedisModule_InfoAddSection(ctx, "cursors");
  CursorsInfoStats cursorsStats = Cursors_GetInfoStats();
  RedisModule_InfoAddFieldLongLong(ctx, "global_idle", cursorsStats.total_idle);
  RedisModule_InfoAddFieldLongLong(ctx, "global_total", cursorsStats.total);

  // GC stats
  RedisModule_InfoAddSection(ctx, "gc");
  InfoGCStats stats = total_info.gc_stats;
  RedisModule_InfoAddFieldDouble(ctx, "bytes_collected", stats.totalCollectedBytes);
  RedisModule_InfoAddFieldDouble(ctx, "total_cycles", stats.totalCycles);
  RedisModule_InfoAddFieldDouble(ctx, "total_ms_run", stats.totalTime);

  // Dialect statistics
  DialectsGlobalStats_AddToInfo(ctx);

  // Run time configuration
  RSConfig_AddToInfo(ctx);

  #ifdef FTINFO_FOR_INFO_MODULES
  // FT.INFO for some of the indexes
  dictIterator *iter = dictGetIterator(specDict_g);
  dictEntry *entry;
  int count = 5;
  while (count-- && (entry = dictNext(iter))) {
    StrongRef ref = dictGetRef(entry);
    IndexSpec *sp = StrongRef_Get(ref);
    if (sp) {
      IndexSpec_AddToInfo(ctx, sp);
    }
  }
  dictReleaseIterator(iter);
  #endif
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
    RSDummyContext = RedisModule_GetDetachedThreadSafeContext(ctx);
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

  GC_ThreadPoolStart();

  CleanPool_ThreadPoolStart();
  DO_LOG("notice", "Initialized thread pools!");

  // Init cursors mechanism
  CursorList_Init(&g_CursorsList, false);
  CursorList_Init(&g_CursorsListCoord, true);

#ifdef MT_BUILD
  // Init threadpool.
  // Threadpool size can only be set on load.
  if ((RSGlobalConfig.mt_mode == MT_MODE_ONLY_ON_OPERATIONS || RSGlobalConfig.mt_mode == MT_MODE_FULL)  && RSGlobalConfig.numWorkerThreads == 0) {
    DO_LOG("warning", "Invalid configuration - cannot run in MT_MODE (FULL/ONLY_ON_OPERATIONS) while WORKERS_THREADS"
           " number is set to zero");
    return REDISMODULE_ERR;
  }
  if(RSGlobalConfig.numWorkerThreads) {
    if (workersThreadPool_CreatePool(RSGlobalConfig.numWorkerThreads) == REDISMODULE_ERR) {
      return REDISMODULE_ERR;
    }
    if (RSGlobalConfig.mt_mode == MT_MODE_FULL) {
      // If the module configuration states that worker threads should always be active,
      // we log about the threadpool creation.
      DO_LOG("notice", "Created workers threadpool of size %lu", RSGlobalConfig.numWorkerThreads);
      DO_LOG("verbose", "threadpool has %lu high-priority bias that always prefer running queries"
             " when possible", RSGlobalConfig.highPriorityBiasNum);
    } else {
      // Otherwise, threads shouldn't always be used, and we're performing inplace writes.
      // VSS lib is async by default.
      VecSim_SetWriteMode(VecSim_WriteInPlace);
    }
  } else
#endif
  {
    // If we don't have a thread pool,
    // we have to make sure that we tell the vecsim library to add and delete in place (can't use submit at all)
    VecSim_SetWriteMode(VecSim_WriteInPlace);
  }

  IndexAlias_InitGlobal();

  // Register aggregation functions
  RegisterAllFunctions();

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

  // Register to Info function
  if (RedisModule_RegisterInfoFunc && RedisModule_RegisterInfoFunc(ctx, RS_moduleInfoFunc) == REDISMODULE_ERR) {
    return REDISMODULE_ERR;
  }

  Initialize_KeyspaceNotifications(ctx);
  Initialize_CommandFilter(ctx);
  Initialize_RdbNotifications(ctx);
  Initialize_RoleChangeNotifications(ctx);

  // Register rm_malloc memory functions as vector similarity memory functions.
  VecSimMemoryFunctions vecsimMemoryFunctions = {.allocFunction = rm_malloc, .callocFunction = rm_calloc, .reallocFunction = rm_realloc, .freeFunction = rm_free};
  VecSim_SetMemoryFunctions(vecsimMemoryFunctions);
  VecSim_SetTimeoutCallbackFunction((timeoutCallbackFunction)TimedOut_WithCtx);
  VecSim_SetLogCallbackFunction(VecSimLogCallback);
  return REDISMODULE_OK;
}
