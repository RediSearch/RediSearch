/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#pragma once

#include "redismodule.h"
#include "hiredis/sds.h"
#include "query_error.h"
#include "reply.h"
#include "util/config_macros.h"
#include "ext/default.h"

typedef enum {
  TimeoutPolicy_Return,       // Return what we have on timeout
  TimeoutPolicy_Fail,         // Just fail without returning anything
  TimeoutPolicy_Invalid       // Not a real value
} RSTimeoutPolicy;

static const int on_timeout_enums[2] = {
  TimeoutPolicy_Return,
  TimeoutPolicy_Fail
};
static const char *on_timeout_vals[2] = {
  "return",
  "fail"
};

typedef enum {
  OomPolicy_Return,       // Return what we have on OOM
  OomPolicy_Fail,         // Just fail without returning anything
  OomPolicy_Ignore,       // Ignore OOM and continue
  OomPolicy_Invalid       // Not a real value
} RSOomPolicy;

static const int on_oom_enums[3] = {
  OomPolicy_Return,
  OomPolicy_Fail,
  OomPolicy_Ignore
};
static const char *on_oom_vals[3] = {
  "return",
  "fail",
  "ignore"
};


typedef enum { GCPolicy_Fork = 0 } GCPolicy;

const char *TimeoutPolicy_ToString(RSTimeoutPolicy);
const char *OomPolicy_ToString(RSOomPolicy);

/**
 * Returns TimeoutPolicy_Invalid if the string could not be parsed
 */
RSTimeoutPolicy TimeoutPolicy_Parse(const char *s, size_t n);
RSOomPolicy OomPolicy_Parse(const char *s, size_t n);

static inline const char *GCPolicy_ToString(GCPolicy policy) {
  switch (policy) {
    case GCPolicy_Fork:
      return "fork";
    default:          // LCOV_EXCL_LINE cannot be reached
      return "huh?";  // LCOV_EXCL_LINE cannot be reached
  }
}
typedef struct {
  size_t forkGcRunIntervalSec;
  size_t forkGcCleanThreshold;
  size_t forkGcRetryInterval;
  size_t forkGcSleepBeforeExit;
  int forkGCCleanNumericEmptyNodes;
} forkGcConfig;

typedef struct {
  // If this is set, GC is enabled on all indexes (default: 1, disable with NOGC)
  bool enableGC;
  size_t gcScanSize;
  GCPolicy gcPolicy;

  forkGcConfig forkGc;
} GCConfig;

// Configuration parameters related to aggregate request.
typedef struct {
  // Default dialect level used throughout database lifetime.
  unsigned int dialectVersion;
  // The maximal amount of time a single query can take before timing out, in milliseconds.
  // 0 means unlimited
  long long queryTimeoutMS;
  RSTimeoutPolicy timeoutPolicy;
  // reply with time on profile
  bool printProfileClock;
  // BM25STD.TANH factor
  unsigned int BM25STD_TanhFactor;
  // OOM policy
  RSOomPolicy oomPolicy;
} RequestConfig;

// Configuration parameters related to the query execution.
typedef struct {
  // The maximal number of expansions we allow for a prefix. Default: 200
  long long maxPrefixExpansions;
  // The minimal number of characters we allow expansion for in a prefix search. Default: 2
  long long minTermPrefix;
  // The minimal word length to stem. Default 4
  unsigned int minStemLength;
  long long minUnionIterHeap;
} IteratorsConfig;

/* RSConfig is a global configuration struct for the module, it can be included from each file,
 * and is initialized with user config options during module startup */
typedef struct {
  // Version of Redis server
  int serverVersion;
  // If not null, this points at a .so file of an extension we try to load (default: NULL)
  const char *extLoad;
  // Path to friso.ini for chinese dictionary file
  const char *frisoIni;
  // Default scorer name to use when no scorer is specified (default: BM25STD)
  const char *defaultScorer;

  IteratorsConfig iteratorsConfigParams;

  RequestConfig requestConfigParams;

  // Number of rows to read from a cursor if not specified
  long long cursorReadSize;

  // Maximum idle time for a cursor. Users can use shorter lifespans, but never
  // longer ones
  long long cursorMaxIdle;

  size_t maxDocTableSize;
  size_t maxSearchResults;
  size_t maxAggregateResults;

  // MT configuration
  size_t numWorkerThreads;
  size_t minOperationWorkers;
  size_t tieredVecSimIndexBufferLimit;
  size_t highPriorityBiasNum;

  size_t minPhoneticTermLen;

  GCConfig gcConfigParams;

  // Chained configuration data
  void *chainedConfig;

  bool noMemPool;

  bool filterCommands;

  // free resource on shutdown
  bool freeResourcesThread;
  // compress double to float
  bool numericCompress;
  // keep numeric ranges in parents of leafs
  size_t numericTreeMaxDepthRange;
  // disable compression for inverted index DocIdsOnly
  bool invertedIndexRawDocidEncoding;

  // sets the memory limit for vector indexes to resize by (in bytes).
  // 0 indicates no limit. Default value is 0.
  unsigned int vssMaxResize;
  // The delta used to increase positional offsets between array slots for multi text values.
  // Can allow to control the separation between phrases in different array slots (related to the SLOP parameter in ft.search command)
  // Default value is 100. 0 will not increment (as if all text is a continuous phrase).
  unsigned int multiTextOffsetDelta;
  // The number of iterations to run while performing background indexing
  // before we call usleep(1) (sleep for 1 micro-second) and make sure that
  // we allow redis process other commands.
  unsigned int numBGIndexingIterationsBeforeSleep;
  // If set, we use an optimization that sorts the children of an intersection iterator in a way
  // where union iterators are being factorize by the number of their own children.
  bool prioritizeIntersectUnionChildren;
    // The number of indexing operations per field to perform before yielding to Redis during indexing while loading (so redis can be responsive)
  unsigned int indexerYieldEveryOpsWhileLoading;
  // Limit the number of cursors that can be created for a single index
  long long indexCursorLimit;
  // The maximum ratio between current memory and max memory for which background indexing is allowed
  uint8_t indexingMemoryLimit;
  // Enable to execute unstable features
  bool enableUnstableFeatures;
  // Control user data obfuscation in logs
  bool hideUserDataFromLog;
  // Set how much time after OOM is detected we should wait to enable the resource manager to
  // allocate more memory.
  uint32_t bgIndexingOomPauseTimeBeforeRetry;
} RSConfig;

typedef enum {
  RSCONFIGVAR_F_IMMUTABLE = 0x01,
  RSCONFIGVAR_F_MODIFIED = 0x02,
  RSCONFIGVAR_F_FLAG = 0x04,
} RSConfigVarFlags;

typedef struct {
  const char *name;
  const char *helpText;
  uint32_t flags;
  uint32_t triggerId;
  // Whether this configuration option can be modified after initial loading
  int (*setValue)(RSConfig *, ArgsCursor *, uint32_t, QueryError *);
  sds (*getValue)(const RSConfig *);
} RSConfigVar;

#define RS_MAX_CONFIG_VARS 255
typedef struct RSConfigOptions {
  RSConfigVar vars[RS_MAX_CONFIG_VARS];
  struct RSConfigOptions *next;
} RSConfigOptions;

typedef int (*RSConfigExternalTrigger)(RSConfig *);

// global config extern references
extern RSConfig RSGlobalConfig;
extern RSConfigOptions RSGlobalConfigOptions;
extern RedisModuleString *config_ext_load;
extern RedisModuleString *config_friso_ini;
extern RedisModuleString *config_default_scorer;

/**
 * Add new configuration options to the chain of already recognized options
 */
void RSConfigOptions_AddConfigs(RSConfigOptions *src, RSConfigOptions *dst);

/**
 * Register a new external trigger for configuration changes.
 * This function should be called on the module load time, before we start reading
 * any configuration.
 * @param trigger the trigger function
 * @param configs an array of configuration names that trigger the function.
 *                The array must be NULL-terminated.
 */
void RSConfigExternalTrigger_Register(RSConfigExternalTrigger trigger, const char **configs);

/* Read configuration from redis module arguments into the global config object. Return
 * REDISMODULE_ERR and sets an error message if something is invalid */
int ReadConfig(RedisModuleString **argv, int argc, char **err);

/* Register module configuration parameters using Module Configuration API */
int RegisterModuleConfig_Local(RedisModuleCtx *ctx);

/**
 * Writes the retrieval of the configuration value to the network.
 * isHelp will use a more dict-like pattern, which should be a bit friendlier
 * on the eyes
 */
void RSConfig_DumpProto(const RSConfig *cfg, const RSConfigOptions *options, const char *name,
                        RedisModule_Reply *reply, bool isHelp);

/**
 * Sets a configuration variable. The argv, argc, and offset variables should
 * point to the global argv array. You can also make argv point at the specific
 * (after-the-option-name) arguments and set offset to 0, and argc to the number
 * of remaining arguments. offset is advanced to the next unread argument (which
 * can be == argc)
 */
int RSConfig_SetOption(RSConfig *config, RSConfigOptions *options, const char *name,
                       RedisModuleString **argv, int argc, size_t *offset, QueryError *status);

sds RSConfig_GetInfoString(const RSConfig *config);

void RSConfig_AddToInfo(RedisModuleInfoCtx *ctx);

void UpgradeDeprecatedMTConfigs();

char *getRedisConfigValue(RedisModuleCtx *ctx, const char* confName);

// We limit the number of worker threads to limit the amount of memory used by the thread pool
// and to prevent the system from running out of resources.
// The number of worker threads should be proportional to the number of cores in the system at most,
// otherwise no performance improvement will be achieved.
#ifndef MAX_WORKER_THREADS
#define MAX_WORKER_THREADS (1 << 4)
#endif
#define DEFAULT_BG_INDEX_SLEEP_GAP 100
#define DEFAULT_DIALECT_VERSION 1
#define DEFAULT_DOC_TABLE_SIZE 1000000
#define DEFAULT_GC_SCANSIZE 100
#define DEFAULT_MIN_PHONETIC_TERM_LEN 3
#define DEFAULT_FORK_GC_CLEAN_THRESHOLD 100
#define DEFAULT_FORK_GC_RETRY_INTERVAL 5
#define DEFAULT_FORK_GC_RUN_INTERVAL 30
#define DEFAULT_INDEX_CURSOR_LIMIT 128
#define MAX_AGGREGATE_REQUEST_RESULTS (1ULL << 31)
#define DEFAULT_MAX_AGGREGATE_REQUEST_RESULTS MAX_AGGREGATE_REQUEST_RESULTS
#define DEFAULT_MAX_CURSOR_IDLE 300000
#define DEFAULT_MAX_PREFIX_EXPANSIONS 200
#define DEFAULT_MAX_SEARCH_REQUEST_RESULTS 1000000
#define MAX_SEARCH_REQUEST_RESULTS (1ULL << 31)
#define MAX_KNN_K (1ULL << 58)
#define DEFAULT_MIN_TERM_PREFIX 2
#define DEFAULT_MIN_STEM_LENGTH 4
#define DEFAULT_MULTI_TEXT_SLOP 100
#define DEFAULT_QUERY_TIMEOUT_MS 500
#define DEFAULT_UNION_ITERATOR_HEAP 20
#define DEFAULT_VSS_MAX_RESIZE 0
#define DEFAULT_WORKER_THREADS 0
#define MAX_DOC_TABLE_SIZE 100000000
#define NR_MAX_DEPTH_BALANCE 2
#define VECSIM_DEFAULT_BLOCK_SIZE   1024
#define MIN_MIN_STEM_LENGTH 2 // Minimum value for minStemLength
#define MIN_OPERATION_WORKERS 4
#define DEFAULT_INDEXING_MEMORY_LIMIT 100
#define DEFAULT_BM25STD_TANH_FACTOR 4
#define BM25STD_TANH_FACTOR_MAX 10000
#define BM25STD_TANH_FACTOR_MIN 1
#define DEFAULT_BG_OOM_PAUSE_TIME_BEFOR_RETRY 5
#define DEFAULT_INDEXER_YIELD_EVERY_OPS 1000
#define DEFAULT_SHARD_WINDOW_RATIO 1.0
#define MIN_SHARD_WINDOW_RATIO 0.0  // Exclusive minimum (must be > 0.0)
#define MAX_SHARD_WINDOW_RATIO 1.0

// default configuration
#define RS_DEFAULT_CONFIG {                                                    \
    .extLoad = NULL,                                                           \
    .frisoIni = NULL,                                                          \
    .defaultScorer = NULL,                                                     \
    .gcConfigParams.enableGC = 1,                                              \
    .iteratorsConfigParams.minTermPrefix = DEFAULT_MIN_TERM_PREFIX,            \
    .iteratorsConfigParams.minStemLength = DEFAULT_MIN_STEM_LENGTH,            \
    .iteratorsConfigParams.maxPrefixExpansions = DEFAULT_MAX_PREFIX_EXPANSIONS,\
    .requestConfigParams.queryTimeoutMS = DEFAULT_QUERY_TIMEOUT_MS,            \
    .requestConfigParams.timeoutPolicy = TimeoutPolicy_Return,                 \
    .cursorReadSize = 1000,                                                    \
    .cursorMaxIdle = DEFAULT_MAX_CURSOR_IDLE,                                  \
    .maxDocTableSize = DEFAULT_DOC_TABLE_SIZE,                                 \
    .numWorkerThreads = DEFAULT_WORKER_THREADS,                                \
    .minOperationWorkers = MIN_OPERATION_WORKERS,                              \
    .tieredVecSimIndexBufferLimit = DEFAULT_BLOCK_SIZE,                        \
    .highPriorityBiasNum = DEFAULT_HIGH_PRIORITY_BIAS_THRESHOLD,               \
    .gcConfigParams.gcScanSize = DEFAULT_GC_SCANSIZE,                          \
    .minPhoneticTermLen = DEFAULT_MIN_PHONETIC_TERM_LEN,                       \
    .gcConfigParams.gcPolicy = GCPolicy_Fork,                                  \
    .gcConfigParams.forkGc.forkGcRunIntervalSec = DEFAULT_FORK_GC_RUN_INTERVAL,\
    .gcConfigParams.forkGc.forkGcSleepBeforeExit = 0,                          \
    .gcConfigParams.forkGc.forkGcRetryInterval = DEFAULT_FORK_GC_RETRY_INTERVAL,\
    .gcConfigParams.forkGc.forkGcCleanThreshold = DEFAULT_FORK_GC_CLEAN_THRESHOLD,\
    .noMemPool = 0,                                                            \
    .filterCommands = 0,                                                       \
    .maxSearchResults = DEFAULT_MAX_SEARCH_REQUEST_RESULTS,                    \
    .maxAggregateResults = DEFAULT_MAX_AGGREGATE_REQUEST_RESULTS,              \
    .iteratorsConfigParams.minUnionIterHeap = DEFAULT_UNION_ITERATOR_HEAP,     \
    .numericCompress = false,                                                  \
    .numericTreeMaxDepthRange = 0,                                             \
    .requestConfigParams.printProfileClock = 1,                                \
    .invertedIndexRawDocidEncoding = false,                                    \
    .gcConfigParams.forkGc.forkGCCleanNumericEmptyNodes = true,                \
    .freeResourcesThread = true,                                               \
    .requestConfigParams.dialectVersion = DEFAULT_DIALECT_VERSION,             \
    .vssMaxResize = DEFAULT_VSS_MAX_RESIZE,                                    \
    .multiTextOffsetDelta = DEFAULT_MULTI_TEXT_SLOP,                           \
    .numBGIndexingIterationsBeforeSleep = DEFAULT_BG_INDEX_SLEEP_GAP,          \
    .prioritizeIntersectUnionChildren = false,                                 \
    .indexCursorLimit = DEFAULT_INDEX_CURSOR_LIMIT,                            \
    .enableUnstableFeatures = DEFAULT_UNSTABLE_FEATURES_ENABLE,                \
    .hideUserDataFromLog = false,                                              \
    .indexingMemoryLimit = DEFAULT_INDEXING_MEMORY_LIMIT,                      \
    .requestConfigParams.BM25STD_TanhFactor = DEFAULT_BM25STD_TANH_FACTOR,     \
    .bgIndexingOomPauseTimeBeforeRetry = DEFAULT_BG_OOM_PAUSE_TIME_BEFOR_RETRY,    \
    .indexerYieldEveryOpsWhileLoading = DEFAULT_INDEXER_YIELD_EVERY_OPS,       \
    .requestConfigParams.oomPolicy = OomPolicy_Return,                         \
  }

#define REDIS_ARRAY_LIMIT 7
#define NO_REPLY_DEPTH_LIMIT 0x00060020
#define RM_SCAN_KEY_API_FIX 0x00060006

static inline int isFeatureSupported(int feature) {
  return feature <= RSGlobalConfig.serverVersion;
}

#ifdef __cplusplus
extern "C" {
#endif

// Gets a pointer to an empty IteratorsConfig struct and copy the current
// RSGlobalConfig.IteratorsConfig parameters values into it.
// The size of the memory @param config points to must be at least sizeof(IteratorsConfig)
void iteratorsConfig_init(IteratorsConfig *config);

void LogWarningDeprecatedFTConfig(RedisModuleCtx *ctx, const char *action,
                                  const char *name);

#ifdef __cplusplus
}
#endif
