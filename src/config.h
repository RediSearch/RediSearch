/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#ifndef RS_CONFIG_H_
#define RS_CONFIG_H_

#include "redismodule.h"
#include "rmutil/sds.h"
#include "query_error.h"
#include "fields_global_stats.h"

typedef enum {
  TimeoutPolicy_Return,       // Return what we have on timeout
  TimeoutPolicy_Fail,         // Just fail without returning anything
  TimeoutPolicy_Invalid       // Not a real value
} RSTimeoutPolicy;

typedef enum { GCPolicy_Fork = 0 } GCPolicy;

const char *TimeoutPolicy_ToString(RSTimeoutPolicy);

/**
 * Returns TimeoutPolicy_Invalid if the string could not be parsed
 */
RSTimeoutPolicy TimeoutPolicy_Parse(const char *s, size_t n);

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
  int enableGC;
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
  int printProfileClock;
} RequestConfig;

// Configuration parameters related to the query execution.
typedef struct {
  // The maximal number of expansions we allow for a prefix. Default: 200
  long long maxPrefixExpansions;
  // The minimal number of characters we allow expansion for in a prefix search. Default: 2
  long long minTermPrefix;
  long long maxResultsToUnsortedMode;
  long long minUnionIterHeap;
} IteratorsConfig;


#ifdef MT_BUILD
typedef enum {
  MT_MODE_OFF,
  MT_MODE_ONLY_ON_OPERATIONS,
  MT_MODE_FULL
} MTMode;
#endif


/* RSConfig is a global configuration struct for the module, it can be included from each file,
 * and is initialized with user config options during module statrtup */
typedef struct {
  // Version of Redis server
  int serverVersion;
  // Use concurrent search (default: 1, disable with SAFEMODE)
  int concurrentMode;
  // If not null, this points at a .so file of an extension we try to load (default: NULL)
  const char *extLoad;
  // Path to friso.ini for chinese dictionary file
  const char *frisoIni;

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
  size_t searchPoolSize;
  size_t indexPoolSize;
  int poolSizeNoAuto;  // Don't auto-detect pool size

#ifdef MT_BUILD
  size_t numWorkerThreads;
  MTMode mt_mode;
  size_t tieredVecSimIndexBufferLimit;
  size_t privilegedThreadsNum;
#endif

  size_t minPhoneticTermLen;

  GCConfig gcConfigParams;

  FieldsGlobalStats fieldsStats;

  // Chained configuration data
  void *chainedConfig;

  int noMemPool;

  int filterCommands;

  // free resource on shutdown
  int freeResourcesThread;
  // compress double to float
  int numericCompress;
  // keep numeric ranges in parents of leafs
  size_t numericTreeMaxDepthRange;
  // disable compression for inverted index DocIdsOnly
  int invertedIndexRawDocidEncoding;

  // sets the memory limit for vector indexes to resize by (in bytes).
  // 0 indicates no limit. Default value is 0.
  unsigned int vssMaxResize;
  // The delta used to increase positional offsets between array slots for multi text values.
  // Can allow to control the seperation between phrases in different array slots (related to the SLOP parameter in ft.search command)
  // Default value is 100. 0 will not increment (as if all text is a continus phrase).
  unsigned int multiTextOffsetDelta;
  // bitarray of dialects used by all indices
  uint_least8_t used_dialects;
  // The number of iterations to run while performing background indexing
  // before we call usleep(1) (sleep for 1 micro-second) and make sure that
  // we allow redis process other commands.
  unsigned int numBGIndexingIterationsBeforeSleep;
} RSConfig;

typedef enum {
  RSCONFIGVAR_F_IMMUTABLE = 0x01,
  RSCONFIGVAR_F_MODIFIED = 0x02,
  RSCONFIGVAR_F_FLAG = 0x04,
  RSCONFIGVAR_F_SHORTHAND = 0x08
} RSConfigVarFlags;

typedef struct {
  const char *name;
  const char *helpText;
  uint32_t flags;
  // Whether this configuration option can be modified after initial loading
  int (*setValue)(RSConfig *, ArgsCursor *, QueryError *);
  sds (*getValue)(const RSConfig *);
} RSConfigVar;

#define RS_MAX_CONFIG_VARS 255
typedef struct RSConfigOptions {
  RSConfigVar vars[RS_MAX_CONFIG_VARS];
  struct RSConfigOptions *next;
} RSConfigOptions;

// global config extern references
extern RSConfig RSGlobalConfig;
extern RSConfigOptions RSGlobalConfigOptions;

/**
 * Add new configuration options to the chain of already recognized options
 */
void RSConfigOptions_AddConfigs(RSConfigOptions *src, RSConfigOptions *dst);

/* Read configuration from redis module arguments into the global config object. Return
 * REDISMODULE_ERR and sets an error message if something is invalid */
int ReadConfig(RedisModuleString **argv, int argc, char **err);

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

void DialectsGlobalStats_AddToInfo(RedisModuleInfoCtx *ctx);

#define DEFAULT_DOC_TABLE_SIZE 1000000
#define MAX_DOC_TABLE_SIZE 100000000
#define CONCURRENT_SEARCH_POOL_DEFAULT_SIZE 20
#define CONCURRENT_INDEX_POOL_DEFAULT_SIZE 8
#define CONCURRENT_INDEX_MAX_POOL_SIZE 200  // Maximum number of threads to create
#define GC_SCANSIZE 100
#define DEFAULT_MIN_PHONETIC_TERM_LEN 3
#define DEFAULT_FORK_GC_RUN_INTERVAL 30
#define DEFAULT_MAX_RESULTS_TO_UNSORTED_MODE 1000
#define SEARCH_REQUEST_RESULTS_MAX 1000000
#define NR_MAX_DEPTH_BALANCE 2
#define MIN_DIALECT_VERSION 1 // MIN_DIALECT_VERSION is expected to change over time as dialects become deprecated.
#define MAX_DIALECT_VERSION 4 // MAX_DIALECT_VERSION may not exceed MIN_DIALECT_VERSION + 7.
#define DIALECT_OFFSET(d) (1ULL << (d - MIN_DIALECT_VERSION))// offset of the d'th bit. begins at MIN_DIALECT_VERSION (bit 0) up to MAX_DIALECT_VERSION.
#define GET_DIALECT(barr, d) (!!(barr & DIALECT_OFFSET(d)))  // return the truth value of the d'th dialect in the dialect bitarray.
#define SET_DIALECT(barr, d) (barr |= DIALECT_OFFSET(d))     // set the d'th dialect in the dialect bitarray to true.
#define VECSIM_DEFAULT_BLOCK_SIZE   1024

#ifdef MT_BUILD  
#define MT_BUILD_CONFIG .numWorkerThreads = 0,                                                                     \
    .mt_mode = MT_MODE_OFF,                                                                                                     \
    .tieredVecSimIndexBufferLimit = DEFAULT_BLOCK_SIZE,                                                            \
    .privilegedThreadsNum = DEFAULT_PRIVILEGED_THREADS_NUM,
#else 
#define MT_BUILD_CONFIG
#endif 

// default configuration
#define RS_DEFAULT_CONFIG {                                                                                           \
    .concurrentMode = 0,                                                                                              \
    .extLoad = NULL,                                                                                                  \
    .gcConfigParams.enableGC = 1,                                                                                                    \
    .iteratorsConfigParams.minTermPrefix = 2,                                                                                               \
    .iteratorsConfigParams.maxPrefixExpansions = 200,                                                                                       \
    .requestConfigParams.queryTimeoutMS = 500,                                                                                            \
    .requestConfigParams.timeoutPolicy = TimeoutPolicy_Return,                                                                            \
    .cursorReadSize = 1000,                                                                                           \
    .cursorMaxIdle = 300000,                                                                                          \
    .maxDocTableSize = DEFAULT_DOC_TABLE_SIZE,                                                                        \
    .searchPoolSize = CONCURRENT_SEARCH_POOL_DEFAULT_SIZE,                                                            \
    .indexPoolSize = CONCURRENT_INDEX_POOL_DEFAULT_SIZE,                                                              \
    .poolSizeNoAuto = 0,   \
    MT_BUILD_CONFIG                                                                                                 \
    .gcConfigParams.gcScanSize = GC_SCANSIZE,                                                                                        \
    .minPhoneticTermLen = DEFAULT_MIN_PHONETIC_TERM_LEN,                                                              \
    .gcConfigParams.gcPolicy = GCPolicy_Fork,                                                                                        \
    .gcConfigParams.forkGc.forkGcRunIntervalSec = DEFAULT_FORK_GC_RUN_INTERVAL,                                                             \
    .gcConfigParams.forkGc.forkGcSleepBeforeExit = 0,                                                                                       \
    .iteratorsConfigParams.maxResultsToUnsortedMode = DEFAULT_MAX_RESULTS_TO_UNSORTED_MODE,                                                 \
    .gcConfigParams.forkGc.forkGcRetryInterval = 5,                                                                                         \
    .gcConfigParams.forkGc.forkGcCleanThreshold = 100,                                                                                      \
    .noMemPool = 0,                                                                                                   \
    .filterCommands = 0,                                                                                              \
    .maxSearchResults = SEARCH_REQUEST_RESULTS_MAX,                                                                   \
    .maxAggregateResults = -1,                                                                                        \
    .iteratorsConfigParams.minUnionIterHeap = 20,                                                                                           \
    .numericCompress = false,                                                                                         \
    .numericTreeMaxDepthRange = 0,                                                                                    \
    .requestConfigParams.printProfileClock = 1,                                                                                           \
    .invertedIndexRawDocidEncoding = false,                                                                           \
    .gcConfigParams.forkGc.forkGCCleanNumericEmptyNodes = true,                                                                             \
    .freeResourcesThread = true,                                                                                      \
    .requestConfigParams.dialectVersion = 1,                                                                                       \
    .vssMaxResize = 0,                                                                                                \
    .multiTextOffsetDelta = 100,                                                                                      \
    .used_dialects = 0,                                                                                               \
    .numBGIndexingIterationsBeforeSleep = 100,                                                                         \
  }

#define REDIS_ARRAY_LIMIT 7
#define NO_REPLY_DEPTH_LIMIT 0x00060020
#define RM_SCAN_KEY_API_FIX 0x00060006

static inline int isFeatureSupported(int feature) {
  return feature <= RSGlobalConfig.serverVersion;
}

#define CONFIG_SETTER(name) int name(RSConfig *config, ArgsCursor *ac, QueryError *status)

#ifdef __cplusplus
extern "C" {
#endif

// Gets a pointer to an empty IteratorsConfig struct and copy the current
// RSGlobalConfig.IteratorsConfig parameters values into it.
// The size of the memory @param config points to must be at least sizeof(IteratorsConfig)
void iteratorsConfig_init(IteratorsConfig *config);

#ifdef __cplusplus
}
#endif
#endif
