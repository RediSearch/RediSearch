/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "config.h"
#include "deps/thpool/thpool.h"
#include "err.h"
#include "rmutil/util.h"
#include "rmutil/strings.h"
#include "rmutil/args.h"
#include <string.h>
#include <stdlib.h>
#include <limits.h>
#include "rmalloc.h"
#include "rules.h"
#include "spec.h"
#include "util/dict.h"
#include "resp3.h"
#include "util/workers.h"

#define __STRINGIFY(x) #x
#define STRINGIFY(x) __STRINGIFY(x)

#define RS_MAX_CONFIG_TRIGGERS 1 // Increase this if you need more triggers
RSConfigExternalTrigger RSGlobalConfigTriggers[RS_MAX_CONFIG_TRIGGERS];

typedef struct {
  const char *FTConfigName;
  const char *ConfigName;
} configPair_t;

configPair_t __configPairs[] = {
  {"_FREE_RESOURCE_ON_THREAD",        "search-_free-resource-on-thread"},
  {"_NUMERIC_COMPRESS",               "search-_numeric-compress"},
  {"_NUMERIC_RANGES_PARENTS",         "search-_numeric-ranges-parents"},
  {"_PRINT_PROFILE_CLOCK",            "search-_print-profile-clock"},
  {"_PRIORITIZE_INTERSECT_UNION_CHILDREN", "search-_prioritize-intersect-union-children"},
  {"BG_INDEX_SLEEP_GAP",              "search-bg-index-sleep-gap"},
  {"CONN_PER_SHARD",                  "search-conn-per-shard"},
  {"CURSOR_MAX_IDLE",                 "search-cursor-max-idle"},
  {"CURSOR_REPLY_THRESHOLD",          "search-cursor-reply-threshold"},
  {"DEFAULT_DIALECT",                 "search-default-dialect"},
  {"EXTLOAD",                         "search-ext-load"},
  {"FORK_GC_CLEAN_THRESHOLD",         "search-fork-gc-clean-threshold"},
  {"FORK_GC_RETRY_INTERVAL",          "search-fork-gc-retry-interval"},
  {"FORK_GC_RUN_INTERVAL",            "search-fork-gc-run-interval"},
  {"FORKGC_SLEEP_BEFORE_EXIT",        "search-forkgc-sleep-before-exit"},
  {"FRISOINI",                        "search-friso-ini"},
  {"GC_POLICY",                       "search-gc-policy"},
  {"GCSCANSIZE",                      "search-gcscansize"},
  {"INDEX_CURSOR_LIMIT",              "search-index-cursor-limit"},
  {"MAXAGGREGATERESULTS",             "search-max-aggregate-results"},
  {"MAXDOCTABLESIZE",                 "search-maxdoctablesize"},
  {"MAXPREFIXEXPANSIONS",             "search-maxprefixexpansions"},
  {"MAXSEARCHRESULTS",                "search-maxsearchresults"},
  {"MIN_OPERATION_WORKERS",           "search-min-operation-workers"},
  {"MIN_PHONETIC_TERM_LEN",           "search-min-phonetic-term-len"},
  {"MINPREFIX",                       "search-minprefix"},
  {"MINSTEMLEN",                      "search-minstemlen"},
  {"NO_MEM_POOLS",                    "search-no-mem-pools"},
  {"NOGC",                            "search-nogc"},
  {"ON_TIMEOUT",                      "search-on-timeout"},
  {"MULTI_TEXT_SLOP",                 "search-multi-text-slop"},
  {"PARTIAL_INDEXED_DOCS",            "search-partial-indexed-docs"},
  {"RAW_DOCID_ENCODING",              "search-raw-docid-encoding"},
  {"SEARCH_THREADS",                  "search-search-threads"},
  {"TIERED_HNSW_BUFFER_LIMIT",        "search-tiered-hnsw-buffer-limit"},
  {"TIMEOUT",                         "search-timeout"},
  {"TOPOLOGY_VALIDATION_TIMEOUT",     "search-topology-validation-timeout"},
  {"UNION_ITERATOR_HEAP",             "search-union_iterator-heap"},
  {"VSS_MAX_RESIZE",                  "search-vss-max-resize"},
  {"WORKERS",                         "search-workers"},
  {"WORKERS_PRIORITY_BIAS_THRESHOLD", "search-workers-priority-bias-threshold"},
};

static const char* FTConfigNameToConfigName(const char *name) {
  size_t num_configs = sizeof(__configPairs) / sizeof(configPair_t);
  for (size_t i = 0; i < num_configs; ++i) {
    if (!strcasecmp(__configPairs[i].FTConfigName, name)) {
      return __configPairs[i].ConfigName;
    }
  }
  return NULL;
}

int set_numeric_config(const char *name, long long val, void *privdata,
                  RedisModuleString **err) {
  REDISMODULE_NOT_USED(name);
  REDISMODULE_NOT_USED(err);
  *(long long *)privdata = val;
  return REDISMODULE_OK;
}

long long get_numeric_config(const char *name, void *privdata) {
  REDISMODULE_NOT_USED(name);
  return (*(long long *)privdata);
}

int set_uint_numeric_config(const char *name, long long val,
                           void *privdata, RedisModuleString **err) {
  REDISMODULE_NOT_USED(name);
  REDISMODULE_NOT_USED(err);
  *(unsigned int *)privdata = (unsigned int) val;
  return REDISMODULE_OK;
}

long long get_uint_numeric_config(const char *name, void *privdata) {
  REDISMODULE_NOT_USED(name);
  return (long long)(*(unsigned int *)privdata);
}

int set_bool_config(const char *name, int val, void *privdata,
                    RedisModuleString **err) {
  REDISMODULE_NOT_USED(name);
  REDISMODULE_NOT_USED(err);
  *(int *)privdata = val;
  return REDISMODULE_OK;
}

int set_inverted_bool_config(const char *name, int val, void *privdata,
                             RedisModuleString **err) {
  REDISMODULE_NOT_USED(name);
  REDISMODULE_NOT_USED(err);
  *(int *)privdata = (val == 0);
  return REDISMODULE_OK;
}

int get_bool_config(const char *name, void *privdata) {
  REDISMODULE_NOT_USED(name);
  return *(int *)privdata;
}

int get_inverted_bool_config(const char *name, void *privdata) {
  REDISMODULE_NOT_USED(name);
  return !*(int *)privdata;
}

int set_immutable_string_config(const char *name, RedisModuleString *val, void *privdata,
                      RedisModuleString **err) {
  REDISMODULE_NOT_USED(name);
  REDISMODULE_NOT_USED(err);
  char **ptr = (char **)privdata;
  size_t len;
  const char *ret = RedisModule_StringPtrLen(val, &len);
  *ptr = rm_strndup(ret, len);
  return REDISMODULE_OK;
}

// EXTLOAD
CONFIG_SETTER(setExtLoad) {
  if (config->extLoad) {
    rm_free((void *)config->extLoad);
    config->extLoad = NULL;
  }
  int acrc = AC_GetString(ac, &config->extLoad, NULL, 0);
  RETURN_STATUS(acrc);
}

CONFIG_GETTER(getExtLoad) {
  if (config->extLoad && strlen(config->extLoad) > 0) {
    return sdsnew(config->extLoad);
  } else {
    return NULL;
  }
}

// ext-load
RedisModuleString* get_ext_load(const char *name, void *privdata) {
  REDISMODULE_NOT_USED(name);
  char *str = *(char **)privdata;
  if (str == NULL) {
    return NULL;
  }
  if (config_ext_load) {
    RedisModule_FreeString(NULL, config_ext_load);
  }
  config_ext_load = RedisModule_CreateString(NULL, str, strlen(str));
  return config_ext_load;
}

// NOGC
CONFIG_SETTER(setNoGc) {
  config->gcConfigParams.enableGC = 0;
  return REDISMODULE_OK;
}

CONFIG_BOOLEAN_GETTER(getNoGc, gcConfigParams.enableGC, 1)

// NO_MEM_POOLS
CONFIG_SETTER(setNoMemPools) {
  config->noMemPool = 1;
  return REDISMODULE_OK;
}

CONFIG_BOOLEAN_GETTER(getNoMemPools, noMemPool, 0)

// MINPREFIX
CONFIG_SETTER(setMinPrefix) {
  int acrc = AC_GetLongLong(ac, &config->iteratorsConfigParams.minTermPrefix, AC_F_GE1);
  RETURN_STATUS(acrc);
}

CONFIG_GETTER(getMinPrefix) {
  sds ss = sdsempty();
  return sdscatprintf(ss, "%lld", config->iteratorsConfigParams.minTermPrefix);
}

// MINSTEMLEN
CONFIG_SETTER(setMinStemLen) {
  unsigned int minStemLen;
  int acrc = AC_GetUnsigned(ac, &minStemLen, AC_F_GE1);
  if (minStemLen < MIN_MIN_STEM_LENGTH) {
    QueryError_SetUserDataAgnosticErrorFmt(status, MIN_MIN_STEM_LENGTH, "Minimum stem length cannot be lower than %u", MIN_MIN_STEM_LENGTH);
    return REDISMODULE_ERR;
  }
  config->iteratorsConfigParams.minStemLength = minStemLen;
  RETURN_STATUS(acrc);
}

CONFIG_GETTER(getMinStemLen) {
  sds ss = sdsempty();
  return sdscatprintf(ss, "%u", config->iteratorsConfigParams.minStemLength);
}

// FORKGC_SLEEP_BEFORE_EXIT
CONFIG_SETTER(setForkGCSleep) {
  int acrc = AC_GetSize(ac, &config->gcConfigParams.forkGc.forkGcSleepBeforeExit, AC_F_GE0);
  RETURN_STATUS(acrc);
}

CONFIG_GETTER(getForkGCSleep) {
  sds ss = sdsempty();
  return sdscatprintf(ss, "%zu", config->gcConfigParams.forkGc.forkGcSleepBeforeExit);
}

// MAXDOCTABLESIZE
CONFIG_SETTER(setMaxDocTableSize) {
  size_t newsize = 0;
  int acrc = AC_GetSize(ac, &newsize, AC_F_GE1);
  CHECK_RETURN_PARSE_ERROR(acrc)
  if (newsize > MAX_DOC_TABLE_SIZE) {
    QueryError_SetError(status, QUERY_ELIMIT, "Value exceeds maximum possible document table size");
    return REDISMODULE_ERR;
  }
  config->maxDocTableSize = newsize;
  return REDISMODULE_OK;
}

CONFIG_GETTER(getMaxDocTableSize) {
  sds ss = sdsempty();
  return sdscatprintf(ss, "%lu", config->maxDocTableSize);
}

// MAXSEARCHRESULTS
CONFIG_SETTER(setMaxSearchResults) {
  long long newSize = 0;
  int acrc = AC_GetLongLong(ac, &newSize, 0);
  CHECK_RETURN_PARSE_ERROR(acrc)
  if (newSize < 0) {
    newSize = MAX_SEARCH_REQUEST_RESULTS;
  } else {
    newSize = MIN(newSize, MAX_SEARCH_REQUEST_RESULTS);
  }
  config->maxSearchResults = newSize;
  return REDISMODULE_OK;
}

CONFIG_GETTER(getMaxSearchResults) {
  sds ss = sdsempty();
  if (config->maxSearchResults == MAX_SEARCH_REQUEST_RESULTS) {
    return sdscatprintf(ss, "unlimited");
  }
  return sdscatprintf(ss, "%lu", config->maxSearchResults);
}

// MAXAGGREGATERESULTS
CONFIG_SETTER(setMaxAggregateResults) {
  long long newSize = 0;
  int acrc = AC_GetLongLong(ac, &newSize, 0);
  CHECK_RETURN_PARSE_ERROR(acrc)
  if (newSize < 0) {
    newSize = MAX_AGGREGATE_REQUEST_RESULTS;
  } else {
    newSize = MIN(newSize, MAX_AGGREGATE_REQUEST_RESULTS);
  }
  config->maxAggregateResults = newSize;
  return REDISMODULE_OK;
}

CONFIG_GETTER(getMaxAggregateResults) {
  sds ss = sdsempty();
  if (config->maxAggregateResults == MAX_AGGREGATE_REQUEST_RESULTS) {
    return sdscatprintf(ss, "unlimited");
  }
  return sdscatprintf(ss, "%lu", config->maxAggregateResults);
}

// MAXEXPANSIONS MAXPREFIXEXPANSIONS
CONFIG_SETTER(setMaxExpansions) {
  int acrc = AC_GetLongLong(ac, &config->iteratorsConfigParams.maxPrefixExpansions, AC_F_GE1);
  RETURN_STATUS(acrc);
}

CONFIG_GETTER(getMaxExpansions) {
  sds ss = sdsempty();
  return sdscatprintf(ss, "%llu", config->iteratorsConfigParams.maxPrefixExpansions);
}

// TIMEOUT
CONFIG_SETTER(setTimeout) {
  int acrc = AC_GetLongLong(ac, &config->requestConfigParams.queryTimeoutMS, AC_F_GE0);
  RETURN_STATUS(acrc);
}

CONFIG_GETTER(getTimeout) {
  sds ss = sdsempty();
  return sdscatprintf(ss, "%lld", config->requestConfigParams.queryTimeoutMS);
}

// We limit the number of worker threads to limit the amount of memory used by the thread pool
// and to prevent the system from running out of resources.
// The number of worker threads should be proportional to the number of cores in the system at most,
// otherwise no performance improvement will be achieved.
#ifndef MAX_WORKER_THREADS
#define MAX_WORKER_THREADS (1 << 4)
#endif

static inline int errorTooManyThreads(QueryError *status) {
  QueryError_SetUserDataAgnosticErrorFmt(status, QUERY_ELIMIT, "Number of worker threads cannot exceed %d", MAX_WORKER_THREADS);
  return REDISMODULE_ERR;
}

// WORKERS
CONFIG_SETTER(setWorkThreads) {
  size_t newNumThreads;
  int acrc = AC_GetSize(ac, &newNumThreads, AC_F_GE0);
  CHECK_RETURN_PARSE_ERROR(acrc);
  if (newNumThreads > MAX_WORKER_THREADS) {
    return errorTooManyThreads(status);
  }
  config->numWorkerThreads = newNumThreads;

  workersThreadPool_SetNumWorkers();
  // Trigger the connection per shard to be updated (only if we are in coordinator mode)
  COORDINATOR_TRIGGER();
  return REDISMODULE_OK;
}

CONFIG_GETTER(getWorkThreads) {
  sds ss = sdsempty();
  return sdscatprintf(ss, "%lu", config->numWorkerThreads);
}

// workers
int set_workers(const char *name, long long val, void *privdata,
RedisModuleString **err) {
  uint32_t externalTriggerId = 0;
  RSConfig *config = (RSConfig *)privdata;
  config->numWorkerThreads = val;
  workersThreadPool_SetNumWorkers();
  // Trigger the connection per shard to be updated (only if we are in coordinator mode)
  COORDINATOR_TRIGGER();
  return REDISMODULE_OK;
}

long long get_workers(const char *name, void *privdata) {
  RSConfig *config = (RSConfig *)privdata;
  return config->numWorkerThreads;
}

// MIN_OPERATION_WORKERS
CONFIG_SETTER(setMinOperationWorkers) {
  size_t newNumThreads;
  int acrc = AC_GetSize(ac, &newNumThreads, AC_F_GE0);
  CHECK_RETURN_PARSE_ERROR(acrc);
  if (newNumThreads > MAX_WORKER_THREADS) {
    return errorTooManyThreads(status);
  }
  config->minOperationWorkers = newNumThreads;
  // Will only change the number of workers if we are in an event,
  // and `numWorkerThreads` is less than `minOperationWorkers`.
  workersThreadPool_SetNumWorkers();
  return REDISMODULE_OK;
}

CONFIG_GETTER(getMinOperationWorkers) {
  sds ss = sdsempty();
  return sdscatprintf(ss, "%lu", config->minOperationWorkers);
}

// min-operation-workers
int set_min_operation_workers(const char *name,
                      long long val, void *privdata, RedisModuleString **err) {
  REDISMODULE_NOT_USED(name);
  REDISMODULE_NOT_USED(err);
  *(size_t *)privdata = (size_t) val;
  // Will only change the number of workers if we are in an event,
  // and `numWorkerThreads` is less than `minOperationWorkers`.
  workersThreadPool_SetNumWorkers();
  return REDISMODULE_OK;
}

long long get_min_operation_workers(const char *name, void *privdata) {
  REDISMODULE_NOT_USED(name);
  return (long long) (*(size_t *)privdata);
}

/************************************ DEPRECATION CANDIDATES *************************************/

enum MTMode {
  MT_MODE_OFF,
  MT_MODE_ONLY_ON_OPERATIONS,
  MT_MODE_FULL,
};

// Old configuration
enum MTMode mt_mode_config = MT_MODE_OFF;
size_t numWorkerThreads_config = 0;

// WORKER_THREADS
CONFIG_SETTER(setDeprWorkThreads) {
  RedisModule_Log(RSDummyContext, "warning", "MT_MODE and WORKER_THREADS are deprecated, use WORKERS and MIN_OPERATION_WORKERS instead");
  size_t newNumThreads;
  int acrc = AC_GetSize(ac, &newNumThreads, AC_F_GE0);
  CHECK_RETURN_PARSE_ERROR(acrc);
  if (newNumThreads > MAX_WORKER_THREADS) {
    QueryError_SetUserDataAgnosticErrorFmt(status, QUERY_ELIMIT, "Number of worker threads cannot exceed %d", MAX_WORKER_THREADS);
    return REDISMODULE_ERR;
  }
  numWorkerThreads_config = newNumThreads;
  return REDISMODULE_OK;
}

CONFIG_GETTER(getDeprWorkThreads) {
  RedisModule_Log(RSDummyContext, "warning", "MT_MODE and WORKER_THREADS are deprecated, use WORKERS and MIN_OPERATION_WORKERS instead");
  sds ss = sdsempty();
  size_t numThreads;
  switch (mt_mode_config) {
  case MT_MODE_OFF:
    numThreads = 0;
    break;
  case MT_MODE_ONLY_ON_OPERATIONS:
    numThreads = config->minOperationWorkers;
    break;
  case MT_MODE_FULL:
    numThreads = config->numWorkerThreads;
    break;
  }
  return sdscatprintf(ss, "%lu", numThreads);
}

// MT_MODE
CONFIG_SETTER(setMtMode) {
  RedisModule_Log(RSDummyContext, "warning", "MT_MODE and WORKER_THREADS are deprecated, use WORKERS and MIN_OPERATION_WORKERS instead");
  const char *mt_mode;
  int acrc = AC_GetString(ac, &mt_mode, NULL, 0);
  CHECK_RETURN_PARSE_ERROR(acrc);
  if (!strcasecmp(mt_mode, "MT_MODE_OFF")) {
    mt_mode_config = MT_MODE_OFF;
  } else if (!strcasecmp(mt_mode, "MT_MODE_ONLY_ON_OPERATIONS")){
    mt_mode_config = MT_MODE_ONLY_ON_OPERATIONS;
  } else if (!strcasecmp(mt_mode, "MT_MODE_FULL")){
    mt_mode_config = MT_MODE_FULL;
  } else {
    QueryError_SetError(status, QUERY_EPARSEARGS, "Invalie MT mode");
    return REDISMODULE_ERR;
  }
  return REDISMODULE_OK;
}

static inline const char *MTMode_ToString(enum MTMode mt_mode) {
  switch (mt_mode) {
    case MT_MODE_OFF:
      return "MT_MODE_OFF";
    case MT_MODE_ONLY_ON_OPERATIONS:
      return "MT_MODE_ONLY_ON_OPERATIONS";
    case MT_MODE_FULL:
      return "MT_MODE_FULL";
  }
}

CONFIG_GETTER(getMtMode) {
  RedisModule_Log(RSDummyContext, "warning", "MT_MODE and WORKER_THREADS are deprecated, use WORKERS and MIN_OPERATION_WORKERS instead");
  return sdsnew(MTMode_ToString(mt_mode_config));
}

/********************************* END OF DEPRECATION CANDIDATES *********************************/

// TIERED_HNSW_BUFFER_LIMIT
CONFIG_SETTER(setTieredIndexBufferLimit) {
  int acrc = AC_GetSize(ac, &config->tieredVecSimIndexBufferLimit, AC_F_GE0);
  RETURN_STATUS(acrc);
}

CONFIG_GETTER(getTieredIndexBufferLimit) {
  sds ss = sdsempty();
  return sdscatprintf(ss, "%lu", config->tieredVecSimIndexBufferLimit);
}

// WORKERS_PRIORITY_BIAS_THRESHOLD
CONFIG_SETTER(setHighPriorityBiasNum) {
  int acrc = AC_GetSize(ac, &config->highPriorityBiasNum, AC_F_GE0);
  RETURN_STATUS(acrc);
}

CONFIG_GETTER(getHighPriorityBiasNum) {
  sds ss = sdsempty();
  return sdscatprintf(ss, "%lu", config->highPriorityBiasNum);
}

// PRIVILEGED_THREADS_NUM
CONFIG_SETTER(setPrivilegedThreadsNum) {
  RedisModule_Log(RSDummyContext, "warning", "PRIVILEGED_THREADS_NUM is deprecated. Setting WORKERS_PRIORITY_BIAS_THRESHOLD instead.");
  return setHighPriorityBiasNum(config, ac, -1, status);
}

// FRISOINI
CONFIG_SETTER(setFrisoINI) {
  if(config->frisoIni) {
    rm_free((void *) config->frisoIni);
    config->frisoIni = NULL;
  }
  int acrc = AC_GetString(ac, &config->frisoIni, NULL, 0);
  RETURN_STATUS(acrc);
}
CONFIG_GETTER(getFrisoINI) {
  if (config->frisoIni && strlen(config->frisoIni) > 0) {
    return sdsnew(config->frisoIni);
  } else {
    return NULL;
  }
}

// friso-ini
RedisModuleString * get_friso_ini(const char *name, void *privdata) {
  char *str = *(char **)privdata;
  if (str == NULL) {
    return NULL;
  }
  if (config_friso_ini) {
    RedisModule_FreeString(NULL, config_friso_ini);
  }
  config_friso_ini = RedisModule_CreateString(NULL, str, strlen(str));
  return config_friso_ini;
}

// ON_TIMEOUT
CONFIG_SETTER(setOnTimeout) {
  size_t len;
  const char *policy;
  int acrc = AC_GetString(ac, &policy, &len, 0);
  CHECK_RETURN_PARSE_ERROR(acrc);
  RSTimeoutPolicy top = TimeoutPolicy_Parse(policy, len);
  if (top == TimeoutPolicy_Invalid) {
    QueryError_SetError(status, QUERY_EBADVAL, "Invalid ON_TIMEOUT value");
    return REDISMODULE_ERR;
  }
  config->requestConfigParams.timeoutPolicy = top;
  return REDISMODULE_OK;
}

CONFIG_GETTER(getOnTimeout) {
  return sdsnew(TimeoutPolicy_ToString(config->requestConfigParams.timeoutPolicy));
}

// on-timeout
int set_on_timeout(const char *name, int val, void *privdata,
                   RedisModuleString **err) {
  REDISMODULE_NOT_USED(name);
  REDISMODULE_NOT_USED(err);
  *((RSTimeoutPolicy *)privdata) = (RSTimeoutPolicy)val;
  return REDISMODULE_OK;
}

int get_on_timeout(const char *name, void *privdata){
  REDISMODULE_NOT_USED(name);
  return *((RSTimeoutPolicy *)privdata);
}

// GC_SCANSIZE
CONFIG_SETTER(setGcScanSize) {
  int acrc = AC_GetSize(ac, &config->gcConfigParams.gcScanSize, AC_F_GE1);
  RETURN_STATUS(acrc);
}

CONFIG_GETTER(getGcScanSize) {
  sds ss = sdsempty();
  return sdscatprintf(ss, "%lu", config->gcConfigParams.gcScanSize);
}

// FORK_GC_RUN_INTERVAL
CONFIG_SETTER(setForkGcInterval) {
  int acrc = AC_GetSize(ac, &config->gcConfigParams.forkGc.forkGcRunIntervalSec, AC_F_GE1);
  RETURN_STATUS(acrc);
}

CONFIG_GETTER(getForkGcInterval) {
  sds ss = sdsempty();
  return sdscatprintf(ss, "%lu", config->gcConfigParams.forkGc.forkGcRunIntervalSec);
}

// FORK_GC_CLEAN_THRESHOLD
CONFIG_SETTER(setForkGcCleanThreshold) {
  int acrc = AC_GetSize(ac, &config->gcConfigParams.forkGc.forkGcCleanThreshold, 0);
  RETURN_STATUS(acrc);
}

CONFIG_GETTER(getForkGcCleanThreshold) {
  sds ss = sdsempty();
  return sdscatprintf(ss, "%lu", config->gcConfigParams.forkGc.forkGcCleanThreshold);
}

// FORK_GC_RETRY_INTERVAL
CONFIG_SETTER(setForkGcRetryInterval) {
  int acrc = AC_GetSize(ac, &config->gcConfigParams.forkGc.forkGcRetryInterval, AC_F_GE1);
  RETURN_STATUS(acrc);
}

CONFIG_GETTER(getForkGcRetryInterval) {
  sds ss = sdsempty();
  return sdscatprintf(ss, "%lu", config->gcConfigParams.forkGc.forkGcRetryInterval);
}

// UNION_ITERATOR_HEAP
CONFIG_SETTER(setMinUnionIteratorHeap) {
  int acrc = AC_GetLongLong(ac, &config->iteratorsConfigParams.minUnionIterHeap, AC_F_GE1);
  RETURN_STATUS(acrc);
}

CONFIG_GETTER(getMinUnionIteratorHeap) {
  sds ss = sdsempty();
  return sdscatprintf(ss, "%lld", config->iteratorsConfigParams.minUnionIterHeap);
}

// CURSOR_MAX_IDLE
CONFIG_SETTER(setCursorMaxIdle) {
  int acrc = AC_GetLongLong(ac, &config->cursorMaxIdle, AC_F_GE1);
  RETURN_STATUS(acrc);
}

CONFIG_GETTER(getCursorMaxIdle) {
  sds ss = sdsempty();
  return sdscatprintf(ss, "%lld", config->cursorMaxIdle);
}

// FORK_GC_CLEAN_NUMERIC_EMPTY_NODES
CONFIG_SETTER(setForkGCCleanNumericEmptyNodes) {
  config->gcConfigParams.forkGc.forkGCCleanNumericEmptyNodes = 1;
  return REDISMODULE_OK;
}

CONFIG_BOOLEAN_GETTER(getForkGCCleanNumericEmptyNodes, gcConfigParams.forkGc.forkGCCleanNumericEmptyNodes, 0)

// _FORK_GC_CLEAN_NUMERIC_EMPTY_NODES
CONFIG_BOOLEAN_SETTER(set_ForkGCCleanNumericEmptyNodes, gcConfigParams.forkGc.forkGCCleanNumericEmptyNodes)
CONFIG_BOOLEAN_GETTER(get_ForkGCCleanNumericEmptyNodes, gcConfigParams.forkGc.forkGCCleanNumericEmptyNodes, 0)

// MIN_PHONETIC_TERM_LEN
CONFIG_SETTER(setMinPhoneticTermLen) {
  int acrc = AC_GetSize(ac, &config->minPhoneticTermLen, AC_F_GE1);
  RETURN_STATUS(acrc);
}

CONFIG_GETTER(getMinPhoneticTermLen) {
  sds ss = sdsempty();
  return sdscatprintf(ss, "%lu", config->minPhoneticTermLen);
}

// _NUMERIC_COMPRESS
CONFIG_BOOLEAN_SETTER(setNumericCompress, numericCompress)
CONFIG_BOOLEAN_GETTER(getNumericCompress, numericCompress, 0)

// _FREE_RESOURCE_ON_THREAD
CONFIG_BOOLEAN_SETTER(setFreeResourcesThread, freeResourcesThread)
CONFIG_BOOLEAN_GETTER(getFreeResourcesThread, freeResourcesThread, 0)

// _PRINT_PROFILE_CLOCK
CONFIG_BOOLEAN_SETTER(setPrintProfileClock, requestConfigParams.printProfileClock)
CONFIG_BOOLEAN_GETTER(getPrintProfileClock, requestConfigParams.printProfileClock, 0)

// RAW_DOCID_ENCODING
CONFIG_BOOLEAN_SETTER(setRawDocIDEncoding, invertedIndexRawDocidEncoding)
CONFIG_BOOLEAN_GETTER(getRawDocIDEncoding, invertedIndexRawDocidEncoding, 0)

// _NUMERIC_RANGES_PARENTS
CONFIG_SETTER(setNumericTreeMaxDepthRange) {
  size_t maxDepthRange;
  int acrc = AC_GetSize(ac, &maxDepthRange, AC_F_GE0);
  // Prevent rebalancing/rotating of nodes with ranges since we use highest node with range.
  if (maxDepthRange > NR_MAX_DEPTH_BALANCE) {
    QueryError_SetError(status, QUERY_EPARSEARGS, "Max depth for range cannot be higher "
                                                  "than max depth for balance");
    return REDISMODULE_ERR;
  }
  config->numericTreeMaxDepthRange = maxDepthRange;
  RETURN_STATUS(acrc);
}

CONFIG_GETTER(getNumericTreeMaxDepthRange) {
  sds ss = sdsempty();
  return sdscatprintf(ss, "%ld", config->numericTreeMaxDepthRange);
}

// DEFAULT_DIALECT
CONFIG_SETTER(setDefaultDialectVersion) {
  unsigned int dialectVersion;
  int acrc = AC_GetUnsigned(ac, &dialectVersion, AC_F_GE1);
  if (dialectVersion > MAX_DIALECT_VERSION) {
    QueryError_SetUserDataAgnosticErrorFmt(status, MAX_DIALECT_VERSION, "Default dialect version cannot be higher than %u", MAX_DIALECT_VERSION);
    return REDISMODULE_ERR;
  }
  config->requestConfigParams.dialectVersion = dialectVersion;
  RETURN_STATUS(acrc);
}

CONFIG_GETTER(getDefaultDialectVersion) {
  sds ss = sdsempty();
  return sdscatprintf(ss, "%u", config->requestConfigParams.dialectVersion);
}

// VSS_MAX_RESIZE
CONFIG_SETTER(setVSSMaxResize) {
  size_t resize;
  int acrc = AC_GetSize(ac, &resize, AC_F_GE0);
  config->vssMaxResize = resize;
  RETURN_STATUS(acrc);
}

CONFIG_GETTER(getVSSMaxResize) {
  sds ss = sdsempty();
  return sdscatprintf(ss, "%u", config->vssMaxResize);
}

// MULTI_TEXT_SLOP
CONFIG_SETTER(setMultiTextOffsetDelta) {
  int acrc = AC_GetUnsigned(ac, &config->multiTextOffsetDelta, AC_F_GE0);
  RETURN_STATUS(acrc);
}

CONFIG_GETTER(getMultiTextOffsetDelta) {
  sds ss = sdsempty();
  return sdscatprintf(ss, "%u", config->multiTextOffsetDelta);
}

CONFIG_SETTER(setGcPolicy) {
  const char *policy;
  int acrc = AC_GetString(ac, &policy, NULL, 0);
  CHECK_RETURN_PARSE_ERROR(acrc);
  if (!strcasecmp(policy, "DEFAULT") || !strcasecmp(policy, "FORK")) {
    config->gcConfigParams.gcPolicy = GCPolicy_Fork;
  } else if (!strcasecmp(policy, "LEGACY")) {
    QueryError_SetError(status, QUERY_EPARSEARGS, "Legacy GC policy is no longer supported (since 2.6.0)");
    return REDISMODULE_ERR;
  } else {
    QueryError_SetError(status, QUERY_EPARSEARGS, "Invalid GC Policy value");
    return REDISMODULE_ERR;
  }
  return REDISMODULE_OK;
}

CONFIG_GETTER(getGcPolicy) {
  return sdsnew(GCPolicy_ToString(config->gcConfigParams.gcPolicy));
}

// PARTIAL_INDEXED_DOCS
CONFIG_SETTER(setFilterCommand) {
  int acrc = AC_GetInt(ac, &config->filterCommands, AC_F_GE0);
  RETURN_STATUS(acrc);
}

CONFIG_BOOLEAN_GETTER(getFilterCommand, filterCommands, 0)

// UPGRADE_INDEX
CONFIG_SETTER(setUpgradeIndex) {
  size_t dummy2;
  const char *rawIndexName;
  SchemaRuleArgs *rule = NULL;
  size_t len;
  int acrc = AC_GetString(ac, &rawIndexName, &len, 0);

  if (acrc != AC_OK) {
    QueryError_SetError(status, QUERY_EPARSEARGS, "Index name was not given to upgrade argument");
    return REDISMODULE_ERR;
  }

  // We aren't taking ownership on the string we got from the user, less cost memory-wise
  HiddenString *indexName = NewHiddenString(rawIndexName, len, false);
  if (dictFetchValue(legacySpecRules, indexName)) {
    HiddenString_Free(indexName, false);
    QueryError_SetError(status, QUERY_EPARSEARGS,
                        "Upgrade index definition was given more then once on the same index");
    return REDISMODULE_ERR;
  }

  rule = rm_calloc(1, sizeof(*rule));

  ArgsCursor rule_prefixes = {0};

  ACArgSpec argopts[] = {
      SPEC_FOLLOW_HASH_ARGS_DEF(rule){.name = NULL},
  };

  ACArgSpec *errarg = NULL;
  int rc = AC_ParseArgSpec(ac, argopts, &errarg);
  // AC_ERR_ENOENT is OK it means that we got the next configuration element
  // and we can stop
  if (rc != AC_OK && rc != AC_ERR_ENOENT) {
    if (rc != AC_ERR_ENOENT) {
      QERR_MKBADARGS_AC(status, errarg->name, rc);
      rm_free(rule);
      HiddenString_Free(indexName, false);
      return REDISMODULE_ERR;
    }
  }

  if (rule_prefixes.argc > 0) {
    rule->nprefixes = rule_prefixes.argc;
    rule->prefixes = rm_malloc(rule->nprefixes * sizeof(char *));
    for (size_t i = 0; i < rule->nprefixes; ++i) {
      rule->prefixes[i] = rm_strdup(RedisModule_StringPtrLen(rule_prefixes.objs[i], NULL));
    }
  } else {
    rule->nprefixes = 1;
    rule->prefixes = rm_malloc(sizeof(char *));
    rule->prefixes[0] = rm_strdup("");
  }

  // duplicate all rule arguments so it will leave after this function finish
#define DUP_IF_NEEDED(arg) \
  if (arg) arg = rm_strdup(arg)
  DUP_IF_NEEDED(rule->filter_exp_str);
  DUP_IF_NEEDED(rule->lang_default);
  DUP_IF_NEEDED(rule->lang_field);
  DUP_IF_NEEDED(rule->payload_field);
  DUP_IF_NEEDED(rule->score_default);
  DUP_IF_NEEDED(rule->score_field);
  rule->type = rm_strdup(RULE_TYPE_HASH);

  // add rule to rules dictionary
  dictAdd(legacySpecRules, (void*)indexName, rule);
  HiddenString_Free(indexName, false);
  return REDISMODULE_OK;
}

CONFIG_GETTER(getUpgradeIndex) {
  return sdsnew("Upgrade config for upgrading");
}

// BG_INDEX_SLEEP_GAP
CONFIG_SETTER(setBGIndexSleepGap) {
  unsigned int sleep_gap;
  int acrc = AC_GetUnsigned(ac, &sleep_gap, AC_F_GE1);
  config->numBGIndexingIterationsBeforeSleep = sleep_gap;
  RETURN_STATUS(acrc);
}

CONFIG_GETTER(getBGIndexSleepGap) {
  sds ss = sdsempty();
  return sdscatprintf(ss, "%u", config->numBGIndexingIterationsBeforeSleep);
}

// _PRIORITIZE_INTERSECT_UNION_CHILDREN
CONFIG_BOOLEAN_SETTER(set_PrioritizeIntersectUnionChildren, prioritizeIntersectUnionChildren)
CONFIG_BOOLEAN_GETTER(get_PrioritizeIntersectUnionChildren, prioritizeIntersectUnionChildren, 0)

// INDEX_CURSOR_LIMIT
CONFIG_SETTER(setIndexCursorLimit) {
  int acrc = AC_GetLongLong(ac, &config->indexCursorLimit, AC_F_GE0);
  RETURN_STATUS(acrc);
}

CONFIG_GETTER(getIndexCursorLimit) {
  sds ss = sdsempty();
  return sdscatprintf(ss, "%lld", config->indexCursorLimit);
}

RSConfig RSGlobalConfig = RS_DEFAULT_CONFIG;

static RSConfigVar *findConfigVar(const RSConfigOptions *config, const char *name) {
  for (; config; config = config->next) {
    const RSConfigVar *vars = config->vars;
    for (; vars->name != NULL; vars++) {
      if (!strcasecmp(name, vars->name)) {
        return (RSConfigVar *)vars;
      }
    }
  }
  return NULL;
}

int ReadConfig(RedisModuleString **argv, int argc, char **err) {
  *err = NULL;
  QueryError status = {0};

  if (RedisModule_GetServerVersion) {   // for rstest
    RSGlobalConfig.serverVersion = RedisModule_GetServerVersion();
  }

  ArgsCursor ac = {0};
  ArgsCursor_InitRString(&ac, argv, argc);
  while (!AC_IsAtEnd(&ac)) {
    const char *name = AC_GetStringNC(&ac, NULL);
    RSConfigVar *curVar = findConfigVar(&RSGlobalConfigOptions, name);
    if (curVar == NULL) {
      rm_asprintf(err, "No such configuration option `%s`", name);
      return REDISMODULE_ERR;
    }
    if (curVar->setValue == NULL) {
      rm_asprintf(err, "%s: Option is read-only", name);
      return REDISMODULE_ERR;
    }

    // `triggerId` is set by the coordinator when it registers a trigger for a configuration.
    // If we don't have a coordinator or this configuration has no trigger, this value
    // is meaningless and should be ignored
    if (curVar->setValue(&RSGlobalConfig, &ac, curVar->triggerId, &status) != REDISMODULE_OK) {
      *err = rm_strdup(QueryError_GetUserError(&status));
      QueryError_ClearError(&status);
      return REDISMODULE_ERR;
    }
    // Mark the option as having been modified
    curVar->flags |= RSCONFIGVAR_F_MODIFIED;
    RedisModule_Log(RSDummyContext, "warning",
      "`%s` was set, but module arguments are deprecated, consider using CONFIG parameter `%s`",
      name, FTConfigNameToConfigName(name));
  }

  return REDISMODULE_OK;
}

RSConfigOptions RSGlobalConfigOptions = {
    .vars = {
        {.name = "EXTLOAD",
         .helpText = "Load extension scoring/expansion module",
         .setValue = setExtLoad,
         .getValue = getExtLoad,
         .flags = RSCONFIGVAR_F_IMMUTABLE},
        {.name = "NOGC",
         .helpText = "Disable garbage collection (for this process)",
         .setValue = setNoGc,
         .getValue = getNoGc,
         .flags = RSCONFIGVAR_F_FLAG | RSCONFIGVAR_F_IMMUTABLE},
        {.name = "MINPREFIX",
         .helpText = "Set the minimum prefix for expansions (`*`)",
         .setValue = setMinPrefix,
         .getValue = getMinPrefix},
        {.name = "MINSTEMLEN",
         .helpText = "Set the minimum word length to stem (default 4)",
         .setValue = setMinStemLen,
         .getValue = getMinStemLen},
        {.name = "FORKGC_SLEEP_BEFORE_EXIT",
         .helpText = "set the amount of seconds for the fork GC to sleep before exists, should "
                     "always be set to 0 (other then on tests).",
         .setValue = setForkGCSleep,
         .getValue = getForkGCSleep},
        {.name = "MAXDOCTABLESIZE",
         .helpText = "Maximum runtime document table size (for this process)",
         .setValue = setMaxDocTableSize,
         .getValue = getMaxDocTableSize,
         .flags = RSCONFIGVAR_F_IMMUTABLE},
        {.name = "MAXSEARCHRESULTS",
         .helpText = "Maximum number of results from ft.search command",
         .setValue = setMaxSearchResults,
         .getValue = getMaxSearchResults},
        {.name = "MAXAGGREGATERESULTS",
         .helpText = "Maximum number of results from ft.aggregate command",
         .setValue = setMaxAggregateResults,
         .getValue = getMaxAggregateResults},
        {.name = "MAXEXPANSIONS",
         .helpText = "Maximum prefix expansions to be used in a query",
         .setValue = setMaxExpansions,
         .getValue = getMaxExpansions},
        {.name = "MAXPREFIXEXPANSIONS",
         .helpText = "Maximum prefix expansions to be used in a query",
         .setValue = setMaxExpansions,
         .getValue = getMaxExpansions},
        {.name = "TIMEOUT",
         .helpText = "Query (search) timeout",
         .setValue = setTimeout,
         .getValue = getTimeout},
        {.name = "WORKERS",
         .helpText = "Number of worker threads to use for query processing and background tasks. Default is 0."
                     " This configuration also affects the number of connections per shard. See CONN_PER_SHARD."
         ,
         .setValue = setWorkThreads,
         .getValue = getWorkThreads,
        },
        {.name = "MIN_OPERATION_WORKERS",
         .helpText = "Number of worker threads to use for background tasks when the server is in an operation event. "
                     "Default is " STRINGIFY(MIN_OPERATION_WORKERS),
         .setValue = setMinOperationWorkers,
         .getValue = getMinOperationWorkers,
        },
        {.name = "WORKER_THREADS",
         .helpText = "Deprecated, see WORKERS and MIN_OPERATION_WORKERS",
         .setValue = setDeprWorkThreads,
         .getValue = getDeprWorkThreads,
         .flags = RSCONFIGVAR_F_IMMUTABLE,
        },
        {.name = "MT_MODE",
         .helpText = "Deprecated, see WORKERS and MIN_OPERATION_WORKERS",
         .setValue = setMtMode,
         .getValue = getMtMode,
         .flags = RSCONFIGVAR_F_IMMUTABLE,
        },
        {.name = "TIERED_HNSW_BUFFER_LIMIT",
        .helpText = "Use for setting the buffer limit threshold for vector similarity tiered"
                    " HNSW index, so that if we are using WORKERS for indexing, and the"
                    " number of vectors waiting in the buffer to be indexed exceeds this limit,"
                    " we insert new vectors directly into HNSW",
        .setValue = setTieredIndexBufferLimit,
        .getValue = getTieredIndexBufferLimit,
        .flags = RSCONFIGVAR_F_IMMUTABLE,  // TODO: can this be mutable?
        },
        {.name = "PRIVILEGED_THREADS_NUM", // Deprecated alias of WORKERS_PRIORITY_BIAS_THRESHOLD
         .helpText = "Deprecated. See `WORKERS_PRIORITY_BIAS_THRESHOLD`",
         .setValue = setPrivilegedThreadsNum,
         .getValue = getHighPriorityBiasNum,
         .flags = RSCONFIGVAR_F_IMMUTABLE,  // TODO: can this be mutable?
        },
        {.name = "WORKERS_PRIORITY_BIAS_THRESHOLD",
         .helpText = "The number of high priority tasks to be executed at any given time by the "
                     "worker thread pool, before executing low priority tasks. After this number "
                     "of high priority tasks are being executed, the worker thread pool will execute "
                     "high and low priority tasks alternately.",
         .setValue = setHighPriorityBiasNum,
         .getValue = getHighPriorityBiasNum,
         .flags = RSCONFIGVAR_F_IMMUTABLE,  // TODO: can this be mutable?
        },
        {.name = "FRISOINI",
         .helpText = "Path to Chinese dictionary configuration file (for Chinese tokenization)",
         .setValue = setFrisoINI,
         .getValue = getFrisoINI,
         .flags = RSCONFIGVAR_F_IMMUTABLE},
        {.name = "ON_TIMEOUT",
         .helpText = "Action to perform when search timeout is exceeded (choose RETURN or FAIL)",
         .setValue = setOnTimeout,
         .getValue = getOnTimeout},
        {.name = "GCSCANSIZE",
         .helpText = "Scan this many documents at a time during every GC iteration",
         .setValue = setGcScanSize,
         .getValue = getGcScanSize},
        {.name = "MIN_PHONETIC_TERM_LEN",
         .helpText = "Minumum length of term to be considered for phonetic matching",
         .setValue = setMinPhoneticTermLen,
         .getValue = getMinPhoneticTermLen},
        {.name = "GC_POLICY",
         .helpText = "gc policy to use (DEFAULT/LEGACY)",
         .setValue = setGcPolicy,
         .getValue = getGcPolicy,
         .flags = RSCONFIGVAR_F_IMMUTABLE},
        {.name = "FORK_GC_RUN_INTERVAL",
         .helpText = "interval (in seconds) in which to run the fork gc (relevant only when fork "
                     "gc is used)",
         .setValue = setForkGcInterval,
         .getValue = getForkGcInterval},
        {.name = "FORK_GC_CLEAN_THRESHOLD",
         .helpText = "the fork gc will only start to clean when the number of not cleaned document "
                     "will exceed this threshold",
         .setValue = setForkGcCleanThreshold,
         .getValue = getForkGcCleanThreshold},
        {.name = "FORK_GC_RETRY_INTERVAL",
         .helpText = "interval (in seconds) in which to retry running the forkgc after failure.",
         .setValue = setForkGcRetryInterval,
         .getValue = getForkGcRetryInterval},
        {.name = "FORK_GC_CLEAN_NUMERIC_EMPTY_NODES",
         .helpText = "clean empty nodes from numeric tree",
         .setValue = setForkGCCleanNumericEmptyNodes,
         .getValue = getForkGCCleanNumericEmptyNodes,
         .flags = RSCONFIGVAR_F_FLAG},
        {.name = "_FORK_GC_CLEAN_NUMERIC_EMPTY_NODES",
         .helpText = "clean empty nodes from numeric tree",
         .setValue = set_ForkGCCleanNumericEmptyNodes,
         .getValue = get_ForkGCCleanNumericEmptyNodes},
        {.name = "UNION_ITERATOR_HEAP",
         .helpText = "minimum number of iterators in a union from which the interator will"
                     "switch to heap based implementation.",
         .setValue = setMinUnionIteratorHeap,
         .getValue = getMinUnionIteratorHeap},
        {.name = "CURSOR_MAX_IDLE",
         .helpText = "max idle time allowed to be set for cursor, setting it hight might cause "
                     "high memory consumption.",
         .setValue = setCursorMaxIdle,
         .getValue = getCursorMaxIdle},
        {.name = "INDEX_CURSOR_LIMIT",
         .helpText = "Max number of cursors for a given index that can be opened inside of a shard. Default is 128",
         .setValue = setIndexCursorLimit,
         .getValue = getIndexCursorLimit},
        {.name = "NO_MEM_POOLS",
         .helpText = "Set RediSearch to run without memory pools",
         .setValue = setNoMemPools,
         .getValue = getNoMemPools,
         .flags = RSCONFIGVAR_F_FLAG | RSCONFIGVAR_F_IMMUTABLE},
        {.name = "PARTIAL_INDEXED_DOCS",
         .helpText = "Enable commands filter which optimize indexing on partial hash updates",
         .setValue = setFilterCommand,
         .getValue = getFilterCommand,
         .flags = RSCONFIGVAR_F_IMMUTABLE},
        {.name = "UPGRADE_INDEX",
         .helpText =
             "Relevant only when loading an v1.x rdb, specify argument for upgrading the index.",
         .setValue = setUpgradeIndex,
         .getValue = getUpgradeIndex,
         .flags = RSCONFIGVAR_F_IMMUTABLE},
        {.name = "_NUMERIC_COMPRESS",
         .helpText = "Enable legacy compression of double to float.",
         .setValue = setNumericCompress,
         .getValue = getNumericCompress},
        {.name = "_FREE_RESOURCE_ON_THREAD",
         .helpText = "Determine whether some index resources are free on a second thread.",
         .setValue = setFreeResourcesThread,
         .getValue = getFreeResourcesThread},
        {.name = "_PRINT_PROFILE_CLOCK",
         .helpText = "Disable print of time for ft.profile. For testing only.",
         .setValue = setPrintProfileClock,
         .getValue = getPrintProfileClock},
        {.name = "RAW_DOCID_ENCODING",
         .helpText = "Disable compression for DocID inverted index. Boost CPU performance.",
         .setValue = setRawDocIDEncoding,
         .getValue = getRawDocIDEncoding,
         .flags = RSCONFIGVAR_F_IMMUTABLE},
        {.name = "_NUMERIC_RANGES_PARENTS",
         .helpText = "Keep numeric ranges in numeric tree parent nodes of leafs "
                     "for `x` generations.",
         .setValue = setNumericTreeMaxDepthRange,
         .getValue = getNumericTreeMaxDepthRange},
        {.name = "DEFAULT_DIALECT",
         .helpText = "Set RediSearch default dialect version throught the lifetime of the server.",
         .setValue = setDefaultDialectVersion,
         .getValue = getDefaultDialectVersion},
        {.name = "VSS_MAX_RESIZE",
         .helpText = "Set RediSearch vector indexes max resize (in bytes).",
         .setValue = setVSSMaxResize,
         .getValue = getVSSMaxResize},
         {.name = "MULTI_TEXT_SLOP",
         .helpText = "Set RediSearch delta used to increase positional offsets between array slots for multi text values."
                      "Can control the level of separation between phrases in different array slots (related to the SLOP parameter of ft.search command)",
         .setValue = setMultiTextOffsetDelta,
         .getValue = getMultiTextOffsetDelta,
         .flags = RSCONFIGVAR_F_IMMUTABLE},
        {.name = "BG_INDEX_SLEEP_GAP",
         .helpText = "The number of iterations to run while performing background indexing"
                     " before we call usleep(1) (sleep for 1 micro-second) and make sure that we"
                     " allow redis process other commands.",
         .setValue = setBGIndexSleepGap,
         .getValue = getBGIndexSleepGap,
         .flags = RSCONFIGVAR_F_IMMUTABLE},
        {.name = "_PRIORITIZE_INTERSECT_UNION_CHILDREN",
         .helpText = "Intersection iterator orders the children iterators by their relative estimated"
                     " number of results in ascending order, so that if we see first iterators with"
                     " a lower count of results we will skip a larger number of results, which"
                     " translates into faster iteration. If this flag is set, we use this"
                     " optimization in a way where union iterators are being factorize by the number"
                     " of their own children, so that we sort by the number of children times the "
                     "overall estimated number of results instead.",
         .setValue = set_PrioritizeIntersectUnionChildren,
         .getValue = get_PrioritizeIntersectUnionChildren},
        {.name = NULL}}};

void RSConfigOptions_AddConfigs(RSConfigOptions *src, RSConfigOptions *dst) {
  while (src->next != NULL) {
    src = src->next;
  }
  src->next = dst;
  dst->next = NULL;
}

void RSConfigExternalTrigger_Register(RSConfigExternalTrigger trigger, const char **configs) {
  static uint32_t numTriggers = 0;
  RS_LOG_ASSERT(numTriggers < RS_MAX_CONFIG_TRIGGERS, "Too many config triggers");
  for (const char *config = *configs; config; config = *++configs) {
    RSConfigVar *var = findConfigVar(&RSGlobalConfigOptions, config);
    var->triggerId = numTriggers;
  }
  RSGlobalConfigTriggers[numTriggers++] = trigger;
}

// Upgrade deprecated configurations if needed.
// Unless MT_MODE is OFF, only the relevant configuration is set, while the other keeps its default value.
void UpgradeDeprecatedMTConfigs() {
  RSConfigVar *mtMode = findConfigVar(&RSGlobalConfigOptions, "MT_MODE");
  RSConfigVar *workerThreads = findConfigVar(&RSGlobalConfigOptions, "WORKER_THREADS");
  if (!(mtMode->flags & RSCONFIGVAR_F_MODIFIED) && !(workerThreads->flags & RSCONFIGVAR_F_MODIFIED)) {
    return; // No deprecated configurations were set.
  }

  // We now know that deprecated configurations were set, and new configurations were not set.
  if ((mt_mode_config == MT_MODE_OFF && numWorkerThreads_config != 0) ||
      (mt_mode_config != MT_MODE_OFF && numWorkerThreads_config == 0)) {
    RedisModule_Log(RSDummyContext, "warning",
                    "Inconsistent configuration: MT_MODE `%s` and WORKER_THREADS `%lu`. Ignoring "
                    "the deprecated configurations.",
                    MTMode_ToString(mt_mode_config), numWorkerThreads_config);
    return; // Inconsistent configuration. Ignore the deprecated configurations.
  }

  RSConfigVar *workers = findConfigVar(&RSGlobalConfigOptions, "WORKERS");
  RSConfigVar *minOperationWorkers = findConfigVar(&RSGlobalConfigOptions, "MIN_OPERATION_WORKERS");
  bool explicit_workers = workers->flags & RSCONFIGVAR_F_MODIFIED;
  bool explicit_minOperationWorkers = minOperationWorkers->flags & RSCONFIGVAR_F_MODIFIED;

  // Set the new configurations based on the deprecated ones.
  // We know that at least one of the deprecated configurations was set.
  // If the new configurations were also set, ignore the deprecated ones.
  switch (mt_mode_config) {
    case MT_MODE_OFF:
      if (!explicit_workers) {
        RSGlobalConfig.numWorkerThreads = 0;
      }
      if (!explicit_minOperationWorkers) {
        RSGlobalConfig.minOperationWorkers = 0;
        RedisModule_Log(RSDummyContext, "warning",
                        "Setting `MIN_OPERATION_WORKERS` to 0 due to explicit `MT_MODE_OFF`, "
                        "overriding the default of " STRINGIFY(MIN_OPERATION_WORKERS));
      }
      break;
    case MT_MODE_FULL:
      if (!explicit_workers) {
        RSGlobalConfig.numWorkerThreads = numWorkerThreads_config;
      }
      break;
    case MT_MODE_ONLY_ON_OPERATIONS:
      if (!explicit_minOperationWorkers) {
        RSGlobalConfig.minOperationWorkers = numWorkerThreads_config;
      }
      break;
  }
}

sds RSConfig_GetInfoString(const RSConfig *config) {
  sds ss = sdsempty();

  ss = sdscatprintf(ss, "gc: %s, ", config->gcConfigParams.enableGC ? "ON" : "OFF");
  ss = sdscatprintf(ss, "prefix min length: %lld, ", config->iteratorsConfigParams.minTermPrefix);
  ss = sdscatprintf(ss, "min word length to stem: %u, ", config->iteratorsConfigParams.minStemLength);
  ss = sdscatprintf(ss, "prefix max expansions: %lld, ", config->iteratorsConfigParams.maxPrefixExpansions);
  ss = sdscatprintf(ss, "query timeout (ms): %lld, ", config->requestConfigParams.queryTimeoutMS);
  ss = sdscatprintf(ss, "timeout policy: %s, ", TimeoutPolicy_ToString(config->requestConfigParams.timeoutPolicy));
  ss = sdscatprintf(ss, "cursor read size: %lld, ", config->cursorReadSize);
  ss = sdscatprintf(ss, "cursor max idle (ms): %lld, ", config->cursorMaxIdle);
  ss = sdscatprintf(ss, "max doctable size: %lu, ", config->maxDocTableSize);
  ss = sdscatprintf(ss, "max number of search results: ");
  ss = (config->maxSearchResults == MAX_SEARCH_REQUEST_RESULTS)
           ?  // value for MaxSearchResults
           sdscatprintf(ss, "unlimited, ")
           : sdscatprintf(ss, " %lu, ", config->maxSearchResults);

  if (config->extLoad && strlen(config->extLoad) > 0) {
    ss = sdscatprintf(ss, "ext load: %s, ", config->extLoad);
  }

  if (config->frisoIni && strlen(config->frisoIni) > 0) {
    ss = sdscatprintf(ss, "friso ini: %s, ", config->frisoIni);
  }
  return ss;
}

static void dumpConfigOption(const RSConfig *config, const RSConfigVar *var, RedisModule_Reply *reply,
                             bool isHelp) {
  sds currValue = var->getValue(config);

  if (!reply->resp3) {
    RedisModule_Reply_Array(reply);
  }

  RedisModule_Reply_SimpleString(reply, var->name);

  if (isHelp) {
    if (reply->resp3) {
      RedisModule_Reply_Map(reply);
    }

    RedisModule_ReplyKV_SimpleString(reply, "Description", var->helpText);
    RedisModule_Reply_SimpleString(reply, "Value");
    if (currValue) {
      RedisModule_Reply_StringBuffer(reply, currValue, sdslen(currValue));
    } else {
      RedisModule_Reply_Null(reply);
    }

    if (reply->resp3) {
      RedisModule_Reply_MapEnd(reply);
    }
  } else {
    if (currValue) {
      RedisModule_Reply_StringBuffer(reply, currValue, sdslen(currValue));
    } else {
      RedisModule_Reply_Null(reply);
    }
  }

  sdsfree(currValue);
  if (!reply->resp3) {
    RedisModule_Reply_ArrayEnd(reply);
  }
}

void RSConfig_DumpProto(const RSConfig *config, const RSConfigOptions *options, const char *name,
                        RedisModule_Reply *reply, bool isHelp) {
  RedisModule_Reply_Map(reply);
    if (!strcmp("*", name)) {
      for (const RSConfigOptions *curOpts = options; curOpts; curOpts = curOpts->next) {
        for (const RSConfigVar *cur = &curOpts->vars[0]; cur->name; cur++) {
          dumpConfigOption(config, cur, reply, isHelp);
        }
      }
    } else {
      const RSConfigVar *v = findConfigVar(options, name);
      if (v) {
        dumpConfigOption(config, v, reply, isHelp);
      }
    }
  RedisModule_Reply_MapEnd(reply);
}

int RSConfig_SetOption(RSConfig *config, RSConfigOptions *options, const char *name,
                       RedisModuleString **argv, int argc, size_t *offset, QueryError *status) {
  RSConfigVar *var = findConfigVar(options, name);
  if (!var) {
    QueryError_SetError(status, QUERY_ENOOPTION, NULL);
    return REDISMODULE_ERR;
  }
  if (var->flags & RSCONFIGVAR_F_IMMUTABLE) {
    QueryError_SetError(status, QUERY_EINVAL, "Not modifiable at runtime");
    return REDISMODULE_ERR;
  }
  ArgsCursor ac;
  ArgsCursor_InitRString(&ac, argv + *offset, argc - *offset);
  int rc = var->setValue(config, &ac, var->triggerId, status);
  *offset += ac.offset;
  return rc;
}

const char *TimeoutPolicy_ToString(RSTimeoutPolicy policy) {
  switch (policy) {
    case TimeoutPolicy_Return:
      return on_timeout_vals[TimeoutPolicy_Return];
    case TimeoutPolicy_Fail:
      return on_timeout_vals[TimeoutPolicy_Fail];
    default:
      return "invalid";
  }
}

RSTimeoutPolicy TimeoutPolicy_Parse(const char *s, size_t n) {
  if (STR_EQCASE(s, n, on_timeout_vals[TimeoutPolicy_Return])) {
    return TimeoutPolicy_Return;
  } else if (STR_EQCASE(s, n, on_timeout_vals[TimeoutPolicy_Fail])) {
    return TimeoutPolicy_Fail;
  } else {
    return TimeoutPolicy_Invalid;
  }
}
void iteratorsConfig_init(IteratorsConfig *config) {
  *config = RSGlobalConfig.iteratorsConfigParams;
}


int RegisterModuleConfig(RedisModuleCtx *ctx) {
  // Numeric parameters
  RM_TRY(
    RedisModule_RegisterNumericConfig(
      ctx, "search-_numeric-ranges-parents", 0,
      REDISMODULE_CONFIG_DEFAULT | REDISMODULE_CONFIG_UNPREFIXED, 0,
      NR_MAX_DEPTH_BALANCE, get_numeric_config,
      set_numeric_config, NULL,
      (void *)&(RSGlobalConfig.numericTreeMaxDepthRange)
    )
  )

  RM_TRY(
    RedisModule_RegisterNumericConfig(
      ctx, "search-bg-index-sleep-gap", DEFAULT_BG_INDEX_SLEEP_GAP,
      REDISMODULE_CONFIG_IMMUTABLE | REDISMODULE_CONFIG_UNPREFIXED, 1,
      UINT32_MAX, get_uint_numeric_config, set_uint_numeric_config, NULL,
      (void *)&(RSGlobalConfig.numBGIndexingIterationsBeforeSleep)
  )
)

  RM_TRY(
    RedisModule_RegisterNumericConfig(
      ctx, "search-default-dialect", DEFAULT_DIALECT_VERSION,
      REDISMODULE_CONFIG_DEFAULT | REDISMODULE_CONFIG_UNPREFIXED,
      MIN_DIALECT_VERSION, MAX_DIALECT_VERSION,
      get_numeric_config, set_numeric_config, NULL,
      (void *)&(RSGlobalConfig.requestConfigParams.dialectVersion)
    )
  )

  RM_TRY(
    RedisModule_RegisterNumericConfig (
      ctx, "search-fork-gc-clean-threshold", DEFAULT_FORK_GC_CLEAN_THRESHOLD,
      REDISMODULE_CONFIG_DEFAULT | REDISMODULE_CONFIG_UNPREFIXED, 1,
      LLONG_MAX, get_numeric_config, set_numeric_config, NULL,
      (void *)&(RSGlobalConfig.gcConfigParams.forkGc.forkGcCleanThreshold)
    )
  )

  RM_TRY(
    RedisModule_RegisterNumericConfig (
      ctx, "search-fork-gc-retry-interval", DEFAULT_FORK_GC_RETRY_INTERVAL,
      REDISMODULE_CONFIG_DEFAULT | REDISMODULE_CONFIG_UNPREFIXED, 1,
      LLONG_MAX, get_numeric_config, set_numeric_config, NULL,
      (void *)&(RSGlobalConfig.gcConfigParams.forkGc.forkGcRetryInterval)
    )
  )

  RM_TRY(
    RedisModule_RegisterNumericConfig(
      ctx, "search-fork-gc-run-interval", DEFAULT_FORK_GC_RUN_INTERVAL,
      REDISMODULE_CONFIG_DEFAULT | REDISMODULE_CONFIG_UNPREFIXED, 1,
      LLONG_MAX, get_numeric_config, set_numeric_config, NULL,
      (void *)&(RSGlobalConfig.gcConfigParams.forkGc.forkGcRunIntervalSec)
    )
  )

  RM_TRY(
    RedisModule_RegisterNumericConfig(
      ctx, "search-fork-gc-sleep-before-exit", 0,
      REDISMODULE_CONFIG_DEFAULT | REDISMODULE_CONFIG_UNPREFIXED, 0,
      LLONG_MAX, get_numeric_config, set_numeric_config, NULL,
      (void *)&(RSGlobalConfig.gcConfigParams.forkGc.forkGcSleepBeforeExit)
    )
  )

  RM_TRY(
    RedisModule_RegisterNumericConfig(
      ctx, "search-gc-scan-size", DEFAULT_GC_SCANSIZE,
      REDISMODULE_CONFIG_IMMUTABLE | REDISMODULE_CONFIG_UNPREFIXED, 1,
      LLONG_MAX, get_numeric_config, set_numeric_config, NULL,
      (void *)&(RSGlobalConfig.gcConfigParams.gcScanSize)
    )
  )

  RM_TRY(
    RedisModule_RegisterNumericConfig(
      ctx, "search-index-cursor-limit", DEFAULT_INDEX_CURSOR_LIMIT,
      REDISMODULE_CONFIG_DEFAULT | REDISMODULE_CONFIG_UNPREFIXED, 0,
      LLONG_MAX, get_numeric_config, set_numeric_config, NULL,
      (void *)&(RSGlobalConfig.indexCursorLimit)
    )
  )

  RM_TRY(
    RedisModule_RegisterNumericConfig(
      ctx, "search-max-aggregate-results", DEFAULT_MAX_AGGREGATE_REQUEST_RESULTS,
      REDISMODULE_CONFIG_DEFAULT | REDISMODULE_CONFIG_UNPREFIXED, 0,
      MAX_AGGREGATE_REQUEST_RESULTS, get_numeric_config, set_numeric_config,
      NULL, (void *)&(RSGlobalConfig.maxAggregateResults)
    )
  )

  RM_TRY(
    RedisModule_RegisterNumericConfig(
      ctx, "search-max-prefix-expansions", DEFAULT_MAX_PREFIX_EXPANSIONS,
      REDISMODULE_CONFIG_DEFAULT | REDISMODULE_CONFIG_UNPREFIXED, 1,
      LLONG_MAX, get_numeric_config, set_numeric_config, NULL,
      (void *)&(RSGlobalConfig.iteratorsConfigParams.maxPrefixExpansions)
    )
  )

  RM_TRY(
    RedisModule_RegisterNumericConfig(
      ctx, "search-max-doctablesize", DEFAULT_DOC_TABLE_SIZE,
      REDISMODULE_CONFIG_IMMUTABLE | REDISMODULE_CONFIG_UNPREFIXED, 1,
      MAX_DOC_TABLE_SIZE, get_numeric_config, set_numeric_config, NULL,
      (void *)&(RSGlobalConfig.maxDocTableSize)
    )
  )

  RM_TRY(
    RedisModule_RegisterNumericConfig(
      ctx, "search-cursor-max-idle", DEFAULT_MAX_CURSOR_IDLE,
      REDISMODULE_CONFIG_DEFAULT | REDISMODULE_CONFIG_UNPREFIXED, 1,
      LLONG_MAX, get_numeric_config, set_numeric_config, NULL,
      (void *)&(RSGlobalConfig.cursorMaxIdle)
    )
  )

  RM_TRY(
    RedisModule_RegisterNumericConfig(
      ctx, "search-max-search-results", DEFAULT_MAX_SEARCH_REQUEST_RESULTS,
      REDISMODULE_CONFIG_DEFAULT | REDISMODULE_CONFIG_UNPREFIXED, 0,
      MAX_SEARCH_REQUEST_RESULTS, get_numeric_config, set_numeric_config, NULL,
      (void *)&(RSGlobalConfig.maxSearchResults)
    )
  )

  RM_TRY(
    RedisModule_RegisterNumericConfig(
      ctx, "search-min-operation-workers", MIN_OPERATION_WORKERS,
      REDISMODULE_CONFIG_DEFAULT | REDISMODULE_CONFIG_UNPREFIXED, 1,
      MAX_WORKER_THREADS, get_min_operation_workers,
      set_min_operation_workers, NULL,
      (void *)&(RSGlobalConfig.minOperationWorkers)
    )
  )

  RM_TRY(
    RedisModule_RegisterNumericConfig(
      ctx, "search-min-phonetic-term-len", DEFAULT_MIN_PHONETIC_TERM_LEN,
      REDISMODULE_CONFIG_DEFAULT | REDISMODULE_CONFIG_UNPREFIXED, 1,
      LLONG_MAX, get_numeric_config, set_numeric_config, NULL,
      (void *)&(RSGlobalConfig.minPhoneticTermLen)
    )
  )

  RM_TRY(
    RedisModule_RegisterNumericConfig(
      ctx, "search-min-prefix", DEFAULT_MIN_TERM_PREFIX,
      REDISMODULE_CONFIG_DEFAULT | REDISMODULE_CONFIG_UNPREFIXED, 1,
      LLONG_MAX, get_numeric_config, set_numeric_config, NULL,
      (void *)&(RSGlobalConfig.iteratorsConfigParams.minTermPrefix)
    )
  )

  RM_TRY(
    RedisModule_RegisterNumericConfig(
      ctx, "search-min-stem-len", DEFAULT_MIN_STEM_LENGTH,
      REDISMODULE_CONFIG_DEFAULT | REDISMODULE_CONFIG_UNPREFIXED, 2,
      UINT32_MAX, get_numeric_config, set_numeric_config, NULL,
      (void *)&(RSGlobalConfig.iteratorsConfigParams.minStemLength)
    )
  )

  RM_TRY(
    RedisModule_RegisterNumericConfig(
      ctx, "search-multi-text-slop", DEFAULT_MULTI_TEXT_SLOP,
      REDISMODULE_CONFIG_IMMUTABLE | REDISMODULE_CONFIG_UNPREFIXED, 1,
      UINT32_MAX, get_uint_numeric_config, set_uint_numeric_config, NULL,
      (void *)&(RSGlobalConfig.multiTextOffsetDelta)
    )
  )

  RM_TRY(
    RedisModule_RegisterNumericConfig(
      ctx, "search-tiered-hnsw-buffer-limit", DEFAULT_BLOCK_SIZE,
      REDISMODULE_CONFIG_IMMUTABLE | REDISMODULE_CONFIG_UNPREFIXED, 0,
      LLONG_MAX, get_numeric_config, set_numeric_config, NULL,
      (void *)&(RSGlobalConfig.tieredVecSimIndexBufferLimit)
    )
  )

  RM_TRY(
    RedisModule_RegisterNumericConfig(
      ctx, "search-timeout", DEFAULT_QUERY_TIMEOUT_MS,
      REDISMODULE_CONFIG_DEFAULT | REDISMODULE_CONFIG_UNPREFIXED, 1,
      LLONG_MAX, get_numeric_config, set_numeric_config, NULL,
      (void *)&(RSGlobalConfig.requestConfigParams.queryTimeoutMS)
    )
  )

  RM_TRY(
    RedisModule_RegisterNumericConfig(
      ctx, "search-union-iterator-heap", DEFAULT_UNION_ITERATOR_HEAP,
      REDISMODULE_CONFIG_DEFAULT | REDISMODULE_CONFIG_UNPREFIXED, 1,
      LLONG_MAX, get_numeric_config, set_numeric_config, NULL,
      (void *)&(RSGlobalConfig.iteratorsConfigParams.minUnionIterHeap)
    )
  )

  RM_TRY(
    RedisModule_RegisterNumericConfig(
      ctx, "search-vss-max-resize", DEFAULT_VSS_MAX_RESIZE,
      REDISMODULE_CONFIG_DEFAULT | REDISMODULE_CONFIG_UNPREFIXED, 0,
      UINT32_MAX, get_uint_numeric_config, set_uint_numeric_config, NULL,
      (void *)&(RSGlobalConfig.vssMaxResize)
    )
  )

  RM_TRY(
    RedisModule_RegisterNumericConfig(
      ctx, "search-workers", DEFAULT_WORKER_THREADS,
      REDISMODULE_CONFIG_DEFAULT | REDISMODULE_CONFIG_UNPREFIXED, 0,
      MAX_WORKER_THREADS, get_workers, set_workers, NULL,
      (void *)&RSGlobalConfig
    )
  )

  RM_TRY(
    RedisModule_RegisterNumericConfig(
      ctx, "search-workers-priority-bias-threshold",
      DEFAULT_HIGH_PRIORITY_BIAS_THRESHOLD,
      REDISMODULE_CONFIG_IMMUTABLE | REDISMODULE_CONFIG_UNPREFIXED, 0,
      LLONG_MAX, get_numeric_config, set_numeric_config, NULL,
      (void *)&(RSGlobalConfig.highPriorityBiasNum)
    )
  )

  // String parameters
  RM_TRY(
    RedisModule_RegisterStringConfig(
      ctx, "search-ext-load", "",
      REDISMODULE_CONFIG_IMMUTABLE | REDISMODULE_CONFIG_UNPREFIXED,
      get_ext_load, set_immutable_string_config, NULL,
      (void *)&(RSGlobalConfig.extLoad)
    )
  )

  RM_TRY(
    RedisModule_RegisterStringConfig(
      ctx, "search-friso-ini", "",
      REDISMODULE_CONFIG_IMMUTABLE | REDISMODULE_CONFIG_UNPREFIXED,
      get_friso_ini, set_immutable_string_config, NULL,
      (void *)&(RSGlobalConfig.frisoIni)
    )
  )

  // Enum parameters
  RM_TRY(
    RedisModule_RegisterEnumConfig(
      ctx, "search-on-timeout", TimeoutPolicy_Fail,
      REDISMODULE_CONFIG_DEFAULT | REDISMODULE_CONFIG_UNPREFIXED,
      on_timeout_vals, on_timeout_enums, 2,
      get_on_timeout, set_on_timeout, NULL,
      (void*)&RSGlobalConfig.requestConfigParams.timeoutPolicy
    )
  )

  // Boolean parameters
  RM_TRY(
    RedisModule_RegisterBoolConfig(
      ctx, "search-_free-resource-on-thread", 1,
      REDISMODULE_CONFIG_DEFAULT | REDISMODULE_CONFIG_UNPREFIXED,
      get_bool_config, set_bool_config, NULL,
      (void *)&(RSGlobalConfig.freeResourcesThread)
    )
  )

  RM_TRY(
    RedisModule_RegisterBoolConfig(
      ctx, "search-_numeric-compress", 0,
      REDISMODULE_CONFIG_DEFAULT | REDISMODULE_CONFIG_UNPREFIXED,
      get_bool_config, set_bool_config, NULL,
      (void *)&(RSGlobalConfig.numericCompress)
    )
  )

  RM_TRY(
    RedisModule_RegisterBoolConfig(
      ctx, "search-_print-profile-clock", 1,
      REDISMODULE_CONFIG_DEFAULT | REDISMODULE_CONFIG_UNPREFIXED,
      get_bool_config, set_bool_config, NULL,
      (void *)&(RSGlobalConfig.requestConfigParams.printProfileClock)
    )
  )

  RM_TRY(
    RedisModule_RegisterBoolConfig(
      ctx, "search-_prioritize-intersect-union-children", 0,
      REDISMODULE_CONFIG_DEFAULT | REDISMODULE_CONFIG_UNPREFIXED,
      get_bool_config, set_bool_config, NULL,
      (void *)&(RSGlobalConfig.prioritizeIntersectUnionChildren)
    )
  )

  // TODO: Confirm if we need this, because it should be deprecated in 8.0
  // RM_TRY(
  //   RedisModule_RegisterBoolConfig(
  //     ctx, "_fork-gc-clean-numeric-empty-nodes", 1,
  //     REDISMODULE_CONFIG_DEFAULT | REDISMODULE_CONFIG_UNPREFIXED,
  //     get_bool_config, set_bool_config, NULL,
  //     (void *)&(RSGlobalConfig.gcConfigParams.forkGc.forkGCCleanNumericEmptyNodes)
  //   )
  // )

  RM_TRY(
    RedisModule_RegisterBoolConfig(
      ctx, "search-no-mem-pools", 0,
      REDISMODULE_CONFIG_IMMUTABLE | REDISMODULE_CONFIG_UNPREFIXED,
      get_bool_config, set_bool_config, NULL,
      (void *)&(RSGlobalConfig.noMemPool)
    )
  )

  RM_TRY(
    RedisModule_RegisterBoolConfig(
      ctx, "search-no-gc", 0,
      REDISMODULE_CONFIG_IMMUTABLE | REDISMODULE_CONFIG_UNPREFIXED,
      get_inverted_bool_config, set_inverted_bool_config, NULL,
      (void *)&(RSGlobalConfig.gcConfigParams.enableGC)
    )
  )

  RM_TRY(
    RedisModule_RegisterBoolConfig(
      ctx, "search-partial-indexed-docs", 0,
      REDISMODULE_CONFIG_IMMUTABLE | REDISMODULE_CONFIG_UNPREFIXED,
      get_bool_config, set_bool_config, NULL,
      (void *)&(RSGlobalConfig.filterCommands)
    )
  )

  RM_TRY(
    RedisModule_RegisterBoolConfig(
      ctx, "search-raw-docid-encoding", 0,
      REDISMODULE_CONFIG_IMMUTABLE | REDISMODULE_CONFIG_UNPREFIXED,
      get_bool_config, set_bool_config, NULL,
      (void *)&(RSGlobalConfig.invertedIndexRawDocidEncoding)
    )
  )

  return REDISMODULE_OK;
}
