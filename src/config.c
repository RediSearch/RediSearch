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

#define RETURN_ERROR(s) return REDISMODULE_ERR;
#define RETURN_PARSE_ERROR(rc)                                    \
  QueryError_SetError(status, QUERY_EPARSEARGS, AC_Strerror(rc)); \
  return REDISMODULE_ERR;

#define CHECK_RETURN_PARSE_ERROR(rc) \
  if (rc != AC_OK) {                 \
    RETURN_PARSE_ERROR(rc);          \
  }

#define RETURN_STATUS(rc)   \
  if (rc == AC_OK) {        \
    return REDISMODULE_OK;  \
  } else {                  \
    RETURN_PARSE_ERROR(rc); \
  }


#define CONFIG_GETTER(name) static sds name(const RSConfig *config)

#define CONFIG_BOOLEAN_GETTER(name, var, invert) \
  CONFIG_GETTER(name) {                          \
    int cv = config->var;                        \
    if (invert) {                                \
      cv = !cv;                                  \
    }                                            \
    return sdsnew(cv ? "true" : "false");        \
  }

#define CONFIG_BOOLEAN_SETTER(name, var)                        \
  CONFIG_SETTER(name) {                                         \
    const char *tf;                                             \
    int acrc = AC_GetString(ac, &tf, NULL, 0);                  \
    CHECK_RETURN_PARSE_ERROR(acrc);                             \
    if (!strcmp(tf, "true") || !strcmp(tf, "TRUE")) {           \
      config->var = 1;                                          \
    } else if (!strcmp(tf, "false") || !strcmp(tf, "FALSE")) {  \
      config->var = 0;                                          \
    } else {                                                    \
      acrc = AC_ERR_PARSE;                                      \
    }                                                           \
    RETURN_STATUS(acrc);                                        \
  }

// EXTLOAD
CONFIG_SETTER(setExtLoad) {
  int acrc = AC_GetString(ac, &config->extLoad, NULL, 0);
  RETURN_STATUS(acrc);
}

CONFIG_GETTER(getExtLoad) {
  if (config->extLoad) {
    return sdsnew(config->extLoad);
  } else {
    return NULL;
  }
}

// SAFEMODE
CONFIG_SETTER(setSafemode) {
  config->concurrentMode = 0;
  return REDISMODULE_OK;
}

CONFIG_BOOLEAN_GETTER(getSafemode, concurrentMode, 1)

CONFIG_SETTER(setConcurentWriteMode) {
  config->concurrentMode = 1;
  return REDISMODULE_OK;
}

CONFIG_BOOLEAN_GETTER(getConcurentWriteMode, concurrentMode, 0)

// NOGC
CONFIG_SETTER(setNoGc) {
  config->gcConfigParams.enableGC = 0;
  return REDISMODULE_OK;
}

CONFIG_BOOLEAN_GETTER(getNoGc, gcConfigParams.enableGC, 1)

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
  CHECK_RETURN_PARSE_ERROR(acrc);
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
  long long newsize = 0;
  int acrc = AC_GetLongLong(ac, &newsize, 0);
  CHECK_RETURN_PARSE_ERROR(acrc);
  if (newsize == -1) {
    newsize = UINT64_MAX;
  }
  config->maxSearchResults = newsize;
  return REDISMODULE_OK;
}

CONFIG_GETTER(getMaxSearchResults) {
  sds ss = sdsempty();
  if (config->maxSearchResults == UINT64_MAX) {
    return sdscatprintf(ss, "unlimited");
  }
  return sdscatprintf(ss, "%lu", config->maxSearchResults);
}

// MAXAGGREGATERESULTS
CONFIG_SETTER(setMaxAggregateResults) {
  long long newsize = 0;
  int acrc = AC_GetLongLong(ac, &newsize, 0);
  CHECK_RETURN_PARSE_ERROR(acrc);
  if (newsize == -1) {
    newsize = UINT64_MAX;
  }
  config->maxAggregateResults = newsize;
  return REDISMODULE_OK;
}

CONFIG_GETTER(getMaxAggregateResults) {
  sds ss = sdsempty();
  if (config->maxAggregateResults == UINT64_MAX) {
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

// INDEX_THREADS
CONFIG_SETTER(setIndexThreads) {
  int acrc = AC_GetSize(ac, &config->indexPoolSize, AC_F_GE1);
  CHECK_RETURN_PARSE_ERROR(acrc);
  config->poolSizeNoAuto = 1;
  return REDISMODULE_OK;
}

CONFIG_GETTER(getIndexthreads) {
  sds ss = sdsempty();
  return sdscatprintf(ss, "%lu", config->indexPoolSize);
}

// INDEX_THREADS
CONFIG_SETTER(setSearchThreads) {
  int acrc = AC_GetSize(ac, &config->searchPoolSize, AC_F_GE1);
  CHECK_RETURN_PARSE_ERROR(acrc);
  config->poolSizeNoAuto = 1;
  return REDISMODULE_OK;
}

CONFIG_GETTER(getSearchThreads) {
  sds ss = sdsempty();
  return sdscatprintf(ss, "%lu", config->searchPoolSize);
}

#ifdef MT_BUILD

// WORKER_THREADS
CONFIG_SETTER(setWorkThreads) {
  int acrc = AC_GetSize(ac, &config->numWorkerThreads, AC_F_GE0);
  RETURN_STATUS(acrc);
}

CONFIG_GETTER(getWorkThreads) {
  sds ss = sdsempty();
  return sdscatprintf(ss, "%lu", config->numWorkerThreads);
}

// MT_MODE

CONFIG_SETTER(setMtMode) {
  const char *mt_mode;
  int acrc = AC_GetString(ac, &mt_mode, NULL, 0);
  CHECK_RETURN_PARSE_ERROR(acrc);
  if (!strcasecmp(mt_mode, "MT_MODE_OFF")) {
    config->mt_mode = MT_MODE_OFF;
  } else if (!strcasecmp(mt_mode, "MT_MODE_ONLY_ON_OPERATIONS")){
    config->mt_mode = MT_MODE_ONLY_ON_OPERATIONS;
  } else if (!strcasecmp(mt_mode, "MT_MODE_FULL")){
    config->mt_mode = MT_MODE_FULL;
  } else {
    QueryError_SetError(status, QUERY_EPARSEARGS, "Invalie MT mode");
    return REDISMODULE_ERR;
  }
  return REDISMODULE_OK;
}
static inline const char *MTMode_ToString(MTMode mt_mode) {
  switch (mt_mode) {
    case MT_MODE_OFF:
      return "MT_MODE_OFF";
    case MT_MODE_ONLY_ON_OPERATIONS:
      return "MT_MODE_ONLY_ON_OPERATIONS";
    case MT_MODE_FULL:
      return "MT_MODE_FULL";
    // No default so the compiler will warn us if we forget to handle a case
  }
}

CONFIG_GETTER(getMtMode) {
  return sdsnew(MTMode_ToString(config->mt_mode));
}

// TIERED_HNSW_BUFFER_LIMIT
CONFIG_SETTER(setTieredIndexBufferLimit) {
  int acrc = AC_GetSize(ac, &config->tieredVecSimIndexBufferLimit, AC_F_GE0);
  RETURN_STATUS(acrc);
}

CONFIG_GETTER(getTieredIndexBufferLimit) {
  sds ss = sdsempty();
  return sdscatprintf(ss, "%lu", config->tieredVecSimIndexBufferLimit);
}

// PRIVILEGED_THREADS_NUM
CONFIG_SETTER(setPrivilegedThreadsNum) {
  int acrc = AC_GetSize(ac, &config->privilegedThreadsNum, AC_F_GE0);
  RETURN_STATUS(acrc);
}

CONFIG_GETTER(getPrivilegedThreadsNum) {
  sds ss = sdsempty();
  return sdscatprintf(ss, "%lu", config->privilegedThreadsNum);
}
#endif // MT_BUILD

// FRISOINI
CONFIG_SETTER(setFrisoINI) {
  int acrc = AC_GetString(ac, &config->frisoIni, NULL, 0);
  RETURN_STATUS(acrc);
}
CONFIG_GETTER(getFrisoINI) {
  return config->frisoIni ? sdsnew(config->frisoIni) : NULL;
}

// ON_TIMEOUT
CONFIG_SETTER(setOnTimeout) {
  const char *policy;
  int acrc = AC_GetString(ac, &policy, NULL, 0);
  CHECK_RETURN_PARSE_ERROR(acrc);
  RSTimeoutPolicy top = TimeoutPolicy_Parse(policy, strlen(policy));
  if (top == TimeoutPolicy_Invalid) {
    RETURN_ERROR("Invalid ON_TIMEOUT value");
  }
  config->requestConfigParams.timeoutPolicy = top;
  return REDISMODULE_OK;
}

CONFIG_GETTER(getOnTimeout) {
  return sdsnew(TimeoutPolicy_ToString(config->requestConfigParams.timeoutPolicy));
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

// MIN_PHONETIC_TERM_LEN
CONFIG_SETTER(setForkGcInterval) {
  int acrc = AC_GetSize(ac, &config->gcConfigParams.forkGc.forkGcRunIntervalSec, AC_F_GE1);
  RETURN_STATUS(acrc);
}

CONFIG_SETTER(setForkGcCleanThreshold) {
  int acrc = AC_GetSize(ac, &config->gcConfigParams.forkGc.forkGcCleanThreshold, 0);
  RETURN_STATUS(acrc);
}

CONFIG_SETTER(setForkGcRetryInterval) {
  int acrc = AC_GetSize(ac, &config->gcConfigParams.forkGc.forkGcRetryInterval, AC_F_GE1);
  RETURN_STATUS(acrc);
}

CONFIG_SETTER(setMaxResultsToUnsortedMode) {
  int acrc = AC_GetLongLong(ac, &config->iteratorsConfigParams.maxResultsToUnsortedMode, AC_F_GE1);
  RETURN_STATUS(acrc);
}

CONFIG_SETTER(setMinUnionIteratorHeap) {
  int acrc = AC_GetLongLong(ac, &config->iteratorsConfigParams.minUnionIterHeap, AC_F_GE1);
  RETURN_STATUS(acrc);
}

CONFIG_SETTER(setCursorMaxIdle) {
  int acrc = AC_GetLongLong(ac, &config->cursorMaxIdle, AC_F_GE1);
  RETURN_STATUS(acrc);
}

CONFIG_GETTER(getForkGcCleanThreshold) {
  sds ss = sdsempty();
  return sdscatprintf(ss, "%lu", config->gcConfigParams.forkGc.forkGcCleanThreshold);
}

CONFIG_GETTER(getForkGcInterval) {
  sds ss = sdsempty();
  return sdscatprintf(ss, "%lu", config->gcConfigParams.forkGc.forkGcRunIntervalSec);
}

CONFIG_GETTER(getForkGcRetryInterval) {
  sds ss = sdsempty();
  return sdscatprintf(ss, "%lu", config->gcConfigParams.forkGc.forkGcRetryInterval);
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

CONFIG_GETTER(getMaxResultsToUnsortedMode) {
  sds ss = sdsempty();
  return sdscatprintf(ss, "%lld", config->iteratorsConfigParams.maxResultsToUnsortedMode);
}

CONFIG_GETTER(getMinUnionIteratorHeap) {
  sds ss = sdsempty();
  return sdscatprintf(ss, "%lld", config->iteratorsConfigParams.minUnionIterHeap);
}

CONFIG_GETTER(getCursorMaxIdle) {
  sds ss = sdsempty();
  return sdscatprintf(ss, "%lld", config->cursorMaxIdle);
}

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

CONFIG_SETTER(setDefaultDialectVersion) {
  unsigned int dialectVersion;
  int acrc = AC_GetUnsigned(ac, &dialectVersion, AC_F_GE1);
  if (dialectVersion > MAX_DIALECT_VERSION) {
    QueryError_SetErrorFmt(status, MAX_DIALECT_VERSION, "Default dialect version cannot be higher than %u", MAX_DIALECT_VERSION);
    return REDISMODULE_ERR;
  }
  config->requestConfigParams.dialectVersion = dialectVersion;
  RETURN_STATUS(acrc);
}

CONFIG_GETTER(getDefaultDialectVersion) {
  sds ss = sdsempty();
  return sdscatprintf(ss, "%u", config->requestConfigParams.dialectVersion);
}

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

CONFIG_SETTER(setFilterCommand) {
  int acrc = AC_GetInt(ac, &config->filterCommands, AC_F_GE0);
  RETURN_STATUS(acrc);
}

CONFIG_SETTER(setUpgradeIndex) {
  size_t dummy2;
  const char *indexName;
  SchemaRuleArgs *rule = NULL;
  int acrc = AC_GetString(ac, &indexName, NULL, 0);

  if (acrc != AC_OK) {
    QueryError_SetError(status, QUERY_EPARSEARGS, "Index name was not given to upgrade argument");
    return REDISMODULE_ERR;
  }

  if (dictFetchValue(legacySpecRules, indexName)) {
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
  dictAdd(legacySpecRules, (char *)indexName, rule);

  return REDISMODULE_OK;
}

CONFIG_GETTER(getUpgradeIndex) {
  return sdsnew("Upgrade config for upgrading");
}

CONFIG_BOOLEAN_GETTER(getFilterCommand, filterCommands, 0)

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

  if (getenv("RS_MIN_THREADS")) {
    printf("Setting thread pool sizes to 1\n");
    RSGlobalConfig.searchPoolSize = 1;
    RSGlobalConfig.indexPoolSize = 1;
    RSGlobalConfig.poolSizeNoAuto = 1;
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

    if (curVar->setValue(&RSGlobalConfig, &ac, &status) != REDISMODULE_OK) {
      *err = rm_strdup(QueryError_GetError(&status));
      QueryError_ClearError(&status);
      return REDISMODULE_ERR;
    }
    // Mark the option as having been modified
    curVar->flags |= RSCONFIGVAR_F_MODIFIED;
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
        {.name = "SAFEMODE",
         .helpText =
             "Perform all operations in main thread (deprecated, use CONCURRENT_WRITE_MODE)",
         .setValue = setSafemode,
         .getValue = getSafemode,
         .flags = RSCONFIGVAR_F_FLAG | RSCONFIGVAR_F_IMMUTABLE},
        {.name = "CONCURRENT_WRITE_MODE",
         .helpText = "Use multi threads for write operations.",
         .setValue = setConcurentWriteMode,
         .getValue = getConcurentWriteMode,
         .flags = RSCONFIGVAR_F_FLAG | RSCONFIGVAR_F_IMMUTABLE},
        {.name = "NOGC",
         .helpText = "Disable garbage collection (for this process)",
         .setValue = setNoGc,
         .getValue = getNoGc,
         .flags = RSCONFIGVAR_F_FLAG | RSCONFIGVAR_F_IMMUTABLE},
        {.name = "MINPREFIX",
         .helpText = "Set the minimum prefix for expansions (`*`)",
         .setValue = setMinPrefix,
         .getValue = getMinPrefix},
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
        {.name = "INDEX_THREADS",
         .helpText = "Create at most this number of background indexing threads (will not "
                     "necessarily parallelize indexing)",
         .setValue = setIndexThreads,
         .getValue = getIndexthreads,
         .flags = RSCONFIGVAR_F_IMMUTABLE},
        {.name = "SEARCH_THREADS",
         .helpText = "Create at most this number of search threads (not, will not "
                     "necessarily parallelize search)",
         .setValue = setSearchThreads,
         .getValue = getSearchThreads,
         .flags = RSCONFIGVAR_F_IMMUTABLE,
        },
#ifdef MT_BUILD
        {.name = "WORKER_THREADS",
         .helpText = "Create at most this number of search threads",
         .setValue = setWorkThreads,
         .getValue = getWorkThreads,
         .flags = RSCONFIGVAR_F_IMMUTABLE,
        },
        {.name = "MT_MODE",
         .helpText = "Let ft.search and vector indexing be done in background threads as default if"
                        "set to MT_MODE_FULL. MT_MODE_ONLY_ON_OPERATIONS use workers thread pool for operational needs only otherwise",
         .setValue = setMtMode,
         .getValue = getMtMode,
         .flags = RSCONFIGVAR_F_IMMUTABLE,
        },
        {.name = "TIERED_HNSW_BUFFER_LIMIT",
        .helpText = "Use for setting the buffer limit threshold for vector similarity tiered"
                        " HNSW index, so that if we are using WORKER_THREADS for indexing, and the"
                        " number of vectors waiting in the buffer to be indexed exceeds this limit, "
                        " we insert new vectors directly into HNSW",
        .setValue = setTieredIndexBufferLimit,
        .getValue = getTieredIndexBufferLimit,
        .flags = RSCONFIGVAR_F_IMMUTABLE,  // TODO: can this be mutable?
        },
        {.name = "PRIVILEGED_THREADS_NUM",
            .helpText = "The number of threads in worker thread pool that always execute high"
                        " priority tasks, if there exist any in the job queue. Other threads will"
                        " excute high and low priority tasks alterntely.",
            .setValue = setPrivilegedThreadsNum,
            .getValue = getPrivilegedThreadsNum,
            .flags = RSCONFIGVAR_F_IMMUTABLE,  // TODO: can this be mutable?
        },
#endif
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
                     "will acceded this threshold",
         .setValue = setForkGcCleanThreshold,
         .getValue = getForkGcCleanThreshold},
        {.name = "FORK_GC_RETRY_INTERVAL",
         .helpText = "interval (in seconds) in which to retry running the forkgc after failure.",
         .setValue = setForkGcRetryInterval,
         .getValue = getForkGcRetryInterval},
        {.name = "FORK_GC_CLEAN_NUMERIC_EMPTY_NODES",
         .helpText = "clean empty nodes from numeric tree",
         .setValue = setForkGCCleanNumericEmptyNodes,
         .getValue = getForkGCCleanNumericEmptyNodes},
        {.name = "_FORK_GC_CLEAN_NUMERIC_EMPTY_NODES",
         .helpText = "clean empty nodes from numeric tree",
         .setValue = set_ForkGCCleanNumericEmptyNodes,
         .getValue = get_ForkGCCleanNumericEmptyNodes},
        {.name = "_MAX_RESULTS_TO_UNSORTED_MODE",
         .helpText = "max results for union interator in which the interator will switch to "
                     "unsorted mode, should be used for debug only.",
         .setValue = setMaxResultsToUnsortedMode,
         .getValue = getMaxResultsToUnsortedMode},
        {.name = "UNION_ITERATOR_HEAP",
         .helpText = "minimum number of interators in a union from which the interator will"
                     "switch to heap based implementation.",
         .setValue = setMinUnionIteratorHeap,
         .getValue = getMinUnionIteratorHeap},
        {.name = "CURSOR_MAX_IDLE",
         .helpText = "max idle time allowed to be set for cursor, setting it hight might cause "
                     "high memory consumption.",
         .setValue = setCursorMaxIdle,
         .getValue = getCursorMaxIdle},
        {.name = "NO_MEM_POOLS",
         .helpText = "Set RediSearch to run without memory pools",
         .setValue = setNoMemPools,
         .getValue = getNoMemPools,
         .flags = RSCONFIGVAR_F_IMMUTABLE},
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
        {.name = NULL}}};

void RSConfigOptions_AddConfigs(RSConfigOptions *src, RSConfigOptions *dst) {
  while (src->next != NULL) {
    src = src->next;
  }
  src->next = dst;
  dst->next = NULL;
}

sds RSConfig_GetInfoString(const RSConfig *config) {
  sds ss = sdsempty();

  ss = sdscatprintf(ss, "concurrent writes: %s, ", config->concurrentMode ? "ON" : "OFF");
  ss = sdscatprintf(ss, "gc: %s, ", config->gcConfigParams.enableGC ? "ON" : "OFF");
  ss = sdscatprintf(ss, "prefix min length: %lld, ", config->iteratorsConfigParams.minTermPrefix);
  ss = sdscatprintf(ss, "prefix max expansions: %lld, ", config->iteratorsConfigParams.maxPrefixExpansions);
  ss = sdscatprintf(ss, "query timeout (ms): %lld, ", config->requestConfigParams.queryTimeoutMS);
  ss = sdscatprintf(ss, "timeout policy: %s, ", TimeoutPolicy_ToString(config->requestConfigParams.timeoutPolicy));
  ss = sdscatprintf(ss, "cursor read size: %lld, ", config->cursorReadSize);
  ss = sdscatprintf(ss, "cursor max idle (ms): %lld, ", config->cursorMaxIdle);
  ss = sdscatprintf(ss, "max doctable size: %lu, ", config->maxDocTableSize);
  ss = sdscatprintf(ss, "max number of search results: ");
  ss = (config->maxSearchResults == UINT64_MAX)
           ?  // value for MaxSearchResults
           sdscatprintf(ss, "unlimited, ")
           : sdscatprintf(ss, " %lu, ", config->maxSearchResults);
  ss = sdscatprintf(ss, "search pool size: %lu, ", config->searchPoolSize);
  ss = sdscatprintf(ss, "index pool size: %lu, ", config->indexPoolSize);

  if (config->extLoad) {
    ss = sdscatprintf(ss, "ext load: %s, ", config->extLoad);
  }

  if (config->frisoIni) {
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
  int rc = var->setValue(config, &ac, status);
  *offset += ac.offset;
  return rc;
}

void RSConfig_AddToInfo(RedisModuleInfoCtx *ctx) {
  RedisModule_InfoAddSection(ctx, "runtime_configurations");

  RedisModule_InfoAddFieldCString(ctx, "concurrent_mode", RSGlobalConfig.concurrentMode ? "ON" : "OFF");
  if (RSGlobalConfig.extLoad != NULL) {
    RedisModule_InfoAddFieldCString(ctx, "extension_load", (char*)RSGlobalConfig.extLoad);
  }
  if (RSGlobalConfig.frisoIni != NULL) {
    RedisModule_InfoAddFieldCString(ctx, "friso_ini", (char*)RSGlobalConfig.frisoIni);
  }
  RedisModule_InfoAddFieldCString(ctx, "enableGC", RSGlobalConfig.gcConfigParams.enableGC ? "ON" : "OFF");
  RedisModule_InfoAddFieldLongLong(ctx, "minimal_term_prefix", RSGlobalConfig.iteratorsConfigParams.minTermPrefix);
  RedisModule_InfoAddFieldLongLong(ctx, "maximal_prefix_expansions", RSGlobalConfig.iteratorsConfigParams.maxPrefixExpansions);
  RedisModule_InfoAddFieldLongLong(ctx, "query_timeout_ms", RSGlobalConfig.requestConfigParams.queryTimeoutMS);
  RedisModule_InfoAddFieldCString(ctx, "timeout_policy", (char*)TimeoutPolicy_ToString(RSGlobalConfig.requestConfigParams.timeoutPolicy));
  RedisModule_InfoAddFieldLongLong(ctx, "cursor_read_size", RSGlobalConfig.cursorReadSize);
  RedisModule_InfoAddFieldLongLong(ctx, "cursor_max_idle_time", RSGlobalConfig.cursorMaxIdle);

  RedisModule_InfoAddFieldLongLong(ctx, "max_doc_table_size", RSGlobalConfig.maxDocTableSize);
  RedisModule_InfoAddFieldLongLong(ctx, "max_search_results", RSGlobalConfig.maxSearchResults);
  RedisModule_InfoAddFieldLongLong(ctx, "max_aggregate_results", RSGlobalConfig.maxAggregateResults);
  RedisModule_InfoAddFieldLongLong(ctx, "search_pool_size", RSGlobalConfig.searchPoolSize);
  RedisModule_InfoAddFieldLongLong(ctx, "index_pool_size", RSGlobalConfig.indexPoolSize);
  RedisModule_InfoAddFieldLongLong(ctx, "gc_scan_size", RSGlobalConfig.gcConfigParams.gcScanSize);
  RedisModule_InfoAddFieldLongLong(ctx, "min_phonetic_term_length", RSGlobalConfig.minPhoneticTermLen);
}

void DialectsGlobalStats_AddToInfo(RedisModuleInfoCtx *ctx) {
  RedisModule_InfoAddSection(ctx, "dialect_statistics");
  for (int dialect = MIN_DIALECT_VERSION; dialect <= MAX_DIALECT_VERSION; ++dialect) {
    char field[16] = {0};
    snprintf(field, sizeof field, "dialect_%d", dialect);
    // extract the d'th bit of the dialects bitfield.
    RedisModule_InfoAddFieldULongLong(ctx, field, GET_DIALECT(RSGlobalConfig.used_dialects, dialect));
  }
}

const char *TimeoutPolicy_ToString(RSTimeoutPolicy policy) {
  switch (policy) {
    case TimeoutPolicy_Return:
      return "return";
    case TimeoutPolicy_Fail:
      return "fail";
    default:
      return "huh?";
  }
}

RSTimeoutPolicy TimeoutPolicy_Parse(const char *s, size_t n) {
  if (!strncasecmp(s, "RETURN", n)) {
    return TimeoutPolicy_Return;
  } else if (!strncasecmp(s, "FAIL", n)) {
    return TimeoutPolicy_Fail;
  } else {
    return TimeoutPolicy_Invalid;
  }
}
void iteratorsConfig_init(IteratorsConfig *config) {
  *config = RSGlobalConfig.iteratorsConfigParams;
}
