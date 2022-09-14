#include "config.h"
#include "err.h"
#include "rmutil/util.h"
#include "rmutil/strings.h"
#include "rmutil/args.h"
#include <string.h>
#include <stdlib.h>
#include <limits.h>
#include "rmalloc.h"

#define RETURN_ERROR(s) return REDISMODULE_ERR;
#define RETURN_PARSE_ERROR(rc)                                    \
  status->SetError(QUERY_EPARSEARGS, AC_Strerror(rc)); \
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

#define CONFIG_SETTER(name) static int name(RSConfig *config, ArgsCursor *ac, QueryError *status)

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
    int acrc = ac->GetString(&tf, NULL, 0);                     \
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
  int acrc = ac->GetString(&config->extLoad, NULL, 0);
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
  config->enableGC = 0;
  return REDISMODULE_OK;
}

CONFIG_BOOLEAN_GETTER(getNoGc, enableGC, 1)

CONFIG_SETTER(setNoMemPools) {
  config->noMemPool = 1;
  return REDISMODULE_OK;
}

CONFIG_BOOLEAN_GETTER(getNoMemPools, noMemPool, 0)

// MINPREFIX
CONFIG_SETTER(setMinPrefix) {
  int acrc = ac->GetLongLong( &config->minTermPrefix, AC_F_GE1);
  RETURN_STATUS(acrc);
}

CONFIG_GETTER(getMinPrefix) {
  sds ss = sdsempty();
  return sdscatprintf(ss, "%lld", config->minTermPrefix);
}

CONFIG_SETTER(setForkGCSleep) {
  int acrc = ac->GetSize(&config->forkGcSleepBeforeExit, AC_F_GE0);
  RETURN_STATUS(acrc);
}

CONFIG_GETTER(getForkGCSleep) {
  sds ss = sdsempty();
  return sdscatprintf(ss, "%zu", config->forkGcSleepBeforeExit);
}

// MAXDOCTABLESIZE
CONFIG_SETTER(setMaxDocTableSize) {
  size_t newsize = 0;
  int acrc = ac->GetSize(&newsize, AC_F_GE1);
  CHECK_RETURN_PARSE_ERROR(acrc);
  if (newsize > MAX_DOC_TABLE_SIZE) {
    status->SetError(QUERY_ELIMIT, "Value exceeds maximum possible document table size");
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
  int acrc = ac->GetLongLong(&newsize, 0);
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

// MAXEXPANSIONS
CONFIG_SETTER(setMaxExpansions) {
  int acrc = ac->GetLongLong( &config->maxPrefixExpansions, AC_F_GE1);
  RETURN_STATUS(acrc);
}

CONFIG_GETTER(getMaxExpansions) {
  sds ss = sdsempty();
  return sdscatprintf(ss, "%llu", config->maxPrefixExpansions);
}

// TIMEOUT
CONFIG_SETTER(setTimeout) {
  int acrc = ac->GetLongLong( &config->queryTimeoutMS, AC_F_GE0);
  RETURN_STATUS(acrc);
}

CONFIG_GETTER(getTimeout) {
  sds ss = sdsempty();
  return sdscatprintf(ss, "%lld", config->queryTimeoutMS);
}

// INDEX_THREADS
CONFIG_SETTER(setIndexThreads) {
  int acrc = ac->GetSize(&config->indexPoolSize, AC_F_GE1);
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
  int acrc = ac->GetSize(&config->searchPoolSize, AC_F_GE1);
  CHECK_RETURN_PARSE_ERROR(acrc);
  config->poolSizeNoAuto = 1;
  return REDISMODULE_OK;
}

CONFIG_GETTER(getSearchThreads) {
  sds ss = sdsempty();
  return sdscatprintf(ss, "%lu", config->searchPoolSize);
}

// FRISOINI
CONFIG_SETTER(setFrisoINI) {
  int acrc = ac->GetString(&config->frisoIni, NULL, 0);
  RETURN_STATUS(acrc);
}
CONFIG_GETTER(getFrisoINI) {
  return config->frisoIni ? sdsnew(config->frisoIni) : NULL;
}

// ON_TIMEOUT
CONFIG_SETTER(setOnTimeout) {
  const char *policy;
  int acrc = ac->GetString(&policy, NULL, 0);
  CHECK_RETURN_PARSE_ERROR(acrc);

  if ((config->timeoutPolicy = TimeoutPolicy_Parse(policy, strlen(policy))) ==
      TimeoutPolicy_Invalid) {
    RETURN_ERROR("Invalid ON_TIMEOUT value");
  }
  return REDISMODULE_OK;
}

CONFIG_GETTER(getOnTimeout) {
  return sdsnew(TimeoutPolicy_ToString(config->timeoutPolicy));
}

// GC_SCANSIZE
CONFIG_SETTER(setGcScanSize) {
  int acrc = ac->GetSize(&config->gcScanSize, AC_F_GE1);
  RETURN_STATUS(acrc);
}

CONFIG_GETTER(getGcScanSize) {
  sds ss = sdsempty();
  return sdscatprintf(ss, "%lu", config->gcScanSize);
}

// MIN_PHONETIC_TERM_LEN
CONFIG_SETTER(setForkGcInterval) {
  int acrc = ac->GetSize(&config->forkGcRunIntervalSec, AC_F_GE1);
  RETURN_STATUS(acrc);
}

CONFIG_SETTER(setForkGcCleanThreshold) {
  int acrc = ac->GetSize(&config->forkGcCleanThreshold, 0);
  RETURN_STATUS(acrc);
}

CONFIG_SETTER(setForkGcRetryInterval) {
  int acrc = ac->GetSize(&config->forkGcRetryInterval, AC_F_GE1);
  RETURN_STATUS(acrc);
}

CONFIG_SETTER(setMaxResultsToUnsortedMode) {
  int acrc = ac->GetLongLong( &config->maxResultsToUnsortedMode, AC_F_GE1);
  RETURN_STATUS(acrc);
}

CONFIG_SETTER(setCursorMaxIdle) {
  int acrc = ac->GetLongLong( &config->cursorMaxIdle, AC_F_GE1);
  RETURN_STATUS(acrc);
}

CONFIG_GETTER(getForkGcCleanThreshold) {
  sds ss = sdsempty();
  return sdscatprintf(ss, "%lu", config->forkGcCleanThreshold);
}

CONFIG_GETTER(getForkGcInterval) {
  sds ss = sdsempty();
  return sdscatprintf(ss, "%lu", config->forkGcRunIntervalSec);
}

CONFIG_GETTER(getForkGcRetryInterval) {
  sds ss = sdsempty();
  return sdscatprintf(ss, "%lu", config->forkGcRetryInterval);
}

CONFIG_GETTER(getMaxResultsToUnsortedMode) {
  sds ss = sdsempty();
  return sdscatprintf(ss, "%lld", config->maxResultsToUnsortedMode);
}

CONFIG_GETTER(getCursorMaxIdle) {
  sds ss = sdsempty();
  return sdscatprintf(ss, "%lld", config->cursorMaxIdle);
}

CONFIG_SETTER(setMinPhoneticTermLen) {
  int acrc = ac->GetSize(&config->minPhoneticTermLen, AC_F_GE1);
  RETURN_STATUS(acrc);
}

CONFIG_GETTER(getMinPhoneticTermLen) {
  sds ss = sdsempty();
  return sdscatprintf(ss, "%lu", config->minPhoneticTermLen);
}

// _NUMERIC_COMPRESS
CONFIG_BOOLEAN_SETTER(setNumericCompress, numericCompress)
CONFIG_BOOLEAN_GETTER(getNumericCompress, numericCompress, 0)

// REPLACE_DELETE_HASH_FIELD
CONFIG_BOOLEAN_SETTER(setReplaceDeleteField, replaceDeleteField)
CONFIG_BOOLEAN_GETTER(getReplaceDeleteField, replaceDeleteField, 0)

CONFIG_SETTER(setGcPolicy) {
  const char *policy;
  int acrc = ac->GetString(&policy, NULL, 0);
  CHECK_RETURN_PARSE_ERROR(acrc);
  if (!strcasecmp(policy, "DEFAULT") || !strcasecmp(policy, "FORK")) {
    config->gcPolicy = GCPolicy_Fork;
  } else if (!strcasecmp(policy, "LEGACY")) {
    config->gcPolicy = GCPolicy_Sync;
  } else {
    RETURN_ERROR("Invalid GC Policy value");
    return REDISMODULE_ERR;
  }
  return REDISMODULE_OK;
}

CONFIG_GETTER(getGcPolicy) {
  return sdsnew(GCPolicy_ToString(config->gcPolicy));
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
  QueryError status;

  if (RedisModule_GetServerVersion) {   // for rstest
    RSGlobalConfig.serverVersion = RedisModule_GetServerVersion();
  }

  if (getenv("RS_MIN_THREADS")) {
    printf("Setting thread pool sizes to 1\n");
    RSGlobalConfig.searchPoolSize = 1;
    RSGlobalConfig.indexPoolSize = 1;
    RSGlobalConfig.poolSizeNoAuto = 1;
  }
  ArgsCursor ac;
  ac.InitRString(argv, argc);
  while (!ac.IsAtEnd()) {
    const char *name = ac.GetStringNC(NULL);
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
      *err = rm_strdup(status.GetError());
      status.ClearError();
      return REDISMODULE_ERR;
    }
    // Mark the option as having been modified
    curVar->flags |= RSCONFIGVAR_F_MODIFIED;
  }

  return REDISMODULE_OK;
}

RSConfigOptions RSGlobalConfigOptions = {
    vars: {
        {name: "EXTLOAD",
         helpText: "Load extension scoring/expansion module",
         setValue: setExtLoad,
         getValue: getExtLoad,
         flags: RSCONFIGVAR_F_IMMUTABLE},
        {name: "SAFEMODE",
         helpText: "Perform all operations in main thread (deprecated, use CONCURRENT_WRITE_MODE)",
         setValue: setSafemode,
         getValue: getSafemode,
         flags: RSCONFIGVAR_F_FLAG | RSCONFIGVAR_F_IMMUTABLE},
        {name: "CONCURRENT_WRITE_MODE",
         helpText: "Use multi threads for write operations.",
         setValue: setConcurentWriteMode,
         getValue: getConcurentWriteMode,
         flags: RSCONFIGVAR_F_FLAG | RSCONFIGVAR_F_IMMUTABLE},
        {name: "NOGC",
         helpText: "Disable garbage collection (for this process)",
         setValue: setNoGc,
         getValue: getNoGc,
         flags: RSCONFIGVAR_F_FLAG | RSCONFIGVAR_F_IMMUTABLE},
        {name: "MINPREFIX",
         helpText: "Set the minimum prefix for expansions (`*`)",
         setValue: setMinPrefix,
         getValue: getMinPrefix},
        {name: "FORKGC_SLEEP_BEFORE_EXIT",
         helpText: "set the amount of seconds for the fork GC to sleep before exists, should always be set to 0 (other then on tests).",
         setValue: setForkGCSleep,
         getValue: getForkGCSleep},
        {name: "MAXDOCTABLESIZE",
         helpText: "Maximum runtime document table size (for this process)",
         setValue: setMaxDocTableSize,
         getValue: getMaxDocTableSize,
         flags: RSCONFIGVAR_F_IMMUTABLE},
        {name: "MAXSEARCHRESULTS",
         helpText: "Maximum number of results from ft.search command",
         setValue: setMaxSearchResults,
         getValue: getMaxSearchResults},
        {name: "MAXEXPANSIONS",
         helpText: "Maximum prefix expansions to be used in a query",
         setValue: setMaxExpansions,
         getValue: getMaxExpansions},
        {name: "TIMEOUT",
         helpText: "Query (search) timeout",
         setValue: setTimeout,
         getValue: getTimeout},
        {name: "INDEX_THREADS",
         helpText: "Create at most this number of background indexing threads (will not necessarily parallelize indexing)",
         setValue: setIndexThreads,
         getValue: getIndexthreads,
         flags: RSCONFIGVAR_F_IMMUTABLE},
        {name: "SEARCH_THREADS",
         helpText: "Create at must this number of search threads (not, will not necessarily parallelize search)",
         setValue: setSearchThreads,
         getValue: getSearchThreads,
         flags: RSCONFIGVAR_F_IMMUTABLE},
        {name: "FRISOINI",
         helpText: "Path to Chinese dictionary configuration file (for Chinese tokenization)",
         setValue: setFrisoINI,
         getValue: getFrisoINI,
         flags: RSCONFIGVAR_F_IMMUTABLE},
        {name: "ON_TIMEOUT",
         helpText: "Action to perform when search timeout is exceeded (choose RETURN or FAIL)",
         setValue: setOnTimeout,
         getValue: getOnTimeout},
        {name: "GCSCANSIZE",
         helpText: "Scan this many documents at a time during every GC iteration",
         setValue: setGcScanSize,
         getValue: getGcScanSize},
        {name: "MIN_PHONETIC_TERM_LEN",
         helpText: "Minumum length of term to be considered for phonetic matching",
         setValue: setMinPhoneticTermLen,
         getValue: getMinPhoneticTermLen},
        {name: "GC_POLICY",
         helpText: "gc policy to use (DEFAULT/LEGACY)",
         setValue: setGcPolicy,
         getValue: getGcPolicy,
         flags: RSCONFIGVAR_F_IMMUTABLE},
        {name: "FORK_GC_RUN_INTERVAL",
         helpText: "interval (in seconds) in which to run the fork gc (relevant only when forkgc is used)",
         setValue: setForkGcInterval,
         getValue: getForkGcInterval},
        {name: "FORK_GC_CLEAN_THRESHOLD",
         helpText: "the fork gc will only start to clean when the number of not cleaned document will acceded this threshold",
         setValue: setForkGcCleanThreshold,
         getValue: getForkGcCleanThreshold},
        {name: "FORK_GC_RETRY_INTERVAL",
         helpText: "interval (in seconds) in which to retry running the forkgc after failure.",
         setValue: setForkGcRetryInterval,
         getValue: getForkGcRetryInterval},
        {name: "_MAX_RESULTS_TO_UNSORTED_MODE",
         helpText: "max results for union interator in which the interator will switch to unsorted mode, should be used for debug only.",
         setValue: setMaxResultsToUnsortedMode,
         getValue: getMaxResultsToUnsortedMode},
        {name: "CURSOR_MAX_IDLE",
         helpText: "max idle time allowed to be set for cursor, setting it hight might cause high memory consumption.",
         setValue: setCursorMaxIdle,
         getValue: getCursorMaxIdle},
        {name: "NO_MEM_POOLS",
         helpText: "Set RediSearch to run without memory pools",
         setValue: setNoMemPools,
         getValue: getNoMemPools,
         flags: RSCONFIGVAR_F_IMMUTABLE},
        {name: "_NUMERIC_COMPRESS",
         helpText: "Enable legacy compression of double to float.",
         setValue: setNumericCompress,
         getValue: getNumericCompress},
        {name: "REPLACE_DELETE_HASH_FIELD",
         helpText: "Change behavior of FT.ADD with REPLACE to delete the document before reinsertion.",
         setValue: setReplaceDeleteField,
         getValue: getReplaceDeleteField},
        {name: NULL}}};

sds RSConfig::GetInfoString() const {
  sds ss = sdsempty();

  ss = sdscatprintf(ss, "concurrent writes: %s, ", concurrentMode ? "ON" : "OFF");
  ss = sdscatprintf(ss, "gc: %s, ", enableGC ? "ON" : "OFF");
  ss = sdscatprintf(ss, "prefix min length: %lld, ", minTermPrefix);
  ss = sdscatprintf(ss, "prefix max expansions: %lld, ", maxPrefixExpansions);
  ss = sdscatprintf(ss, "query timeout (ms): %lld, ", queryTimeoutMS);
  ss = sdscatprintf(ss, "timeout policy: %s, ", TimeoutPolicy_ToString(timeoutPolicy));
  ss = sdscatprintf(ss, "cursor read size: %lld, ", cursorReadSize);
  ss = sdscatprintf(ss, "cursor max idle (ms): %lld, ", cursorMaxIdle);
  ss = sdscatprintf(ss, "max doctable size: %lu, ", maxDocTableSize);
  ss = sdscatprintf(ss, "max number of search results: ");
  ss = maxSearchResults == UINT64_MAX ? // value for MaxSearchResults
       sdscatprintf(ss, "unlimited, ") : sdscatprintf(ss, " %lu, ", maxSearchResults);
  ss = sdscatprintf(ss, "search pool size: %lu, ", searchPoolSize);
  ss = sdscatprintf(ss, "index pool size: %lu, ", indexPoolSize);

  if (extLoad) {
    ss = sdscatprintf(ss, "ext load: %s, ", extLoad);
  }

  if (frisoIni) {
    ss = sdscatprintf(ss, "friso ini: %s, ", frisoIni);
  }
  return ss;
}

static void dumpConfigOption(const RSConfig *config, const RSConfigVar *var, RedisModuleCtx *ctx,
                             int isHelp) {
  size_t numElems = 0;
  sds currValue = var->getValue(config);

  RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
  RedisModule_ReplyWithSimpleString(ctx, var->name);
  numElems++;
  if (isHelp) {
    RedisModule_ReplyWithSimpleString(ctx, "Description");
    RedisModule_ReplyWithSimpleString(ctx, var->helpText);
    RedisModule_ReplyWithSimpleString(ctx, "Value");
    if (currValue) {
      RedisModule_ReplyWithStringBuffer(ctx, currValue, sdslen(currValue));
    } else {
      RedisModule_ReplyWithNull(ctx);
    }
    numElems += 4;
  } else {
    if (currValue) {
      RedisModule_ReplyWithSimpleString(ctx, currValue);
    } else {
      RedisModule_ReplyWithNull(ctx);
    }
    numElems++;
  }
  sdsfree(currValue);
  RedisModule_ReplySetArrayLength(ctx, numElems);
}

/**
 * Writes the retrieval of the configuration value to the network.
 * isHelp will use a more dict-like pattern, which should be a bit friendlier
 * on the eyes
 */

void RSConfig::DumpProto(const RSConfigOptions *options, const char *name,
                        RedisModuleCtx *ctx, int isHelp) const {
  size_t numElems = 0;
  RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
  if (!strcmp("*", name)) {
    for (const RSConfigOptions *curOpts = options; curOpts; curOpts = curOpts->next) {
      for (const RSConfigVar *cur = &curOpts->vars[0]; cur->name; cur++) {
        dumpConfigOption(this, cur, ctx, isHelp);
        numElems++;
      }
    }
  } else {
    const RSConfigVar *v = findConfigVar(options, name);
    if (v) {
      numElems++;
      dumpConfigOption(this, v, ctx, isHelp);
    }
  }
  RedisModule_ReplySetArrayLength(ctx, numElems);
}

/**
 * Sets a configuration variable. The argv, argc, and offset variables should
 * point to the global argv array. You can also make argv point at the specific
 * (after-the-option-name) arguments and set offset to 0, and argc to the number
 * of remaining arguments. offset is advanced to the next unread argument (which
 * can be == argc)
 */

int RSConfig::SetOption(RSConfigOptions *options, const char *name, RedisModuleString **argv,
                        int argc, size_t *offset, QueryError *status) {
  RSConfigVar *var = findConfigVar(options, name);
  if (!var) {
    status->SetError(QUERY_ENOOPTION, NULL);
    return REDISMODULE_ERR;
  }
  if (var->flags & RSCONFIGVAR_F_IMMUTABLE) {
    status->SetError(QUERY_EINVAL, "Not modifiable at runtime");
    return REDISMODULE_ERR;
  }
  ArgsCursor ac;
  ac.InitRString(argv + *offset, argc - *offset);
  int rc = var->setValue(this, &ac, status);
  *offset += ac.offset;
  return rc;
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
