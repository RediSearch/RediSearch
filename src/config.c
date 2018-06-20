#include "config.h"
#include "rmutil/util.h"
#include "rmutil/strings.h"
#include <string.h>

RSConfig RSGlobalConfig = RS_DEFAULT_CONFIG;

int ReadConfig(RedisModuleString **argv, int argc, const char **err) {
  *err = NULL;
  /* Read the extension we want to load */
  if (argc > 1 && RMUtil_ArgIndex("EXTLOAD", argv, argc) >= 0) {
    RMUtil_ParseArgsAfter("EXTLOAD", argv, argc, "c", &RSGlobalConfig.extLoad);
  }

  /* If SafeMode is enabled, we turn down concurrent mode in the default config */
  if (RMUtil_ArgIndex("SAFEMODE", argv, argc) >= 0) {
    RSGlobalConfig.concurrentMode = 0;
  }

  /* If NOGC is sent, we disable gc in the config */
  if (RMUtil_ArgIndex("NOGC", argv, argc) >= 0) {
    RSGlobalConfig.enableGC = 0;
  }

  /* Read the minimum query prefix allowed */
  if (argc >= 2 && RMUtil_ArgIndex("MINPREFIX", argv, argc) >= 0) {
    RMUtil_ParseArgsAfter("MINPREFIX", argv, argc, "l", &RSGlobalConfig.minTermPrefix);
    if (RSGlobalConfig.minTermPrefix <= 0) {
      *err = "Invalid MINPREFIX value";
      return REDISMODULE_ERR;
    }
  }

  /* Read the maximum document table size allowed */
  if (argc >= 2 && RMUtil_ArgIndex("MAXDOCTABLESIZE", argv, argc) >= 0) {
    RMUtil_ParseArgsAfter("MAXDOCTABLESIZE", argv, argc, "l", &RSGlobalConfig.maxDocTableSize);
    if (RSGlobalConfig.maxDocTableSize <= 0 ||
        RSGlobalConfig.maxDocTableSize > MAX_DOC_TABLE_SIZE) {
      *err = "Invalid MAXDOCTABLESIZE value";
      return REDISMODULE_ERR;
    }
  }

  /* Read the maximum prefix expansions */
  if (argc >= 2 && RMUtil_ArgIndex("MAXEXPANSIONS", argv, argc) >= 0) {
    RMUtil_ParseArgsAfter("MAXEXPANSIONS", argv, argc, "l", &RSGlobalConfig.maxPrefixExpansions);
    if (RSGlobalConfig.maxPrefixExpansions <= 0) {
      *err = "Invalid MAXEXPANSIONS value";
      return REDISMODULE_ERR;
    }
  }

  /* Read the query timeout */
  if (argc >= 2 && RMUtil_ArgIndex("TIMEOUT", argv, argc) >= 0) {
    RMUtil_ParseArgsAfter("TIMEOUT", argv, argc, "l", &RSGlobalConfig.queryTimeoutMS);
    if (RSGlobalConfig.queryTimeoutMS < 0) {
      *err = "Invalid TIMEOUT value";
      return REDISMODULE_ERR;
    }
  }

  if (RMUtil_ArgIndex("FRISOINI", argv, argc) >= 0) {
    RMUtil_ParseArgsAfter("FRISOINI", argv, argc, "c", &RSGlobalConfig.frisoIni);
  }

  const char *policy = NULL;
  RMUtil_ParseArgsAfter("ON_TIMEOUT", argv, argc, "c", &policy);
  if (policy != NULL) {
    if (!strcasecmp(policy, "RETURN")) {
      RSGlobalConfig.timeoutPolicy = TimeoutPolicy_Return;
    } else if (!strcasecmp(policy, "FAIL")) {
      RSGlobalConfig.timeoutPolicy = TimeoutPolicy_Fail;
    } else {
      *err = "Invalid ON_TIMEOUT value";
      return REDISMODULE_ERR;
    }
  }
  return REDISMODULE_OK;
}

sds RSConfig_GetInfoString(const RSConfig *config) {
  sds ss = sdsempty();

  ss = sdscatprintf(ss, "concurrency: %s, ", config->concurrentMode ? "ON" : "OFF(SAFEMODE)");
  ss = sdscatprintf(ss, "gc: %s, ", config->enableGC ? "ON" : "OFF");
  ss = sdscatprintf(ss, "prefix min length: %lld, ", config->minTermPrefix);
  ss = sdscatprintf(ss, "prefix max expansions: %lld, ", config->maxPrefixExpansions);
  ss = sdscatprintf(ss, "query timeout (ms): %lld, ", config->queryTimeoutMS);
  ss = sdscatprintf(ss, "timeout policy: %s, ", TimeoutPolicy_ToString(config->timeoutPolicy));
  ss = sdscatprintf(ss, "cursor read size: %lld, ", config->cursorReadSize);
  ss = sdscatprintf(ss, "cursor max idle (ms): %lld, ", config->cursorMaxIdle);
  ss = sdscatprintf(ss, "max doctable size: %lu, ", config->maxDocTableSize);

  if (config->extLoad) {
    ss = sdscatprintf(ss, "ext load: %s, ", config->extLoad);
  }

  if (config->frisoIni) {
    ss = sdscatprintf(ss, "friso ini: %s, ", config->frisoIni);
  }
  return ss;
}
