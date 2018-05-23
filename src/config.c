#include "config.h"
#include "rmutil/util.h"
#include "rmutil/strings.h"
#include <string.h>

RSConfig RSGlobalConfig;

int ReadConfig(RedisModuleString **argv, int argc, const char **err) {

  RSGlobalConfig = RS_DEFAULT_CONFIG;
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

  /* Read the minum query prefix allowed */
  if (argc >= 2 && RMUtil_ArgIndex("MINPREFIX", argv, argc) >= 0) {
    RMUtil_ParseArgsAfter("MINPREFIX", argv, argc, "l", &RSGlobalConfig.minTermPrefix);
    if (RSGlobalConfig.minTermPrefix <= 0) {
      *err = "Invalid MINPREFIX value";
      return REDISMODULE_ERR;
    }
  }

  /* Read the minum query prefix allowed */
  if (argc >= 2 && RMUtil_ArgIndex("MAXDOCTABLESIZE", argv, argc) >= 0) {
    RMUtil_ParseArgsAfter("MAXDOCTABLESIZE", argv, argc, "l", &RSGlobalConfig.maxDocTableSize);
    if (RSGlobalConfig.maxDocTableSize <= 0 ||
        RSGlobalConfig.minTermPrefix >= DEFAULT_MAX_DOC_TABLE_SIZE) {
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
