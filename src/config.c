#include "config.h"
#include "rmutil/util.h"
#include "rmutil/strings.h"

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

  return REDISMODULE_OK;
}