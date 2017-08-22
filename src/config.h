#ifndef RS_CONFIG_H_
#define RS_CONFIG_H_

#include "redismodule.h"

typedef struct {
  int concurrentMode;
  const char *extLoad;
  int enableGC;
} RSConfig;

extern RSConfig RSGlobalConfig;

/* Read configuration from redis module arguments into the global config object. Return
 * REDISMODULE_ERR and sets an error message if something is invalid */
int ReadConfig(RedisModuleString **argv, int argc, const char **err);

// default configuration
#define RS_DEFAULT_CONFIG                                  \
  (RSConfig){                                              \
      .concurrentMode = 1, .extLoad = NULL, .enableGC = 1, \
  };

#endif