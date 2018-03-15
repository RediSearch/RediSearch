#ifndef RS_CONFIG_H_
#define RS_CONFIG_H_

#include "redismodule.h"

typedef enum {
  TimeoutPolicy_Default = 0,  // Defer to global config
  TimeoutPolicy_Return,       // Return what we have on timeout
  TimeoutPolicy_Fail          // Just fail without returning anything
} RSTimeoutPolicy;

static inline const char *TimeoutPolicy_ToString(RSTimeoutPolicy policy) {
  switch (policy) {
    case TimeoutPolicy_Return:
      return "return";
    case TimeoutPolicy_Fail:
      return "fail";
    default:
      return "huh?";
  }
}

/* RSConfig is a global configuration struct for the module, it can be included from each file, and
 * is initialized with user config options during module statrtup */
typedef struct {
  // Use concurrent serach (default: 1, disable with SAFEMODE)
  int concurrentMode;
  // If not null, this points at a .so file of an extension we try to load (default: NULL)
  const char *extLoad;
  // Path to friso.ini for chinese dictionary file
  const char *frisoIni;
  // If this is set, GC is enabled on all indexes (default: 1, disable with NOGC)
  int enableGC;

  // The minimal number of characters we allow expansion for in a prefix search. Default: 2
  long long minTermPrefix;

  // The maximal number of expansions we allow for a prefix. Default: 200
  long long maxPrefixExpansions;

  // The maximal amount of time a single query can take before timing out, in milliseconds.
  // 0 means unlimited
  long long queryTimeoutMS;

  RSTimeoutPolicy timeoutPolicy;
} RSConfig;

// global config extern reference
extern RSConfig RSGlobalConfig;

/* Read configuration from redis module arguments into the global config object. Return
 * REDISMODULE_ERR and sets an error message if something is invalid */
int ReadConfig(RedisModuleString **argv, int argc, const char **err);

// default configuration
#define RS_DEFAULT_CONFIG                                                                    \
  (RSConfig) {                                                                               \
    .concurrentMode = 1, .extLoad = NULL, .enableGC = 1, .minTermPrefix = 2,                 \
    .maxPrefixExpansions = 200, .queryTimeoutMS = 500, .timeoutPolicy = TimeoutPolicy_Return \
  }
;

#endif