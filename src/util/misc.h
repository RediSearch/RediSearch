#ifndef RS_MISC_H
#define RS_MISC_H

#include "redismodule.h"
#include "module.h"

/**
 * This handler crashes
 */
void GenericAofRewrite_DisabledHandler(RedisModuleIO *aof, RedisModuleString *key, void *value);

#define RM_XFreeString(s)                        \
  do {                                           \
    if (s) {                                     \
      RedisModule_FreeString(RSDummyContext, s); \
    }                                            \
  } while (0);

#define RM_XRetainString(s)                        \
  ({                                               \
    if (s) {                                       \
      RedisModule_RetainString(RSDummyContext, s); \
    }                                              \
    s;                                             \
  })

#endif