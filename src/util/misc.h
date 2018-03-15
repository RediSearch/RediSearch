#ifndef RS_MISC_H
#define RS_MISC_H

#include "redismodule.h"

/**
 * This handler crashes
 */
void GenericAofRewrite_DisabledHandler(RedisModuleIO *aof, RedisModuleString *key, void *value);

#endif