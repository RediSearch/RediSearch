#pragma once

#include "redismodule.h"

/**
 * This handler crashes
 */
void GenericAofRewrite_DisabledHandler(RedisModuleIO *aof, RedisModuleString *key, void *value);
