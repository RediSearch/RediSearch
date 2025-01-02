/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#ifndef RS_MISC_H
#define RS_MISC_H

#include <stdbool.h>
#include "redismodule.h"

/**
 * This handler crashes
 */
void GenericAofRewrite_DisabledHandler(RedisModuleIO *aof, RedisModuleString *key, void *value);

char *strtolower(char *str);

int GetRedisErrorCodeLength(const char* error);

bool contains_non_alphabetic_char(char* str, size_t len);

#endif
