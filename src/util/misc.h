/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#ifndef RS_MISC_H
#define RS_MISC_H

#include "redismodule.h"
#include "query_error.h"

/**
 * This handler crashes
 */
void GenericAofRewrite_DisabledHandler(RedisModuleIO *aof, RedisModuleString *key, void *value);

// null-unsafe
int GetRedisErrorCodeLength(const char* error);

/**
 * Extract the key name from a string, handling prefixes and errors.
 * @param s The string to extract the key name from
 * @param len The length of the string
 * @param status The error status to set in case of error
 * @param strictPrefix Whether to fail if the key prefix is not supported, currently we support $ for JSON paths and @ for regular fields.
 * @return The key name, or NULL if an error occurred
 */
const char *ExtractKeyName(const char *s, size_t *len, QueryError *status, bool strictPrefix, const char *context);

#endif
