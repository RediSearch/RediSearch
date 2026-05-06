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

#include <stdbool.h>
#include <stdint.h>

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

/**
 * Validate `(offset, count)` against `max_results` and check that
 * `offset + count` does not overflow.
 *
 * Precondition: `max_results <= LLONG_MAX`. The overflow check below relies
 * on this so that `(uint64_t)LLONG_MAX - count` does not underflow when
 * `count > LLONG_MAX`. All current callers cap `max_results` at
 * `MAX_AGGREGATE_REQUEST_RESULTS` (= 1ULL << 31), well within the bound.
 * Violation is asserted in debug builds.
 *
 * On failure sets `*status` and returns false:
 *   - offset > max -> "OFFSET exceeds maximum of <max>"
 *     with QUERY_ERROR_CODE_LIMIT
 *   - count > max  -> "LIMIT exceeds maximum of <max>"
 *     with QUERY_ERROR_CODE_LIMIT
 *   - offset + count > LLONG_MAX  -> "LIMIT offset + count overflow"
 *     with QUERY_ERROR_CODE_PARSE_ARGS
 */
bool ValidateLimitBounds(uint64_t offset, uint64_t count,
                         uint64_t max_results, QueryError *status);

#endif
