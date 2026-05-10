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
 * Validate `(offset, count)` against independent caps and check that
 * `offset + count` does not overflow.
 *
 * Precondition: both `max_offset` and `max_count` must be <= LLONG_MAX.
 * The overflow check relies on `max_count <= LLONG_MAX` so that
 * `(uint64_t)LLONG_MAX - count` does not underflow after the count guard
 * passes. All current callers cap their maxima at
 * `MAX_AGGREGATE_REQUEST_RESULTS` (= 1ULL << 31), well within the bound.
 * Violations are asserted in debug builds.
 *
 * On failure sets `*status` and returns false:
 *   - offset > max_offset -> "OFFSET exceeds maximum of <max_offset>"
 *     with QUERY_ERROR_CODE_LIMIT
 *   - count > max_count   -> "LIMIT exceeds maximum of <max_count>"
 *     with QUERY_ERROR_CODE_LIMIT
 *   - offset + count > LLONG_MAX -> "LIMIT offset + count overflow"
 *     with QUERY_ERROR_CODE_PARSE_ARGS
 */
bool ValidateLimitBounds(uint64_t offset, uint64_t count,
                         uint64_t max_offset, uint64_t max_count,
                         QueryError *status);

#endif
