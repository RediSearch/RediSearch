/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#include "misc.h"
#include <stdlib.h>
#include <ctype.h>
#include <limits.h>
#include <string.h>

#include "rmutil/rm_assert.h"

void GenericAofRewrite_DisabledHandler(RedisModuleIO *aof, RedisModuleString *key, void *value) {
  RedisModule_Log(RedisModule_GetContextFromIO(aof), "error",
                  "Requested AOF, but this is unsupported for this module");
  abort();
}

int GetRedisErrorCodeLength(const char* error) {
  const char* errorSpace = strchr(error, ' ');
  return errorSpace ? errorSpace - error : 0;
}

const char *ExtractKeyName(const char *s, size_t *len, QueryError *status, bool strictPrefix, const char *context) {
  if (*s == '@') {
    --*len;
    return s + 1;
  } else if (*s == '$') {
    return s;
  } else if (strictPrefix) {
    QueryError_SetWithUserDataFmt(status, QUERY_ERROR_CODE_PARSE_ARGS, "Missing prefix: name requires '@' prefix, JSON path require '$' prefix", ", got: %s in %s", s, context);
    return NULL;
  } else {
    return s;
  }
}

bool ValidateLimitBounds(uint64_t offset, uint64_t count,
                         uint64_t max_offset, uint64_t max_count,
                         QueryError *status) {
  // Preconditions: see header. `max_count <= LLONG_MAX` guarantees that
  // `(uint64_t)LLONG_MAX - count` does not underflow in the overflow check.
  RS_ASSERT(max_offset <= (uint64_t)LLONG_MAX);
  RS_ASSERT(max_count  <= (uint64_t)LLONG_MAX);

  if (offset > max_offset) {
    QueryError_SetWithoutUserDataFmt(status, QUERY_ERROR_CODE_LIMIT,
        "OFFSET exceeds maximum of %llu", (unsigned long long)max_offset);
    return false;
  }
  if (count > max_count) {
    QueryError_SetWithoutUserDataFmt(status, QUERY_ERROR_CODE_LIMIT,
        "LIMIT exceeds maximum of %llu", (unsigned long long)max_count);
    return false;
  }
  // count <= max_count <= LLONG_MAX, so the subtraction below cannot underflow.
  if (offset > (uint64_t)LLONG_MAX - count) {
    QueryError_SetError(status, QUERY_ERROR_CODE_PARSE_ARGS,
        "LIMIT offset + count overflow");
    return false;
  }
  return true;
}
