/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#pragma once
#include <stddef.h>
#include "redismodule.h"
#include "reply.h"

typedef struct IndexError {
    size_t error_count; // Number of errors.
    char *last_error;   // Last error message.
    RedisModuleString *key;          // Key of the document that caused the error.
} IndexError;

// No errors message. Used when there are no errors.
// This is a constant string, so it should not be freed.
extern const char* no_errors;

// Initializes an IndexError. The error_count is set to 0 and the last_error is set to no_errors.
IndexError IndexError_init();

// Adds an error message to the IndexError. The error_count is incremented and the last_error is set to the error_message.
void IndexError_add_error(IndexError *error, const char *error_message, const RedisModuleString *key);

// Clears an IndexError. If the last_error is not no_errors, it is freed.
void IndexError_clear(IndexError error);


// IO and cluser traits
// Adds the error message of the other IndexError to the IndexError. The error_count is incremented and the last_error is set to the error_message.
// This is used when merging errors from different shards in a cluster.
void IndexError_OpPlusEquals(IndexError *error, const IndexError *other);

// Reply the index errors to the client.
void IndexError_Reply(const IndexError *error, RedisModule_Reply *reply);
