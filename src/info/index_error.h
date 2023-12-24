/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#pragma once
#include <stddef.h>
#include "redismodule.h"
#include "reply.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct IndexError {
    size_t error_count; // Number of errors.
    char *last_error;   // Last error message.
    RedisModuleString *key;          // Key of the document that caused the error.
} IndexError;

// Global constant to place an index error object in maps/dictionaries.
extern char* const IndexError_ObjectName;

// Initializes an IndexError. The error_count is set to 0 and the last_error is set to NA.
IndexError IndexError_Init();

// Adds an error message to the IndexError. The error_count is incremented and the last_error is set to the error_message.
void IndexError_AddError(IndexError *error, const char *error_message, RedisModuleString *key);

// Returns the number of errors in the IndexError.
size_t IndexError_ErrorCount(const IndexError *error);

// Returns the last error message in the IndexError.
const char *IndexError_LastError(const IndexError *error);

// Returns the key of the document that caused the error.
const RedisModuleString *IndexError_LastErrorKey(const IndexError *error);

// Clears an IndexError. If the last_error is not NA, it is freed.
void IndexError_Clear(IndexError error);

// IO and cluster traits
// Reply the index errors to the client.
void IndexError_Reply(const IndexError *error, RedisModule_Reply *reply);

#ifdef RS_COORDINATOR
#include "coord/src/rmr/reply.h"

// Adds the error message of the other IndexError to the IndexError. The error_count is incremented and the last_error is set to the error_message.
// This is used when merging errors from different shards in a cluster.
void IndexError_OpPlusEquals(IndexError *error, const IndexError *other);

IndexError IndexError_Deserialize(MRReply *reply);

#endif // RS_COORDINATOR

#ifdef __cplusplus
}
#endif
