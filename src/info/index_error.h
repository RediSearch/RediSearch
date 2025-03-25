/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#pragma once
#include <stddef.h>
#include "redismodule.h"
#include "reply.h"
#include <time.h>
#include "query_error.h"

#define WITH_INDEX_ERROR_TIME "_WITH_INDEX_ERROR_TIME"

#ifdef __cplusplus
extern "C" {
#endif

typedef char* ErrorMessage;
typedef const char* ConstErrorMessage;

typedef struct IndexError {
    size_t error_count;                 // Number of errors.
    ErrorMessage last_error_with_user_data;    // Last error message, can contain formatted user data
    ErrorMessage last_error_without_user_data; // Last error message, should not contain formatted user data
    RedisModuleString *key;             // Key of the document that caused the error.
    struct timespec last_error_time;    // Time of the last error.
    bool background_indexing_OOM_failure; // Background indexing OOM failure occurred.
} IndexError;

// Global constant to place an index error object in maps/dictionaries.
extern char* const IndexError_ObjectName;

// Initializes an IndexError. The error_count is set to 0 and the last_error is set to NA.
IndexError IndexError_Init();

// Adds an error message to the IndexError. The error_count is incremented and the last_error is set to the error_message.
void IndexError_AddError(IndexError *error, ConstErrorMessage withoutUserData, ConstErrorMessage withUserData, RedisModuleString *key);

// Adds a query error to the index error using IndexError_AddError
// IndexError_AddError is more abstract and is not explicitly tied to a query
// This function wraps around it and ties it a bit with the query error object
// it will pass obfuscated data for the withoutUserData and pass non-obfuscated data for the withUserData arguments
static inline void IndexError_AddQueryError(IndexError *error, const QueryError* queryError, RedisModuleString *key) {
    IndexError_AddError(error, QueryError_GetDisplayableError(queryError, true), QueryError_GetDisplayableError(queryError, false), key);
}

// Returns the number of errors in the IndexError.
size_t IndexError_ErrorCount(const IndexError *error);

// Returns the last error message in the IndexError.
const char *IndexError_LastError(const IndexError *error);

// Returns the last error message in the IndexError, obfuscated.
const char *IndexError_LastErrorObfuscated(const IndexError *error);

// Returns the key of the document that caused the error.
RedisModuleString *IndexError_LastErrorKey(const IndexError *error);

// Returns the key of the document that caused the error, obfuscated.
RedisModuleString *IndexError_LastErrorKeyObfuscated(const IndexError *error);

// Returns the time of the last error.
struct timespec IndexError_LastErrorTime(const IndexError *error);

// Clears an IndexError. If the last_error is not NA, it is freed.
void IndexError_Clear(IndexError error);

// IO and cluster traits
// Reply the index errors to the client.
void IndexError_Reply(const IndexError *error, RedisModule_Reply *reply, bool withTimestamp, bool obfuscate, bool withBgIndexingStatus);

#include "coord/rmr/reply.h"

// Adds the error message of the other IndexError to the IndexError. The error_count is incremented and the last_error is set to the error_message.
// This is used when merging errors from different shards in a cluster.
void IndexError_Combine(IndexError *error, const IndexError *other);

IndexError IndexError_Deserialize(MRReply *reply, bool withOOMstatus);

// Change the background_indexing_OOM_failure flag to true.
void IndexError_RaiseBackgroundIndexFailureFlag(IndexError *error);

// Get the background_indexing_OOM_failure flag.
bool IndexError_HasBackgroundIndexingOOMFailure(const IndexError *error);

#ifdef __cplusplus
}
#endif
