/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "index_error.h"
#include "rmalloc.h"
#include "util/timeout.h"

extern RedisModuleCtx *RSDummyContext;

char* const NA = "N/A";
char* const IndexError_ObjectName = "Index Errors";
char* const IndexingFailure_String = "indexing failures";
char* const IndexingError_String = "last indexing error";
char* const IndexingErrorKey_String = "last indexing error key";
char* const IndexingErrorTime_String = "last indexing error time";
RedisModuleString* NA_rstr = NULL;

static void initDefaultKey() {
    NA_rstr = RedisModule_CreateString(RSDummyContext, NA, strlen(NA));
}

IndexError IndexError_Init() {
    if (!NA_rstr) initDefaultKey();
    IndexError error = {0}; // Initialize all fields to 0.
    error.last_error = NA;  // Last error message set to NA.
    // Key of the document that caused the error set to NA.
    error.key = RedisModule_CreateStringFromString(RSDummyContext, NA_rstr);
    return error;
}
void IndexError_AddError(IndexError *error, const char *error_message, RedisModuleString *key) {
    if (!NA_rstr) initDefaultKey();
    if (!error_message) {
        RedisModule_Log(RSDummyContext, REDISMODULE_LOGLEVEL_WARNING,
                        "Index error occurred but no index error message was set.");
    }
    if (error->last_error != NA) {
        rm_free(error->last_error);
    }
    RedisModule_FreeString(RSDummyContext, error->key);
    error->last_error = error_message ? rm_strdup(error_message) : NA; // Don't strdup NULL.
    error->key = RedisModule_CreateStringFromString(RSDummyContext, key);
    // Atomically increment the error_count by 1, since this might be called when spec is unlocked.
    __atomic_add_fetch(&error->error_count, 1, __ATOMIC_RELAXED);
    clock_gettime(CLOCK_MONOTONIC_RAW, &error->last_error_time);
}

void IndexError_Clear(IndexError error) {
    if (!NA_rstr) initDefaultKey();
    if (error.last_error != NA && error.last_error != NULL) {
        rm_free(error.last_error);
        error.last_error = NA;
    }
    if (error.key != NA_rstr) {
        RedisModule_FreeString(RSDummyContext, error.key);
        error.key = RedisModule_HoldString(RSDummyContext, NA_rstr);
    }
}

// Returns the number of errors in the IndexError.
size_t IndexError_ErrorCount(const IndexError *error) {
    return error->error_count;
}

// Returns the last error message in the IndexError.
const char *IndexError_LastError(const IndexError *error) {
    return error->last_error;
}

// Returns the key of the document that caused the error.
const RedisModuleString *IndexError_LastErrorKey(const IndexError *error) {
    return error->key;
}

// Returns the last error time in the IndexError.
struct timespec IndexError_LastErrorTime(const IndexError *error) {
    return error->last_error_time;
}
