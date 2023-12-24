/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "index_error.h"
#include "rmalloc.h"
#include "reply_macros.h"

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
    RedisModule_TrimStringAllocation(NA_rstr);
}

IndexError IndexError_Init() {
    if (!NA_rstr) initDefaultKey();
    IndexError error = {0}; // Initialize all fields to 0.
    error.last_error = NA;  // Last error message set to NA.
    // Key of the document that caused the error set to NA.
    error.key = RedisModule_HoldString(RSDummyContext, NA_rstr);
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
    error->key = RedisModule_HoldString(RSDummyContext, key);
    RedisModule_TrimStringAllocation(error->key);
    // Atomically increment the error_count by 1, since this might be called when spec is unlocked.
    __atomic_add_fetch(&error->error_count, 1, __ATOMIC_RELAXED);
    error->last_error_time = time(NULL);
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

void IndexError_Reply(const IndexError *error, RedisModule_Reply *reply, bool with_timestamp) {
    RedisModule_Reply_Map(reply);
    REPLY_KVINT(IndexingFailure_String, IndexError_ErrorCount(error));
    REPLY_KVSTR(IndexingError_String, IndexError_LastError(error));
    REPLY_KVRSTR(IndexingErrorKey_String, IndexError_LastErrorKey(error));
    if (with_timestamp) {
        REPLY_KVINT(IndexingErrorTime_String, IndexError_LastErrorTime(error));
    }
    RedisModule_Reply_MapEnd(reply);
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
time_t IndexError_LastErrorTime(const IndexError *error) {
    return error->last_error_time;
}

#ifdef RS_COORDINATOR

void IndexError_OpPlusEquals(IndexError *error, const IndexError *other) {
    if (!NA_rstr) initDefaultKey();
    // Condition is valid even if one or both errors are NA (`last_error_time` is 0).
    if (error->last_error_time < other->last_error_time) {
        // Prefer the other error.
        // copy/add error count later with atomic add.
        if (error->last_error != NA) rm_free(error->last_error);
        RedisModule_FreeString(RSDummyContext, error->key);
        error->last_error = rm_strdup(other->last_error);
        error->key = RedisModule_HoldString(RSDummyContext, other->key);
        error->last_error_time = other->last_error_time;
    }
    // Atomically increment the error_count by other->error_count, since this might be called when spec is unlocked.
    __atomic_add_fetch(&error->error_count, other->error_count, __ATOMIC_RELAXED);
}

// Setters
// Set the error_count of the IndexError.
void IndexError_SetErrorCount(IndexError *error, size_t error_count) {
    error->error_count = error_count;
}

// Set the last_error of the IndexError.
void IndexError_SetLastError(IndexError *error, const char *last_error) {
    if (error->last_error != NA) {
        rm_free(error->last_error);
    }
    // Don't strdup NULL.
    error->last_error = (last_error != NULL && last_error != NA) ? rm_strdup(last_error) : NA;
}

// Set the key of the IndexError. The key should be owned by the error already.
void IndexError_SetKey(IndexError *error, RedisModuleString *key) {
    if (!NA_rstr) initDefaultKey();
    RedisModule_FreeString(RSDummyContext, error->key);
    error->key = key;
}
void IndexError_SetErrorTime(IndexError *error, time_t error_time) {
    error->last_error_time = error_time;
}

IndexError IndexError_Deserialize(MRReply *reply) {
    IndexError error = IndexError_Init();

    // Validate the reply. It should be a map with 3 elements.
    RedisModule_Assert(reply && (MRReply_Type(reply) == MR_REPLY_MAP || (MRReply_Type(reply) == MR_REPLY_ARRAY && MRReply_Length(reply) % 2 == 0)));
    // Make sure the reply is a map, regardless of the protocol.
    MRReply_ArrayToMap(reply);
    // print_mr_reply(reply);

    MRReply *error_count = MRReply_MapElement(reply, IndexingFailure_String);
    RedisModule_Assert(error_count);
    RedisModule_Assert(MRReply_Type(error_count) == MR_REPLY_INTEGER);
    IndexError_SetErrorCount(&error, MRReply_Integer(error_count));

    MRReply *last_error = MRReply_MapElement(reply, IndexingError_String);
    RedisModule_Assert(last_error);
    // In hiredis with resp2 '+' is a status reply.
    RedisModule_Assert(MRReply_Type(last_error) == MR_REPLY_STRING || MRReply_Type(last_error) == MR_REPLY_STATUS);
    size_t error_len;
    const char *last_error_str = MRReply_String(last_error, &error_len);

    MRReply *key = MRReply_MapElement(reply, IndexingErrorKey_String);
    RedisModule_Assert(key);
    // In hiredis with resp2 '+' is a status reply.
    RedisModule_Assert(MRReply_Type(key) == MR_REPLY_STRING || MRReply_Type(key) == MR_REPLY_STATUS);
    size_t key_len;
    const char *key_str = MRReply_String(key, &key_len);

    MRReply *last_error_time = MRReply_MapElement(reply, IndexingErrorTime_String);
    RedisModule_Assert(last_error_time);
    RedisModule_Assert(MRReply_Type(last_error_time) == MR_REPLY_INTEGER);
    IndexError_SetErrorTime(&error, MRReply_Integer(last_error_time));

    if (strncmp(last_error_str, NA, error_len)) {
        IndexError_SetLastError(&error, last_error_str);
        RedisModuleString *key_rstr = RedisModule_CreateString(RSDummyContext, key_str, key_len);
        IndexError_SetKey(&error, key_rstr);
    } else {
        if (!NA_rstr) initDefaultKey();
        IndexError_SetLastError(&error, NA);
        IndexError_SetKey(&error, RedisModule_HoldString(RSDummyContext, NA_rstr));
    }

    return error;
}

#endif
