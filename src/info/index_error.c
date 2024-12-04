/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "index_error.h"
#include "rmalloc.h"
#include "reply_macros.h"
#include "util/timeout.h"
#include "util/strconv.h"
#include "obfuscation/obfuscation_api.h"
#include "query_error.h"

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
    error.short_last_error = NA;  // Last error message set to NA.
    error.detailed_last_error = NA;  // Last error message set to NA.
    // Key of the document that caused the error set to NA.
    error.key = RedisModule_HoldString(RSDummyContext, NA_rstr);
    return error;
}
void IndexError_AddError(IndexError *error, const char *shortError, const char* detailedError, RedisModuleString *key) {
    if (!NA_rstr) initDefaultKey();
    if (!error) {
        RedisModule_Log(RSDummyContext, REDISMODULE_LOGLEVEL_WARNING,
                        "Index error occurred but no index error message was set.");
    }
    if (error->short_last_error != NA) {
        rm_free(error->short_last_error);
    }
    if (error->detailed_last_error != NA) {
        rm_free(error->detailed_last_error);
    }
    RedisModule_FreeString(RSDummyContext, error->key);
    error->short_last_error = shortError ? rm_strdup(shortError) : NA; // Don't strdup NULL.
    error->detailed_last_error = detailedError ? rm_strdup(detailedError) : NA; // Don't strdup NULL.
    error->key = RedisModule_HoldString(RSDummyContext, key);
    RedisModule_TrimStringAllocation(error->key);
    // Atomically increment the error_count by 1, since this might be called when spec is unlocked.
    __atomic_add_fetch(&error->error_count, 1, __ATOMIC_RELAXED);
    clock_gettime(CLOCK_MONOTONIC_RAW, &error->last_error_time);
}

void IndexError_Clear(IndexError error) {
    if (!NA_rstr) initDefaultKey();
    if (error.short_last_error != NA && error.short_last_error != NULL) {
        rm_free(error.short_last_error);
        error.short_last_error = NA;
    }
    if (error.detailed_last_error != NA && error.detailed_last_error != NULL) {
      rm_free(error.detailed_last_error);
      error.detailed_last_error = NA;
    }
    if (error.key != NA_rstr) {
        RedisModule_FreeString(RSDummyContext, error.key);
        error.key = RedisModule_HoldString(RSDummyContext, NA_rstr);
    }
}

void IndexError_Reply(const IndexError *error, RedisModule_Reply *reply, bool withTimestamp, bool obfuscate) {
    RedisModule_Reply_Map(reply);
    REPLY_KVINT(IndexingFailure_String, IndexError_ErrorCount(error));
    REPLY_KVSTR_SAFE(IndexingError_String, IndexError_LastError(error, obfuscate));
    RedisModuleString *lastError = IndexError_LastErrorKey(error, obfuscate);
    REPLY_KVRSTR(IndexingErrorKey_String, lastError);
    RedisModule_FreeString(RSDummyContext, lastError);
    if (withTimestamp) {
        struct timespec ts = IndexError_LastErrorTime(error);
        REPLY_KVARRAY(IndexingErrorTime_String);
        RedisModule_Reply_LongLong(reply, ts.tv_sec);
        RedisModule_Reply_LongLong(reply, ts.tv_nsec);
        REPLY_ARRAY_END;
    }
    RedisModule_Reply_MapEnd(reply);
}

// Returns the number of errors in the IndexError.
size_t IndexError_ErrorCount(const IndexError *error) {
    return error->error_count;
}

// Returns the last error message in the IndexError.
const char *IndexError_LastError(const IndexError *error, bool obfuscate) {
    return obfuscate ? error->short_last_error : error->detailed_last_error;
}

// Returns the key of the document that caused the error.
RedisModuleString *IndexError_LastErrorKey(const IndexError *error, bool obfuscate) {
    // if there is no obfuscation or the key is NA_rstr then return the key as is
    if (!obfuscate || error->key == NA_rstr) {
      // We use hold string so the caller can always call free string regardless which clause of the if was reached
      return RedisModule_HoldString(RSDummyContext, error->key);
    } else {
      char documentName[MAX_OBFUSCATED_KEY_NAME];
      // When a document indexing error occurs we will not assign the document with an id
      // There is nothing for us to pass around between the shard and the coordinator
      // We use the last error time to obfuscate the document name
      Obfuscate_KeyWithTime(error->last_error_time, documentName);
      return RedisModule_CreateString(RSDummyContext, documentName, strlen(documentName));
    }
}

// Returns the last error time in the IndexError.
struct timespec IndexError_LastErrorTime(const IndexError *error) {
    return error->last_error_time;
}

void IndexError_OpPlusEquals(IndexError *error, const IndexError *other) {
    if (!NA_rstr) initDefaultKey();
    // Condition is valid even if one or both errors are NA (`last_error_time` is 0).
    if (!rs_timer_ge(&error->last_error_time, &other->last_error_time)) {
        // Prefer the other error.
        // copy/add error count later.
        if (error->short_last_error != NA) rm_free(error->short_last_error);
        if (error->detailed_last_error != NA) rm_free(error->detailed_last_error);
        RedisModule_FreeString(RSDummyContext, error->key);
        error->short_last_error = rm_strdup(other->short_last_error);
        error->detailed_last_error = rm_strdup(other->detailed_last_error);
        error->key = RedisModule_HoldString(RSDummyContext, other->key);
        error->last_error_time = other->last_error_time;
    }
    // Currently `error` is not a shared object, so we don't need to use atomic add.
    error->error_count += other->error_count;
}

// Setters
// Set the error_count of the IndexError.
void IndexError_SetErrorCount(IndexError *error, size_t error_count) {
    error->error_count = error_count;
}

// Set the last_error of the IndexError.
void IndexError_SetLastError(IndexError *error, const char *last_error) {
    if (error->short_last_error != NA) {
        rm_free(error->short_last_error);
    }
    if (error->detailed_last_error != NA) {
        rm_free(error->detailed_last_error);
    }
    // Don't strdup NULL.
    error->short_last_error = (last_error != NULL && last_error != NA) ? rm_strdup(last_error) : NA;
    error->detailed_last_error = (last_error != NULL && last_error != NA) ? rm_strdup(last_error) : NA;
}

// Set the key of the IndexError. The key should be owned by the error already.
void IndexError_SetKey(IndexError *error, RedisModuleString *key) {
    if (!NA_rstr) initDefaultKey();
    RedisModule_FreeString(RSDummyContext, error->key);
    error->key = key;
}
void IndexError_SetErrorTime(IndexError *error, struct timespec error_time) {
    error->last_error_time = error_time;
}

IndexError IndexError_Deserialize(MRReply *reply) {
    IndexError error = IndexError_Init();

    // Validate the reply. It should be a map with 3 elements.
    RedisModule_Assert(reply && (MRReply_Type(reply) == MR_REPLY_MAP || (MRReply_Type(reply) == MR_REPLY_ARRAY && MRReply_Length(reply) % 2 == 0)));
    // Make sure the reply is a map, regardless of the protocol.
    MRReply_ArrayToMap(reply);

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
    RedisModule_Assert(MRReply_Type(last_error_time) == MR_REPLY_ARRAY && MRReply_Length(last_error_time) == 2);
    struct timespec ts = {MRReply_Integer(MRReply_ArrayElement(last_error_time, 0)),
                          MRReply_Integer(MRReply_ArrayElement(last_error_time, 1))};
    IndexError_SetErrorTime(&error, ts);

    if (!STR_EQ(last_error_str, error_len, NA)) {
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
