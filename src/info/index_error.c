/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#include "index_error.h"
#include "rmalloc.h"
#include "reply_macros.h"
#include "util/timeout.h"
#include "util/strconv.h"
#include "rmutil/rm_assert.h"
#include "obfuscation/obfuscation_api.h"
#include "query_error.h"

extern RedisModuleCtx *RSDummyContext;

char* const NA = "N/A";
char* const OK = "OK";
char* const IndexError_ObjectName = "Index Errors";
char* const IndexingFailure_String = "indexing failures";
char* const IndexingError_String = "last indexing error";
char* const IndexingErrorKey_String = "last indexing error key";
char* const IndexingErrorTime_String = "last indexing error time";
char* const BackgroundIndexingOOMfailure_String = "background indexing status";
char* const outOfMemoryFailure = "OOM failure";
RedisModuleString* NA_rstr = NULL;



static void initDefaultKey() {
    NA_rstr = RedisModule_CreateString(RSDummyContext, NA, strlen(NA));
    RedisModule_TrimStringAllocation(NA_rstr);
}

RedisModuleString* getNAstring() {
    if (!NA_rstr) {
        initDefaultKey();
    }
    return NA_rstr;
}

IndexError IndexError_Init() {
    if (!NA_rstr) initDefaultKey();
    IndexError error = {0}; // Initialize all fields to 0.
    error.last_error_without_user_data = NA;  // Last error message set to NA.
    error.last_error_with_user_data = NA;  // Last error message set to NA.
    // Key of the document that caused the error set to NA.
    error.key = RedisModule_HoldString(RSDummyContext, NA_rstr);
    return error;
}

static inline void IndexError_ClearLastError(IndexError *error) {
    if (error->last_error_without_user_data != NA) {
        rm_free(error->last_error_without_user_data);
    }
    if (error->last_error_with_user_data != NA) {
        rm_free(error->last_error_with_user_data);
    }
}

void IndexError_AddError(IndexError *error, ConstErrorMessage withoutUserData, ConstErrorMessage withUserData, RedisModuleString *key) {
    if (!NA_rstr) initDefaultKey();
    if (!withoutUserData || !withUserData) {
        RedisModule_Log(RSDummyContext, REDISMODULE_LOGLEVEL_WARNING,
                        "Index error occurred but no index error message was set.");
    }
    IndexError_ClearLastError(error);
    RedisModule_FreeString(RSDummyContext, error->key);
    error->last_error_without_user_data = withoutUserData ? rm_strdup(withoutUserData) : NA; // Don't strdup NULL.
    error->last_error_with_user_data = withUserData ? rm_strdup(withUserData) : NA; // Don't strdup NULL.
    error->key = RedisModule_HoldString(RSDummyContext, key);
    RedisModule_TrimStringAllocation(error->key);
    // Atomically increment the error_count by 1, since this might be called when spec is unlocked.
    __atomic_add_fetch(&error->error_count, 1, __ATOMIC_RELAXED);
    clock_gettime(CLOCK_MONOTONIC_RAW, &error->last_error_time);
}

void IndexError_RaiseBackgroundIndexFailureFlag(IndexError *error) {
    // Change the background_indexing_OOM_failure flag to true.
    error->background_indexing_OOM_failure = true;
}

void IndexError_Clear(IndexError error) {
    if (!NA_rstr) initDefaultKey();
    if (error.last_error_without_user_data != NA && error.last_error_without_user_data != NULL) {
        rm_free(error.last_error_without_user_data);
        error.last_error_without_user_data = NA;
    }
    if (error.last_error_with_user_data != NA && error.last_error_with_user_data != NULL) {
      rm_free(error.last_error_with_user_data);
      error.last_error_with_user_data = NA;
    }
    if (error.key != NA_rstr) {
        RedisModule_FreeString(RSDummyContext, error.key);
        error.key = RedisModule_HoldString(RSDummyContext, NA_rstr);
    }
}

void IndexError_Reply(const IndexError *error, RedisModule_Reply *reply, bool withTimestamp, bool obfuscate, bool withOOMstatus) {
    RedisModule_Reply_Map(reply);
    REPLY_KVINT(IndexingFailure_String, IndexError_ErrorCount(error));
    RedisModuleString *lastErrorKey = NULL;
    if (obfuscate) {
      lastErrorKey = IndexError_LastErrorKeyObfuscated(error);
      REPLY_KVSTR_SAFE(IndexingError_String, IndexError_LastErrorObfuscated(error));
    } else {
      lastErrorKey = IndexError_LastErrorKey(error);
      REPLY_KVSTR_SAFE(IndexingError_String, IndexError_LastError(error));
    }
    REPLY_KVRSTR(IndexingErrorKey_String, lastErrorKey);
    RedisModule_FreeString(RSDummyContext, lastErrorKey);
    if (withTimestamp) {
        struct timespec ts = IndexError_LastErrorTime(error);
        REPLY_KVARRAY(IndexingErrorTime_String);
        RedisModule_Reply_LongLong(reply, ts.tv_sec);
        RedisModule_Reply_LongLong(reply, ts.tv_nsec);
        REPLY_ARRAY_END;
    }
    // Should only be displayed in "Index Errors", and not in, for example, "Field Statistics".
    if (withOOMstatus)
        REPLY_KVSTR_SAFE(BackgroundIndexingOOMfailure_String, IndexError_HasBackgroundIndexingOOMFailure(error) ? outOfMemoryFailure : OK);

    RedisModule_Reply_MapEnd(reply);
}

// Returns the number of errors in the IndexError.
size_t IndexError_ErrorCount(const IndexError *error) {
    return error->error_count;
}

// Returns the last error message in the IndexError.
const char *IndexError_LastError(const IndexError *error) {
    return error->last_error_with_user_data;
}

const char *IndexError_LastErrorObfuscated(const IndexError *error) {
  return error->last_error_without_user_data;
}

// Returns the key of the document that caused the error.
RedisModuleString *IndexError_LastErrorKey(const IndexError *error) {
  // We use hold string so the caller can always call free string regardless which clause of the if was reached
  return RedisModule_HoldString(RSDummyContext, error->key);
}

RedisModuleString *IndexError_LastErrorKeyObfuscated(const IndexError *error) {
  if (error->key == NA_rstr) {
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

void IndexError_Combine(IndexError *error, const IndexError *other) {
    if (!NA_rstr) initDefaultKey();
    // Condition is valid even if one or both errors are NA (`last_error_time` is 0).
    if (!rs_timer_ge(&error->last_error_time, &other->last_error_time)) {
        // Prefer the other error.
        // copy/add error count later.
        IndexError_ClearLastError(error);
        RedisModule_FreeString(RSDummyContext, error->key);
        error->last_error_without_user_data = rm_strdup(other->last_error_without_user_data);
        error->last_error_with_user_data = rm_strdup(other->last_error_with_user_data);
        error->key = RedisModule_HoldString(RSDummyContext, other->key);
        error->last_error_time = other->last_error_time;
    }
    // Currently `error` is not a shared object, so we don't need to use atomic add.
    error->error_count += other->error_count;
    error->background_indexing_OOM_failure |= other->background_indexing_OOM_failure;

}

// Setters
// Set the error_count of the IndexError.
void IndexError_SetErrorCount(IndexError *error, size_t error_count) {
    error->error_count = error_count;
}

// Set the last_error of the IndexError.
void IndexError_SetLastError(IndexError *error, const char *last_error) {
    IndexError_ClearLastError(error);
    // Don't strdup NULL.
    error->last_error_without_user_data = (last_error != NULL && last_error != NA) ? rm_strdup(last_error) : NA;
    error->last_error_with_user_data = (last_error != NULL && last_error != NA) ? rm_strdup(last_error) : NA;
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

bool IndexError_HasBackgroundIndexingOOMFailure(const IndexError *error) {
    return error->background_indexing_OOM_failure;
}

IndexError IndexError_Deserialize(MRReply *reply, bool withOOMstatus) {
    IndexError error = IndexError_Init();

    // Validate the reply. It should be a map with 3 elements.
    RS_ASSERT(reply && (MRReply_Type(reply) == MR_REPLY_MAP || (MRReply_Type(reply) == MR_REPLY_ARRAY && MRReply_Length(reply) % 2 == 0)));
    // Make sure the reply is a map, regardless of the protocol.
    MRReply_ArrayToMap(reply);

    MRReply *error_count = MRReply_MapElement(reply, IndexingFailure_String);
    RS_ASSERT(error_count);
    RS_ASSERT(MRReply_Type(error_count) == MR_REPLY_INTEGER);
    IndexError_SetErrorCount(&error, MRReply_Integer(error_count));

    MRReply *last_error = MRReply_MapElement(reply, IndexingError_String);
    RS_ASSERT(last_error);
    // In hiredis with resp2 '+' is a status reply.
    RS_ASSERT(MRReply_Type(last_error) == MR_REPLY_STRING || MRReply_Type(last_error) == MR_REPLY_STATUS);
    size_t error_len;
    const char *last_error_str = MRReply_String(last_error, &error_len);

    MRReply *key = MRReply_MapElement(reply, IndexingErrorKey_String);
    RS_ASSERT(key);
    // In hiredis with resp2 '+' is a status reply.
    RS_ASSERT(MRReply_Type(key) == MR_REPLY_STRING || MRReply_Type(key) == MR_REPLY_STATUS);
    size_t key_len;
    const char *key_str = MRReply_String(key, &key_len);
    MRReply *last_error_time = MRReply_MapElement(reply, IndexingErrorTime_String);
    RS_ASSERT(last_error_time);
    RS_ASSERT(MRReply_Type(last_error_time) == MR_REPLY_ARRAY && MRReply_Length(last_error_time) == 2);
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
    if (withOOMstatus) {
        MRReply *oomFailure = MRReply_MapElement(reply, BackgroundIndexingOOMfailure_String);
        RS_ASSERT(oomFailure);
        RS_ASSERT(MRReply_Type(oomFailure) == MR_REPLY_STRING || MRReply_Type(oomFailure) == MR_REPLY_STATUS);
        if (MRReply_StringEquals(oomFailure, outOfMemoryFailure, 1)) {
            IndexError_RaiseBackgroundIndexFailureFlag(&error);
        }
    }

    return error;
}
