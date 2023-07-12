/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "index_error.h"
#include "rmalloc.h"
#include "reply_macros.h"

const char* NA = "NA";
const char* IndexError_ObjectName = "Index Errors";
const char *IndexingFailure_String = "indexing failures";
const char *IndexingError_String = "indexing error";
const char *IndexingErrorKey_String = "indexing error key";

IndexError IndexError_init() {
    IndexError error = {
        .error_count = 0,           // Number of errors set to 0.
        .last_error = (char*)NA,    // Last error message set to no_errors.
        .key = RedisModule_CreateString(NULL, NA, strlen(NA))                 // Key of the document that caused the error set to NULL.
    };
    return error;
}
void IndexError_add_error(IndexError *error, const char *error_message, const RedisModuleString *key) {
    if(!error_message) {
        RedisModule_Log(NULL, "error", "Index error occured but no index error message was set.");
    }
    if(error->last_error != NA) {
        rm_free(error->last_error);
    }
    if(error->key != NULL) {
        RedisModule_FreeString(NULL, error->key);
    }
    error->last_error = error_message ? rm_strdup(error_message) : (char*) NA; // Don't strdup NULL.
    error->key = RedisModule_CreateStringFromString(NULL, key);
    // Atomically increment the error_count by 1, since this might be called when spec is unlocked.
    __atomic_add_fetch(&error->error_count, 1, __ATOMIC_RELAXED);
}

void IndexError_clear(IndexError error) {
    if(error.last_error != NA) {
        rm_free(error.last_error);
    }

    if(error.key != NULL) {
        RedisModule_FreeString(NULL, error.key);
    }
}

void IndexError_Reply(const IndexError *error, RedisModule_Reply *reply) {
    RedisModule_Reply_Map(reply);
    REPLY_KVNUM(IndexingFailure_String, error->error_count);
    REPLY_KVSTR(IndexingError_String, error->last_error);
    REPLY_KVRSTR(IndexingErrorKey_String, error->key);
    RedisModule_Reply_MapEnd(reply);
}


#ifdef RS_COORDINATOR

void IndexError_OpPlusEquals(IndexError *error, const IndexError *other) {
    if(other->last_error != NA) {
        if(error->last_error != NA) {
            rm_free(error->last_error);
        }
        error->last_error = rm_strdup(other->last_error);
    }
    if(other->key != NULL) {
        if(error->key != NULL) {
            RedisModule_FreeString(NULL, error->key);
        }
        error->key = RedisModule_CreateStringFromString(NULL, other->key);
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
    if(error->last_error != NA) {
        rm_free(error->last_error);
    }
    error->last_error = last_error ? rm_strdup(last_error) : (char*) NA; // Don't strdup NULL.
}

// Set the key of the IndexError.
void IndexError_SetKey(IndexError *error, const RedisModuleString *key) {
    if(error->key != NULL) {
        RedisModule_FreeString(NULL, error->key);
    }
    error->key = RedisModule_CreateStringFromString(NULL, key);
}

IndexError IndexError_Deserialize(MRReply *reply) {
    IndexError error = {0};

    MRReply *error_count = MRReply_MapElement(reply, IndexingFailure_String);
    RedisModule_Assert(error_count && MRReply_Type(error_count) == MR_REPLY_INTEGER);
    IndexError_SetErrorCount(&error, MRReply_Integer(error_count));

    MRReply *last_error = MRReply_MapElement(reply, IndexingError_String);
    RedisModule_Assert(last_error && MRReply_Type(last_error) == MR_REPLY_STRING);
    size_t len;
    const char *last_error_str = MRReply_String(last_error, &len);
    IndexError_SetLastError(&error, last_error_str);

    MRReply *key = MRReply_MapElement(reply, IndexingErrorKey_String);
    RedisModule_Assert(key && MRReply_Type(key) == MR_REPLY_STRING);
    const char *key_str = MRReply_String(key, &len);
    RedisModuleString *key_rstr =  RedisModule_CreateString(NULL, key_str, len);
    IndexError_SetKey(&error, key_rstr);
    RedisModule_FreeString(NULL, key_rstr);

    return error;
}

#endif

