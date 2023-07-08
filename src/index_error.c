/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "index_error.h"
#include "rmalloc.h"

const char* NA = "NA";

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
