/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "index_error.h"
#include "rmalloc.h"

const char* no_errors = "No errors";

IndexError IndexError_init() {
    IndexError error = {
        .error_count = 0,           // Number of errors set to 0.
        .last_error = (char*)no_errors,    // Last error message set to no_errors.
    };
    return error;
}
void IndexError_add_error(IndexError *error, char *error_message) {
    if(error->last_error != no_errors) {
        rm_free(error->last_error);
    }
    error->last_error = error_message;
    // Atomically increment the error_count by 1, since this might be called when spec is unlocked.
    __atomic_add_fetch(&error->error_count, 1, __ATOMIC_RELAXED);
}

void IndexError_clear(IndexError error) {
    if(error.last_error != no_errors) {
        rm_free(error.last_error);
    }
}
