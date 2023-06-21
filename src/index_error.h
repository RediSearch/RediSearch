/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#pragma once
#include <stddef.h>


typedef struct IndexError {
    size_t error_count; // Number of errors.
    char *last_error;   // Last error message.
} IndexError;

// No errors message. Used when there are no errors.
// This is a constant string, so it should not be freed.
extern const char* no_errors;

// Initializes an IndexError. The error_count is set to 0 and the last_error is set to no_errors.
IndexError IndexError_init();

// Adds an error message to the IndexError. The error_count is incremented and the last_error is set to the error_message.
// The error message should be allocated on the heap with rmalloc commands, as it will be freed when the IndexError is cleared.
void IndexError_add_error(IndexError *error, char *error_message);

// Clears an IndexError. If the last_error is not no_errors, it is freed.
void IndexError_clear(IndexError error);
