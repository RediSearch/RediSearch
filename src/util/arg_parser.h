/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#pragma once

#include "rmutil/args.h"
#include "redismodule.h"
#include <stdint.h>
#include <stdbool.h>
#include "util/arr/arr.h"

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Enhanced argument parser built on top of ArgsCursor for more flexible and readable parsing.
 *
 * Key features:
 * - Declarative argument definition with variadic API
 * - Built-in error handling with descriptive messages
 * - Support for optional arguments with defaults
 * - Validation callbacks and custom validators
 * - Automatic help generation
 * - Context preservation for better error reporting
 */

typedef struct ArgParser ArgParser;
typedef struct ArgParseResult ArgParseResult;

// Forward declarations
typedef int (*ArgValidator)(const void *value, const char **error_msg);
typedef void (*ArgCallback)(ArgParser *parser, const void *value, void *user_data);

// Argument types supported by the parser
typedef enum {
    ARG_TYPE_FLAG,          // Boolean flag (presence = true)
    ARG_TYPE_BITFLAG,       // Bitwise flag (ORs mask into target)
    ARG_TYPE_STRING,        // String argument
    ARG_TYPE_INT,           // Integer argument
    ARG_TYPE_LONG_LONG,     // Long long integer
    ARG_TYPE_ULONG_LONG,    // Unsigned long long
    ARG_TYPE_DOUBLE,        // Double precision float
    ARG_TYPE_SUBARGS,       // Variable number of sub-arguments
} ArgType;

// Argument definition structure
typedef struct {
    const char *name;           // Argument name (e.g., "LIMIT", "TIMEOUT")
    const char *description;    // Help description
    ArgType type;               // Argument type
    void *target;               // Pointer to store parsed value
    bool required;              // Whether argument is required
    bool repeatable;            // Whether argument can appear multiple times

    // Positional constraint (1-based). If set, this argument is parsed by position instead of name.
    uint16_t position;          // 1 = first argument after command, 2 = second, etc.
    bool has_position;

    // Type-specific options (tagged union for type safety)
    union {
        struct {
            int min_args;       // For SUBARGS: minimum arguments
            int max_args;       // For SUBARGS: maximum arguments (-1 = unlimited)
        } subargs;

        struct {
            long long min_val;  // For numeric types: minimum value
            long long max_val;  // For numeric types: maximum value
            bool has_min;       // Whether min_val is set
            bool has_max;       // Whether max_val is set
        } numeric;

        struct {
            const char **allowed_values;  // For strings: allowed values (NULL-terminated)
        } string;

        struct {
            size_t target_size;           // Size of target for type safety
            unsigned long long mask;      // Bitmask to OR into target
        } bitflag;
    } options;

    // Validation and callbacks
    ArgValidator validator;     // Custom validation function
    ArgCallback callback;       // Callback when argument is parsed
    void *user_data;           // User data for callback

    // Default value (optional)
    union {
        const char *str_default;
        long long int_default;
        double double_default;
        bool flag_default;
    } defaults;
    bool has_default;

    // Parse state
    bool parsed;                // Whether this argument has been parsed
} ArgDefinition;

// Parse result structure
struct ArgParseResult {
    bool success;
    const char *error_message;
    const char *error_arg;      // Which argument caused the error
    int error_position;         // Position in argument list where error occurred
};

// Main parser structure
struct ArgParser {
    ArgsCursor *cursor;         // Underlying cursor
    arrayof(ArgDefinition) definitions; // Array of argument definitions
    const char *command_name;   // Command name for error messages

    // Internal state
    char *error_buffer;        // Thread-safe error message buffer
    ArgParseResult last_result; // Last parse result
};

// Constructor/Destructor
ArgParser *ArgParser_New(ArgsCursor *cursor, const char *command_name);
void ArgParser_Free(ArgParser *parser);

// Configuration methods (fluent API)
ArgParser *ArgParser_AddFlag(ArgParser *parser, const char *name, const char *description,
                            bool *target);
ArgParser *ArgParser_AddString(ArgParser *parser, const char *name, const char *description,
                              const char **target);
ArgParser *ArgParser_AddInt(ArgParser *parser, const char *name, const char *description,
                           int *target);
ArgParser *ArgParser_AddLongLong(ArgParser *parser, const char *name, const char *description,
                            long long *target);
ArgParser *ArgParser_AddULongLong(ArgParser *parser, const char *name, const char *description,
                             unsigned long long *target);
ArgParser *ArgParser_AddDouble(ArgParser *parser, const char *name, const char *description,
                              double *target);
ArgParser *ArgParser_AddSubArgs(ArgParser *parser, const char *name, const char *description,
                               ArgsCursor *target, int min_args, int max_args);

// Argument configuration options (for variadic functions)
typedef enum {
    ARG_OPT_END = 0,           // Marks end of options
    ARG_OPT_REQUIRED,          // Argument is required
    ARG_OPT_OPTIONAL,          // Argument is optional (default)
    ARG_OPT_REPEATABLE,        // Can appear multiple times
    ARG_OPT_VALIDATOR,         // Next arg is ArgValidator function
    ARG_OPT_CALLBACK,          // Next two args are ArgCallback function and user_data
    ARG_OPT_RANGE,             // Next two args are min_val, max_val (long long)
    ARG_OPT_ALLOWED_VALUES,    // Next arg is const char** array
    ARG_OPT_DEFAULT_STR,       // Next arg is const char* default value
    ARG_OPT_DEFAULT_INT,       // Next arg is long long default value
    ARG_OPT_DEFAULT_DOUBLE,    // Next arg is double default value
    ARG_OPT_DEFAULT_FLAG,      // Next arg is int (bool) default value
    ARG_OPT_POSITION           // Next arg is int (1-based position)
} ArgOption;

// Enhanced variadic API - pass all configuration in one call
ArgParser *ArgParser_AddBoolV(ArgParser *parser, const char *name, const char *description,
                             bool *target, ...);  // Terminated by ARG_OPT_END
// Bitwise flag: when present, OR 'mask' into the integer pointed by 'target' (size is sizeof(*target))
ArgParser *ArgParser_AddBitflagV(ArgParser *parser, const char *name, const char *description,
                                void *target, size_t target_size, unsigned long long mask, ...);
ArgParser *ArgParser_AddStringV(ArgParser *parser, const char *name, const char *description,
                               const char **target, ...);
ArgParser *ArgParser_AddIntV(ArgParser *parser, const char *name, const char *description,
                            int *target, ...);
ArgParser *ArgParser_AddLongLongV(ArgParser *parser, const char *name, const char *description,
                             long long *target, ...);
ArgParser *ArgParser_AddULongLongV(ArgParser *parser, const char *name, const char *description,
                              unsigned long long *target, ...);
ArgParser *ArgParser_AddDoubleV(ArgParser *parser, const char *name, const char *description,
                               double *target, ...);
ArgParser *ArgParser_AddSubArgsV(ArgParser *parser, const char *name, const char *description,
                                ArgsCursor *target, int min_args, int max_args, ...);



// Parsing methods
ArgParseResult ArgParser_Parse(ArgParser *parser);
ArgParseResult ArgParser_ParseNext(ArgParser *parser);  // Parse single argument
bool ArgParser_HasMore(ArgParser *parser);

// Utility methods
const char *ArgParser_GetErrorString(ArgParser *parser);
void ArgParser_PrintHelp(ArgParser *parser);
bool ArgParser_WasParsed(ArgParser *parser, const char *arg_name);

// Common validators
int ArgParser_ValidatePositive(const void *value, const char **error_msg);
int ArgParser_ValidateNonNegative(const void *value, const char **error_msg);
int ArgParser_ValidateRange(const void *value, const char **error_msg);  // Uses parser context

#ifdef __cplusplus
}
#endif
