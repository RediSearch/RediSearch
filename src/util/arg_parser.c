/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "arg_parser.h"
#include "rmalloc.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <stdarg.h>
#include <assert.h>

#define INITIAL_DEF_CAPACITY 16
#define MAX_ERROR_MSG_LEN 512
#define MAX_POSITIONAL_ARGS 20 // reasonable limit for number of expected positional arguments

// Internal helper functions
static ArgDefinition *find_definition(ArgParser *parser, const char *name);
static ArgDefinition *find_positional_definition(ArgParser *parser, uint16_t position, const char *name);
static int parse_single_arg(ArgParser *parser, ArgDefinition *def);
static void set_error(ArgParser *parser, const char *message, const char *arg_name);
static void apply_defaults(ArgParser *parser);


ArgParser *ArgParser_New(ArgsCursor *cursor, const char *command_name) {
    ArgParser *parser = rm_calloc(1, sizeof(ArgParser));
    if (!parser) return NULL;

    parser->cursor = cursor;
    parser->command_name = command_name ? rm_strdup(command_name) : NULL;
    parser->definitions = array_new(ArgDefinition, INITIAL_DEF_CAPACITY);
    parser->error_buffer = NULL;

    if (!parser->definitions) {
        rm_free((void*)parser->command_name);
        rm_free(parser);
        return NULL;
    }

    // Skip the first argument if it matches the command name
    // This handles cases where the cursor includes the command name as the first argument
    if (cursor && !AC_IsAtEnd(cursor) && command_name) {
        AC_AdvanceIfMatch(cursor, command_name);
    }

    return parser;
}

void ArgParser_Free(ArgParser *parser) {
    if (!parser) return;

    // Free allocated strings in definitions
    for (size_t i = 0; i < array_len(parser->definitions); i++) {
        ArgDefinition *def = &parser->definitions[i];
        if (def->name) rm_free((void*)def->name);
        if (def->description) rm_free((void*)def->description);
    }

    array_free(parser->definitions);
    rm_free(parser->error_buffer);
    rm_free((void*)parser->command_name);
    rm_free(parser);
}

static ArgDefinition *add_definition(ArgParser *parser, const char *name,
                                   const char *description, ArgType type, void *target) {
    if (!parser || !name) return NULL;

    char *name_copy = rm_strdup(name);
    char *desc_copy = description ? rm_strdup(description) : NULL;

    // Check for allocation failures
    if (!name_copy || (description && !desc_copy)) {
        rm_free(name_copy);
        rm_free(desc_copy);
        return NULL;
    }

    ArgDefinition def = {
        .name = name_copy,
        .description = desc_copy,
        .type = type,
        .target = target,
        .required = false,
        .repeatable = false,
        .has_default = false,
        .has_position = false,
        .position = 0,
        .parsed = false,
    };

    array_append(parser->definitions, def);

    return &array_tail(parser->definitions);
}

ArgParser *ArgParser_AddFlag(ArgParser *parser, const char *name, const char *description,
                            bool *target) {
    ArgDefinition *def = add_definition(parser, name, description, ARG_TYPE_FLAG, target);
    if (def && target) {
        *target = false;  // Initialize to false
        def->defaults.flag_default = false;
        def->has_default = true;
    }
    return parser;
}

ArgParser *ArgParser_AddString(ArgParser *parser, const char *name, const char *description,
                              const char **target) {
    ArgDefinition *def = add_definition(parser, name, description, ARG_TYPE_STRING, target);
    if (def && target) {
        *target = NULL;  // Initialize to NULL
    }
    return parser;
}

ArgParser *ArgParser_AddInt(ArgParser *parser, const char *name, const char *description,
                           int *target) {
    ArgDefinition *def = add_definition(parser, name, description, ARG_TYPE_INT, target);
    if (def && target) {
        *target = 0;  // Initialize to 0
    }
    return parser;
}

ArgParser *ArgParser_AddLongLong(ArgParser *parser, const char *name, const char *description,
                            long long *target) {
    ArgDefinition *def = add_definition(parser, name, description, ARG_TYPE_LONG_LONG, target);
    if (def && target) {
        *target = 0;  // Initialize to 0
    }
    return parser;
}

ArgParser *ArgParser_AddULongLong(ArgParser *parser, const char *name, const char *description,
                             unsigned long long *target) {
    ArgDefinition *def = add_definition(parser, name, description, ARG_TYPE_ULONG_LONG, target);
    if (def && target) {
        *target = 0;  // Initialize to 0
    }
    return parser;
}

ArgParser *ArgParser_AddDouble(ArgParser *parser, const char *name, const char *description,
                              double *target) {
    ArgDefinition *def = add_definition(parser, name, description, ARG_TYPE_DOUBLE, target);
    if (def && target) {
        *target = 0.0;  // Initialize to 0.0
    }
    return parser;
}

ArgParser *ArgParser_AddSubArgs(ArgParser *parser, const char *name, const char *description,
                               ArgsCursor *target, int min_args, int max_args) {
    ArgDefinition *def = add_definition(parser, name, description, ARG_TYPE_SUBARGS, target);
    if (def) {
        def->options.subargs.min_args = min_args;
        def->options.subargs.max_args = max_args;
    }
    return parser;
}

// Internal helper to access the last added definition (used by variadic option applier)
static ArgDefinition *get_last_definition(ArgParser *parser) {
    if (array_len(parser->definitions) == 0) return NULL;
    return &array_tail(parser->definitions);
}

static ArgDefinition *find_definition(ArgParser *parser, const char *name) {
    // Early return for empty definitions or null name
    if (!name || array_len(parser->definitions) == 0) {
        return NULL;
    }

    // Linear search - could be optimized with hash table for large numbers of arguments
    for (size_t i = 0; i < array_len(parser->definitions); i++) {
        ArgDefinition *def = &parser->definitions[i];
        if (strcasecmp(def->name, name) == 0) {
            return def;
        }
    }
    return NULL;
}

// Optimized lookup for positional arguments
static ArgDefinition *find_positional_definition(ArgParser *parser, uint16_t position, const char *name) {
    if (!parser || position < 1) return NULL;

    for (size_t i = 0; i < array_len(parser->definitions); i++) {
        ArgDefinition *def = &parser->definitions[i];
        if (def->has_position && def->position == position && (name == NULL || strcasecmp(def->name, name) == 0)) {
            return def;
        }
    }
    return NULL;
}



static void set_error(ArgParser *parser, const char *message, const char *arg_name) {
    parser->last_result.success = false;
    parser->last_result.error_message = message;
    parser->last_result.error_arg = arg_name;
    parser->last_result.error_position = parser->cursor->offset;
}

static int parse_single_arg(ArgParser *parser, ArgDefinition *def) {
    int rv = AC_OK;

    switch (def->type) {
        case ARG_TYPE_FLAG:
            if (def->target) {
                *(bool*)def->target = true;
            }
            break;
        case ARG_TYPE_BITFLAG: {
            if (def->target) {
                switch (def->options.bitflag.target_size) {
                    case sizeof(uint32_t):
                        *(uint32_t*)def->target |= (uint32_t)def->options.bitflag.mask;
                        break;
                    case sizeof(uint64_t):
                        *(uint64_t*)def->target |= (uint64_t)def->options.bitflag.mask;
                        break;
                    case sizeof(unsigned short):
                        *(unsigned short*)def->target |= (unsigned short)def->options.bitflag.mask;
                    break;
                    case sizeof(unsigned char):
                        *(unsigned char*)def->target |= (unsigned char)def->options.bitflag.mask;
                    break;
                    default:
                        set_error(parser, "Unsupported target size for bitwise flag", def->name);
                        break;
                }
            }
            break;
        }

        case ARG_TYPE_STRING: {
            const char *str_val;
            rv = AC_GetString(parser->cursor, &str_val, NULL, 0);
            if (rv == AC_OK && def->target) {
                // Validate against allowed values if specified
                if (def->options.string.allowed_values) {
                    bool found = false;
                    for (const char **allowed = def->options.string.allowed_values; *allowed; allowed++) {
                        if (strcasecmp(str_val, *allowed) == 0) {
                            found = true;
                            break;
                        }
                    }
                    if (!found) {
                        set_error(parser, "Invalid value for argument", def->name);
                        return AC_ERR_ELIMIT;
                    }
                }
                *(const char**)def->target = str_val;
            }
            break;
        }

        case ARG_TYPE_INT: {
            int int_val;
            rv = AC_GetInt(parser->cursor, &int_val, 0);
            if (rv == AC_OK && def->target) {
                // Range validation
                if (def->options.numeric.has_min && int_val < def->options.numeric.min_val) {
                    set_error(parser, "Value below minimum", def->name);
                    return AC_ERR_ELIMIT;
                }
                if (def->options.numeric.has_max && int_val > def->options.numeric.max_val) {
                    set_error(parser, "Value above maximum", def->name);
                    return AC_ERR_ELIMIT;
                }
                *(int*)def->target = int_val;
            }
            break;
        }

        case ARG_TYPE_LONG_LONG: {
            long long long_val;
            rv = AC_GetLongLong(parser->cursor, &long_val, 0);
            if (rv == AC_OK && def->target) {
                // Range validation
                if (def->options.numeric.has_min && long_val < def->options.numeric.min_val) {
                    set_error(parser, "Value below minimum", def->name);
                    return AC_ERR_ELIMIT;
                }
                if (def->options.numeric.has_max && long_val > def->options.numeric.max_val) {
                    set_error(parser, "Value above maximum", def->name);
                    return AC_ERR_ELIMIT;
                }
                *(long long*)def->target = long_val;
            }
            break;
        }

        case ARG_TYPE_ULONG_LONG: {
            unsigned long long ulong_val;
            rv = AC_GetUnsignedLongLong(parser->cursor, &ulong_val, 0);
            if (rv == AC_OK && def->target) {
                // Range validation (only max makes sense for unsigned)
                if (def->options.numeric.has_max && ulong_val > (unsigned long long)def->options.numeric.max_val) {
                    set_error(parser, "Value above maximum", def->name);
                    return AC_ERR_ELIMIT;
                }
                *(unsigned long long*)def->target = ulong_val;
            }
            break;
        }

        case ARG_TYPE_DOUBLE: {
            double double_val;
            rv = AC_GetDouble(parser->cursor, &double_val, 0);
            if (rv == AC_OK && def->target) {
                *(double*)def->target = double_val;
            }
            break;
        }

        case ARG_TYPE_SUBARGS: {
            unsigned int count = 0;
            const char *notEnoughArgMessage = NULL;
            if (def->options.subargs.max_args && def->options.subargs.min_args == def->options.subargs.max_args) {
                count = def->options.subargs.max_args;
                notEnoughArgMessage = "Not enough arguments were provided";
            } else {
                rv = AC_GetUnsigned(parser->cursor, &count, 0);
                notEnoughArgMessage = "Not enough arguments were provided based on argument count";
            }
            if (rv != AC_OK) {
                set_error(parser, "Failed to parse the argument count", def->name);
            } else if (count < (unsigned int)def->options.subargs.min_args || (def->options.subargs.max_args != -1 && count > (unsigned int)def->options.subargs.max_args)) {
                set_error(parser, "Invalid argument count", def->name);
                rv = AC_ERR_ELIMIT;
            } else {
                // Single argument slice
                rv = AC_GetSlice(parser->cursor, (ArgsCursor*)def->target, count);
                if (rv == AC_ERR_NOARG) {
                    set_error(parser, notEnoughArgMessage, def->name);
                }
            }
            break;
        }

        default:
            set_error(parser, "Unknown argument type", def->name);
            return AC_ERR_PARSE;
    }

    // Run custom validator if provided
    if (rv == AC_OK && def->validator) {
        const char *error_msg = NULL;
        if (def->validator(def->target, &error_msg) != 0) {
            set_error(parser, error_msg ? error_msg : "Validation failed", def->name);
            return AC_ERR_ELIMIT;
        }
    }

    // Run callback if provided
    if (rv == AC_OK && def->callback) {
        def->callback(parser, def->target, def->user_data);
    }

    return rv;
}

ArgParseResult ArgParser_Parse(ArgParser *parser) {
    static ArgParseResult null_result = {false, "Invalid parser or cursor", NULL, -1};
    if (!parser || !parser->cursor) {
        return null_result;
    }

    // Initialize result as success
    parser->last_result.success = true;
    parser->last_result.error_message = NULL;
    parser->last_result.error_arg = NULL;
    parser->last_result.error_position = -1;

    // Reset all parsed flags to false for this parse
    for (size_t i = 0; i < array_len(parser->definitions); i++) {
        parser->definitions[i].parsed = false;
    }

    // First pass: parse positional arguments in order
    uint16_t current_position = 1;
    while (!AC_IsAtEnd(parser->cursor)) {
        // Check if the current argument is a known named argument
        const char *def_name;
        int rv = AC_GetString(parser->cursor, &def_name, NULL, AC_F_NOADVANCE);
        if (rv != AC_OK) {
            set_error(parser, "Failed to read argument", NULL);
            break;
        }

        // Find positional argument for current position (optimized lookup)
        ArgDefinition *pos_def = find_positional_definition(parser, current_position, def_name);
        if (!pos_def) {
            // No more positional arguments, break to named argument parsing
            break;
        }

        // Check if this is a named argument (not positional)
        ArgDefinition *named_def = find_definition(parser, def_name);
        if (named_def && !named_def->has_position) {
            // This is a named argument, stop positional parsing
            break;
        }

        // This should be a positional argument value
        // Check if already parsed and not repeatable
        if (pos_def->parsed && !pos_def->repeatable) {
            set_error(parser, "Argument specified multiple times", pos_def->name);
            break;
        }

        // Advance the cursor to the argument value
        rv = AC_Advance(parser->cursor);
        if (rv != AC_OK) {
            set_error(parser, "Failed to parse past", pos_def->name);
            break;
        }
        // Parse the positional argument value directly (no name expected)
        rv = parse_single_arg(parser, pos_def);
        if (rv != AC_OK) {
            if (parser->last_result.success) {
                set_error(parser, AC_Strerror(rv), pos_def->name);
            }
            break;
        }

        pos_def->parsed = true;
        current_position++;
    }

    // Check for missing required positional arguments
    uint16_t check_position = current_position;
    while (true) {
        ArgDefinition *pos_def = find_positional_definition(parser, check_position, NULL);
        if (!pos_def) break;

        if (pos_def->required) {
            if (!pos_def->parsed) {
                set_error(parser, "Required positional argument missing or out of order", pos_def->name);
                break;
            }
        }
        check_position++;
    }

    // Second pass: parse remaining arguments (both named and positional)
    while (!AC_IsAtEnd(parser->cursor) && parser->last_result.success) {
        const char *arg_name;
        int rv = AC_GetString(parser->cursor, &arg_name, NULL, AC_F_NOADVANCE);
        if (rv != AC_OK) {
            set_error(parser, "Failed to read argument", NULL);
            break;
        }

        // Check if this is a known argument (named or positional)
        ArgDefinition *def = find_definition(parser, arg_name);
        if (!def) {
            // Check if this could be a positional argument value
            // Find the next unparsed positional argument
            ArgDefinition *pos_def = NULL;
            for (uint16_t pos = 1; pos <= MAX_POSITIONAL_ARGS; pos++) { // reasonable limit
                ArgDefinition *candidate = find_positional_definition(parser, pos, arg_name);
                if (!candidate) break;

                if (!candidate->parsed) {
                    pos_def = candidate;
                    break;
                }
            }

            if (pos_def) {
                // Advance past the argument name
                rv = AC_Advance(parser->cursor);
                if (rv != AC_OK) {
                    set_error(parser, "Failed to parse past", pos_def->name);
                    break;
                }
                // Parse as positional argument
                rv = parse_single_arg(parser, pos_def);
                if (rv != AC_OK) {
                    if (parser->last_result.success) {
                        set_error(parser, AC_Strerror(rv), pos_def->name);
                    }
                    break;
                }
                pos_def->parsed = true;
                continue;
            }

            // Unknown argument
            set_error(parser, "Unknown argument", arg_name);
            break;
        }

        // Skip if this is a positional argument that was already handled
        if (def->has_position) {
            if (def->parsed) {
                AC_Advance(parser->cursor);
                continue;
            }
        }

        // Check if already parsed and not repeatable
        if (def->parsed && !def->repeatable) {
            set_error(parser, "Argument specified multiple times", def->name);
            break;
        }

        // Advance past the argument name
        rv = AC_Advance(parser->cursor);
        if (rv != AC_OK) {
            set_error(parser, "Failed to parse past", def->name);
            break;
        }

        // Parse the argument value
        rv = parse_single_arg(parser, def);
        if (rv != AC_OK) {
            if (parser->last_result.success) {
                set_error(parser, AC_Strerror(rv), def->name);
            }
            break;
        }

        def->parsed = true;
    }

    // Check for required arguments that weren't parsed
    if (parser->last_result.success) {
        for (size_t i = 0; i < array_len(parser->definitions); i++) {
            if (parser->definitions[i].required && !parser->definitions[i].parsed) {
                set_error(parser, "Required argument missing", parser->definitions[i].name);
                break;
            }
        }
    }

    // Apply default values for unparsed optional arguments
    if (parser->last_result.success) {
        apply_defaults(parser);
    }

    return parser->last_result;
}

const char *ArgParser_GetErrorString(ArgParser *parser) {
    if (!parser || parser->last_result.success) {
        return NULL;
    }

    // Thread-safe: use parser's own buffer instead of static
    if (!parser->error_buffer) {
        parser->error_buffer = rm_malloc(MAX_ERROR_MSG_LEN);
        if (!parser->error_buffer) {
            return "Memory allocation failed for error message";
        }
    }

    if (parser->last_result.error_arg) {
        snprintf(parser->error_buffer, MAX_ERROR_MSG_LEN, "%s: %s",
                parser->last_result.error_arg, parser->last_result.error_message);
    } else {
        snprintf(parser->error_buffer, MAX_ERROR_MSG_LEN, "%s", parser->last_result.error_message);
    }

    return parser->error_buffer;
}

bool ArgParser_HasMore(ArgParser *parser) {
    if (!parser || !parser->cursor) return false;
    return !AC_IsAtEnd(parser->cursor);
}

bool ArgParser_WasParsed(ArgParser *parser, const char *arg_name) {
    if (!parser || !arg_name) {
        return false;
    }

    ArgDefinition *def = find_definition(parser, arg_name);
    if (!def) return false;

    return def->parsed;
}

// Common validators
int ArgParser_ValidatePositive(const void *value, const char **error_msg) {
    long long val = *(const long long*)value;
    if (val <= 0) {
        *error_msg = "Value must be positive";
        return -1;
    }
    return 0;
}

int ArgParser_ValidateNonNegative(const void *value, const char **error_msg) {
    long long val = *(const long long*)value;
    if (val < 0) {
        *error_msg = "Value must be non-negative";
        return -1;
    }
    return 0;
}

// Support for default values with variadic arguments

// Apply default values for unparsed optional arguments
static void apply_defaults(ArgParser *parser) {
    for (size_t i = 0; i < array_len(parser->definitions); i++) {
        ArgDefinition *def = &parser->definitions[i];

        // Skip if already parsed or no default
        if (def->parsed || !def->has_default || !def->target) {
            continue;
        }

        // Apply default value based on type
        switch (def->type) {
            case ARG_TYPE_FLAG:
                *(bool*)def->target = def->defaults.flag_default;
                break;
            case ARG_TYPE_STRING:
                *(const char**)def->target = def->defaults.str_default;
                break;
            case ARG_TYPE_INT:
                *(int*)def->target = (int)def->defaults.int_default;
                break;
            case ARG_TYPE_LONG_LONG:
                *(long long*)def->target = def->defaults.int_default;
                break;
            case ARG_TYPE_DOUBLE:
                *(double*)def->target = def->defaults.double_default;
                break;
            default:
                break;
        }
    }
}

// Helper function to apply variadic options to the last added definition
static void apply_variadic_options(ArgParser *parser, va_list args) {
    ArgDefinition *def = get_last_definition(parser);
    if (!def) return;

    ArgOption opt;
    while ((opt = va_arg(args, ArgOption)) != ARG_OPT_END) {
        switch (opt) {
            case ARG_OPT_REQUIRED:
                def->required = true;
                break;

            case ARG_OPT_OPTIONAL:
                def->required = false;
                break;

            case ARG_OPT_REPEATABLE:
                def->repeatable = true;
                break;

            case ARG_OPT_VALIDATOR:
                def->validator = va_arg(args, ArgValidator);
                break;

            case ARG_OPT_CALLBACK: {
                def->callback = va_arg(args, ArgCallback);
                def->user_data = va_arg(args, void*);
                break;
            }

            case ARG_OPT_RANGE:
                if (def->type == ARG_TYPE_INT || def->type == ARG_TYPE_LONG_LONG ||
                    def->type == ARG_TYPE_ULONG_LONG) {
                    def->options.numeric.min_val = va_arg(args, long long);
                    def->options.numeric.max_val = va_arg(args, long long);
                    def->options.numeric.has_min = true;
                    def->options.numeric.has_max = true;
                }
                break;

            case ARG_OPT_ALLOWED_VALUES:
                if (def->type == ARG_TYPE_STRING) {
                    def->options.string.allowed_values = va_arg(args, const char**);
                }
                break;

            case ARG_OPT_DEFAULT_STR:
                if (def->type == ARG_TYPE_STRING) {
                    def->defaults.str_default = va_arg(args, const char*);
                    def->has_default = true;
                }
                break;

            case ARG_OPT_DEFAULT_INT:
                if (def->type == ARG_TYPE_INT || def->type == ARG_TYPE_LONG_LONG) {
                    def->defaults.int_default = va_arg(args, long long);
                    def->has_default = true;
                }
                break;

            case ARG_OPT_DEFAULT_DOUBLE:
                if (def->type == ARG_TYPE_DOUBLE) {
                    def->defaults.double_default = va_arg(args, double);
                    def->has_default = true;
                }
                break;

            case ARG_OPT_DEFAULT_FLAG:
                if (def->type == ARG_TYPE_FLAG) {
                    def->defaults.flag_default = va_arg(args, int) ? true : false;
                    def->has_default = true;
                }
                break;

            case ARG_OPT_POSITION: {
                int pos = va_arg(args, int);
                if (pos < 1) pos = 1;
                def->has_position = true;
                def->position = pos;
                break;
            }

            default:
                // Unknown option, skip
                break;
        }
    }
}

// Variadic API implementations
ArgParser *ArgParser_AddBoolV(ArgParser *parser, const char *name, const char *description,
                             bool *target, ...) {
    ArgParser_AddFlag(parser, name, description, target);

    va_list args;
    va_start(args, target);
    apply_variadic_options(parser, args);
    va_end(args);

    return parser;
}

// Bitwise flag adder: when present, OR mask into the integer pointed by target
ArgParser *ArgParser_AddBitflagV(ArgParser *parser, const char *name, const char *description,
                                void *target, size_t target_size, unsigned long long mask, ...) {
    // Store mask and target info in definition
    // Add as a bitflag type with target assignment
    ArgDefinition *def = add_definition(parser, name, description, ARG_TYPE_BITFLAG, target);
    if (!def) return parser;

    def->options.bitflag.target_size = target_size;
    def->options.bitflag.mask = mask;

    va_list args;
    va_start(args, mask);
    apply_variadic_options(parser, args);
    va_end(args);
    return parser;
}

ArgParser *ArgParser_AddStringV(ArgParser *parser, const char *name, const char *description,
                               const char **target, ...) {
    ArgParser_AddString(parser, name, description, target);

    va_list args;
    va_start(args, target);
    apply_variadic_options(parser, args);
    va_end(args);

    return parser;
}

ArgParser *ArgParser_AddIntV(ArgParser *parser, const char *name, const char *description,
                            int *target, ...) {
    ArgParser_AddInt(parser, name, description, target);

    va_list args;
    va_start(args, target);
    apply_variadic_options(parser, args);
    va_end(args);

    return parser;
}

ArgParser *ArgParser_AddLongLongV(ArgParser *parser, const char *name, const char *description,
                             long long *target, ...) {
    ArgParser_AddLongLong(parser, name, description, target);

    va_list args;
    va_start(args, target);
    apply_variadic_options(parser, args);
    va_end(args);

    return parser;
}

ArgParser *ArgParser_AddULongLongV(ArgParser *parser, const char *name, const char *description,
                              unsigned long long *target, ...) {
    ArgParser_AddULongLong(parser, name, description, target);

    va_list args;
    va_start(args, target);
    apply_variadic_options(parser, args);
    va_end(args);

    return parser;
}

ArgParser *ArgParser_AddDoubleV(ArgParser *parser, const char *name, const char *description,
                               double *target, ...) {
    ArgParser_AddDouble(parser, name, description, target);

    va_list args;
    va_start(args, target);
    apply_variadic_options(parser, args);
    va_end(args);

    return parser;
}

ArgParser *ArgParser_AddSubArgsV(ArgParser *parser, const char *name, const char *description,
                                ArgsCursor *target, int min_args, int max_args, ...) {
    ArgParser_AddSubArgs(parser, name, description, target, min_args, max_args);

    va_list args;
    va_start(args, max_args);
    apply_variadic_options(parser, args);
    va_end(args);

    return parser;
}
