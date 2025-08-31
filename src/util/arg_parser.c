/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
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

// Internal helper functions
static int resize_definitions(ArgParser *parser);
static ArgDefinition *find_definition(ArgParser *parser, const char *name);
static int parse_single_arg(ArgParser *parser, ArgDefinition *def);
static void set_error(ArgParser *parser, const char *message, const char *arg_name);
static void apply_defaults(ArgParser *parser);

// Context and wrapper for bitwise flag arguments
typedef struct {
    void *target;
    size_t size;
    unsigned long long mask;
    ArgCallback user_cb;
    void *user_ud;
} BitFlagCtx;

static void bitflag_wrapper_cb(ArgParser *p, const void *v, void *ud) {
    BitFlagCtx *ctx = (BitFlagCtx*)ud;
    if (ctx && ctx->target) {
        switch (ctx->size) {
            case sizeof(uint32_t):
                *(uint32_t*)ctx->target |= (uint32_t)ctx->mask;
                break;
            case sizeof(uint64_t):
                *(uint64_t*)ctx->target |= (uint64_t)ctx->mask;
                break;
            default:
                if (ctx->size == sizeof(unsigned short)) {
                    *(unsigned short*)ctx->target |= (unsigned short)ctx->mask;
                } else if (ctx->size == sizeof(unsigned char)) {
                    *(unsigned char*)ctx->target |= (unsigned char)ctx->mask;
                }
                break;
        }
    }
    if (ctx && ctx->user_cb) {
        ctx->user_cb(p, v, ctx->user_ud);
    }
}

ArgParser *ArgParser_New(ArgsCursor *cursor, const char *command_name) {
    ArgParser *parser = rm_calloc(1, sizeof(ArgParser));
    if (!parser) return NULL;

    parser->cursor = cursor;
    parser->command_name = command_name ? rm_strdup(command_name) : NULL;
    parser->definitions = rm_calloc(INITIAL_DEF_CAPACITY, sizeof(ArgDefinition));
    parser->def_count = 0;
    parser->strict_mode = true;

    if (!parser->definitions) {
        rm_free(parser);
        return NULL;
    }

    return parser;
}

void ArgParser_Free(ArgParser *parser) {
    if (!parser) return;

    // Free allocated strings in definitions
    for (size_t i = 0; i < parser->def_count; i++) {
        ArgDefinition *def = &parser->definitions[i];
        if (def->name) rm_free((void*)def->name);
        if (def->description) rm_free((void*)def->description);
    }

    rm_free(parser->definitions);
    rm_free(parser->parsed_flags);
    rm_free((void*)parser->command_name);
    rm_free(parser);
}

static int resize_definitions(ArgParser *parser) {
    size_t new_capacity = parser->def_count * 2;
    if (new_capacity < INITIAL_DEF_CAPACITY) {
        new_capacity = INITIAL_DEF_CAPACITY;
    }

    ArgDefinition *new_defs = rm_realloc(parser->definitions,
                                        new_capacity * sizeof(ArgDefinition));
    if (!new_defs) return -1;

    parser->definitions = new_defs;
    return 0;
}

static ArgDefinition *add_definition(ArgParser *parser, const char *name,
                                   const char *description, ArgType type, void *target) {
    if (parser->def_count >= INITIAL_DEF_CAPACITY && resize_definitions(parser) != 0) {
        return NULL;
    }

    ArgDefinition *def = &parser->definitions[parser->def_count++];
    memset(def, 0, sizeof(ArgDefinition));

    def->name = rm_strdup(name);
    def->description = description ? rm_strdup(description) : NULL;
    def->type = type;
    def->target = target;
    def->required = false;
    def->repeatable = false;
    def->has_default = false;
    def->has_position = false;
    def->position = 0;

    return def;
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

ArgParser *ArgParser_AddLong(ArgParser *parser, const char *name, const char *description,
                            long long *target) {
    ArgDefinition *def = add_definition(parser, name, description, ARG_TYPE_LONG, target);
    if (def && target) {
        *target = 0;  // Initialize to 0
    }
    return parser;
}

ArgParser *ArgParser_AddULong(ArgParser *parser, const char *name, const char *description,
                             unsigned long long *target) {
    ArgDefinition *def = add_definition(parser, name, description, ARG_TYPE_ULONG, target);
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
    if (parser->def_count == 0) return NULL;
    return &parser->definitions[parser->def_count - 1];
}

static ArgDefinition *find_definition(ArgParser *parser, const char *name) {
    for (size_t i = 0; i < parser->def_count; i++) {
        if (strcasecmp(parser->definitions[i].name, name) == 0) {
            return &parser->definitions[i];
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

        case ARG_TYPE_LONG: {
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

        case ARG_TYPE_ULONG: {
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
            if (def->options.subargs.max_args == 1) {
                // Single argument slice
                rv = AC_GetSlice(parser->cursor, (ArgsCursor*)def->target, 1);
            } else {
                // Variable arguments
                rv = AC_GetVarArgs(parser->cursor, (ArgsCursor*)def->target);
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
    if (!parser || !parser->cursor) {
        parser->last_result.success = false;
        parser->last_result.error_message = "Invalid parser or cursor";
        return parser->last_result;
    }

    // Initialize result as success
    parser->last_result.success = true;
    parser->last_result.error_message = NULL;
    parser->last_result.error_arg = NULL;
    parser->last_result.error_position = -1;

    // Allocate parsed flags tracking
    if (!parser->parsed_flags) {
        parser->parsed_flags = rm_calloc(parser->def_count, sizeof(bool));
        if (!parser->parsed_flags) {
            set_error(parser, "Memory allocation failed", NULL);
            return parser->last_result;
        }
    }

    // First pass: parse positional arguments in order
    int current_position = 1;
    while (!AC_IsAtEnd(parser->cursor)) {
        // Find positional argument for current position
        ArgDefinition *pos_def = NULL;
        for (size_t i = 0; i < parser->def_count; i++) {
            if (parser->definitions[i].has_position &&
                parser->definitions[i].position == current_position) {
                pos_def = &parser->definitions[i];
                break;
            }
        }

        if (!pos_def) {
            // No more positional arguments, break to named argument parsing
            break;
        }

        // Check if this looks like a named argument (starts with keyword)
        const char *arg_name;
        int rv = AC_GetString(parser->cursor, &arg_name, NULL, AC_F_NOADVANCE);
        if (rv != AC_OK) {
            set_error(parser, "Failed to read argument", NULL);
            break;
        }

        // If this matches the expected positional argument name, parse it
        if (strcmp(arg_name, pos_def->name) == 0) {
            // Advance past the argument name
            AC_Advance(parser->cursor);

            // Check if already parsed and not repeatable
            size_t def_index = pos_def - parser->definitions;
            if (parser->parsed_flags[def_index] && !pos_def->repeatable) {
                set_error(parser, "Argument specified multiple times", pos_def->name);
                break;
            }

            // Parse the argument value
            rv = parse_single_arg(parser, pos_def);
            if (rv != AC_OK) {
                if (parser->last_result.success) {
                    set_error(parser, AC_Strerror(rv), pos_def->name);
                }
                break;
            }

            parser->parsed_flags[def_index] = true;
            current_position++;
        } else {
            // Expected positional argument not found at this position
            if (pos_def->required) {
                set_error(parser, "Required positional argument missing or out of order", pos_def->name);
                break;
            } else {
                // Skip this optional positional argument
                current_position++;
            }
        }
    }

    // Second pass: parse remaining named arguments
    while (!AC_IsAtEnd(parser->cursor) && parser->last_result.success) {
        const char *arg_name;
        int rv = AC_GetString(parser->cursor, &arg_name, NULL, AC_F_NOADVANCE);
        if (rv != AC_OK) {
            set_error(parser, "Failed to read argument", NULL);
            break;
        }

        ArgDefinition *def = find_definition(parser, arg_name);
        if (!def) {
            if (parser->strict_mode) {
                set_error(parser, "Unknown argument", arg_name);
                break;
            } else {
                // Skip unknown argument in non-strict mode
                AC_Advance(parser->cursor);
                continue;
            }
        }

        // Skip if this is a positional argument (already handled)
        if (def->has_position) {
            AC_Advance(parser->cursor);
            continue;
        }

        // Advance past the argument name
        AC_Advance(parser->cursor);

        // Check if already parsed and not repeatable
        size_t def_index = def - parser->definitions;
        if (parser->parsed_flags[def_index] && !def->repeatable) {
            set_error(parser, "Argument specified multiple times", def->name);
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

        parser->parsed_flags[def_index] = true;
    }

    // Check for required arguments that weren't parsed
    if (parser->last_result.success) {
        for (size_t i = 0; i < parser->def_count; i++) {
            if (parser->definitions[i].required && !parser->parsed_flags[i]) {
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

    static char error_buffer[MAX_ERROR_MSG_LEN];
    if (parser->last_result.error_arg) {
        snprintf(error_buffer, sizeof(error_buffer), "%s: %s",
                parser->last_result.error_arg, parser->last_result.error_message);
    } else {
        snprintf(error_buffer, sizeof(error_buffer), "%s", parser->last_result.error_message);
    }

    return error_buffer;
}

bool ArgParser_WasParsed(ArgParser *parser, const char *arg_name) {
    if (!parser || !parser->parsed_flags || !arg_name) {
        return false;
    }

    ArgDefinition *def = find_definition(parser, arg_name);
    if (!def) return false;

    size_t def_index = def - parser->definitions;
    return parser->parsed_flags[def_index];
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
    for (size_t i = 0; i < parser->def_count; i++) {
        ArgDefinition *def = &parser->definitions[i];

        // Skip if already parsed or no default
        if (parser->parsed_flags[i] || !def->has_default || !def->target) {
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
            case ARG_TYPE_LONG:
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

ArgParser *ArgParser_SetStrictMode(ArgParser *parser, bool strict) {
    if (parser) {
        parser->strict_mode = strict;
    }
    return parser;
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
                if (def->type == ARG_TYPE_INT || def->type == ARG_TYPE_LONG ||
                    def->type == ARG_TYPE_UINT || def->type == ARG_TYPE_ULONG) {
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
                if (def->type == ARG_TYPE_INT || def->type == ARG_TYPE_LONG) {
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
    // Store mask and target info in definition's user_data via callback
    // We wrap the user's callback (if any) to perform the OR before invoking it
    // Add as a boolean presence flag (no direct target assignment)
    ArgDefinition *def = add_definition(parser, name, description, ARG_TYPE_FLAG, NULL);
    if (!def) return parser;

    BitFlagCtx *bf = rm_malloc(sizeof(*bf));
    if (!bf) return parser;
    bf->target = target; bf->size = target_size; bf->mask = mask; bf->user_cb = NULL; bf->user_ud = NULL;

    // Capture variadic options to detect a user-supplied callback and user_data
    va_list args; va_start(args, mask);
    // Temporarily intercept options to extract ARG_OPT_CALLBACK if provided
    // We'll re-run apply_variadic options afterwards to set other options
    ArgOption opt; ArgOption first_non_cb_opt = ARG_OPT_END; void *saved_user_ud = NULL; ArgCallback saved_user_cb = NULL;
    while ((opt = va_arg(args, ArgOption)) != ARG_OPT_END) {
        if (opt == ARG_OPT_CALLBACK) {
            saved_user_cb = va_arg(args, ArgCallback);
            saved_user_ud = va_arg(args, void*);
            continue;
        }
        if (first_non_cb_opt == ARG_OPT_END) first_non_cb_opt = opt;
        // Skip payloads of other options conservatively
        switch (opt) {
            case ARG_OPT_REQUIRED:
            case ARG_OPT_OPTIONAL:
            case ARG_OPT_REPEATABLE:
                break;
            case ARG_OPT_VALIDATOR:
                (void)va_arg(args, ArgValidator); break;
            case ARG_OPT_RANGE:
                (void)va_arg(args, long long); (void)va_arg(args, long long); break;
            case ARG_OPT_ALLOWED_VALUES:
                (void)va_arg(args, const char**); break;
            case ARG_OPT_DEFAULT_STR:
                (void)va_arg(args, const char*); break;
            case ARG_OPT_DEFAULT_INT:
                (void)va_arg(args, long long); break;
            case ARG_OPT_DEFAULT_DOUBLE:
                (void)va_arg(args, double); break;
            case ARG_OPT_DEFAULT_FLAG:
                (void)va_arg(args, int); break;
            default:
                break;
        }
    }
    va_end(args);

    // Set wrapped callback that performs the OR then forwards to user callback
    def->callback = bitflag_wrapper_cb;
    bf->user_cb = saved_user_cb;
    bf->user_ud = saved_user_ud;
    def->user_data = bf;

    // Now apply original variadic options again (they'll set repeatable/required/etc.)
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

ArgParser *ArgParser_AddLongV(ArgParser *parser, const char *name, const char *description,
                             long long *target, ...) {
    ArgParser_AddLong(parser, name, description, target);

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
