# ArgParser - Enhanced Argument Parsing for RediSearch

## Overview

ArgParser is a flexible argument parsing library built on top of ArgsCursor that provides a clean, declarative API for defining and parsing command-line arguments. It supports both named arguments (like `TIMEOUT 5000`) and positional arguments (arguments that must appear in specific positions).

## Key Benefits

- **Readability**: Declarative argument definitions instead of long if-else chains
- **Maintainability**: Adding new arguments requires minimal code changes
- **Reliability**: Built-in type validation and comprehensive error reporting
- **Flexibility**: Supports named arguments, positional arguments, bitflags, and custom validators
- **Performance**: Efficient bitflag operations for setting multiple flags

## Files

- `src/util/arg_parser.h` - Header file with API definitions
- `src/util/arg_parser.c` - Implementation
- `src/hybrid/parse/hybrid_optional_args.c` - Real-world usage examples

## Basic Usage

### Variadic API (Recommended)
```c
#include "util/arg_parser.h"

// Define variables to hold parsed values
bool verbose = false;
long long timeout = 5000;
const char *format = NULL;
int flags = 0;  // For bitflag operations

// Create parser
ArgParser *parser = ArgParser_New(&cursor, "MYCOMMAND");

// Boolean flag argument
ArgParser_AddBoolV(parser, "VERBOSE", "Enable verbose output", &verbose,
                  ARG_OPT_OPTIONAL,
                  ARG_OPT_DEFAULT_FLAG, 0,
                  ARG_OPT_END);

// Numeric argument with range validation
ArgParser_AddLongV(parser, "TIMEOUT", "Query timeout in ms", &timeout,
                  ARG_OPT_OPTIONAL,
                  ARG_OPT_RANGE, 100LL, 300000LL,
                  ARG_OPT_VALIDATOR, ArgParser_ValidatePositive,
                  ARG_OPT_DEFAULT_INT, 5000LL,
                  ARG_OPT_END);

// String argument with allowed values
const char *allowed_formats[] = {"json", "xml", "csv", NULL};
ArgParser_AddStringV(parser, "FORMAT", "Output format", &format,
                     ARG_OPT_OPTIONAL,
                     ARG_OPT_ALLOWED_VALUES, allowed_formats,
                     ARG_OPT_DEFAULT_STR, "json",
                     ARG_OPT_END);

// Bitflag argument - sets bit when present
#define FLAG_WITHSCORES 0x01
ArgParser_AddBitflagV(parser, "WITHSCORES", "Include scores in results",
                      &flags, sizeof(flags), FLAG_WITHSCORES,
                      ARG_OPT_OPTIONAL,
                      ARG_OPT_END);

// Positional argument (must be first argument after command)
const char *index_name = NULL;
ArgParser_AddStringV(parser, "INDEX", "Index name", &index_name,
                     ARG_OPT_REQUIRED,
                     ARG_OPT_POSITION, 1,
                     ARG_OPT_END);

// Parse all arguments
ArgParseResult result = ArgParser_Parse(parser);
if (!result.success) {
    printf("Error: %s\n", ArgParser_GetErrorString(parser));
    ArgParser_Free(parser);
    return -1;
}

// Use parsed values...
printf("Index: %s, Verbose: %s, Timeout: %lld, Format: %s, WithScores: %s\n",
       index_name, verbose ? "yes" : "no", timeout, format,
       (flags & FLAG_WITHSCORES) ? "yes" : "no");

ArgParser_Free(parser);
```

### Legacy Chained API (Still Available)
```c
// For compatibility, the old chained API still works
ArgParser_AddFlag(parser, "VERBOSE", "Enable verbose output", &verbose)
    ->Optional()
    ->WithDefault(0);

ArgParser_AddLong(parser, "TIMEOUT", "Query timeout", &timeout)
    ->Optional()
    ->WithRange(100, 300000)
    ->WithValidator(ArgParser_ValidatePositive);
```

## Comparison: Old vs New

### Before (handleCommonArgs style):
```c
if (AC_AdvanceIfMatch(ac, "LIMIT")) {
    if (AC_NumRemaining(ac) < 2) {
        QueryError_SetError(status, QUERY_EPARSEARGS, "LIMIT requires two arguments");
        return ARG_ERROR;
    }
    if ((rv = AC_GetU64(ac, &arng->offset, 0)) != AC_OK ||
        (rv = AC_GetU64(ac, &arng->limit, 0)) != AC_OK) {
        QueryError_SetError(status, QUERY_EPARSEARGS, "LIMIT needs two numeric arguments");
        return ARG_ERROR;
    }
    // ... 15+ more lines of validation
} else if (AC_AdvanceIfMatch(ac, "TIMEOUT")) {
    // ... 10+ more lines
}
// ... continues for 100+ lines
```

### After (ArgParser style):
```c
ArgParser *parser = ArgParser_New(ac, "AGGREGATE");

// Using variadic API (recommended)
ArgsCursor limit_args;
ArgParser_AddSubArgsV(parser, "LIMIT", "Limit results", &limit_args, 2, 2,
                      ARG_OPT_OPTIONAL,
                      ARG_OPT_CALLBACK, handle_limit, &result,
                      ARG_OPT_END);

long long timeout = 0;
ArgParser_AddLongV(parser, "TIMEOUT", "Query timeout", &timeout,
                   ARG_OPT_OPTIONAL,
                   ARG_OPT_VALIDATOR, ArgParser_ValidateNonNegative,
                   ARG_OPT_END);

ArgParseResult parse_result = ArgParser_Parse(parser);
if (!parse_result.success) {
    QueryError_SetError(status, QUERY_EPARSEARGS, ArgParser_GetErrorString(parser));
    ArgParser_Free(parser);
    return ARG_ERROR;
}
// Use parsed values...
ArgParser_Free(parser);
```

## API Reference

### Core Functions
- `ArgParser_New(cursor, command_name)` - Create new parser
- `ArgParser_Free(parser)` - Free parser and resources
- `ArgParser_Parse(parser)` - Parse all arguments, returns `ArgParseResult`
- `ArgParser_GetErrorString(parser)` - Get detailed error message
- `ArgParser_WasParsed(parser, arg_name)` - Check if argument was parsed
- `ArgParser_SetStrictMode(parser, strict)` - Enable/disable strict mode
- `ArgParser_GetRemainingCount(parser)` - Get count of unparsed arguments
- `ArgParser_GetRemainingArgs(parser)` - Get cursor to remaining arguments

### Variadic Argument Types (Recommended)
- `ArgParser_AddBoolV()` - Boolean flags (true when present)
- `ArgParser_AddBitflagV()` - Bitwise flags (OR mask into target)
- `ArgParser_AddStringV()` - String arguments
- `ArgParser_AddIntV()` / `ArgParser_AddLongV()` / `ArgParser_AddULongV()` - Numeric arguments
- `ArgParser_AddDoubleV()` - Double precision floating point
- `ArgParser_AddSubArgsV()` - Variable sub-arguments (like `LIMIT 0 10`)

### Legacy Chained API
- `ArgParser_AddFlag()` - Boolean flags
- `ArgParser_AddString()` - String arguments
- `ArgParser_AddInt()` / `ArgParser_AddLong()` / `ArgParser_AddULong()` - Numeric arguments
- `ArgParser_AddDouble()` - Double precision floating point
- `ArgParser_AddSubArgs()` - Variable sub-arguments

### Additional Parsing Methods
- `ArgParser_ParseNext(parser)` - Parse single argument (for incremental parsing)
- `ArgParser_HasMore(parser)` - Check if more arguments are available
- `ArgParser_PrintHelp(parser)` - Print help information for all arguments

### Constraints (Chainable for Legacy API)
- `->Required()` / `->Optional()` - Set requirement
- `->WithRange(min, max)` - Numeric range validation
- `->WithAllowedValues(values)` - String value validation
- `->WithValidator(func)` - Custom validation
- `->WithCallback(func, data)` - Custom processing
- `->WithDefault(value)` - Default values

### Variadic Options (for `*V` functions)
All variadic functions accept these options, terminated by `ARG_OPT_END`:

- `ARG_OPT_REQUIRED` / `ARG_OPT_OPTIONAL` - Set requirement (optional is default)
- `ARG_OPT_REPEATABLE` - Allow argument to appear multiple times
- `ARG_OPT_POSITION, pos` - Make argument positional (1-based position)
- `ARG_OPT_RANGE, min, max` - Set numeric range validation
- `ARG_OPT_ALLOWED_VALUES, array` - Set allowed string values (NULL-terminated array)
- `ARG_OPT_VALIDATOR, func` - Set custom validation function
- `ARG_OPT_CALLBACK, func, data` - Set callback function with user data
- `ARG_OPT_DEFAULT_STR, value` - Set default string value
- `ARG_OPT_DEFAULT_INT, value` - Set default integer value
- `ARG_OPT_DEFAULT_DOUBLE, value` - Set default double value
- `ARG_OPT_DEFAULT_FLAG, value` - Set default boolean value

### Built-in Validators
- `ArgParser_ValidatePositive()` - Ensures value > 0
- `ArgParser_ValidateNonNegative()` - Ensures value >= 0
- `ArgParser_ValidateRange()` - Uses parser context for range validation

### Custom Validators
You can create custom validation functions with this signature:
```c
typedef int (*ArgValidator)(const void *value, const char **error_msg);

// Example custom validator
int validate_port_number(const void *value, const char **error_msg) {
    long long port = *(const long long*)value;
    if (port < 1 || port > 65535) {
        *error_msg = "Port number must be between 1 and 65535";
        return -1;
    }
    return 0;
}

// Usage
ArgParser_AddLongV(parser, "PORT", "Server port", &port,
                   ARG_OPT_REQUIRED,
                   ARG_OPT_VALIDATOR, validate_port_number,
                   ARG_OPT_END);
```

## Advanced Features

### Positional Arguments
Arguments can be positional (parsed by position rather than name):
```c
// First argument after command must be index name
ArgParser_AddStringV(parser, "INDEX", "Index name", &index_name,
                     ARG_OPT_REQUIRED,
                     ARG_OPT_POSITION, 1,
                     ARG_OPT_END);

// Second argument is the query
ArgParser_AddStringV(parser, "QUERY", "Search query", &query,
                     ARG_OPT_REQUIRED,
                     ARG_OPT_POSITION, 2,
                     ARG_OPT_END);
```

### Bitflag Arguments
Efficiently set bits in integer flags:
```c
int request_flags = 0;
#define QEXEC_F_SEND_SCORES 0x01
#define QEXEC_F_IS_CURSOR   0x02

// When WITHSCORES is present, OR the flag into request_flags
ArgParser_AddBitflagV(parser, "WITHSCORES", "Include scores",
                      &request_flags, sizeof(request_flags), QEXEC_F_SEND_SCORES,
                      ARG_OPT_OPTIONAL, ARG_OPT_END);

ArgParser_AddBitflagV(parser, "WITHCURSOR", "Enable cursor",
                      &request_flags, sizeof(request_flags), QEXEC_F_IS_CURSOR,
                      ARG_OPT_OPTIONAL, ARG_OPT_END);
```

### Sub-Arguments
Handle complex arguments with multiple values:
```c
ArgsCursor limit_args;
ArgParser_AddSubArgsV(parser, "LIMIT", "Limit results", &limit_args, 2, 2,
                      ARG_OPT_OPTIONAL,
                      ARG_OPT_CALLBACK, handle_limit_callback, ctx,
                      ARG_OPT_END);

// In callback:
static void handle_limit_callback(ArgParser *parser, void *target, void *user_data) {
    ArgsCursor *args = (ArgsCursor*)target;
    MyContext *ctx = (MyContext*)user_data;

    AC_GetU64(args, &ctx->offset, 0);
    AC_GetU64(args, &ctx->limit, 0);
}
```

## Migration Guide

1. **Identify** current parsing logic with `AC_AdvanceIfMatch` chains
2. **Create** variables to hold parsed arguments
3. **Replace** with ArgParser variadic definitions
4. **Use** parsed values directly (no need to apply to context in most cases)
5. **Handle** errors using `ArgParseResult` and `ArgParser_GetErrorString()`

### Migration Example
```c
// Old style:
if (AC_AdvanceIfMatch(ac, "TIMEOUT")) {
    long long timeout;
    if (AC_GetLongLong(ac, &timeout, 0) != AC_OK) {
        return error;
    }
    if (timeout < 0) {
        return error;
    }
    ctx->timeout = timeout;
}

// New style:
long long timeout = 0;
ArgParser_AddLongV(parser, "TIMEOUT", "Query timeout", &timeout,
                   ARG_OPT_OPTIONAL,
                   ARG_OPT_VALIDATOR, ArgParser_ValidateNonNegative,
                   ARG_OPT_END);
// After parsing, timeout is automatically set
```

## Error Handling

ArgParser provides comprehensive error reporting through the `ArgParseResult` structure:

```c
typedef struct ArgParseResult {
    bool success;               // Whether parsing succeeded
    const char *error_message;  // Detailed error message
    const char *error_arg;      // Which argument caused the error
    int error_position;         // Position in argument list where error occurred
} ArgParseResult;
```

### Error Handling Example
```c
ArgParseResult result = ArgParser_Parse(parser);
if (!result.success) {
    printf("Parse error: %s", ArgParser_GetErrorString(parser));
    if (result.error_arg) {
        printf(" (argument: %s)", result.error_arg);
    }
    if (result.error_position >= 0) {
        printf(" (position: %d)", result.error_position);
    }
    printf("\n");
    ArgParser_Free(parser);
    return -1;
}
```

### Strict Mode
By default, ArgParser operates in strict mode, which means it will fail if it encounters unknown arguments. You can disable strict mode to allow unknown arguments to be ignored:

```c
// Disable strict mode - unknown arguments will be ignored
ArgParser_SetStrictMode(parser, false);

// After parsing, get remaining unparsed arguments
if (ArgParser_GetRemainingCount(parser) > 0) {
    ArgsCursor *remaining = ArgParser_GetRemainingArgs(parser);
    // Process remaining arguments manually if needed
}
```

## Real-World Usage

See `src/hybrid/parse/hybrid_optional_args.c` for a complete real-world example of ArgParser usage in the RediSearch hybrid search feature. This example demonstrates:

- Complex argument parsing with callbacks
- Bitflag operations for setting request flags
- Error handling integration with QueryError
- Mixed positional and named arguments

This provides a unified, clean code structure that avoids complex parsing logic and makes user code easily readable and maintainable.
