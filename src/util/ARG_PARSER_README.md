# ArgParser - Enhanced Argument Parsing for RediSearch

## Overview

ArgParser is a flexible argument parsing library built on top of ArgsCursor that provides a clean, declarative API for defining and parsing command-line arguments.

## Key Benefits

- **Readability**: Declarative argument definitions instead of long if-else chains
- **Maintainability**: Adding new arguments requires minimal code changes  
- **Reliability**: Built-in type validation and comprehensive error reporting
- **Flexibility**: Fluent API with custom validators and callbacks

## Files

- `src/util/arg_parser.h` - Header file with API definitions
- `src/util/arg_parser.c` - Implementation
- `src/aggregate/aggregate_request_refactored.c` - Example refactoring of handleCommonArgs

## Basic Usage

### New Variadic API (Recommended)
```c
#include "util/arg_parser.h"

// Define variables to hold parsed values
bool verbose = false;
long long timeout = 5000;
const char *format = NULL;

// Create parser
ArgParser *parser = ArgParser_New(&cursor, "MYCOMMAND");

// Define arguments with ALL configuration in ONE call each!
ArgParser_AddBoolV(parser, "VERBOSE", "Enable verbose output", &verbose,
                  ARG_OPT_OPTIONAL,
                  ARG_OPT_DEFAULT_FLAG, 0,
                  ARG_OPT_END);

ArgParser_AddLongV(parser, "TIMEOUT", "Query timeout in ms", &timeout,
                  ARG_OPT_OPTIONAL,
                  ARG_OPT_RANGE, 100LL, 300000LL,
                  ARG_OPT_VALIDATOR, ArgParser_ValidatePositive,
                  ARG_OPT_DEFAULT_INT, 5000LL,
                  ARG_OPT_END);

const char *allowed_formats[] = {"json", "xml", "csv", NULL};
ArgParser_AddStringV(parser, "FORMAT", "Output format", &format,
                     ARG_OPT_OPTIONAL,
                     ARG_OPT_ALLOWED_VALUES, allowed_formats,
                     ARG_OPT_DEFAULT_STR, "json",
                     ARG_OPT_END);

// Parse all arguments
ArgParseResult result = ArgParser_Parse(parser);
if (!result.success) {
    printf("Error: %s\n", ArgParser_GetErrorString(parser));
    ArgParser_Free(parser);
    return -1;
}

// Use parsed values...
printf("Verbose: %s, Timeout: %lld, Format: %s\n",
       verbose ? "yes" : "no", timeout, format);

ArgParser_Free(parser);
```

### Legacy Chained API (Still Available)
```c
// For compatibility, the old chained API still works
ArgParser_AddFlag(parser, "VERBOSE", "Enable verbose output", &verbose);
ArgParser_Optional(parser);
ArgParser_WithDefault(parser, 0);
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

ArgParser_AddSubArgs(parser, "LIMIT", "Limit results", NULL, 2, 2)
    ->Optional()
    ->WithCallback(handle_limit, &result);

ArgParser_AddLong(parser, "TIMEOUT", "Query timeout", &result.timeout)
    ->Optional()
    ->WithValidator(ArgParser_ValidateNonNegative);

ArgParseResult parse_result = ArgParser_Parse(parser);
if (!parse_result.success) {
    QueryError_SetError(status, QUERY_EPARSEARGS, ArgParser_GetErrorString(parser));
    return ARG_ERROR;
}
// Apply results...
```

## API Reference

### Core Functions
- `ArgParser_New()` / `ArgParser_Free()` - Constructor/destructor
- `ArgParser_Parse()` - Parse all arguments
- `ArgParser_GetErrorString()` - Get error message
- `ArgParser_WasParsed()` - Check if argument was parsed

### Argument Types
- `ArgParser_AddFlag()` - Boolean flags
- `ArgParser_AddString()` - String arguments
- `ArgParser_AddInt()` / `ArgParser_AddLong()` - Numeric arguments
- `ArgParser_AddSubArgs()` - Variable sub-arguments

### Constraints (Chainable)
- `->Required()` / `->Optional()` - Set requirement
- `->WithRange(min, max)` - Numeric range validation
- `->WithAllowedValues(values)` - String value validation
- `->WithValidator(func)` - Custom validation
- `->WithCallback(func, data)` - Custom processing
- `->WithDefault(value)` - Default values

## Migration Guide

1. **Identify** current parsing logic with `AC_AdvanceIfMatch` chains
2. **Create** structure to hold parsed arguments
3. **Replace** with ArgParser definitions and single parse call
4. **Apply** parsed results to your context

This provides exactly what you wanted: a unified, clean code structure that avoids complex parsing logic and makes user code easily readable.
