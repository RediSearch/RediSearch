# RediSearch Error Message Prefix Standardization

This document summarizes the error message changes made to support unique prefixes for Redis commandstats tracking.

## Overview

All RediSearch error messages now include unique `SEARCH_` prefixes to enable proper tracking in Redis commandstats, which uses the first token of an error message as its identifier.

## Prefix Categories

### Core Error Prefixes

| Prefix | Description | Usage |
|--------|-------------|-------|
| `SEARCH_INDEX_NOT_FOUND` | Index does not exist | When trying to access a non-existent index |
| `SEARCH_CURSOR_NOT_FOUND` | Cursor does not exist | When trying to access a non-existent cursor |
| `SEARCH_UNKNOWN_ARG` | Unknown command argument | When an unrecognized argument is provided |
| `SEARCH_VALUE_BAD` | Invalid value provided | When a parameter has an invalid value |
| `SEARCH_SYNTAX_ERR` | Query syntax error | When query parsing fails |
| `SEARCH_PARSE_ARG_ERR` | Argument parsing error | When command argument parsing fails |

### Rust-Defined Prefixes

These prefixes are defined in the Rust `query_error` module and accessed via `QueryError_Strerror()`:

| Error Code | Prefix | Description |
|------------|--------|-------------|
| `QUERY_ERROR_CODE_NO_INDEX` | `SEARCH_INDEX_NOT_FOUND` | Index not found |
| `QUERY_ERROR_CODE_PARSE_ARGS` | `SEARCH_PARSE_ARG_ERR` | Argument parsing errors |
| `QUERY_ERROR_CODE_SYNTAX` | `SEARCH_SYNTAX_ERR` | Syntax errors |
| `QUERY_ERROR_CODE_INVAL` | `SEARCH_VALUE_BAD` | Invalid values |
| `QUERY_ERROR_CODE_LIMIT` | `SEARCH_LIMIT_EXCEEDED` | Resource limits exceeded |

## Files Modified

### Core Infrastructure
- `src/query_error_compat.c` - Added prefix extraction logic
- `src/vector_index.c` - Updated error message parameter order

### Error Message Sites
- `src/module.c` - 3 instances updated
- `src/aggregate/aggregate_exec.c` - 4 instances updated
- `src/hybrid/hybrid_exec.c` - 1 instance updated
- `src/hybrid/hybrid_debug.c` - 1 instance updated
- `src/coord/hybrid/dist_hybrid.c` - 1 instance updated
- `src/spec.c` - 3 instances updated
- `src/hybrid/parse_hybrid.c` - 2 instances updated
- `src/aggregate/aggregate_request.c` - 3 instances updated

## Error Message Changes

### Messages with Rephrasing (Not Just Prefixed)

#### Index Not Found Errors
```diff
- "No such index"
+ "SEARCH_INDEX_NOT_FOUND: Index not found"
```
**Rationale**: "Index not found" is clearer and more standard than "No such index"

#### Field Definition Error
```diff
- "'Field cannot be defined with both `NOINDEX` and `INDEXMISSING` `%s` '"
+ "SEARCH_PARSE_ARG_ERR: Field cannot be defined with both `NOINDEX` and `INDEXMISSING` `%s`"
```
**Rationale**: Cleaned up inconsistent quote usage

### Messages with Only Prefix Added

#### Cursor Errors
```diff
- "Cursor not found, id: %d"
+ "SEARCH_CURSOR_NOT_FOUND: Cursor not found, id: %d"
```

#### Argument Errors
```diff
- "Unknown argument `%s`"
+ "SEARCH_UNKNOWN_ARG: Unknown argument `%s`"

- "Bad value for COUNT: `%s`"
+ "SEARCH_VALUE_BAD: Bad value for COUNT: `%s`"
```

#### Parsing Errors
```diff
- "FORMAT %s is not supported"
+ "SEARCH_PARSE_ARG_ERR: FORMAT %s is not supported"

- "MISSING ASC or DESC after sort field (%s)"
+ "SEARCH_PARSE_ARG_ERR: MISSING ASC or DESC after sort field (%s)"

- "TRAINING_THRESHOLD is irrelevant when compression was not requested"
+ "SEARCH_PARSE_ARG_ERR: TRAINING_THRESHOLD is irrelevant when compression was not requested"

- "REDUCE is irrelevant when compression is not of type LeanVec"
+ "SEARCH_PARSE_ARG_ERR: REDUCE is irrelevant when compression is not of type LeanVec"
```

#### Syntax Errors
```diff
- "Not enough arguments in %s, specified %llu but provided only %u"
+ "SEARCH_SYNTAX_ERR: Not enough arguments in %s, specified %llu but provided only %u"

- "Bad arguments in %s: %s"
+ "SEARCH_SYNTAX_ERR: Bad arguments in %s: %s"

- "Expected a VECTOR field `%s`"
+ "SEARCH_SYNTAX_ERR: Expected a VECTOR field `%s`"
```

## Implementation Pattern

### C Code Error Messages
For C code using `QueryError_SetWithUserDataFmt`:

```c
// Before:
QueryError_SetWithUserDataFmt(&status, QUERY_ERROR_CODE_NO_INDEX, "No such index", " %s", indexname);

// After:
QueryError_SetWithUserDataFmt(&status, QUERY_ERROR_CODE_NO_INDEX, QueryError_Strerror(QUERY_ERROR_CODE_NO_INDEX), " %s", indexname);
```

### Direct Redis Replies
For direct Redis error replies:

```c
// Before:
return RedisModule_ReplyWithErrorFormat(ctx, "No such index %s", idx);

// After:
return RedisModule_ReplyWithErrorFormat(ctx, "SEARCH_INDEX_NOT_FOUND: Index not found %s", idx);
```

## Testing Updates

Test assertions were updated to use `.contains()` instead of exact string matching for better stability:

```python
# Before:
env.expect('FT.INFO', 'idx').error().equal('No such index idx')

# After:
env.expect('FT.INFO', 'idx').error().contains('SEARCH_INDEX_NOT_FOUND: Index not found')
```

## Benefits

1. **Redis Commandstats Tracking**: Each error type now has a unique identifier for metrics
2. **Consistency**: All error messages follow the same `SEARCH_PREFIX: Description` format
3. **Maintainability**: Centralized error message management through Rust error codes
4. **Test Stability**: Using `.contains()` makes tests more resilient to message variations

## Migration Notes

- Old error message parsers may need updates to handle the new prefixes
- The prefix extraction logic in `query_error_compat.c` automatically handles Rust error codes
- Tests using exact string matching should be updated to use `.contains()` for stability
