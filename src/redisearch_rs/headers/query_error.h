#pragma once

#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

/**
 * Error codes for query execution failures.
 *
 * **IMPORTANT**: Variants must be contiguous starting from `Ok = 0` with no explicit
 * discriminants (except for `Ok`). The `query_error_code_max_value()` function and
 * C/C++ test iteration logic rely on this assumption. The test
 * `error_code_full_msg_equals_prefix_plus_default_msg` validates this by iterating
 * all codes and will panic if gaps are introduced.
 *
 * cbindgen:prefix-with-name
 * cbindgen:rename-all=ScreamingSnakeCase
 */
enum QueryErrorCode
#ifdef __cplusplus
  : uint8_t
#endif // __cplusplus
 {
  QueryErrorCode_OK = 0,
  QueryErrorCode_GENERIC,
  QueryErrorCode_SYNTAX,
  QueryErrorCode_PARSE_ARGS,
  QueryErrorCode_ADD_ARGS,
  QueryErrorCode_EXPR,
  QueryErrorCode_KEYWORD,
  QueryErrorCode_NO_RESULTS,
  QueryErrorCode_BAD_ATTR,
  QueryErrorCode_INVAL,
  QueryErrorCode_BUILD_PLAN,
  QueryErrorCode_CONSTRUCT_PIPELINE,
  QueryErrorCode_NO_REDUCER,
  QueryErrorCode_REDUCER_GENERIC,
  QueryErrorCode_AGG_PLAN,
  QueryErrorCode_CURSOR_ALLOC,
  QueryErrorCode_REDUCER_INIT,
  QueryErrorCode_Q_STRING,
  QueryErrorCode_NO_PROP_KEY,
  QueryErrorCode_NO_PROP_VAL,
  QueryErrorCode_NO_DOC,
  QueryErrorCode_NO_OPTION,
  QueryErrorCode_REDIS_KEY_TYPE,
  QueryErrorCode_INVAL_PATH,
  QueryErrorCode_INDEX_EXISTS,
  QueryErrorCode_BAD_OPTION,
  QueryErrorCode_BAD_ORDER_OPTION,
  QueryErrorCode_LIMIT,
  QueryErrorCode_NO_INDEX,
  QueryErrorCode_DOC_EXISTS,
  QueryErrorCode_DOC_NOT_ADDED,
  QueryErrorCode_DUP_FIELD,
  QueryErrorCode_GEO_FORMAT,
  QueryErrorCode_NO_DISTRIBUTE,
  QueryErrorCode_UNSUPP_TYPE,
  QueryErrorCode_TIMED_OUT,
  QueryErrorCode_NO_PARAM,
  QueryErrorCode_DUP_PARAM,
  QueryErrorCode_BAD_VAL,
  QueryErrorCode_NON_HYBRID,
  QueryErrorCode_HYBRID_NON_EXIST,
  QueryErrorCode_ADHOC_WITH_BATCH_SIZE,
  QueryErrorCode_ADHOC_WITH_EF_RUNTIME,
  QueryErrorCode_NON_RANGE,
  QueryErrorCode_MISSING,
  QueryErrorCode_MISMATCH,
  QueryErrorCode_DROPPED_BACKGROUND,
  QueryErrorCode_ALIAS_CONFLICT,
  QueryErrorCode_INDEX_BG_OOM_FAIL,
  QueryErrorCode_WEIGHT_NOT_ALLOWED,
  QueryErrorCode_VECTOR_NOT_ALLOWED,
  QueryErrorCode_OUT_OF_MEMORY,
  QueryErrorCode_UNAVAILABLE_SLOTS,
  QueryErrorCode_FLEX_LIMIT_NUMBER_OF_INDEXES,
  QueryErrorCode_FLEX_UNSUPPORTED_FIELD,
  QueryErrorCode_FLEX_UNSUPPORTED_FT_CREATE_ARGUMENT,
  QueryErrorCode_DISK_CREATION,
  QueryErrorCode_FLEX_SKIP_INITIAL_SCAN_MISSING_ARGUMENT,
  QueryErrorCode_VECTOR_BLOB_SIZE_MISMATCH,
  QueryErrorCode_VECTOR_LEN_BAD,
  QueryErrorCode_NUMERIC_VALUE_INVALID,
  QueryErrorCode_ARG_UNRECOGNIZED,
  QueryErrorCode_GEO_COORDINATES_INVALID,
  QueryErrorCode_JSON_TYPE_BAD,
  QueryErrorCode_CLUSTER_NO_RESPONSES,
  QueryErrorCode_FLEX_SEARCH_NOCONTENT_OR_RETURN0_REQUIRED,
  QueryErrorCode_FLEX_SEARCH_LOAD_UNSUPPORTED,
  QueryErrorCode_FLEX_UNSUPPORTED_ARGUMENT,
  QueryErrorCode_SAFE_DEPLETER_FAILURE,
  QueryErrorCode_FLEX_UNSUPPORTED_QUERY,
};
#ifndef __cplusplus
typedef uint8_t QueryErrorCode;
#endif // __cplusplus

/**
 * cbindgen:prefix-with-name
 * cbindgen:rename-all=ScreamingSnakeCase
 */
enum QueryWarningCode
#ifdef __cplusplus
  : uint8_t
#endif // __cplusplus
 {
  QueryWarningCode_OK = 0,
  QueryWarningCode_TIMED_OUT,
  QueryWarningCode_REACHED_MAX_PREFIX_EXPANSIONS,
  QueryWarningCode_OUT_OF_MEMORY_SHARD,
  QueryWarningCode_OUT_OF_MEMORY_COORD,
  QueryWarningCode_UNAVAILABLE_SLOTS,
  QueryWarningCode_ASM_INACCURATE_RESULTS,
};
#ifndef __cplusplus
typedef uint8_t QueryWarningCode;
#endif // __cplusplus

typedef struct QueryError QueryError;

#ifndef SIZE_38_DEFINED
#define SIZE_38_DEFINED
/**
 * A type with size `N`.
 */
typedef uint8_t Size_38[38];
#endif /* SIZE_38_DEFINED */

/**
 * An opaque query error which can be passed by value to C.
 *
 * The size and alignment of this struct must match the Rust `QueryError`
 * structure exactly.
 */
typedef struct QueryError {
  Size_38 m0;
} QueryError;
