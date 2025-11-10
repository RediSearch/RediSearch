/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#ifndef QUERY_ERROR_H
#define QUERY_ERROR_H
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
#include <stdio.h>
#include <rmutil/args.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

#define QUERY_XERRS(X)                                                                               \
  X(QUERY_ERROR_CODE_GENERIC, "Generic error evaluating the query")                                  \
  X(QUERY_ERROR_CODE_SYNTAX, "Parsing/Syntax error for query string")                                \
  X(QUERY_ERROR_CODE_PARSE_ARGS, "Error parsing query/aggregation arguments")                        \
  X(QUERY_ERROR_CODE_ADD_ARGS, "Error parsing document indexing arguments")                          \
  X(QUERY_ERROR_CODE_EXPR, "Parsing/Evaluating dynamic expression failed")                           \
  X(QUERY_ERROR_CODE_KEYWORD, "Could not handle query keyword")                                      \
  X(QUERY_ERROR_CODE_NO_RESULTS, "Query matches no results")                                         \
  X(QUERY_ERROR_CODE_BAD_ATTR, "Attribute not supported for term")                                   \
  X(QUERY_ERROR_CODE_INVAL, "Could not validate the query nodes (bad attribute?)")                   \
  X(QUERY_ERROR_CODE_BUILD_PLAN, "Could not build plan from query")                                  \
  X(QUERY_ERROR_CODE_CONSTRUCT_PIPELINE, "Could not construct query pipeline")                       \
  X(QUERY_ERROR_CODE_NO_REDUCER, "Missing reducer")                                                  \
  X(QUERY_ERROR_CODE_REDUCER_GENERIC, "Generic reducer error")                                       \
  X(QUERY_ERROR_CODE_AGG_PLAN, "Could not plan aggregation request")                                 \
  X(QUERY_ERROR_CODE_CURSOR_ALLOC, "Could not allocate a cursor")                                    \
  X(QUERY_ERROR_CODE_REDUCER_INIT, "Could not initialize reducer")                                   \
  X(QUERY_ERROR_CODE_Q_STRING, "Bad query string")                                                   \
  X(QUERY_ERROR_CODE_NO_PROP_KEY, "Property does not exist in schema")                               \
  X(QUERY_ERROR_CODE_NO_PROP_VAL, "Value was not found in result (not a hard error)")                \
  X(QUERY_ERROR_CODE_NO_DOC, "Document does not exist")                                              \
  X(QUERY_ERROR_CODE_NO_OPTION, "Invalid option")                                                    \
  X(QUERY_ERROR_CODE_REDIS_KEY_TYPE, "Invalid Redis key")                                            \
  X(QUERY_ERROR_CODE_INVAL_PATH, "Invalid path")                                                     \
  X(QUERY_ERROR_CODE_INDEX_EXISTS, "Index already exists")                                           \
  X(QUERY_ERROR_CODE_BAD_OPTION, "Option not supported for current mode")                            \
  X(QUERY_ERROR_CODE_BAD_ORDER_OPTION, "Path with undefined ordering does not support slop/inorder") \
  X(QUERY_ERROR_CODE_LIMIT, "Limit exceeded")                                                        \
  X(QUERY_ERROR_CODE_NO_INDEX, "Index not found")                                                    \
  X(QUERY_ERROR_CODE_DOC_EXISTS, "Document already exists")                                          \
  X(QUERY_ERROR_CODE_DOC_NOT_ADDED, "Document was not added because condition was unmet")            \
  X(QUERY_ERROR_CODE_DUP_FIELD, "Field was specified twice")                                         \
  X(QUERY_ERROR_CODE_GEO_FORMAT, "Invalid lon/lat format. Use \"lon lat\" or \"lon,lat\"")           \
  X(QUERY_ERROR_CODE_NO_DISTRIBUTE, "Could not distribute the operation")                            \
  X(QUERY_ERROR_CODE_UNSUPP_TYPE, "Unsupported index type")                                          \
  X(QUERY_ERROR_CODE_NOT_NUMERIC, "Could not convert value to a number")                             \
  X(QUERY_ERROR_CODE_TIMED_OUT, "Timeout limit was reached")                                         \
  X(QUERY_ERROR_CODE_NO_PARAM, "Parameter not found")                                                \
  X(QUERY_ERROR_CODE_DUP_PARAM, "Parameter was specified twice")                                     \
  X(QUERY_ERROR_CODE_BAD_VAL, "Invalid value was given")                                             \
  X(QUERY_ERROR_CODE_NON_HYBRID, "hybrid query attributes were sent for a non-hybrid query")         \
  X(QUERY_ERROR_CODE_HYBRID_NON_EXIST, "invalid hybrid policy was given")                            \
  X(QUERY_ERROR_CODE_ADHOC_WITH_BATCH_SIZE, "'batch size' is irrelevant for 'ADHOC_BF' policy")      \
  X(QUERY_ERROR_CODE_ADHOC_WITH_EF_RUNTIME, "'EF_RUNTIME' is irrelevant for 'ADHOC_BF' policy")      \
  X(QUERY_ERROR_CODE_NON_RANGE, "range query attributes were sent for a non-range query")            \
  X(QUERY_ERROR_CODE_MISSING, "'ismissing' requires field to be defined with 'INDEXMISSING'")        \
  X(QUERY_ERROR_CODE_MISMATCH, "Index mismatch: Shard index is different than queried index")        \
  X(QUERY_ERROR_CODE_UNKNOWN_INDEX, "Unknown index name")                                            \
  X(QUERY_ERROR_CODE_DROPPED_BACKGROUND, "The index was dropped before the query could be executed") \
  X(QUERY_ERROR_CODE_ALIAS_CONFLICT, "Alias conflicts with an existing index name")                  \
  X(QUERY_ERROR_CODE_INDEX_BG_OOM_FAIL, "Index background scan did not complete due to OOM")         \
  X(QUERY_ERROR_CODE_WEIGHT_NOT_ALLOWED, "Weight attributes are not allowed")                        \
  X(QUERY_ERROR_CODE_VECTOR_NOT_ALLOWED, "Vector queries are not allowed")                           \
  X(QUERY_ERROR_CODE_OUT_OF_MEMORY, "Not enough memory available to execute the query")              \


#define QUERY_WMAXPREFIXEXPANSIONS "Max prefix expansions limit was reached"
#define QUERY_WINDEXING_FAILURE "Index contains partial data due to an indexing failure caused by insufficient memory"
#define QUERY_WOOM_CLUSTER "One or more shards failed to execute the query due to insufficient memory"

typedef enum {
  QUERY_ERROR_CODE_OK = 0,

#define X(N, msg) N,
  QUERY_XERRS(X)
#undef X
} QueryErrorCode;

typedef struct QueryError {
  QueryErrorCode _code;
  // The error message which we can expose in the logs, does not contain user data
  const char* _message;
  // The formatted error message in its entirety, can be shown only to the user
  char *_detail;

  // warnings
  bool _reachedMaxPrefixExpansions;
  bool _queryOOM;
} QueryError;

/**
 * Create a Query error with default fields: QUERY_ERROR_CODE_OK error code, no messages,
 * no detail, and no warning flags set.
 */
QueryError QueryError_Default();

/** Return the constant string of an error code */
const char *QueryError_Strerror(QueryErrorCode code);

/**
 * Set the error code of the query. If `err` is present, then the error
 * object must eventually be released using QueryError_ClearError().
 *
 * Only has an effect if no error is already present
 */
void QueryError_SetError(QueryError *status, QueryErrorCode code, const char *message);

/** Set the error code of the query without setting an error string. */
void QueryError_SetCode(QueryError *status, QueryErrorCode code);

/** Set the error code using a custom-formatted string */
void QueryError_SetWithUserDataFmt(QueryError *status, QueryErrorCode code, const char* message, const char *fmt, ...);

/**
 * Set the error code using a custom-formatted string
 * Only use this function if you are certain that no user data is leaked in the format string
 */
void QueryError_SetWithoutUserDataFmt(QueryError *status, QueryErrorCode code, const char *fmt, ...);


/** Convenience macro to extract the error string of the argument parser */
#define QERR_MKBADARGS_AC(status, name, rv)                                          \
  QueryError_SetWithUserDataFmt(status, QUERY_ERROR_CODE_PARSE_ARGS, "Bad arguments", " for %s: %s", name, \
                         AC_Strerror(rv))

#define QERR_MKSYNTAXERR(status, message) QueryError_SetError(status, QUERY_ERROR_CODE_SYNTAX, message)

/**
 * Convenience macro to reply the error string to redis and clear the error code.
 * I'm making this into a macro so I don't need to include redismodule.h
 */
#define QueryError_ReplyAndClear(rctx, qerr)                         \
  ({                                                                 \
    RedisModule_ReplyWithError(rctx, QueryError_GetUserError(qerr)); \
    QueryError_ClearError(qerr);                                     \
    REDISMODULE_OK;                                                  \
  })

/**
 * Sets the current error from the current argument within the args cursor
 * @param err the error object
 * @param ac the argument cursor
 * @param name a prefix to be used in the message to better identify the subsystem
 *  which threw the error. This is similar to the 'message' functionality in
 *  perror(3)
 *
 * Equivalent to the following boilerplate:
 * @code{c}
 *  const char *unknown = AC_GetStringNC(ac, NULL);
 *  QueryError_SetWithUserDataFmt(err, QUERY_ERROR_CODE_PARSE_ARGS, "Unknown argument for %s:", " %s", name, unknown);
 * @endcode
 */
void QueryError_FmtUnknownArg(QueryError *err, ArgsCursor *ac, const char *name);

/**
 * Clone the error from src to dest. If dest already has an error, it is not overwritten.
 */
void QueryError_CloneFrom(const QueryError *src, QueryError *dest);

/**
 * Retrieve the error string of the error itself. This will use either the
 * built-in error string for the given code, or the custom string within the
 * object.
 */
const char *QueryError_GetUserError(const QueryError *status);

/**
* Retrieve the error suitable for being displayed.
* If obfuscate is true, the error message will only contain the error without any user data.
* If obfuscate is false, the error message will contain the error and the user data, equivalent to QueryError_GetUserError
*/
const char *QueryError_GetDisplayableError(const QueryError *status, bool obfuscate);

/**
 * Retrieve the error code.
 */
QueryErrorCode QueryError_GetCode(const QueryError *status);

// Extracts the query error from the error message
// Returns the error code
// Only checks for timeout and OOM errors
QueryErrorCode QueryError_GetCodeFromMessage(const char *errorMessage);

/**
 * Clear the error state, potentially releasing the embedded string
 */
void QueryError_ClearError(QueryError *err);

/**
 * Return true if the object has an error set
 */
static inline bool QueryError_HasError(const QueryError *status) {
  return status->_code != QUERY_ERROR_CODE_OK;
}

/**
 * Return true if the object has no error set
 */
static inline bool QueryError_IsOk(const QueryError *status) {
  return status->_code == QUERY_ERROR_CODE_OK;
}

void QueryError_MaybeSetCode(QueryError *status, QueryErrorCode code);

/*** Whether the reached max prefix expansions warning is set */
bool QueryError_HasReachedMaxPrefixExpansionsWarning(const QueryError *status);

/*** Sets the reached max prefix expansions warning */
void QueryError_SetReachedMaxPrefixExpansionsWarning(QueryError *status);

/*** Whether the query OOM warning is set */
bool QueryError_HasQueryOOMWarning(const QueryError *status);

/*** Sets the query OOM warning */
void QueryError_SetQueryOOMWarning(QueryError *status);

#ifdef __cplusplus
}
#endif
#endif  // QUERY_ERROR_H
