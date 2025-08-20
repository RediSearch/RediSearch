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

#define QUERY_XERRS(X)                                                                                                          \
  X(QUERY_EGENERIC, "Generic error evaluating the query")                                                                       \
  X(QUERY_ESYNTAX, "Parsing/Syntax error for query string")                                                                     \
  X(QUERY_EPARSEARGS, "Error parsing query/aggregation arguments")                                                              \
  X(QUERY_EADDARGS, "Error parsing document indexing arguments")                                                                \
  X(QUERY_EEXPR, "Parsing/Evaluating dynamic expression failed")                                                                \
  X(QUERY_EKEYWORD, "Could not handle query keyword")                                                                           \
  X(QUERY_ENORESULTS, "Query matches no results")                                                                               \
  X(QUERY_EBADATTR, "Attribute not supported for term")                                                                         \
  X(QUERY_EINVAL, "Could not validate the query nodes (bad attribute?)")                                                        \
  X(QUERY_EBUILDPLAN, "Could not build plan from query")                                                                        \
  X(QUERY_ECONSTRUCT_PIPELINE, "Could not construct query pipeline")                                                            \
  X(QUERY_ENOREDUCER, "Missing reducer")                                                                                        \
  X(QUERY_EREDUCER_GENERIC, "Generic reducer error")                                                                            \
  X(QUERY_EAGGPLAN, "Could not plan aggregation request")                                                                       \
  X(QUERY_ECURSORALLOC, "Could not allocate a cursor")                                                                          \
  X(QUERY_EREDUCERINIT, "Could not initialize reducer")                                                                         \
  X(QUERY_EQSTRING, "Bad query string")                                                                                         \
  X(QUERY_ENOPROPKEY, "Property does not exist in schema")                                                                      \
  X(QUERY_ENOPROPVAL, "Value was not found in result (not a hard error)")                                                       \
  X(QUERY_ENODOC, "Document does not exist")                                                                                    \
  X(QUERY_ENOOPTION, "Invalid option")                                                                                          \
  X(QUERY_EREDISKEYTYPE, "Invalid Redis key")                                                                                   \
  X(QUERY_EINVALPATH, "Invalid path")                                                                                           \
  X(QUERY_EINDEXEXISTS, "Index already exists")                                                                                 \
  X(QUERY_EBADOPTION, "Option not supported for current mode")                                                                  \
  X(QUERY_EBADORDEROPTION, "Path with undefined ordering does not support slop/inorder")                                        \
  X(QUERY_ELIMIT, "Limit exceeded")                                                                                             \
  X(QUERY_ENOINDEX, "Index not found")                                                                                          \
  X(QUERY_EDOCEXISTS, "Document already exists")                                                                                \
  X(QUERY_EDOCNOTADDED, "Document was not added because condition was unmet")                                                   \
  X(QUERY_EDUPFIELD, "Field was specified twice")                                                                               \
  X(QUERY_EGEOFORMAT, "Invalid lon/lat format. Use \"lon lat\" or \"lon,lat\"")                                                 \
  X(QUERY_ENODISTRIBUTE, "Could not distribute the operation")                                                                  \
  X(QUERY_EUNSUPPTYPE, "Unsupported index type")                                                                                \
  X(QUERY_ENOTNUMERIC, "Could not convert value to a number")                                                                   \
  X(QUERY_ETIMEDOUT, "Timeout limit was reached")                                                                               \
  X(QUERY_ENOPARAM, "Parameter not found")                                                                                      \
  X(QUERY_EDUPPARAM, "Parameter was specified twice")                                                                           \
  X(QUERY_EBADVAL, "Invalid value was given")                                                                                   \
  X(QUERY_ENHYBRID, "hybrid query attributes were sent for a non-hybrid query")                                                 \
  X(QUERY_EHYBRIDNEXIST, "invalid hybrid policy was given")                                                                     \
  X(QUERY_EADHOCWBATCHSIZE, "'batch size' is irrelevant for 'ADHOC_BF' policy")                                                 \
  X(QUERY_EADHOCWEFRUNTIME, "'EF_RUNTIME' is irrelevant for 'ADHOC_BF' policy")                                                 \
  X(QUERY_ENRANGE, "range query attributes were sent for a non-range query")                                                    \
  X(QUERY_EMISSING, "'ismissing' requires field to be defined with 'INDEXMISSING'")                                             \
  X(QUERY_EMISSMATCH, "Index mismatch: Shard index is different than queried index")                                            \
  X(QUERY_EUNKNOWNINDEX, "Unknown index name")                                                                                  \
  X(QUERY_EDROPPEDBACKGROUND, "The index was dropped before the query could be executed")                                       \
  X(QUERY_EALIASCONFLICT, "Alias conflicts with an existing index name")                                                        \
  X(QUERY_INDEXBGOOMFAIL, "Index background scan did not complete due to OOM")                                                  \
  X(QUERY_EHYBRID_VSIM_FILTER_INVALID_QUERY, "Vector queries are not allowed in FT.HYBRID VSIM subquery FILTER")                \
  X(QUERY_EHYBRID_VSIM_FILTER_INVALID_WEIGHT, "Weight attributes are not allowed in FT.HYBRID VSIM subquery FILTER")            \
  X(QUERY_EHYBRID_SEARCH_INVALID_QUERY, "Vector queries are not allowed in FT.HYBRID SEARCH subquery")                          \
  X(QUERY_EHYBRID_HYBRID_ALIAS, "Alias is not allowed in FT.HYBRID VSIM")                                                       \
  X(QUERY_EHYBRID_AGGREGATE_INVALID_QUERY, "Vector queries are not allowed in FT.HYBRID AGGREGATE subquery")                    \
  //TODO: remove QUERY_EHYBRID_HYBRID_ALIAS after YIELD_DISTANCE_AS is enabled

#define QUERY_WMAXPREFIXEXPANSIONS "Max prefix expansions limit was reached"
#define QUERY_WINDEXING_FAILURE "Index contains partial data due to an indexing failure caused by insufficient memory"
typedef enum {
  QUERY_OK = 0,

#define X(N, msg) N,
  QUERY_XERRS(X)
#undef X
} QueryErrorCode;

typedef struct QueryError {
  QueryErrorCode code;
  // The error message which we can expose in the logs, does not contain user data
  const char* message;
  // The formatted error message in its entirety, can be shown only to the user
  char *detail;

  // warnings
  bool reachedMaxPrefixExpansions;
} QueryError;

/** Initialize QueryError object */
void QueryError_Init(QueryError *qerr);

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
  QueryError_SetWithUserDataFmt(status, QUERY_EPARSEARGS, "Bad arguments", " for %s: %s", name, \
                         AC_Strerror(rv))

#define QERR_MKSYNTAXERR(status, message) QueryError_SetError(status, QUERY_ESYNTAX, message)

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
 *  QueryError_SetWithUserDataFmt(err, QUERY_EPARSEARGS, "Unknown argument for %s:", " %s", name, unknown);
 * @endcode
 */
void QueryError_FmtUnknownArg(QueryError *err, ArgsCursor *ac, const char *name);

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

/**
 * Clear the error state, potentially releasing the embedded string
 */
void QueryError_ClearError(QueryError *err);

/**
 * Return true if the object has an error set
 */
static inline int QueryError_HasError(const QueryError *status) {
  return status->code;
}

void QueryError_MaybeSetCode(QueryError *status, QueryErrorCode code);

#ifdef __cplusplus
}
#endif
#endif  // QUERY_ERROR_H
