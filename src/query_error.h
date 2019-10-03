#ifndef QUERY_ERROR_H
#define QUERY_ERROR_H
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
#include <stdio.h>
#include <rmutil/args.h>

#ifdef __cplusplus
extern "C" {
#endif

#define QUERY_XERRS(X)                                                          \
  X(QUERY_EGENERIC, "Generic error evaluating the query")                       \
  X(QUERY_ESYNTAX, "Parsing/Syntax error for query string")                     \
  X(QUERY_EPARSEARGS, "Error parsing query/aggregation arguments")              \
  X(QUERY_EADDARGS, "Error parsing document indexing arguments")                \
  X(QUERY_EEXPR, "Parsing/Evaluating dynamic expression failed")                \
  X(QUERY_EKEYWORD, "Could not handle query keyword")                           \
  X(QUERY_ENORESULTS, "Query matches no results")                               \
  X(QUERY_EBADATTR, "Attribute not supported for term")                         \
  X(QUERY_EINVAL, "Could not validate the query nodes (bad attribute?)")        \
  X(QUERY_EBUILDPLAN, "Could not build plan from query")                        \
  X(QUERY_ECONSTRUCT_PIPELINE, "Could not construct query pipeline")            \
  X(QUERY_ENOREDUCER, "Missing reducer")                                        \
  X(QUERY_EREDUCER_GENERIC, "Generic reducer error")                            \
  X(QUERY_EAGGPLAN, "Could not plan aggregation request")                       \
  X(QUERY_ECURSORALLOC, "Could not allocate a cursor")                          \
  X(QUERY_EREDUCERINIT, "Could not initialize reducer")                         \
  X(QUERY_EQSTRING, "Bad query string")                                         \
  X(QUERY_ENOPROPKEY, "Property does not exist in schema")                      \
  X(QUERY_ENOPROPVAL, "Value was not found in result (not a hard error)")       \
  X(QUERY_ENODOC, "Document does not exist")                                    \
  X(QUERY_ENOOPTION, "Invalid option")                                          \
  X(QUERY_EREDISKEYTYPE, "Invalid Redis key")                                   \
  X(QUERY_EINDEXEXISTS, "Index already exists")                                 \
  X(QUERY_EBADOPTION, "Option not supported for current mode")                  \
  X(QUERY_ELIMIT, "Limit exceeded")                                             \
  X(QUERY_ENOINDEX, "Index not found")                                          \
  X(QUERY_EDOCEXISTS, "Document already exists")                                \
  X(QUERY_EDOCNOTADDED, "Document was not added because condition was unmet")   \
  X(QUERY_EDUPFIELD, "Field was specified twice")                               \
  X(QUERY_EGEOFORMAT, "Invalid lon/lat format. Use \"lon lat\" or \"lon,lat\"") \
  X(QUERY_ENODISTRIBUTE, "Could not distribute the operation")                  \
  X(QUERY_EUNSUPPTYPE, "Unsupported index type")                                \
  X(QUERY_ENOTNUMERIC, "Could not convert value to a number")

typedef enum {
  QUERY_OK = 0,

#define X(N, msg) N,
  QUERY_XERRS(X)
#undef X
} QueryErrorCode;

typedef struct QueryError {
  QueryErrorCode code;
  char *detail;
} QueryError;

/** Initialize QueryError object */
void QueryError_Init(QueryError *qerr);

/** Return the constant string of an error code */
const char *QueryError_Strerror(QueryErrorCode code);

/**
 * Set the error code of the query. If `err` is present, then the error
 * object must eventually be released using QueryError_Clear().
 *
 * Only has an effect if no error is already present
 */
void QueryError_SetError(QueryError *status, QueryErrorCode code, const char *err);

/** Set the error code of the query without setting an error string. */
void QueryError_SetCode(QueryError *status, QueryErrorCode code);

/** Set the error code using a custom-formatted string */
void QueryError_SetErrorFmt(QueryError *status, QueryErrorCode code, const char *fmt, ...);

/** Convenience macro to set an error of a 'bad argument' with the name of the argument */
#define QERR_MKBADARGS_FMT(status, fmt, ...) \
  QueryError_SetErrorFmt(status, QUERY_EPARSEARGS, fmt, ##__VA_ARGS__)

/** Convenience macro to extract the error string of the argument parser */
#define QERR_MKBADARGS_AC(status, name, rv)                                          \
  QueryError_SetErrorFmt(status, QUERY_EPARSEARGS, "Bad arguments for %s: %s", name, \
                         AC_Strerror(rv))

#define QERR_MKSYNTAXERR(status, ...) QueryError_SetErrorFmt(status, QUERY_ESYNTAX, ##__VA_ARGS__)

/**
 * Convenience macro to reply the error string to redis and clear the error code.
 * I'm making this into a macro so I don't need to include redismodule.h
 */
#define QueryError_ReplyAndClear(rctx, qerr)                     \
  ({                                                             \
    RedisModule_ReplyWithError(rctx, QueryError_GetError(qerr)); \
    QueryError_ClearError(qerr);                                 \
    REDISMODULE_OK;                                              \
  })

#define QueryError_ReplyNoIndex(rctx, ixname)                                        \
  {                                                                                  \
    QueryError qidx__tmp = {0};                                                      \
    QueryError_SetErrorFmt(&qidx__tmp, QUERY_ENOINDEX, "%s: No such index", ixname); \
    QueryError_ReplyAndClear(rctx, &qidx__tmp);                                      \
  }

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
 *  QueryError_SetErrorFmt(err, QUERY_EPARSEARGS, "Unknown argument for %s: %s", name, unknown);
 * @endcode
 */
void QueryError_FmtUnknownArg(QueryError *err, ArgsCursor *ac, const char *name);

/**
 * Retrieve the error string of the error itself. This will use either the
 * built-in error string for the given code, or the custom string within the
 * object.
 */
const char *QueryError_GetError(const QueryError *status);

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
