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

#define QUERY_XERRS(X)                                                    \
  X(QUERY_EGENERIC, "Generic error evaluating the query")                 \
  X(QUERY_ESYNTAX, "Parsing/Syntax error for query string")               \
  X(QUERY_EPARSEARGS, "Error parsing query/aggregation arguments")        \
  X(QUERY_EADDARGS, "Error parsing document indexing arguments")          \
  X(QUERY_EEXPR, "Parsing/Evaluating dynamic expression failed")          \
  X(QUERY_EKEYWORD, "Could not handle query keyword")                     \
  X(QUERY_ENORESULTS, "Query matches no results")                         \
  X(QUERY_EBADATTR, "Attribute not supported for term")                   \
  X(QUERY_EINVAL, "Could not validate the query nodes (bad attribute?)")  \
  X(QUERY_EBUILDPLAN, "Could not build plan from query")                  \
  X(QUERY_ECONSTRUCT_PIPELINE, "Could not construct query pipeline")      \
  X(QUERY_ENOREDUCER, "Missing reducer")                                  \
  X(QUERY_EREDUCER_GENERIC, "Generic reducer error")                      \
  X(QUERY_EAGGPLAN, "Could not plan aggregation request")                 \
  X(QUERY_ECURSORALLOC, "Could not allocate a cursor")                    \
  X(QUERY_EREDUCERINIT, "Could not initialize reducer")                   \
  X(QUERY_EQSTRING, "Bad query string")                                   \
  X(QUERY_ENOPROPKEY, "Property does not exist in schema")                \
  X(QUERY_ENOPROPVAL, "Value was not found in result (not a hard error)") \
  X(QUERY_ENODOC, "Document does not exist")                              \
  X(QUERY_ENOOPTION, "Invalid option")                                    \
  X(QUERY_EREDISKEYTYPE, "Invalid Redis key")                             \
  X(QUERY_EINDEXEXISTS, "Index already exists")                           \
  X(QUERY_EBADOPTION, "Option not supported for current mode")            \
  X(QUERY_ELIMIT, "Limit exceeded")

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

static inline const char *QueryError_Strerror(QueryErrorCode code) {
  if (code == QUERY_OK) {
    return "Success (not an error)";
  }
#define X(N, M)    \
  if (code == N) { \
    return M;      \
  }
  QUERY_XERRS(X)
#undef X
  return "Unknown status code";
}

static inline void QueryError_SetError(QueryError *status, QueryErrorCode code, const char *err) {
  if (status->code != QUERY_OK) {
    return;
  }
  status->code = code;
  if (err) {
    status->detail = strdup(err);
  } else {
    status->detail = strdup(QueryError_Strerror(code));
  }
}

static inline void QueryError_SetCode(QueryError *status, QueryErrorCode code) {
  if (status->code == QUERY_OK) {
    status->code = code;
  }
}

static inline void QueryError_SetErrorFmt(QueryError *status, QueryErrorCode code, const char *fmt,
                                          ...) {
  if (status->code != QUERY_OK) {
    return;
  }
  va_list ap;
  va_start(ap, fmt);
  vasprintf(&status->detail, fmt, ap);
  va_end(ap);
}

#define QERR_MKBADARGS_FMT(status, fmt, ...) \
  QueryError_SetErrorFmt(status, QUERY_EPARSEARGS, fmt, ##__VA_ARGS__)

#define QERR_MKBADARGS_AC(status, name, rv)                                          \
  QueryError_SetErrorFmt(status, QUERY_EPARSEARGS, "Bad arguments for %s: %s", name, \
                         AC_Strerror(rv))

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

static inline const char *QueryError_GetError(const QueryError *status) {
  return status->detail ? status->detail : QueryError_Strerror(status->code);
}

static inline void QueryError_ClearError(QueryError *err) {
  if (err->detail) {
    free(err->detail);
  }
  err->code = QUERY_OK;
}

static inline int QueryError_HasError(const QueryError *status) {
  return status->code;
}

static inline void QueryError_MaybeSetCode(QueryError *status, QueryErrorCode code) {
  // Set the code if not previously set. This should be used by code which makes
  // use of the ::detail field, and is a placeholder for something like:
  // functionWithCharPtr(&status->detail);
  // if (status->detail && status->code == QUERY_OK) {
  //    status->code = MYCODE;
  // }
  if (status->detail == NULL) {
    return;
  }
  if (status->code != QUERY_OK) {
    return;
  }
  status->code = code;
}

#ifdef __cplusplus
}
#endif
#endif  // QUERY_ERROR_H