#include "query_error.h"
#include "rmutil/rm_assert.h"
#include "rmalloc.h"

///////////////////////////////////////////////////////////////////////////////////////////////

QueryError::QueryError() {
  code = QUERY_OK;
  detail = NULL;
}

//---------------------------------------------------------------------------------------------

/**
 * Sets the current error from the current argument within the args cursor
 * @param err the error object
 * @param ac the argument cursor
 * @param name a prefix to be used in the message to better identify the subsystem
 *  which threw the error. This is similar to the 'message' functionality in perror(3)
 *
 * Equivalent to the following boilerplate:
 * @code{c}
 *  const char *unknown = AC_GetStringNC(ac, NULL);
 *  err.SetErrorFmt(QUERY_EPARSEARGS, "Unknown argument for %s: %s", name, unknown);
 * @endcode
 */

void QueryError::FmtUnknownArg(ArgsCursor *ac, const char *name) {
  RS_LOG_ASSERT(!AC_IsAtEnd(ac), "cursor should not be at the end");
  const char *s;
  size_t n;
  if (AC_GetString(ac, &s, &n, AC_F_NOADVANCE) != AC_OK) {
    s = "Unknown (FIXME)";
    n = strlen(s);
  }

  SetErrorFmt(QUERY_EPARSEARGS, "Unknown argument `%.*s` at position %lu for %s",
              (int)n, s, ac->offset, name);
}

//---------------------------------------------------------------------------------------------

// Return the constant string of an error code

const char *QueryError::Strerror(QueryErrorCode code) {
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

//---------------------------------------------------------------------------------------------

/**
 * Set the error code of the query. If `err` is present, then the error
 * object must eventually be released using QueryError_Clear().
 *
 * Only has an effect if no error is already present
 */

void QueryError::SetError(QueryErrorCode c, const char *err) {
  if (code != QUERY_OK) {
    return;
  }
  RS_LOG_ASSERT(!detail, "detail of error is missing");
  code = c;

  if (err) {
    detail = rm_strdup(err);
  } else {
    detail = rm_strdup(Strerror(code));
  }
}

//---------------------------------------------------------------------------------------------

// Set the error code of the query without setting an error string

void QueryError::SetCode(QueryErrorCode c) {
  if (code == QUERY_OK) {
    code = c;
  }
}

//---------------------------------------------------------------------------------------------

// Clear the error state, potentially releasing the embedded string

void QueryError::ClearError() {
  if (detail) {
    rm_free(detail);
    detail = NULL;
  }
  code = QUERY_OK;
}

//---------------------------------------------------------------------------------------------

// Set the error code using a custom-formatted string

void QueryError::SetErrorFmt(QueryErrorCode c, const char *fmt, ...) {
  if (code != QUERY_OK) {
    return;
  }
  va_list ap;
  va_start(ap, fmt);
  rm_vasprintf(&detail, fmt, ap);
  va_end(ap);
  code = c;
}

//---------------------------------------------------------------------------------------------

void QueryError::MaybeSetCode(QueryErrorCode c) {
  // Set the code if not previously set. This should be used by code which makes
  // use of the ::detail field, and is a placeholder for something like:
  // functionWithCharPtr(&status->detail);
  // if (status->detail && status->code == QUERY_OK) {
  //    status->code = MYCODE;
  // }
  if (detail == NULL) {
    return;
  }
  if (code != QUERY_OK) {
    return;
  }
  code = c;
}

//---------------------------------------------------------------------------------------------

// Retrieve the error string of the error itself. This will use either the
// built-in error string for the given code, or the custom string within the object.

const char *QueryError::GetError() {
  return detail ? detail : Strerror(code);
}

///////////////////////////////////////////////////////////////////////////////////////////////
