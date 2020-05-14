#include "query_error.h"
#include <assert.h>
#include "rmalloc.h"

void QueryError_Init(QueryError *qerr) {
  assert(qerr);
  qerr->code = QUERY_OK;
  qerr->detail = NULL;
}

void QueryError_FmtUnknownArg(QueryError *err, ArgsCursor *ac, const char *name) {
  assert(!AC_IsAtEnd(ac));
  const char *s;
  size_t n;
  if (AC_GetString(ac, &s, &n, AC_F_NOADVANCE) != AC_OK) {
    s = "Unknown (FIXME)";
    n = strlen(s);
  }

  QueryError_SetErrorFmt(err, QUERY_EPARSEARGS, "Unknown argument `%.*s` at position %u for %s",
                         (int)n, s, ac->offset, name);
}

const char *QueryError_Strerror(QueryErrorCode code) {
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

void QueryError_SetError(QueryError *status, QueryErrorCode code, const char *err) {
  if (status->code != QUERY_OK) {
    return;
  }
  assert(!status->detail);
  status->code = code;

  if (err) {
    status->detail = rm_strdup(err);
  } else {
    status->detail = rm_strdup(QueryError_Strerror(code));
  }
}

void QueryError_SetCode(QueryError *status, QueryErrorCode code) {
  if (status->code == QUERY_OK) {
    status->code = code;
  }
}

void QueryError_ClearError(QueryError *err) {
  if (err->detail) {
    rm_free(err->detail);
    err->detail = NULL;
  }
  err->code = QUERY_OK;
}

void QueryError_SetErrorFmt(QueryError *status, QueryErrorCode code, const char *fmt, ...) {
  if (status->code != QUERY_OK) {
    return;
  }
  va_list ap;
  va_start(ap, fmt);
  rm_vasprintf(&status->detail, fmt, ap);
  va_end(ap);
  status->code = code;
}

void QueryError_MaybeSetCode(QueryError *status, QueryErrorCode code) {
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

const char *QueryError_GetError(const QueryError *status) {
  return status->detail ? status->detail : QueryError_Strerror(status->code);
}
