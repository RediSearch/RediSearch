/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "query_error.h"
#include "rmutil/rm_assert.h"
#include "rmalloc.h"

void QueryError_Init(QueryError *qerr) {
  RS_LOG_ASSERT(qerr, "QueryError should not be NULL");
  qerr->code = QUERY_OK;
  qerr->detail = NULL;
}

void QueryError_FmtUnknownArg(QueryError *err, ArgsCursor *ac, const char *name) {
  RS_LOG_ASSERT(!AC_IsAtEnd(ac), "cursor should not be at the end");
  const char *s;
  size_t n;
  HiddenString *hs;
  if (AC_GetHiddenString(ac, &hs, AC_F_NOADVANCE) != AC_OK) {
    s = "Unknown (FIXME)";
    n = strlen(s);
  } else {
    s = HiddenString_GetUnsafe(hs, &n);
  }
  QueryError_SetErrorFmt(err, QUERY_EPARSEARGS, "Unknown argument", " `%.*s` at position %lu for %s",
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
  RS_LOG_ASSERT(!status->detail, "detail of error is missing");
  status->code = code;

  if (err) {
    status->detail = rm_strdup(err);
  } else {
    status->detail = rm_strdup(QueryError_Strerror(code));
  }
  status->message = status->detail;
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

void QueryError_SetErrorFmt(QueryError *status, QueryErrorCode code, const char *message, const char *fmt, ...) {
  if (status->code != QUERY_OK) {
    return;
  }

  char *formatted = NULL;
  va_list ap;
  va_start(ap, fmt);
  rm_vasprintf(&formatted, fmt, ap);
  va_end(ap);

  rm_asprintf(&status->detail, "%s%s", message, formatted);
  rm_free(formatted);
  status->code = code;
  status->message = message;
}

void QueryError_SetUserDataAgnosticErrorFmt(QueryError *status, QueryErrorCode code, const char *fmt, ...) {
  if (status->code != QUERY_OK) {
    return;
  }
  va_list ap;
  va_start(ap, fmt);
  rm_vasprintf(&status->detail, fmt, ap);
  va_end(ap);
  status->code = code;
  status->message = status->detail;
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

const char *QueryError_GetUserError(const QueryError *status) {
  return status->detail ? status->detail : QueryError_Strerror(status->code);
}

const char *QueryError_GetDisplayableError(const QueryError *status, bool obfuscate) {
  if (status->detail == NULL || obfuscate) {
    return status->message ? status->message : QueryError_Strerror(status->code);
  } else {
    return status->detail ? status->detail : QueryError_Strerror(status->code);
  }
}

QueryErrorCode QueryError_GetCode(const QueryError *status) {
  return status->code;
}
