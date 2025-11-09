/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#include "query_error.h"
#include "rmutil/rm_assert.h"
#include "rmalloc.h"

QueryError QueryError_Default() {
  #ifdef __cplusplus
    return QueryError{};
  #else
    return ((QueryError){0});
  #endif
}

void QueryError_FmtUnknownArg(QueryError *err, ArgsCursor *ac, const char *name) {
  RS_LOG_ASSERT(!AC_IsAtEnd(ac), "cursor should not be at the end");
  const char *s;
  size_t n;
  if (AC_GetString(ac, &s, &n, AC_F_NOADVANCE) != AC_OK) {
    s = "Unknown (FIXME)";
    n = strlen(s);
  }

  QueryError_SetWithUserDataFmt(err, QUERY_ERROR_CODE_PARSE_ARGS, "Unknown argument", " `%.*s` at position %lu for %s",
                         (int)n, s, ac->offset, name);
}

void QueryError_CloneFrom(const QueryError *src, QueryError *dest) {
  if (QueryError_HasError(dest)) {
    return;
  }
  dest->_code = src->_code;
  const char *error = src->_detail ? src->_detail : QueryError_Strerror(src->_code);
  dest->_detail = rm_strdup(error);
  dest->_message = src->_message;
}

const char *QueryError_Strerror(QueryErrorCode code) {
  if (code == QUERY_ERROR_CODE_OK) {
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
  if (QueryError_HasError(status)) {
    return;
  }
  RS_LOG_ASSERT(!status->_detail, "detail of error is missing");
  status->_code = code;

  if (err) {
    status->_detail = rm_strdup(err);
  } else {
    status->_detail = rm_strdup(QueryError_Strerror(code));
  }
  status->_message = status->_detail;
}

void QueryError_SetCode(QueryError *status, QueryErrorCode code) {
  if (QueryError_IsOk(status)) {
    status->_code = code;
  }
}

void QueryError_ClearError(QueryError *err) {
  if (err->_detail) {
    rm_free(err->_detail);
    err->_detail = NULL;
  }
  err->_message = NULL;
  err->_code = QUERY_ERROR_CODE_OK;
}

void QueryError_SetWithUserDataFmt(QueryError *status, QueryErrorCode code, const char *message, const char *fmt, ...) {
  if (QueryError_HasError(status)) {
    return;
  }

  char *formatted = NULL;
  va_list ap;
  va_start(ap, fmt);
  rm_vasprintf(&formatted, fmt, ap);
  va_end(ap);

  rm_asprintf(&status->_detail, "%s%s", message, formatted);
  rm_free(formatted);
  status->_code = code;
  status->_message = message;
}

void QueryError_SetWithoutUserDataFmt(QueryError *status, QueryErrorCode code, const char *fmt, ...) {
  if (QueryError_HasError(status)) {
    return;
  }
  va_list ap;
  va_start(ap, fmt);
  rm_vasprintf(&status->_detail, fmt, ap);
  va_end(ap);
  status->_code = code;
  status->_message = status->_detail;
}

void QueryError_MaybeSetCode(QueryError *status, QueryErrorCode code) {
  // Set the code if not previously set. This should be used by code which makes
  // use of the ::detail field, and is a placeholder for something like:
  // functionWithCharPtr(&status->_detail);
  // if (status->_detail && status->_code == QUERY_ERROR_CODE_OK) {
  //    status->_code = MYCODE;
  // }
  if (status->_detail == NULL) {
    return;
  }
  if (QueryError_HasError(status)) {
    return;
  }
  status->_code = code;
}

const char *QueryError_GetUserError(const QueryError *status) {
  return status->_detail ? status->_detail : QueryError_Strerror(status->_code);
}

const char *QueryError_GetDisplayableError(const QueryError *status, bool obfuscate) {
  if (status->_detail == NULL || obfuscate) {
    return status->_message ? status->_message : QueryError_Strerror(status->_code);
  } else {
    return status->_detail ? status->_detail : QueryError_Strerror(status->_code);
  }
}

QueryErrorCode QueryError_GetCode(const QueryError *status) {
  return status->_code;
}

QueryErrorCode QueryError_GetCodeFromMessage(const char *errorMessage) {
  if (!errorMessage) {
    return QUERY_ERROR_CODE_GENERIC;
  }

  if (!strcmp(errorMessage, QueryError_Strerror(QUERY_ERROR_CODE_TIMED_OUT))) {
    return QUERY_ERROR_CODE_TIMED_OUT;
  }

  if (!strcmp(errorMessage, QueryError_Strerror(QUERY_ERROR_CODE_OUT_OF_MEMORY))) {
    return QUERY_ERROR_CODE_OUT_OF_MEMORY;
  }

  return QUERY_ERROR_CODE_GENERIC;
}

bool QueryError_HasReachedMaxPrefixExpansionsWarning(const QueryError *status) {
  return status->_reachedMaxPrefixExpansions;
}

void QueryError_SetReachedMaxPrefixExpansionsWarning(QueryError *status) {
  status->_reachedMaxPrefixExpansions = true;
}

bool QueryError_HasQueryOOMWarning(const QueryError *status) {
  return status->_queryOOM;
}

void QueryError_SetQueryOOMWarning(QueryError *status) {
  status->_queryOOM = true;
}
