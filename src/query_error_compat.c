/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include <rmutil/rm_assert.h>
#include "rmalloc.h"
#include "query_error.h"

/**
 * Set the error code using a custom-formatted string
 *
 * Not implemented in Rust as variadic functions are not supported across an FFI boundary.
 */
void QueryError_SetWithUserDataFmt(QueryError *status, QueryErrorCode code, const char* message, const char *fmt, ...) {
  if (QueryError_HasError(status)) {
    return;
  }

  char *formatted = NULL;
  va_list ap;
  va_start(ap, fmt);
  rm_vasprintf(&formatted, fmt, ap);
  va_end(ap);

  char *detail = NULL;
  rm_asprintf(&detail, "%s%s", message, formatted);
  rm_free(formatted);

  QueryError_SetError(status, code, message);
  QueryError_SetDetail(status, detail);
  rm_free(detail);
}

/**
 * Set the error code using a custom-formatted string
 * Only use this function if you are certain that no user data is leaked in the format string
 *
 * Not implemented in Rust as variadic functions are not supported across an FFI boundary.
 */
void QueryError_SetWithoutUserDataFmt(QueryError *status, QueryErrorCode code, const char *fmt, ...) {
  if (QueryError_HasError(status)) {
    return;
  }

  char *formatted = NULL;
  va_list ap;
  va_start(ap, fmt);
  rm_vasprintf(&formatted, fmt, ap);
  va_end(ap);

  QueryError_SetError(status, code, formatted);
  rm_free(formatted);
}

/**
 * Not implemented in Rust yet as mocking ArgsCursor would be a large lift.
 *
 * Once `ArgsCursor` and `QueryError_SetWithUserDataFmt` are ported to Rust,
 * this should also be ported to Rust.
 */
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
