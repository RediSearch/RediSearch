#include "query_error.h"
void QueryError_FmtUnknownArg(QueryError *err, ArgsCursor *ac, const char *name) {
  const char *s;
  size_t n;
  if (!AC_GetString(ac, &s, &n, AC_F_NOADVANCE) != AC_OK) {
    s = "Unknown (FIXME)";
    n = strlen(s);
  }

  QueryError_SetErrorFmt(err, QUERY_EPARSEARGS, "Unknown argument `%s.*` for %s", (int)n, s, name);
}