/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#include "index_loading.h"

#include <ctype.h>

#include "config.h"
#include "query.h"
#include "query_error_ffi.h"
#include "rmalloc.h"

// Admits an alias that SORTBY, APPLY and FILTER can reference as `@alias`: one or more bytes, none
// of them ASCII punctuation (`_` excepted), control or space. Stricter than the expression lexer's
// `property` rule, which also accepts punctuation escaped with a backslash (`@a\-b`); relaxing it
// later is not a breaking change.
static bool isValidPropertyName(const char *name, size_t len) {
  for (size_t i = 0; i < len; i++) {
    unsigned char c = name[i];
    if (c != '_' && (ispunct(c) || c <= ' ' || c == 0x7f)) {
      return false;
    }
  }
  return len > 0;
}

int IndexLoading_ApplyAlias(QueryNode *qn, QueryAttribute *attr, QueryError *status) {
  if (!RSGlobalConfig.enableUnstableFeatures) {
    QueryError_SetError(status, QUERY_ERROR_CODE_INVAL,
                        "`$as` is unavailable when `ENABLE_UNSTABLE_FEATURES` is off. "
                        "Enable it with `CONFIG SET search-enable-unstable-features yes`");
    return 0;
  }
  if (qn->type != QN_NUMERIC) {
    QueryError_SetError(status, QUERY_ERROR_CODE_BAD_ATTR,
                        "`$as` is supported only on NUMERIC clauses");
    return 0;
  }
  if (!isValidPropertyName(attr->value, attr->vallen)) {
    QueryError_SetWithUserDataFmt(status, QUERY_ERROR_CODE_BAD_ATTR,
                                  "`$as` requires a valid property name", ": %.*s",
                                  (int)attr->vallen, attr->value);
    return 0;
  }
  // The pipeline names its own row keys `__key`, `__score` and so on; an alias shadowing one would
  // corrupt the rows that rely on it, such as the FT.HYBRID merge by `__key`.
  if (attr->vallen >= 2 && attr->value[0] == '_' && attr->value[1] == '_') {
    QueryError_SetWithUserDataFmt(status, QUERY_ERROR_CODE_BAD_ATTR,
                                  "`$as` aliases starting with `__` are reserved", ": %.*s",
                                  (int)attr->vallen, attr->value);
    return 0;
  }
  // Move ownership of the value string, so it won't get freed with the QueryAttribute.
  rm_free(qn->opts.distField);
  qn->opts.distField = (char *)attr->value;
  attr->value = NULL;
  qn->opts.flags |= QueryNodeFlags_LoadsValue;
  return 1;
}

static int nodeDoesNotLoad(QueryNode *qn, QueryNode *root, void *ctx) {
  return !IndexLoading_NodeLoads(qn);
}

bool IndexLoading_TreeLoads(QueryNode *root) {
  return !QueryNode_ForEach(root, nodeDoesNotLoad, NULL, 0);
}
