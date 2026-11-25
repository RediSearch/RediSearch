/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#include "index_loading.h"

#include "config.h"
#include "query.h"
#include "query_error_ffi.h"
#include "rmalloc.h"

// Admits an alias that SORTBY, APPLY and FILTER can reference as `@alias`: one or more bytes, none
// of them ASCII punctuation (`_` excepted), control or space. Stricter than the expression lexer's
// `property` rule, which also accepts punctuation escaped with a backslash (`@a\-b`); relaxing it
// later is not a breaking change.
static bool isValidPropertyName(const char *name, size_t len) {
  if (len == 0) {
    return false;
  }
  for (size_t i = 0; i < len; i++) {
    unsigned char c = name[i];
    bool asciiPunct = (c >= 0x21 && c <= 0x2f) || (c >= 0x3a && c <= 0x40) ||
                      (c >= 0x5b && c <= 0x60) || (c >= 0x7b && c <= 0x7e);
    bool asciiCntrlOrSpace = c <= 0x20 || c == 0x7f;
    if (c != '_' && (asciiPunct || asciiCntrlOrSpace)) {
      return false;
    }
  }
  return true;
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

static int nodeDoesNotLoad(QueryNode *node, QueryNode *root, void *ctx) {
  return !IndexLoading_NodeLoads(node);
}

bool IndexLoading_TreeLoads(QueryNode *root) {
  // No node loads while the gate is off, so skip the walk. A query parsed just before the gate was
  // turned off may then lose its loaded values, never more.
  if (!RSGlobalConfig.enableUnstableFeatures) {
    return false;
  }
  // `QueryNode_ForEach` stops, and returns 0, at the first node the callback rejects.
  return !QueryNode_ForEach(root, nodeDoesNotLoad, NULL, 0);
}
