/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "score_explain_mr.h"
#include "rmalloc.h"
#include "rmutil/rm_assert.h"
#include <string.h>

// Allocate via rm_malloc so SEDestroy's rm_free is balanced.
static char *dupStringReply(const MRReply *r) {
  size_t len = 0;
  const char *s = MRReply_String(r, &len);
  RS_LOG_ASSERT(s, "SE_FromMRReply: expected a string reply for explain node label");
  char *out = rm_malloc(len + 1);
  memcpy(out, s, len);
  out[len] = '\0';
  return out;
}

static void fillFromMRReply(RSScoreExplain *node, const MRReply *r) {
  RS_LOG_ASSERT(r, "SE_FromMRReply: NULL reply node");
  const int type = MRReply_Type(r);
  if (type == MR_REPLY_STRING || type == MR_REPLY_STATUS) {
    node->str = dupStringReply(r);
    node->numChildren = 0;
    node->children = NULL;
    return;
  }
  RS_LOG_ASSERT(type == MR_REPLY_ARRAY && MRReply_Length(r) == SE_REPLY_NODE_ARITY,
                "SE_FromMRReply: malformed explain node (serializer/deserializer mismatch)");

  node->str = dupStringReply(MRReply_ArrayElement(r, 0));
  const MRReply *childArr = MRReply_ArrayElement(r, 1);
  RS_LOG_ASSERT(childArr && MRReply_Type(childArr) == MR_REPLY_ARRAY,
                "SE_FromMRReply: malformed explain node children");

  const size_t n = MRReply_Length(childArr);
  node->numChildren = (int)n;
  node->children = n ? rm_calloc(n, sizeof(RSScoreExplain)) : NULL;
  for (size_t i = 0; i < n; i++) {
    fillFromMRReply(&node->children[i], MRReply_ArrayElement(childArr, i));
  }
}

RSScoreExplain *SE_FromMRReply(const MRReply *r) {
  RSScoreExplain *root = rm_calloc(1, sizeof(RSScoreExplain));
  fillFromMRReply(root, r);
  return root;
}
