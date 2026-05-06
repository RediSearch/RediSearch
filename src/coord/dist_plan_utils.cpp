/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "dist_plan_utils.h"
#include <string.h>
#include <string>

extern "C" {
#include "rmutil/rm_assert.h"
#include "util/misc.h"
}

ArgsCursor buildLocalCollectArgs(void **objs_buf, const char *count_buf,
                                 const ArgsCursor *src_args, const char *user_alias) {

  // Set args count as the first element
  objs_buf[0] = (void *)count_buf;
  // Copy the remaining args from the source args
  if (src_args->argc)
    memcpy(objs_buf + 1, src_args->objs, src_args->argc * sizeof(void *));
  // Set the AS keyword and user alias
  objs_buf[src_args->argc + 1] = (void *)"AS";
  objs_buf[src_args->argc + 2] = (void *)user_alias;

  ArgsCursor out;
  out.objs = objs_buf;
  out.type = AC_TYPE_CHAR;
  out.argc = collectObjsBufLen(src_args->argc, /*has_alias=*/true);
  out.offset = 0;
  return out;
}

bool parseCollectLimit(const ArgsCursor *src_args, uint64_t max_results, bool *out_present,
                       CollectLimit *out_limit, QueryError *status) {
  // Scan for the LIMIT keyword — loop does nothing but find the index.
  size_t limit_idx = src_args->argc;  // sentinel: not found
  for (size_t i = 0; i < src_args->argc; ++i) {
    if (strcasecmp((const char *)src_args->objs[i], "LIMIT") == 0) {
      limit_idx = i;
      break;
    }
  }

  if (limit_idx == src_args->argc) {
    *out_present = false;
    return true;
  }

  // Position a copy of the cursor right after LIMIT and read the next 2 tokens.
  ArgsCursor ac = *src_args;
  ac.offset = limit_idx + 1;

  if (AC_NumRemaining(&ac) < 2) {
    QueryError_SetError(status, QUERY_ERROR_CODE_PARSE_ARGS,
                        "LIMIT requires offset and count arguments");
    return false;
  }

  uint64_t offset, count;
  if (AC_GetU64(&ac, &offset, AC_F_GE0) != AC_OK) {
    QueryError_SetError(status, QUERY_ERROR_CODE_PARSE_ARGS,
                        "LIMIT offset is not a valid number");
    return false;
  }
  if (AC_GetU64(&ac, &count, AC_F_GE1) != AC_OK) {
    QueryError_SetError(status, QUERY_ERROR_CODE_PARSE_ARGS,
                        "LIMIT count is not a valid number");
    return false;
  }

  if (!ValidateLimitBounds(offset, count, max_results, status)) {
    return false;
  }

  *out_present = true;
  out_limit->offset = offset;
  out_limit->count = count;
  return true;
}

ShardCollectLimit rewriteCollectLimit(const CollectLimit *limit) {
  return {"0", std::to_string(limit->offset + limit->count)};
}

ArgsCursor buildRemoteCollectArgs(void **objs_buf, const char *count_buf,
                                  const ArgsCursor *src_args,
                                  const ShardCollectLimit *rewrite) {
  // Set args count as the first element
  objs_buf[0] = (void *)count_buf;
  // Copy the remaining args from the source args
  if (src_args->argc)
    memcpy(objs_buf + 1, src_args->objs, src_args->argc * sizeof(void *));

  if (rewrite) {
    // Scan for LIMIT and patch the offset + count slots.
    bool found = false;
    for (size_t i = 0; i + 2 <= src_args->argc; ++i) {
      if (strcasecmp((const char *)src_args->objs[i], "LIMIT") == 0) {
        objs_buf[1 + i + 1] = (void *)rewrite->offset.c_str();
        objs_buf[1 + i + 2] = (void *)rewrite->count.c_str();
        found = true;
        break;
      }
    }
    RS_ASSERT(found);  // rewrite != NULL implies parseCollectLimit found LIMIT
  }

  ArgsCursor out;
  out.objs = objs_buf;
  out.type = AC_TYPE_CHAR;
  out.argc = collectObjsBufLen(src_args->argc, /*has_alias=*/false);
  out.offset = 0;
  return out;
}
