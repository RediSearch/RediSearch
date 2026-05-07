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

ArgsCursor buildCollectArgs(void **objs_buf, const char *count_buf, const ArgsCursor *src_args,
                            const char *user_alias) {
  size_t argc = src_args->argc;

  objs_buf[0] = (void *)count_buf;
  if (argc) {
    memcpy(objs_buf + 1, src_args->objs, argc * sizeof(void *));
  }

  ArgsCursor out;
  if (user_alias) {
    objs_buf[argc + 1] = (void *)"AS";
    objs_buf[argc + 2] = (void *)user_alias;
    out.argc = argc + 3;
  } else {
    out.argc = argc + 1;
  }

  out.objs = objs_buf;
  out.offset = 0;
  out.type = AC_TYPE_CHAR;
  return out;
}
