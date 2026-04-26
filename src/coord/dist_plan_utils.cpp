/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "dist_plan_utils.h"
#include "aggregate/reducers/collect.h"
#include <stdio.h>
#include <string.h>

void buildShardCollectArgs(ArgsCursor *out, void **objs_buf, char *count_buf,
                           const ArgsCursor *src_args) {
  size_t argc = src_args->argc;
  snprintf(count_buf, 16, "%zu", argc);

  objs_buf[0] = count_buf;
  memcpy(objs_buf + 1, src_args->objs, argc * sizeof(void *));

  out->objs = objs_buf;
  out->argc = argc + 1;
  out->offset = 0;
  out->type = AC_TYPE_CHAR;
}

void buildCoordCollectArgs(ArgsCursor *out, void **objs_buf, char *count_buf,
                           const ArgsCursor *src_args,
                           const char *shard_alias, const char *user_alias) {
  size_t argc = src_args->argc;
  snprintf(count_buf, 16, "%zu", argc + 2);

  objs_buf[0] = count_buf;
  memcpy(objs_buf + 1, src_args->objs, argc * sizeof(void *));
  objs_buf[argc + 1] = (void *)COLLECT_SOURCE_KEY;
  objs_buf[argc + 2] = (void *)shard_alias;
  objs_buf[argc + 3] = (void *)"AS";
  objs_buf[argc + 4] = (void *)user_alias;

  out->objs = objs_buf;
  out->argc = argc + 5;
  out->offset = 0;
  out->type = AC_TYPE_CHAR;
}
