/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#include <stdlib.h>
#include <stdio.h>
#include "rmutil/rm_assert.h"
#include "libnu/libnu.h"
#include "rmutil/util.h"
#include "rmutil/strings.h"
#include "rmalloc.h"
#include "buffer.h"
#include "sortable.h"
#include "sorting_vector_ffi.h"

/* Load a sorting vector from RDB */
RSSortingVector SortingVector_RdbLoad(RedisModuleIO *rdb) {

  int len = (int)RedisModule_LoadUnsigned(rdb);
  if (len > RS_SORTABLES_MAX || len <= 0) {
    return RSSortingVector_Empty();
  }
  RSSortingVector vec = RSSortingVector_New(len);
  for (int i = 0; i < len; i++) {
    RSValueType t = RedisModule_LoadUnsigned(rdb);

    switch (t) {
      case RSValueType_String: {
        size_t len;
        // strings include an extra character for null terminator. we set it to zero just in case
        char *s = RedisModule_LoadStringBuffer(rdb, &len);
        s[len - 1] = '\0';
        RSSortingVector_PutStr(&vec, i, s);
        RedisModule_Free(s);
        break;
      }
      case RSValueType_Number:
        // load numeric value
        RSSortingVector_PutNum(&vec, i, RedisModule_LoadDouble(rdb));
        break;
      // for nil we read nothing
      case RSValueType_Null:
      default:
        RSSortingVector_PutNull(&vec, i);
        break;
    }
  }
  return vec;
}
