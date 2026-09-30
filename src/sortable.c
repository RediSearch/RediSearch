/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#include <stdint.h>
#include <stdio.h>

#include "sortable.h"
#include "value_ffi.h"
#include "sorting_vector_ffi.h"

// Element tags as written by the 1.x `SortingVector_RdbSave`. They are not `RSValueType`
// values, whose numbering has since changed; every other tag, including the legacy null tag,
// has no payload.
#define LEGACY_SORTABLE_NUM 1
#define LEGACY_SORTABLE_STR 3

/* Load a sorting vector from RDB */
RSSortingVector SortingVector_RdbLoad(RedisModuleIO *rdb) {

  int len = (int)RedisModule_LoadUnsigned(rdb);
  if (len > RS_SORTABLES_MAX || len <= 0) {
    return RSSortingVector_Empty();
  }
  RSSortingVector vec = RSSortingVector_New(len);
  for (int i = 0; i < len; i++) {
    uint64_t t = RedisModule_LoadUnsigned(rdb);

    switch (t) {
      case LEGACY_SORTABLE_STR: {
        size_t len = 0;
        // strings include an extra character for null terminator. we set it to zero just in case
        char *s = RedisModule_LoadStringBuffer(rdb, &len);
        if (!s) {
          // A failed read; the IO error stays recorded on `rdb` for the caller.
          RSSortingVector_PutNull(&vec, i);
          break;
        }
        if (len > 0) {
          s[len - 1] = '\0';
        }
        RSSortingVector_PutStr(&vec, i, len > 0 ? s : "");
        RedisModule_Free(s);
        break;
      }
      case LEGACY_SORTABLE_NUM:
        // load numeric value
        RSSortingVector_PutNum(&vec, i, RedisModule_LoadDouble(rdb));
        break;
      // for nil we read nothing
      default:
        RSSortingVector_PutNull(&vec, i);
        break;
    }
  }
  return vec;
}
