/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#include "reply_value.h"
#include "rmalloc.h"
#include "rmutil/rm_assert.h"

// Converts an MRReply to an RSValue, consuming the reply. String buffers can be
// transferred directly because hiredis uses the Redis module allocator.
RSValue *MRReply_ToValue(MRReply *r) {
  if (!r) return RSValue_NullStatic();
  RSValue *v = NULL;
  switch (MRReply_Type(r)) {
    case MR_REPLY_STATUS:
    case MR_REPLY_STRING: {
      size_t l;
      char *s = MRReply_TakeString(r, &l);
      RS_ASSERT(l <= UINT32_MAX);
      v = RSValue_NewString(s, (uint32_t)l);
      break;
    }
    case MR_REPLY_ERROR: {
      double d = 42;
      MRReply_ToDouble(r, &d);
      v = RSValue_NewNumber(d);
      break;
    }
    case MR_REPLY_INTEGER:
      v = RSValue_NewNumber((double)MRReply_Integer(r));
      break;
    case MR_REPLY_DOUBLE:
      v = RSValue_NewNumber(MRReply_Double(r));
      break;
    case MR_REPLY_MAP: {
      size_t n = MRReply_Length(r);
      RS_LOG_ASSERT(n % 2 == 0, "map of odd length");
      size_t map_len = n / 2;
      RSValueMapBuilder *map = RSValue_NewMapBuilder(map_len);
      for (size_t i = 0; i < map_len; i++) {
        MRReply *e_k = MRReply_TakeArrayElement(r, i * 2);
        RS_LOG_ASSERT(MRReply_Type(e_k) == MR_REPLY_STRING, "non-string map key");
        MRReply *e_v = MRReply_TakeArrayElement(r, (i * 2) + 1);
        RSValue_MapBuilderSetEntry(map, i, MRReply_ToValue(e_k), MRReply_ToValue(e_v));
      }
      v = RSValue_NewMapFromBuilder(map);
      break;
    }
    case MR_REPLY_ARRAY: {
      size_t n = MRReply_Length(r);
      RSValue **arr = RSValue_NewArrayBuilder(n);
      for (size_t i = 0; i < n; ++i) {
        arr[i] = MRReply_ToValue(MRReply_TakeArrayElement(r, i));
      }
      v = RSValue_NewArrayFromBuilder(arr, n);
      break;
    }
    case MR_REPLY_NIL:
      v = RSValue_NullStatic();
      break;
    default:
      v = RSValue_NullStatic();
      break;
  }
  MRReply_Free(r);
  return v;
}
