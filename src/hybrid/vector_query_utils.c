/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "vector_query_utils.h"
#include "../rmalloc.h"
#include "../util/arr.h"

void ParsedVectorQuery_Free(ParsedVectorQuery *pvq) {
  if (!pvq) return;

  // Vector data is NOT owned (just a reference to args) - don't free it

  // Free QueryAttribute arrays with callback
  if (pvq->attributes) {
    array_free_ex(pvq->attributes, {
      QueryAttribute *attr = (QueryAttribute*)ptr;
      // Only free values that weren't transferred (still non-NULL)
      if (attr->value) {
        rm_free((char*)attr->value);
      }
      // Note: .name is not freed because it points to string literals like "yield_distance_as"
    });
  }

  rm_free(pvq);
}
