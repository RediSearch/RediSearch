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
  RS_ASSERT(pvq);

  // pvq.fieldName and pvq.vector are NOT owned (just a reference to args) - don't free it

  // Free QueryAttribute array, attribute names are NOT owned (point to parser tokens), only values are freed.
  if (pvq->attributes) {
    array_free_ex(pvq->attributes, rm_free((char*)((QueryAttribute*)ptr)->value));
  }

  rm_free(pvq);
}
