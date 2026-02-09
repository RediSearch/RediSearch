/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "vector_query_utils.h"
#include "rmalloc.h"
#include "../util/arr.h"
#include "../vector_index.h"

void ParsedVectorData_Free(ParsedVectorData *pvd) {
  if (!pvd) return;

  if (pvd->query) {
    VectorQuery_Free(pvd->query);
  }

  // Free attributes array, attribute names are NOT owned (point to parser tokens), only values are freed.
  if (pvd->attributes) {
    array_free_ex(pvd->attributes, rm_free((char*)((QueryAttribute*)ptr)->value));
  }

  if (pvd->vectorScoreFieldAlias) {
    rm_free(pvd->vectorScoreFieldAlias);
  }

  rm_free(pvd);
}
