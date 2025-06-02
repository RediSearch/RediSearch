/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#pragma once

#include "redisearch.h"
#include "field_spec.h"

typedef struct NumericFilter {
  const FieldSpec *fieldSpec;
  double min;             // beginning of range
  double max;             // end of range
  const void *geoFilter;  // geo filter
  bool inclusiveMin;      // range includes min value
  bool inclusiveMax;      // range includes max val

  // used by optimizer
  bool asc;       // order of SORTBY asc/desc
  size_t limit;   // minimum number of result needed
  size_t offset;  // record number of documents in iterated ranges. used to skip them
} NumericFilter;
