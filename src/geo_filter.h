/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#pragma once

#include "rqe_core.h"

typedef struct NumericFilter NumericFilter;
typedef struct HiddenString HiddenString;

typedef enum {
  GEO_DISTANCE_INVALID = -1,
  GEO_DISTANCE_KM = 0,
  GEO_DISTANCE_M = 1,
  GEO_DISTANCE_FT = 2,
  GEO_DISTANCE_MI = 3,
} GeoDistance;

typedef struct GeoFilter {
  // Stable index into IndexSpec.fields: a FieldSpec pointer may be freed before evaluation.
  t_fieldIndex fieldIndex;
  double lat;
  double lon;
  double radius;
  GeoDistance unitType;
  NumericFilter **numericFilters;
} GeoFilter;

// Legacy parsing precedes spec lookup, so the borrowed field name is resolved to
// base.fieldIndex when applying the query context.
typedef struct {
  GeoFilter base;
  HiddenString *field;
} LegacyGeoFilter;
