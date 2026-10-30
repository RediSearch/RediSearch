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
#include "obfuscation/hidden.h"

typedef struct NumericFilter NumericFilter;

typedef enum {  // Placeholder for bad/invalid unit
  GEO_DISTANCE_INVALID = -1,
  GEO_DISTANCE_KM,
  GEO_DISTANCE_M,
  GEO_DISTANCE_FT,
  GEO_DISTANCE_MI,
} GeoDistance;

typedef struct GeoFilter {
  // Stable index of the field into IndexSpec.fields; re-derive the FieldSpec* from
  // this at evaluation time - a pointer captured at construction time may
  // already be freed by then
  t_fieldIndex fieldIndex;
  double lat;
  double lon;
  double radius;
  GeoDistance unitType;
  NumericFilter **numericFilters;
} GeoFilter;

// Legacy geo filter
// This struct is used to parse the legacy query syntax and convert it to the new query syntax
// When parsing the legacy filters we do not have the index spec and we only have the field name
// For that reason during the parsing phase the base.fieldIndex will be RS_INVALID_FIELD_INDEX
// We will fill it during the apply context phase where we will use the field name to find the field
// spec This struct was added in order to fix previous behaviour where the string pointer was stored
// inside the field spec pointer
typedef struct {
  GeoFilter base;
  HiddenString *field;
} LegacyGeoFilter;
