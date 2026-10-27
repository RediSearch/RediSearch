/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#pragma once

#include "geo_index_types.h"
#include "geo_ffi.h"

/* Create a geo filter from parsed strings and numbers */
GeoFilter *NewGeoFilter(double lon, double lat, double radius, const char *unit, size_t unit_len);

// Sets `gf->fieldIndex` from `fs` (or RS_INVALID_FIELD_INDEX if NULL).
void GeoFilter_SetField(GeoFilter *gf, const FieldSpec *fs);

/** @param s CString (null-terminated string) */
GeoDistance GeoDistance_Parse(const char *s);
const char *GeoDistance_ToString(GeoDistance dist);

/** @param s non null-terminated string */
GeoDistance GeoDistance_Parse_Buffer(const char *s, size_t len);

/* Make sure that the parameters of the filter make sense - i.e. coordinates are in range, radius is
 * sane, unit is valid. Return 1 if valid, 0 if not, and set the error string into err */
int GeoFilter_Validate(const GeoFilter *gf, QueryError *status);

/* Parse a geo filter from redis arguments. We assume the filter args start at argv[0] */
int GeoFilter_LegacyParse(LegacyGeoFilter *gf, ArgsCursor *ac, bool *hasEmptyFilterValue,
                          QueryError *status);
void GeoFilter_Free(GeoFilter *gf);
void LegacyGeoFilter_Free(LegacyGeoFilter *gf);

int isWithinRadius(const GeoFilter *gf, double d, double *distance);
