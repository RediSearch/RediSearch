/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

// reference.c is the unchanged src/geo_index.c at
// 58fba8cb56f5fcdeb91e1915db26974f6e469c07. Keep the C comparison executable
// after the production implementation is removed.
#define GeoFilter_LegacyParse CGeoFilter_LegacyParse
#define GeoFilter_SetField CGeoFilter_SetField
#define GeoFilter_Free CGeoFilter_Free
#define LegacyGeoFilter_Free CLegacyGeoFilter_Free
#define GeoDistance_Parse CGeoDistance_Parse
#define GeoDistance_Parse_Buffer CGeoDistance_Parse_Buffer
#define GeoDistance_ToString CGeoDistance_ToString
#define NewGeoFilter CNewGeoFilter
#define GeoFilter_Validate CGeoFilter_Validate
#define isWithinRadius CIsWithinRadius
#include "reference.c"
