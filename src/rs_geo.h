/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#pragma once

#include "geohash/geohash_helper.h"
#include "geo_index.h"

#define GEO_RANGE_COUNT 9

/*
 * Encode longetude and latitude doubles into a single double.
 * This value can be sorted and used for distance.
 */
int  encodeGeo(double lon, double lat, double *bits);

/*
 * Decode longetude and latitude doubles from a single double.
 */
int  decodeGeo(double bits, double *xy);

/*
 * Calculate which neighboring squares around a point contain the given radius.
 *
 * `isWithinRadiusLonLat` must be use the filter out results that are within
 * the squares but not in radius.
 */
void calcRanges(double longitude, double latitude, double radius_meters,
                GeoHashRange *ranges);

/*
 * Return true is distance is smaller than radius. radius must be in meters.
 * If `distance' is not NULL, the distance value is returned.
 */
bool isWithinRadiusLonLat(double lon1, double lat1,
                         double lon2, double lat2,
                         double radius, double *distance);

int parseGeo(const char *c, size_t len, double *lon, double *lat);
