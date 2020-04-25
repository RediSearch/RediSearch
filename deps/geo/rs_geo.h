#ifndef __RS_GEO_H__
#define __RS_GEO_H__

#include "geohash_helper.h"
#include "../../src/geo_index.h"

#define GEO_RANGE_COUNT 9

int encodeGeo(double lon, double lat, double *bits);
int decodeGeo(double bits, double *xy);
void calcRanges(double longitude, double latitude, double radius_meters,
                GeoHashRange *ranges);
int isWithinRadiusLonLat(double lon1, double lat1,
                         double lon2, double lat2,
                         double radius, double *distance);

#endif
