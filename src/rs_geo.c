/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "rs_geo.h"

int encodeGeo(double lon, double lat, double *bits) {
  GeoHashBits hash;
  int rv = geohashEncodeWGS84(lon, lat, GEO_STEP_MAX, &hash);
  *bits = (double)geohashAlign52Bits(hash);
  return rv;
}

int decodeGeo(double bits, double *xy) {
  GeoHashBits hash = {.bits = (uint64_t)bits, .step = GEO_STEP_MAX};
  return geohashDecodeToLongLatWGS84(hash, xy);
}

/* Compute the sorted set scores min (inclusive), max (exclusive) we should
 * query in order to retrieve all the elements inside the specified area
 * 'hash'. The two scores are returned by reference in *min and *max. */
static void scoresOfGeoHashBox(GeoHashBits hash, GeoHashFix52Bits *min, GeoHashFix52Bits *max) {
  /* We want to compute the sorted set scores that will include all the
   * elements inside the specified Geohash 'hash', which has as many
   * bits as specified by hash.step * 2.
   *
   * So if step is, for example, 3, and the hash value in binary
   * is 101010, since our score is 52 bits we want every element which
   * is in binary: 101010?????????????????????????????????????????????
   * Where ? can be 0 or 1.
   *
   * To get the min score we just use the initial hash value left
   * shifted enough to get the 52 bit value. Later we increment the
   * 6 bit prefis (see the hash.bits++ statement), and get the new
   * prefix: 101011, which we align again to 52 bits to get the maximum
   * value (which is excluded from the search). So we get everything
   * between the two following scores (represented in binary):
   *
   * 1010100000000000000000000000000000000000000000000000 (included)
   * and
   * 1010110000000000000000000000000000000000000000000000 (excluded).
   */
  *min = geohashAlign52Bits(hash);
  hash.bits++;
  *max = geohashAlign52Bits(hash);
}

/* Search all eight neighbors + self geohash box */
static void calcAllNeighbors(GeoHashRadius *n, double lon, double lat, double radius,
                             GeoHashRange *ranges) {
  GeoHashBits neighbors[GEO_RANGE_COUNT];
  unsigned int i, last_processed = 0;

  neighbors[0] = n->hash;
  neighbors[1] = n->neighbors.north;
  neighbors[2] = n->neighbors.south;
  neighbors[3] = n->neighbors.east;
  neighbors[4] = n->neighbors.west;
  neighbors[5] = n->neighbors.north_east;
  neighbors[6] = n->neighbors.north_west;
  neighbors[7] = n->neighbors.south_east;
  neighbors[8] = n->neighbors.south_west;

  /* For each neighbor (*and* our own hashbox), get all the matching
   * members and add them to the potential result list. */
  for (i = 0; i < GEO_RANGE_COUNT; i++) {
    if (HASHISZERO(neighbors[i])) {
      continue;
    }

    /* When a huge Radius (in the 5000 km range or more) is used,
     * adjacent neighbors can be the same, leading to duplicated
     * elements. Skip every range which is the same as the one
     * processed previously. */
    if (last_processed && neighbors[i].bits == neighbors[last_processed].bits &&
        neighbors[i].step == neighbors[last_processed].step) {
      continue;
    }

    GeoHashFix52Bits min, max;
    scoresOfGeoHashBox(neighbors[i], &min, &max);
    ranges[i].min = min;
    ranges[i].max = max;

    last_processed = i;
  }
}

/* Calculate range for relevant squares around center.
 * If min == max, range is included in other ranges */
void calcRanges(double longitude, double latitude, double radius_meters, GeoHashRange *ranges) {
  GeoHashRadius georadius = geohashGetAreasByRadiusWGS84(longitude, latitude, radius_meters);

  calcAllNeighbors(&georadius, longitude, latitude, radius_meters, ranges);
}

bool isWithinRadiusLonLat(double lon1, double lat1, double lon2, double lat2, double radius,
                          double *distance) {
  double dist = geohashGetDistance(lon1, lat1, lon2, lat2);
  if (distance != NULL) *distance = dist;
  if (dist > radius) return false;
  return true;
}

extern RedisModuleCtx *RSDummyContext;

int parseGeo(const char *c, size_t len, double *lon, double *lat, QueryError *status) {
  // pretect the heap from a large string. 128 is sufficient
  if (len > 128) {
    QueryError_SetError(status, QUERY_EPARSEARGS, "Geo string cannot be longer than 128 bytes");
    return REDISMODULE_ERR;
  }
  char str[len + 1];
  memcpy(str, c, len + 1);
  char *pos = strpbrk(str, " ,");
  if (!pos) {
    QueryError_SetError(status, QUERY_EPARSEARGS, "Invalid geo string");
    return REDISMODULE_ERR;
  }
  *pos = '\0';
  pos++;

  char *end1 = NULL, *end2 = NULL;
  *lon = strtod(str, &end1);
  *lat = strtod(pos, &end2);
  if (*end1 || *end2) {
    QueryError_SetError(status, QUERY_EPARSEARGS, "Invalid geo string");
    return REDISMODULE_ERR;
  }

  return REDISMODULE_OK;
}

/*
int isWithinRadius(double center, double point, double radius, double *distance) {
  double xyCenter[2], xyPoint[2];
  decodeGeo(center, xyCenter);
  decodeGeo(point, xyPoint);
  return isWithinRadiusLonLat(xyCenter[0], xyCenter[1], xyPoint[0], xyPoint[1],
                                    radius, distance);
}

IndexIterator *NewGeoRangeIterator(GeoIndex *gi, const GeoFilter *gf, double weight) {
  GeoHashRange ranges[GEO_RANGE_COUNT] = {0};
  calcRanges(gf, ranges);

  int iterCount = 0;
  IndexIterator **iters = rm_calloc(GEO_RANGE_COUNT, sizeof(*iters));
  for (size_t ii = 0; ii < GEO_RANGE_COUNT; ++ii) {
    if (ranges[ii].min != ranges[ii].max) {
      NumericFilter *filt = NewNumericFilter(ranges[ii].min, ranges[ii].max, 1, 1);
      iters[iterCount++] = NewNumericFilterIterator(NULL, filt, NULL);
    }
  }
  iters = rm_realloc(iters, iterCount * sizeof(*iters));
  IndexIterator *it = NewUnionIterator(iters, iterCount, NULL, 1, 1);
  return it;
}*/

