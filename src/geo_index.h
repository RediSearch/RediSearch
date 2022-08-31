
#pragma once

#include "redisearch.h"
#include "redismodule.h"
#include "index_result.h"
#include "index_iterator.h"
#include "search_ctx.h"
#include "query_error.h"

///////////////////////////////////////////////////////////////////////////////////////////////

struct GeoFilter;
struct FieldSpec;

//---------------------------------------------------------------------------------------------

struct GeoIndex : Object {
  RedisSearchCtx *ctx;
  const FieldSpec fs;

  GeoIndex(RedisSearchCtx *ctx, const FieldSpec &fs) : ctx(ctx), fs(fs) {}

  int AddStrings(t_docId docId, const char *slon, const char *slat);
  void RemoveEntries(t_docId docId);
  void RemoveEntries(IndexSpec *sp, t_docId docId);

  IndexIterator *NewGeoRangeIterator(const GeoFilter &gf, double weight);
  Vector<t_docId> RangeLoad(const GeoFilter &gf) const;
};

#define GEOINDEX_KEY_FMT "geo:%s/%s"

//---------------------------------------------------------------------------------------------

struct GeoDistance {
  typedef GeoDistance This;

  enum Unit {
    INVALID = -1, // Placeholder for bad/invalid unit
#define X_GEO_DISTANCE(X) \
    X(KM, "km")           \
    X(M, "m")             \
    X(FT, "ft")           \
    X(MI, "mi")

#define X(c, unused) c,
    X_GEO_DISTANCE(X)
#undef X
  } dist;

  GeoDistance() : dist(Unit::INVALID) {}
  GeoDistance(Unit u) : dist(u) {}
  GeoDistance(const char *s);

  const char *ToString() const;

  bool operator==(const This &gd) const { return dist == gd.dist; }
  //This &operator=(Unit u) { dist = u; return *this; }
  This &operator=(const This &x) { dist = x.dist; return *this; }
};

//---------------------------------------------------------------------------------------------

struct GeoFilter : Object {
  double lon;
  double lat;
  double radius;
  GeoDistance unitType;
  String property;

  GeoFilter(double lon, double lat, double radius, const char *unit);

  GeoFilter(ArgsCursor *ac, QueryError *status);

  bool Validate(QueryError *status);
};

///////////////////////////////////////////////////////////////////////////////////////////////
