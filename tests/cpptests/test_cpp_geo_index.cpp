/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#include "common.h"
extern "C" {
#include "geo_index.h"
#include "query_error_ffi.h"
}
#include "gtest/gtest.h"

#include <cmath>
#include <limits>

TEST(GeoIndex, UnitsAndConstruction) {
  const struct {
    const char* text;
    GeoDistance unit;
  } cases[] = {{"km", GEO_DISTANCE_KM},       {"M", GEO_DISTANCE_M},
               {"fT", GEO_DISTANCE_FT},       {"Mi", GEO_DISTANCE_MI},
               {"", GEO_DISTANCE_INVALID},    {"meters", GEO_DISTANCE_INVALID},
               {" km", GEO_DISTANCE_INVALID}, {"km ", GEO_DISTANCE_INVALID},
               {"\xff", GEO_DISTANCE_INVALID}};
  for (const auto& c : cases) {
    EXPECT_EQ(GeoDistance_Parse(c.text), c.unit);
    EXPECT_EQ(GeoDistance_Parse_Buffer(c.text, strlen(c.text)), c.unit);
  }
  EXPECT_EQ(GeoDistance_Parse_Buffer("km\0garbage", 10), GEO_DISTANCE_KM);
  EXPECT_EQ(GeoDistance_Parse_Buffer("km\0xxxxxxxxxxxxx", 16), GEO_DISTANCE_INVALID);
  EXPECT_STREQ(GeoDistance_ToString(GEO_DISTANCE_INVALID), "<badunit>");
  EXPECT_STREQ(GeoDistance_ToString(GEO_DISTANCE_FT), "ft");

  GeoFilter* gf = NewGeoFilter(31.5, -24.75, 12.25, nullptr, 0);
  EXPECT_EQ(gf->unitType, GEO_DISTANCE_KM);
  EXPECT_EQ(gf->fieldIndex, RS_INVALID_FIELD_INDEX);
  EXPECT_EQ(gf->numericFilters, nullptr);
  EXPECT_DOUBLE_EQ(gf->lon, 31.5);
  EXPECT_DOUBLE_EQ(gf->lat, -24.75);
  EXPECT_DOUBLE_EQ(gf->radius, 12.25);
  FieldSpec field = {};
  field.index = 7;
  GeoFilter_SetField(gf, &field);
  EXPECT_EQ(gf->fieldIndex, 7);
  GeoFilter_SetField(gf, nullptr);
  EXPECT_EQ(gf->fieldIndex, RS_INVALID_FIELD_INDEX);
  GeoFilter_Free(gf);
}

TEST(GeoIndex, ValidationOrderAndBoundaries) {
  GeoFilter gf = {};
  gf.unitType = GEO_DISTANCE_KM;
  gf.radius = 1;
  for (double lat : {-90.0, 90.0}) {
    for (double lon : {-180.0, 180.0}) {
      gf.lat = lat;
      gf.lon = lon;
      QueryError error = QueryError_Default();
      EXPECT_EQ(GeoFilter_Validate(&gf, &error), 1);
      EXPECT_FALSE(QueryError_HasError(&error));
      QueryError_ClearError(&error);
    }
  }
  gf.unitType = GEO_DISTANCE_INVALID;
  gf.lon = 181;
  gf.radius = 0;
  for (const char* expected :
       {"SEARCH_SYNTAX Invalid GeoFilter unit", "SEARCH_SYNTAX Invalid GeoFilter lat/lon",
        "SEARCH_SYNTAX Invalid GeoFilter radius"}) {
    QueryError error = QueryError_Default();
    EXPECT_EQ(GeoFilter_Validate(&gf, &error), 0);
    EXPECT_STREQ(QueryError_GetUserError(&error), expected);
    QueryError_ClearError(&error);
    if (gf.unitType == GEO_DISTANCE_INVALID) {
      gf.unitType = GEO_DISTANCE_KM;
    } else {
      gf.lon = 0;
    }
  }
  gf.lat = 0;
  gf.radius = std::numeric_limits<double>::infinity();
  QueryError error = QueryError_Default();
  EXPECT_EQ(GeoFilter_Validate(&gf, &error), 1);
  // Validation itself uses comparisons, not a finiteness check. The command parser
  // rejects NaN before it reaches execution; do not run a NaN filter here.
  gf.lon = gf.lat = gf.radius = std::numeric_limits<double>::quiet_NaN();
  EXPECT_EQ(GeoFilter_Validate(&gf, &error), 1);
  QueryError_ClearError(&error);
}

TEST(GeoIndex, RadiusIncludesBoundaryAndWritesDistanceOnMiss) {
  const double score = encodeGeo(12.25, -33.5);
  double xy[2];
  decodeGeo(score, xy);
  const double expected = geohashGetDistance(12.0, -33.25, xy[0], xy[1]);
  GeoFilter gf = {};
  gf.lon = 12.0;
  gf.lat = -33.25;
  gf.unitType = GEO_DISTANCE_M;
  gf.radius = expected;
  double actual = -1;
  EXPECT_EQ(isWithinRadius(&gf, score, &actual), 1);
  EXPECT_DOUBLE_EQ(actual, expected);
  gf.radius = std::nextafter(expected, 0.0);
  EXPECT_EQ(isWithinRadius(&gf, score, &actual), 0);
  EXPECT_DOUBLE_EQ(actual, expected);
  for (const auto& [unit, factor] :
       {std::pair{GEO_DISTANCE_KM, 1000.0}, std::pair{GEO_DISTANCE_FT, 0.3048},
        std::pair{GEO_DISTANCE_MI, 1609.34}}) {
    gf.unitType = unit;
    gf.radius = (expected + 1) / factor;
    EXPECT_EQ(isWithinRadius(&gf, score, nullptr), 1);
    gf.radius = (expected - 1) / factor;
    EXPECT_EQ(isWithinRadius(&gf, score, nullptr), 0);
  }
}

TEST(GeoIndex, LegacyParseCursorAndFailureCleanup) {
  const char* args[] = {"place", "31.5", "-24.75", "12.25", "KM", "tail"};
  ArgsCursor cursor;
  ArgsCursor_InitCString(&cursor, args, 6);
  auto* gf = static_cast<LegacyGeoFilter*>(rm_malloc(sizeof(LegacyGeoFilter)));
  QueryError error = QueryError_Default();
  bool empty = false;
  ASSERT_EQ(GeoFilter_LegacyParse(gf, &cursor, &empty, &error), REDISMODULE_OK);
  EXPECT_EQ(cursor.offset, 5);
  EXPECT_FALSE(empty);
  EXPECT_EQ(gf->base.fieldIndex, RS_INVALID_FIELD_INDEX);
  EXPECT_DOUBLE_EQ(gf->base.lon, 31.5);
  EXPECT_DOUBLE_EQ(gf->base.lat, -24.75);
  EXPECT_DOUBLE_EQ(gf->base.radius, 12.25);
  EXPECT_STREQ(HiddenString_GetUnsafe(gf->field, nullptr), "place");
  LegacyGeoFilter_Free(gf);

  for (size_t bad = 1; bad <= 4; ++bad) {
    const char* invalid[] = {"place", "31.5", "-24.75", "12.25", "km"};
    invalid[bad] = "bad";
    ArgsCursor_InitCString(&cursor, invalid, 5);
    gf = static_cast<LegacyGeoFilter*>(rm_malloc(sizeof(LegacyGeoFilter)));
    EXPECT_EQ(GeoFilter_LegacyParse(gf, &cursor, &empty, &error), REDISMODULE_ERR);
    EXPECT_EQ(cursor.offset, bad == 4 ? 5 : bad);
    EXPECT_EQ(gf->field, nullptr);
    EXPECT_EQ(gf->base.numericFilters, nullptr);
    LegacyGeoFilter_Free(gf);
    QueryError_ClearError(&error);
  }
  ArgsCursor_InitCString(&cursor, args, 4);
  gf = static_cast<LegacyGeoFilter*>(rm_malloc(sizeof(LegacyGeoFilter)));
  EXPECT_EQ(GeoFilter_LegacyParse(gf, &cursor, &empty, &error), REDISMODULE_ERR);
  EXPECT_EQ(cursor.offset, 0);
  EXPECT_STREQ(QueryError_GetUserError(&error), "SEARCH_PARSE_ARGS GEOFILTER requires 5 arguments");
  LegacyGeoFilter_Free(gf);
  QueryError_ClearError(&error);
}
