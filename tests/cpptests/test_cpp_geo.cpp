/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#include "gtest/gtest.h"
#include <cmath>
#include <cstring>
#include <memory>
#include "search_ctx.h"
#include "redismock/util.h"

extern "C" {
#include "geo_index.h"
#include "query_error_ffi.h"
#include "rmalloc.h"
#include "value_ffi.h"
}

// Exercise the Rust adapters before production callers switch away from C.
#define GeoDistance_Parse Rust_GeoDistance_Parse
#define GeoDistance_Parse_Buffer Rust_GeoDistance_Parse_Buffer
#define GeoDistance_ToString Rust_GeoDistance_ToString
#define NewGeoFilter Rust_NewGeoFilter
#define GeoFilter_SetField Rust_GeoFilter_SetField
#define GeoFilter_Validate Rust_GeoFilter_Validate
#define GeoFilter_Free Rust_GeoFilter_Free
#define GeoFilter_LegacyParse Rust_GeoFilter_LegacyParse
#define LegacyGeoFilter_Free Rust_LegacyGeoFilter_Free
#define isWithinRadius Rust_isWithinRadius
#define GeoDistance_Evaluate Rust_GeoDistance_Evaluate

namespace {
using Filter = std::unique_ptr<GeoFilter, decltype(&GeoFilter_Free)>;
using LegacyFilter = std::unique_ptr<LegacyGeoFilter, decltype(&LegacyGeoFilter_Free)>;

struct Error {
  QueryError value = QueryError_Default();
  ~Error() {
    QueryError_ClearError(&value);
  }
};
}  // namespace

TEST(GeoIndexTest, DistanceUnits) {
  struct Unit {
    const char *text;
    const char *mixedCase;
    GeoDistance value;
  };
  for (const auto &unit : {Unit{"km", "Km", GEO_DISTANCE_KM}, Unit{"m", "M", GEO_DISTANCE_M},
                           Unit{"ft", "fT", GEO_DISTANCE_FT}, Unit{"mi", "MI", GEO_DISTANCE_MI}}) {
    SCOPED_TRACE(unit.text);
    EXPECT_EQ(GeoDistance_Parse(unit.text), unit.value);
    EXPECT_EQ(GeoDistance_Parse(unit.mixedCase), unit.value);
    EXPECT_EQ(GeoDistance_Parse_Buffer(unit.mixedCase, strlen(unit.mixedCase)), unit.value);
    EXPECT_STREQ(GeoDistance_ToString(unit.value), unit.text);
  }
  for (const char *invalid : {"", "meters", "k", " km", "km ", "miles"}) {
    SCOPED_TRACE(invalid);
    EXPECT_EQ(GeoDistance_Parse(invalid), GEO_DISTANCE_INVALID);
    EXPECT_EQ(GeoDistance_Parse_Buffer(invalid, strlen(invalid)), GEO_DISTANCE_INVALID);
  }
  EXPECT_STREQ(GeoDistance_ToString(GEO_DISTANCE_INVALID), "<badunit>");
  EXPECT_STREQ(GeoDistance_ToString(static_cast<GeoDistance>(123)), "<badunit>");

  const char buffer[] = {'K', 'M', 'x'};
  EXPECT_EQ(GeoDistance_Parse_Buffer(buffer, 2), GEO_DISTANCE_KM);
  EXPECT_EQ(GeoDistance_Parse_Buffer(buffer, 3), GEO_DISTANCE_INVALID);
  EXPECT_EQ(GeoDistance_Parse_Buffer(buffer, 0), GEO_DISTANCE_INVALID);
  // The bounded parser still uses C-string semantics after copying.
  const char embeddedNul[16] = {'m', '\0', 'x'};
  EXPECT_EQ(GeoDistance_Parse_Buffer(embeddedNul, 15), GEO_DISTANCE_M);
  EXPECT_EQ(GeoDistance_Parse_Buffer(embeddedNul, 16), GEO_DISTANCE_INVALID);
}

TEST(GeoIndexTest, CreateAndSetField) {
  Filter filter(NewGeoFilter(12.5, -34.5, 6.25, nullptr, 0), GeoFilter_Free);
  EXPECT_DOUBLE_EQ(filter->lon, 12.5);
  EXPECT_DOUBLE_EQ(filter->lat, -34.5);
  EXPECT_DOUBLE_EQ(filter->radius, 6.25);
  EXPECT_EQ(filter->unitType, GEO_DISTANCE_KM);
  EXPECT_EQ(filter->fieldIndex, RS_INVALID_FIELD_INDEX);
  EXPECT_EQ(filter->numericFilters, nullptr);

  FieldSpec field = {};
  field.index = 7;
  GeoFilter_SetField(filter.get(), &field);
  EXPECT_EQ(filter->fieldIndex, 7);
  GeoFilter_SetField(filter.get(), nullptr);
  EXPECT_EQ(filter->fieldIndex, RS_INVALID_FIELD_INDEX);

  const char unit[] = {'F', 'T', 'x'};
  Filter explicitUnit(NewGeoFilter(0, 0, 1, unit, 2), GeoFilter_Free);
  EXPECT_EQ(explicitUnit->unitType, GEO_DISTANCE_FT);
  Filter invalidUnit(NewGeoFilter(0, 0, 1, "", 0), GeoFilter_Free);
  EXPECT_EQ(invalidUnit->unitType, GEO_DISTANCE_INVALID);
}

TEST(GeoIndexTest, ValidateBoundsAndErrorPrecedence) {
  struct Case {
    double lon;
    double lat;
    double radius;
    const char *unit;
    const char *error;
  };
  for (const auto &test : {
           Case{-180, -90, 1, "m", nullptr},
           Case{180, 90, 1, "m", nullptr},
           Case{0, 86, 1, "m", nullptr},
           Case{180.01, 0, 1, "m", "Invalid GeoFilter lat/lon"},
           Case{-180.01, 0, 1, "m", "Invalid GeoFilter lat/lon"},
           Case{0, 90.01, 1, "m", "Invalid GeoFilter lat/lon"},
           Case{0, -90.01, 1, "m", "Invalid GeoFilter lat/lon"},
           Case{INFINITY, 0, 1, "m", "Invalid GeoFilter lat/lon"},
           Case{0, -INFINITY, 1, "m", "Invalid GeoFilter lat/lon"},
           Case{0, 0, 0, "m", "Invalid GeoFilter radius"},
           Case{0, 0, -1, "m", "Invalid GeoFilter radius"},
           Case{0, 0, -INFINITY, "m", "Invalid GeoFilter radius"},
           Case{181, 91, -1, "bad", "Invalid GeoFilter unit"},
           Case{181, 91, -1, "m", "Invalid GeoFilter lat/lon"},
           // Validation is comparison-based, not a finite-number check.
           Case{NAN, NAN, NAN, "m", nullptr},
           Case{0, 0, INFINITY, "m", nullptr},
       }) {
    SCOPED_TRACE(test.error ? test.error : "valid");
    Filter filter(NewGeoFilter(test.lon, test.lat, test.radius, test.unit, strlen(test.unit)),
                  GeoFilter_Free);
    Error error;
    EXPECT_EQ(GeoFilter_Validate(filter.get(), &error.value), test.error ? 0 : 1);
    if (test.error) {
      EXPECT_EQ(QueryError_GetCode(&error.value), QUERY_ERROR_CODE_SYNTAX);
      EXPECT_EQ(std::string(QueryError_GetUserError(&error.value)),
                std::string("SEARCH_SYNTAX ") + test.error);
    } else {
      EXPECT_FALSE(QueryError_HasError(&error.value));
    }
  }
}

TEST(GeoIndexTest, LegacyParseConsumesOnlyItsArguments) {
  const char *args[] = {"location", "12.5", "-34.5", "6.25", "Mi", "NEXT"};
  ArgsCursor cursor;
  ArgsCursor_InitCString(&cursor, args, 6);
  LegacyFilter filter(static_cast<LegacyGeoFilter *>(rm_calloc(1, sizeof(LegacyGeoFilter))),
                      LegacyGeoFilter_Free);
  Error error;
  bool empty = false;
  ASSERT_EQ(GeoFilter_LegacyParse(filter.get(), &cursor, &empty, &error.value), REDISMODULE_OK);
  EXPECT_FALSE(empty);
  EXPECT_FALSE(QueryError_HasError(&error.value));
  EXPECT_EQ(cursor.offset, 5);
  EXPECT_STREQ(AC_GetStringNC(&cursor, nullptr), "NEXT");
  EXPECT_DOUBLE_EQ(filter->base.lon, 12.5);
  EXPECT_DOUBLE_EQ(filter->base.lat, -34.5);
  EXPECT_DOUBLE_EQ(filter->base.radius, 6.25);
  EXPECT_EQ(filter->base.unitType, GEO_DISTANCE_MI);
  EXPECT_EQ(filter->base.fieldIndex, RS_INVALID_FIELD_INDEX);
  EXPECT_EQ(filter->base.numericFilters, nullptr);
  // The field is a borrowed string; freeing the filter must not free the argument.
  EXPECT_EQ(HiddenString_GetUnsafe(filter->field, nullptr), args[0]);
}

TEST(GeoIndexTest, LegacyParseEmptyValuesAndDeferredValidation) {
  for (int emptyIndex : {1, 2, 3, -1}) {
    const char *args[] = {"g", "181", "91", "-1", "m"};
    if (emptyIndex >= 0) {
      args[emptyIndex] = "";
    }
    // User arguments use strict Redis conversions, unlike internal C-string cursors.
    RMCK::ArgvList redisArgs(nullptr, args, size_t{5});
    ArgsCursor cursor;
    ArgsCursor_InitRString(&cursor, redisArgs.data(), redisArgs.size());
    LegacyFilter filter(static_cast<LegacyGeoFilter *>(rm_calloc(1, sizeof(LegacyGeoFilter))),
                        LegacyGeoFilter_Free);
    Error error;
    // A flag set by a prior filter is not cleared by parsing a nonempty one.
    bool empty = emptyIndex == -1;
    ASSERT_EQ(GeoFilter_LegacyParse(filter.get(), &cursor, &empty, &error.value), REDISMODULE_OK);
    EXPECT_TRUE(empty);
    EXPECT_EQ(cursor.offset, 5);
    EXPECT_DOUBLE_EQ(filter->base.lon, emptyIndex == 1 ? 0 : 181);
    EXPECT_DOUBLE_EQ(filter->base.lat, emptyIndex == 2 ? 0 : 91);
    EXPECT_DOUBLE_EQ(filter->base.radius, emptyIndex == 3 ? 0 : -1);
    EXPECT_FALSE(QueryError_HasError(&error.value));
    EXPECT_EQ(GeoFilter_Validate(&filter->base, &error.value), 0);
  }
}

TEST(GeoIndexTest, LegacyParseErrorsAndCursorPosition) {
  for (int badIndex : {1, 2, 3, 4, -1}) {
    const char *args[] = {"g", "0", "0", "1", "km"};
    if (badIndex >= 0) {
      args[badIndex] = "bad";
    }
    ArgsCursor cursor;
    ArgsCursor_InitCString(&cursor, args, badIndex == -1 ? 4 : 5);
    LegacyFilter filter(static_cast<LegacyGeoFilter *>(rm_calloc(1, sizeof(LegacyGeoFilter))),
                        LegacyGeoFilter_Free);
    Error error;
    bool empty = false;
    EXPECT_EQ(GeoFilter_LegacyParse(filter.get(), &cursor, &empty, &error.value), REDISMODULE_ERR);
    EXPECT_FALSE(empty);
    EXPECT_EQ(filter->field, nullptr);
    EXPECT_EQ(filter->base.numericFilters, nullptr);
    EXPECT_EQ(filter->base.fieldIndex, RS_INVALID_FIELD_INDEX);
    EXPECT_EQ(QueryError_GetCode(&error.value), QUERY_ERROR_CODE_PARSE_ARGS);
    const char *details[] = {"", "for <lon>", "for <lat>", "for <radius>",
                             "Unknown distance unit bad"};
    EXPECT_NE(std::string(QueryError_GetUserError(&error.value))
                  .find(badIndex == -1 ? "GEOFILTER requires 5 arguments" : details[badIndex]),
              std::string::npos);
    EXPECT_EQ(cursor.offset, badIndex == -1 ? 0 : badIndex == 4 ? 5 : badIndex);
  }
}

TEST(GeoIndexTest, LegacyPartialOutputAndCursorGrammar) {
  for (bool redisStrings : {false, true}) {
    const char *args[] = {"PREFIX", "g", "12.5", "", "bad", "m"};
    RMCK::ArgvList redisArgs(nullptr, args, size_t{6});
    ArgsCursor cursor;
    if (redisStrings) {
      ArgsCursor_InitRString(&cursor, redisArgs.data(), redisArgs.size());
    } else {
      ArgsCursor_InitCString(&cursor, args, 6);
    }
    cursor.offset = 1;
    LegacyFilter filter(static_cast<LegacyGeoFilter *>(rm_malloc(sizeof(LegacyGeoFilter))),
                        LegacyGeoFilter_Free);
    Error error;
    bool empty = false;
    EXPECT_EQ(GeoFilter_LegacyParse(filter.get(), &cursor, &empty, &error.value), REDISMODULE_ERR);
    EXPECT_EQ(cursor.offset, 4);
    EXPECT_EQ(empty, redisStrings);
    EXPECT_DOUBLE_EQ(filter->base.lon, 12.5);
    EXPECT_DOUBLE_EQ(filter->base.lat, 0);
    EXPECT_DOUBLE_EQ(filter->base.radius, 0);
    EXPECT_EQ(filter->base.unitType, GEO_DISTANCE_KM);
    EXPECT_EQ(filter->field, nullptr);
  }
}

TEST(GeoIndexTest, AggregateReferenceAndTrioViews) {
  using Value = std::unique_ptr<RSValue, decltype(&RSValue_DecrRef)>;
  Value point(RSValue_NewString(rm_strdup("0,0"), 3), RSValue_DecrRef);
  Value reference(RSValue_NewReference(point.get()), RSValue_DecrRef);
  Value trio(RSValue_NewTrio(RSValue_NewNumber(0), RSValue_NewNumber(1), RSValue_NewNumber(2)),
             RSValue_DecrRef);
  Value latitude(RSValue_NewNumber(0), RSValue_DecrRef);
  Error error;
  RSValue *fields[] = {reference.get(), point.get()};
  EXPECT_DOUBLE_EQ(GeoDistance_Evaluate(fields, 2, &error.value), 0);
  RSValue *coordinates[] = {trio.get(), latitude.get(), reference.get()};
  EXPECT_DOUBLE_EQ(GeoDistance_Evaluate(coordinates, 3, &error.value), 0);
  RSValue *trioField[] = {trio.get(), point.get()};
  EXPECT_TRUE(std::isnan(GeoDistance_Evaluate(trioField, 2, &error.value)));
  EXPECT_FALSE(QueryError_HasError(&error.value));
}

TEST(GeoIndexTest, RadiusUnitsAndDistanceOutput) {
  const double encoded = encodeGeo(0.01, 0);
  ASSERT_NE(encoded, INVALID_GEOHASH);
  double decoded[2];
  decodeGeo(encoded, decoded);
  const double expected = geohashGetDistance(0, 0, decoded[0], decoded[1]);
  struct Unit {
    const char *name;
    double meters;
  };
  for (const auto &unit :
       {Unit{"m", 1}, Unit{"km", 1000}, Unit{"ft", 0.3048}, Unit{"mi", 1609.34}}) {
    SCOPED_TRACE(unit.name);
    Filter filter(NewGeoFilter(0, 0, (expected + 1) / unit.meters, unit.name, strlen(unit.name)),
                  GeoFilter_Free);
    double distance = -1;
    EXPECT_EQ(isWithinRadius(filter.get(), encoded, &distance), 1);
    EXPECT_DOUBLE_EQ(distance, expected);
    EXPECT_EQ(isWithinRadius(filter.get(), encoded, nullptr), 1);
    filter->radius = (expected - 1) / unit.meters;
    distance = -1;
    EXPECT_EQ(isWithinRadius(filter.get(), encoded, &distance), 0);
    EXPECT_DOUBLE_EQ(distance, expected);
  }
  Filter boundary(NewGeoFilter(0, 0, expected, "m", 1), GeoFilter_Free);
  EXPECT_EQ(isWithinRadius(boundary.get(), encoded, nullptr), 1);
  boundary->radius = std::nextafter(expected, 0);
  EXPECT_EQ(isWithinRadius(boundary.get(), encoded, nullptr), 0);
}
