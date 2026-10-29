/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#include "gtest/gtest.h"
#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdio>
#include <limits>
#include <vector>

#include "search_ctx.h"

extern "C" {
#include "geo_index.h"
#include "query_error_ffi.h"
#include "rmalloc.h"
}

#include "indexes.h"
#include "query_eval_ffi.h"
#include "query_request.h"
#include "query_test_utils.h"
#include "redismock/redismock.h"
#include "util/references.h"

TEST(GeoIndex, UnitsAndConstruction) {
  const char *names[] = {"km", "m", "ft", "mi"};
  const char *mixed[] = {"kM", "M", "Ft", "mI"};
  for (int i = 0; i < 4; ++i) {
    EXPECT_EQ(GeoDistance_Parse(mixed[i]), i);
    EXPECT_STREQ(GeoDistance_ToString(static_cast<GeoDistance>(i)), names[i]);
  }
  for (const char *bad : {"", " km", "km ", "meters", "\xff", "K"}) {
    EXPECT_EQ(GeoDistance_Parse(bad), GEO_DISTANCE_INVALID);
  }
  EXPECT_STREQ(GeoDistance_ToString(GEO_DISTANCE_INVALID), "<badunit>");
  EXPECT_STREQ(GeoDistance_ToString(static_cast<GeoDistance>(-2)), "<badunit>");
  const char binary[] = {'k', 'M', '\0', 'x'};
  EXPECT_EQ(GeoDistance_Parse_Buffer(binary, sizeof(binary)), GEO_DISTANCE_KM);
  EXPECT_EQ(GeoDistance_Parse_Buffer(binary, 1), GEO_DISTANCE_INVALID);
  EXPECT_EQ(GeoDistance_Parse_Buffer(binary, 2), GEO_DISTANCE_KM);
  EXPECT_EQ(GeoDistance_Parse_Buffer(binary, 0), GEO_DISTANCE_INVALID);
  const char padded[16] = {'m', '\0'};
  EXPECT_EQ(GeoDistance_Parse_Buffer(padded, 15), GEO_DISTANCE_M);
  EXPECT_EQ(GeoDistance_Parse_Buffer(padded, 16), GEO_DISTANCE_INVALID);

  auto *gf = NewGeoFilter(12.5, -42.25, -3.0, nullptr, 99);
  EXPECT_EQ(gf->fieldIndex, RS_INVALID_FIELD_INDEX);
  EXPECT_DOUBLE_EQ(gf->lon, 12.5);
  EXPECT_DOUBLE_EQ(gf->lat, -42.25);
  EXPECT_DOUBLE_EQ(gf->radius, -3.0);
  EXPECT_EQ(gf->unitType, GEO_DISTANCE_KM);
  EXPECT_EQ(gf->numericFilters, nullptr);
  FieldSpec fs = {};
  fs.index = 7;
  GeoFilter_SetField(gf, &fs);
  fs.index = 8;
  EXPECT_EQ(gf->fieldIndex, 7);
  GeoFilter_SetField(gf, nullptr);
  EXPECT_EQ(gf->fieldIndex, RS_INVALID_FIELD_INDEX);
  GeoFilter_Free(gf);
  gf = NewGeoFilter(0, 0, 1, "", 0);
  EXPECT_EQ(gf->unitType, GEO_DISTANCE_INVALID);
  GeoFilter_Free(gf);
}

TEST(GeoIndex, ValidationOrderAndFloatingPoint) {
  const double nan = std::numeric_limits<double>::quiet_NaN();
  const double inf = std::numeric_limits<double>::infinity();
  struct Case {
    double lon, lat, radius;
    const char *unit;
    const char *error;
  };
  const Case cases[] = {
      {181, 91, -1, "bad", "Invalid GeoFilter unit"},
      {181, 91, -1, "m", "Invalid GeoFilter lat/lon"},
      {0, 0, 0, "m", "Invalid GeoFilter radius"},
      {0, 0, -0.0, "m", "Invalid GeoFilter radius"},
      {0, 0, -inf, "m", "Invalid GeoFilter radius"},
      {-180, -90, 1, "m", nullptr},
      {180, 90, 1, "m", nullptr},
      {std::nextafter(-180.0, -inf), 0, 1, "m", "Invalid GeoFilter lat/lon"},
      {0, std::nextafter(90.0, inf), 1, "m", "Invalid GeoFilter lat/lon"},
      {inf, 0, 1, "m", "Invalid GeoFilter lat/lon"},
      {0, -inf, 1, "m", "Invalid GeoFilter lat/lon"},
      {nan, nan, nan, "m", nullptr},
      {0, 0, inf, "m", nullptr},
  };
  for (const auto &test : cases) {
    auto *gf = NewGeoFilter(test.lon, test.lat, test.radius, test.unit, strlen(test.unit));
    auto status = QueryError_Default();
    EXPECT_EQ(GeoFilter_Validate(gf, &status), test.error == nullptr);
    if (test.error) {
      EXPECT_EQ(QueryError_GetCode(&status), QUERY_ERROR_CODE_SYNTAX);
      EXPECT_STREQ(QueryError_GetDisplayableError(&status, true), test.error);
    } else {
      EXPECT_EQ(QueryError_GetCode(&status), QUERY_ERROR_CODE_OK);
    }
    QueryError_ClearError(&status);
    GeoFilter_Free(gf);
  }
}

TEST(GeoIndex, LegacyCursorAndBorrowedField) {
  for (bool redisStrings : {false, true}) {
    for (const char *lon : {"12.5", ""}) {
      const char field[] = "location\0ignored";
      const char *args[] = {field, lon, "-42.25", "3.5", "MI", "tail"};
      RedisModuleString *strings[6];
      ArgsCursor ac;
      if (redisStrings) {
        for (int i = 0; i < 6; ++i) {
          strings[i] =
              RedisModule_CreateString(nullptr, args[i], i == 0 ? sizeof(field) : strlen(args[i]));
        }
        ArgsCursor_InitRString(&ac, strings, 6);
      } else {
        ArgsCursor_InitCString(&ac, args, 6);
      }
      auto *gf = static_cast<LegacyGeoFilter *>(rm_malloc(sizeof(LegacyGeoFilter)));
      auto status = QueryError_Default();
      bool empty = false;
      ASSERT_EQ(GeoFilter_LegacyParse(gf, &ac, &empty, &status), REDISMODULE_OK);
      EXPECT_EQ(ac.offset, 5);
      EXPECT_EQ(gf->base.fieldIndex, RS_INVALID_FIELD_INDEX);
      EXPECT_DOUBLE_EQ(gf->base.lon, *lon ? 12.5 : 0.0);
      EXPECT_DOUBLE_EQ(gf->base.lat, -42.25);
      EXPECT_DOUBLE_EQ(gf->base.radius, 3.5);
      EXPECT_EQ(gf->base.unitType, GEO_DISTANCE_MI);
      EXPECT_EQ(empty, redisStrings && !*lon);
      size_t len;
      const char *borrowed = HiddenString_GetUnsafe(gf->field, &len);
      const char *original = redisStrings ? RedisModule_StringPtrLen(strings[0], nullptr) : field;
      EXPECT_EQ(borrowed, original);
      EXPECT_STREQ(borrowed, "location");
      EXPECT_EQ(len, 8);
      LegacyGeoFilter_Free(gf);
      EXPECT_STREQ(original, "location");
      if (redisStrings) {
        for (auto *s : strings) RedisModule_FreeString(nullptr, s);
      }
      QueryError_ClearError(&status);
    }
  }
}

TEST(GeoIndex, LegacyErrorsAndPartialConsumption) {
  for (int bad = 0; bad < 5; ++bad) {
    const char *args[] = {"location", "12", "-42", "3", "m"};
    const char *errors[] = {"GEOFILTER requires 5 arguments", "Bad arguments", "Bad arguments",
                            "Bad arguments", "Unknown distance unit"};
    const char *privateErrors[] = {
        "SEARCH_PARSE_ARGS GEOFILTER requires 5 arguments",
        "SEARCH_PARSE_ARGS Bad arguments for <lon>: Could not convert argument to expected type",
        "SEARCH_PARSE_ARGS Bad arguments for <lat>: Could not convert argument to expected type",
        "SEARCH_PARSE_ARGS Bad arguments for <radius>: Could not convert argument to expected type",
        "SEARCH_PARSE_ARGS Unknown distance unit bad"};
    if (bad) args[bad] = "bad";
    ArgsCursor ac;
    ArgsCursor_InitCString(&ac, args, bad ? 5 : 4);
    auto status = QueryError_Default();
    bool empty = true;
    auto *gf = static_cast<LegacyGeoFilter *>(rm_malloc(sizeof(LegacyGeoFilter)));
    EXPECT_EQ(GeoFilter_LegacyParse(gf, &ac, &empty, &status), REDISMODULE_ERR);
    EXPECT_EQ(ac.offset, bad == 4 ? 5 : bad);
    EXPECT_EQ(gf->base.fieldIndex, RS_INVALID_FIELD_INDEX);
    EXPECT_EQ(gf->field, nullptr);
    EXPECT_EQ(gf->base.numericFilters, nullptr);
    EXPECT_TRUE(empty);
    EXPECT_EQ(QueryError_GetCode(&status), QUERY_ERROR_CODE_PARSE_ARGS);
    EXPECT_STREQ(QueryError_GetDisplayableError(&status, false), privateErrors[bad]);
    EXPECT_STREQ(QueryError_GetDisplayableError(&status, true), errors[bad]);
    LegacyGeoFilter_Free(gf);
    QueryError_ClearError(&status);
  }
}

TEST(GeoIndex, RadiusUnitsAndInclusiveBoundary) {
  const char *units[] = {"m", "km", "ft", "mi"};
  const double factors[] = {1, 1000, 0.3048, 1609.34};
  const double score = encodeGeo(12.6, -42.2);
  double point[2];
  decodeGeo(score, point);
  const double expected = geohashGetDistance(12.5, -42.25, point[0], point[1]);
  for (int i = 0; i < 4; ++i) {
    auto *gf = NewGeoFilter(12.5, -42.25, (expected + 1) / factors[i], units[i], strlen(units[i]));
    double distance = -1;
    EXPECT_TRUE(isWithinRadius(gf, score, &distance));
    EXPECT_DOUBLE_EQ(distance, expected);
    gf->radius = (expected - 1) / factors[i];
    EXPECT_FALSE(isWithinRadius(gf, score, &distance));
    EXPECT_DOUBLE_EQ(distance, expected);
    EXPECT_FALSE(isWithinRadius(gf, score, nullptr));
    GeoFilter_Free(gf);
  }
  auto *gf = NewGeoFilter(12.5, -42.25, expected, "m", 1);
  EXPECT_TRUE(isWithinRadius(gf, score, nullptr));
  gf->radius = std::nextafter(expected, 0.0);
  EXPECT_FALSE(isWithinRadius(gf, score, nullptr));
  double distance = -1;
  gf->radius = std::numeric_limits<double>::quiet_NaN();
  EXPECT_FALSE(isWithinRadius(gf, score, &distance));
  EXPECT_DOUBLE_EQ(distance, expected);
  gf->radius = std::numeric_limits<double>::infinity();
  EXPECT_TRUE(isWithinRadius(gf, score, &distance));
  EXPECT_DOUBLE_EQ(distance, expected);
  GeoFilter_Free(gf);
}

TEST(GeoIndex, PopulatedFilterCleanup) {
  const char *schema[] = {"SCHEMA", "location", "GEO"};
  auto status = QueryError_Default();
  StrongRef ref = IndexSpec_ParseC(nullptr, "geo-cleanup", schema, 3, &status);
  auto *spec = static_cast<IndexSpec *>(StrongRef_Get(ref));
  ASSERT_NE(spec, nullptr);
  RedisSearchCtx ctx = SEARCH_CTX_STATIC(nullptr, spec);
  QueryRequestTimeout timeout = {};
  ctx.timeout = &timeout;
  for (bool legacy : {false, true}) {
    QASTCXX ast(ctx);
    ASSERT_TRUE(ast.parse("@location:[12.5 -42.25 100 km]", 2));
    GeoFilter_Free(ast.root->gn.gf);
    ast.root->gn.gf = nullptr;
    LegacyGeoFilter *lgf = nullptr;
    GeoFilter *gf;
    char field[] = "location";
    if (legacy) {
      lgf = static_cast<LegacyGeoFilter *>(rm_malloc(sizeof(LegacyGeoFilter)));
      const char *args[] = {field, "12.5", "-42.25", "100", "km"};
      ArgsCursor ac;
      ArgsCursor_InitCString(&ac, args, 5);
      bool empty = false;
      ASSERT_EQ(GeoFilter_LegacyParse(lgf, &ac, &empty, &status), REDISMODULE_OK);
      gf = &lgf->base;
    } else {
      gf = NewGeoFilter(12.5, -42.25, 100, "km", 2);
    }
    GeoFilter_SetField(gf, &spec->fields[0]);
    ast.root->gn.gf = gf;
    SearchOptionsCXX opts;
    auto *tree = QAST_Iterate(&ast, &opts, &ctx, 0, &status);
    QueryIteratorTree_Free(tree);
    ASSERT_NE(gf->numericFilters, nullptr);
    std::vector<void *> owned = {gf, gf->numericFilters};
    for (size_t i = 0; i < GEO_RANGE_COUNT; ++i) {
      if (gf->numericFilters[i]) owned.push_back(gf->numericFilters[i]);
    }
    ASSERT_GT(owned.size(), 2);
    if (legacy) owned.push_back(lgf->field);
    static thread_local std::vector<void *> freed;
    freed.clear();
    RMCK_SetFreeObserver([](void *p) { freed.push_back(p); });
    if (legacy)
      LegacyGeoFilter_Free(lgf);
    else
      GeoFilter_Free(gf);
    RMCK_SetFreeObserver(nullptr);
    ast.root->gn.gf = nullptr;
    for (void *p : owned) EXPECT_EQ(std::count(freed.begin(), freed.end(), p), 1);
    EXPECT_STREQ(field, "location");
  }
  Indexes_RemoveSpecFromGlobals(ref, false);
  QueryError_ClearError(&status);
}

// Medians of long batches reduce timer and scheduler noise for nanosecond-scale operations.
template <typename F>
static double measureGeo(F run) {
  constexpr int iterations = 200000;
  std::vector<double> samples;
  for (int sample = 0; sample < 15; ++sample) {
    const auto start = std::chrono::steady_clock::now();
    for (int i = 0; i < iterations; ++i) run(i);
    const auto elapsed = std::chrono::steady_clock::now() - start;
    samples.push_back(std::chrono::duration<double, std::nano>(elapsed).count() / iterations);
  }
  std::sort(samples.begin(), samples.end());
  return samples[samples.size() / 2];
}

TEST(GeoIndex, Benchmark) {
  auto *gf = NewGeoFilter(12.5, -42.25, 100, "km", 2);
  auto status = QueryError_Default();
  const char *units[] = {"km", "M", "ft", "MI", "bad"};
  volatile int result = 0;
  const double parse = measureGeo(
      [&](int i) { result = GeoDistance_Parse_Buffer(units[i % 5], strlen(units[i % 5])); });
  const double validate = measureGeo([&](int i) {
    gf->lon = i % 180;
    result = GeoFilter_Validate(gf, &status);
  });
  gf->lon = 12.5;
  const double score = encodeGeo(12.6, -42.2);
  double distance = 0;
  const double radius = measureGeo([&](int) { result = isWithinRadius(gf, score, &distance); });
  const double lifecycle =
      measureGeo([&](int i) { GeoFilter_Free(NewGeoFilter(i % 180, -42.25, 3, "km", 2)); });
  std::printf(
      "GEO_BENCH "
      "{\"implementation\":\"c\",\"parse\":%.6f,\"validate\":%.6f,\"radius\":%.6f,\"lifecycle\":%."
      "6f}\n",
      parse, validate, radius, lifecycle);
  EXPECT_GE(result, 0);
  EXPECT_GT(distance, 0);
  GeoFilter_Free(gf);
  QueryError_ClearError(&status);
}
