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

// One contract suite can exercise both implementations while the bridge coexists with C.
struct GeoApi {
  const char *name;
  decltype(&NewGeoFilter) create;
  decltype(&GeoFilter_Free) destroy;
  decltype(&GeoFilter_SetField) setField;
  decltype(&GeoFilter_Validate) validate;
  decltype(&GeoDistance_Parse) parse;
  decltype(&GeoDistance_Parse_Buffer) parseBuffer;
  decltype(&GeoDistance_ToString) toString;
  decltype(&GeoFilter_LegacyParse) legacyParse;
  decltype(&LegacyGeoFilter_Free) legacyFree;
  decltype(&isWithinRadius) within;
};

static const GeoApi cApi = {"c",
                            NewGeoFilter,
                            GeoFilter_Free,
                            GeoFilter_SetField,
                            GeoFilter_Validate,
                            GeoDistance_Parse,
                            GeoDistance_Parse_Buffer,
                            GeoDistance_ToString,
                            GeoFilter_LegacyParse,
                            LegacyGeoFilter_Free,
                            isWithinRadius};

static const GeoApi rustApi = {"rust",
                               Rust_NewGeoFilter,
                               Rust_GeoFilter_Free,
                               Rust_GeoFilter_SetField,
                               Rust_GeoFilter_Validate,
                               Rust_GeoDistance_Parse,
                               Rust_GeoDistance_Parse_Buffer,
                               Rust_GeoDistance_ToString,
                               Rust_GeoFilter_LegacyParse,
                               Rust_LegacyGeoFilter_Free,
                               Rust_isWithinRadius};

class GeoFilterTest : public testing::TestWithParam<GeoApi> {};

TEST_P(GeoFilterTest, UnitsAndConstruction) {
  const auto &api = GetParam();
  const char *names[] = {"km", "m", "ft", "mi"};
  const char *mixed[] = {"kM", "M", "Ft", "mI"};
  for (int i = 0; i < 4; ++i) {
    EXPECT_EQ(api.parse(mixed[i]), i);
    EXPECT_STREQ(api.toString(static_cast<GeoDistance>(i)), names[i]);
  }
  for (const char *bad : {"", " km", "km ", "meters", "\xff", "K"}) {
    EXPECT_EQ(api.parse(bad), GEO_DISTANCE_INVALID);
  }
  EXPECT_STREQ(api.toString(GEO_DISTANCE_INVALID), "<badunit>");
  EXPECT_STREQ(api.toString(static_cast<GeoDistance>(-2)), "<badunit>");
  const char binary[] = {'k', 'M', '\0', 'x'};
  EXPECT_EQ(api.parseBuffer(binary, sizeof(binary)), GEO_DISTANCE_KM);
  EXPECT_EQ(api.parseBuffer(binary, 1), GEO_DISTANCE_INVALID);
  EXPECT_EQ(api.parseBuffer(binary, 2), GEO_DISTANCE_KM);
  EXPECT_EQ(api.parseBuffer(binary, 0), GEO_DISTANCE_INVALID);
  const char padded[16] = {'m', '\0'};
  EXPECT_EQ(api.parseBuffer(padded, 15), GEO_DISTANCE_M);
  EXPECT_EQ(api.parseBuffer(padded, 16), GEO_DISTANCE_INVALID);
  for (size_t len : {size_t{16}, std::numeric_limits<size_t>::max()}) {
    EXPECT_EQ(api.parseBuffer("m", len), GEO_DISTANCE_INVALID);
    EXPECT_EQ(api.parseBuffer(nullptr, len), GEO_DISTANCE_INVALID);
    auto *oversized = api.create(12.5, -42.25, 3, "m", len);
    EXPECT_EQ(oversized->unitType, GEO_DISTANCE_INVALID);
    api.destroy(oversized);
  }

  auto *gf = api.create(12.5, -42.25, -3.0, nullptr, std::numeric_limits<size_t>::max());
  EXPECT_EQ(gf->fieldIndex, RS_INVALID_FIELD_INDEX);
  EXPECT_DOUBLE_EQ(gf->lon, 12.5);
  EXPECT_DOUBLE_EQ(gf->lat, -42.25);
  EXPECT_DOUBLE_EQ(gf->radius, -3.0);
  EXPECT_EQ(gf->unitType, GEO_DISTANCE_KM);
  EXPECT_EQ(gf->numericFilters, nullptr);
  FieldSpec fs = {};
  fs.index = 7;
  api.setField(gf, &fs);
  fs.index = 8;
  EXPECT_EQ(gf->fieldIndex, 7);
  api.setField(gf, nullptr);
  EXPECT_EQ(gf->fieldIndex, RS_INVALID_FIELD_INDEX);
  api.destroy(gf);
  gf = api.create(0, 0, 1, "", 0);
  EXPECT_EQ(gf->unitType, GEO_DISTANCE_INVALID);
  api.destroy(gf);
}

TEST_P(GeoFilterTest, ValidationOrderAndFloatingPoint) {
  const auto &api = GetParam();
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
    auto *gf = api.create(test.lon, test.lat, test.radius, test.unit, strlen(test.unit));
    auto status = QueryError_Default();
    EXPECT_EQ(api.validate(gf, &status), test.error == nullptr);
    if (test.error) {
      EXPECT_EQ(QueryError_GetCode(&status), QUERY_ERROR_CODE_SYNTAX);
      EXPECT_STREQ(QueryError_GetDisplayableError(&status, true), test.error);
    } else {
      EXPECT_EQ(QueryError_GetCode(&status), QUERY_ERROR_CODE_OK);
    }
    QueryError_ClearError(&status);
    api.destroy(gf);
  }
  auto *gf = api.create(12.5, -42.25, 10000, "m", 1);
  auto status = QueryError_Default();
  const double score = encodeGeo(12.6, -42.2);
  for (int unit : {-1, -2}) {
    gf->unitType = static_cast<GeoDistance>(unit);
    gf->radius = 10000;
    EXPECT_EQ(api.validate(gf, &status), unit != -1);
    EXPECT_EQ(QueryError_GetCode(&status),
              unit == -1 ? QUERY_ERROR_CODE_SYNTAX : QUERY_ERROR_CODE_OK);
#ifdef ENABLE_ASSERT
    if (api.within == isWithinRadius) {
      // The Redis mock turns the debug-only C assertion into an exception.
      EXPECT_ANY_THROW(api.within(gf, score, nullptr));
    } else
#endif
    {
      double distance = 0;
      EXPECT_FALSE(api.within(gf, score, &distance));
      EXPECT_NEAR(distance, 9938.194472756348, 1e-7);
      gf->radius = -10000;
      EXPECT_TRUE(api.within(gf, score, &distance));
      EXPECT_NEAR(distance, 9938.194472756348, 1e-7);
    }
    QueryError_ClearError(&status);
  }
  api.destroy(gf);
}

TEST_P(GeoFilterTest, LegacyCursorAndBorrowedField) {
  const auto &api = GetParam();
  for (bool redisStrings : {false, true}) {
    for (const char *lon : {"12.5", ""}) {
      const char field[] = "location\0ignored";
      const char binaryUnit[] = "mI\0payload longer than the unit buffer";
      const char *args[] = {field, lon, "-42.25", "3.5", binaryUnit, "tail"};
      RedisModuleString *strings[6];
      ArgsCursor ac;
      if (redisStrings) {
        for (int i = 0; i < 6; ++i) {
          size_t len = i == 0 ? sizeof(field) : i == 4 ? sizeof(binaryUnit) - 1 : strlen(args[i]);
          strings[i] = RedisModule_CreateString(nullptr, args[i], len);
        }
        ArgsCursor_InitRString(&ac, strings, 6);
      } else {
        ArgsCursor_InitCString(&ac, args, 6);
      }
      auto *gf = static_cast<LegacyGeoFilter *>(rm_malloc(sizeof(LegacyGeoFilter)));
      auto status = QueryError_Default();
      bool empty = false;
      ASSERT_EQ(api.legacyParse(gf, &ac, &empty, &status), REDISMODULE_OK);
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
      api.legacyFree(gf);
      EXPECT_STREQ(original, "location");
      if (redisStrings) {
        for (auto *s : strings) RedisModule_FreeString(nullptr, s);
      }
      QueryError_ClearError(&status);
    }
  }
}

TEST_P(GeoFilterTest, LegacyErrorsAndPartialConsumption) {
  const auto &api = GetParam();
  for (int scenario = 0; scenario < 7; ++scenario) {
    const int bad = std::min(scenario, 4);
    const bool existingError = scenario == 6;
    const char *args[] = {"location", "12", "-42", "3", "m"};
    const char *errors[] = {
        "GEOFILTER requires 5 arguments", "Bad arguments",         "Bad arguments", "Bad arguments",
        "Unknown distance unit",          "Unknown distance unit", "earlier error"};
    const char *privateErrors[] = {
        "SEARCH_PARSE_ARGS GEOFILTER requires 5 arguments",
        "SEARCH_PARSE_ARGS Bad arguments for <lon>: Could not convert argument to expected type",
        "SEARCH_PARSE_ARGS Bad arguments for <lat>: Could not convert argument to expected type",
        "SEARCH_PARSE_ARGS Bad arguments for <radius>: Could not convert argument to expected type",
        "SEARCH_PARSE_ARGS Unknown distance unit bad",
        "SEARCH_PARSE_ARGS Unknown distance unit \xff",
        "SEARCH_SYNTAX earlier error"};
    if (bad) args[bad] = scenario < 5 ? "bad" : "\xff\0ignored";
    ArgsCursor ac;
    ArgsCursor_InitCString(&ac, args, bad ? 5 : 4);
    auto status = QueryError_Default();
    if (existingError) QueryError_SetError(&status, QUERY_ERROR_CODE_SYNTAX, "earlier error");
    bool empty = true;
    auto *gf = static_cast<LegacyGeoFilter *>(rm_malloc(sizeof(LegacyGeoFilter)));
    EXPECT_EQ(api.legacyParse(gf, &ac, &empty, &status), REDISMODULE_ERR);
    EXPECT_EQ(ac.offset, bad == 4 ? 5 : bad);
    EXPECT_EQ(gf->base.fieldIndex, RS_INVALID_FIELD_INDEX);
    EXPECT_EQ(gf->field, nullptr);
    EXPECT_EQ(gf->base.numericFilters, nullptr);
    EXPECT_TRUE(empty);
    EXPECT_EQ(QueryError_GetCode(&status),
              existingError ? QUERY_ERROR_CODE_SYNTAX : QUERY_ERROR_CODE_PARSE_ARGS);
    EXPECT_STREQ(QueryError_GetDisplayableError(&status, false), privateErrors[scenario]);
    EXPECT_STREQ(QueryError_GetDisplayableError(&status, true), errors[scenario]);
    api.legacyFree(gf);
    QueryError_ClearError(&status);
  }
}

TEST_P(GeoFilterTest, RadiusUnitsAndInclusiveBoundary) {
  const auto &api = GetParam();
  const char *units[] = {"m", "km", "ft", "mi"};
  const double factors[] = {1, 1000, 0.3048, 1609.34};
  const double score = encodeGeo(12.6, -42.2);
  double point[2];
  decodeGeo(score, point);
  const double expected = geohashGetDistance(12.5, -42.25, point[0], point[1]);
  for (int i = 0; i < 4; ++i) {
    auto *gf = api.create(12.5, -42.25, (expected + 1) / factors[i], units[i], strlen(units[i]));
    double distance;
    // Only the new bridge is under test for accepting uninitialized output storage.
    if (api.within == isWithinRadius) distance = 0;
    EXPECT_TRUE(api.within(gf, score, &distance));
    EXPECT_DOUBLE_EQ(distance, expected);
    gf->radius = (expected - 1) / factors[i];
    EXPECT_FALSE(api.within(gf, score, &distance));
    EXPECT_DOUBLE_EQ(distance, expected);
    EXPECT_FALSE(api.within(gf, score, nullptr));
    gf->radius = std::numeric_limits<double>::max();
    auto status = QueryError_Default();
    EXPECT_TRUE(api.validate(gf, &status));
    EXPECT_TRUE(api.within(gf, score, &distance));
    EXPECT_DOUBLE_EQ(distance, expected);
    QueryError_ClearError(&status);
    api.destroy(gf);
  }
  auto *gf = api.create(12.5, -42.25, expected, "m", 1);
  EXPECT_TRUE(api.within(gf, score, nullptr));
  gf->radius = std::nextafter(expected, 0.0);
  EXPECT_FALSE(api.within(gf, score, nullptr));
  double distance = -1;
  gf->radius = std::numeric_limits<double>::quiet_NaN();
  EXPECT_FALSE(api.within(gf, score, &distance));
  EXPECT_DOUBLE_EQ(distance, expected);
  gf->radius = std::numeric_limits<double>::infinity();
  EXPECT_TRUE(api.within(gf, score, &distance));
  EXPECT_DOUBLE_EQ(distance, expected);
  api.destroy(gf);
}

TEST_P(GeoFilterTest, PopulatedFilterCleanup) {
  const auto &api = GetParam();
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
    api.destroy(ast.root->gn.gf);
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
      ASSERT_EQ(api.legacyParse(lgf, &ac, &empty, &status), REDISMODULE_OK);
      gf = &lgf->base;
    } else {
      gf = api.create(12.5, -42.25, 100, "km", 2);
    }
    api.setField(gf, &spec->fields[0]);
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
      api.legacyFree(lgf);
    else
      api.destroy(gf);
    RMCK_SetFreeObserver(nullptr);
    ast.root->gn.gf = nullptr;
    for (void *p : owned) EXPECT_EQ(std::count(freed.begin(), freed.end(), p), 1);
    EXPECT_STREQ(field, "location");
  }
  Indexes_RemoveSpecFromGlobals(ref, false);
  QueryError_ClearError(&status);
}

INSTANTIATE_TEST_SUITE_P(GeoIndex, GeoFilterTest, testing::Values(cApi, rustApi));

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

TEST_P(GeoFilterTest, Benchmark) {
  const auto &api = GetParam();
  auto *gf = api.create(12.5, -42.25, 100, "km", 2);
  auto status = QueryError_Default();
  const char *units[] = {"km", "M", "ft", "MI", "bad"};
  volatile int result = 0;
  const double parse =
      measureGeo([&](int i) { result = api.parseBuffer(units[i % 5], strlen(units[i % 5])); });
  const double validate = measureGeo([&](int i) {
    gf->lon = i % 180;
    result = api.validate(gf, &status);
  });
  gf->lon = 12.5;
  const double score = encodeGeo(12.6, -42.2);
  double distance = 0;
  const double radius = measureGeo([&](int) { result = api.within(gf, score, &distance); });
  const double lifecycle =
      measureGeo([&](int i) { api.destroy(api.create(i % 180, -42.25, 3, "km", 2)); });
  std::printf(
      "GEO_BENCH "
      "{\"implementation\":\"%s\",\"parse\":%.6f,\"validate\":%.6f,\"radius\":%.6f,\"lifecycle\":%."
      "6f}\n",
      api.name, parse, validate, radius, lifecycle);
  EXPECT_GE(result, 0);
  EXPECT_GT(distance, 0);
  api.destroy(gf);
  QueryError_ClearError(&status);
}
