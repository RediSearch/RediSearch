/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#include "gtest/gtest.h"
#include "redismock/util.h"
#include "rmutil/args.h"
#include "spec.h"

extern "C" {
#include "geo_index.h"
}
#include "query_error_ffi.h"
#include "rmalloc.h"
#include "aggregate/expr/expression.h"
#include "aggregate/functions/function.h"

#include <cmath>
#include <cstring>
#include <limits>
#include <memory>
#include <type_traits>

namespace {
using FilterOwner = std::unique_ptr<GeoFilter, decltype(&GeoFilter_Free)>;
using LegacyOwner = std::unique_ptr<LegacyGeoFilter, decltype(&LegacyGeoFilter_Free)>;

struct ErrorOwner {
  QueryError value = QueryError_Default();
  ~ErrorOwner() {
    QueryError_ClearError(&value);
  }
};
}  // namespace

TEST(GeoContract, UnitsRespectCaseLengthsAndFirstNul) {
  const struct {
    const char* text;
    GeoDistance unit;
    const char* canonical;
  } cases[] = {{"Km", GEO_DISTANCE_KM, "km"},
               {"M", GEO_DISTANCE_M, "m"},
               {"FT", GEO_DISTANCE_FT, "ft"},
               {"mI", GEO_DISTANCE_MI, "mi"},
               {"", GEO_DISTANCE_INVALID, "<badunit>"},
               {"meters", GEO_DISTANCE_INVALID, "<badunit>"},
               {" km", GEO_DISTANCE_INVALID, "<badunit>"},
               {"km ", GEO_DISTANCE_INVALID, "<badunit>"}};
  for (const auto& item : cases) {
    SCOPED_TRACE(item.text);
    EXPECT_EQ(GeoDistance_Parse(item.text), item.unit);
    EXPECT_EQ(GeoDistance_Parse_Buffer(item.text, std::strlen(item.text)), item.unit);
    EXPECT_STREQ(GeoDistance_ToString(item.unit), item.canonical);
  }
  const char embedded[] = {'k', 'm', '\0', 'j', 'u', 'n', 'k'};
  for (size_t len : {2u, 3u, 7u}) {
    EXPECT_EQ(GeoDistance_Parse_Buffer(embedded, len), GEO_DISTANCE_KM);
  }
  const char suffix[] = {'k', 'm', 'j', 'u', 'n', 'k'};
  EXPECT_EQ(GeoDistance_Parse_Buffer(suffix, 2), GEO_DISTANCE_KM);
  EXPECT_EQ(GeoDistance_Parse_Buffer(suffix, sizeof(suffix)), GEO_DISTANCE_INVALID);
  char boundary[16] = {'m', '\0'};
  EXPECT_EQ(GeoDistance_Parse_Buffer(boundary, 0), GEO_DISTANCE_INVALID);
  EXPECT_EQ(GeoDistance_Parse_Buffer(boundary, 15), GEO_DISTANCE_M);
  EXPECT_EQ(GeoDistance_Parse_Buffer(boundary, 16), GEO_DISTANCE_INVALID);
}

TEST(GeoContract, CreationDefaultsAndFieldAssignment) {
  for (const char* unit : {static_cast<const char*>(nullptr), "bad", "ft"}) {
    FilterOwner filter(NewGeoFilter(31.52, 32.1342, 10.01, unit, unit ? strlen(unit) : 0),
                       GeoFilter_Free);
    ASSERT_NE(filter, nullptr);
    EXPECT_EQ(filter->fieldIndex, RS_INVALID_FIELD_INDEX);
    EXPECT_EQ(filter->numericFilters, nullptr);
    EXPECT_EQ(filter->lon, 31.52);
    EXPECT_EQ(filter->lat, 32.1342);
    EXPECT_EQ(filter->radius, 10.01);
    EXPECT_EQ(filter->unitType, unit ? GeoDistance_Parse(unit) : GEO_DISTANCE_KM);
    FieldSpec field{};
    field.index = 17;
    GeoFilter_SetField(filter.get(), &field);
    field.index = 18;
    EXPECT_EQ(filter->fieldIndex, 17);
    GeoFilter_SetField(filter.get(), nullptr);
    EXPECT_EQ(filter->fieldIndex, RS_INVALID_FIELD_INDEX);
  }
}

TEST(GeoContract, ValidationBoundsPrecedenceAndFirstError) {
  const double inf = std::numeric_limits<double>::infinity();
  const struct {
    double lon, lat, radius;
    GeoDistance unit;
    const char* message;
  } cases[] = {
      {-180, -90, 1, GEO_DISTANCE_M, nullptr},
      {180, 90, 1, GEO_DISTANCE_KM, nullptr},
      {-0.0, -0.0, std::numeric_limits<double>::denorm_min(), GEO_DISTANCE_FT, nullptr},
      {0, 86, inf, GEO_DISTANCE_MI, nullptr},
      {0, -86, 1, GEO_DISTANCE_M, nullptr},
      {std::nextafter(180.0, inf), 0, 1, GEO_DISTANCE_M, "Invalid GeoFilter lat/lon"},
      {std::nextafter(-180.0, -inf), 0, 1, GEO_DISTANCE_M, "Invalid GeoFilter lat/lon"},
      {0, std::nextafter(90.0, inf), 1, GEO_DISTANCE_M, "Invalid GeoFilter lat/lon"},
      {0, std::nextafter(-90.0, -inf), 1, GEO_DISTANCE_M, "Invalid GeoFilter lat/lon"},
      {inf, 0, 1, GEO_DISTANCE_M, "Invalid GeoFilter lat/lon"},
      {0, -inf, 1, GEO_DISTANCE_M, "Invalid GeoFilter lat/lon"},
      {181, 91, -1, GEO_DISTANCE_INVALID, "Invalid GeoFilter unit"},
      {181, 91, -1, GEO_DISTANCE_M, "Invalid GeoFilter lat/lon"},
      {0, 0, 0, GEO_DISTANCE_M, "Invalid GeoFilter radius"},
      {0, 0, -0.0, GEO_DISTANCE_M, "Invalid GeoFilter radius"},
      {0, 0, -inf, GEO_DISTANCE_M, "Invalid GeoFilter radius"},
  };
  for (const auto& item : cases) {
    SCOPED_TRACE(::testing::Message() << item.lon << ',' << item.lat << ',' << item.radius);
    FilterOwner filter(NewGeoFilter(item.lon, item.lat, item.radius, nullptr, 0), GeoFilter_Free);
    filter->unitType = item.unit;
    ErrorOwner error;
    EXPECT_EQ(GeoFilter_Validate(filter.get(), &error.value), item.message ? 0 : 1);
    EXPECT_EQ(QueryError_GetCode(&error.value),
              item.message ? QUERY_ERROR_CODE_SYNTAX : QUERY_ERROR_CODE_OK);
    if (item.message) {
      EXPECT_EQ(std::string(QueryError_GetUserError(&error.value)),
                std::string("SEARCH_SYNTAX ") + item.message);
    }
    EXPECT_EQ(filter->lon, item.lon);
    EXPECT_EQ(filter->lat, item.lat);
    EXPECT_EQ(filter->radius, item.radius);
    QueryError_ClearError(&error.value);
    QueryError_SetError(&error.value, QUERY_ERROR_CODE_GENERIC, "retained first error");
    EXPECT_EQ(GeoFilter_Validate(filter.get(), &error.value), item.message ? 0 : 1);
    EXPECT_STREQ(QueryError_GetUserError(&error.value), "SEARCH_GENERIC retained first error");
  }
}

TEST(GeoContractBaselineDefect, ValidatorAcceptsNaN) {
  for (size_t slot : {0u, 1u, 2u}) {
    FilterOwner filter(NewGeoFilter(0, 0, 1, "m", 1), GeoFilter_Free);
    double* fields[] = {&filter->lon, &filter->lat, &filter->radius};
    *fields[slot] = std::numeric_limits<double>::quiet_NaN();
    ErrorOwner error;
    EXPECT_EQ(GeoFilter_Validate(filter.get(), &error.value), 1);
    EXPECT_FALSE(QueryError_HasError(&error.value));
    // Validation is not a finiteness proof; do not feed this to the Rust radius callback.
  }
}

TEST(GeoContract, RadiusIsInclusiveAndWritesRejectedDistance) {
  static_assert(
      std::is_same_v<decltype(&isWithinRadius), int (*)(const GeoFilter*, double, double*)>);
  FilterOwner filter(NewGeoFilter(0, 0, 1, "m", 1), GeoFilter_Free);
  const double score = encodeGeo(1, 1);
  ASSERT_NE(score, INVALID_GEOHASH);
  double distance = -1;
  ASSERT_EQ(isWithinRadius(filter.get(), score, &distance), 0);
  ASSERT_TRUE(std::isfinite(distance));
  ASSERT_GT(distance, 1);
  // Reusing the measured distance isolates the comparison from the distance formula.
  const double measured = distance;
  for (double radius : {std::nextafter(measured, 0.0), measured,
                        std::nextafter(measured, std::numeric_limits<double>::infinity())}) {
    filter->radius = radius;
    distance = -1;
    EXPECT_EQ(isWithinRadius(filter.get(), score, &distance), radius >= measured ? 1 : 0);
    EXPECT_EQ(distance, measured);
    EXPECT_EQ(isWithinRadius(filter.get(), score, nullptr), radius >= measured ? 1 : 0);
  }
  filter->radius = std::numeric_limits<double>::infinity();
  EXPECT_EQ(isWithinRadius(filter.get(), score, nullptr), 1);
}

TEST(GeoContract, RadiusUsesHistoricalUnitFactors) {
  FilterOwner filter(NewGeoFilter(0, 0, 1, "m", 1), GeoFilter_Free);
  const double score = encodeGeo(1, 0);
  double distance = -1;
  ASSERT_EQ(isWithinRadius(filter.get(), score, &distance), 0);
  ASSERT_GT(distance, 1);
  const struct {
    GeoDistance unit;
    double factor;
  } cases[] = {{GEO_DISTANCE_M, 1},
               {GEO_DISTANCE_KM, 1000},
               {GEO_DISTANCE_FT, 0.3048},
               {GEO_DISTANCE_MI, 1609.34}};
  for (const auto& item : cases) {
    SCOPED_TRACE(GeoDistance_ToString(item.unit));
    filter->unitType = item.unit;
    filter->radius = distance / item.factor * (1 - 1e-8);
    EXPECT_EQ(isWithinRadius(filter.get(), score, nullptr), 0);
    filter->radius = distance / item.factor * (1 + 1e-8);
    EXPECT_EQ(isWithinRadius(filter.get(), score, nullptr), 1);
  }
}

TEST(GeoContract, LegacySuccessBorrowsFieldAndSupportsBaseTransfer) {
  LegacyOwner filter(static_cast<LegacyGeoFilter*>(rm_calloc(1, sizeof(LegacyGeoFilter))),
                     LegacyGeoFilter_Free);
  char field[] = "loc";
  const char* args[] = {field, "31.52", "32.1342", "10.01", "KM", "tail"};
  ArgsCursor cursor;
  ArgsCursor_InitCString(&cursor, args, 6);
  bool empty = false;
  ErrorOwner error;
  ASSERT_EQ(GeoFilter_LegacyParse(filter.get(), &cursor, &empty, &error.value), REDISMODULE_OK);
  size_t length = 0;
  EXPECT_EQ(HiddenString_GetUnsafe(filter->field, &length), field);
  EXPECT_EQ(length, 3u);
  EXPECT_EQ(cursor.offset, 5u);
  EXPECT_FALSE(empty);
  EXPECT_EQ(filter->base.lon, 31.52);
  EXPECT_EQ(filter->base.lat, 32.1342);
  EXPECT_EQ(filter->base.radius, 10.01);
  EXPECT_EQ(filter->base.unitType, GEO_DISTANCE_KM);
  EXPECT_EQ(filter->base.fieldIndex, RS_INVALID_FIELD_INDEX);
  EXPECT_EQ(filter->base.numericFilters, nullptr);
  HiddenString_Free(filter->field, false);
  filter->field = nullptr;
  FilterOwner transferred(&filter.release()->base, GeoFilter_Free);
  transferred.reset();
  EXPECT_STREQ(field, "loc");
}

TEST(GeoContract, LegacyFailuresKeepCursorProgressAndAreFreeable) {
  const struct {
    const char* lon;
    const char* lat;
    const char* radius;
    const char* unit;
    int argc;
    size_t offset;
    const char* message;
  } cases[] = {
      {"1", "2", "3", "m", 4, 0, "GEOFILTER requires 5 arguments"},
      {"bad", "2", "3", "m", 5, 1,
       "Bad arguments for <lon>: Could not convert argument to expected type"},
      {"1", "bad", "3", "m", 5, 2,
       "Bad arguments for <lat>: Could not convert argument to expected type"},
      {"1", "2", "bad", "m", 5, 3,
       "Bad arguments for <radius>: Could not convert argument to expected type"},
      {"1", "2", "3", "yards", 5, 5, "Unknown distance unit yards"},
  };
  for (const auto& item : cases) {
    SCOPED_TRACE(item.message);
    LegacyOwner filter(static_cast<LegacyGeoFilter*>(rm_calloc(1, sizeof(LegacyGeoFilter))),
                       LegacyGeoFilter_Free);
    const char* args[] = {"loc", item.lon, item.lat, item.radius, item.unit};
    ArgsCursor cursor;
    ArgsCursor_InitCString(&cursor, args, item.argc);
    bool empty = false;
    ErrorOwner error;
    EXPECT_EQ(GeoFilter_LegacyParse(filter.get(), &cursor, &empty, &error.value), REDISMODULE_ERR);
    EXPECT_EQ(cursor.offset, item.offset);
    EXPECT_EQ(QueryError_GetCode(&error.value), QUERY_ERROR_CODE_PARSE_ARGS);
    EXPECT_EQ(std::string(QueryError_GetUserError(&error.value)),
              std::string("SEARCH_PARSE_ARGS ") + item.message);
    EXPECT_EQ(filter->field, nullptr);
    EXPECT_EQ(filter->base.numericFilters, nullptr);
    EXPECT_EQ(filter->base.fieldIndex, RS_INVALID_FIELD_INDEX);
    EXPECT_FALSE(empty);
  }
}

TEST(GeoContract, LegacyRedisEmptyValuesAreFlagged) {
  for (size_t emptySlot : {1u, 2u, 3u}) {
    const char* args[] = {"loc", "1", "2", "3", "m"};
    args[emptySlot] = "";
    RMCK::ArgvList argv(nullptr, args, size_t{5});
    ArgsCursor cursor;
    ArgsCursor_InitRString(&cursor, argv.data(), static_cast<int>(argv.size()));
    LegacyOwner filter(static_cast<LegacyGeoFilter*>(rm_calloc(1, sizeof(LegacyGeoFilter))),
                       LegacyGeoFilter_Free);
    ErrorOwner error;
    bool empty = false;
    EXPECT_EQ(GeoFilter_LegacyParse(filter.get(), &cursor, &empty, &error.value), REDISMODULE_OK);
    EXPECT_TRUE(empty);
    EXPECT_EQ(cursor.offset, 5u);
    EXPECT_EQ(filter->base.lon, emptySlot == 1 ? 0 : 1);
    EXPECT_EQ(filter->base.lat, emptySlot == 2 ? 0 : 2);
    EXPECT_EQ(filter->base.radius, emptySlot == 3 ? 0 : 3);
    EXPECT_EQ(GeoFilter_Validate(&filter->base, &error.value), emptySlot == 3 ? 0 : 1);
  }
}

TEST(GeoContract, AggregateNumericAndReferenceInputs) {
  if (!RSFunctionRegistry_Get("geodistance", 11)) {
    RegisterGeoFunctions();
  }
  const auto* function = RSFunctionRegistry_Get("geodistance", 11);
  ASSERT_NE(function, nullptr);
  EXPECT_EQ(function->minArgs, 2);
  EXPECT_EQ(function->maxArgs, 4);
  using ValueOwner = std::unique_ptr<RSValue, decltype(&RSValue_DecrRef)>;
  ValueOwner point(RSValue_NewNumber(encodeGeo(1.23, 4.56)), RSValue_DecrRef);
  ValueOwner reference(RSValue_NewReference(point.get()), RSValue_DecrRef);
  ValueOwner target(RSValue_NewBorrowedString("1.25,4.5", 8), RSValue_DecrRef);
  ValueOwner result(RSValue_NewUndefined(), RSValue_DecrRef);
  ErrorOwner error;
  ExprEval context{};
  context.err = &error.value;
  for (RSValue* value : {point.get(), reference.get()}) {
    RSValue* args[] = {value, target.get()};
    EXPECT_EQ(function->f(&context, args, 2, result.get()), EXPR_EVAL_OK);
    EXPECT_EQ(RSValue_Type(result.get()), RSValueType_Number);
    // Numeric values decode the geohash cell midpoint, not the original coordinate string.
    EXPECT_DOUBLE_EQ(RSValue_Number_Get(result.get()), 7032.41);
    EXPECT_FALSE(QueryError_HasError(&error.value));
  }
}

TEST(GeoContract, AggregateCallbackNanAndErrorState) {
  if (!RSFunctionRegistry_Get("geodistance", 11)) {
    RegisterGeoFunctions();
  }
  const auto* function = RSFunctionRegistry_Get("geodistance", 11);
  ASSERT_NE(function, nullptr);
  using ValueOwner = std::unique_ptr<RSValue, decltype(&RSValue_DecrRef)>;
  ValueOwner target(RSValue_NewBorrowedString("0,0", 3), RSValue_DecrRef);
  ValueOwner result(RSValue_NewUndefined(), RSValue_DecrRef);
  for (bool malformedString : {false, true}) {
    ValueOwner bad(malformedString ? RSValue_NewBorrowedString("bad", 3) : RSValue_NewNull(),
                   RSValue_DecrRef);
    ErrorOwner error;
    ExprEval context{};
    context.err = &error.value;
    RSValue* args[] = {bad.get(), target.get()};
    EXPECT_EQ(function->f(&context, args, 2, result.get()), EXPR_EVAL_OK);
    EXPECT_EQ(RSValue_Type(result.get()), RSValueType_Number);
    EXPECT_TRUE(std::isnan(RSValue_Number_Get(result.get())));
    EXPECT_EQ(QueryError_GetCode(&error.value),
              malformedString ? QUERY_ERROR_CODE_PARSE_ARGS : QUERY_ERROR_CODE_OK);
  }
}
