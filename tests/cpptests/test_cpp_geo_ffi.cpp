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
#include "aggregate/expr/expression.h"
#include "aggregate/functions/function.h"
#include "query_error_ffi.h"

#include <array>
#include <cmath>
#include <memory>
#include <string>

TEST(GeoFFI, ValidationRejectsOnlyCanonicalInvalidUnitBeforeScalarErrors) {
  // This enum's C++ value range is [-4, 3]; -4, -3, and -2 are valid non-enumerator values.
  for (int rawUnit : {-4, -3, -2, -1, 0, 1, 2, 3}) {
    for (const auto& scalars : {std::array<double, 3>{0, 0, 1}, {181, 91, 0}, {0, 0, 0}}) {
      SCOPED_TRACE(::testing::Message()
                   << rawUnit << ':' << scalars[0] << ',' << scalars[1] << ',' << scalars[2]);
      GeoFilter filter{};
      filter.lon = scalars[0];
      filter.lat = scalars[1];
      filter.radius = scalars[2];
      filter.unitType = static_cast<GeoDistance>(rawUnit);
      const char* message = rawUnit == -1      ? "Invalid GeoFilter unit"
                            : scalars[0] > 180 ? "Invalid GeoFilter lat/lon"
                            : scalars[2] <= 0  ? "Invalid GeoFilter radius"
                                               : nullptr;
      QueryError error = QueryError_Default();
      EXPECT_EQ(GeoFilter_Validate(&filter, &error), message ? 0 : 1);
      EXPECT_EQ(QueryError_GetCode(&error),
                message ? QUERY_ERROR_CODE_SYNTAX : QUERY_ERROR_CODE_OK);
      if (message) {
        EXPECT_EQ(std::string(QueryError_GetUserError(&error)),
                  std::string("SEARCH_SYNTAX ") + message);
      }
      EXPECT_EQ(static_cast<int>(filter.unitType), rawUnit);
      QueryError_ClearError(&error);
      QueryError_SetError(&error, QUERY_ERROR_CODE_GENERIC, "retained first error");
      EXPECT_EQ(GeoFilter_Validate(&filter, &error), message ? 0 : 1);
      EXPECT_EQ(QueryError_GetCode(&error), QUERY_ERROR_CODE_GENERIC);
      EXPECT_STREQ(QueryError_GetUserError(&error), "SEARCH_GENERIC retained first error");
      QueryError_ClearError(&error);
    }
  }
}

TEST(GeoFFI, LegacyNonUtf8UnitPreservesRawAndObfuscatedErrors) {
  const char unit[] = {'m', static_cast<char>(0xff), '\0', 'x', '\0'};
  const char* args[] = {"prefix", "loc", "1", "2", "3", unit};
  for (bool redisStrings : {false, true}) {
    RMCK::ArgvList argv(nullptr, args, size_t{6});
    ArgsCursor cursor;
    if (redisStrings) {
      ArgsCursor_InitRString(&cursor, argv.data(), static_cast<int>(argv.size()));
    } else {
      ArgsCursor_InitCString(&cursor, args, 6);
    }
    cursor.offset = 1;
    LegacyGeoFilter filter{};
    QueryError error = QueryError_Default();
    bool empty = true;
    ASSERT_EQ(GeoFilter_LegacyParse(&filter, &cursor, &empty, &error), REDISMODULE_ERR);
    EXPECT_EQ(cursor.offset, 6u);
    EXPECT_TRUE(empty);
    EXPECT_EQ(filter.base.lon, 1);
    EXPECT_EQ(filter.base.lat, 2);
    EXPECT_EQ(filter.base.radius, 3);
    EXPECT_EQ(filter.base.unitType, GEO_DISTANCE_INVALID);
    EXPECT_EQ(filter.field, nullptr);
    EXPECT_EQ(QueryError_GetCode(&error), QUERY_ERROR_CODE_PARSE_ARGS);
    std::string expected = "SEARCH_PARSE_ARGS Unknown distance unit ";
    expected.append(unit, 2);
    EXPECT_EQ(std::string(QueryError_GetUserError(&error)), expected);
    EXPECT_STREQ(QueryError_GetDisplayableError(&error, true), "Unknown distance unit");
    QueryError_ClearError(&error);
  }
}

TEST(GeoFFI, LegacySdsUsesLogicalLengthForEmptyCoalescingAndKeepsPartialWrites) {
  const char embedded[] = {'\0', 'x'};
  sds args[] = {sdsnew("loc"), sdsnew("1"), sdsnewlen(embedded, sizeof(embedded)), sdsnew("bad"),
                sdsnew("m")};
  ArgsCursor cursor;
  ArgsCursor_InitSDS(&cursor, args, 5);
  LegacyGeoFilter filter{};
  QueryError error = QueryError_Default();
  bool empty = false;
  // Internal strtod accepts the NUL prefix as zero despite the nonempty SDS payload.
  EXPECT_EQ(GeoFilter_LegacyParse(&filter, &cursor, &empty, &error), REDISMODULE_ERR);
  EXPECT_EQ(cursor.offset, 3u);
  EXPECT_EQ(filter.base.lon, 1);
  EXPECT_EQ(filter.base.lat, 0);
  EXPECT_EQ(filter.base.radius, 0);
  EXPECT_EQ(filter.base.unitType, GEO_DISTANCE_KM);
  EXPECT_EQ(filter.field, nullptr);
  EXPECT_FALSE(empty);
  EXPECT_STREQ(QueryError_GetDisplayableError(&error, true), "Bad arguments");
  EXPECT_STREQ(
      QueryError_GetUserError(&error),
      "SEARCH_PARSE_ARGS Bad arguments for <radius>: Could not convert argument to expected type");
  QueryError_ClearError(&error);
  for (sds arg : args) {
    sdsfree(arg);
  }
}

TEST(GeoFFI, AggregateCoercesTriosOnlyForRawCoordinates) {
  if (!RSFunctionRegistry_Get("geodistance", 11)) {
    RegisterGeoFunctions();
  }
  const auto* function = RSFunctionRegistry_Get("geodistance", 11);
  ASSERT_NE(function, nullptr);
  using ValueOwner = std::unique_ptr<RSValue, decltype(&RSValue_DecrRef)>;
  ValueOwner trio(RSValue_NewTrio(RSValue_NewNumber(0), RSValue_NewNull(), RSValue_NewNull()),
                  RSValue_DecrRef);
  ValueOwner reference(RSValue_NewReference(trio.get()), RSValue_DecrRef);
  ValueOwner zero(RSValue_NewNumber(0), RSValue_DecrRef);
  ValueOwner target(RSValue_NewBorrowedString("0,0", 3), RSValue_DecrRef);
  ValueOwner result(RSValue_NewUndefined(), RSValue_DecrRef);
  QueryError error = QueryError_Default();
  ExprEval context{};
  context.err = &error;
  for (RSValue* value : {trio.get(), reference.get()}) {
    RSValue* rawArgs[] = {value, zero.get(), target.get()};
    EXPECT_EQ(function->f(&context, rawArgs, 3, result.get()), EXPR_EVAL_OK);
    EXPECT_EQ(RSValue_Number_Get(result.get()), 0);
    EXPECT_FALSE(QueryError_HasError(&error));
    RSValue* fieldArgs[] = {value, target.get()};
    EXPECT_EQ(function->f(&context, fieldArgs, 2, result.get()), EXPR_EVAL_OK);
    EXPECT_TRUE(std::isnan(RSValue_Number_Get(result.get())));
    EXPECT_FALSE(QueryError_HasError(&error));
  }
  QueryError_ClearError(&error);
}

TEST(GeoFFI, AggregateReferenceToRedisStringPreservesCoordinateParseErrors) {
  if (!RSFunctionRegistry_Get("geodistance", 11)) {
    RegisterGeoFunctions();
  }
  const auto* function = RSFunctionRegistry_Get("geodistance", 11);
  ASSERT_NE(function, nullptr);
  const char bytes[] = {'1', ',', static_cast<char>(0xff)};
  RedisModuleString* string = RedisModule_CreateString(nullptr, bytes, sizeof(bytes));
  using ValueOwner = std::unique_ptr<RSValue, decltype(&RSValue_DecrRef)>;
  ValueOwner field(RSValue_NewRedisString(string), RSValue_DecrRef);
  ValueOwner reference(RSValue_NewReference(field.get()), RSValue_DecrRef);
  ValueOwner target(RSValue_NewBorrowedString("0,0", 3), RSValue_DecrRef);
  ValueOwner result(RSValue_NewUndefined(), RSValue_DecrRef);
  QueryError error = QueryError_Default();
  ExprEval context{};
  context.err = &error;
  RSValue* args[] = {reference.get(), target.get()};
  EXPECT_EQ(function->f(&context, args, 2, result.get()), EXPR_EVAL_OK);
  EXPECT_TRUE(std::isnan(RSValue_Number_Get(result.get())));
  EXPECT_EQ(QueryError_GetCode(&error), QUERY_ERROR_CODE_PARSE_ARGS);
  const std::string firstError = QueryError_GetUserError(&error);
  EXPECT_NE(firstError.find("UTF-8"), std::string::npos);
  EXPECT_EQ(function->f(&context, args, 2, result.get()), EXPR_EVAL_OK);
  EXPECT_EQ(std::string(QueryError_GetUserError(&error)), firstError);
  QueryError_ClearError(&error);
}
