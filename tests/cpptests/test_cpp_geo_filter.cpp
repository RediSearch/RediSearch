/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#include "common.h"
#include "gtest/gtest.h"
extern "C" {
#include "geo_filter_ffi.h"
#include "geo_ffi.h"
#include "query_error_ffi.h"
#include "rmalloc.h"
}

TEST(GeoFilterContract, Lifecycle) {
  auto *filter = Rust_NewGeoFilter(12, 34, 1, nullptr, 0);
  ASSERT_NE(filter, nullptr);
  EXPECT_EQ(filter->fieldIndex, RS_INVALID_FIELD_INDEX);
  EXPECT_EQ(filter->numericFilters, nullptr);
  EXPECT_EQ(filter->unitType, GEO_DISTANCE_KM);
  EXPECT_STREQ(Rust_GeoDistance_ToString(GEO_DISTANCE_INVALID), "<badunit>");
  FieldSpec field = {};
  field.index = 7;
  Rust_GeoFilter_SetField(filter, &field);
  EXPECT_EQ(filter->fieldIndex, 7);
  Rust_GeoFilter_SetField(filter, nullptr);
  EXPECT_EQ(filter->fieldIndex, RS_INVALID_FIELD_INDEX);
  double distance = -1;
  const double score = encodeGeo(12.01, 34);
  ASSERT_EQ(Rust_isWithinRadius(filter, score, &distance), 1);
  filter->unitType = GEO_DISTANCE_M;
  filter->radius = distance;
  EXPECT_EQ(Rust_isWithinRadius(filter, score, nullptr), 1);
  filter->radius = 0.001;
  double outside_distance = -1;
  EXPECT_EQ(Rust_isWithinRadius(filter, score, &outside_distance), 0);
  EXPECT_DOUBLE_EQ(outside_distance, distance);
  Rust_GeoFilter_Free(filter);

  const char *args[] = {"g", "12", "bad", "1", "km"};
  ArgsCursor cursor;
  ArgsCursor_InitCString(&cursor, args, 5);
  auto *legacy = static_cast<LegacyGeoFilter *>(rm_malloc(sizeof(LegacyGeoFilter)));
  auto error = QueryError_Default();
  bool empty = false;
  EXPECT_EQ(Rust_GeoFilter_LegacyParse(legacy, &cursor, &empty, &error), REDISMODULE_ERR);
  EXPECT_EQ(cursor.offset, 2);
  EXPECT_FALSE(empty);
  EXPECT_EQ(legacy->field, nullptr);
  Rust_LegacyGeoFilter_Free(legacy);
  QueryError_ClearError(&error);
}
