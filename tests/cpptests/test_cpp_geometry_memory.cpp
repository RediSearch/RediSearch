/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#include "gtest/gtest.h"
#include "redismock/redismock.h"

#include "config.h"
#include "geometry/geometry_api.h"
#include "query_error_ffi.h"
#include "query_memory.h"

#include "geometry_test_utils.h"

#include <string>

// Without a request (no search context), memory pressure while collecting the matching ids fails
// the iterator build with the OOM message.
class GeometryMemoryTest : public ::testing::TestWithParam<GEOMETRY_COORDS> {
 protected:
  GeometryIndex *idx = nullptr;
  const GeometryApi *api = nullptr;
  RSOomPolicy previousPolicy = OomPolicy_Return;

  void SetUp() override {
    previousPolicy = RSGlobalConfig.requestConfigParams.oomPolicy;
    RSGlobalConfig.requestConfigParams.oomPolicy = OomPolicy_Return;
    idx = GeometryIndexFactory(GetParam());
    api = GeometryApi_Get(idx);
    for (int i = 0; i < 10; ++i) {
      const std::string wkt = shapeAt(2 * i);
      RedisModuleString *err = nullptr;
      ASSERT_EQ(api->addGeomStr(idx, GEOMETRY_FORMAT_WKT, wkt.data(), wkt.size(), i + 1, &err), 1);
    }
  }
  void TearDown() override {
    QueryMemory_ResetFakePressure();
    RSGlobalConfig.requestConfigParams.oomPolicy = previousPolicy;
    api->freeIndex(idx);
  }

  QueryIterator *within(RedisModuleString **err) const {
    const std::string area = "POLYGON((0 0, 0 2, 20 2, 20 0, 0 0))";
    return api->query(nullptr, nullptr, idx, WITHIN, GEOMETRY_FORMAT_WKT, area.data(), area.size(),
                      err);
  }
};

TEST_P(GeometryMemoryTest, PressureWithoutARequestFailsTheBuild) {
  QueryMemory_SetPressureAtCall(2);
  RedisModuleString *err = nullptr;
  EXPECT_EQ(within(&err), nullptr);
  ASSERT_NE(err, nullptr);
  EXPECT_STREQ(RedisModule_StringPtrLen(err, nullptr),
               QueryError_StrerrorDefaultMessage(QUERY_ERROR_CODE_OUT_OF_MEMORY));
  RedisModule_FreeString(nullptr, err);
}

INSTANTIATE_TEST_SUITE_P(Coords, GeometryMemoryTest,
                         ::testing::Values(GEOMETRY_COORDS_Cartesian, GEOMETRY_COORDS_Geographic));
