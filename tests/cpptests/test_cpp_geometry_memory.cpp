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
#include "iterators/iterator_api.h"
#include "query_memory.h"
#include "query_request.h"
#include "search_ctx.h"
#include "spec.h"

#include "geometry_test_utils.h"

#include <string>

class GeometryMemoryTest : public ::testing::TestWithParam<GEOMETRY_COORDS> {
 protected:
  GeometryIndex *idx = nullptr;
  const GeometryApi *api = nullptr;
  IndexSpec spec{};
  QueryRequestLimits limits{};
  RedisSearchCtx sctx = SEARCH_CTX_STATIC(nullptr, &spec);
  FieldFilterContext filter{};
  RSOomPolicy previousPolicy = OomPolicy_Return;

  void SetUp() override {
    // ON_OOM IGNORE reads no memory.
    previousPolicy = RSGlobalConfig.requestConfigParams.oomPolicy;
    RSGlobalConfig.requestConfigParams.oomPolicy = OomPolicy_Return;
    TimeoutConfig config = {};
    QueryRequestLimits_Init(&limits, &config);
    sctx.limits = &limits;
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

  size_t withinCount() {
    const std::string area = "POLYGON((0 0, 0 2, 20 2, 20 0, 0 0))";
    RedisModuleString *err = nullptr;
    QueryIterator *it = api->query(&sctx, &filter, idx, WITHIN, GEOMETRY_FORMAT_WKT, area.data(),
                                   area.size(), &err);
    EXPECT_EQ(err, nullptr);
    if (!it) {
      return 0;
    }
    const size_t n = it->NumEstimated(it);
    it->Free(it);
    return n;
  }
};

TEST_P(GeometryMemoryTest, NoPressureYieldsEveryMatch) {
  EXPECT_EQ(withinCount(), 10u);
  EXPECT_FALSE(QueryRequestLimits_IsMemoryStopped(&limits));
}

// 10 matches are far below the read interval, so only the read at buffer growth can see this.
TEST_P(GeometryMemoryTest, PressureAtBufferGrowthYieldsNoIdsAndRequestsTheStop) {
  QueryMemory_SetForcedRatio(1.0f);
  EXPECT_EQ(withinCount(), 0u);
  EXPECT_TRUE(QueryRequestLimits_IsMemoryStopped(&limits));
}

INSTANTIATE_TEST_SUITE_P(Coords, GeometryMemoryTest,
                         ::testing::Values(GEOMETRY_COORDS_Cartesian, GEOMETRY_COORDS_Geographic));
