/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

// rstest is C++17 and rtree.hpp C++20, so only the C API is tested.

#include "gtest/gtest.h"
#include "redismock/redismock.h"

#include "geometry/geometry_api.h"

// geometry_index.h has no extern "C" guard; include its C++-aware dependencies first.
#include "search_ctx.h"
extern "C" {
#include "geometry_index.h"
}

#include "geometry_test_utils.h"

#include <cstdio>
#include <string>

namespace {
const std::string kPoly = "POLYGON((1 1, 1 50, 50 50, 50 1, 1 1))";
const std::string kPolyCcw = "POLYGON((1 1, 50 1, 50 50, 1 50, 1 1))";
// Same MBR as kPoly.
const std::string kPolyMoved = "POLYGON((1 1, 1 50, 50 50, 50 2, 1 1))";
const std::string kPolyRotated = "POLYGON((1 50, 50 50, 50 1, 1 1, 1 50))";
const std::string kPoint = "POINT(10 10)";
const std::string kPointMoved = "POINT(10 10.000001)";
const std::string kPointZero = "POINT(0 0)";
const std::string kPointNegZero = "POINT(-0 0)";
const std::string kPolyHole =
    "POLYGON((1 1, 1 50, 50 50, 50 1, 1 1), (10 10, 20 10, 20 20, 10 20, 10 10))";
const std::string kPolyHoleMoved =
    "POLYGON((1 1, 1 50, 50 50, 50 1, 1 1), (10 10, 20 10, 20 21, 10 20, 10 10))";
const std::string kMalformed = "POLYGON((1 1, 1 50";
const std::string kUnknownType = "LINESTRING(1 1, 2 2)";
}  // namespace

class GeometryRelabelTest : public ::testing::TestWithParam<GEOMETRY_COORDS> {
 protected:
  GeometryIndex *idx = nullptr;
  const GeometryApi *api = nullptr;

  void SetUp() override {
    idx = GeometryIndexFactory(GetParam());
    api = GeometryApi_Get(idx);
  }
  void TearDown() override {
    api->freeIndex(idx);
  }

  void add(t_docId id, const std::string &wkt, GeometryIndex *in = nullptr) {
    GeometryIndex *target = in ? in : idx;
    RedisModuleString *err = nullptr;
    ASSERT_EQ(api->addGeomStr(target, GEOMETRY_FORMAT_WKT, wkt.data(), wkt.size(), id, &err), 1)
        << wkt;
    ASSERT_EQ(err, nullptr);
  }
  int holds(t_docId id, const std::string &wkt) const {
    return api->holdsGeomStr(idx, GEOMETRY_FORMAT_WKT, wkt.data(), wkt.size(), id);
  }
  TreeShape shape(const GeometryIndex *in = nullptr) const {
    return treeShapeOf(in ? in : idx);
  }
};

TEST_P(GeometryRelabelTest, relabelMovesPolygon) {
  add(1, kPoly);
  ASSERT_EQ(api->relabelGeom(idx, 1, 11), 1);
  EXPECT_EQ(holds(11, kPoly), 1);
  EXPECT_EQ(holds(1, kPoly), 0);
  EXPECT_EQ(shape(), (TreeShape{1, 1, 0})) << "one R-tree entry, and its id is in the lookup table";
  EXPECT_EQ(api->delGeom(idx, 1), 0);
  EXPECT_EQ(api->delGeom(idx, 11), 1);
  EXPECT_EQ(shape().numDocs, 0) << "the R-tree entry was (box, 11): remove(11) found it";
}

TEST_P(GeometryRelabelTest, relabelMovesPoint) {
  add(2, kPoint);
  ASSERT_EQ(api->relabelGeom(idx, 2, 12), 1);
  EXPECT_EQ(holds(12, kPoint), 1);
  EXPECT_EQ(holds(2, kPoint), 0);
  EXPECT_EQ(shape(), (TreeShape{1, 1, 0}));
  EXPECT_EQ(api->delGeom(idx, 12), 1);
  EXPECT_EQ(shape().numDocs, 0);
}

TEST_P(GeometryRelabelTest, relabelMissingOldIdChangesNothing) {
  add(1, kPoly);
  const size_t before = api->report(idx);
  EXPECT_EQ(api->relabelGeom(idx, 7, 8), 0);
  EXPECT_EQ(api->report(idx), before);
  EXPECT_EQ(shape(), (TreeShape{1, 1, 0}));
  EXPECT_EQ(holds(1, kPoly), 1);
  EXPECT_EQ(holds(8, kPoly), 0);
}

TEST_P(GeometryRelabelTest, relabelOntoTakenIdRefuses) {
  add(1, kPoly);
  add(2, kPoint);
  const size_t before = api->report(idx);
  EXPECT_EQ(api->relabelGeom(idx, 1, 2), 0);
  EXPECT_EQ(api->report(idx), before);
  EXPECT_EQ(holds(1, kPoly), 1);
  EXPECT_EQ(holds(2, kPoint), 1);
  EXPECT_EQ(shape(), (TreeShape{2, 2, 0}));
}

TEST_P(GeometryRelabelTest, relabelSameIdIsNoOp) {
  add(1, kPoly);
  EXPECT_EQ(api->relabelGeom(idx, 1, 1), 1);
  EXPECT_EQ(holds(1, kPoly), 1);
  EXPECT_EQ(shape(), (TreeShape{1, 1, 0}));
}

TEST_P(GeometryRelabelTest, relabelChain) {
  add(1, kPoly);
  ASSERT_EQ(api->relabelGeom(idx, 1, 5), 1);
  ASSERT_EQ(api->relabelGeom(idx, 5, 9), 1);
  EXPECT_EQ(holds(9, kPoly), 1);
  EXPECT_EQ(holds(5, kPoly), 0);
  EXPECT_EQ(holds(1, kPoly), 0);
  EXPECT_EQ(api->delGeom(idx, 9), 1);
  EXPECT_EQ(shape().numDocs, 0);
}

// > 16 entries, so the tree has inner nodes. In SPHERICAL, remove can miss a point that ties a
// square corner (see RTree::relabel); relabel then refuses.
TEST_P(GeometryRelabelTest, relabelManyKeepsTreeConsistent) {
  constexpr int kN = 200;
  const bool flat = GetParam() == GEOMETRY_COORDS_Cartesian;
  for (int i = 1; i <= kN; ++i) add(i, shapeAt(i));
  int moved = 0;
  for (int i = 1; i <= kN; ++i) {
    if (api->relabelGeom(idx, i, i + 1000) == 1) {
      ++moved;
      EXPECT_EQ(holds(i + 1000, shapeAt(i)), 1) << i;
      EXPECT_EQ(holds(i, shapeAt(i)), 0) << i;
    } else {
      EXPECT_FALSE(flat) << "FLAT must never refuse: " << i;
      EXPECT_EQ(i % 2, 0) << "only SPHERICAL points may refuse: " << i;
      EXPECT_EQ(holds(i, shapeAt(i)), 1) << "a refusal leaves the entry where it was: " << i;
      EXPECT_EQ(holds(i + 1000, shapeAt(i)), 0) << i;
    }
  }
  EXPECT_EQ(shape(), (TreeShape{kN, kN, 0})) << "moves and refusals keep R-tree and lookup in step";
  EXPECT_GE(moved, flat ? kN : kN / 2) << "every polygon (odd i) moves";
  if (flat) {
    for (int i = 1; i <= kN; ++i) EXPECT_EQ(api->delGeom(idx, i + 1000), 1) << i;
    EXPECT_EQ(shape().numDocs, 0);
  }
}

TEST_P(GeometryRelabelTest, relabelMissesExactlyWhereRemoveMisses) {
  constexpr int kN = 200;
  int misses = 0;
  for (int i = 1; i <= kN; ++i) {
    GeometryIndex *removed = GeometryIndexFactory(GetParam());
    GeometryIndex *moved = GeometryIndexFactory(GetParam());
    for (int j = 1; j <= kN; ++j) {
      add(j, shapeAt(j), removed);
      add(j, shapeAt(j), moved);
    }
    ASSERT_EQ(api->delGeom(removed, i), 1) << i;
    const bool removeHit = shape(removed).numDocs == kN - 1;
    const bool relabelHit = api->relabelGeom(moved, i, i + 1000) == 1;
    EXPECT_EQ(relabelHit, removeHit) << i << " " << shapeAt(i);
    misses += !removeHit;
    api->freeIndex(removed);
    api->freeIndex(moved);
  }
  std::printf("[ PROBE    ] %s: remove/relabel miss on a fresh %d-entry tree: %d ids\n",
              GeometryCoordsToName(GetParam()), kN, misses);
}

TEST_P(GeometryRelabelTest, relabelPathLeaksNoMoreThanRemoveAdd) {
  constexpr int kN = 200;
  GeometryIndex *today = GeometryIndexFactory(GetParam());
  GeometryIndex *baseline = GeometryIndexFactory(GetParam());
  for (int i = 1; i <= kN; ++i) {
    add(i, shapeAt(i));
    add(i, shapeAt(i), today);
    add(i, shapeAt(i), baseline);
  }
  for (int i = 1; i <= kN; ++i) {
    if (!GeometryIndex_RelabelField(idx, i, i + 1000)) add(i + 1000, shapeAt(i));
    EXPECT_EQ(api->delGeom(today, i), 1) << i;
    add(i + 1000, shapeAt(i), today);
  }
  for (int i = 1; i <= kN; ++i) EXPECT_EQ(api->delGeom(baseline, i), 1) << i;
  const TreeShape moved = shape(), removed = shape(today), deleted = shape(baseline);
  EXPECT_EQ(moved.withGeom, kN);
  EXPECT_EQ(removed.withGeom, kN);
  EXPECT_LE(moved.withoutGeom, removed.withoutGeom);
  std::printf(
      "[ PROBE    ] %s: stale R-tree pairs: relabel path %ld, remove+add path %ld, "
      "delete all %ld\n",
      GeometryCoordsToName(GetParam()), moved.withoutGeom, removed.withoutGeom, deleted.numDocs);
  if (GetParam() == GEOMETRY_COORDS_Cartesian) {
    EXPECT_EQ(moved.withoutGeom, 0);
    EXPECT_EQ(removed.withoutGeom, 0);
    EXPECT_EQ(deleted.numDocs, 0);
  }
  api->freeIndex(today);
  api->freeIndex(baseline);
}

// Not compared to an empty index: polygon insert + remove leaves the counter drifted (copied
// allocator count). Both sides drift the same.
TEST_P(GeometryRelabelTest, relabelThenRemoveMatchesPlainRemove) {
  GeometryIndex *plain = GeometryIndexFactory(GetParam());
  add(1, kPoly, plain);
  add(1, kPoly);
  EXPECT_EQ(api->report(idx), api->report(plain));
  ASSERT_EQ(api->relabelGeom(idx, 1, 11), 1);
  EXPECT_EQ(api->report(idx), api->report(plain)) << "relabel does not change the counter";
  EXPECT_EQ(api->delGeom(plain, 1), 1);
  EXPECT_EQ(api->delGeom(idx, 11), 1);
  EXPECT_EQ(api->report(idx), api->report(plain))
      << "moved geometry reports what a stored one does";
  api->freeIndex(plain);
}

TEST_P(GeometryRelabelTest, holdsSameWkt) {
  add(1, kPoly);
  add(2, kPoint);
  add(3, kPolyCcw);
  EXPECT_EQ(holds(1, kPoly), 1);
  EXPECT_EQ(holds(2, kPoint), 1);
  EXPECT_EQ(holds(3, kPolyCcw), 1) << "holds must apply bg::correct like insert does";
  add(4, kPolyHole);
  EXPECT_EQ(holds(4, kPolyHole), 1);
}

TEST_P(GeometryRelabelTest, holdsRejectsDifferentValue) {
  add(1, kPoly);
  add(2, kPoint);
  EXPECT_EQ(holds(1, kPolyMoved), 0);
  EXPECT_EQ(holds(1, kPolyRotated), 0) << "exact compare, not semantic";
  EXPECT_EQ(holds(1, kPoint), 0);
  EXPECT_EQ(holds(2, kPointMoved), 0);
  EXPECT_EQ(holds(2, kPoly), 0);
  add(3, kPolyHole);
  EXPECT_EQ(holds(3, kPolyHoleMoved), 0) << "hole vertex differs";
  EXPECT_EQ(holds(3, kPoly), 0) << "stored has a hole, input has none";
  EXPECT_EQ(holds(1, kPolyHole), 0) << "input has a hole, stored has none";
  add(4, kPointZero);
  EXPECT_EQ(holds(4, kPointZero), 1);
  EXPECT_EQ(holds(4, kPointNegZero), 0) << "bit-exact: -0.0 is not 0.0";
}

TEST_P(GeometryRelabelTest, holdsRejectsBadInput) {
  add(1, kPoly);
  EXPECT_EQ(holds(1, kMalformed), 0);
  EXPECT_EQ(holds(1, kUnknownType), 0);
  EXPECT_EQ(holds(1, ""), 0);
  EXPECT_EQ(api->holdsGeomStr(idx, GEOMETRY_FORMAT_GEOJSON, kPoly.data(), kPoly.size(), 1), 0);
  EXPECT_EQ(holds(99, kPoly), 0) << "missing id";
}

TEST_P(GeometryRelabelTest, holdsDoesNotChangeIndex) {
  add(1, kPoly);
  const size_t before = api->report(idx);
  const TreeShape s = shape();
  holds(1, kPoly);
  holds(1, kPolyMoved);
  holds(1, kMalformed);
  EXPECT_EQ(api->report(idx), before);
  EXPECT_EQ(shape(), s);
}

TEST_P(GeometryRelabelTest, relabelFieldMoves) {
  add(1, kPoly);
  EXPECT_TRUE(GeometryIndex_RelabelField(idx, 1, 11));
  EXPECT_EQ(holds(11, kPoly), 1);
  EXPECT_EQ(shape(), (TreeShape{1, 1, 0}));
}

TEST_P(GeometryRelabelTest, relabelFieldDropsOldOnRefusal) {
  add(1, kPoly);
  add(2, kPoint);
  EXPECT_FALSE(GeometryIndex_RelabelField(idx, 1, 2));
  EXPECT_EQ(holds(1, kPoly), 0) << "old entry dropped so the caller can insert cleanly";
  EXPECT_EQ(holds(2, kPoint), 1);
  EXPECT_EQ(shape(), (TreeShape{1, 1, 0}));
  EXPECT_FALSE(GeometryIndex_RelabelField(idx, 7, 8)) << "missing old id";
}

TEST_P(GeometryRelabelTest, holdsGeomFacade) {
  add(1, kPoly);
  EXPECT_TRUE(GeometryIndex_HoldsGeom(idx, 1, GEOMETRY_FORMAT_WKT, kPoly.data(), kPoly.size()));
  EXPECT_FALSE(
      GeometryIndex_HoldsGeom(idx, 1, GEOMETRY_FORMAT_WKT, kPolyMoved.data(), kPolyMoved.size()));
  EXPECT_FALSE(
      GeometryIndex_HoldsGeom(idx, 1, GEOMETRY_FORMAT_GEOJSON, kPoly.data(), kPoly.size()));
}

INSTANTIATE_TEST_SUITE_P(Coords, GeometryRelabelTest,
                         ::testing::Values(GEOMETRY_COORDS_Cartesian, GEOMETRY_COORDS_Geographic),
                         [](const ::testing::TestParamInfo<GEOMETRY_COORDS> &info) {
                           return std::string(GeometryCoordsToName(info.param));
                         });
