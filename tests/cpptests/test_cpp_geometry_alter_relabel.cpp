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
#include "redismock/util.h"

#include "spec.h"
#include "indexes.h"
#include "doc_id_meta.h"
#include "VecSim/vec_sim.h"
#include "geometry/geometry_api.h"

// geometry_index.h has no extern "C" guard, and openVectorIndex is outside vector_index.h's.
#include "search_ctx.h"
extern "C" {
#include "geometry_index.h"
#include "vector_index.h"
#include "redis_index.h"
#include "info/global_stats.h"
}

#include "geometry_test_utils.h"
#include "partial_update_fixture.h"

#include <cmath>
#include <string>
#include <vector>

extern "C" int IndexSpec_UpdateDoc(IndexSpec *spec, RedisModuleCtx *ctx, RedisModuleString *key,
                                   DocumentType type, RedisModuleKey *openKey,
                                   RedisModuleString **changedFields, size_t numChangedFields);
extern "C" int IndexSpec_UpdateDocForAlter(IndexSpec *spec, RedisModuleCtx *ctx,
                                           RedisModuleString *key, DocumentType type,
                                           t_fieldIndex addedFieldsStart);

namespace {
const char *const kPoly = "POLYGON((1 1, 1 50, 50 50, 50 1, 1 1))";
const char *const kPolyMoved = "POLYGON((1 1, 1 50, 50 50, 50 2, 1 1))";
const char *const kPolyHole =
    "POLYGON((1 1, 1 50, 50 50, 50 1, 1 1), (10 10, 20 10, 20 20, 10 20, 10 10))";
const char *const kPoint = "POINT(10 10)";
// Parses, but fails validation.
const char *const kPolyInvalid = "POLYGON((1 1, 1 100, 1 1))";
const char *const kVecA = "aaaabbbbccccdddd";
}  // namespace

class GeometryAlterRelabelTest : public PartialUpdateTest {
 protected:
  GeometryAlterRelabelTest() : PartialUpdateTest("geomalteridx") {
  }

  template <typename... Args>
  void createSchema(Args... schema) {
    QueryError err = QueryError_Default();
    RMCK::ArgvList args(ctx, "FT.CREATE", indexName.c_str(), "ON", "HASH", "SCHEMA", schema...);
    spec = Indexes_CreateNewSpec(ctx, args, args.size(), &err);
    ASSERT_FALSE(QueryError_HasError(&err)) << QueryError_GetUserError(&err);
    ASSERT_TRUE(spec != nullptr);
  }
  // `extra` stands for the field the ALTER added.
  void createAlterIndex(const char *coords = "FLAT") {
    createSchema("title", "TEXT", "geom", "GEOSHAPE", coords, "extra", "TAG");
  }

  t_fieldIndex fieldIndex(const std::string &name) {
    for (size_t i = 0; i < spec->numFields; ++i) {
      if (!HiddenString_CompareC(spec->fields[i].fieldName, name.c_str(), name.size())) {
        return spec->fields[i].index;
      }
    }
    ADD_FAILURE() << "no field " << name;
    return RS_INVALID_FIELD_INDEX;
  }

  // HSET `fields` and index with no change set.
  t_docId indexFields(const char *key,
                      std::initializer_list<std::pair<const char *, const char *>> fields) {
    for (const auto &[name, value] : fields) {
      RMCK::hset(ctx, key, name, value);
    }
    EXPECT_EQ(
        IndexSpec_UpdateDoc(spec, ctx, RMCK::RString(key), DocumentType_Hash, nullptr, nullptr, 0),
        REDISMODULE_OK);
    return docIdOf(key);
  }

  t_docId reindexForAlter(const char *key, const char *firstAddedField) {
    EXPECT_EQ(IndexSpec_UpdateDocForAlter(spec, ctx, RMCK::RString(key), DocumentType_Hash,
                                          fieldIndex(firstAddedField)),
              REDISMODULE_OK);
    return docIdOf(key);
  }
  t_docId backfillExtra(const char *key = "doc:1") {
    RMCK::hset(ctx, key, "extra", "x");
    return reindexForAlter(key, "extra");
  }

  GeometryIndex *geomNamed(const std::string &field) {
    for (size_t i = 0; i < spec->numFields; ++i) {
      if (!(spec->fields[i].types & INDEXFLD_T_GEOMETRY)) continue;
      if (!HiddenString_CompareC(spec->fields[i].fieldName, field.c_str(), field.size())) {
        return OpenGeometryIndex(&spec->fields[i], DONT_CREATE_INDEX);
      }
    }
    return nullptr;
  }
  bool geomHolds(const std::string &field, t_docId id, const std::string &wkt) {
    GeometryIndex *idx = geomNamed(field);
    return idx && GeometryIndex_HoldsGeom(idx, id, GEOMETRY_FORMAT_WKT, wkt.data(), wkt.size());
  }
  TreeShape treeShape(const std::string &field) {
    const GeometryIndex *idx = geomNamed(field);
    return idx ? treeShapeOf(idx) : TreeShape{};
  }
  static size_t geomIndexedOps() {
    return RSGlobalStats.fieldsStats.geometryTotalDocsIndexed;
  }

  // End state only: the indexing counter tells a move from an insert.
  void expectGeomOnlyUnder(const std::string &field, t_docId old, t_docId neu, const char *wkt,
                           long entries = 1) {
    EXPECT_TRUE(geomHolds(field, neu, wkt)) << field;
    EXPECT_FALSE(geomHolds(field, old, wkt)) << field;
    EXPECT_EQ(treeShape(field), (TreeShape{entries, entries, 0})) << field;
  }

  void expectPreexistingMoved(const char *coords, const char *wkt) {
    createAlterIndex(coords);
    t_docId old = indexFields("doc:1", {{"title", "hello"}, {"geom", wkt}});
    ASSERT_NE(old, 0);
    ASSERT_TRUE(geomHolds("geom", old, wkt));
    const size_t before = geomIndexedOps();

    t_docId neu = backfillExtra();

    EXPECT_GT(neu, old);
    expectGeomOnlyUnder("geom", old, neu, wkt);
    EXPECT_EQ(geomIndexedOps() - before, 0u) << "moved, not re-indexed";
  }

  // g2 fails after g1 moved; g3's kept old entry must be dropped by the error sweep.
  void expectErrorAfterMove(const char *coords) {
    createSchema("title", "TEXT", "g1", "GEOSHAPE", coords, "g2", "GEOSHAPE", coords, "g3",
                 "GEOSHAPE", coords, "extra", "TAG");
    t_docId old = indexFields(
        "doc:1", {{"title", "hello"}, {"g1", kPoly}, {"g2", kPoint}, {"g3", kPolyHole}});
    ASSERT_NE(old, 0);
    ASSERT_TRUE(geomHolds("g3", old, kPolyHole));
    RMCK::hset(ctx, "doc:1", "g2", kPolyInvalid);
    const size_t before = geomIndexedOps();

    backfillExtra();
    const t_docId neu = spec->docs.maxDocId;

    EXPECT_GT(neu, old);
    expectGeomOnlyUnder("g1", old, neu, kPoly);
    EXPECT_FALSE(geomHolds("g3", old, kPolyHole));
    EXPECT_FALSE(geomHolds("g3", neu, kPolyHole));
    EXPECT_EQ(treeShape("g3"), (TreeShape{0, 0, 0}));
    EXPECT_EQ(geomIndexedOps() - before, 0u) << "the move is not an indexing op";
  }
};

TEST_F(GeometryAlterRelabelTest, preexistingGeoshapeMovedFlat) {
  expectPreexistingMoved("FLAT", kPoly);
}

TEST_F(GeometryAlterRelabelTest, preexistingGeoshapeMovedSpherical) {
  expectPreexistingMoved("SPHERICAL", kPoly);
}

TEST_F(GeometryAlterRelabelTest, preexistingPolygonWithHoleMoved) {
  expectPreexistingMoved("FLAT", kPolyHole);
}

TEST_F(GeometryAlterRelabelTest, preexistingPointMovedFlat) {
  expectPreexistingMoved("FLAT", kPoint);
}

TEST_F(GeometryAlterRelabelTest, preexistingPointMovedSpherical) {
  expectPreexistingMoved("SPHERICAL", kPoint);
}

TEST_F(GeometryAlterRelabelTest, addedGeoshapeInserted) {
  createSchema("title", "TEXT", "geom", "GEOSHAPE", "FLAT", "geom2", "GEOSHAPE", "FLAT");
  t_docId old = indexFields("doc:1", {{"title", "hello"}, {"geom", kPoly}});
  ASSERT_NE(old, 0);
  RMCK::hset(ctx, "doc:1", "geom2", kPoint);
  const size_t before = geomIndexedOps();

  t_docId neu = reindexForAlter("doc:1", "geom2");

  expectGeomOnlyUnder("geom", old, neu, kPoly);
  expectGeomOnlyUnder("geom2", old, neu, kPoint);
  EXPECT_EQ(geomIndexedOps() - before, 1u) << "only geom2 is indexed";
}

TEST_F(GeometryAlterRelabelTest, twoPreexistingGeoshapeFieldsMoveIndependently) {
  createSchema("title", "TEXT", "geom", "GEOSHAPE", "FLAT", "geom2", "GEOSHAPE", "FLAT", "extra",
               "TAG");
  t_docId old1 = indexFields("doc:1", {{"title", "a"}, {"geom", kPoly}, {"geom2", kPoint}});
  t_docId old2 = indexFields("doc:2", {{"title", "b"}, {"geom", kPolyHole}});
  ASSERT_NE(old1, 0);
  ASSERT_NE(old2, 0);
  size_t before = geomIndexedOps();

  t_docId neu1 = backfillExtra("doc:1");
  t_docId neu2 = backfillExtra("doc:2");

  EXPECT_TRUE(geomHolds("geom", neu1, kPoly));
  EXPECT_TRUE(geomHolds("geom2", neu1, kPoint));
  EXPECT_TRUE(geomHolds("geom", neu2, kPolyHole));
  EXPECT_FALSE(geomHolds("geom", old1, kPoly));
  EXPECT_FALSE(geomHolds("geom2", old1, kPoint));
  EXPECT_FALSE(geomHolds("geom", old2, kPolyHole));
  EXPECT_EQ(treeShape("geom"), (TreeShape{2, 2, 0}));
  EXPECT_EQ(treeShape("geom2"), (TreeShape{1, 1, 0})) << "doc:2 has no geom2";
  EXPECT_EQ(geomIndexedOps() - before, 0u);

  // geom2 changed, geom did not.
  t_docId old3 = neu1;
  RMCK::hset(ctx, "doc:1", "geom2", "POINT(11 11)");
  before = geomIndexedOps();
  t_docId neu3 = reindexForAlter("doc:1", "extra");

  EXPECT_TRUE(geomHolds("geom", neu3, kPoly));
  EXPECT_FALSE(geomHolds("geom", old3, kPoly));
  EXPECT_TRUE(geomHolds("geom2", neu3, "POINT(11 11)"));
  EXPECT_FALSE(geomHolds("geom2", old3, kPoint));
  EXPECT_EQ(treeShape("geom2"), (TreeShape{1, 1, 0}));
  EXPECT_EQ(geomIndexedOps() - before, 1u);
}

TEST_F(GeometryAlterRelabelTest, oldEntryMissingInserts) {
  createAlterIndex();
  t_docId old = indexFields("doc:1", {{"title", "hello"}, {"geom", kPoly}});
  ASSERT_NE(old, 0);
  GeometryIndex *idx = geomNamed("geom");
  ASSERT_EQ(GeometryApi_Get(idx)->delGeom(idx, old), 1);
  const size_t before = geomIndexedOps();

  t_docId neu = backfillExtra();

  expectGeomOnlyUnder("geom", old, neu, kPoly);
  EXPECT_EQ(geomIndexedOps() - before, 1u);
}

TEST_F(GeometryAlterRelabelTest, valueChangedUnderIndexReAdds) {
  createAlterIndex();
  t_docId old = indexFields("doc:1", {{"title", "hello"}, {"geom", kPoly}});
  ASSERT_NE(old, 0);
  RMCK::hset(ctx, "doc:1", "geom", kPolyMoved);  // no reindex
  const size_t before = geomIndexedOps();

  t_docId neu = backfillExtra();

  EXPECT_TRUE(geomHolds("geom", neu, kPolyMoved));
  EXPECT_FALSE(geomHolds("geom", neu, kPoly));
  EXPECT_FALSE(geomHolds("geom", old, kPoly));
  EXPECT_EQ(treeShape("geom"), (TreeShape{1, 1, 0}));
  EXPECT_EQ(geomIndexedOps() - before, 1u);
}

TEST_F(GeometryAlterRelabelTest, fieldRemovedFromDocLeavesNoEntry) {
  createAlterIndex();
  t_docId old = indexFields("doc:1", {{"title", "hello"}, {"geom", kPoly}});
  ASSERT_NE(old, 0);
  hdel("doc:1", "geom");

  t_docId neu = backfillExtra();

  EXPECT_NE(neu, old);
  EXPECT_FALSE(geomHolds("geom", old, kPoly));
  EXPECT_FALSE(geomHolds("geom", neu, kPoly));
  EXPECT_EQ(treeShape("geom"), (TreeShape{0, 0, 0}));
}

// geom0 fails after geom was kept for the move; the error sweep must drop the kept entry.
TEST_F(GeometryAlterRelabelTest, errorBeforeGeoshapeLeavesNoOrphan) {
  createSchema("title", "TEXT", "geom0", "GEOSHAPE", "FLAT", "geom", "GEOSHAPE", "FLAT", "extra",
               "TAG");
  t_docId old = indexFields("doc:1", {{"title", "hello"}, {"geom0", kPoint}, {"geom", kPoly}});
  ASSERT_NE(old, 0);
  ASSERT_TRUE(geomHolds("geom", old, kPoly));

  RMCK::hset(ctx, "doc:1", "geom0", kPolyInvalid);
  backfillExtra();

  EXPECT_FALSE(geomHolds("geom", old, kPoly));
  EXPECT_EQ(treeShape("geom"), (TreeShape{0, 0, 0}));
}

TEST_F(GeometryAlterRelabelTest, errorAfterMoveFlat) {
  expectErrorAfterMove("FLAT");
}

TEST_F(GeometryAlterRelabelTest, errorAfterMoveSpherical) {
  expectErrorAfterMove("SPHERICAL");
}

TEST_F(GeometryAlterRelabelTest, flagOffNoMove) {
  RSGlobalConfig.optimizePartialUpdate = false;  // restored by TearDown
  createAlterIndex();
  t_docId old = indexFields("doc:1", {{"title", "hello"}, {"geom", kPoly}});
  ASSERT_NE(old, 0);
  const size_t before = geomIndexedOps();

  t_docId neu = backfillExtra();

  expectGeomOnlyUnder("geom", old, neu, kPoly);
  EXPECT_EQ(geomIndexedOps() - before, 1u);
}

TEST_F(GeometryAlterRelabelTest, ordinaryUpdateNoMove) {
  createAlterIndex();
  t_docId old = indexFields("doc:1", {{"title", "hello"}, {"geom", kPoly}});
  ASSERT_NE(old, 0);
  const size_t before = geomIndexedOps();

  t_docId neu = indexFields("doc:1", {{"title", "goodbye"}});

  EXPECT_GT(neu, old);
  expectGeomOnlyUnder("geom", old, neu, kPoly);
  EXPECT_EQ(geomIndexedOps() - before, 1u);
}

TEST_F(GeometryAlterRelabelTest, vectorAndGeoshapeTogether) {
  createSchema("title", "TEXT", "vec", "VECTOR", "FLAT", "6", "TYPE", "FLOAT32", "DIM", "4",
               "DISTANCE_METRIC", "L2", "geom", "GEOSHAPE", "FLAT", "extra", "TAG");
  t_docId old = indexFields("doc:1", {{"title", "hello"}, {"vec", kVecA}, {"geom", kPoly}});
  ASSERT_NE(old, 0);
  const size_t geomBefore = geomIndexedOps();
  const size_t vecIndexedBefore = RSGlobalStats.fieldsStats.vectorTotalDocsIndexed;
  const size_t vecRelabeledBefore = RSGlobalStats.fieldsStats.vectorTotalDocsRelabeled;

  t_docId neu = backfillExtra();

  expectGeomOnlyUnder("geom", old, neu, kPoly);
  EXPECT_EQ(geomIndexedOps() - geomBefore, 0u);
  VecSimIndex *vecsim = openVectorIndex(ctx, &spec->fields[fieldIndex("vec")], DONT_CREATE_INDEX);
  ASSERT_TRUE(vecsim != nullptr);
  EXPECT_EQ(VecSimIndex_GetDistanceFrom_Unsafe(vecsim, neu, kVecA), 0.0);
  EXPECT_TRUE(std::isnan(VecSimIndex_GetDistanceFrom_Unsafe(vecsim, old, kVecA)));
  EXPECT_EQ(RSGlobalStats.fieldsStats.vectorTotalDocsIndexed - vecIndexedBefore, 0u);
  EXPECT_EQ(RSGlobalStats.fieldsStats.vectorTotalDocsRelabeled - vecRelabeledBefore, 1u);
}

// shapeAt's points tie square corners, so some SPHERICAL moves are refused. Each refusal
// inserts, and leaves a stale old pair as plain remove does.
TEST_F(GeometryAlterRelabelTest, refusedMoveReinsertsSpherical) {
  constexpr int kN = 200;
  createAlterIndex("SPHERICAL");
  std::vector<t_docId> old(kN);
  for (int i = 0; i < kN; ++i) {
    const std::string key = "doc:" + std::to_string(i);
    const std::string wkt = shapeAt(i);
    old[i] = indexFields(key.c_str(), {{"title", "t"}, {"geom", wkt.c_str()}});
    ASSERT_NE(old[i], 0);
  }
  ASSERT_EQ(treeShape("geom"), (TreeShape{kN, kN, 0}));
  const size_t before = geomIndexedOps();

  for (int i = 0; i < kN; ++i) {
    const std::string key = "doc:" + std::to_string(i);
    const std::string wkt = shapeAt(i);
    const t_docId neu = backfillExtra(key.c_str());
    EXPECT_GT(neu, old[i]) << key;
    EXPECT_TRUE(geomHolds("geom", neu, wkt)) << key;
    EXPECT_FALSE(geomHolds("geom", old[i], wkt)) << key;
  }

  const size_t refused = geomIndexedOps() - before;
  EXPECT_GT(refused, 0u) << "premise: some moves are refused and fall back to an insert";
  EXPECT_LE(refused, size_t(kN / 2)) << "only points may be refused";
  const TreeShape s = treeShape("geom");
  EXPECT_EQ(s.withGeom, kN) << "each shape once, under its new doc-id";
  EXPECT_EQ(s.withoutGeom, long(refused)) << "one stale old pair per refusal, as remove leaves";
}
