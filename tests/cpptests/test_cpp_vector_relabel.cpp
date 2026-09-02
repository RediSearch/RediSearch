/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

// Relabeling an unchanged vector onto a document's new doc-id, instead of
// deleting the entry and re-adding the blob (MOD-17688).
//
// The optimization is driven by the change set a hash subkey notification
// carries, so these tests call IndexSpec_UpdateDoc with that set directly rather
// than going through a keyspace notification: the SKN path needs a server that
// emits subkeys, and what is under test here is the indexer's response to the
// set, not how the set is obtained.

#include "gtest/gtest.h"
#include "redismock/redismock.h"
#include "redismock/util.h"

#include "spec.h"
#include "indexes.h"
#include "doc_id_meta.h"
#include "VecSim/vec_sim.h"

// `openVectorIndex` is declared outside vector_index.h's own extern "C" block.
extern "C" {
#include "vector_index.h"
#include "redis_index.h"
#include "vector_compare/vector_compare.h"
}

#include <array>
#include <cmath>
#include <string>

extern "C" int IndexSpec_UpdateDoc(IndexSpec *spec, RedisModuleCtx *ctx, RedisModuleString *key,
                                   DocumentType type, RedisModuleKey *openKey,
                                   RedisModuleString **changedFields, size_t numChangedFields);

// FLOAT32 DIM 4 -- the blob is 16 bytes, matching expBlobSize.
static const char *const kVecA = "aaaabbbbccccdddd";
static const char *const kVecB = "eeeeffffgggghhhh";
// Read as four float32s, kVecA and kVecB happen to be nearly parallel (their
// components share a ~1:4:16:64 ratio), so cosine cannot tell them apart -- the
// distance between them is 2e-10. kVecC is kVecA byte-reversed, which keeps the same
// magnitudes but puts it 0.94 away under cosine. Use it wherever direction matters.
static const char *const kVecC = "ddddccccbbbbaaaa";

class VectorRelabelTest : public ::testing::Test {
protected:
  RedisModuleCtx *ctx = nullptr;
  IndexSpec *spec = nullptr;
  std::string indexName;

  bool previousOptimizeUpdateVec = false;

  void SetUp() override {
    ctx = RedisModule_GetThreadSafeContext(nullptr);
    RMCK::flushdb(ctx);
    static int counter = 0;
    indexName = "relabelidx" + std::to_string(++counter);
    // Relabeling is gated behind OPTIMIZE_UPDATE_VEC (on by default). Forced here so a
    // config change elsewhere can't disable it out from under these tests; restored in
    // TearDown, which runs even when an assertion fails, so no state leaks to other tests.
    previousOptimizeUpdateVec = RSGlobalConfig.optimizeUpdateVec;
    RSGlobalConfig.optimizeUpdateVec = true;
  }

  void TearDown() override {
    RSGlobalConfig.optimizeUpdateVec = previousOptimizeUpdateVec;
    if (ctx) {
      RedisModule_FreeThreadSafeContext(ctx);
      ctx = nullptr;
    }
  }

  // `vectorAlias` is the AS alias for the vector field, or nullptr for none. The
  // alias case matters: a change set names the hash field (the path), so a
  // schema-side comparison against the alias would classify every field wrongly.
  //
  // `algo` selects the vector backend. It is a test parameter because relabeling
  // is optional in VecSim: FLAT and HNSW implement it, SVS does not.
  // `metric` is a test parameter because the no-change-set path can only compare
  // blobs under L2; see `storedVectorIsUnchanged`.
  void createIndex(const char *vectorAlias, const char *algo = "FLAT",
                   const char *metric = "L2") {
    QueryError err = QueryError_Default();
    RMCK::ArgvList args =
        vectorAlias
            ? RMCK::ArgvList(ctx, "FT.CREATE", indexName.c_str(), "ON", "HASH", "SCHEMA", "title",
                             "TEXT", "vec", "AS", vectorAlias, "VECTOR", algo, "6", "TYPE",
                             "FLOAT32", "DIM", "4", "DISTANCE_METRIC", metric)
            : RMCK::ArgvList(ctx, "FT.CREATE", indexName.c_str(), "ON", "HASH", "SCHEMA", "title",
                             "TEXT", "vec", "VECTOR", algo, "6", "TYPE", "FLOAT32", "DIM", "4",
                             "DISTANCE_METRIC", metric);
    spec = Indexes_CreateNewSpec(ctx, args, args.size(), &err);
    ASSERT_FALSE(QueryError_HasError(&err)) << QueryError_GetUserError(&err);
    ASSERT_TRUE(spec != nullptr);
  }

  VecSimIndex *vecsim() {
    const FieldSpec *fs = nullptr;
    for (size_t i = 0; i < spec->numFields; ++i) {
      if (spec->fields[i].types & INDEXFLD_T_VECTOR) {
        fs = &spec->fields[i];
        break;
      }
    }
    if (!fs) return nullptr;
    return openVectorIndex(ctx, (FieldSpec *)fs, DONT_CREATE_INDEX);
  }

  t_docId docIdOf(const char *key) {
    uint64_t docId = 0;
    if (DocIdMeta_Get(ctx, RMCK::RString(key), spec->specId, &docId) != REDISMODULE_OK) {
      return 0;
    }
    return (t_docId)docId;
  }

  // A label holds `blob` iff the distance to itself is 0. An absent label yields
  // NaN, which is how these tests tell "moved" from "still there".
  bool labelHolds(t_docId label, const char *blob) {
    VecSimIndex *idx = vecsim();
    if (!idx) return false;
    double d = VecSimIndex_GetDistanceFrom_Unsafe(idx, label, blob);
    return !std::isnan(d) && d == 0.0;
  }

  // The cosine counterpart of `labelHolds`. A cosine index stores the blob normalized
  // and `VecSimIndex_GetDistanceFrom_Unsafe` documents that the caller passes an
  // already-normalized vector, so the probe has to normalize a copy -- and then compare
  // with a tolerance, since the normalize-store-compare round trip is not exact. That
  // inexactness is the reason the product code refuses to compare cosine vectors at
  // all; here it only has to separate two vectors 0.94 apart.
  bool labelHoldsNormalized(t_docId label, const char *blob) {
    VecSimIndex *idx = vecsim();
    if (!idx) return false;
    std::array<float, 4> normalized;
    memcpy(normalized.data(), blob, sizeof(normalized));
    VecSim_Normalize(normalized.data(), normalized.size(), VecSimType_FLOAT32);
    double d = VecSimIndex_GetDistanceFrom_Unsafe(idx, label, normalized.data());
    return !std::isnan(d) && std::fabs(d) < 1e-6;
  }

  bool labelAbsent(t_docId label) {
    VecSimIndex *idx = vecsim();
    if (!idx) return true;
    return std::isnan(VecSimIndex_GetDistanceFrom_Unsafe(idx, label, kVecA));
  }

  // Index `key` for the first time, with no change set.
  t_docId indexFresh(const char *key, const char *title, const char *blob) {
    RMCK::hset(ctx, key, "title", title);
    RMCK::hset(ctx, key, "vec", blob, false);
    EXPECT_EQ(IndexSpec_UpdateDoc(spec, ctx, RMCK::RString(key), DocumentType_Hash, nullptr,
                                  nullptr, 0),
              REDISMODULE_OK);
    return docIdOf(key);
  }

  // Re-index `key`, declaring exactly `changed` as the modified fields.
  //
  // The strings are built directly rather than via RMCK::RString: that is a
  // scope guard whose destructor frees the string, so collecting temporaries
  // into a vector would leave it holding freed pointers.
  t_docId reindexWithChangeSet(const char *key, const std::vector<std::string> &changed) {
    std::vector<RedisModuleString *> fields;
    for (const std::string &f : changed) {
      fields.push_back(RedisModule_CreateString(nullptr, f.c_str(), f.size()));
    }
    int rc = IndexSpec_UpdateDoc(spec, ctx, RMCK::RString(key), DocumentType_Hash, nullptr,
                                 fields.empty() ? nullptr : fields.data(), fields.size());
    for (RedisModuleString *f : fields) {
      RedisModule_FreeString(nullptr, f);
    }
    EXPECT_EQ(rc, REDISMODULE_OK);
    return docIdOf(key);
  }
  // A schema with two vector fields on different backends. Relabeling is decided
  // per field, so the interesting case is one vector changing while the other does
  // not; the single-vector helpers above cannot express it.
  void createTwoVectorIndex() {
    QueryError err = QueryError_Default();
    RMCK::ArgvList args(ctx, "FT.CREATE", indexName.c_str(), "ON", "HASH", "SCHEMA", "title",
                        "TEXT", "v_flat", "VECTOR", "FLAT", "6", "TYPE", "FLOAT32", "DIM", "4",
                        "DISTANCE_METRIC", "L2", "v_hnsw", "VECTOR", "HNSW", "6", "TYPE",
                        "FLOAT32", "DIM", "4", "DISTANCE_METRIC", "L2");
    spec = Indexes_CreateNewSpec(ctx, args, args.size(), &err);
    ASSERT_FALSE(QueryError_HasError(&err)) << QueryError_GetUserError(&err);
    ASSERT_TRUE(spec != nullptr);
  }

  VecSimIndex *vecsimNamed(const std::string &field) {
    for (size_t i = 0; i < spec->numFields; ++i) {
      if (!(spec->fields[i].types & INDEXFLD_T_VECTOR)) continue;
      if (!HiddenString_CompareC(spec->fields[i].fieldName, field.c_str(), field.size())) {
        return openVectorIndex(ctx, &spec->fields[i], DONT_CREATE_INDEX);
      }
    }
    return nullptr;
  }

  bool namedLabelHolds(const std::string &field, t_docId label, const char *blob) {
    VecSimIndex *idx = vecsimNamed(field);
    if (!idx) return false;
    double d = VecSimIndex_GetDistanceFrom_Unsafe(idx, label, blob);
    return !std::isnan(d) && d == 0.0;
  }

  bool namedLabelAbsent(const std::string &field, t_docId label) {
    VecSimIndex *idx = vecsimNamed(field);
    return !idx || std::isnan(VecSimIndex_GetDistanceFrom_Unsafe(idx, label, kVecA));
  }
};

// The optimization itself: text changed, vector did not, so the doc takes a new
// doc-id and the existing vector entry moves onto it.
TEST_F(VectorRelabelTest, textChangeRelabelsUnchangedVector) {
  createIndex(nullptr);
  t_docId first = indexFresh("doc:1", "hello", kVecA);
  ASSERT_NE(first, 0);
  ASSERT_TRUE(labelHolds(first, kVecA));
  size_t sizeBefore = VecSimIndex_IndexSize(vecsim());

  RMCK::hset(ctx, "doc:1", "title", "goodbye");
  t_docId second = reindexWithChangeSet("doc:1", {"title"});

  ASSERT_NE(second, 0);
  EXPECT_NE(second, first) << "an append-only field changed, so the doc-id must advance";
  EXPECT_TRUE(labelHolds(second, kVecA)) << "the vector should have moved to the new doc-id";
  EXPECT_TRUE(labelAbsent(first)) << "the old label must not survive the relabel";
  // A relabel rewrites bookkeeping only, so the stored vector count is unchanged.
  EXPECT_EQ(VecSimIndex_IndexSize(vecsim()), sizeBefore);
}

// The vector itself changed, so there is new data to store and relabeling cannot
// serve it: the entry is re-added under the new doc-id.
TEST_F(VectorRelabelTest, vectorChangeReAddsUnderNewDocId) {
  createIndex(nullptr);
  t_docId first = indexFresh("doc:1", "hello", kVecA);
  ASSERT_NE(first, 0);

  RMCK::hset(ctx, "doc:1", "vec", kVecB, false);
  t_docId second = reindexWithChangeSet("doc:1", {"vec"});

  ASSERT_NE(second, 0);
  EXPECT_TRUE(labelHolds(second, kVecB)) << "the new blob must be indexed";
  EXPECT_FALSE(labelHolds(second, kVecA)) << "the stale blob must not be what is stored";
  EXPECT_TRUE(labelAbsent(first));
}

// Both changed: the vector is re-added, not relabeled, because its data moved.
TEST_F(VectorRelabelTest, textAndVectorChangeReAdds) {
  createIndex(nullptr);
  t_docId first = indexFresh("doc:1", "hello", kVecA);
  ASSERT_NE(first, 0);

  RMCK::hset(ctx, "doc:1", "title", "goodbye");
  RMCK::hset(ctx, "doc:1", "vec", kVecB, false);
  t_docId second = reindexWithChangeSet("doc:1", {"title", "vec"});

  ASSERT_NE(second, 0);
  EXPECT_TRUE(labelHolds(second, kVecB));
  EXPECT_TRUE(labelAbsent(first));
}

// With no change set -- the path every JSON write and every background scan takes --
// the question is settled by comparing the field's new value against what the index
// holds. An unchanged value still ends up under the new doc-id.
//
// Whether it got there by a move or a re-add is not observable from here: for an
// unchanged blob the two are identical in label, distance and count, and the
// tombstone route needs a tiered index (workers are disabled in these tests). The
// pytest suite pins the move itself via FT.INFO's `marked_deleted`.
TEST_F(VectorRelabelTest, unknownChangeSetKeepsUnchangedVector) {
  createIndex(nullptr);
  t_docId first = indexFresh("doc:1", "hello", kVecA);
  ASSERT_NE(first, 0);
  size_t sizeBefore = VecSimIndex_IndexSize(vecsim());

  RMCK::hset(ctx, "doc:1", "title", "goodbye");
  t_docId second = reindexWithChangeSet("doc:1", {});  // NULL change set

  ASSERT_NE(second, 0);
  EXPECT_NE(second, first);
  EXPECT_TRUE(labelHolds(second, kVecA));
  EXPECT_TRUE(labelAbsent(first));
  EXPECT_EQ(VecSimIndex_IndexSize(vecsim()), sizeBefore) << "no orphan at the old label";
}

// A GEOSHAPE field ordered before the VECTOR field in the schema, so it is also processed first
// in `bulkIndexFields`. The vector is absent from the change set, so it is marked to keep its
// old label for a relabel -- but the WKT is only validated in `geometryIndexer`, the bulk-add
// step, which runs after doc-id assignment (and the relabel mark) already happened. An invalid
// update fails there and `bulkIndexFields` bails before ever reaching the vector applier that
// would consume the mark. The old label must be dropped by the caller instead of stranded under
// a doc-id nothing else refers to.
TEST_F(VectorRelabelTest, earlierFieldFailureAbandonsPendingRelabel) {
  QueryError err = QueryError_Default();
  RMCK::ArgvList args(ctx, "FT.CREATE", indexName.c_str(), "ON", "HASH", "SCHEMA", "title", "TEXT",
                      "geom", "GEOSHAPE", "FLAT", "vec", "VECTOR", "FLAT", "6", "TYPE", "FLOAT32",
                      "DIM", "4", "DISTANCE_METRIC", "L2");
  spec = Indexes_CreateNewSpec(ctx, args, args.size(), &err);
  ASSERT_FALSE(QueryError_HasError(&err)) << QueryError_GetUserError(&err);
  ASSERT_TRUE(spec != nullptr);

  RMCK::hset(ctx, "doc:1", "title", "hello");
  RMCK::hset(ctx, "doc:1", "geom", "POLYGON((1 1, 1 100, 100 100, 100 1, 1 1))");
  RMCK::hset(ctx, "doc:1", "vec", kVecA, false);
  ASSERT_EQ(IndexSpec_UpdateDoc(spec, ctx, RMCK::RString("doc:1"), DocumentType_Hash, nullptr,
                                nullptr, 0),
            REDISMODULE_OK);
  t_docId first = docIdOf("doc:1");
  ASSERT_NE(first, 0);
  ASSERT_TRUE(labelHolds(first, kVecA));

  // Too few points -- valid WKT syntax, but `geometryIndexer` rejects the geometry itself.
  RMCK::hset(ctx, "doc:1", "geom", "POLYGON((1 1, 1 100, 1 1))");
  RedisModuleString *changed = RedisModule_CreateString(nullptr, "geom", 4);
  int rc = IndexSpec_UpdateDoc(spec, ctx, RMCK::RString("doc:1"), DocumentType_Hash, nullptr,
                               &changed, 1);
  RedisModule_FreeString(nullptr, changed);
  EXPECT_EQ(rc, REDISMODULE_OK) << "a per-field error does not fail the call itself";

  EXPECT_TRUE(labelAbsent(first))
      << "the old label must not survive when the field that would have relabeled it never ran";
}

// The comparison is what makes the no-change-set path safe, and this is the test that
// exercises it: the vector really did change, and nothing external says so. Moving the
// entry here would leave the *old* blob under the new doc-id, so a KNN query would
// return a vector the document no longer has.
TEST_F(VectorRelabelTest, unknownChangeSetWithChangedVectorReAdds) {
  createIndex(nullptr);
  t_docId first = indexFresh("doc:1", "hello", kVecA);
  ASSERT_NE(first, 0);

  RMCK::hset(ctx, "doc:1", "vec", kVecB, false);
  t_docId second = reindexWithChangeSet("doc:1", {});  // NULL change set

  ASSERT_NE(second, 0);
  EXPECT_TRUE(labelHolds(second, kVecB)) << "the document's current value must be indexed";
  EXPECT_FALSE(labelHolds(second, kVecA))
      << "the old blob here means the entry was moved without checking whether it changed";
  EXPECT_TRUE(labelAbsent(first));
  EXPECT_EQ(VecSimIndex_IndexSize(vecsim()), 1u);
}

// Cosine is the case a distance-based comparison could not serve, and the byte one can.
// The index stores the blob normalized, so the comparison has to normalize before comparing
// -- `VecSimIndexAbstract::holdsVector` runs the blob through `preprocessForStorage` for
// exactly that reason. Here the vector genuinely changed, so it must be re-added.
TEST_F(VectorRelabelTest, unknownChangeSetOnCosineIndexDetectsChange) {
  createIndex(nullptr, "FLAT", "COSINE");
  t_docId first = indexFresh("doc:1", "hello", kVecA);
  ASSERT_NE(first, 0);

  RMCK::hset(ctx, "doc:1", "vec", kVecC, false);
  t_docId second = reindexWithChangeSet("doc:1", {});

  ASSERT_NE(second, 0);
  EXPECT_TRUE(labelHoldsNormalized(second, kVecC)) << "the document's current value must be indexed";
  EXPECT_FALSE(labelHoldsNormalized(second, kVecA))
      << "the old blob here means the comparison missed a changed vector";
  EXPECT_TRUE(labelAbsent(first));
  EXPECT_EQ(VecSimIndex_IndexSize(vecsim()), 1u);
}

// The primitive the unverified path rests on, tested directly rather than through its effect.
//
// Worth pinning separately because every test above is outcome-based, and for an unchanged
// vector the two paths produce identical index contents -- so nothing else here would notice
// `VectorIndex_HoldsVectors` answering true for a vector that changed, or false for one that
// did not. Cosine is the interesting case: the stored form is normalized, so a comparison
// against the raw blob would say "changed" every time and silently cost the optimization.
TEST_F(VectorRelabelTest, holdsVectorComparesTheStoredForm) {
  createIndex(nullptr, "FLAT", "COSINE");
  t_docId first = indexFresh("doc:1", "hello", kVecA);
  ASSERT_NE(first, 0);

  EXPECT_TRUE(VectorIndex_HoldsVectors(vecsim(), first, kVecA, 1))
      << "the stored vector is the normalized kVecA, so the raw blob has to be normalized too";
  EXPECT_FALSE(VectorIndex_HoldsVectors(vecsim(), first, kVecC, 1));
  EXPECT_FALSE(VectorIndex_HoldsVectors(vecsim(), first + 1000, kVecA, 1))
      << "an absent label holds nothing";
  EXPECT_FALSE(VectorIndex_HoldsVectors(vecsim(), first, kVecA, 2))
      << "a label holding one vector does not hold two";
}

// L2 needs no preprocessing, so this is the same primitive with the transformation removed --
// and the pair rules out a comparison that only works because normalization happens to be a
// no-op, or only when it is not.
TEST_F(VectorRelabelTest, holdsVectorOnL2Index) {
  createIndex(nullptr);
  t_docId first = indexFresh("doc:1", "hello", kVecA);
  ASSERT_NE(first, 0);

  EXPECT_TRUE(VectorIndex_HoldsVectors(vecsim(), first, kVecA, 1));
  EXPECT_FALSE(VectorIndex_HoldsVectors(vecsim(), first, kVecB, 1));
}

// An SVS schema field is created as a *tiered* index (see `spec.c`), so with workers disabled
// nothing is ever ingested and the comparison is answered from the flat buffer -- which is a
// brute-force index like any other. SVS's own accessor reports nothing, by design: it keeps
// vectors in the SVS library's reduced form and cannot hand them back.
//
// So this covers the tiered read, not SVS storage, and what it pins is that the comparison and
// the relabel refusal are independent: a match here still ends in delete + re-add, because
// relabeling is what SVS does not implement. `svsRefusalFallsBackToDeleteAndAdd` covers that
// half. Conflating the two is the easy mistake.
TEST_F(VectorRelabelTest, holdsVectorOnSvsIndexAnswersFromTheFlatBuffer) {
  createIndex(nullptr, "SVS-VAMANA");
  t_docId first = indexFresh("doc:1", "hello", kVecA);
  ASSERT_NE(first, 0);

  EXPECT_TRUE(VectorIndex_HoldsVectors(vecsim(), first, kVecA, 1));
  EXPECT_FALSE(VectorIndex_HoldsVectors(vecsim(), first, kVecB, 1));
}

// A field the schema does not index cannot force a reindex of the vector.
TEST_F(VectorRelabelTest, nonSchemaFieldChangeRelabels) {
  createIndex(nullptr);
  t_docId first = indexFresh("doc:1", "hello", kVecA);
  ASSERT_NE(first, 0);

  RMCK::hset(ctx, "doc:1", "untracked", "whatever");
  t_docId second = reindexWithChangeSet("doc:1", {"untracked"});

  ASSERT_NE(second, 0);
  EXPECT_TRUE(labelHolds(second, kVecA));
  EXPECT_TRUE(labelAbsent(first));
}

// With `AS`, the change set names the hash field (`vec`) while the schema knows the field by
// its alias. Classifying on the alias would read a changed vector as unchanged and relabel a
// stale entry, so the blob at the new doc-id would be the old one.
TEST_F(VectorRelabelTest, aliasedVectorChangeIsDetected) {
  createIndex("embedding");
  t_docId first = indexFresh("doc:1", "hello", kVecA);
  ASSERT_NE(first, 0);

  RMCK::hset(ctx, "doc:1", "vec", kVecB, false);
  t_docId second = reindexWithChangeSet("doc:1", {"vec"});

  ASSERT_NE(second, 0);
  EXPECT_TRUE(labelHolds(second, kVecB)) << "aliased vector change must re-add the new blob";
  EXPECT_FALSE(labelHolds(second, kVecA)) << "a stale blob here means the alias was mismatched";
}

// Pins which primitive ran on the *verified* path, not just where the vector ended up.
//
// For a genuinely unchanged blob a relabel and a delete + re-add are indistinguishable --
// same label, same distance, same count. This separates them by feeding a change set that
// deliberately understates what changed: the hash's vector is rewritten to a different blob
// while the change set names only `title`. The two paths then diverge in what the new label
// holds:
//
//   relabel       -> moves the existing entry, so the label holds the OLD blob
//   delete+re-add -> re-reads the document, so the label holds the CURRENT blob
//
// The input is intentionally inconsistent -- a real notification would list `vec` -- and it is
// not modelling a reachable state. It is the only way to observe the choice from outside,
// since the tombstone route needs a tiered index (plain HNSW's deleteVector removes in place,
// marking nothing) and so is unavailable with workers disabled.
//
// It is also what separates a verified mark from an unverified one: on the unverified path the
// blob comparison would notice the rewrite and re-add.
TEST_F(VectorRelabelTest, relabelMovesTheExistingEntryRatherThanReReading) {
  createIndex(nullptr, "HNSW");
  t_docId first = indexFresh("doc:1", "hello", kVecA);
  ASSERT_NE(first, 0);
  ASSERT_TRUE(labelHolds(first, kVecA));

  // Rewrite the stored vector but claim only `title` changed.
  RMCK::hset(ctx, "doc:1", "title", "goodbye");
  RMCK::hset(ctx, "doc:1", "vec", kVecB, false);
  t_docId second = reindexWithChangeSet("doc:1", {"title"});

  ASSERT_NE(second, 0);
  ASSERT_NE(second, first);
  EXPECT_TRUE(labelHolds(second, kVecA))
      << "the new label should hold the moved entry; the current document value here would mean "
         "the vector was re-read and re-added instead of relabeled";
  EXPECT_FALSE(labelHolds(second, kVecB));
  EXPECT_TRUE(labelAbsent(first));

  // Control on the same backend: name the vector in the change set and the value is re-read,
  // proving the probe above can distinguish the two paths at all rather than being insensitive
  // to both.
  t_docId third = reindexWithChangeSet("doc:1", {"vec"});
  ASSERT_NE(third, 0);
  EXPECT_TRUE(labelHolds(third, kVecB)) << "a declared vector change must re-read the document";
  EXPECT_FALSE(labelHolds(third, kVecA));
}

// SVS does not implement relabeling, so `VecSimIndex_RelabelVector` refuses with
// `VecSimRelabel_Unsupported`. `Indexer_HandleReplacedDocVectorAndGeometry` has already
// skipped this field's delete on the strength of the unchanged mark, so the refusal
// has to perform it before letting the insert run -- otherwise the old entry is
// orphaned at a doc-id no document owns, and a KNN query can return it.
//
// The index-size assertion is what catches that: an orphan plus the re-add
// leaves two stored vectors for one document.
TEST_F(VectorRelabelTest, svsRefusalFallsBackToDeleteAndAdd) {
  createIndex(nullptr, "SVS-VAMANA");
  t_docId first = indexFresh("doc:1", "hello", kVecA);
  ASSERT_NE(first, 0);
  ASSERT_TRUE(labelHolds(first, kVecA));
  ASSERT_EQ(VecSimIndex_IndexSize(vecsim()), 1u);

  // Pin the premise. Without it this test passes whether the relabel was refused
  // or succeeded, because an unchanged blob leaves identical state either way --
  // so it would stop covering the fallback the moment SVS gained relabel support,
  // silently. Probing with an unused target label mutates nothing on refusal.
  ASSERT_EQ(VecSimIndex_RelabelVector(vecsim(), first, first + 1000),
            VecSimRelabel_Unsupported)
      << "premise: SVS does not implement relabeling. If this now succeeds, this test no "
         "longer exercises the refusal fallback and needs a different backend.";

  RMCK::hset(ctx, "doc:1", "title", "goodbye");
  t_docId second = reindexWithChangeSet("doc:1", {"title"});

  ASSERT_NE(second, 0);
  EXPECT_NE(second, first);
  EXPECT_TRUE(labelHolds(second, kVecA)) << "the vector must still be reachable at the new doc-id";
  EXPECT_TRUE(labelAbsent(first)) << "the refused relabel must not leave the old entry behind";
  EXPECT_EQ(VecSimIndex_IndexSize(vecsim()), 1u)
      << "an orphan at the old label would make this 2";
}

// The gate. With OPTIMIZE_UPDATE_VEC off, a text-only change must take the
// pre-existing delete + re-add path even though the change set proves the vector
// is unchanged. Uses the same understated-change-set probe as
// `relabelMovesTheExistingEntryRatherThanReReading`, so the two are direct
// opposites on identical input: there, the new label holds the moved (old) blob;
// here it must hold the document's current one, because the vector was re-read.
TEST_F(VectorRelabelTest, relabelRequiresOptimizeUpdateVec) {
  RSGlobalConfig.optimizeUpdateVec = false;  // restored by TearDown

  createIndex(nullptr, "HNSW");
  t_docId first = indexFresh("doc:1", "hello", kVecA);
  ASSERT_NE(first, 0);

  RMCK::hset(ctx, "doc:1", "title", "goodbye");
  RMCK::hset(ctx, "doc:1", "vec", kVecB, false);
  t_docId second = reindexWithChangeSet("doc:1", {"title"});

  ASSERT_NE(second, 0);
  EXPECT_TRUE(labelHolds(second, kVecB))
      << "with the flag off the vector must be re-read and re-added, not relabeled";
  EXPECT_FALSE(labelHolds(second, kVecA));
  EXPECT_TRUE(labelAbsent(first));
}

// The mirror of the above: on an aliased schema a text-only change must still
// relabel, so the alias fix cannot have been made by simply never matching.
TEST_F(VectorRelabelTest, aliasedTextChangeRelabels) {
  createIndex("embedding");
  t_docId first = indexFresh("doc:1", "hello", kVecA);
  ASSERT_NE(first, 0);
  size_t sizeBefore = VecSimIndex_IndexSize(vecsim());

  RMCK::hset(ctx, "doc:1", "title", "goodbye");
  t_docId second = reindexWithChangeSet("doc:1", {"title"});

  ASSERT_NE(second, 0);
  EXPECT_NE(second, first);
  EXPECT_TRUE(labelHolds(second, kVecA));
  EXPECT_TRUE(labelAbsent(first));
  EXPECT_EQ(VecSimIndex_IndexSize(vecsim()), sizeBefore);
}

// Per-field granularity: one vector changed, the other did not, so within a single
// update one field is re-added and the other is relabeled. A whole-document
// decision would get one of the two wrong -- either re-adding a vector that never
// changed, or moving a stale entry for the one that did.
//
// Both hash values are rewritten while the change set names only `v_flat`, which
// is the probe from `relabelMovesTheExistingEntryRatherThanReReading`: the
// re-added field ends up holding the current blob, the relabeled one the old.
TEST_F(VectorRelabelTest, perFieldRelabelWithTwoVectorFields) {
  createTwoVectorIndex();

  RMCK::hset(ctx, "doc:1", "title", "hello");
  RMCK::hset(ctx, "doc:1", "v_flat", kVecA, false);
  RMCK::hset(ctx, "doc:1", "v_hnsw", kVecA, false);
  ASSERT_EQ(IndexSpec_UpdateDoc(spec, ctx, RMCK::RString("doc:1"), DocumentType_Hash, nullptr,
                                nullptr, 0),
            REDISMODULE_OK);
  t_docId first = docIdOf("doc:1");
  ASSERT_NE(first, 0);
  ASSERT_TRUE(namedLabelHolds("v_flat", first, kVecA));
  ASSERT_TRUE(namedLabelHolds("v_hnsw", first, kVecA));

  RMCK::hset(ctx, "doc:1", "v_flat", kVecB, false);
  RMCK::hset(ctx, "doc:1", "v_hnsw", kVecB, false);
  t_docId second = reindexWithChangeSet("doc:1", {"v_flat"});

  ASSERT_NE(second, 0);
  ASSERT_NE(second, first);
  EXPECT_TRUE(namedLabelHolds("v_flat", second, kVecB))
      << "the declared change must be re-read and re-added";
  EXPECT_TRUE(namedLabelHolds("v_hnsw", second, kVecA))
      << "the undeclared field must be moved, not re-read";
  EXPECT_TRUE(namedLabelAbsent("v_flat", first));
  EXPECT_TRUE(namedLabelAbsent("v_hnsw", first));
  EXPECT_EQ(VecSimIndex_IndexSize(vecsimNamed("v_flat")), 1u);
  EXPECT_EQ(VecSimIndex_IndexSize(vecsimNamed("v_hnsw")), 1u);
}
