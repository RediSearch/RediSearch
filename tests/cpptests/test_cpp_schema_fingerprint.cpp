/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#include "gtest/gtest.h"
#include "common.h"
#include "spec.h"
#include "synonym_map.h"
#include "util/hash/hash.h"

#include <initializer_list>
#include <vector>

class SchemaFingerprintTest : public ::testing::Test {
 protected:
  void TearDown() override {
    for (auto &ref : specs) {
      IndexSpec_RemoveFromGlobals(ref, false);
    }
    specs.clear();
  }

  IndexSpec *parse(const char *name, std::initializer_list<const char *> args) {
    QueryError err = QueryError_Default();
    std::vector<const char *> argv(args);
    StrongRef ref = IndexSpec_ParseC(NULL, name, argv.data(), argv.size(), &err);
    IndexSpec *sp = (IndexSpec *)StrongRef_Get(ref);
    if (QueryError_HasError(&err) || sp == nullptr) {
      ADD_FAILURE() << "could not parse " << name << ": " << QueryError_GetUserError(&err);
      return nullptr;
    }
    Spec_AddToDict(ref.rm);
    specs.push_back(ref);
    return sp;
  }

  static uint64_t fp(const IndexSpec *sp) {
    uint64_t out = 0;
    if (sp == nullptr) {
      ADD_FAILURE() << "no spec to fingerprint";
      return out;
    }
    return IndexSpec_SchemaFingerprint(sp);
  }

  std::vector<StrongRef> specs;
};

TEST_F(SchemaFingerprintTest, EqualSchemasHashEqualRegardlessOfName) {
  IndexSpec *a = parse("idx_a", {"SCHEMA", "title", "TEXT", "price", "NUMERIC", "SORTABLE"});
  IndexSpec *b = parse("idx_b", {"SCHEMA", "title", "TEXT", "price", "NUMERIC", "SORTABLE"});
  ASSERT_EQ(fp(a), fp(b));
  ASSERT_EQ(fp(a), fp(a));
}

TEST_F(SchemaFingerprintTest, SchemaChangesChangeFingerprint) {
  const uint64_t base = fp(parse("idx_base", {"SCHEMA", "title", "TEXT"}));

  ASSERT_NE(base, fp(parse("idx_extra_field", {"SCHEMA", "title", "TEXT", "body", "TEXT"})));
  ASSERT_NE(base, fp(parse("idx_other_type", {"SCHEMA", "title", "TAG"})));
  ASSERT_NE(base, fp(parse("idx_sortable", {"SCHEMA", "title", "TEXT", "SORTABLE"})));
  ASSERT_NE(base, fp(parse("idx_weight", {"SCHEMA", "title", "TEXT", "WEIGHT", "2.0"})));
  ASSERT_NE(base, fp(parse("idx_prefix", {"PREFIX", "1", "doc:", "SCHEMA", "title", "TEXT"})));
  ASSERT_NE(base, fp(parse("idx_filter", {"FILTER", "@title != ''", "SCHEMA", "title", "TEXT"})));
  ASSERT_NE(base, fp(parse("idx_other_name", {"SCHEMA", "subject", "TEXT"})));
}

TEST_F(SchemaFingerprintTest, StopwordsContentSensitiveOrderInsensitive) {
  IndexSpec *a = parse("idx_sw_a", {"STOPWORDS", "2", "hello", "world", "SCHEMA", "t", "TEXT"});
  IndexSpec *b = parse("idx_sw_b", {"STOPWORDS", "2", "world", "hello", "SCHEMA", "t", "TEXT"});
  IndexSpec *c = parse("idx_sw_c", {"STOPWORDS", "2", "hello", "there", "SCHEMA", "t", "TEXT"});
  IndexSpec *d = parse("idx_sw_d", {"SCHEMA", "t", "TEXT"});
  ASSERT_EQ(fp(a), fp(b));
  ASSERT_NE(fp(a), fp(c));
  ASSERT_NE(fp(a), fp(d));
}

TEST_F(SchemaFingerprintTest, SynonymMapOrderIndependent) {
  SynonymMap *a = SynonymMap_New(false);
  SynonymMap *b = SynonymMap_New(false);
  const char *wheels[] = {"car", "automobile"};
  const char *speed[] = {"fast", "quick"};
  SynonymMap_Add(a, "g1", wheels, 2);
  SynonymMap_Add(a, "g2", speed, 2);
  SynonymMap_Add(b, "g2", speed, 2);
  SynonymMap_Add(b, "g1", wheels, 2);
  ASSERT_EQ(SynonymMap_Fingerprint(a), SynonymMap_Fingerprint(b));

  SynonymMap *c = SynonymMap_New(false);
  SynonymMap_Add(c, "g1", wheels, 2);
  ASSERT_NE(SynonymMap_Fingerprint(a), SynonymMap_Fingerprint(c));

  SynonymMap_Free(a);
  SynonymMap_Free(b);
  SynonymMap_Free(c);
}

TEST_F(SchemaFingerprintTest, DataDependentStateDoesNotAffectFingerprint) {
  IndexSpec *sp = parse("idx_data", {"SCHEMA", "t", "TEXT"});
  ASSERT_NE(sp, nullptr);
  const uint64_t before = fp(sp);
  // Direct field pokes stand in for a shard that differs only in data state.
  sp->scan_in_progress = true;
  sp->stats.termsSize += 42;
  ASSERT_EQ(before, fp(sp));
}

TEST_F(SchemaFingerprintTest, DoesNotUseRdbSerialization) {
  IndexSpec *sp = parse("idx_direct", {"SCHEMA", "t", "TEXT"});
  ASSERT_NE(sp, nullptr);
  const uint64_t before = fp(sp);
  const auto save = RedisModule_SaveDataTypeToString;
  RedisModule_SaveDataTypeToString = nullptr;
  const uint64_t direct = IndexSpec_SchemaFingerprint(sp);
  RedisModule_SaveDataTypeToString = save;
  ASSERT_EQ(before, direct);
}

TEST_F(SchemaFingerprintTest, VectorConfigurationIsHashedWithoutRuntimeState) {
  IndexSpec *a = parse("idx_vec_a", {"SCHEMA", "v", "VECTOR", "HNSW", "6", "TYPE", "FLOAT32", "DIM",
                                     "8", "DISTANCE_METRIC", "L2"});
  IndexSpec *b = parse("idx_vec_b", {"SCHEMA", "v", "VECTOR", "HNSW", "6", "TYPE", "FLOAT32", "DIM",
                                     "8", "DISTANCE_METRIC", "L2"});
  ASSERT_NE(a, nullptr);
  ASSERT_NE(b, nullptr);
  ASSERT_EQ(fp(a), fp(b));
  auto &params = b->fields[0].vectorOpts.vecSimParams.algoParams.tieredParams;
  auto &hnsw = params.primaryIndexParams->algoParams.hnswParams;
  const uint64_t baseline = fp(a);
  hnsw.M++;
  ASSERT_NE(baseline, fp(b));
  hnsw.M--;
  hnsw.efConstruction++;
  ASSERT_NE(baseline, fp(b));
  hnsw.efConstruction--;
  hnsw.efRuntime++;
  ASSERT_NE(baseline, fp(b));
  hnsw.efRuntime--;
  const double epsilon = hnsw.epsilon;
  hnsw.epsilon += 0.1;
  ASSERT_NE(baseline, fp(b));
  hnsw.epsilon = epsilon;
  params.specificParams.tieredHnswParams.swapJobThreshold++;
  ASSERT_NE(baseline, fp(b));
}

TEST_F(SchemaFingerprintTest, TagGeometryAndRulesAffectFingerprint) {
  const uint64_t tag = fp(parse("idx_tag", {"SCHEMA", "t", "TAG"}));
  ASSERT_NE(tag, fp(parse("idx_sep", {"SCHEMA", "t", "TAG", "SEPARATOR", ";"})));
  ASSERT_NE(tag, fp(parse("idx_case", {"SCHEMA", "t", "TAG", "CASESENSITIVE"})));
  const uint64_t text = fp(parse("idx_text", {"SCHEMA", "t", "TEXT"}));
  ASSERT_NE(text, fp(parse("idx_score", {"SCORE", "0.5", "SCHEMA", "t", "TEXT"})));
  ASSERT_NE(text, fp(parse("idx_lang", {"LANGUAGE", "french", "SCHEMA", "t", "TEXT"})));
  ASSERT_NE(text, fp(parse("idx_score_field", {"SCORE_FIELD", "rank", "SCHEMA", "t", "TEXT"})));
  ASSERT_NE(text, fp(parse("idx_lang_field", {"LANGUAGE_FIELD", "lang", "SCHEMA", "t", "TEXT"})));
  ASSERT_NE(text, fp(parse("idx_payload", {"PAYLOAD_FIELD", "payload", "SCHEMA", "t", "TEXT"})));
  ASSERT_NE(fp(parse("idx_geo1", {"SCHEMA", "shape", "GEOSHAPE", "FLAT"})),
            fp(parse("idx_geo2", {"SCHEMA", "shape", "GEOSHAPE", "SPHERICAL"})));
}

TEST(SchemaHashEncoding, UsesBigEndianNumbersAndStringBoundaries) {
  const auto visit = [](Sha1Context *hash, const void *) {
    Sha1_UpdateU64(hash, 0x0102030405060708ULL);
    Sha1_UpdateBuffer(hash, "ab", 2);
    Sha1_UpdateDouble(hash, 1.0);
  };
  const unsigned char expected[] = {1, 2, 3, 4,   5,   6,    7,    8, 0, 0, 0, 0, 0,
                                    0, 0, 2, 'a', 'b', 0x3f, 0xf0, 0, 0, 0, 0, 0, 0};
  Sha1 sha;
  Sha1_Compute(reinterpret_cast<const char *>(expected), sizeof(expected), &sha);
  ASSERT_EQ(Sha1_ComputeValue(visit, nullptr), Sha1_LeadingU64(&sha));
  const auto strings = [](Sha1Context *hash, const void *value) {
    Sha1_UpdateCString(hash, static_cast<const char *>(value));
  };
  ASSERT_NE(Sha1_ComputeValue(strings, nullptr), Sha1_ComputeValue(strings, ""));
}

TEST_F(SchemaFingerprintTest, FlatAndSvsVectorDefinitionsAreCovered) {
  IndexSpec *flat = parse("idx_flat", {"SCHEMA", "v", "VECTOR", "FLAT", "6", "TYPE", "FLOAT32",
                                       "DIM", "8", "DISTANCE_METRIC", "L2"});
  ASSERT_NE(flat, nullptr);
  const uint64_t flatBefore = fp(flat);
  flat->fields[0].vectorOpts.vecSimParams.algoParams.bfParams.metric = VecSimMetric_IP;
  ASSERT_NE(flatBefore, fp(flat));

  IndexSpec *svs = parse("idx_svs", {"SCHEMA", "v", "VECTOR", "SVS-VAMANA", "6", "TYPE", "FLOAT32",
                                     "DIM", "8", "DISTANCE_METRIC", "L2"});
  ASSERT_NE(svs, nullptr);
  auto &tiered = svs->fields[0].vectorOpts.vecSimParams.algoParams.tieredParams;
  auto &params = tiered.primaryIndexParams->algoParams.svsParams;
  const uint64_t before = fp(svs);
  params.graph_max_degree++;
  ASSERT_NE(before, fp(svs));
  params.graph_max_degree--;
  params.search_window_size++;
  ASSERT_NE(before, fp(svs));
  params.search_window_size--;
  tiered.specificParams.tieredSVSParams.trainingTriggerThreshold++;
  ASSERT_NE(before, fp(svs));
}

TEST_F(SchemaFingerprintTest, OnlyTemporaryTimeoutAffectsFingerprint) {
  IndexSpec *permanent = parse("idx_permanent", {"SCHEMA", "t", "TEXT"});
  ASSERT_NE(permanent, nullptr);
  ASSERT_FALSE(permanent->flags & Index_Temporary);
  const uint64_t fingerprint = fp(permanent);
  permanent->timeout = -1;  // Pre-v13 RDB sentinel for the same permanent schema.
  ASSERT_EQ(fingerprint, fp(permanent));
  permanent->timeout = 0;
  ASSERT_EQ(fingerprint, fp(permanent));

  IndexSpec *temporary = parse("idx_temporary", {"TEMPORARY", "60", "SCHEMA", "t", "TEXT"});
  ASSERT_NE(temporary, nullptr);
  ASSERT_TRUE(temporary->flags & Index_Temporary);
  const uint64_t temporaryFingerprint = fp(temporary);
  ASSERT_NE(fingerprint, temporaryFingerprint);
  temporary->timeout += 1000;
  ASSERT_NE(temporaryFingerprint, fp(temporary));
}

TEST(SchemaHashEncoding, SignedZeroHashesEqually) {
  const auto visit = [](Sha1Context *hash, const void *value) {
    Sha1_UpdateDouble(hash, *static_cast<const double *>(value));
  };
  const double positive = 0.0, negative = -0.0;
  ASSERT_EQ(Sha1_ComputeValue(visit, &positive), Sha1_ComputeValue(visit, &negative));
}

TEST_F(SchemaFingerprintTest, ImplicitLegacyFieldPathMatchesFieldName) {
  IndexSpec *sp = parse("idx_legacy_path", {"SCHEMA", "title", "TEXT"});
  ASSERT_NE(sp, nullptr);
  const uint64_t explicitPath = fp(sp);
  HiddenString *path = sp->fields[0].fieldPath;
  sp->fields[0].fieldPath = nullptr;
  const uint64_t implicitPath = fp(sp);
  sp->fields[0].fieldPath = path;
  ASSERT_EQ(explicitPath, implicitPath);
  ASSERT_NE(explicitPath,
            fp(parse("idx_aliased_path", {"SCHEMA", "source", "AS", "title", "TEXT"})));
}

TEST_F(SchemaFingerprintTest, EmptySynonymMapMatchesAbsentMap) {
  IndexSpec *sp = parse("idx_empty_synonyms", {"SCHEMA", "t", "TEXT"});
  ASSERT_NE(sp, nullptr);
  ASSERT_EQ(sp->smap, nullptr);
  const uint64_t absent = fp(sp);
  IndexSpec_InitializeSynonym(sp);
  ASSERT_TRUE(sp->flags & Index_HasSmap);
  ASSERT_EQ(absent, fp(sp));
  const char *terms[] = {"hello", "hi"};
  SynonymMap_Add(sp->smap, "group", terms, 2);
  ASSERT_NE(absent, fp(sp));
}

TEST_F(SchemaFingerprintTest, RerankOnlyAffectsDiskBackedHnsw) {
  IndexSpec *sp = parse("idx_rerank", {"SCHEMA", "v", "VECTOR", "HNSW", "6", "TYPE", "FLOAT32",
                                       "DIM", "8", "DISTANCE_METRIC", "L2"});
  ASSERT_NE(sp, nullptr);
  ASSERT_EQ(sp->diskSpec, nullptr);
  auto &rerank = sp->fields[0].vectorOpts.diskCtx.rerank;
  rerank = false;
  const uint64_t memoryWithoutRerank = fp(sp);
  rerank = true;
  ASSERT_EQ(memoryWithoutRerank, fp(sp));

  // Fingerprinting only tests diskSpec for nullness; no storage is accessed.
  sp->diskSpec = reinterpret_cast<RedisSearchDiskIndexSpec *>(uintptr_t{1});
  const uint64_t diskWithRerank = fp(sp);
  rerank = false;
  const uint64_t diskWithoutRerank = fp(sp);
  sp->diskSpec = nullptr;
  ASSERT_NE(diskWithRerank, diskWithoutRerank);
}
