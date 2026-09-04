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
#include "indexes.h"
#include "synonym_map.h"

#include <initializer_list>
#include <vector>

class SchemaFingerprintTest : public ::testing::Test {
 protected:
  void TearDown() override {
    for (auto &ref : specs) {
      Indexes_RemoveSpecFromGlobals(ref, false);
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
    EXPECT_TRUE(IndexSpec_SchemaFingerprint(sp, &out));
    return out;
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
