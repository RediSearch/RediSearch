/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

// Tests for the reindex skip in `Indexes_UpdateMatchingWithSchemaRules`.
//
// Subkey notifications name the fields a command wrote, which lets an update that touches
// nothing the index reads be dropped instead of reindexing the document. Every condition
// guarding that decision, wrong in the permissive direction, silently loses indexed data,
// and the failure is invisible until someone queries for what went missing. So each
// condition gets its own test.
//
// These drive the public dispatcher rather than the static gate itself, which also covers
// the wiring: a gate that is correct but never consulted, or consulted with the wrong
// arguments, fails here too.
//
// The observable throughout is the doc-id. Indexing a document again gives it a new one, so
// an unchanged doc-id means the update was skipped and a higher one means it was not.

#include "gtest/gtest.h"
#include "redismock/redismock.h"
#include "redismock/util.h"

#include "spec.h"
#include "indexes.h"
#include "doc_id_meta.h"

#include <string>
#include <vector>

class ReindexSkipTest : public ::testing::Test {
protected:
  RedisModuleCtx *ctx = nullptr;
  IndexSpec *spec = nullptr;
  std::string indexName;

  void SetUp() override {
    ctx = RedisModule_GetThreadSafeContext(nullptr);
    RMCK::flushdb(ctx);
    static int counter = 0;
    indexName = "skipidx" + std::to_string(++counter);
  }

  void TearDown() override {
    if (ctx) {
      RedisModule_FreeThreadSafeContext(ctx);
      ctx = nullptr;
    }
  }

  // `extraArgs` go between the index name and SCHEMA, for rule options such as FILTER or
  // SCORE_FIELD. The schema is always a single TEXT field named `title`.
  void createIndex(const std::vector<std::string> &extraArgs = {}) {
    std::vector<std::string> args = {"FT.CREATE", indexName, "ON", "HASH"};
    args.insert(args.end(), extraArgs.begin(), extraArgs.end());
    args.insert(args.end(), {"SCHEMA", "title", "TEXT"});

    QueryError err = QueryError_Default();
    RMCK::ArgvList argv(ctx, args);
    spec = Indexes_CreateNewSpec(ctx, argv, argv.size(), &err);
    ASSERT_FALSE(QueryError_HasError(&err)) << QueryError_GetUserError(&err);
    ASSERT_TRUE(spec != nullptr);
  }

  t_docId docIdOf(const char *key) {
    uint64_t docId = 0;
    if (DocIdMeta_Get(ctx, RMCK::RString(key), spec->specId, &docId) != REDISMODULE_OK) {
      return 0;
    }
    return (t_docId)docId;
  }

  // Run the update the way a keyspace notification would, naming `changed` as the fields the
  // command wrote. An empty `changed` means no change set at all -- what a server without
  // subkey notifications, a JSON document or a background scan delivers -- which is a
  // different statement from a change set that happens to name nothing.
  void notifyUpdate(const char *key, const std::vector<std::string> &changed) {
    std::vector<RedisModuleString *> fields;
    for (const std::string &f : changed) {
      fields.push_back(RedisModule_CreateString(nullptr, f.c_str(), f.size()));
    }
    Indexes_UpdateMatchingWithSchemaRules(ctx, RMCK::RString(key), DocumentType_Hash,
                                         fields.empty() ? nullptr : fields.data(), fields.size());
    for (RedisModuleString *f : fields) {
      RedisModule_FreeString(nullptr, f);
    }
  }
};

// A field the schema does not mention cannot change anything the index holds for a document
// it already has, so the update is dropped. This is the one case the skip exists for; every
// other test here is a case where it must not fire.
TEST_F(ReindexSkipTest, unindexedFieldChangeIsSkipped) {
  createIndex();
  RMCK::hset(ctx, "doc:1", "title", "hello");
  notifyUpdate("doc:1", {"title"});
  const t_docId first = docIdOf("doc:1");
  ASSERT_NE(first, 0u);

  RMCK::hset(ctx, "doc:1", "unread", "x");
  notifyUpdate("doc:1", {"unread"});
  EXPECT_EQ(docIdOf("doc:1"), first);
}

TEST_F(ReindexSkipTest, schemaFieldChangeReindexes) {
  createIndex();
  RMCK::hset(ctx, "doc:1", "title", "hello");
  notifyUpdate("doc:1", {"title"});
  const t_docId first = docIdOf("doc:1");

  RMCK::hset(ctx, "doc:1", "title", "goodbye");
  notifyUpdate("doc:1", {"title"});
  EXPECT_GT(docIdOf("doc:1"), first);
}

// A document the index does not hold yet has to be indexed however little the write touched:
// a hash matching the prefix is a document whether or not it carries an indexed field, and
// `*`, result counts and `ismissing()` all depend on it being registered.
TEST_F(ReindexSkipTest, unseenDocumentIsIndexedEvenWithNoIndexedFieldChanged) {
  createIndex();
  RMCK::hset(ctx, "doc:1", "unread", "x");
  notifyUpdate("doc:1", {"unread"});
  EXPECT_NE(docIdOf("doc:1"), 0u) << "a document absent from the index must be indexed";
}

// A rule FILTER may test a field the schema never mentions, and its verdict flips when that
// field changes, so a spec carrying one can never skip. Here the verdict does not flip: the
// document stays indexed, and the proof the update was not dropped is a new doc-id.
TEST_F(ReindexSkipTest, filterExpressionDisablesTheSkip) {
  createIndex({"FILTER", "@indexme!='no'"});
  RMCK::hset(ctx, "doc:1", "title", "hello");
  RMCK::hset(ctx, "doc:1", "indexme", "yes");
  notifyUpdate("doc:1", {"title", "indexme"});
  const t_docId first = docIdOf("doc:1");
  ASSERT_NE(first, 0u);

  // `indexme` is not in the schema, so without the FILTER check this would be skipped.
  // Still not "no", so the document remains a match either way.
  RMCK::hset(ctx, "doc:1", "indexme", "maybe");
  notifyUpdate("doc:1", {"indexme"});
  EXPECT_GT(docIdOf("doc:1"), first);
}

// The write makes the rule stop matching, so the document has to leave the index.
//
// Unlike its neighbours this one does not exercise the gate, and deliberately survives having
// every guard in it removed: a rule that no longer matches sends the dispatcher down its
// SpecOp_Del branch, which is not gated on the change set at all. What it pins is that the
// branch stays ungated -- gating a delete on "no indexed field changed" would leave the
// document queryable on the strength of a filter that no longer holds.
TEST_F(ReindexSkipTest, filterTurningFalseRemovesTheDocument) {
  createIndex({"FILTER", "@indexme!='no'"});
  RMCK::hset(ctx, "doc:1", "title", "hello");
  RMCK::hset(ctx, "doc:1", "indexme", "yes");
  notifyUpdate("doc:1", {"title", "indexme"});
  ASSERT_NE(docIdOf("doc:1"), 0u);

  RMCK::hset(ctx, "doc:1", "indexme", "no");
  notifyUpdate("doc:1", {"indexme"});
  EXPECT_EQ(docIdOf("doc:1"), 0u) << "the rule no longer matches, so the document must be gone";
}

// SCORE_FIELD, LANGUAGE_FIELD and PAYLOAD_FIELD are read off the document without appearing
// in the schema. Three independent comparisons in the gate, so three cases: dropping any one
// of them leaves the doc-table entry holding a stale score, language or payload.
TEST_F(ReindexSkipTest, ruleFieldChangeReindexes) {
  createIndex({"SCORE_FIELD", "__score", "LANGUAGE_FIELD", "__language", "PAYLOAD_FIELD",
               "__payload"});
  RMCK::hset(ctx, "doc:1", "title", "hello");
  RMCK::hset(ctx, "doc:1", "__score", "1");
  RMCK::hset(ctx, "doc:1", "__language", "english");
  RMCK::hset(ctx, "doc:1", "__payload", "p0");
  notifyUpdate("doc:1", {"title"});
  t_docId previous = docIdOf("doc:1");
  ASSERT_NE(previous, 0u);

  // Values a document would plausibly carry: a bad language logs a warning and tells us
  // nothing about the skip.
  const std::vector<std::pair<const char *, const char *>> writes = {
      {"__score", "0.5"}, {"__language", "french"}, {"__payload", "p1"}};
  for (const auto &[field, value] : writes) {
    RMCK::hset(ctx, "doc:1", field, value);
    notifyUpdate("doc:1", {field});
    const t_docId now = docIdOf("doc:1");
    EXPECT_GT(now, previous) << "writing " << field << " must reindex the document";
    previous = now;
  }
}

// No change set is not a statement that nothing changed -- it is the absence of one, and the
// document has to be reindexed. This is the path JSON, background scans and servers without
// subkey notifications take.
TEST_F(ReindexSkipTest, absentChangeSetNeverSkips) {
  createIndex();
  RMCK::hset(ctx, "doc:1", "title", "hello");
  notifyUpdate("doc:1", {});
  const t_docId first = docIdOf("doc:1");
  ASSERT_NE(first, 0u);

  RMCK::hset(ctx, "doc:1", "unread", "x");
  notifyUpdate("doc:1", {});
  EXPECT_GT(docIdOf("doc:1"), first) << "without a change set there is nothing to skip on";
}
