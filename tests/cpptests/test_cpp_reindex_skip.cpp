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
// an unchanged doc-id means the index entries were retained, including metadata-only updates.

#include "gtest/gtest.h"
#include "redismock/redismock.h"
#include "redismock/util.h"

#include "spec.h"
#include "indexes.h"
#include "doc_id_meta.h"
#include "info/index_error.h"
#include "VecSim/vec_sim.h"

// openVectorIndex is declared outside vector_index.h's own extern "C" block.
extern "C" {
#include "vector_index.h"
#include "redis_index.h"
}

#include <cmath>
#include <string>
#include <vector>

class ReindexSkipTest : public ::testing::Test {
protected:
  RedisModuleCtx *ctx = nullptr;
  IndexSpec *spec = nullptr;
  std::string indexName;
  bool previousOptimizePartialUpdate = false;

  void SetUp() override {
    ctx = RedisModule_GetThreadSafeContext(nullptr);
    RMCK::flushdb(ctx);
    static int counter = 0;
    indexName = "skipidx" + std::to_string(++counter);
    // The vector-only fast path is gated behind OPTIMIZE_PARTIAL_UPDATE (on by default).
    // Forced here so a config change elsewhere can't disable it out from under these tests;
    // restored in TearDown, which runs even when an assertion fails.
    previousOptimizePartialUpdate = RSGlobalConfig.optimizePartialUpdate;
    RSGlobalConfig.optimizePartialUpdate = true;
  }

  void TearDown() override {
    RSGlobalConfig.optimizePartialUpdate = previousOptimizePartialUpdate;
    if (ctx) {
      RedisModule_FreeThreadSafeContext(ctx);
      ctx = nullptr;
    }
  }

  // Same schema as `createIndex`, plus a FLAT vector field `vec` (FLOAT32, DIM 4, L2).
  void createIndexWithVector(const std::vector<std::string> &extraArgs = {}) {
    std::vector<std::string> args = {"FT.CREATE", indexName, "ON", "HASH"};
    args.insert(args.end(), extraArgs.begin(), extraArgs.end());
    args.insert(args.end(), {"SCHEMA", "title", "TEXT", "vec", "VECTOR", "FLAT", "6", "TYPE",
                             "FLOAT32", "DIM", "4", "DISTANCE_METRIC", "L2"});

    QueryError err = QueryError_Default();
    RMCK::ArgvList argv(ctx, args);
    spec = Indexes_CreateNewSpec(ctx, argv, argv.size(), &err);
    ASSERT_FALSE(QueryError_HasError(&err)) << QueryError_GetUserError(&err);
    ASSERT_TRUE(spec != nullptr);
  }

  VecSimIndex *vecsim() {
    for (size_t i = 0; i < spec->numFields; ++i) {
      if (spec->fields[i].types & INDEXFLD_T_VECTOR) {
        return openVectorIndex(ctx, &spec->fields[i], DONT_CREATE_INDEX);
      }
    }
    return nullptr;
  }

  // A label holds `blob` iff the distance to itself is 0. An absent label yields NaN.
  bool labelHolds(t_docId label, const char *blob) {
    VecSimIndex *idx = vecsim();
    if (!idx) return false;
    double d = VecSimIndex_GetDistanceFrom_Unsafe(idx, label, blob);
    return !std::isnan(d) && d == 0.0;
  }

  // `extraArgs` go between the index name and SCHEMA, for rule options such as FILTER or
  // SCORE_FIELD. The schema is a single TEXT field fed from the hash field `title`, exposed
  // under `alias` when one is given -- `SCHEMA title AS <alias> TEXT`.
  void createIndex(const std::vector<std::string> &extraArgs = {}, const char *alias = nullptr) {
    std::vector<std::string> args = {"FT.CREATE", indexName, "ON", "HASH"};
    args.insert(args.end(), extraArgs.begin(), extraArgs.end());
    args.insert(args.end(), {"SCHEMA", "title"});
    if (alias) {
      args.insert(args.end(), {"AS", alias});
    }
    args.push_back("TEXT");

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

// A change set names the hash field the command wrote, which is the field's *path*. Comparing
// it against `fieldName` -- the `AS` alias -- finds no match on an aliased schema, so the write
// looks like it touched nothing indexed and the reindex is skipped, leaving the old value
// indexed and queryable. Nothing else in this file uses an alias, so without this the
// comparison could be reverted with every test still passing.
TEST_F(ReindexSkipTest, aliasedSchemaFieldChangeReindexes) {
  createIndex({}, "renamed");
  RMCK::hset(ctx, "doc:1", "title", "hello");
  notifyUpdate("doc:1", {"title"});
  const t_docId first = docIdOf("doc:1");
  ASSERT_NE(first, 0u);

  // The command writes `title`; the schema calls the field `renamed`.
  RMCK::hset(ctx, "doc:1", "title", "goodbye");
  notifyUpdate("doc:1", {"title"});
  EXPECT_GT(docIdOf("doc:1"), first)
      << "a write to an aliased field's path must still reindex the document";
}

// The alias must not become a way to skip either: `renamed` is what the schema calls the
// field, but no hash field is named that, so a change set naming it touches nothing.
TEST_F(ReindexSkipTest, aliasNameInChangeSetIsNotTheFieldPath) {
  createIndex({}, "renamed");
  RMCK::hset(ctx, "doc:1", "title", "hello");
  notifyUpdate("doc:1", {"title"});
  const t_docId first = docIdOf("doc:1");
  ASSERT_NE(first, 0u);

  RMCK::hset(ctx, "doc:1", "renamed", "goodbye");
  notifyUpdate("doc:1", {"renamed"});
  EXPECT_EQ(docIdOf("doc:1"), first)
      << "the alias is not a hash field, so writing it changes nothing indexed";
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

TEST_F(ReindexSkipTest, languageFieldChangeReindexes) {
  createIndex({"SCORE_FIELD", "__score", "LANGUAGE_FIELD", "__language"});
  RMCK::hset(ctx, "doc:1", "title", "hello");
  RMCK::hset(ctx, "doc:1", "__language", "english");
  notifyUpdate("doc:1", {"title", "__language"});
  const t_docId first = docIdOf("doc:1");
  ASSERT_NE(first, 0u);

  RMCK::hset(ctx, "doc:1", "__score", "0.5");
  RMCK::hset(ctx, "doc:1", "__language", "french");
  notifyUpdate("doc:1", {"__score", "__language"});
  EXPECT_GT(docIdOf("doc:1"), first);
}

TEST_F(ReindexSkipTest, scoreAndPayloadChangesPreserveMetadataIdentity) {
  createIndex({"SCORE", "0.25", "SCORE_FIELD", "__score", "PAYLOAD_FIELD", "__payload"});
  RMCK::hset(ctx, "doc:1", "title", "hello");
  notifyUpdate("doc:1", {"title"});
  const t_docId first = docIdOf("doc:1");
  const RSDocumentMetadata *borrowed = DocTable_Borrow(&spec->docs, first);
  ASSERT_NE(borrowed, nullptr);
  const auto docLen = borrowed->docLen;
  const auto maxTermFreq = borrowed->maxTermFreq;
  const auto tableSize = spec->docs.size;
  const auto maxDocId = spec->docs.maxDocId;
  DMD_Return(borrowed);

  auto expectMetadata = [&](double score, const char *payload) {
    EXPECT_EQ(docIdOf("doc:1"), first);
    const RSDocumentMetadata *current = DocTable_Borrow(&spec->docs, first);
    ASSERT_NE(current, nullptr);
    EXPECT_FLOAT_EQ(current->score, score);
    EXPECT_EQ(current->docLen, docLen);
    EXPECT_EQ(current->maxTermFreq, maxTermFreq);
    EXPECT_EQ(spec->docs.size, tableSize);
    EXPECT_EQ(spec->docs.maxDocId, maxDocId);
    if (payload) {
      ASSERT_TRUE(hasPayload(current->flags));
      EXPECT_EQ(std::string(current->payload->data, current->payload->len), payload);
    } else {
      EXPECT_FALSE(hasPayload(current->flags));
    }
    DMD_Return(current);
  };

  RMCK::hset(ctx, "doc:1", "__score", "0.5");
  notifyUpdate("doc:1", {"__score"});
  expectMetadata(0.5, nullptr);

  RMCK::hset(ctx, "doc:1", "__payload", "first");
  notifyUpdate("doc:1", {"__payload"});
  expectMetadata(0.5, "first");

  RMCK::hset(ctx, "doc:1", "__score", "0.75");
  RMCK::hset(ctx, "doc:1", "__payload", "second");
  RMCK::hset(ctx, "doc:1", "unread", "x");
  notifyUpdate("doc:1", {"__payload", "unread", "__score", "__payload"});
  expectMetadata(0.75, "second");
}

TEST_F(ReindexSkipTest, metadataUpdateReadsOnlyChangedFields) {
  createIndex({"SCORE", "0.25", "SCORE_FIELD", "__score", "PAYLOAD_FIELD", "__payload"});
  RMCK::hset(ctx, "doc:1", "title", "hello");
  RMCK::hset(ctx, "doc:1", "__payload", "original");
  notifyUpdate("doc:1", {"title", "__payload"});
  const t_docId first = docIdOf("doc:1");

  // The mock lets the Hash contents and reported change set differ, making an accidental payload
  // read observable without relying on allocator address reuse.
  RMCK::hset(ctx, "doc:1", "__score", "0.5");
  RMCK::hset(ctx, "doc:1", "__payload", "unreported");
  notifyUpdate("doc:1", {"__score"});
  EXPECT_EQ(docIdOf("doc:1"), first);
  const RSDocumentMetadata *dmd = DocTable_Borrow(&spec->docs, first);
  ASSERT_NE(dmd, nullptr);
  EXPECT_FLOAT_EQ(dmd->score, 0.5);
  ASSERT_TRUE(hasPayload(dmd->flags));
  EXPECT_EQ(std::string(dmd->payload->data, dmd->payload->len), "original");
  DMD_Return(dmd);

  RMCK::hset(ctx, "doc:1", "__score", "0.75");
  RMCK::hset(ctx, "doc:1", "__payload", "replacement");
  notifyUpdate("doc:1", {"__payload"});
  EXPECT_EQ(docIdOf("doc:1"), first);
  dmd = DocTable_Borrow(&spec->docs, first);
  ASSERT_NE(dmd, nullptr);
  EXPECT_FLOAT_EQ(dmd->score, 0.5);
  ASSERT_TRUE(hasPayload(dmd->flags));
  EXPECT_EQ(std::string(dmd->payload->data, dmd->payload->len), "replacement");
  DMD_Return(dmd);
}

TEST_F(ReindexSkipTest, scoreChangePreservesMetadataIdentity) {
  createIndex({"SCORE", "0.25", "SCORE_FIELD", "__score"});
  RMCK::hset(ctx, "doc:1", "title", "hello");
  notifyUpdate("doc:1", {"title"});
  const t_docId first = docIdOf("doc:1");
  const RSDocumentMetadata *borrowed = DocTable_Borrow(&spec->docs, first);
  ASSERT_NE(borrowed, nullptr);
  const auto docLen = borrowed->docLen;
  const auto maxTermFreq = borrowed->maxTermFreq;
  const auto tableSize = spec->docs.size;
  const auto maxDocId = spec->docs.maxDocId;
  DMD_Return(borrowed);

  RMCK::hset(ctx, "doc:1", "__score", "0.5");
  notifyUpdate("doc:1", {"__score"});
  EXPECT_EQ(docIdOf("doc:1"), first);
  const RSDocumentMetadata *current = DocTable_Borrow(&spec->docs, first);
  ASSERT_NE(current, nullptr);
  EXPECT_FLOAT_EQ(current->score, 0.5);
  EXPECT_EQ(current->docLen, docLen);
  EXPECT_EQ(current->maxTermFreq, maxTermFreq);
  EXPECT_EQ(spec->docs.size, tableSize);
  EXPECT_EQ(spec->docs.maxDocId, maxDocId);
  DMD_Return(current);
}

TEST_F(ReindexSkipTest, sharedScoreAndPayloadFieldUpdatesBoth) {
  createIndex({"SCORE", "0.25", "SCORE_FIELD", "metadata", "PAYLOAD_FIELD", "metadata"});
  RMCK::hset(ctx, "doc:1", "title", "hello");
  notifyUpdate("doc:1", {"title"});
  const t_docId first = docIdOf("doc:1");

  RMCK::hset(ctx, "doc:1", "metadata", "0.5");
  notifyUpdate("doc:1", {"metadata"});
  EXPECT_EQ(docIdOf("doc:1"), first);
  const RSDocumentMetadata *dmd = DocTable_Borrow(&spec->docs, first);
  ASSERT_NE(dmd, nullptr);
  EXPECT_FLOAT_EQ(dmd->score, 0.5);
  ASSERT_TRUE(hasPayload(dmd->flags));
  EXPECT_EQ(std::string(dmd->payload->data, dmd->payload->len), "0.5");
  DMD_Return(dmd);
}

TEST_F(ReindexSkipTest, metadataAndIndexedFieldChangeReindexes) {
  createIndex({"SCORE_FIELD", "__score"}, "renamed");
  RMCK::hset(ctx, "doc:1", "title", "hello");
  notifyUpdate("doc:1", {"title"});
  const t_docId first = docIdOf("doc:1");

  RMCK::hset(ctx, "doc:1", "__score", "0.5");
  RMCK::hset(ctx, "doc:1", "title", "goodbye");
  notifyUpdate("doc:1", {"__score", "title"});
  EXPECT_GT(docIdOf("doc:1"), first);
}

TEST_F(ReindexSkipTest, metadataFieldThatIsAlsoAnAliasedSchemaPathReindexes) {
  createIndex({"SCORE_FIELD", "title"}, "renamed");
  RMCK::hset(ctx, "doc:1", "title", "0.5");
  notifyUpdate("doc:1", {"title"});
  const t_docId first = docIdOf("doc:1");

  RMCK::hset(ctx, "doc:1", "title", "0.75");
  notifyUpdate("doc:1", {"title"});
  EXPECT_GT(docIdOf("doc:1"), first);
}

TEST_F(ReindexSkipTest, unseenDocumentIsIndexedOnMetadataOnlyWrite) {
  createIndex({"SCORE_FIELD", "__score", "PAYLOAD_FIELD", "__payload"});
  RMCK::hset(ctx, "doc:1", "__score", "0.5");
  RMCK::hset(ctx, "doc:1", "__payload", "first");
  notifyUpdate("doc:1", {"__score", "__payload"});
  const t_docId first = docIdOf("doc:1");
  ASSERT_NE(first, 0u);
  const RSDocumentMetadata *dmd = DocTable_Borrow(&spec->docs, first);
  ASSERT_NE(dmd, nullptr);
  EXPECT_FLOAT_EQ(dmd->score, 0.5);
  ASSERT_TRUE(hasPayload(dmd->flags));
  EXPECT_EQ(std::string(dmd->payload->data, dmd->payload->len), "first");
  DMD_Return(dmd);
}

// No change set is not a statement that nothing changed -- it is the absence of one, and the
// document has to be reindexed. This is the path JSON, background scans and servers without
// subkey notifications take.
TEST_F(ReindexSkipTest, absentChangeSetNeverSkips) {
  createIndex({"SCORE_FIELD", "__score"});
  RMCK::hset(ctx, "doc:1", "title", "hello");
  notifyUpdate("doc:1", {});
  const t_docId first = docIdOf("doc:1");
  ASSERT_NE(first, 0u);

  RMCK::hset(ctx, "doc:1", "__score", "0.5");
  notifyUpdate("doc:1", {});
  EXPECT_GT(docIdOf("doc:1"), first) << "without a change set there is nothing to skip on";
}

TEST_F(ReindexSkipTest, metadataUpdateWithoutReservedPayloadSlotReindexes) {
  createIndex();
  RMCK::hset(ctx, "doc:1", "title", "hello");
  notifyUpdate("doc:1", {"title"});
  const t_docId first = docIdOf("doc:1");
  const RSDocumentMetadata *existing = DocTable_Borrow(&spec->docs, first);
  ASSERT_NE(existing, nullptr);
  EXPECT_FALSE(existing->flags & Document_HasPayloadSlot);
  DMD_Return(existing);

  // Legacy command paths can configure PAYLOAD_FIELD after documents have been indexed.
  spec->rule->payload_field = rm_strdup("__payload");
  RMCK::hset(ctx, "doc:1", "__payload", "first");
  notifyUpdate("doc:1", {"__payload"});
  const t_docId current = docIdOf("doc:1");
  EXPECT_GT(current, first);
  const RSDocumentMetadata *dmd = DocTable_Borrow(&spec->docs, current);
  ASSERT_NE(dmd, nullptr);
  EXPECT_TRUE(dmd->flags & Document_HasPayloadSlot);
  ASSERT_TRUE(hasPayload(dmd->flags));
  EXPECT_EQ(std::string(dmd->payload->data, dmd->payload->len), "first");
  DMD_Return(dmd);
}

TEST_F(ReindexSkipTest, metadataUpdateWithRetainedExpirationReindexes) {
  createIndex({"SCORE_FIELD", "__score"});
  RMCK::hset(ctx, "doc:1", "title", "hello");
  notifyUpdate("doc:1", {"title"});
  const t_docId first = docIdOf("doc:1");
  // Stale DMD state alone must force a reload even when Redis no longer reports a TTL.
  RSDocumentMetadata *existing =
      const_cast<RSDocumentMetadata *>(DocTable_Borrow(&spec->docs, first));
  ASSERT_NE(existing, nullptr);
  existing->expirationTimeNs = 123456789;
  DMD_Return(existing);

  RMCK::hset(ctx, "doc:1", "__score", "0.5");
  notifyUpdate("doc:1", {"__score"});
  const t_docId current = docIdOf("doc:1");
  EXPECT_GT(current, first);
  const RSDocumentMetadata *dmd = DocTable_Borrow(&spec->docs, current);
  ASSERT_NE(dmd, nullptr);
  EXPECT_FLOAT_EQ(dmd->score, 0.5);
  EXPECT_EQ(dmd->expirationTimeNs, 0);
  DMD_Return(dmd);
}

TEST_F(ReindexSkipTest, metadataUpdateWithBorrowedReaderReindexes) {
  createIndex({"SCORE", "0.25", "SCORE_FIELD", "__score", "PAYLOAD_FIELD", "__payload"});
  RMCK::hset(ctx, "doc:1", "title", "hello");
  RMCK::hset(ctx, "doc:1", "__payload", "original");
  notifyUpdate("doc:1", {"title", "__payload"});
  const t_docId first = docIdOf("doc:1");
  const RSDocumentMetadata *reader = DocTable_Borrow(&spec->docs, first);
  ASSERT_NE(reader, nullptr);
  EXPECT_FLOAT_EQ(reader->score, 0.25);
  ASSERT_TRUE(hasPayload(reader->flags));
  EXPECT_EQ(std::string(reader->payload->data, reader->payload->len), "original");

  RMCK::hset(ctx, "doc:1", "__score", "0.5");
  RMCK::hset(ctx, "doc:1", "__payload", "replacement");
  notifyUpdate("doc:1", {"__score", "__payload"});
  const t_docId currentId = docIdOf("doc:1");
  EXPECT_GT(currentId, first);
  EXPECT_FLOAT_EQ(reader->score, 0.25);
  EXPECT_EQ(std::string(reader->payload->data, reader->payload->len), "original");

  const RSDocumentMetadata *current = DocTable_Borrow(&spec->docs, currentId);
  ASSERT_NE(current, nullptr);
  EXPECT_NE(current, reader);
  EXPECT_FLOAT_EQ(current->score, 0.5);
  ASSERT_TRUE(hasPayload(current->flags));
  EXPECT_EQ(std::string(current->payload->data, current->payload->len), "replacement");
  DMD_Return(current);
  DMD_Return(reader);
}

TEST_F(ReindexSkipTest, metadataUpdateAfterFailedOpenReindexes) {
  createIndex({"SCORE_FIELD", "__score"});
  RMCK::hset(ctx, "doc:1", "title", "hello");
  notifyUpdate("doc:1", {"title"});
  const t_docId first = docIdOf("doc:1");
  RSDocumentMetadata *existing =
      const_cast<RSDocumentMetadata *>(DocTable_Borrow(&spec->docs, first));
  ASSERT_NE(existing, nullptr);
  existing->flags = static_cast<RSDocumentFlags>(existing->flags | Document_FailedToOpen);
  DMD_Return(existing);

  RMCK::hset(ctx, "doc:1", "__score", "0.5");
  notifyUpdate("doc:1", {"__score"});
  const t_docId current = docIdOf("doc:1");
  EXPECT_GT(current, first);
  const RSDocumentMetadata *dmd = DocTable_Borrow(&spec->docs, current);
  ASSERT_NE(dmd, nullptr);
  EXPECT_FLOAT_EQ(dmd->score, 0.5);
  EXPECT_FALSE(dmd->flags & Document_FailedToOpen);
  DMD_Return(dmd);
}

TEST_F(ReindexSkipTest, metadataUpdateWithRetainedFieldExpirationReindexes) {
  createIndex({"SCORE_FIELD", "__score"});
  RMCK::hset(ctx, "doc:1", "title", "hello");
  notifyUpdate("doc:1", {"title"});
  const t_docId first = docIdOf("doc:1");
  RSDocumentMetadata *existing =
      const_cast<RSDocumentMetadata *>(DocTable_Borrow(&spec->docs, first));
  ASSERT_NE(existing, nullptr);
  FieldExpirations fields = FieldExpirations_Empty();
  FieldExpiration expiration = {0, {123456789, 0}};
  FieldExpirations_Push(&fields, expiration);
  DocTable_UpdateFieldExpiration(&spec->docs, existing, fields);
  DMD_Return(existing);
  ASSERT_EQ(DocTable_GetFieldExpirations(&spec->docs, first).len, 1u);

  RMCK::hset(ctx, "doc:1", "__score", "0.5");
  notifyUpdate("doc:1", {"__score"});
  const t_docId current = docIdOf("doc:1");
  EXPECT_GT(current, first);
  EXPECT_EQ(DocTable_GetFieldExpirations(&spec->docs, first).len, 0u);
  EXPECT_EQ(DocTable_GetFieldExpirations(&spec->docs, current).len, 0u);
  const RSDocumentMetadata *dmd = DocTable_Borrow(&spec->docs, current);
  ASSERT_NE(dmd, nullptr);
  EXPECT_FLOAT_EQ(dmd->score, 0.5);
  DMD_Return(dmd);
}

TEST_F(ReindexSkipTest, metadataUpdateHonorsBackgroundScanOOMFailure) {
  createIndex({"SCORE", "0.25", "SCORE_FIELD", "__score"});
  RMCK::hset(ctx, "doc:1", "title", "hello");
  notifyUpdate("doc:1", {"title"});
  const t_docId first = docIdOf("doc:1");
  const size_t errors = IndexError_ErrorCount(&spec->stats.indexError);

  RS_AtomicBoolStoreRelaxed(&spec->scan_failed_OOM, true);
  RMCK::hset(ctx, "doc:1", "__score", "0.75");
  notifyUpdate("doc:1", {"__score"});
  RS_AtomicBoolStoreRelaxed(&spec->scan_failed_OOM, false);

  EXPECT_EQ(docIdOf("doc:1"), first);
  EXPECT_EQ(IndexError_ErrorCount(&spec->stats.indexError), errors + 1);
  const RSDocumentMetadata *dmd = DocTable_Borrow(&spec->docs, first);
  ASSERT_NE(dmd, nullptr);
  EXPECT_FLOAT_EQ(dmd->score, 0.25);
  DMD_Return(dmd);
}

// FLOAT32 DIM 4 -- 16 bytes, matching expBlobSize.
static const char *const kVecA = "aaaabbbbccccdddd";
static const char *const kVecB = "eeeeffffgggghhhh";

// The case this optimization exists for: only a VECTOR field changed, so the label is updated
// via VecSimIndex_UpdateVectors under the doc's existing id instead of a full reindex.
TEST_F(ReindexSkipTest, vectorOnlyChangeUpdatesInPlace) {
  createIndexWithVector();
  RMCK::hset(ctx, "doc:1", "title", "hello");
  RMCK::hset(ctx, "doc:1", "vec", kVecA);
  notifyUpdate("doc:1", {"title", "vec"});
  const t_docId first = docIdOf("doc:1");
  ASSERT_NE(first, 0u);
  ASSERT_TRUE(labelHolds(first, kVecA));

  RMCK::hset(ctx, "doc:1", "vec", kVecB);
  notifyUpdate("doc:1", {"vec"});
  EXPECT_EQ(docIdOf("doc:1"), first) << "a vector-only change must not reindex";
  EXPECT_TRUE(labelHolds(first, kVecB)) << "the vector must be updated in place";
}

// The vector-only fast path is gated behind OPTIMIZE_PARTIAL_UPDATE: with it off, the same
// vector-only change set must fall back to a full reindex under a new doc-id, not update in
// place.
TEST_F(ReindexSkipTest, vectorOnlyChangeForcesFullReindexWhenOptimizationDisabled) {
  createIndexWithVector();
  RMCK::hset(ctx, "doc:1", "title", "hello");
  RMCK::hset(ctx, "doc:1", "vec", kVecA);
  notifyUpdate("doc:1", {"title", "vec"});
  const t_docId first = docIdOf("doc:1");
  ASSERT_NE(first, 0u);

  RSGlobalConfig.optimizePartialUpdate = false;
  RMCK::hset(ctx, "doc:1", "vec", kVecB);
  notifyUpdate("doc:1", {"vec"});
  EXPECT_GT(docIdOf("doc:1"), first) << "the fast path must not fire while disabled";
  EXPECT_TRUE(labelHolds(docIdOf("doc:1"), kVecB));
}

// A non-vector schema field changed alongside the vector, so the fast path must not fire: the
// document still needs a full reindex, and the vector is re-added (not updated) under the new id.
TEST_F(ReindexSkipTest, vectorAndNonVectorChangeStillReindexes) {
  createIndexWithVector();
  RMCK::hset(ctx, "doc:1", "title", "hello");
  RMCK::hset(ctx, "doc:1", "vec", kVecA);
  notifyUpdate("doc:1", {"title", "vec"});
  const t_docId first = docIdOf("doc:1");
  ASSERT_NE(first, 0u);

  RMCK::hset(ctx, "doc:1", "title", "goodbye");
  RMCK::hset(ctx, "doc:1", "vec", kVecB);
  notifyUpdate("doc:1", {"title", "vec"});
  const t_docId second = docIdOf("doc:1");
  EXPECT_GT(second, first);
  EXPECT_TRUE(labelHolds(second, kVecB));
}

// IndexUpdate_VectorOnly composes with the metadata fast path: a single write touching both the
// vector and the score field must update both without a reindex.
TEST_F(ReindexSkipTest, vectorChangeAlongsideMetadataUpdatesBothWithoutReindex) {
  createIndexWithVector({"SCORE_FIELD", "__score"});
  RMCK::hset(ctx, "doc:1", "title", "hello");
  RMCK::hset(ctx, "doc:1", "vec", kVecA);
  notifyUpdate("doc:1", {"title", "vec"});
  const t_docId first = docIdOf("doc:1");
  ASSERT_NE(first, 0u);

  RMCK::hset(ctx, "doc:1", "__score", "0.5");
  RMCK::hset(ctx, "doc:1", "vec", kVecB);
  notifyUpdate("doc:1", {"__score", "vec"});
  EXPECT_EQ(docIdOf("doc:1"), first);
  EXPECT_TRUE(labelHolds(first, kVecB));
  const RSDocumentMetadata *dmd = DocTable_Borrow(&spec->docs, first);
  ASSERT_NE(dmd, nullptr);
  EXPECT_FLOAT_EQ(dmd->score, 0.5);
  DMD_Return(dmd);
}

// A document the index does not hold yet must still take the full path on a vector-only change:
// there is no existing id to update in place, and the document itself has to be registered.
TEST_F(ReindexSkipTest, vectorOnlyChangeOnUnseenDocumentStillIndexes) {
  createIndexWithVector();
  RMCK::hset(ctx, "doc:1", "vec", kVecA);
  notifyUpdate("doc:1", {"vec"});
  const t_docId first = docIdOf("doc:1");
  EXPECT_NE(first, 0u) << "a document absent from the index must be indexed";
  EXPECT_TRUE(labelHolds(first, kVecA));
}

// A single Hash path can be mapped to more than one schema field (`v AS vv VECTOR ..., v AS txt
// TEXT` -- see testSchemaWithAs_Duplicates in test.py). A write to that path must still force a
// full reindex if *any* of its mappings is non-vector, even though another mapping of the same
// path is the vector field this file's other tests take the fast path for.
TEST_F(ReindexSkipTest, sharedPathWithNonVectorMappingStillReindexes) {
  QueryError err = QueryError_Default();
  std::vector<std::string> args = {"FT.CREATE", indexName, "ON", "HASH", "SCHEMA",
                                   "v", "AS", "vv", "VECTOR", "FLAT", "6", "TYPE", "FLOAT32",
                                   "DIM", "4", "DISTANCE_METRIC", "L2",
                                   "v", "AS", "txt", "TEXT"};
  RMCK::ArgvList argv(ctx, args);
  spec = Indexes_CreateNewSpec(ctx, argv, argv.size(), &err);
  ASSERT_FALSE(QueryError_HasError(&err)) << QueryError_GetUserError(&err);
  ASSERT_TRUE(spec != nullptr);

  RMCK::hset(ctx, "doc:1", "v", kVecA);
  notifyUpdate("doc:1", {"v"});
  const t_docId first = docIdOf("doc:1");
  ASSERT_NE(first, 0u);

  RMCK::hset(ctx, "doc:1", "v", kVecB);
  notifyUpdate("doc:1", {"v"});
  EXPECT_GT(docIdOf("doc:1"), first)
      << "a path also mapped to a non-vector field must not take the vector-only fast path";
  EXPECT_TRUE(labelHolds(docIdOf("doc:1"), kVecB));
}
