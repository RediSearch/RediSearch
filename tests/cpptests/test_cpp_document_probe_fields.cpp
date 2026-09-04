/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

// Tests for the error branches of `Document_ProbeFieldsPresent` (declared in document.h,
// defined in document_basic.c).
//
// The probe backs the selective-rescan skip decision: it tells the caller whether a document
// still carries any of the fields a rule change touches, without a full field load.
// DOCUMENT_FIELDS_PROBE_FAILED exists so an inconclusive read is never folded into "absent" --
// callers must fall back to a full reindex instead. None of its failure branches are reachable
// from a flow test: the async scan that calls this probe already resolves and type-checks the
// key before calling it, and the Python suite always loads RedisJSON. A unit test can reach
// them directly, by calling the probe with a NULL key, a wrong-typed key, or an unsupported
// DocumentType -- which is what this file does.

#include "gtest/gtest.h"
#include "redismock/redismock.h"
#include "redismock/util.h"

#include "document.h"
#include "indexes.h"
#include "spec.h"

#include <string>
#include <vector>

class DocumentProbeFieldsPresentTest : public ::testing::Test {
 protected:
  RedisModuleCtx *ctx = nullptr;
  IndexSpec *spec = nullptr;
  std::string indexName;

  void SetUp() override {
    ctx = RedisModule_GetThreadSafeContext(nullptr);
    RMCK::flushdb(ctx);
    static int counter = 0;
    indexName = "probeidx" + std::to_string(++counter);

    QueryError err = QueryError_Default();
    RMCK::ArgvList argv(ctx, std::vector<std::string>{"FT.CREATE", indexName, "ON", "HASH",
                                                      "SCHEMA", "title", "TEXT"});
    spec = Indexes_CreateNewSpec(ctx, argv, argv.size(), &err);
    ASSERT_FALSE(QueryError_HasError(&err)) << QueryError_GetUserError(&err);
    ASSERT_TRUE(spec != nullptr);
  }

  void TearDown() override {
    if (ctx) {
      RedisModule_FreeThreadSafeContext(ctx);
      ctx = nullptr;
    }
  }
};

// The async scan passes an already-open key; a NULL here means opening it failed. The Hash
// branch must fail closed rather than dereference it.
TEST_F(DocumentProbeFieldsPresentTest, hashProbeWithNullKeyFails) {
  EXPECT_EQ(Document_ProbeFieldsPresent(spec, nullptr, DocumentType_Hash, 0, spec->numFields),
            DOCUMENT_FIELDS_PROBE_FAILED);
}

// A key that exists but is not a hash (e.g. overwritten by SET since the rule last matched it)
// cannot be resolved the way Document_LoadSchemaFieldHash resolves a hash's fields, and must
// not be mistaken for one that confirms the fields absent.
TEST_F(DocumentProbeFieldsPresentTest, hashProbeAgainstWrongKeyTypeFails) {
  RedisModuleCallReply *noReply = nullptr;
  ASSERT_EQ(RedisModule_Call(ctx, "SET", "ss", RMCK::RString("doc:string").rstring(),
                             RMCK::RString("value").rstring()),
            noReply);

  RedisModuleKey *key = RedisModule_OpenKey(ctx, RMCK::RString("doc:string"), REDISMODULE_READ);
  ASSERT_TRUE(key != nullptr);
  EXPECT_EQ(Document_ProbeFieldsPresent(spec, key, DocumentType_Hash, 0, spec->numFields),
            DOCUMENT_FIELDS_PROBE_FAILED);
  RedisModule_CloseKey(key);
}

// DocumentType_Hash and DocumentType_Json are the only types the selective-rescan path ever
// passes; DocumentType_Unsupported only reaches this function through the switch's default
// arm, which is otherwise unexercised.
TEST_F(DocumentProbeFieldsPresentTest, unsupportedDocumentTypeFails) {
  EXPECT_EQ(
      Document_ProbeFieldsPresent(spec, nullptr, DocumentType_Unsupported, 0, spec->numFields),
      DOCUMENT_FIELDS_PROBE_FAILED);
}

// RedisJSON is always loaded in the Python flow-test suite, so the `!japi` branch is only
// reachable where it is not: the C++ unit harness never registers the JSON module API.
TEST_F(DocumentProbeFieldsPresentTest, jsonProbeWithoutJapiFails) {
  ASSERT_EQ(japi, nullptr) << "this test exercises the !japi branch; it requires japi unset";
  EXPECT_EQ(Document_ProbeFieldsPresent(spec, nullptr, DocumentType_Json, 0, spec->numFields),
            DOCUMENT_FIELDS_PROBE_FAILED);
}

// Positive controls: without at least one case that is not PROBE_FAILED, a probe that always
// returns PROBE_FAILED unconditionally would satisfy every test above.
TEST_F(DocumentProbeFieldsPresentTest, hashProbeFindsPresentField) {
  RMCK::hset(ctx, "doc:present", "title", "hello");
  RedisModuleKey *key = RedisModule_OpenKey(ctx, RMCK::RString("doc:present"), REDISMODULE_READ);
  ASSERT_TRUE(key != nullptr);
  EXPECT_EQ(Document_ProbeFieldsPresent(spec, key, DocumentType_Hash, 0, spec->numFields),
            DOCUMENT_FIELDS_PRESENT);
  RedisModule_CloseKey(key);
}

TEST_F(DocumentProbeFieldsPresentTest, hashProbeConfirmsAbsentField) {
  RMCK::hset(ctx, "doc:absent", "unread", "x");
  RedisModuleKey *key = RedisModule_OpenKey(ctx, RMCK::RString("doc:absent"), REDISMODULE_READ);
  ASSERT_TRUE(key != nullptr);
  EXPECT_EQ(Document_ProbeFieldsPresent(spec, key, DocumentType_Hash, 0, spec->numFields),
            DOCUMENT_FIELDS_ABSENT);
  RedisModule_CloseKey(key);
}
