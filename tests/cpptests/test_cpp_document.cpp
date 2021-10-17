#include <gtest/gtest.h>
#include "redismock/redismock.h"
#include "redismock/util.h"
#include "document.h"

class DocumentTest : public ::testing::Test {
 protected:
  RedisModuleCtx *ctx;
  void SetUp() override {
    ctx = RedisModule_GetThreadSafeContext(NULL);
    RMCK::flushdb(ctx);
  }
  void TearDown() override {
    if (ctx) {
      RedisModule_FreeThreadSafeContext(ctx);
      ctx = NULL;
    }
  }
};

TEST_F(DocumentTest, testClear) {
  Document d = {0};
  RedisModuleString *s = RedisModule_CreateString(ctx, "foo", 3);
  ASSERT_EQ(1, RMCK::GetRefcount(s));
  Document_Init(&d, s, 0, DEFAULT_LANGUAGE, DocumentType_Hash);

  ASSERT_EQ(0, d.flags);
  ASSERT_EQ(s, d.docKey);
  ASSERT_EQ(1, RMCK::GetRefcount(s));

  Document_AddField(&d, "foo", RMCK::RString("bar"), 0);
  ASSERT_EQ(0, d.flags);
  ASSERT_EQ(1, d.numFields);

  Document_Clear(&d);
  ASSERT_EQ(0, d.numFields);
  ASSERT_EQ(0, d.fields);
  Document_Free(&d);
  RedisModule_FreeString(ctx, s);
}

TEST_F(DocumentTest, testLoadAll) {
  Document d = {0};
  RMCK::RString docKey("doc1");
  Document_Init(&d, docKey, 42, RS_LANG_FRENCH, DocumentType_Hash);
  ASSERT_EQ(42, d.score);
  ASSERT_EQ(RS_LANG_FRENCH, d.language);
  // etc...

  // Store a document:
  RMCK::hset(ctx, "doc1", "ni1", "foo1");
  RMCK::hset(ctx, "doc1", "ni2", "foo2");
  int rv = Document_LoadAllFields(&d, ctx);
  ASSERT_EQ(REDISMODULE_OK, rv);
  ASSERT_EQ(2, d.numFields);
  auto f = Document_GetField(&d, "ni2");
  ASSERT_FALSE(f == NULL);
  ASSERT_STREQ("ni2", f->name);
  ASSERT_TRUE(0 == RedisModule_StringCompare(f->text, RMCK::RString("foo2")));
  f = Document_GetField(&d, "ni1");
  ASSERT_FALSE(f == NULL);
  ASSERT_STREQ("ni1", f->name);
  ASSERT_TRUE(0 == RedisModule_StringCompare(f->text, RMCK::RString("foo1")));
  ASSERT_EQ(DOCUMENT_F_OWNSTRINGS, d.flags);
  Document_Free(&d);
}

#ifdef HAVE_RM_SCANCURSOR_CREATE
//@@ TODO: avoid background indexing so cursor won't be needed

TEST_F(DocumentTest, testLoadSchema) {
  // Create a database
  QueryError status = {};
  RMCK::ArgvList args(ctx, "FT.CREATE", "idx", "ON", "HASH", "SCHEMA", "t1", "TEXT", "t2", "TEXT");
  auto spec = IndexSpec_CreateNew(ctx, args, args.size(), &status);
  ASSERT_FALSE(spec == NULL);

  Document d = {0};
  RMCK::RString docKey("doc1");
  Document_Init(&d, docKey, 1, DEFAULT_LANGUAGE);
  int rv = Document_LoadAllFields(&d, ctx);
  ASSERT_EQ(REDISMODULE_ERR, rv);

  // Add some values
  RMCK::hset(ctx, "doc1", "somefield", "someval");
  RMCK::hset(ctx, "doc1", "secondfield", "secondval");
  RMCK::hset(ctx, "doc1", "t1", "Hello World");
  RMCK::hset(ctx, "doc1", "t2", "foobar");

  RedisSearchCtx sctx = SEARCH_CTX_STATIC(ctx, spec);
  rv = Document_LoadSchemaFieldHash(&d, &sctx);
  ASSERT_EQ(REDISMODULE_OK, rv);
  ASSERT_EQ(2, d.numFields);  // Only a single field
  ASSERT_EQ(NULL, Document_GetField(&d, "somefield"));
  ASSERT_EQ(NULL, Document_GetField(&d, "secondfield"));
  auto f = Document_GetField(&d, "t1");
  ASSERT_FALSE(f == NULL);
  ASSERT_STREQ("t1", f->name);
  ASSERT_EQ(0, RedisModule_StringCompare(RMCK::RString("Hello World"), f->text));

  f = Document_GetField(&d, "t2");
  ASSERT_FALSE(f == NULL);
  ASSERT_STREQ("t2", f->name);
  ASSERT_EQ(0, RedisModule_StringCompare(RMCK::RString("foobar"), f->text));

  ASSERT_EQ(DOCUMENT_F_OWNSTRINGS, d.flags);
  Document_Free(&d);
  IndexSpec_Free(spec);
}

#endif // HAVE_RM_SCANCURSOR_CREATE
