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
  RedisModuleString *s = RedisModule_CreateString(ctx, "foo", 3);
  ASSERT_EQ(1, RMCK::GetRefcount(s));
  Document d = new Document(s, 0, DEFAULT_LANGUAGE);

  ASSERT_EQ(0, d.flags);
  ASSERT_EQ(s, d.docKey);
  ASSERT_EQ(1, RMCK::GetRefcount(s));

  d.AddField("foo", RMCK::RString("bar"), 0);
  ASSERT_EQ(0, d.flags);
  ASSERT_EQ(1, d.numFields);

  d.Clear();
  ASSERT_EQ(0, d.numFields);
  ASSERT_EQ(0, d.fields);
  RedisModule_FreeString(ctx, s);
}

TEST_F(DocumentTest, testLoadAll) {
  RMCK::RString docKey("doc1");
  Document d = new Document(docKey, 42, RS_LANG_FRENCH);
  ASSERT_EQ(42, d.score);
  ASSERT_EQ(RS_LANG_FRENCH, d.language);
  // etc...

  // Store a document:
  RMCK::hset(ctx, "doc1", "ni1", "foo1");
  RMCK::hset(ctx, "doc1", "ni2", "foo2");
  int rv = d.LoadAllFields(ctx);
  ASSERT_EQ(REDISMODULE_OK, rv);
  ASSERT_EQ(2, d.numFields);
  auto f = d.GetField("ni2");
  ASSERT_FALSE(f == NULL);
  ASSERT_STREQ("ni2", f->name);
  ASSERT_TRUE(0 == RedisModule_StringCompare(f->text, RMCK::RString("foo2")));
  f = d.GetField("ni1");
  ASSERT_FALSE(f == NULL);
  ASSERT_STREQ("ni1", f->name);
  ASSERT_TRUE(0 == RedisModule_StringCompare(f->text, RMCK::RString("foo1")));
  ASSERT_EQ(DOCUMENT_F_OWNSTRINGS, d.flags);
}

TEST_F(DocumentTest, testLoadSchema) {
  // Create a database
  QueryError status = {};
  RMCK::ArgvList args(ctx, "FT.CREATE", "idx", "SCHEMA", "t1", "TEXT", "t2", "TEXT");
  auto spec = IndexSpec_CreateNew(ctx, args, args.size(), &status);
  ASSERT_FALSE(spec == NULL);

  RMCK::RString docKey("doc1");
  Document d = new Document(docKey, 1, DEFAULT_LANGUAGE);
  int rv = d.LoadAllFields(ctx);
  ASSERT_EQ(REDISMODULE_ERR, rv);

  // Add some values
  RMCK::hset(ctx, "doc1", "somefield", "someval");
  RMCK::hset(ctx, "doc1", "secondfield", "secondval");
  RMCK::hset(ctx, "doc1", "t1", "Hello World");
  RMCK::hset(ctx, "doc1", "t2", "foobar");

  RedisSearchCtx sctx = SEARCH_CTX_STATIC(ctx, spec);
  rv = d.LoadSchemaFields(&sctx);
  ASSERT_EQ(REDISMODULE_OK, rv);
  ASSERT_EQ(2, d.numFields);  // Only a single field
  ASSERT_EQ(NULL, d.GetField("somefield"));
  ASSERT_EQ(NULL, d.GetField("secondfield"));
  auto f = d.GetField("t1");
  ASSERT_FALSE(f == NULL);
  ASSERT_STREQ("t1", f->name);
  ASSERT_EQ(0, RedisModule_StringCompare(RMCK::RString("Hello World"), f->text));

  f = d.GetField("t2");
  ASSERT_FALSE(f == NULL);
  ASSERT_STREQ("t2", f->name);
  ASSERT_EQ(0, RedisModule_StringCompare(RMCK::RString("foobar"), f->text));

  ASSERT_EQ(DOCUMENT_F_OWNSTRINGS, d.flags);
  IndexSpec_FreeWithKey(spec, ctx);
}