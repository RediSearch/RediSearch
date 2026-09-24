/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "gtest/gtest.h"
extern "C" {
#include "reply.h"
}
#include "redismock/redismock.h"
#include "redismock/internal.h"

#include <string>
#include <vector>

// The reply wrapper decides whether a collection's length reaches Redis when it is opened or as a
// deferred fixup on close; the mock logs exactly those calls.
class ReplyTest : public ::testing::Test {
 protected:
  RedisModuleCtx *ctx;
  void SetUp() override { ctx = RedisModule_GetThreadSafeContext(NULL); }
  void TearDown() override { RedisModule_FreeThreadSafeContext(ctx); }
  std::vector<std::string> log() { return RMCK_GetReplyLog(ctx); }
  using Log = std::vector<std::string>;
};

TEST_F(ReplyTest, postponedArrayClosesWithTheWrittenCount) {
  RedisModule_Reply reply = RedisModule_NewReply(ctx);
  RedisModule_Reply_Array(&reply);
  RedisModule_Reply_LongLong(&reply, 1);
  RedisModule_Reply_LongLong(&reply, 2);
  RedisModule_Reply_ArrayEnd(&reply);
  RedisModule_EndReply(&reply);
  ASSERT_EQ(log(), (Log{"array:postponed", "setarray:2"}));
}

TEST_F(ReplyTest, declaredArrayNeedsNoFixupAndNoEnd) {
  RedisModule_Reply reply = RedisModule_NewReply(ctx);
  RedisModule_Reply_ArrayWithLen(&reply, 2);
  RedisModule_Reply_LongLong(&reply, 1);
  RedisModule_Reply_LongLong(&reply, 2);
  RedisModule_EndReply(&reply);
  ASSERT_EQ(log(), (Log{"array:2"}));
}

// Redis renders a map as an array of pairs for RESP2 clients on its own, so the wrapper uses the map
// API in both protocols.
TEST_F(ReplyTest, mapsUseTheMapApiInBothProtocols) {
  RedisModule_Reply reply = RedisModule_NewReply(ctx);
  ASSERT_FALSE(RedisModule_IsRESP3(&reply));
  RedisModule_Reply_MapWithLen(&reply, 2);
  RedisModule_ReplyKV_LongLong(&reply, "a", 1);
  RedisModule_ReplyKV_LongLong(&reply, "b", 2);
  RedisModule_Reply_Map(&reply);
  RedisModule_ReplyKV_LongLong(&reply, "c", 3);
  RedisModule_Reply_MapEnd(&reply);
  RedisModule_EndReply(&reply);
  // The postponed form closes with the entry count, not the element count.
  ASSERT_EQ(log(), (Log{"map:2", "map:postponed", "setmap:1"}));
}

// A declared collection counts as one element of the open postponed parent, whatever it contains.
TEST_F(ReplyTest, declaredCollectionsCountAsOneElementOfTheirParent) {
  RedisModule_Reply reply = RedisModule_NewReply(ctx);
  RedisModule_Reply_Array(&reply);
  RedisModule_Reply_MapWithLen(&reply, 1);
  RedisModule_ReplyKV_LongLong(&reply, "a", 1);
  RedisModule_ReplyKV_ArrayWithLen(&reply, "rows", 3);
  RedisModule_Reply_LongLong(&reply, 7);
  RedisModule_Reply_ArrayWithLen(&reply, 0);
  RedisModule_Reply_Null(&reply);
  RedisModule_Reply_ArrayEnd(&reply);
  RedisModule_EndReply(&reply);
  // map, "rows" key, rows array -> three elements in the postponed parent.
  ASSERT_EQ(log(), (Log{"array:postponed", "map:1", "array:3", "array:0", "setarray:3"}));
}

TEST_F(ReplyTest, closingAnInnerFrameResumesCountingInTheOuterOne) {
  RedisModule_Reply reply = RedisModule_NewReply(ctx);
  RedisModule_Reply_Array(&reply);
  RedisModule_Reply_LongLong(&reply, 1);
  RedisModule_Reply_Array(&reply);
  RedisModule_Reply_LongLong(&reply, 2);
  RedisModule_Reply_LongLong(&reply, 3);
  RedisModule_Reply_ArrayEnd(&reply);
  RedisModule_Reply_LongLong(&reply, 4);
  RedisModule_Reply_ArrayEnd(&reply);
  // Back at the top level nothing is counted.
  RedisModule_Reply_LongLong(&reply, 5);
  ASSERT_EQ(reply.cur, nullptr);
  RedisModule_EndReply(&reply);
  ASSERT_EQ(log(), (Log{"array:postponed", "array:postponed", "setarray:2", "setarray:3"}));
}

TEST_F(ReplyTest, emptyDeclaredCollectionsAreLegal) {
  RedisModule_Reply reply = RedisModule_NewReply(ctx);
  RedisModule_Reply_ArrayWithLen(&reply, 0);
  RedisModule_Reply_MapWithLen(&reply, 0);
  RedisModule_Reply_EmptyMap(&reply);
  RedisModule_EndReply(&reply);
  ASSERT_EQ(log(), (Log{"array:0", "map:0", "map:0"}));
}

#ifdef ENABLE_ASSERT
// Assert builds mirror the reply as JSON so a failing wrapper assert can show what was written.
TEST_F(ReplyTest, assertBuildsMirrorTheReplyAsJson) {
  RedisModule_Reply reply = RedisModule_NewReply(ctx);
  RedisModule_Reply_Map(&reply);
  RedisModule_ReplyKV_LongLong(&reply, "n", 1);
  RedisModule_ReplyKV_ArrayWithLen(&reply, "rows", 2);
  RedisModule_Reply_StringBuffer(&reply, "ab", 2);
  RedisModule_Reply_Null(&reply);
  RedisModule_ReplyKV_Array(&reply, "more");
  RedisModule_Reply_ArrayEnd(&reply);
  RedisModule_Reply_MapEnd(&reply);
  ASSERT_STREQ(reply.json, "{ \"n\": 1, \"rows\": [ \"ab\", null ], \"more\": [  ] }");
  RedisModule_EndReply(&reply);
  ASSERT_EQ(reply.json, nullptr);
}
#endif
