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

TEST_F(ReplyTest, declaredArrayNeedsNoFixup) {
  RedisModule_Reply reply = RedisModule_NewReply(ctx);
  RedisModule_Reply_ArrayWithLen(&reply, 2);
  RedisModule_Reply_LongLong(&reply, 1);
  RedisModule_Reply_LongLong(&reply, 2);
  RedisModule_Reply_ArrayEnd(&reply);
  RedisModule_EndReply(&reply);
  ASSERT_EQ(log(), (Log{"array:2"}));
}

TEST_F(ReplyTest, declaredMapIsAnArrayOfPairsInResp2) {
  RedisModule_Reply reply = RedisModule_NewReply(ctx);
  ASSERT_FALSE(RedisModule_IsRESP3(&reply));
  RedisModule_Reply_MapWithLen(&reply, 2);
  RedisModule_ReplyKV_LongLong(&reply, "a", 1);
  RedisModule_ReplyKV_LongLong(&reply, "b", 2);
  RedisModule_Reply_MapEnd(&reply);
  RedisModule_EndReply(&reply);
  ASSERT_EQ(log(), (Log{"array:4"}));
}

TEST_F(ReplyTest, declaredMapCountsEntriesInResp3) {
  ctx->ctx_flags = REDISMODULE_CTX_FLAGS_RESP3;
  RedisModule_Reply reply = RedisModule_NewReply(ctx);
  ASSERT_TRUE(RedisModule_IsRESP3(&reply));
  RedisModule_Reply_MapWithLen(&reply, 2);
  RedisModule_ReplyKV_LongLong(&reply, "a", 1);
  RedisModule_ReplyKV_LongLong(&reply, "b", 2);
  RedisModule_Reply_MapEnd(&reply);
  // The postponed form still closes with the entry count, not the element count.
  RedisModule_Reply_Map(&reply);
  RedisModule_ReplyKV_LongLong(&reply, "c", 3);
  RedisModule_Reply_MapEnd(&reply);
  RedisModule_EndReply(&reply);
  ASSERT_EQ(log(), (Log{"map:2", "map:postponed", "setmap:1"}));
}

TEST_F(ReplyTest, declaredCollectionsCountAsOneElementOfTheirParent) {
  RedisModule_Reply reply = RedisModule_NewReply(ctx);
  RedisModule_Reply_Array(&reply);
  RedisModule_Reply_MapWithLen(&reply, 1);
  RedisModule_ReplyKV_LongLong(&reply, "a", 1);
  RedisModule_Reply_MapEnd(&reply);
  RedisModule_ReplyKV_ArrayWithLen(&reply, "rows", 1);
  RedisModule_Reply_LongLong(&reply, 7);
  RedisModule_Reply_ArrayEnd(&reply);
  RedisModule_Reply_ArrayEnd(&reply);
  RedisModule_EndReply(&reply);
  // map, "rows" key, rows array -> three elements in the postponed parent.
  ASSERT_EQ(log(), (Log{"array:postponed", "array:2", "array:1", "setarray:3"}));
}

TEST_F(ReplyTest, emptyDeclaredCollectionsAreLegal) {
  RedisModule_Reply reply = RedisModule_NewReply(ctx);
  RedisModule_Reply_ArrayWithLen(&reply, 0);
  RedisModule_Reply_ArrayEnd(&reply);
  RedisModule_Reply_MapWithLen(&reply, 0);
  RedisModule_Reply_MapEnd(&reply);
  RedisModule_EndReply(&reply);
  ASSERT_EQ(log(), (Log{"array:0", "array:0"}));
}
