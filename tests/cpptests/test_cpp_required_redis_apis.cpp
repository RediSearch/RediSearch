/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#include "gtest/gtest.h"

#include "module.h"
#include "redismodule.h"

namespace {

RedisModuleCtx *fakeCreateReplyBufferContext(RedisModuleCtx *) { return nullptr; }
int fakeReplyWithBufferedReply(RedisModuleCtx *, RedisModuleCtx *) { return REDISMODULE_OK; }

// Swaps in the reply-buffer API pointers for one test and restores the real ones afterwards.
class RequiredRedisApisTest : public ::testing::Test {
 protected:
  void SetUp() override {
    savedCreate = RedisModule_CreateReplyBufferContext;
    savedReply = RedisModule_ReplyWithBufferedReply;
  }
  void TearDown() override {
    RedisModule_CreateReplyBufferContext = savedCreate;
    RedisModule_ReplyWithBufferedReply = savedReply;
  }
  void Install(bool create, bool reply) {
    RedisModule_CreateReplyBufferContext = create ? fakeCreateReplyBufferContext : nullptr;
    RedisModule_ReplyWithBufferedReply = reply ? fakeReplyWithBufferedReply : nullptr;
  }

  decltype(RedisModule_CreateReplyBufferContext) savedCreate;
  decltype(RedisModule_ReplyWithBufferedReply) savedReply;
};

}  // namespace

TEST_F(RequiredRedisApisTest, AcceptsCoreWithReplyBufferApis) {
  Install(true, true);
  EXPECT_TRUE(RediSearch_HasRequiredRedisAPIs());
}

TEST_F(RequiredRedisApisTest, RejectsCoreMissingEitherReplyBufferApi) {
  Install(false, true);
  EXPECT_FALSE(RediSearch_HasRequiredRedisAPIs());
  Install(true, false);
  EXPECT_FALSE(RediSearch_HasRequiredRedisAPIs());
  Install(false, false);
  EXPECT_FALSE(RediSearch_HasRequiredRedisAPIs());
}
