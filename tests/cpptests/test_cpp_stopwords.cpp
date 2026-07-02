/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#include "gtest/gtest.h"
#include "redismock/redismock.h"

#include "stopwords.h"
#include "rmalloc.h"
#include "util/arr.h"

#include <cstring>

class StopwordsInfoTest : public ::testing::Test {};

// Stock the allocator's freelist with dirty blocks of the size
// AddStopWordsListToInfo allocates for its render buffer, so that a missing
// NUL terminator shows up as garbage instead of depending on the block
// happening to be zero-filled.
static void dirtyRenderBufferBlocks() {
  const size_t renderBufSize = sizeof(array_hdr_t) + 512;
  void *blocks[8];
  for (auto &block : blocks) {
    block = rm_malloc(renderBufSize);
    memset(block, 'X', renderBufSize);
  }
  for (auto &block : blocks) {
    rm_free(block);
  }
}

TEST_F(StopwordsInfoTest, testInfoEmptyList) {
  StopWordList *sl = NewStopWordListCStr(NULL, 0);
  ASSERT_TRUE(sl != nullptr);

  dirtyRenderBufferBlocks();

  RedisModuleInfoCtx info;
  AddStopWordsListToInfo(&info, sl);

  ASSERT_EQ(info.fields.size(), 1);
  ASSERT_EQ(info.fields[0].first, "stop_words");
  ASSERT_EQ(info.fields[0].second, "");

  StopWordList_Unref(sl);
}

TEST_F(StopwordsInfoTest, testInfoTerm) {
  const char *terms[] = {"FOO"};
  StopWordList *sl = NewStopWordListCStr(terms, 1);
  ASSERT_TRUE(sl != nullptr);

  dirtyRenderBufferBlocks();

  RedisModuleInfoCtx info;
  AddStopWordsListToInfo(&info, sl);

  ASSERT_EQ(info.fields.size(), 1);
  ASSERT_EQ(info.fields[0].first, "stop_words");
  // Terms are stored lowercased.
  ASSERT_EQ(info.fields[0].second, "\"foo\"");

  StopWordList_Unref(sl);
}

TEST_F(StopwordsInfoTest, testInfoNullList) {
  RedisModuleInfoCtx info;
  AddStopWordsListToInfo(&info, NULL);

  ASSERT_TRUE(info.fields.empty());
}
