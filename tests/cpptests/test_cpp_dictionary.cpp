/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#include "gtest/gtest.h"
#include "redismock/internal.h"
#include "redismock/redismock.h"

#include <cstring>
#include <memory>

extern "C" {
#include "dictionary.h"
}

class DictionaryRdbTest : public ::testing::Test {
 protected:
  void SetUp() override {
    Dictionary_Clear();
  }
  void TearDown() override {
    Dictionary_Clear();
  }
};

TEST_F(DictionaryRdbTest, DuplicateNamesKeepFirstDictionaryAndContinueLoading) {
  auto type = RedisModuleType::typemap.find("scdtype00");
  ASSERT_NE(type, RedisModuleType::typemap.end());
  const auto load = type->second->typemeths.aux_load;
  ASSERT_NE(load, nullptr);
  std::unique_ptr<RedisModuleIO, decltype(&RMCK_FreeRdbIO)> io(RMCK_CreateRdbIO(), RMCK_FreeRdbIO);
  ASSERT_NE(io, nullptr);
  RMCK_SaveUnsigned(io.get(), 4);
  const char* names[] = {"duplicate", "duplicate", "empty", "unique"};
  const char* terms[] = {"kept", "rejected", nullptr, "later"};
  for (size_t i = 0; i < 4; ++i) {
    RMCK_SaveStringBuffer(io.get(), names[i], strlen(names[i]) + 1);
    RMCK_SaveUnsigned(io.get(), terms[i] ? 1 : 0);
    if (terms[i]) {
      RMCK_SaveStringBuffer(io.get(), terms[i], strlen(terms[i]) + 1);
      RMCK_SaveDouble(io.get(), 1.0);
    }
  }
  io->read_pos = 0;
  ASSERT_EQ(load(io.get(), type->second->encver, REDISMODULE_AUX_AFTER_RDB), REDISMODULE_OK);
  EXPECT_EQ(RMCK_IsIOError(io.get()), 0);
  EXPECT_EQ(Dictionary_Size(), 2);
  Trie* duplicate = SpellCheck_OpenDict(nullptr, "duplicate", REDISMODULE_READ);
  ASSERT_NE(duplicate, nullptr);
  EXPECT_EQ(Trie_Size(duplicate), 1);
  EXPECT_EQ(Trie_Delete(duplicate, "rejected", sizeof("rejected") - 1), 0);
  EXPECT_EQ(Trie_Delete(duplicate, "kept", sizeof("kept") - 1), 1);
  EXPECT_EQ(SpellCheck_OpenDict(nullptr, "empty", REDISMODULE_READ), nullptr);
  Trie* unique = SpellCheck_OpenDict(nullptr, "unique", REDISMODULE_READ);
  ASSERT_NE(unique, nullptr);
  EXPECT_EQ(Trie_Size(unique), 1);
  EXPECT_EQ(Trie_Delete(unique, "later", sizeof("later") - 1), 1);
}
