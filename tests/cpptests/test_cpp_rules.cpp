/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "gtest/gtest.h"
#include "common.h"
#include "index_utils.h"
#include "rules.h"
#include "redisearch_api.h"
#include "obfuscation/hidden_unicode.h"
#include "rmutil/args.h"
#include "util/arr.h"

class SchemaRuleTest : public ::testing::Test {};

// SchemaRule_CreateFromAC reads prefixes from an ArgsCursor and consumes it
// fully, producing a rule whose prefix list matches the cursor contents.
TEST_F(SchemaRuleTest, testCreateFromACPrefixes) {
  auto ism = RediSearch_CreateIndex("idx_rule_ac", NULL);
  ASSERT_NE(ism, nullptr);

  const char *prefix_strs[] = {"users:", "products:", "orders:"};
  const int nprefixes = sizeof(prefix_strs) / sizeof(prefix_strs[0]);

  ArgsCursor ac;
  ArgsCursor_InitCString(&ac, prefix_strs, nprefixes);

  SchemaRuleArgs args = {0};
  args.type = "HASH";

  QueryError status = QueryError_Default();
  SchemaRule *rule = SchemaRule_CreateFromAC(&args, &ac, {ism}, &status);
  ASSERT_NE(rule, nullptr) << QueryError_GetUserError(&status);
  ASSERT_FALSE(QueryError_HasError(&status));

  ASSERT_EQ((int)array_len(rule->prefixes), nprefixes);
  for (int i = 0; i < nprefixes; i++) {
    size_t len = 0;
    const char *got = HiddenUnicodeString_GetUnsafe(rule->prefixes[i], &len);
    ASSERT_STREQ(got, prefix_strs[i]);
  }
  // The cursor should be fully consumed.
  ASSERT_EQ(AC_NumRemaining(&ac), 0u);

  get_spec(ism)->rule = rule;
  Spec_AddToDict(ism);
  freeSpec(ism);
}

// The AC and the C-array constructors must produce the same prefix list when
// fed equivalent inputs.
TEST_F(SchemaRuleTest, testCreateFromACEquivalentToCArray) {
  const char *prefix_strs[] = {"a:", "b:"};
  const int nprefixes = sizeof(prefix_strs) / sizeof(prefix_strs[0]);

  auto ism_arr = RediSearch_CreateIndex("idx_rule_arr", NULL);
  ASSERT_NE(ism_arr, nullptr);
  SchemaRuleArgs args_arr = {0};
  args_arr.type = "HASH";
  args_arr.prefixes = prefix_strs;
  args_arr.nprefixes = nprefixes;
  QueryError s_arr = QueryError_Default();
  SchemaRule *rule_arr = SchemaRule_Create(&args_arr, {ism_arr}, &s_arr);
  ASSERT_NE(rule_arr, nullptr) << QueryError_GetUserError(&s_arr);

  auto ism_ac = RediSearch_CreateIndex("idx_rule_ac2", NULL);
  ASSERT_NE(ism_ac, nullptr);
  ArgsCursor ac;
  ArgsCursor_InitCString(&ac, prefix_strs, nprefixes);
  SchemaRuleArgs args_ac = {0};
  args_ac.type = "HASH";
  QueryError s_ac = QueryError_Default();
  SchemaRule *rule_ac = SchemaRule_CreateFromAC(&args_ac, &ac, {ism_ac}, &s_ac);
  ASSERT_NE(rule_ac, nullptr) << QueryError_GetUserError(&s_ac);

  ASSERT_EQ(array_len(rule_arr->prefixes), array_len(rule_ac->prefixes));
  for (int i = 0; i < nprefixes; i++) {
    ASSERT_EQ(HiddenUnicodeString_Compare(rule_arr->prefixes[i], rule_ac->prefixes[i]), 0);
  }
  ASSERT_EQ(rule_arr->type, rule_ac->type);

  get_spec(ism_arr)->rule = rule_arr;
  Spec_AddToDict(ism_arr);
  freeSpec(ism_arr);
  get_spec(ism_ac)->rule = rule_ac;
  Spec_AddToDict(ism_ac);
  freeSpec(ism_ac);
}

// An AC with no remaining args produces a rule with an empty prefix list.
TEST_F(SchemaRuleTest, testCreateFromACEmpty) {
  auto ism = RediSearch_CreateIndex("idx_rule_ac_empty", NULL);
  ASSERT_NE(ism, nullptr);

  ArgsCursor ac;
  ArgsCursor_InitCString(&ac, NULL, 0);

  SchemaRuleArgs args = {0};
  args.type = "HASH";

  QueryError status = QueryError_Default();
  SchemaRule *rule = SchemaRule_CreateFromAC(&args, &ac, {ism}, &status);
  ASSERT_NE(rule, nullptr) << QueryError_GetUserError(&status);
  ASSERT_EQ(array_len(rule->prefixes), 0u);

  get_spec(ism)->rule = rule;
  Spec_AddToDict(ism);
  freeSpec(ism);
}

// Bad args (invalid document type) must surface as an error and return NULL,
// regardless of which constructor variant is used.
TEST_F(SchemaRuleTest, testCreateFromACInvalidType) {
  auto ism = RediSearch_CreateIndex("idx_rule_ac_bad", NULL);
  ASSERT_NE(ism, nullptr);

  const char *prefix_strs[] = {"x:"};
  ArgsCursor ac;
  ArgsCursor_InitCString(&ac, prefix_strs, 1);

  SchemaRuleArgs args = {0};
  args.type = "BOGUS";

  QueryError status = QueryError_Default();
  SchemaRule *rule = SchemaRule_CreateFromAC(&args, &ac, {ism}, &status);
  ASSERT_EQ(rule, nullptr);
  ASSERT_TRUE(QueryError_HasError(&status));
  QueryError_ClearError(&status);

  freeSpec(ism);
}
