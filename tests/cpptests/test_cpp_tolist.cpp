/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#include "gtest/gtest.h"

#include "aggregate/reducer.h"
#include "rlookup.h"
#include "value_ffi.h"

#include <algorithm>
#include <memory>
#include <string>
#include <vector>

using ValuePtr = std::unique_ptr<RSValue, decltype(&RSValue_DecrRef)>;

// Exercise reducer lifetime boundaries without retaining the source rows.
class TolistTest : public ::testing::TestWithParam<unsigned> {
 protected:
  RLookup lookup;
  RLookupRow row;
  const RLookupKey* key = nullptr;
  Reducer* reducer = nullptr;
  void* group = nullptr;

  void SetUp() override {
    lookup = RLookup_New();
    row = RLookupRow_New();
    key = RLookup_GetKey_Write(&lookup, "value", RLOOKUP_F_NOFLAGS);
    const char* args[] = {"value"};
    ArgsCursor ac;
    ArgsCursor_InitCString(&ac, args, 1);
    QueryError status = QueryError_Default();
    ReducerOptions opts =
        REDUCEROPTS_INIT("TOLIST", &ac, &lookup, nullptr, &status, false, false, nullptr, 0);
    reducer = RDCRToList_New(&opts);
    ASSERT_NE(reducer, nullptr) << QueryError_GetUserError(&status);
    QueryError_ClearError(&status);
    group = reducer->NewInstance(reducer);
  }

  void TearDown() override {
    if (group) reducer->FreeInstance(reducer, group);
    if (reducer) reducer->Free(reducer);
    RLookupRow_Reset(&row);
    RLookup_Cleanup(&lookup);
  }

  static RSValue* makeValue(unsigned i) {
    std::string text = "value-" + std::to_string(i);
    return RSValue_NewString(rm_strdup(text.c_str()), text.size());
  }

  void addOwnedValue(RSValue* value) {
    RLookup_WriteOwnKey(key, &row, value);
    ASSERT_EQ(reducer->Add(reducer, group, &row), 1);
    RLookupRow_Wipe(&row);
  }

  ValuePtr finishAndFreeGroup() {
    ValuePtr result(reducer->Finalize(reducer, group), RSValue_DecrRef);
    reducer->FreeInstance(reducer, group);
    group = nullptr;
    return result;
  }

  void expectValues(const RSValue* result) {
    ASSERT_TRUE(RSValue_IsArray(result));
    ASSERT_EQ(RSValue_ArrayLen(result), GetParam());
    std::vector<std::string> actual, expected;
    for (unsigned i = 0; i < GetParam(); ++i) {
      const RSValue* value = RSValue_ArrayItem(result, i);
      ASSERT_EQ(RSValue_Type(value), RSValueType_String);
      size_t length;
      const char* text = RSValue_StringPtrLen(value, &length);
      actual.emplace_back(text, length);
      expected.push_back("value-" + std::to_string(i));
    }
    std::sort(actual.begin(), actual.end());
    std::sort(expected.begin(), expected.end());
    EXPECT_EQ(actual, expected);
  }
};

TEST_P(TolistTest, DistinctValuesSurviveGroupCleanup) {
  for (unsigned i = 0; i < GetParam(); ++i) {
    addOwnedValue(makeValue(i));
    addOwnedValue(makeValue(i));
  }
  // Revisit old values after the last insertion, including after dictionary promotion.
  for (unsigned i = 0; i < GetParam(); ++i) {
    addOwnedValue(makeValue(i));
  }
  auto result = finishAndFreeGroup();
  expectValues(result.get());
}

TEST_P(TolistTest, ArrayInputsAreFlattenedAndDeduplicated) {
  for (unsigned repeat = 0; repeat < 2; ++repeat) {
    RSValue** values = RSValue_NewArrayBuilder(GetParam());
    for (unsigned i = 0; i < GetParam(); ++i) {
      values[i] = makeValue(i);
    }
    addOwnedValue(RSValue_NewArrayFromBuilder(values, GetParam()));
  }
  if (GetParam()) addOwnedValue(makeValue(0));
  auto result = finishAndFreeGroup();
  expectValues(result.get());
}

TEST_P(TolistTest, CleanupWithoutFinalizeReleasesReferences) {
  std::vector<ValuePtr> values;
  for (unsigned i = 0; i < GetParam(); ++i) {
    values.emplace_back(makeValue(i), RSValue_DecrRef);
    addOwnedValue(RSValue_IncrRef(values.back().get()));
    addOwnedValue(makeValue(i));
    EXPECT_EQ(RSValue_Refcount(values.back().get()), 2);
  }
  reducer->FreeInstance(reducer, group);
  group = nullptr;
  for (const auto& value : values) {
    EXPECT_EQ(RSValue_Refcount(value.get()), 1);
  }
}

INSTANTIATE_TEST_SUITE_P(StorageBoundaries, TolistTest, ::testing::Values(0u, 8u, 9u, 16u, 17u));
