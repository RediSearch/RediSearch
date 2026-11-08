/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "gtest/gtest.h"
#include "spec.h"

#include <random>
#include <string>
#include <vector>

namespace {

// Owns a FieldSpec array whose only populated member is fieldName.
class Fields {
 public:
  ~Fields() {
    for (FieldSpec &fs : specs_) HiddenString_Free(fs.fieldName, true);
  }

  // `name` may contain NULs; the length is explicit.
  size_t Add(const std::string &name) {
    FieldSpec fs = {};
    fs.fieldName = NewHiddenString(name.data(), name.size(), true);
    specs_.push_back(fs);
    return specs_.size() - 1;
  }

  const FieldSpec *data() const { return specs_.data(); }
  size_t size() const { return specs_.size(); }

  // The lookup the table replaces.
  const FieldSpec *LinearFind(const char *name, size_t len) const {
    for (const FieldSpec &fs : specs_) {
      if (!HiddenString_CompareC(fs.fieldName, name, len)) return &fs;
    }
    return nullptr;
  }

 private:
  std::vector<FieldSpec> specs_;
};

class FieldNameTableTest : public ::testing::Test {
 protected:
  FieldNameTable table = {};
  void TearDown() override { FieldNameTable_Free(&table); }

  const FieldSpec *Find(const Fields &f, const std::string &name) const {
    return FieldNameTable_Find(&table, f.data(), f.size(), name.data(), name.size());
  }
};

}  // namespace

TEST_F(FieldNameTableTest, GrowsAcrossThresholdsAndKeepsEverythingFindable) {
  Fields f;
  for (size_t i = 0; i < 300; i++) {
    size_t idx = f.Add("field_" + std::to_string(i));
    FieldNameTable_Add(&table, f.data(), idx);
    size_t n = i + 1;
    if (n <= FIELD_NAME_LINEAR_MAX) {
      ASSERT_EQ(table.cap, 0u) << n;
    } else {
      ASSERT_GE(table.cap, 2 * n) << n;
      ASSERT_EQ(table.cap & (table.cap - 1), 0u) << "power of two at " << n;
    }
    // Spot-check the newest and the oldest on every step, everything at the end.
    ASSERT_EQ(Find(f, "field_" + std::to_string(i)), f.data() + i);
    ASSERT_EQ(Find(f, "field_0"), f.data());
  }
  for (size_t i = 0; i < 300; i++) {
    ASSERT_EQ(Find(f, "field_" + std::to_string(i)), f.data() + i);
  }
  ASSERT_EQ(Find(f, "field_300"), nullptr);
  ASSERT_EQ(Find(f, "field_"), nullptr);
  ASSERT_EQ(Find(f, "FIELD_1"), nullptr) << "case sensitive";
}

TEST_F(FieldNameTableTest, RebuildShrinksAndDropsTheTable) {
  Fields f;
  for (size_t i = 0; i < 300; i++) f.Add("n" + std::to_string(i));
  FieldNameTable_Rebuild(&table, f.data(), 300);
  ASSERT_NE(table.slots, nullptr);
  uint32_t big = table.cap;

  FieldNameTable_Rebuild(&table, f.data(), 20);
  ASSERT_LT(table.cap, big);
  ASSERT_EQ(Find(f, "n19") , f.data() + 19);
  // Entries beyond the rebuilt prefix are gone even though the array still holds them.
  FieldNameTable_Rebuild(&table, f.data(), 20);
  ASSERT_EQ(FieldNameTable_Find(&table, f.data(), 20, "n250", 4), nullptr);

  FieldNameTable_Rebuild(&table, f.data(), FIELD_NAME_LINEAR_MAX);
  ASSERT_EQ(table.cap, 0u);
  ASSERT_EQ(table.slots, nullptr);
  ASSERT_EQ(FieldNameTable_Find(&table, f.data(), FIELD_NAME_LINEAR_MAX, "n3", 2), f.data() + 3);
  ASSERT_EQ(FieldNameTable_Find(&table, f.data(), FIELD_NAME_LINEAR_MAX, "n8", 2), nullptr);

  FieldNameTable_Rebuild(&table, f.data(), 0);
  ASSERT_EQ(table.cap, 0u);
}

TEST_F(FieldNameTableTest, EarliestDuplicateWins) {
  for (size_t total : {5u, 40u}) {
    Fields f;
    for (size_t i = 0; i < total; i++) f.Add("f" + std::to_string(i));
    size_t first = f.Add("dup");
    for (size_t i = 0; i < total; i++) f.Add("g" + std::to_string(i));
    f.Add("dup");
    FieldNameTable_Rebuild(&table, f.data(), f.size());
    ASSERT_EQ(Find(f, "dup"), f.data() + first) << total;
    ASSERT_EQ(Find(f, "dup"), f.LinearFind("dup", 3));
  }
}

TEST_F(FieldNameTableTest, IncrementalAddKeepsEarliestDuplicate) {
  Fields f;
  for (size_t i = 0; i < 30; i++) f.Add("a" + std::to_string(i));
  size_t first = f.Add("dup");
  FieldNameTable_Rebuild(&table, f.data(), f.size());
  size_t second = f.Add("dup");
  FieldNameTable_Add(&table, f.data(), second);
  ASSERT_EQ(Find(f, "dup"), f.data() + first);
}

TEST_F(FieldNameTableTest, EmbeddedNulFollowsStrncmpSemantics) {
  Fields f;
  for (size_t i = 0; i < 20; i++) f.Add("pad" + std::to_string(i));
  size_t a = f.Add(std::string("ab\0c", 4));
  f.Add(std::string("ab\0d", 4));
  FieldNameTable_Rebuild(&table, f.data(), f.size());
  for (const std::string &q : {std::string("ab\0c", 4), std::string("ab\0d", 4), std::string("ab\0z", 4),
                               std::string("ab", 2), std::string("ab\0", 3), std::string("abc", 3)}) {
    ASSERT_EQ(Find(f, q), f.LinearFind(q.data(), q.size())) << q.size();
  }
  // Same length and same bytes up to the first NUL compare equal, so the earliest wins.
  ASSERT_EQ(Find(f, std::string("ab\0d", 4)), f.data() + a);
}

TEST_F(FieldNameTableTest, QueryLengthShorterThanTheBuffer) {
  Fields f;
  for (size_t i = 0; i < 30; i++) f.Add("field_" + std::to_string(i));
  FieldNameTable_Rebuild(&table, f.data(), f.size());
  const char *buf = "field_10xyz";
  // Only the first `len` bytes of the query take part.
  ASSERT_EQ(FieldNameTable_Find(&table, f.data(), f.size(), buf, 8), f.data() + 10);
  ASSERT_EQ(FieldNameTable_Find(&table, f.data(), f.size(), buf, 7), f.data() + 1);
  ASSERT_EQ(FieldNameTable_Find(&table, f.data(), f.size(), buf, 9), nullptr);
  ASSERT_EQ(FieldNameTable_Find(&table, f.data(), f.size(), buf, 0), nullptr);
}

TEST_F(FieldNameTableTest, RandomizedAgreesWithLinearScan) {
  std::mt19937 rng(12345);
  auto random_name = [&rng](size_t max_len) {
    size_t len = 1 + rng() % max_len;
    std::string s;
    static const char alphabet[] = {'a', 'b', 'A', 'B', '_', '\0'};
    for (size_t i = 0; i < len; i++) s.push_back(alphabet[rng() % sizeof(alphabet)]);
    return s;
  };
  for (int round = 0; round < 200; round++) {
    Fields f;
    size_t n = rng() % 200;
    // Short names from a tiny alphabet so duplicates and prefixes are common.
    for (size_t i = 0; i < n; i++) f.Add(random_name(round % 2 ? 3 : 8));
    FieldNameTable_Rebuild(&table, f.data(), f.size());
    for (int q = 0; q < 300; q++) {
      std::string name = random_name(round % 2 ? 3 : 8);
      ASSERT_EQ(Find(f, name), f.LinearFind(name.data(), name.size()))
          << "round " << round << " n " << n;
    }
    for (size_t i = 0; i < n; i++) {
      const HiddenString *hs = f.data()[i].fieldName;
      size_t len;
      const char *s = HiddenString_GetUnsafe(hs, &len);
      ASSERT_EQ(FieldNameTable_Find(&table, f.data(), f.size(), s, len), f.LinearFind(s, len));
    }
  }
}
