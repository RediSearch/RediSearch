/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "gtest/gtest.h"
#include "field_path_table.h"

#include <string>
#include <vector>

namespace {

// Owns a FieldSpec array whose only populated member is fieldPath.
class Paths {
 public:
  ~Paths() {
    for (FieldSpec &fs : specs_) HiddenString_Free(fs.fieldPath, true);
  }
  void Add(const std::string &path) {
    FieldSpec fs = {};
    fs.fieldPath = NewHiddenString(path.data(), path.size(), true);
    specs_.push_back(fs);
  }
  const FieldSpec *data() const { return specs_.data(); }
  size_t size() const { return specs_.size(); }

 private:
  std::vector<FieldSpec> specs_;
};

std::vector<int> Chain(const FieldPathTable *t, t_fieldIndex head) {
  std::vector<int> out;
  for (t_fieldIndex p = head; p; p = FieldPathTable_Next(t, p)) out.push_back(p - 1);
  return out;
}

std::vector<int> Lookup(const FieldPathTable *t, const std::string &key) {
  return Chain(t, FieldPathTable_Find(t, key.data(), key.size()));
}

}  // namespace

TEST(FieldPathTableTest, HashPathsSharedByAliasesFormOrderedChains) {
  Paths p;
  for (const char *path : {"a", "v", "b", "v", "c", "v"}) p.Add(path);
  FieldPathTable *t = FieldPathTable_New(p.data(), p.size(), DocumentType_Hash);
  EXPECT_EQ(Lookup(t, "v"), (std::vector<int>{1, 3, 5}));
  EXPECT_EQ(Lookup(t, "a"), (std::vector<int>{0}));
  EXPECT_EQ(Lookup(t, "c"), (std::vector<int>{4}));
  EXPECT_TRUE(Lookup(t, "d").empty());
  // Exact, case sensitive and length sensitive.
  EXPECT_TRUE(Lookup(t, "V").empty());
  EXPECT_TRUE(Lookup(t, "vv").empty());
  EXPECT_TRUE(Lookup(t, "").empty());
  EXPECT_EQ(FieldPathTable_FirstGeneric(t), 0);
  FieldPathTable_Free(t);
}

TEST(FieldPathTableTest, HashLookupSeesEveryFieldOfALargeSchema) {
  Paths p;
  const int n = 5000;
  for (int i = 0; i < n; i++) p.Add("field" + std::to_string(i % 4000));
  FieldPathTable *t = FieldPathTable_New(p.data(), p.size(), DocumentType_Hash);
  for (int i = 0; i < 4000; i++) {
    std::vector<int> expect = {i};
    if (i + 4000 < n) expect.push_back(i + 4000);
    EXPECT_EQ(Lookup(t, "field" + std::to_string(i)), expect);
  }
  FieldPathTable_Free(t);
}

TEST(FieldPathTableTest, JsonRootKeyShapes) {
  struct Case {
    const char *path;
    const char *key;  // NULL when the path names no root key
  };
  const Case cases[] = {
      {"$.a", "a"},
      {"$.a.b.c", "a"},
      {"$.a[0]", "a"},
      {"$.a[*].b", "a"},
      {"$.a[?(@.x>1)]", "a"},
      {"$.Key_9", "Key_9"},
      {"$['a b']", "a b"},
      {"$[\"a b\"].c", "a b"},
      {"$", nullptr},
      {"$.", nullptr},
      {"$..a", nullptr},
      {"$.a..b", nullptr},
      {"$.*", nullptr},
      {"$[*]", nullptr},
      {"$[0]", nullptr},
      {"$.a-b", nullptr},
      {"$['a\\'b']", nullptr},
      {"$['a\"]", nullptr},
      {"$['']", nullptr},
      {"$['a'", nullptr},
      {".a", nullptr},
      {"a", nullptr},
  };
  for (const Case &c : cases) {
    SCOPED_TRACE(c.path);
    const char *key = nullptr;
    size_t len = 0;
    const bool ok = FieldPathTable_JsonRootKey(c.path, &key, &len);
    ASSERT_EQ(ok, c.key != nullptr);
    if (ok) EXPECT_EQ(std::string(key, len), c.key);
  }
}

TEST(FieldPathTableTest, JsonPathsGroupByRootKeyAndKeepOpenEndedOnesApart) {
  Paths p;
  for (const char *path : {"$.a", "$..d", "$.a.b", "$['k']", "$[*]", "$.k"}) p.Add(path);
  FieldPathTable *t = FieldPathTable_New(p.data(), p.size(), DocumentType_Json);
  EXPECT_EQ(Lookup(t, "a"), (std::vector<int>{0, 2}));
  EXPECT_EQ(Lookup(t, "k"), (std::vector<int>{3, 5}));
  EXPECT_TRUE(Lookup(t, "d").empty());
  EXPECT_EQ(Chain(t, FieldPathTable_FirstGeneric(t)), (std::vector<int>{1, 4}));
  FieldPathTable_Free(t);
}
