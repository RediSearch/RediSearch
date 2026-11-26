/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#include "gtest/gtest.h"

// The parser is exposed only under ENABLE_ASSERT (Debug builds); in Release this translation unit
// compiles to nothing, so `rstest` still links.
#ifdef ENABLE_ASSERT

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <limits>
#include <random>
#include <string>
#include <utility>
#include <vector>

#include "document.h"
#include "json_test_api.h"
#include "redismock/internal.h"
#include "rmalloc.h"

namespace {

// A JSON value as RedisJSON hands it to the parser. Objects keep their keys in insertion order,
// as RedisJSON does, so a test controls the order the parser sees them in.
// A key/value is a struct rather than `std::pair` because `std::vector` accepts an incomplete
// element type but `std::pair` does not; with libstdc++ the pair form fails to compile.
struct Field;
struct Node {
  JSONType type = JSONType_Null;
  long long integer = 0;
  double number = 0;
  std::string string;
  std::vector<Node> array;
  std::vector<Field> object;
};
struct Field {
  std::string first;
  Node second;
};

Node Int(long long v) {
  Node n;
  n.type = JSONType_Int;
  n.integer = v;
  return n;
}
Node Dbl(double v) {
  Node n;
  n.type = JSONType_Double;
  n.number = v;
  return n;
}
Node Str(const char *v) {
  Node n;
  n.type = JSONType_String;
  n.string = v;
  return n;
}
Node Bool() {
  Node n;
  n.type = JSONType_Bool;
  return n;
}
Node Null() {
  return Node{};
}
Node Arr(std::vector<Node> items) {
  Node n;
  n.type = JSONType_Array;
  n.array = std::move(items);
  return n;
}
Node Obj(std::vector<Field> fields) {
  Node n;
  n.type = JSONType_Object;
  n.object = std::move(fields);
  return n;
}
Node Ints(std::vector<long long> v) {
  std::vector<Node> items;
  for (long long x : v) items.push_back(Int(x));
  return Arr(std::move(items));
}
Node Dbls(std::vector<double> v) {
  std::vector<Node> items;
  for (double x : v) items.push_back(Dbl(x));
  return Arr(std::move(items));
}

using Pairs = std::vector<std::pair<uint32_t, float>>;

class JSONSparseVectorTest : public ::testing::Test {
 protected:
  struct KeyValues {
    const Node *object;
    size_t position = 0;
  };

  inline static JSONSparseVectorTest *current = nullptr;
  RedisJSONAPI api = {};
  RedisJSONAPI *savedApi = nullptr;
  int savedVersion = 0;
  int liveHandles = 0, liveIterators = 0;
  bool failGetLen = false;
  // Every key string handed out, with one extra reference held here, so TearDown can check the
  // parser released its own.
  std::vector<RedisModuleString *> keys;

  static const Node *node(RedisJSON json) {
    return static_cast<const Node *>(json);
  }

  void SetUp() override {
    current = this;
    savedApi = japi;
    savedVersion = japi_ver;
    api.getType = [](RedisJSON json) { return node(json)->type; };
    api.getLen = [](RedisJSON json, size_t *count) {
      const Node *n = node(json);
      if (current->failGetLen) {
        return REDISMODULE_ERR;
      } else if (n->type == JSONType_Array) {
        *count = n->array.size();
      } else if (n->type == JSONType_Object) {
        *count = n->object.size();
      } else {
        return REDISMODULE_ERR;
      }
      return REDISMODULE_OK;
    };
    api.getAt = [](RedisJSON json, size_t index, RedisJSONPtr ptr) {
      const Node *n = node(json);
      if (n->type != JSONType_Array || index >= n->array.size()) return REDISMODULE_ERR;
      *ptr = &n->array[index];
      return REDISMODULE_OK;
    };
    // Like RedisJSON, an integer is not read as a double, nor a double as an integer.
    api.getInt = [](RedisJSON json, long long *out) {
      if (node(json)->type != JSONType_Int) return REDISMODULE_ERR;
      *out = node(json)->integer;
      return REDISMODULE_OK;
    };
    api.getDouble = [](RedisJSON json, double *out) {
      if (node(json)->type != JSONType_Double) return REDISMODULE_ERR;
      *out = node(json)->number;
      return REDISMODULE_OK;
    };
    api.allocJson = []() -> RedisJSONPtr {
      ++current->liveHandles;
      return new RedisJSON(nullptr);
    };
    api.freeJson = [](RedisJSONPtr ptr) {
      --current->liveHandles;
      delete ptr;
    };
    api.getKeyValues = [](RedisJSON json) -> JSONKeyValuesIterator {
      EXPECT_EQ(node(json)->type, JSONType_Object);
      ++current->liveIterators;
      return new KeyValues{node(json)};
    };
    api.nextKeyValue = [](JSONKeyValuesIterator it, RedisModuleString **key, RedisJSONPtr ptr) {
      auto *kv = const_cast<KeyValues *>(static_cast<const KeyValues *>(it));
      if (kv->position == kv->object->object.size()) return REDISMODULE_ERR;
      const auto &field = kv->object->object[kv->position++];
      *key = RedisModule_CreateString(nullptr, field.first.data(), field.first.size());
      (*key)->incref();
      current->keys.push_back(*key);
      *ptr = &field.second;
      return REDISMODULE_OK;
    };
    api.freeKeyValuesIter = [](JSONKeyValuesIterator it) {
      --current->liveIterators;
      delete static_cast<const KeyValues *>(it);
    };
    japi = &api;
    japi_ver = 10;
  }

  void TearDown() override {
    EXPECT_EQ(liveHandles, 0) << "a JSON handle was not freed";
    EXPECT_EQ(liveIterators, 0) << "a key/value iterator was not freed";
    for (RedisModuleString *key : keys) {
      EXPECT_EQ(key->refcount, 1u) << "key `" << *key << "` was not freed";
      key->decref();
    }
    japi = savedApi;
    japi_ver = savedVersion;
    current = nullptr;
  }

  // Parse `value` and return the pairs it was stored as.
  Pairs parse(const Node &value) {
    DocumentField df = {};
    QueryError status = QueryError_Default();
    int rv = JSONTest_StoreSparseVectorInDocField(&value, &df, &status);
    EXPECT_EQ(rv, REDISMODULE_OK) << QueryError_GetUserError(&status);
    QueryError_ClearError(&status);
    Pairs pairs;
    if (rv != REDISMODULE_OK) return pairs;
    EXPECT_EQ(df.unionType, FLD_VAR_T_CSTR);
    // Non-NULL even for an empty vector: the indexer reads an absent value as a missing field.
    EXPECT_NE(df.strval, nullptr);
    EXPECT_EQ(df.strlen % 8, 0u);
    for (size_t off = 0; off < df.strlen; off += 8) {
      uint32_t id;
      float weight;
      memcpy(&id, df.strval + off, sizeof(id));
      memcpy(&weight, df.strval + off + 4, sizeof(weight));
      pairs.emplace_back(id, weight);
    }
    rm_free(df.strval);
    return pairs;
  }

  // Parse `value`, expect it rejected, and return the user-facing error.
  std::string reject(const Node &value) {
    DocumentField df = {};
    QueryError status = QueryError_Default();
    EXPECT_EQ(JSONTest_StoreSparseVectorInDocField(&value, &df, &status), REDISMODULE_ERR);
    EXPECT_EQ(df.strval, nullptr) << "a rejected value must not be stored";
    std::string error = QueryError_GetUserError(&status);
    QueryError_ClearError(&status);
    return error;
  }
};

#define EXPECT_REJECTED(value, substring)                                        \
  do {                                                                           \
    std::string error_ = reject(value);                                          \
    EXPECT_NE(error_.find(substring), std::string::npos) << "error: " << error_; \
  } while (0)

// ---------------------------------------------------------------------------
// Arrays shape: {"indices": [...], "values": [...]}
// ---------------------------------------------------------------------------

TEST_F(JSONSparseVectorTest, arraysPairIndicesWithValues) {
  EXPECT_EQ(parse(Obj({{"indices", Ints({3, 10})}, {"values", Dbls({0.5, 2.25})}})),
            (Pairs{{3, 0.5f}, {10, 2.25f}}));
}

TEST_F(JSONSparseVectorTest, arraysKeyOrderDoesNotMatter) {
  EXPECT_EQ(parse(Obj({{"values", Dbls({0.5, 2.25})}, {"indices", Ints({3, 10})}})),
            (Pairs{{3, 0.5f}, {10, 2.25f}}));
}

TEST_F(JSONSparseVectorTest, arraysAcceptIntegerWeights) {
  EXPECT_EQ(parse(Obj({{"indices", Ints({1, 2})}, {"values", Arr({Int(7), Dbl(-1.5)})}})),
            (Pairs{{1, 7.0f}, {2, -1.5f}}));
}

TEST_F(JSONSparseVectorTest, arraysKeepTheirOrder) {
  // Ordering is checked at indexing, so the parser must not hide a wrong order by sorting.
  EXPECT_EQ(parse(Obj({{"indices", Ints({9, 4})}, {"values", Dbls({1, 2})}})),
            (Pairs{{9, 1.0f}, {4, 2.0f}}));
}

TEST_F(JSONSparseVectorTest, arraysCanBeEmpty) {
  EXPECT_EQ(parse(Obj({{"indices", Arr({})}, {"values", Arr({})}})), Pairs{});
}

TEST_F(JSONSparseVectorTest, arraysAcceptTheFullDimensionIdRange) {
  EXPECT_EQ(parse(Obj({{"indices", Ints({0, UINT32_MAX})}, {"values", Dbls({1, 2})}})),
            (Pairs{{0, 1.0f}, {UINT32_MAX, 2.0f}}));
}

TEST_F(JSONSparseVectorTest, arraysRejectMismatchedLengths) {
  EXPECT_REJECTED(Obj({{"indices", Ints({1, 2})}, {"values", Dbls({1})}}),
                  "must have the same length");
  EXPECT_REJECTED(Obj({{"values", Dbls({1})}, {"indices", Ints({1, 2})}}),
                  "must have the same length");
  EXPECT_REJECTED(Obj({{"indices", Arr({})}, {"values", Dbls({1})}}), "must have the same length");
}

TEST_F(JSONSparseVectorTest, arraysRejectAMissingArray) {
  EXPECT_REJECTED(Obj({{"indices", Ints({1})}}), "must have `indices` and `values` arrays");
  EXPECT_REJECTED(Obj({{"values", Dbls({1})}}), "must have `indices` and `values` arrays");
}

TEST_F(JSONSparseVectorTest, arraysRejectANonArray) {
  for (const Node &bad : {Int(1), Str("1"), Obj({}), Null()}) {
    EXPECT_REJECTED(Obj({{"indices", bad}, {"values", Dbls({1})}}),
                    "indices and values must be arrays");
    EXPECT_REJECTED(Obj({{"indices", Ints({1})}, {"values", bad}}),
                    "indices and values must be arrays");
  }
}

TEST_F(JSONSparseVectorTest, arraysRejectABadDimensionId) {
  long long tooBig = static_cast<long long>(UINT32_MAX) + 1;
  for (const Node &bad : {Int(-1), Int(tooBig), Int(std::numeric_limits<long long>::min()),
                          Dbl(1.0), Dbl(1.5), Str("1"), Bool(), Null(), Arr({})}) {
    EXPECT_REJECTED(Obj({{"indices", Arr({Int(0), bad})}, {"values", Dbls({1, 1})}}),
                    "Invalid sparse vector index at index 1");
  }
}

TEST_F(JSONSparseVectorTest, arraysRejectABadWeight) {
  for (const Node &bad : {Str("1"), Bool(), Null(), Arr({}), Obj({})}) {
    EXPECT_REJECTED(Obj({{"indices", Ints({0, 1})}, {"values", Arr({Dbl(1), bad})}}),
                    "Invalid sparse vector value at index 1");
  }
}

TEST_F(JSONSparseVectorTest, arraysRejectOtherKeys) {
  // Wherever it sits, a third key makes the object neither shape, so it is never half-read.
  const char *msg = "cannot have other keys";
  EXPECT_REJECTED(Obj({{"indices", Ints({1})}, {"values", Dbls({1})}, {"5", Dbl(1)}}), msg);
  EXPECT_REJECTED(Obj({{"5", Dbl(1)}, {"indices", Ints({1})}, {"values", Dbls({1})}}), msg);
  EXPECT_REJECTED(Obj({{"indices", Ints({1})}, {"meta", Str("x")}, {"values", Dbls({1})}}), msg);
  // One of the two keys is enough to pick the arrays shape.
  EXPECT_REJECTED(Obj({{"values", Dbls({1})}, {"7", Dbl(1)}}), msg);
}

TEST_F(JSONSparseVectorTest, eachShapeIsReadInOnePass) {
  // The arrays shape is read in one pass after a single key of the map reader; a map is read
  // in a single pass.
  parse(Obj({{"indices", Ints({1})}, {"values", Dbls({1})}}));
  EXPECT_EQ(keys.size(), 1u + 2u);
  size_t before = keys.size();
  parse(Obj({{"1", Dbl(1)}, {"2", Dbl(1)}, {"3", Dbl(1)}}));
  EXPECT_EQ(keys.size() - before, 3u);
}

TEST_F(JSONSparseVectorTest, mixedObjectErrorDependsOnKeyOrder) {
  // Rejected either way. Before an arrays key, a bad key is read as a map key.
  EXPECT_REJECTED(Obj({{"bad", Dbl(1)}, {"indices", Ints({1})}, {"values", Dbls({1})}}),
                  "Invalid sparse vector dimension id: `bad`");
  EXPECT_REJECTED(Obj({{"indices", Ints({1})}, {"bad", Dbl(1)}, {"values", Dbls({1})}}),
                  "cannot have other keys");
}

TEST_F(JSONSparseVectorTest, unreadableObjectIsAnError) {
  failGetLen = true;
  EXPECT_REJECTED(Obj({{"1", Dbl(1)}}), "Could not read sparse vector object");
}

TEST_F(JSONSparseVectorTest, arraysStopAtTheFirstError) {
  // A later bad array must not overwrite the first error.
  EXPECT_REJECTED(Obj({{"indices", Ints({-1})}, {"values", Str("x")}}),
                  "Invalid sparse vector index at index 0");
}

// ---------------------------------------------------------------------------
// Map shape: {"<dim_id>": weight, ...}
// ---------------------------------------------------------------------------

TEST_F(JSONSparseVectorTest, mapIsSortedByDimensionId) {
  EXPECT_EQ(parse(Obj({{"42", Dbl(1.25)}, {"3", Dbl(0.5)}, {"10", Int(2)}})),
            (Pairs{{3, 0.5f}, {10, 2.0f}, {42, 1.25f}}));
}

TEST_F(JSONSparseVectorTest, mapMatchesTheArraysShape) {
  Pairs fromArrays = parse(Obj({{"indices", Ints({3, 10, 42})}, {"values", Dbls({0.5, 2, 1.25})}}));
  Pairs fromMap = parse(Obj({{"10", Dbl(2)}, {"42", Dbl(1.25)}, {"3", Dbl(0.5)}}));
  EXPECT_EQ(fromMap, fromArrays);
}

TEST_F(JSONSparseVectorTest, mapCanBeEmpty) {
  EXPECT_EQ(parse(Obj({})), Pairs{});
}

TEST_F(JSONSparseVectorTest, mapAcceptsTheFullDimensionIdRange) {
  EXPECT_EQ(parse(Obj({{"4294967295", Dbl(2)}, {"0", Dbl(1)}})),
            (Pairs{{0, 1.0f}, {UINT32_MAX, 2.0f}}));
}

TEST_F(JSONSparseVectorTest, mapRejectsANonCanonicalDimensionId) {
  // Only the canonical form is accepted, so two distinct keys never name the same dimension.
  for (const char *key : {"", "-1", "+1", "01", "00", "1.5", "1e3", " 1", "1 ", "0x1", "abc",
                          "Indices", "4294967296", "99999999999999999999"}) {
    std::string error = reject(Obj({{"1", Dbl(1)}, {key, Dbl(1)}}));
    EXPECT_NE(error.find("Invalid sparse vector dimension id"), std::string::npos)
        << "key `" << key << "`: " << error;
  }
}

TEST_F(JSONSparseVectorTest, mapErrorNamesTheKey) {
  EXPECT_REJECTED(Obj({{"07", Dbl(1)}}), "`07`");
}

TEST_F(JSONSparseVectorTest, mapErrorHintsAtAMisspelledArraysKey) {
  const char *hint = "misspelled `indices` or `values` key";
  EXPECT_REJECTED(Obj({{"indice", Ints({1, 2})}, {"value", Dbls({0.5, 0.7})}}), hint);
  EXPECT_REJECTED(Obj({{"1", Dbl(1)}, {"vals", Dbls({1})}}), hint);
  // Only an array value suggests the arrays shape.
  std::string error = reject(Obj({{"token", Dbl(1)}}));
  EXPECT_EQ(error.find(hint), std::string::npos) << error;
}

TEST_F(JSONSparseVectorTest, mapRejectsABadWeight) {
  for (const Node &bad : {Str("1"), Bool(), Null(), Arr({Dbl(1)}), Obj({})}) {
    EXPECT_REJECTED(Obj({{"3", Dbl(1)}, {"8", bad}}),
                    "Invalid sparse vector value for dimension id 8");
  }
}

TEST_F(JSONSparseVectorTest, mapStopsAtTheFirstError) {
  EXPECT_REJECTED(Obj({{"x", Str("bad")}, {"y", Dbl(1)}}), "`x`");
}

TEST_F(JSONSparseVectorTest, mapPassesNonFiniteWeightsToIndexing) {
  // Finiteness is checked at indexing for every shape, so the parser stores what it read.
  Pairs pairs = parse(Obj({{"1", Dbl(1e39)}, {"2", Dbl(-1e39)}}));
  ASSERT_EQ(pairs.size(), 2u);
  EXPECT_TRUE(std::isinf(pairs[0].second) && pairs[0].second > 0);
  EXPECT_TRUE(std::isinf(pairs[1].second) && pairs[1].second < 0);
}

TEST_F(JSONSparseVectorTest, mapLargeShuffledVectorIsSorted) {
  std::vector<uint32_t> ids(2000);
  std::mt19937 rng(42);
  for (size_t i = 0; i < ids.size(); ++i) ids[i] = static_cast<uint32_t>(i * 7919 + 13);
  std::shuffle(ids.begin(), ids.end(), rng);
  std::vector<Field> fields;
  for (uint32_t id : ids) fields.push_back({std::to_string(id), Dbl(id / 2.0)});

  Pairs pairs = parse(Obj(std::move(fields)));
  ASSERT_EQ(pairs.size(), ids.size());
  std::sort(ids.begin(), ids.end());
  for (size_t i = 0; i < ids.size(); ++i) {
    EXPECT_EQ(pairs[i].first, ids[i]);
    EXPECT_EQ(pairs[i].second, static_cast<float>(ids[i] / 2.0));
  }
}

}  // namespace

#endif  // ENABLE_ASSERT
