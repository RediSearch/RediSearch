/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#include "gtest/gtest.h"
#include "gtest/internal/gtest-port.h"  // For CaptureStderr/GetCapturedStderr
#include "document.h"
#include "indexes.h"
#include "json.h"
#include "redismock/util.h"
#include "rmutil/args.h"

#include <cstring>
#include <map>
#include <optional>
#include <set>
#include <string>
#include <vector>

namespace {

// Literal path-to-value fixtures exercise indexing without duplicating JSONPath parsing.
class JSONPathIndexingTest : public ::testing::Test {
 protected:
  struct Value {
    double number = 0;
    const char *string = nullptr;
    // Reported by getString instead of strlen, to model RedisJSON strings, which are not
    // NUL-terminated.
    std::optional<size_t> length;
  };
  using Root = std::map<std::string, Value>;
  struct Iterator {
    std::vector<const Value *> values;
    size_t position = 0;
  };

  inline static JSONPathIndexingTest *current = nullptr;
  RedisModuleCtx *ctx = nullptr;
  RedisJSONAPI api = {};
  Root *root = nullptr;
  std::set<const std::string *> paths;
  std::set<const Iterator *> iterators;
  int parses = 0, stringGets = 0;
  std::vector<StrongRef> specs;
  RedisJSONAPI *savedApi = nullptr;
  int savedVersion = 0;

  static JSONPath parse(const char *path) {
    ++current->parses;
    auto *compiled = new std::string(path);
    current->paths.insert(compiled);
    return compiled;
  }

  static void freePath(JSONPath path) {
    auto *compiled = static_cast<const std::string *>(path);
    ASSERT_EQ(current->paths.erase(compiled), 1u);
    delete compiled;
  }

  static JSONResultsIterator evaluate(RedisJSON json, const std::string &path) {
    auto *doc = static_cast<const Root *>(json);
    auto *iter = new Iterator;
    auto found = doc->find(path);
    if (found != doc->end()) {
      iter->values.push_back(&found->second);
    }
    current->iterators.insert(iter);
    return iter;
  }

  static JSONResultsIterator get(RedisJSON json, const char *path) {
    ++current->stringGets;
    return evaluate(json, path);
  }

  static JSONResultsIterator getWithPath(RedisJSON json, JSONPath path) {
    auto *compiled = static_cast<const std::string *>(path);
    EXPECT_EQ(current->paths.count(compiled), 1u);
    return evaluate(json, *compiled);
  }

  static RedisJSON next(JSONResultsIterator results) {
    auto *iter = const_cast<Iterator *>(static_cast<const Iterator *>(results));
    return iter->position < iter->values.size() ? iter->values[iter->position++] : nullptr;
  }

  static size_t length(JSONResultsIterator results) {
    return static_cast<const Iterator *>(results)->values.size();
  }

  static void freeIterator(JSONResultsIterator results) {
    auto *iter = static_cast<const Iterator *>(results);
    ASSERT_EQ(current->iterators.erase(iter), 1u);
    delete iter;
  }

  static int getDouble(RedisJSON json, double *out) {
    auto *value = static_cast<const Value *>(json);
    if (value->string) return REDISMODULE_ERR;
    *out = value->number;
    return REDISMODULE_OK;
  }

  static int getString(RedisJSON json, const char **out, size_t *len) {
    auto *value = static_cast<const Value *>(json);
    if (!value->string) return REDISMODULE_ERR;
    *out = value->string;
    *len = value->length.value_or(strlen(*out));
    return REDISMODULE_OK;
  }

  void SetUp() override {
    current = this;
    ctx = RedisModule_GetThreadSafeContext(nullptr);
    savedApi = japi;
    savedVersion = japi_ver;
    api.get = get;
    api.getWithPath = getWithPath;
    api.pathParse = [](const char *path, RedisModuleCtx *, RedisModuleString **) {
      return parse(path);
    };
    api.pathParseNoErrorMsg = parse;
    api.pathFree = freePath;
    api.pathIsSingle = [](JSONPath) { return 1; };
    api.pathHasDefinedOrder = [](JSONPath) { return 1; };
    api.next = next;
    api.len = length;
    api.freeIter = freeIterator;
    api.getDouble = getDouble;
    api.getString = getString;
    api.getType = [](RedisJSON json) {
      return static_cast<const Value *>(json)->string ? JSONType_String : JSONType_Double;
    };
    api.getJsonFromHandle = [](RedisModuleKey *) -> RedisJSON { return current->root; };
    japi = &api;
    japi_ver = 10;
  }

  void TearDown() override {
    for (auto ref : specs) Indexes_RemoveSpecFromGlobals(ref, false);
    EXPECT_TRUE(paths.empty());
    EXPECT_TRUE(iterators.empty());
    for (auto *path : paths) delete path;
    for (auto *iter : iterators) delete iter;
    japi = savedApi;
    japi_ver = savedVersion;
    RedisModule_FreeThreadSafeContext(ctx);
    current = nullptr;
  }

  IndexSpec *createSpec(std::vector<const char *> args) {
    static int counter = 0;
    std::string name = "json_path_cache_" + std::to_string(++counter);
    QueryError error = QueryError_Default();
    StrongRef ref = IndexSpec_ParseC(ctx, name.c_str(), args.data(), args.size(), &error);
    auto *spec = static_cast<IndexSpec *>(StrongRef_Get(ref));
    EXPECT_FALSE(QueryError_HasError(&error)) << QueryError_GetUserError(&error);
    QueryError_ClearError(&error);
    if (spec) {
      Spec_AddToDict(ref.rm);
      specs.push_back(ref);
      spec->monitorDocumentExpiration = false;
    }
    return spec;
  }
};

TEST_F(JSONPathIndexingTest, schemaFieldsLoadThroughUpdatesAndAlter) {
  auto *spec = createSpec({"ON", "JSON", "SCHEMA", "$.price", "AS", "first", "NUMERIC", "$.price",
                           "AS", "second", "NUMERIC"});
  ASSERT_NE(spec, nullptr);
  EXPECT_EQ(parses, 2);

  Root doc = {{"$.price", {10}}, {"$.extra", {30}}};
  root = &doc;
  RMCK::RString keyName("json_cache_doc");
  RedisModuleKey *key = RedisModule_OpenKey(ctx, keyName, REDISMODULE_READ);
  RedisSearchCtx sctx = SEARCH_CTX_STATIC(ctx, spec);
  for (double price : {10, 20}) {
    doc["$.price"].number = price;
    EXPECT_EQ(Document_ProbeFieldsPresent(spec, key, DocumentType_Json, 0, 2),
              DOCUMENT_FIELDS_PRESENT);
    Document loaded = {};
    Document_Init(&loaded, keyName, 1, DEFAULT_LANGUAGE, DocumentType_Json);
    QueryError error = QueryError_Default();
    ASSERT_EQ(Document_LoadSchemaFieldJson(&loaded, &sctx, key, &error), REDISMODULE_OK);
    ASSERT_EQ(loaded.numFields, 2);
    EXPECT_EQ(loaded.fields[0].numval, price);
    EXPECT_EQ(loaded.fields[1].numval, price);
    Document_Free(&loaded);
    QueryError_ClearError(&error);
  }

  const char *args[] = {"$.extra", "NUMERIC"};
  ArgsCursor ac;
  ArgsCursor_InitCString(&ac, args, 2);
  QueryError alterError = QueryError_Default();
  ASSERT_EQ(IndexSpec_AddFields(specs.back(), spec, ctx, &ac, &alterError), 1);
  QueryError_ClearError(&alterError);

  EXPECT_EQ(Document_ProbeFieldsPresent(spec, key, DocumentType_Json, 0, 3),
            DOCUMENT_FIELDS_PRESENT);
  Document loaded = {};
  Document_Init(&loaded, keyName, 1, DEFAULT_LANGUAGE, DocumentType_Json);
  QueryError loadError = QueryError_Default();
  ASSERT_EQ(Document_LoadSchemaFieldJson(&loaded, &sctx, key, &loadError), REDISMODULE_OK);
  ASSERT_EQ(loaded.numFields, 3);
  EXPECT_EQ(loaded.fields[0].numval, 20);
  EXPECT_EQ(loaded.fields[1].numval, 20);
  EXPECT_EQ(loaded.fields[2].numval, 30);
  Document_Free(&loaded);
  QueryError_ClearError(&loadError);

  // The existing paths survived document updates and ALTER without another compile per load.
  EXPECT_EQ(parses, 3);
  EXPECT_EQ(stringGets, 0);
  RedisModule_CloseKey(key);
}

TEST_F(JSONPathIndexingTest, languageAndScorePathsAreCompiledAndReused) {
  auto *spec = createSpec({"ON", "JSON", "LANGUAGE_FIELD", "$.lang", "SCORE_FIELD", "$.score",
                           "SCHEMA", "$.price", "NUMERIC"});
  ASSERT_NE(spec, nullptr);
  EXPECT_EQ(parses, 3);
  Root first = {{"$.lang", {0, "italian"}}, {"$.score", {0.8}}};
  Root second = {{"$.lang", {0, "french"}}, {"$.score", {0.2}}};
  EXPECT_EQ(SchemaRule_JsonLang(ctx, spec->rule, &first, "first"), RS_LANG_ITALIAN);
  EXPECT_DOUBLE_EQ(SchemaRule_JsonScore(ctx, spec->rule, &first, "first"), 0.8);
  EXPECT_EQ(SchemaRule_JsonLang(ctx, spec->rule, &second, "second"), RS_LANG_FRENCH);
  EXPECT_DOUBLE_EQ(SchemaRule_JsonScore(ctx, spec->rule, &second, "second"), 0.2);

  EXPECT_EQ(parses, 3);
  EXPECT_EQ(stringGets, 0);
}

TEST_F(JSONPathIndexingTest, emptyLanguageFallsBackToDefault) {
  auto *spec = createSpec({"ON", "JSON", "LANGUAGE", "italian", "LANGUAGE_FIELD", "$.lang",
                           "SCHEMA", "$.price", "NUMERIC"});
  ASSERT_NE(spec, nullptr);
  // An empty value whose pointer is followed by a valid language name: a lookup that reads past
  // the reported length finds English instead of falling back to the index default.
  Root doc = {{"$.lang", {0, "english", 0}}};
  testing::internal::CaptureStderr();
  EXPECT_EQ(SchemaRule_JsonLang(ctx, spec->rule, &doc, "doc"), RS_LANG_ITALIAN);
  std::string log = testing::internal::GetCapturedStderr();
  EXPECT_NE(log.find("invalid language for key doc"), std::string::npos) << log;
}

}  // namespace
