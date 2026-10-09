/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#include "gtest/gtest.h"
#include "document.h"
#include "indexes.h"
#include "json.h"
#include "redismock/util.h"
#include "rmutil/args.h"

#include <cstring>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <vector>

namespace {

// Literal path-to-value fixtures test the adapter's arguments and ownership, not JSON parsing.
class JSONPathIndexingTest : public ::testing::Test {
 protected:
  struct Value {
    double number = 0;
    const char *string = nullptr;
  };
  using Root = std::map<std::string, std::vector<Value>>;
  struct Iterator {
    std::vector<const Value *> values;
    size_t position = 0;
  };

  inline static JSONPathIndexingTest *current = nullptr;
  RedisModuleCtx *ctx = nullptr;
  RedisJSONAPI api = {};
  void *provider = nullptr;
  int version = 9;
  std::unique_ptr<unsigned char[]> prefix;
  Root *root = nullptr;
  std::set<const std::string *> paths;
  std::set<const Iterator *> iterators;
  std::map<JSONPath, int> frees;
  int parses = 0, stringGets = 0, compiledGets = 0, errorFrees = 0;
  RedisModuleString *parseError = nullptr;
  std::vector<StrongRef> specs;
  RedisJSONAPI *savedApi = nullptr;
  int savedVersion = 0;
  decltype(RedisModule_GetSharedAPI) savedGetSharedAPI = nullptr;
  decltype(RedisModule_FreeString) savedFreeString = nullptr;

  static void *getSharedAPI(RedisModuleCtx *, const char *name) {
    return name == "RedisJSON_V" + std::to_string(current->version) ? current->provider : nullptr;
  }

  static JSONPath parse(const char *path, RedisModuleCtx *ctx, RedisModuleString **error) {
    ++current->parses;
    if (!strcmp(path, "$.invalid[")) {
      *error = RedisModule_CreateString(ctx, "invalid path", 12);
      current->parseError = *error;
      return nullptr;
    }
    auto *compiled = new std::string(path);
    current->paths.insert(compiled);
    return compiled;
  }

  static void freePath(JSONPath path) {
    auto *compiled = static_cast<const std::string *>(path);
    ASSERT_EQ(current->paths.erase(compiled), 1u);
    ++current->frees[path];
    delete compiled;
  }

  static void freeString(RedisModuleCtx *ctx, RedisModuleString *str) {
    if (str == current->parseError) {
      ++current->errorFrees;
      current->parseError = nullptr;
    }
    current->savedFreeString(ctx, str);
  }

  static JSONResultsIterator evaluate(RedisJSON json, const std::string &path) {
    auto *doc = static_cast<const Root *>(json);
    auto *iter = new Iterator;
    auto found = doc->find(path);
    if (found != doc->end()) {
      for (const auto &value : found->second) {
        iter->values.push_back(&value);
      }
    }
    current->iterators.insert(iter);
    return iter;
  }

  static JSONResultsIterator get(RedisJSON json, const char *path) {
    ++current->stringGets;
    return evaluate(json, path);
  }

  static JSONResultsIterator getWithPath(RedisJSON json, JSONPath path) {
    ++current->compiledGets;
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
    *len = strlen(*out);
    return REDISMODULE_OK;
  }

  void SetUp() override {
    current = this;
    ctx = RedisModule_GetThreadSafeContext(nullptr);
    savedApi = japi;
    savedVersion = japi_ver;
    savedGetSharedAPI = RedisModule_GetSharedAPI;
    savedFreeString = RedisModule_FreeString;
    RedisModule_GetSharedAPI = getSharedAPI;
    RedisModule_FreeString = freeString;
    api.get = get;
    api.getWithPath = getWithPath;
    api.pathParse = parse;
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
    acquire(9);
  }

  void TearDown() override {
    for (auto ref : specs) Indexes_RemoveSpecFromGlobals(ref, false);
    EXPECT_TRUE(paths.empty());
    EXPECT_TRUE(iterators.empty());
    for (auto *path : paths) delete path;
    for (auto *iter : iterators) delete iter;
    if (parseError) RedisModule_FreeString(ctx, parseError);
    // Restore the private dispatch through negotiation as well as the public globals.
    version = savedApi ? savedVersion : 7;
    provider = savedApi ? savedApi : &api;
    EXPECT_EQ(GetJSONAPIs(ctx, 0), 1);
    japi = savedApi;
    japi_ver = savedVersion;
    RedisModule_GetSharedAPI = savedGetSharedAPI;
    RedisModule_FreeString = savedFreeString;
    RedisModule_FreeThreadSafeContext(ctx);
    current = nullptr;
  }

  void acquire(int requestedVersion, bool compiled = true) {
    version = requestedVersion;
    api.getWithPath = compiled ? getWithPath : nullptr;
    size_t size = version == 7   ? offsetof(RedisJSONAPI, getJsonFromHandle)
                  : version == 8 ? offsetof(RedisJSONAPI, getWithPath)
                                 : sizeof(api);
    // Older providers really end at their version's prefix, including under ASan.
    prefix = std::make_unique<unsigned char[]>(size);
    memcpy(prefix.get(), &api, size);
    provider = prefix.get();
    ASSERT_EQ(GetJSONAPIs(ctx, 0), 1);
    ASSERT_EQ(japi_ver, version);
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

  static std::vector<double> numbers(JSONResultsIterator iter) {
    std::vector<double> result;
    EXPECT_NE(iter, nullptr);
    if (!iter) return result;
    while (auto value = japi->next(iter)) {
      double number = 0;
      EXPECT_EQ(japi->getDouble(value, &number), REDISMODULE_OK);
      result.push_back(number);
    }
    japi->freeIter(iter);
    return result;
  }
};

TEST_F(JSONPathIndexingTest, compiledEvaluationMatchesStringAndReusesPathAcrossRoots) {
  Root first = {{"$.nested.price", {{10}}}, {"$.items[*]", {{1}, {2}}}};
  Root second = {{"$.nested.price", {{20}}}, {"$.items[*]", {}}, {"$.missing", {{5}}}};
  for (const char *path : {"$.nested.price", "$.items[*]", "$.missing"}) {
    JSONPath compiled = nullptr;
    for (const Root *doc : {&first, &second}) {
      auto legacy = japi->get(doc, path);
      auto cached = JSON_GetWithCachedPath(doc, path, &compiled);
      EXPECT_NE(legacy, cached);
      EXPECT_EQ(numbers(legacy), numbers(cached));
    }
    japi->pathFree(compiled);
  }
  EXPECT_EQ(parses, 3);
  EXPECT_EQ(compiledGets, 6);
  EXPECT_EQ(stringGets, 6);
}

TEST_F(JSONPathIndexingTest, olderApisAndMissingV9CallbackUseStringEvaluation) {
  Root doc = {{"$.price", {{12}}}};
  for (int supportedVersion : {7, 8, 9}) {
    acquire(supportedVersion, false);
    JSONPath compiled = nullptr;
    EXPECT_EQ(numbers(JSON_GetWithCachedPath(&doc, "$.price", &compiled)), std::vector<double>{12});
    EXPECT_EQ(compiled, nullptr);
  }
  EXPECT_EQ(parses, 0);
  EXPECT_EQ(compiledGets, 0);
  EXPECT_EQ(stringGets, 3);
}

TEST_F(JSONPathIndexingTest, failedCompilationReleasesErrorAndRetries) {
  Root doc;
  JSONPath compiled = nullptr;
  for (int attempt = 0; attempt < 2; ++attempt) {
    EXPECT_EQ(JSON_GetWithCachedPath(&doc, "$.invalid[", &compiled), nullptr);
    EXPECT_EQ(compiled, nullptr);
  }
  EXPECT_EQ(parses, 2);
  EXPECT_EQ(errorFrees, 2);
  EXPECT_EQ(compiledGets, 0);
  EXPECT_EQ(stringGets, 0);
}

TEST_F(JSONPathIndexingTest, schemaLoaderAndAlterProbeReuseIndependentFieldHandles) {
  auto *spec = createSpec({"ON", "JSON", "SCHEMA", "$.price", "AS", "first", "NUMERIC", "$.price",
                           "AS", "second", "NUMERIC"});
  ASSERT_NE(spec, nullptr);
  parses = 0;  // Schema validation parses are unrelated to indexing reuse.
  Root doc = {{"$.price", {{10}}}};
  root = &doc;
  RMCK::RString keyName("json_cache_doc");
  RedisModuleKey *key = RedisModule_OpenKey(ctx, keyName, REDISMODULE_READ);
  RedisSearchCtx sctx = SEARCH_CTX_STATIC(ctx, spec);
  for (double price : {10, 20}) {
    doc["$.price"][0].number = price;
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
  RedisModule_CloseKey(key);
  EXPECT_EQ(parses, 2);
  EXPECT_EQ(compiledGets, 6);
  EXPECT_EQ(stringGets, 0);
  EXPECT_NE(spec->fields[0].compiledPath, spec->fields[1].compiledPath);
}

TEST_F(JSONPathIndexingTest, rulePathsReuseHandlesAndPreserveValuesAndDefaults) {
  auto *spec = createSpec({"ON", "JSON", "LANGUAGE_FIELD", "$.lang", "SCORE_FIELD", "$.score",
                           "LANGUAGE", "english", "SCORE", "0.5", "SCHEMA", "$.price", "NUMERIC"});
  ASSERT_NE(spec, nullptr);
  parses = 0;
  Root first = {{"$.lang", {{0, "italian"}}}, {"$.score", {{0.8}}}};
  Root second = {{"$.lang", {{0, "french"}}}, {"$.score", {{0.2}}}};
  EXPECT_EQ(SchemaRule_JsonLang(ctx, spec->rule, &first, "first"), RS_LANG_ITALIAN);
  EXPECT_DOUBLE_EQ(SchemaRule_JsonScore(ctx, spec->rule, &first, "first"), 0.8);
  EXPECT_EQ(SchemaRule_JsonLang(ctx, spec->rule, &second, "second"), RS_LANG_FRENCH);
  EXPECT_DOUBLE_EQ(SchemaRule_JsonScore(ctx, spec->rule, &second, "second"), 0.2);
  Root absent;
  Root wrongTypes = {{"$.lang", {{7}}}, {"$.score", {{0, "wrong"}}}};
  for (auto *doc : {&absent, &wrongTypes}) {
    EXPECT_EQ(SchemaRule_JsonLang(ctx, spec->rule, doc, "defaults"), RS_LANG_ENGLISH);
    EXPECT_DOUBLE_EQ(SchemaRule_JsonScore(ctx, spec->rule, doc, "defaults"), 0.5);
  }
  EXPECT_EQ(parses, 2);
  EXPECT_EQ(compiledGets, 8);
  EXPECT_EQ(stringGets, 0);
  EXPECT_NE(spec->rule->compiled_lang_path, spec->rule->compiled_score_path);
}

TEST_F(JSONPathIndexingTest, snapshotsAndAlterPreserveLiveOwnershipUntilSpecCleanup) {
  auto *spec = createSpec({"ON", "JSON", "SCHEMA", "$.price", "NUMERIC"});
  ASSERT_NE(spec, nullptr);
  Root doc = {{"$.price", {{10}}}};
  numbers(JSON_GetWithCachedPath(&doc, "$.price", &spec->fields[0].compiledPath));
  JSONPath owned = spec->fields[0].compiledPath;
  int previousFrees = frees[owned];  // An allocator may reuse a schema-validation handle's address.
  IndexSpec_RefreshSpecCache(spec);
  auto old = std::unique_ptr<IndexSpecCache, decltype(&IndexSpecCache_Decref)>(
      IndexSpec_GetSpecCache(spec), IndexSpecCache_Decref);
  EXPECT_EQ(old->fields[0].compiledPath, nullptr);

  const char *args[] = {"$.extra", "NUMERIC"};
  ArgsCursor ac;
  ArgsCursor_InitCString(&ac, args, 2);
  QueryError error = QueryError_Default();
  ASSERT_EQ(IndexSpec_AddFields(specs.back(), spec, ctx, &ac, &error), 1);
  EXPECT_EQ(spec->fields[0].compiledPath, owned);
  EXPECT_EQ(spec->fields[1].compiledPath, nullptr);
  auto fresh = std::unique_ptr<IndexSpecCache, decltype(&IndexSpecCache_Decref)>(
      IndexSpec_GetSpecCache(spec), IndexSpecCache_Decref);
  EXPECT_EQ(fresh->nfields, 2);
  EXPECT_EQ(fresh->fields[0].compiledPath, nullptr);
  EXPECT_EQ(fresh->fields[1].compiledPath, nullptr);
  old.reset();
  fresh.reset();
  EXPECT_EQ(frees[owned], previousFrees);
  int before = parses;
  EXPECT_EQ(numbers(JSON_GetWithCachedPath(&doc, "$.price", &spec->fields[0].compiledPath)),
            std::vector<double>{10});
  EXPECT_EQ(parses, before);
  Indexes_RemoveSpecFromGlobals(specs.back(), false);
  specs.pop_back();
  EXPECT_EQ(frees[owned], previousFrees + 1);
  EXPECT_TRUE(paths.empty());
  QueryError_ClearError(&error);
}

TEST_F(JSONPathIndexingTest, unusedJsonAndHashOwnersHaveNoCompiledHandlesToFree) {
  for (const char *type : {"JSON", "HASH"}) {
    auto *spec = createSpec({"ON", type, "SCHEMA", "$.price", "NUMERIC"});
    ASSERT_NE(spec, nullptr);
    EXPECT_EQ(spec->fields[0].compiledPath, nullptr);
    EXPECT_EQ(spec->rule->compiled_lang_path, nullptr);
    EXPECT_EQ(spec->rule->compiled_score_path, nullptr);
    size_t before = frees.size();
    Indexes_RemoveSpecFromGlobals(specs.back(), false);
    specs.pop_back();
    EXPECT_EQ(frees.size(), before);
  }
}

}  // namespace
