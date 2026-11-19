/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "gtest/gtest.h"

extern "C" {
#include "util/dict.h"
}
#include "common.h"
#include "config.h"
#include "indexer.h"
#include "indexes.h"
#include "json.h"
#include "redismock/redismock.h"
#include "spec.h"

#include <algorithm>
#include <cstring>
#include <functional>
#include <memory>
#include <string>
#include <vector>

extern "C" dict *legacySpecDict;

namespace {

// A JSON index with two NESTED groups, parsed against a JSON API that only knows which paths
// select more than one value.
class NestedGroupTest : public ::testing::Test {
 protected:
  RedisModuleCtx *ctx = nullptr;
  RedisJSONAPI api = {};
  RedisJSONAPI *savedApi = nullptr;
  int savedVersion = 0;
  bool savedSimulateInFlex = false;
  std::vector<StrongRef> specs;

  static JSONPath parse(const char *path) {
    return new std::string(path);
  }

  static bool selectsElements(JSONPath path) {
    const std::string &text = *static_cast<const std::string *>(path);
    return text.find("[*]") != std::string::npos;
  }

  void SetUp() override {
    ctx = RedisModule_GetThreadSafeContext(nullptr);
    savedApi = japi;
    savedVersion = japi_ver;
    savedSimulateInFlex = RSGlobalConfig.simulateInFlex;
    api.pathParse = [](const char *path, RedisModuleCtx *, RedisModuleString **) {
      return parse(path);
    };
    api.pathParseNoErrorMsg = parse;
    api.pathFree = [](JSONPath path) { delete static_cast<const std::string *>(path); };
    api.pathIsSingle = [](JSONPath path) -> int { return !selectsElements(path); };
    api.pathHasDefinedOrder = [](JSONPath) { return 1; };
    japi = &api;
    japi_ver = 10;
  }

  void TearDown() override {
    for (auto ref : specs) Indexes_RemoveSpecFromGlobals(ref, false);
    RSGlobalConfig.simulateInFlex = savedSimulateInFlex;
    japi = savedApi;
    japi_ver = savedVersion;
    RedisModule_FreeThreadSafeContext(ctx);
  }

  // An index with the schema `args`, or NULL and the error in `error`.
  IndexSpec *create(std::vector<const char *> args, std::string *error = nullptr) {
    static int counter = 0;
    std::string name = "nested_group_" + std::to_string(++counter);
    QueryError status = QueryError_Default();
    StrongRef ref = IndexSpec_ParseC(ctx, name.c_str(), args.data(), args.size(), &status);
    auto *spec = static_cast<IndexSpec *>(StrongRef_Get(ref));
    if (error) {
      *error = QueryError_HasError(&status) ? QueryError_GetUserError(&status) : "";
    } else {
      EXPECT_FALSE(QueryError_HasError(&status)) << QueryError_GetUserError(&status);
    }
    QueryError_ClearError(&status);
    if (spec) {
      Spec_AddToDict(ref.rm);
      specs.push_back(ref);
    }
    return spec;
  }

  // hq, the group `metrics` of k, r and v, the group `owners` of name and share, and title.
  IndexSpec *createSpec() {
    return create({"ON", "JSON", "SCHEMA",
                   "$.hq", "AS", "hq", "TAG",
                   "$.metrics[*]", "AS", "metrics", "NESTED", "3",
                   "$.k", "AS", "k", "TAG",
                   "$.r", "AS", "r", "TAG",
                   "$.v", "AS", "v", "NUMERIC",
                   "$.owners[*]", "AS", "owners", "NESTED", "2",
                   "$.name", "AS", "name", "TAG",
                   "$.share", "AS", "share", "NUMERIC",
                   "$.title", "AS", "title", "TEXT"});
  }

  static FieldSpec *field(IndexSpec *spec, const char *name) {
    return const_cast<FieldSpec *>(IndexSpec_GetFieldWithLength(spec, name, strlen(name)));
  }

  static NestedGroup *group(IndexSpec *spec, const char *name) {
    const int position = IndexSpec_FindNestedGroup(spec, name, strlen(name));
    return position < 0 ? nullptr : &spec->nestedGroups[position];
  }
};

// The positions of the groups of `createSpec`.
constexpr uint16_t kMetrics = 0, kOwners = 1;

using Io = std::unique_ptr<RedisModuleIO, decltype(&RMCK_FreeRdbIO)>;

Io saved(IndexSpec *spec) {
  Io io(RMCK_CreateRdbIO(), RMCK_FreeRdbIO);
  IndexSpec_RdbSave(io.get(), spec, 0);
  EXPECT_EQ(RMCK_IsIOError(io.get()), 0);
  return io;
}

// Sets or clears, in what `io` holds for `spec`, the marker that NESTED groups follow the fields.
void markGroups(const Io &io, const IndexSpec *spec, bool marked) {
  Io name(RMCK_CreateRdbIO(), RMCK_FreeRdbIO);
  HiddenString_SaveToRdb(spec->specName, name.get());
  uint64_t flags;
  uint8_t *saved = &io->buffer[name->buffer.size()];
  memcpy(&flags, saved, sizeof(flags));
  flags = marked ? flags | Index_HasNestedGroups : flags & ~(uint64_t)Index_HasNestedGroups;
  memcpy(saved, &flags, sizeof(flags));
}

// Loads what `io` holds as an index of version `encver`. A loaded index is the caller's to
// discard. A refused load returns NULL and its error in `error`.
IndexSpec *loaded(const Io &io, int encver, std::string *error = nullptr) {
  io->read_pos = 0;
  QueryError status = QueryError_Default();
  IndexSpec *spec = IndexSpec_RdbLoad(io.get(), encver, false, &status);
  EXPECT_EQ(spec == nullptr, QueryError_HasError(&status));
  if (error) {
    *error = QueryError_HasError(&status) ? QueryError_GetUserError(&status) : "";
  }
  QueryError_ClearError(&status);
  return spec;
}

// The errors of a load that refuses the groups of a schema: the groups break a rule of
// FT.CREATE, or the section that holds them cannot be read.
const char *const kInvalidGroup = "Invalid NESTED group in the loaded schema";
const char *const kUnreadableGroups = "Failed to load the NESTED groups";

// Expects the load of what `io` holds to fail with `message`.
void expectRefused(const Io &io, const char *message, int encver = INDEX_CURRENT_VERSION) {
  std::string error;
  EXPECT_EQ(loaded(io, encver, &error), nullptr);
  EXPECT_NE(error.find(message), std::string::npos) << error;
}

///////////////////////////////////////////////////////////////////////////////////////////////
// Schema
///////////////////////////////////////////////////////////////////////////////////////////////

TEST_F(NestedGroupTest, createKeepsGroupsApartFromFields) {
  IndexSpec *spec = createSpec();
  ASSERT_NE(spec, nullptr);
  // A group takes no field position.
  EXPECT_EQ(spec->numFields, 7);
  ASSERT_EQ(array_len(spec->nestedGroups), 2);
  const NestedGroup *metrics = group(spec, "metrics"), *owners = group(spec, "owners");
  ASSERT_NE(metrics, nullptr);
  ASSERT_NE(owners, nullptr);
  EXPECT_EQ(field(spec, "metrics"), nullptr);
  EXPECT_EQ(HiddenString_CompareC(metrics->path, "$.metrics[*]", strlen("$.metrics[*]")), 0);
  for (const char *member : {"k", "r", "v"}) {
    EXPECT_EQ(IndexSpec_NestedGroupOf(spec, field(spec, member)), metrics) << member;
  }
  for (const char *member : {"name", "share"}) {
    EXPECT_EQ(IndexSpec_NestedGroupOf(spec, field(spec, member)), owners) << member;
  }
  for (const char *other : {"hq", "title"}) {
    EXPECT_EQ(IndexSpec_NestedGroupOf(spec, field(spec, other)), nullptr) << other;
  }
  EXPECT_STREQ(field(spec, "k")->nestedValuesPathText, "$.metrics[*].k");
  EXPECT_STREQ(field(spec, "share")->nestedValuesPathText, "$.owners[*].share");
}

// An option at the end of a member's definition does not hide the group after it.
TEST_F(NestedGroupTest, createFindsGroupAfterMemberWithTrailingOption) {
  IndexSpec *spec = create({"ON", "JSON", "SCHEMA",
                            "$.a[*]", "AS", "a", "NESTED", "1",
                            "$.v", "AS", "v", "NUMERIC", "SORTABLE",
                            "$.b[*]", "AS", "b", "NESTED", "1",
                            "$.k", "AS", "k", "TAG", "CASESENSITIVE",
                            "$.c[*]", "AS", "c", "NESTED", "1",
                            "$.r", "AS", "r", "TAG"});
  ASSERT_NE(spec, nullptr);
  EXPECT_EQ(spec->numFields, 3);
  ASSERT_EQ(array_len(spec->nestedGroups), 3);
  EXPECT_EQ(IndexSpec_NestedGroupOf(spec, field(spec, "v")), group(spec, "a"));
  EXPECT_EQ(IndexSpec_NestedGroupOf(spec, field(spec, "k")), group(spec, "b"));
  EXPECT_EQ(IndexSpec_NestedGroupOf(spec, field(spec, "r")), group(spec, "c"));
}

TEST_F(NestedGroupTest, createRejectsNameSharedByGroupAndField) {
  const std::vector<std::vector<const char *>> schemas = {
      // a field with the name of a group before it
      {"ON", "JSON", "SCHEMA", "$.m[*]", "AS", "m", "NESTED", "1", "$.k", "AS", "k", "TAG",
       "$.other", "AS", "m", "TAG"},
      // a field of the group with the name of the group
      {"ON", "JSON", "SCHEMA", "$.m[*]", "AS", "m", "NESTED", "1", "$.k", "AS", "m", "TAG"},
      // a group with the name of a field before it
      {"ON", "JSON", "SCHEMA", "$.other", "AS", "m", "TAG", "$.m[*]", "AS", "m", "NESTED", "1",
       "$.k", "AS", "k", "TAG"},
      // two groups with one name
      {"ON", "JSON", "SCHEMA", "$.m[*]", "AS", "m", "NESTED", "1", "$.k", "AS", "k", "TAG",
       "$.n[*]", "AS", "m", "NESTED", "1", "$.r", "AS", "r", "TAG"},
  };
  for (const auto &schema : schemas) {
    std::string error;
    EXPECT_EQ(create(schema, &error), nullptr);
    EXPECT_NE(error.find("Duplicate NESTED group or field in schema"), std::string::npos) << error;
  }
}

TEST_F(NestedGroupTest, createRejectsGroupOnDisk) {
  const std::vector<const char *> schema = {"ON", "JSON", "SCHEMA", "$.m[*]", "AS", "m",
                                            "NESTED", "1", "$.k", "AS", "k", "TAG"};
  RSGlobalConfig.simulateInFlex = true;
  std::string error;
  IndexSpec *onDisk = create(schema, &error);
  RSGlobalConfig.simulateInFlex = false;
  EXPECT_EQ(onDisk, nullptr);
  EXPECT_NE(error.find("NESTED is supported only for in-memory JSON indexes"), std::string::npos)
      << error;
  EXPECT_NE(create(schema), nullptr);
}

// One rule for the path of a member, whatever its type.
TEST_F(NestedGroupTest, createRejectsMemberPathThatDoesNotStartAtTheElement) {
  for (const char *type : {"TAG", "NUMERIC"}) {
    std::string error;
    EXPECT_EQ(create({"ON", "JSON", "SCHEMA", "$.m[*]", "AS", "m", "NESTED", "1", "k", type},
                     &error),
              nullptr)
        << type;
    EXPECT_NE(error.find("must start at an element"), std::string::npos) << type << ": " << error;
  }
}

///////////////////////////////////////////////////////////////////////////////////////////////
// RDB
///////////////////////////////////////////////////////////////////////////////////////////////

TEST_F(NestedGroupTest, rdbRoundTripKeepsGroupsAndTheGroupOfEachField) {
  IndexSpec *spec = createSpec();
  ASSERT_NE(spec, nullptr);
  Io io = saved(spec);

  IndexSpec *copy = loaded(io, INDEX_CURRENT_VERSION);
  ASSERT_NE(copy, nullptr);
  EXPECT_EQ(io->read_pos, io->buffer.size());
  // The marker of the groups is only in the RDB.
  EXPECT_FALSE(spec->flags & Index_HasNestedGroups);
  EXPECT_FALSE(copy->flags & Index_HasNestedGroups);
  ASSERT_EQ(copy->numFields, spec->numFields);
  ASSERT_EQ(array_len(copy->nestedGroups), array_len(spec->nestedGroups));
  for (uint32_t i = 0; i < array_len(spec->nestedGroups); ++i) {
    const NestedGroup *original = &spec->nestedGroups[i], *reloaded = &copy->nestedGroups[i];
    EXPECT_EQ(HiddenString_Compare(reloaded->name, original->name), 0) << i;
    EXPECT_EQ(HiddenString_Compare(reloaded->path, original->path), 0) << i;
    EXPECT_NE(reloaded->compiledPath, nullptr) << i;
  }
  for (int i = 0; i < spec->numFields; ++i) {
    const FieldSpec *original = &spec->fields[i], *reloaded = &copy->fields[i];
    EXPECT_EQ(reloaded->options, original->options) << i;
    EXPECT_EQ(reloaded->types, original->types) << i;
    ASSERT_EQ(FieldSpec_IsNestedMember(reloaded), FieldSpec_IsNestedMember(original)) << i;
    if (FieldSpec_IsNestedMember(original)) {
      EXPECT_EQ(reloaded->nestedGroup, original->nestedGroup) << i;
      EXPECT_STREQ(reloaded->nestedValuesPathText, original->nestedValuesPathText) << i;
      EXPECT_EQ(reloaded->nestedValuesPath != nullptr, original->types == INDEXFLD_T_NUMERIC)
          << i;
    }
  }
  EXPECT_EQ(IndexSpec_SchemaFingerprint(copy), IndexSpec_SchemaFingerprint(spec));
  IndexSpec_RdbLoadDiscard(copy->own_ref);
}

// The saved group, not the order of the schema, says which group a field belongs to.
TEST_F(NestedGroupTest, rdbLoadTakesTheGroupFromTheSavedPosition) {
  IndexSpec *spec = createSpec();
  ASSERT_NE(spec, nullptr);
  FieldSpec *name = field(spec, "name");
  name->nestedGroup = kMetrics;
  Io io = saved(spec);
  name->nestedGroup = kOwners;

  IndexSpec *copy = loaded(io, INDEX_CURRENT_VERSION);
  ASSERT_NE(copy, nullptr);
  EXPECT_EQ(IndexSpec_NestedGroupOf(copy, field(copy, "name")), group(copy, "metrics"));
  EXPECT_STREQ(field(copy, "name")->nestedValuesPathText, "$.metrics[*].name");
  EXPECT_EQ(IndexSpec_NestedGroupOf(copy, field(copy, "share")), group(copy, "owners"));
  EXPECT_NE(IndexSpec_SchemaFingerprint(copy), IndexSpec_SchemaFingerprint(spec));
  IndexSpec_RdbLoadDiscard(copy->own_ref);
}

TEST_F(NestedGroupTest, rdbOfEarlierVersionCannotHoldAGroup) {
  IndexSpec *spec = createSpec();
  ASSERT_NE(spec, nullptr);
  Io io = saved(spec);
  expectRefused(io, kInvalidGroup, INDEX_NESTED_GROUP_VERSION - 1);

  // The option of a field alone is refused too.
  markGroups(io, spec, false);
  expectRefused(io, "Failed to load index field", INDEX_NESTED_GROUP_VERSION - 1);
}

// Without a group, the version changes nothing in what is saved.
TEST_F(NestedGroupTest, rdbOfEarlierVersionLoadsWithoutGroups) {
  IndexSpec *spec = create({"ON", "JSON", "SCHEMA", "$.hq", "AS", "hq", "TAG", "$.v", "AS", "v",
                            "NUMERIC", "SORTABLE"});
  ASSERT_NE(spec, nullptr);
  Io io = saved(spec);

  for (int encver : {INDEX_NESTED_GROUP_VERSION - 1, INDEX_NESTED_GROUP_VERSION}) {
    IndexSpec *copy = loaded(io, encver);
    ASSERT_NE(copy, nullptr) << encver;
    EXPECT_EQ(io->read_pos, io->buffer.size()) << encver;
    EXPECT_EQ(copy->nestedGroups, nullptr) << encver;
    for (int i = 0; i < copy->numFields; ++i) {
      EXPECT_FALSE(FieldSpec_IsNestedMember(&copy->fields[i])) << encver;
    }
    IndexSpec_RdbLoadDiscard(copy->own_ref);
  }
}

// A deployment on disk has no NESTED groups, which FT.CREATE refuses there.
TEST_F(NestedGroupTest, rdbWithGroupIsRefusedOnDisk) {
  IndexSpec *spec = createSpec();
  ASSERT_NE(spec, nullptr);
  Io io = saved(spec);
  RSGlobalConfig.simulateInFlex = true;
  expectRefused(io, kInvalidGroup);
  RSGlobalConfig.simulateInFlex = false;
  IndexSpec *copy = loaded(io, INDEX_CURRENT_VERSION);
  ASSERT_NE(copy, nullptr);
  IndexSpec_RdbLoadDiscard(copy->own_ref);
}

// Saves the index after `corrupt` changed its schema in memory, which is what an RDB with that
// schema holds, and expects the load to refuse its groups.
class NestedGroupInvalidRdbTest : public NestedGroupTest {
 protected:
  void expectInvalid(const std::function<void(IndexSpec *)> &corrupt) {
    expectInvalid(createSpec(), corrupt);
  }

  void expectInvalid(IndexSpec *spec, const std::function<void(IndexSpec *)> &corrupt) {
    ASSERT_NE(spec, nullptr);
    // The schema is put back before the index is freed, which reads the field types.
    std::vector<FieldSpec> original(spec->fields, spec->fields + spec->numFields);
    const DocumentType type = spec->rule->type;
    const IndexFlags flags = spec->flags;
    corrupt(spec);
    Io io = saved(spec);
    for (int i = 0; i < spec->numFields; ++i) {
      spec->fields[i].types = original[i].types;
      spec->fields[i].options = original[i].options;
      spec->fields[i].sortIdx = original[i].sortIdx;
      spec->fields[i].nestedGroup = original[i].nestedGroup;
    }
    spec->rule->type = type;
    spec->flags = flags;

    expectRefused(io, kInvalidGroup);
  }

  // Where the section of the groups starts in what `io` holds for `spec`: at their number.
  static uint8_t *groupSection(const Io &io, const IndexSpec *spec) {
    Io section(RMCK_CreateRdbIO(), RMCK_FreeRdbIO);
    RMCK_SaveUnsigned(section.get(), array_len(spec->nestedGroups));
    HiddenString_SaveToRdb(spec->nestedGroups[0].name, section.get());
    auto found = std::search(io->buffer.begin(), io->buffer.end(), section->buffer.begin(),
                             section->buffer.end());
    EXPECT_NE(found, io->buffer.end());
    return &*found;
  }

  static void setOptions(FieldSpec *fs, int options) {
    fs->options = (FieldSpecOptions)options;
  }

  static void replace(HiddenString **string, const char *text) {
    HiddenString_Free(*string, true);
    *string = NewHiddenString(text, strlen(text), true);
  }
};

TEST_F(NestedGroupInvalidRdbTest, savedGroupPositionOutOfRange) {
  expectInvalid([](IndexSpec *spec) { field(spec, "k")->nestedGroup = 2; });
  expectInvalid([](IndexSpec *spec) { field(spec, "k")->nestedGroup = UINT16_MAX; });
}

TEST_F(NestedGroupInvalidRdbTest, memberOfSchemaWithoutGroups) {
  IndexSpec *spec = create({"ON", "JSON", "SCHEMA", "$.hq", "AS", "hq", "TAG"});
  expectInvalid(spec, [](IndexSpec *spec) {
    FieldSpec *hq = field(spec, "hq");
    setOptions(hq, hq->options | FieldSpec_NestedMember);
  });
}

TEST_F(NestedGroupInvalidRdbTest, groupsMarkerWithoutGroups) {
  IndexSpec *spec = create({"ON", "JSON", "SCHEMA", "$.hq", "AS", "hq", "TAG"});
  ASSERT_NE(spec, nullptr);
  Io io = saved(spec);
  markGroups(io, spec, true);
  expectRefused(io, kUnreadableGroups);
}

// The section is the number of groups, then the name and the path of each group. A string is
// its length, then its bytes with the terminator.
TEST_F(NestedGroupInvalidRdbTest, groupSectionThatCannotBeRead) {
  IndexSpec *spec = createSpec();
  ASSERT_NE(spec, nullptr);
  Io name(RMCK_CreateRdbIO(), RMCK_FreeRdbIO), path(RMCK_CreateRdbIO(), RMCK_FreeRdbIO);
  HiddenString_SaveToRdb(spec->nestedGroups[0].name, name.get());
  HiddenString_SaveToRdb(spec->nestedGroups[0].path, path.get());
  const size_t nameStart = sizeof(uint64_t), nameEnd = nameStart + name->buffer.size(),
               pathEnd = nameEnd + path->buffer.size();
  const uint64_t tooMany = (uint64_t)UINT16_MAX + 1, none = 0;

  enum Change { SetNumGroups, SetByte, Truncate };
  const struct {
    const char *name;
    Change change;
    size_t offset;  // in the section
    uint64_t value;
  } cases[] = {
      {"no group", SetNumGroups, 0, none},
      {"more groups than a schema can hold", SetNumGroups, 0, tooMany},
      {"name without terminator", SetByte, nameEnd - 1, 'x'},
      {"NUL inside a name", SetByte, nameEnd - 2, '\0'},
      {"path without terminator", SetByte, pathEnd - 1, 'x'},
      {"payload that ends inside a name", Truncate, nameEnd - 2, 0},
      {"payload that ends inside a path", Truncate, pathEnd - 2, 0},
  };
  for (const auto &c : cases) {
    SCOPED_TRACE(c.name);
    Io io = saved(spec);
    uint8_t *section = groupSection(io, spec);
    if (c.change == SetNumGroups) {
      memcpy(section, &c.value, sizeof(c.value));
    } else if (c.change == SetByte) {
      section[c.offset] = (uint8_t)c.value;
    } else {
      io->buffer.resize(section - io->buffer.data() + c.offset);
    }
    expectRefused(io, kUnreadableGroups);
  }
}

TEST_F(NestedGroupInvalidRdbTest, groupWithoutFields) {
  expectInvalid([](IndexSpec *spec) {
    for (const char *member : {"name", "share"}) {
      field(spec, member)->nestedGroup = kMetrics;
    }
  });
}

TEST_F(NestedGroupInvalidRdbTest, memberThatIsNeitherTagNorNumeric) {
  expectInvalid([](IndexSpec *spec) { field(spec, "k")->types = INDEXFLD_T_GEO; });
  expectInvalid([](IndexSpec *spec) {
    field(spec, "k")->types = (FieldType)(INDEXFLD_T_TAG | INDEXFLD_T_NUMERIC);
  });
  expectInvalid([](IndexSpec *spec) { field(spec, "k")->types = (FieldType)0; });
}

TEST_F(NestedGroupInvalidRdbTest, memberWithOptionThatAGroupDoesNotSupport) {
  for (int option : {FieldSpec_NotIndexable, FieldSpec_IndexEmpty, FieldSpec_IndexMissing,
                     FieldSpec_Sortable}) {
    expectInvalid([option](IndexSpec *spec) {
      FieldSpec *k = field(spec, "k");
      setOptions(k, k->options | option);
    });
  }
}

TEST_F(NestedGroupInvalidRdbTest, numericMemberThatIsNotSortable) {
  expectInvalid([](IndexSpec *spec) {
    FieldSpec *v = field(spec, "v");
    setOptions(v, v->options & ~FieldSpec_Sortable);
  });
  expectInvalid([](IndexSpec *spec) { field(spec, "v")->sortIdx = -1; });
}

TEST_F(NestedGroupInvalidRdbTest, memberPathThatDoesNotStartAtTheElement) {
  for (const char *member : {"k", "v"}) {
    expectInvalid([member](IndexSpec *spec) { replace(&field(spec, member)->fieldPath, member); });
  }
}

TEST_F(NestedGroupInvalidRdbTest, memberPathThatSelectsSeveralValues) {
  expectInvalid([](IndexSpec *spec) { replace(&field(spec, "k")->fieldPath, "$.tags[*]"); });
}

TEST_F(NestedGroupInvalidRdbTest, groupPathThatDoesNotSelectElements) {
  expectInvalid([](IndexSpec *spec) { replace(&group(spec, "metrics")->path, "$.metrics"); });
}

TEST_F(NestedGroupInvalidRdbTest, groupNameSharedWithGroupOrField) {
  expectInvalid([](IndexSpec *spec) { replace(&group(spec, "owners")->name, "metrics"); });
  expectInvalid([](IndexSpec *spec) { replace(&group(spec, "owners")->name, "hq"); });
}

TEST_F(NestedGroupInvalidRdbTest, pathThatDoesNotCompile) {
  // The path of a member, of a group, and to the values of a NUMERIC and of a TAG member.
  for (const char *path : {"$.r", "$.metrics[*]", "$.metrics[*].v", "$.metrics[*].k"}) {
    static const char *failing;
    failing = path;
    IndexSpec *spec = createSpec();
    ASSERT_NE(spec, nullptr);
    Io io = saved(spec);
    api.pathParseNoErrorMsg = [](const char *path) -> JSONPath {
      return strcmp(path, failing) ? parse(path) : nullptr;
    };
    SCOPED_TRACE(path);
    expectRefused(io, kInvalidGroup);
    api.pathParseNoErrorMsg = parse;
  }
}

TEST_F(NestedGroupInvalidRdbTest, groupOnAHashIndex) {
  expectInvalid([](IndexSpec *spec) { spec->rule->type = DocumentType_Hash; });
}

// Lets IndexSpec_LegacyRdbLoad run: it needs a server that is loading, and the dictionaries of
// the legacy indexes.
class NestedGroupLegacyRdbTest : public NestedGroupTest {
 protected:
  decltype(RedisModule_GetKeyNameFromIO) savedGetKeyName = RedisModule_GetKeyNameFromIO;
  bool savedLoading = g_isLoading;
  dict *savedRules = legacySpecRules, *savedSpecs = legacySpecDict;

  void SetUp() override {
    NestedGroupTest::SetUp();
    RedisModule_GetKeyNameFromIO = [](RedisModuleIO *) -> const RedisModuleString * {
      return nullptr;
    };
    g_isLoading = true;
    legacySpecRules = dictCreate(&dictTypeHeapHiddenStrings, nullptr);
    legacySpecDict = dictCreate(&dictTypeHeapHiddenStrings, nullptr);
  }

  void TearDown() override {
    dictRelease(legacySpecRules);
    dictRelease(legacySpecDict);
    legacySpecRules = savedRules;
    legacySpecDict = savedSpecs;
    g_isLoading = savedLoading;
    RedisModule_GetKeyNameFromIO = savedGetKeyName;
    NestedGroupTest::TearDown();
  }
};

// The payload is an index of one NUMERIC field, saved up to the sort index of the field, so no
// load of it succeeds. The load stops at the marker of the groups or at the option of a
// member. Without them it reads all that the payload holds.
TEST_F(NestedGroupLegacyRdbTest, loadStopsAtTheMarkerOfTheGroupsAndAtTheOptionOfAMember) {
  enum Stop { AtFlags, AtOptions, AtEnd };
  const struct {
    const char *name;
    uint64_t indexFlags, fieldOptions;
    Stop stop;
  } cases[] = {
      {"marker of the groups", INDEX_DEFAULT_FLAGS | Index_HasNestedGroups, 0, AtFlags},
      {"option of a member", INDEX_DEFAULT_FLAGS, FieldSpec_NestedMember, AtOptions},
      {"neither", INDEX_DEFAULT_FLAGS, 0, AtEnd},
  };
  // Version 4 is the first that saves the options of a field.
  for (int encver = 4; encver <= LEGACY_INDEX_MAX_VERSION; ++encver) {
    for (const auto &c : cases) {
      SCOPED_TRACE(::testing::Message() << "encver=" << encver << ", " << c.name);
      Io io(RMCK_CreateRdbIO(), RMCK_FreeRdbIO);
      RMCK_SaveStringBuffer(io.get(), "legacy_group", sizeof("legacy_group"));
      RMCK_SaveUnsigned(io.get(), c.indexFlags);
      const size_t flagsEnd = io->buffer.size();
      RMCK_SaveUnsigned(io.get(), 1);
      RMCK_SaveStringBuffer(io.get(), "f", sizeof("f"));
      if (encver < INDEX_MIN_TAGFIELD_VERSION) {
        RMCK_SaveUnsigned(io.get(), 0);  // the id of the field
        RMCK_SaveUnsigned(io.get(), IDXFLD_LEGACY_NUMERIC);
        RMCK_SaveDouble(io.get(), 1.0);
      } else {
        RMCK_SaveUnsigned(io.get(), encver < INDEX_MIN_MULTITYPE_VERSION ? IDXFLD_LEGACY_NUMERIC
                                                                         : INDEXFLD_T_NUMERIC);
      }
      RMCK_SaveUnsigned(io.get(), c.fieldOptions);
      const size_t optionsEnd = io->buffer.size();
      RMCK_SaveSigned(io.get(), -1);
      const size_t stops[] = {flagsEnd, optionsEnd, io->buffer.size()};

      EXPECT_EQ(IndexSpec_LegacyRdbLoad(io.get(), encver), nullptr);
      EXPECT_EQ(io->read_pos, stops[c.stop]);
    }
  }
}

}  // namespace
