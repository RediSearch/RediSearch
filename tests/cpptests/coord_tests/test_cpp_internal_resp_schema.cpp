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
#include "rmr/reply.h"
}
#include "rpnet.h"
#include "aggregate/internal_resp_schema.h"
#include "rlookup_ffi.h"
#include "value_ffi.h"
#include "hiredis/hiredis.h"
#include "hiredis/read.h"

#include <initializer_list>
#include <string>
#include <vector>

#ifdef ENABLE_ASSERT

namespace {
std::string str(const std::string &value) {
  return "$" + std::to_string(value.size()) + "\r\n" + value + "\r\n";
}

std::string arr(const std::vector<std::string> &values) {
  std::string out = "*" + std::to_string(values.size()) + "\r\n";
  for (const auto &value : values) out += value;
  return out;
}

const std::string nil = "$-1\r\n";

std::string chunk(int protocol, std::vector<std::string> rows,
                  const std::vector<std::string> &names) {
  if (protocol == 2) rows.insert(rows.begin(), ":1\r\n");
  return arr({str(INTERNAL_RESP_SCHEMA_TAG), arr(rows), arr(names)});
}

MRReply *parse(const std::string &wire) {
  redisReader *reader = redisReaderCreate();
  EXPECT_EQ(redisReaderFeed(reader, wire.data(), wire.size()), REDIS_OK);
  void *reply = nullptr;
  EXPECT_EQ(redisReaderGetReply(reader, &reply), REDIS_OK);
  redisReaderFree(reader);
  return reinterpret_cast<MRReply *>(reply);
}

void check(const std::string &wire, int protocol, bool accepted) {
  MRReply *reply = parse(wire);
  ASSERT_NE(reply, nullptr);
  RLookup lookup = RLookup_New();
  RPNet nc = {};
  nc.cmd.protocol = protocol;
  nc.lookup = &lookup;
  EXPECT_EQ(RPNet_DebugPrepareRespSchema(&nc, reply), accepted);
  if (accepted) {
    EXPECT_TRUE(nc.current.schema);
    EXPECT_EQ(array_len(nc.current.schemaKeys), MRReply_Length(MRReply_ArrayElement(reply, 2)));
  }
  RPNet_resetCurrent(&nc);
  MRReply_Free(reply);
  RLookup_Cleanup(&lookup);
}
}  // namespace

TEST(InternalRespSchema, DenseSparsePrefixesAndNull) {
  for (int protocol : {2, 3}) {
    const auto dense = arr({nil, arr({str(std::string("a\0b", 3))})});
    const auto sparse = arr({str("01"), arr({nil})});
    check(chunk(protocol, {dense, sparse}, {str("first"), str("late")}), protocol, true);
    check(chunk(protocol, {arr({nil, arr({})})}, {}), protocol, true);
    check(chunk(protocol, {}, {}), protocol, true);
  }
}

TEST(InternalRespSchema, MalformedWidthsMasksAndNames) {
  for (int protocol : {2, 3}) {
    for (const auto &row :
         {arr({nil, arr({str("a"), str("b")})}), arr({str("11"), arr({str("a"), str("b")})}),
          arr({str("1"), arr({})}), arr({str("0"), arr({str("a")})}),
          arr({str("x"), arr({str("a")})}), arr({str(std::string("1\0", 2)), arr({str("a")})}),
          arr({":1\r\n", arr({str("a")})}), arr({nil, str("a")}), arr({nil})}) {
      check(chunk(protocol, {row}, {str("field")}), protocol, false);
    }
    const auto row = arr({nil, arr({str("value")})});
    check(chunk(protocol, {row}, {str(std::string("field\0name", 10))}), protocol, false);
    check(chunk(protocol, {row}, {":1\r\n"}), protocol, false);
    check(chunk(protocol, {row}, {str("field"), str("field")}), protocol, false);
    check(chunk(protocol, {}, std::vector<std::string>((size_t)UINT16_MAX + 1, str("field"))),
          protocol, false);
    check(arr({str(INTERNAL_RESP_SCHEMA_TAG), arr({}), arr({}), nil}), protocol, false);
    check(arr({str(INTERNAL_RESP_SCHEMA_TAG), str("rows"), arr({})}), protocol, false);
  }
  check(arr({str(INTERNAL_RESP_SCHEMA_TAG), arr({}), arr({})}), 2, false);
  check(arr({str(INTERNAL_RESP_SCHEMA_TAG), arr({str("count")}), arr({})}), 2, false);
  check(arr({str("not-a-schema"), arr({}), arr({})}), 3, false);
}

TEST(InternalRespSchema, WideSealedLookupRetainsKeysAcrossChunks) {
  RLookup lookup = RLookup_New();
  RLookup_Seal(&lookup);
  RPNet nc = {};
  nc.cmd.protocol = 3;
  nc.lookup = &lookup;
  std::vector<std::string> names;
  for (size_t i = 0; i < 1024; ++i) names.push_back(str("field" + std::to_string(i)));
  MRReply *reply = parse(chunk(3, {arr({str("1"), arr({str("value")})})}, names));
  ASSERT_NE(reply, nullptr);
  ASSERT_TRUE(RPNet_DebugPrepareRespSchema(&nc, reply));
  ASSERT_EQ(array_len(nc.current.schemaKeys), names.size());
  const RLookupKey *first = nc.current.schemaKeys[0];
  const RLookupKey *last = nc.current.schemaKeys[names.size() - 1];
  RPNet_resetCurrent(&nc);
  MRReply_Free(reply);
  reply =
      parse(chunk(3, {arr({nil, arr({str("a"), str("b")})})}, {str("field1023"), str("field0")}));
  ASSERT_NE(reply, nullptr);
  ASSERT_TRUE(RPNet_DebugPrepareRespSchema(&nc, reply));
  EXPECT_EQ(nc.current.schemaKeys[0], last);
  EXPECT_EQ(nc.current.schemaKeys[1], first);
  EXPECT_EQ(RLookup_Iter(&lookup).remaining, names.size());
  RPNet_resetCurrent(&nc);
  MRReply_Free(reply);
  RLookup_Cleanup(&lookup);
}

TEST(InternalRespSchema, BoundedResolverPreservesSealedKeysAndOwnedNames) {
  RLookup lookup = RLookup_New();
  const RLookupKey *existing = RLookup_GetKey_Write(&lookup, "existing", RLOOKUP_F_HIDDEN);
  ASSERT_NE(existing, nullptr);
  uint32_t flags = RLookupKey_GetFlags(existing);
  RLookup_Seal(&lookup);
  EXPECT_EQ(RLookup_GetOrCreateKeyByName(&lookup, "existing", 8, 1), existing);
  EXPECT_EQ(RLookupKey_GetFlags(existing), flags);
  EXPECT_EQ(RLookup_GetOrCreateKeyByName(&lookup, "missing", 7, 1), nullptr);
  EXPECT_EQ(RLookup_GetOrCreateKeyByName(&lookup, "bad\0name", 8, 2), nullptr);
  EXPECT_EQ(RLookup_Iter(&lookup).remaining, 1u);
  const RLookupKey *owned;
  {
    std::string name = "transient";
    owned = RLookup_GetOrCreateKeyByName(&lookup, name.data(), name.size(), 2);
    ASSERT_NE(owned, nullptr);
    name.assign(name.size(), 'x');
  }
  EXPECT_STREQ(RLookupKey_GetName(owned), "transient");
  EXPECT_EQ(RLookup_GetOrCreateKeyByName(&lookup, "transient", 9, 2), owned);
  EXPECT_EQ(RLookup_GetOrCreateKeyByName(&lookup, "overflow", 8, 2), nullptr);
  EXPECT_EQ(RLookup_Iter(&lookup).remaining, 2u);
  RLookup_Cleanup(&lookup);
}

#endif
