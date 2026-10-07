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

struct Decoder {
  RLookup lookup = RLookup_New();
  RPNet nc = {};
  MRReply *reply = nullptr;

  explicit Decoder(int protocol = 3) {
    nc.cmd.protocol = protocol;
    nc.lookup = &lookup;
  }
  ~Decoder() {
    reset();
    RLookup_Cleanup(&lookup);
  }
  void reset() {
    RPNet_resetCurrent(&nc);
    MRReply_Free(reply);
    reply = nullptr;
  }
  bool prepare(const std::string &wire, uint16_t maxColumns = UINT16_MAX) {
    reset();
    reply = parse(wire);
    EXPECT_NE(reply, nullptr);
    bool accepted = reply && RPNet_DebugPrepareRespSchema(&nc, reply, maxColumns);
    if (!accepted) EXPECT_EQ(nc.current.schemaKeys, nullptr);
    return accepted;
  }
};

void check(const std::string &wire, int protocol, bool accepted, uint16_t maxColumns = UINT16_MAX) {
  Decoder decoder(protocol);
  EXPECT_EQ(decoder.prepare(wire, maxColumns), accepted);
  if (accepted) {
    EXPECT_NE(decoder.nc.current.schemaKeys, nullptr);
    EXPECT_EQ(array_len(decoder.nc.current.schemaKeys),
              MRReply_Length(MRReply_ArrayElement(decoder.reply, 2)));
  }
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
  Decoder decoder;
  RLookup_Seal(&decoder.lookup);
  std::vector<std::string> names;
  for (size_t i = 0; i < 1024; ++i) names.push_back(str("field" + std::to_string(i)));
  ASSERT_TRUE(decoder.prepare(chunk(3, {arr({str("1"), arr({str("value")})})}, names)));
  ASSERT_EQ(array_len(decoder.nc.current.schemaKeys), names.size());
  const RLookupKey *first = decoder.nc.current.schemaKeys[0];
  const RLookupKey *last = decoder.nc.current.schemaKeys[names.size() - 1];
  ASSERT_TRUE(decoder.prepare(
      chunk(3, {arr({nil, arr({str("a"), str("b")})})}, {str("field1023"), str("field0")})));
  EXPECT_EQ(decoder.nc.current.schemaKeys[0], last);
  EXPECT_EQ(decoder.nc.current.schemaKeys[1], first);
  EXPECT_EQ(RLookup_Iter(&decoder.lookup).remaining, names.size());
}

TEST(InternalRespSchema, CapacityPreservesExistingKeysAndOwnedNames) {
  check(chunk(3, {}, {}), 3, true, 0);
  check(chunk(3, {}, {str("field")}), 3, false, 0);
  Decoder decoder;
  const RLookupKey *existing = RLookup_GetKey_Write(&decoder.lookup, "existing", RLOOKUP_F_HIDDEN);
  ASSERT_NE(existing, nullptr);
  uint32_t flags = RLookupKey_GetFlags(existing);
  RLookup_Seal(&decoder.lookup);
  ASSERT_TRUE(decoder.prepare(chunk(3, {}, {str("existing")}), 1));
  EXPECT_EQ(decoder.nc.current.schemaKeys[0], existing);
  EXPECT_EQ(RLookupKey_GetFlags(existing), flags);
  ASSERT_TRUE(decoder.prepare(chunk(3, {}, {str("existing"), str("transient\xff")}), 2));
  const RLookupKey *owned = decoder.nc.current.schemaKeys[1];
  EXPECT_FALSE(decoder.prepare(chunk(3, {}, {str("overflow")}), 2));
  EXPECT_EQ(RLookup_Iter(&decoder.lookup).remaining, 2u);
  EXPECT_STREQ(RLookupKey_GetName(owned), "transient\xff");
  EXPECT_FALSE(decoder.prepare(chunk(3, {}, {}), 1));
  ASSERT_TRUE(decoder.prepare(chunk(3, {}, {str("transient\xff"), str("existing")}), 2));
  EXPECT_EQ(decoder.nc.current.schemaKeys[0], owned);
  EXPECT_EQ(decoder.nc.current.schemaKeys[1], existing);
  EXPECT_EQ(RLookup_Iter(&decoder.lookup).remaining, 2u);
}

#endif
