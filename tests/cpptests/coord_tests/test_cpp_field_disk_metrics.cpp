/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

// Coordinator FT.INFO reducer: aggregation of per-field disk metrics.
//
// The shard emits per-field `disk_*` keys inside each `field statistics` entry
// (see src/info/field_spec_info.c). The coordinator deserializes each shard's
// entry into an AggregatedFieldSpecInfo and folds them together with
// AggregatedFieldSpecInfo_Combine. These tests drive that deserialize/combine
// path with synthetic shard replies, since a real disk backend is not present
// in the OSS build.

#include "gtest/gtest.h"

#include "info/field_spec_info.h"
#include "hiredis/hiredis.h"
#include "hiredis/read.h"

#include <string>

// Minimal RESP2 serializer used to hand-build shard replies. RESP2 arrays are
// fine: AggregatedFieldSpecInfo_Deserialize reinterprets even-length arrays as
// maps, and the MR_REPLY_* type codes match hiredis' REDIS_REPLY_* values.
namespace {

class Resp {
 public:
  std::string s;
  Resp &arr(size_t n) { s += "*" + std::to_string(n) + "\r\n"; return *this; }
  Resp &str(const std::string &v) {
    s += "$" + std::to_string(v.size()) + "\r\n" + v + "\r\n";
    return *this;
  }
  Resp &integer(long long v) { s += ":" + std::to_string(v) + "\r\n"; return *this; }
};

// Append an "N/A" (no-error) Index Errors sub-object: 4 key/value pairs.
void appendEmptyIndexError(Resp &r) {
  r.arr(8);
  r.str("indexing failures").integer(0);
  r.str("last indexing error").str("N/A");
  r.str("last indexing error key").str("N/A");
  r.str("last indexing error time").arr(2).integer(0).integer(0);
}

// A shard `field statistics` entry with the given identifier/attribute and an
// empty error object, plus any extra key/value pairs appended by `extra`.
template <typename Fn>
std::string fieldEntry(const char *identifier, const char *attribute, size_t extraPairs,
                       Fn extra) {
  Resp r;
  r.arr((3 + extraPairs) * 2);
  r.str("identifier").str(identifier);
  r.str("attribute").str(attribute);
  r.str("Index Errors");
  appendEmptyIndexError(r);
  extra(r);
  return r.s;
}

MRReply *parseReply(const std::string &resp) {
  redisReader *reader = redisReaderCreate();
  EXPECT_NE(reader, nullptr);
  EXPECT_EQ(redisReaderFeed(reader, resp.data(), resp.size()), REDIS_OK);
  void *replyPtr = nullptr;
  EXPECT_EQ(redisReaderGetReply(reader, &replyPtr), REDIS_OK);
  EXPECT_NE(replyPtr, nullptr);
  redisReaderFree(reader);
  return reinterpret_cast<MRReply *>(replyPtr);
}

// A shard reply plus its deserialized view, tied together so the borrowed
// identifier/attribute pointers stay valid for the lifetime of the object.
class ShardEntry {
 public:
  explicit ShardEntry(const std::string &resp) : reply_(parseReply(resp)) {
    info_ = AggregatedFieldSpecInfo_Deserialize(reply_);
  }
  ~ShardEntry() {
    AggregatedFieldSpecInfo_Clear(&info_);
    freeReplyObject(reply_);
  }
  const AggregatedFieldSpecInfo *info() const { return &info_; }

 private:
  MRReply *reply_;
  AggregatedFieldSpecInfo info_;
};

}  // namespace

class FieldDiskMetricsTest : public ::testing::Test {};

// A single text-field shard reply parses into available textDisk metrics and no
// cf metrics.
TEST_F(FieldDiskMetricsTest, DeserializeTextField) {
  ShardEntry shard(fieldEntry("title", "title", 2, [](Resp &r) {
    r.str("disk_exclusive_bytes").integer(100);
    r.str("disk_shared_bytes").integer(40);
  }));

  const FieldSpecStats *stats = &shard.info()->stats;
  EXPECT_TRUE(stats->textDisk.available);
  EXPECT_EQ(stats->textDisk.exclusive_bytes, 100u);
  EXPECT_EQ(stats->textDisk.shared_bytes, 40u);
  EXPECT_FALSE(stats->cfDisk.available);
}

// A single tag/numeric/vector-field shard reply parses into available cf
// metrics and no text metrics.
TEST_F(FieldDiskMetricsTest, DeserializeCfField) {
  ShardEntry shard(fieldEntry("n", "n", 2, [](Resp &r) {
    r.str("disk_total_bytes").integer(4096);
    r.str("disk_num_keys").integer(12);
  }));

  const FieldSpecStats *stats = &shard.info()->stats;
  EXPECT_TRUE(stats->cfDisk.available);
  EXPECT_EQ(stats->cfDisk.total_bytes, 4096u);
  EXPECT_EQ(stats->cfDisk.estimate_num_keys, 12u);
  EXPECT_FALSE(stats->textDisk.available);
}

// Text-field metrics are summed across shards.
TEST_F(FieldDiskMetricsTest, CombineTextFieldSums) {
  ShardEntry s1(fieldEntry("title", "title", 2, [](Resp &r) {
    r.str("disk_exclusive_bytes").integer(100);
    r.str("disk_shared_bytes").integer(40);
  }));
  ShardEntry s2(fieldEntry("title", "title", 2, [](Resp &r) {
    r.str("disk_exclusive_bytes").integer(200);
    r.str("disk_shared_bytes").integer(70);
  }));

  AggregatedFieldSpecInfo agg = AggregatedFieldSpecInfo_Init();
  AggregatedFieldSpecInfo_Combine(&agg, s1.info());
  AggregatedFieldSpecInfo_Combine(&agg, s2.info());

  EXPECT_TRUE(agg.stats.textDisk.available);
  EXPECT_EQ(agg.stats.textDisk.exclusive_bytes, 300u);
  EXPECT_EQ(agg.stats.textDisk.shared_bytes, 110u);
  AggregatedFieldSpecInfo_Clear(&agg);
}

// Cf-field metrics are summed across shards. Regression guard: before the fix,
// non-vector fields carry type 0 on the coordinator and Combine overwrote
// instead of accumulating, so only the last shard's value survived.
TEST_F(FieldDiskMetricsTest, CombineCfFieldSums) {
  ShardEntry s1(fieldEntry("n", "n", 2, [](Resp &r) {
    r.str("disk_total_bytes").integer(1000);
    r.str("disk_num_keys").integer(10);
  }));
  ShardEntry s2(fieldEntry("n", "n", 2, [](Resp &r) {
    r.str("disk_total_bytes").integer(2500);
    r.str("disk_num_keys").integer(25);
  }));

  AggregatedFieldSpecInfo agg = AggregatedFieldSpecInfo_Init();
  AggregatedFieldSpecInfo_Combine(&agg, s1.info());
  AggregatedFieldSpecInfo_Combine(&agg, s2.info());

  EXPECT_TRUE(agg.stats.cfDisk.available);
  EXPECT_EQ(agg.stats.cfDisk.total_bytes, 3500u);
  EXPECT_EQ(agg.stats.cfDisk.estimate_num_keys, 35u);
  AggregatedFieldSpecInfo_Clear(&agg);
}

// Mixed-version cluster: a shard that omits the disk keys must not fail the
// reduce nor clobber the metrics contributed by disk-aware shards.
TEST_F(FieldDiskMetricsTest, MixedVersionShardsDoNotClobber) {
  ShardEntry diskShard(fieldEntry("title", "title", 2, [](Resp &r) {
    r.str("disk_exclusive_bytes").integer(500);
    r.str("disk_shared_bytes").integer(60);
  }));
  // Old shard: identifier/attribute/error only, no disk_* keys.
  ShardEntry oldShard(fieldEntry("title", "title", 0, [](Resp &) {}));

  const FieldSpecStats *oldStats = &oldShard.info()->stats;
  EXPECT_FALSE(oldStats->textDisk.available);
  EXPECT_FALSE(oldStats->cfDisk.available);

  // Combine in either order: the disk-aware shard's values must survive intact.
  AggregatedFieldSpecInfo agg = AggregatedFieldSpecInfo_Init();
  AggregatedFieldSpecInfo_Combine(&agg, oldShard.info());
  AggregatedFieldSpecInfo_Combine(&agg, diskShard.info());
  AggregatedFieldSpecInfo_Combine(&agg, oldShard.info());

  EXPECT_TRUE(agg.stats.textDisk.available);
  EXPECT_EQ(agg.stats.textDisk.exclusive_bytes, 500u);
  EXPECT_EQ(agg.stats.textDisk.shared_bytes, 60u);
  AggregatedFieldSpecInfo_Clear(&agg);
}

// A field for which no shard reports disk metrics stays unavailable (metrics
// are omitted from the reply), and the reduce still succeeds.
TEST_F(FieldDiskMetricsTest, NoDiskMetricsAcrossShards) {
  ShardEntry s1(fieldEntry("body", "body", 0, [](Resp &) {}));
  ShardEntry s2(fieldEntry("body", "body", 0, [](Resp &) {}));

  AggregatedFieldSpecInfo agg = AggregatedFieldSpecInfo_Init();
  AggregatedFieldSpecInfo_Combine(&agg, s1.info());
  AggregatedFieldSpecInfo_Combine(&agg, s2.info());

  EXPECT_FALSE(agg.stats.textDisk.available);
  EXPECT_FALSE(agg.stats.cfDisk.available);
  AggregatedFieldSpecInfo_Clear(&agg);
}
