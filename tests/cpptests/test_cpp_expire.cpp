
#include "gtest/gtest.h"
#include "aggregate/aggregate.h"
#include "redismock/redismock.h"
#include "redismock/util.h"
#include "redismock/internal.h"
#include "spec.h"
#include "common.h"
#include "module.h"
#include "version.h"
#include "tag_index.h"

#include <vector>
#include <array>
#include <iostream>
#include <cstdarg>
#include <chrono>

class ExpireTest : public ::testing::Test {};
using RS::addDocument;

TEST_F(ExpireTest, testSkipTo) {
  RedisModuleCtx *ctx = RedisModule_GetThreadSafeContext(NULL);
  QueryError qerr = {QueryErrorCode(0)};

  RMCK::ArgvList args(ctx, "FT.CREATE", "expire_idx", "ON", "HASH", "SKIPINITIALSCAN",
                      "SCHEMA", "t1", "TAG");
  IndexSpec *spec = IndexSpec_CreateNew(ctx, args, args.size(), &qerr);
  ASSERT_NE(spec, nullptr);
  const FieldSpec *fs = IndexSpec_GetFieldWithLength(spec, "t1", 2);
  ASSERT_NE(fs, nullptr);
  RSIndex *index = spec->own_ref.rm;
  ASSERT_TRUE(spec);
  RedisModuleString* hset_args[3] = { NULL,
                                 RedisModule_CreateString(ctx, "t1", strlen("t1")),
                                 RedisModule_CreateString(ctx, "one", strlen("one"))};
  static const t_docId maxDocId = 1000;
  // Add 1000 documents to the index and expire the fields
  for (t_docId doc = 1; doc <= maxDocId; ++doc) {
    char buf[1024];
    sprintf(buf, "doc:%ld", doc);
    hset_args[0] = RedisModule_CreateString(ctx, buf, strlen(buf));
    RedisModuleCallReply *hset = RedisModule_Call(ctx, "HSET", "!v", hset_args, sizeof(hset_args) / sizeof(hset_args[0]));
    RedisModule_FreeCallReply(hset);
    RedisModuleCallReply *hexpire = RedisModule_Call(ctx, "HPEXPIRE", "cvc", buf, 1ull/*expireAt*/, 1ull/*count*/, "t1");
    RedisModule_FreeCallReply(hexpire);
    RedisModule_FreeString(ctx, hset_args[0]);
  }
  RedisModule_FreeString(ctx, hset_args[1]);
  RedisModule_FreeString(ctx, hset_args[2]);

  RedisSearchCtx *sctx = NewSearchCtxC(ctx, spec->name, true);
  const auto epoch = std::chrono::system_clock::now().time_since_epoch();
  const auto seconds = std::chrono::duration_cast<std::chrono::seconds>(epoch);
  const auto remaining = epoch - seconds;
  // set the time so all previous fields should now be considered expired
  sctx->time.current.tv_sec = seconds.count() + 1;
  sctx->time.current.tv_nsec = std::chrono::duration_cast<std::chrono::nanoseconds>(remaining).count();

  RedisModuleString *kstr = IndexSpec_GetFormattedKey(spec, fs, INDEXFLD_T_TAG);
  TagIndex *idx = TagIndex_Open(sctx, kstr, DONT_CREATE_INDEX);
  ASSERT_NE(idx, nullptr);
  IndexIterator *it = TagIndex_OpenReader(idx, sctx, "one", strlen("one"), 1.0, 0);
  ASSERT_EQ(it->LastDocId(it->ctx), 1);
  // should skip to last document, we index every doc twice so we should have 2 * maxDocId entries in the inverted index
  for (t_docId doc = 2; doc <= (2 * maxDocId); doc += 2) {
    RSIndexResult *result = NULL;
    // we index in hset and in hexpire
    // for hset there won't be an expiration
    // for hexpire there will be
    // if we skip to an odd doc number we should get the requested doc id
    // if we skip to an even doc number we should not get it since it will be expired
    it->SkipTo(it->ctx, doc, &result);
    ASSERT_EQ(result->docId, doc + 1);
  }
  it->Free(it);
  SearchCtx_Free(sctx);
  IndexSpec_RemoveFromGlobals(spec->own_ref);
  args.clear();
  RedisModule_FreeThreadSafeContext(ctx);
}
