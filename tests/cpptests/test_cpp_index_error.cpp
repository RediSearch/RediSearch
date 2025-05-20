/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "gtest/gtest.h"

#include "src/info/index_error.h"
#include "index_utils.h"
#include "tests/cpptests/redismock/util.h"
#include "redismodule.h"
#include "redismock/internal.h"
#include "reply.h"

class IndexErrorTest : public ::testing::Test {};

TEST_F(IndexErrorTest, testBasic) {
  IndexError error;
  error = IndexError_Init();
  const char* expected = "secret";
  RedisModuleString *key = RedisModule_CreateString(NULL, expected, 6);
  IndexError_AddError(&error, "error", "error1", key);
  ASSERT_STREQ(error.last_error_with_user_data, "error1");
  ASSERT_STREQ(error.last_error_without_user_data, "error");
  RedisModuleString *lastErrorKey = IndexError_LastErrorKey(&error);
  ASSERT_EQ(key, lastErrorKey);
  const char* text = RedisModule_StringPtrLen(lastErrorKey, NULL);
  ASSERT_STREQ(text, expected);
  RedisModule_FreeString(NULL, lastErrorKey);

  error.last_error_time = {0};
  lastErrorKey = IndexError_LastErrorKeyObfuscated(&error);
  text = RedisModule_StringPtrLen(lastErrorKey, NULL);
  ASSERT_NE(key, lastErrorKey);
  ASSERT_STREQ("Key@0", text);
  RedisModule_FreeString(NULL, lastErrorKey);
  IndexError_Destroy(&error);
  RedisModule_FreeString(NULL, key);
}

// TEST_F(IndexErrorTest, testBasic1) {
//   RedisModuleCtx *ctx = RedisModule_GetThreadSafeContext(NULL);
//   QueryError qerr = {QueryErrorCode(0)};

//   // Create a new index spec with error
//   RMCK::ArgvList args(ctx, "FT.CREATE", "idx", "ON", "HASH",
//                       "SCHEMA", "t1", "TEst");
//   auto spec = IndexSpec_CreateNew(ctx, args, args.size(), &qerr);
//   ASSERT_EQ(spec, nullptr);
//   ASSERT_EQ(na->getRefCount(), 1); // Test failure, refcount should not change
//   args.clear();
//   RedisModule_FreeThreadSafeContext(ctx);
// }

// TEST_F(IndexErrorTest, testBasic2) {
//   RedisModuleCtx *ctx = RedisModule_GetThreadSafeContext(NULL);
//   QueryError qerr = {QueryErrorCode(0)};
//   auto na = getNAstring(); // initialize the NA string
//   ASSERT_EQ(na->getRefCount(), 1);

//   // Create a new index spec with error
//   RMCK::ArgvList args(ctx, "FT.CREATE", "idx", "ON", "HASH",
//                       "SCHEMA", "t1", "text");

//   auto spec = IndexSpec_CreateNew(ctx, args, args.size(), &qerr);
//   ASSERT_NE(spec, nullptr);
//   ASSERT_EQ(na->getRefCount(), 3);
//   // Drop the index
//   IndexSpec_RemoveFromGlobals(spec->own_ref, true);
//   ASSERT_EQ(na->getRefCount(), 1);
//   args.clear();
//   RedisModule_FreeThreadSafeContext(ctx);
// }

// TEST_F(IndexErrorTest, testBasic3) {
//   // create index
//   // add error
//   // drop index
//   // verify N/A refcount
//   // verify spec->error->key->refcount
//   // verify field->error->key->refcount
//   RedisModuleCtx *ctx = RedisModule_GetThreadSafeContext(NULL);
//   QueryError qerr = {QueryErrorCode(0)};
//   auto na = getNAstring(); // initialize the NA string
//   ASSERT_EQ(na->getRefCount(), 1);

//   // Create a new index spec with error
//   RMCK::ArgvList args(ctx, "FT.CREATE", "idx", "ON", "HASH",
//                       "SCHEMA", "n", "numeric");

//   auto spec = IndexSpec_CreateNew(ctx, args, args.size(), &qerr);
//   ASSERT_NE(spec, nullptr);
//   ASSERT_EQ(na->getRefCount(), 3);

//   // Add an error to the index
//   const char* expected = "meow";
//   RedisModuleString *key = RedisModule_CreateString(NULL, expected, strlen(expected));
//   IndexError_AddError(&spec->stats.indexError, "error", "error1", key);
//   ASSERT_EQ(na->getRefCount(), 2);
//   ASSERT_EQ(spec->stats.indexError.key->getRefCount(), 2);
//   ASSERT_EQ(key->getRefCount(), 2);

//   // Drop the index
//   IndexSpec_RemoveFromGlobals(spec->own_ref, true);
//   ASSERT_EQ(na->getRefCount(), 1); // in index clear function - if key is not N/A we replace it with N/A and increase the refcount
//   ASSERT_EQ(key->getRefCount(), 1);
//   args.clear();
//   RedisModule_FreeThreadSafeContext(ctx);
// }
// TEST_F(IndexErrorTest, testBasic3) {
  // create index
  // // test info
  // RedisModuleCtx *ctx = RedisModule_GetThreadSafeContext(NULL);
  // QueryError qerr = {QueryErrorCode(0)};

  // RMCK::ArgvList args(ctx, "FT.CREATE", "idx", "ON", "HASH",
  //                     "SCHEMA", "n", "numeric");

  // auto spec = IndexSpec_CreateNew(ctx, args, args.size(), &qerr);
  // RedisModuleCallReply* replycall = RedisModule_Call(ctx, "FT.info", "idx");



//   IndexError error = IndexError_Init();
//   auto na = getNAstring();
//   // ASSERT_EQ(na->getRefCount(), 2);
//   //IndexError_Reply
//   RedisModule_Reply reply = RedisModule_NewReply(ctx);
//   bool obfuscate = false;
//   IndexError_Reply(&error, &reply, false, obfuscate, false);
//   // ASSERT_EQ(na->getRefCount(), 2);

//   obfuscate = true;
//   IndexError_Reply(&error, &reply, false, obfuscate, false);
//   // ASSERT_EQ(na->getRefCount(), 2);
//   RedisModule_EndReply(&reply);


// //FieldSpecInfo_Reply

// //InfoReplyReducer (calls init)
// //IndexError_Deserialize(reply)
// //AggregatedFieldSpecInfo_Init
// //AggregatedFieldSpecInfo_Reply


//   // test info
//   // RedisModule_FreeThreadSafeContext(ctx);
// }


// TEST_F(IndexErrorTest, testBasic) {
  //test 1:
  // create index with error
  // verify N/A refcount



  // test 2:
  // create index
  // drop index
  // verify N/A refcount

  // test3:
  // create index
  // add error
  // drop index
  // verify N/A refcount
  // verify spec->error->key->refcount
  // verify field->error->key->refcount


  // test4:
  // create index
  // ft.info shard
  // verify N/A refcount
  // verify spec->error->key->refcount
  // verify field->error->key->refcount


  // test5:
  // create index
  // add error
  // ft.info cluster
  // verify N/A refcount
  // verify spec->error->key->refcount
  // verify field->error->key->refcount
// }
