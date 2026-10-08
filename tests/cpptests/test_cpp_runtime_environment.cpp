/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#include "gtest/gtest.h"
extern "C" size_t GetDefaultWorkerThreads(void);
#include "commands.h"
#include "search_disk.h"
#include "module.h"
#include <cstring>
extern "C" {
#include "coord/config.h"
}

class RuntimeEnvironmentTest : public ::testing::Test {
 protected:
  bool savedEnterprise = isEnterprise;
  bool savedFlex = isFlex;
  bool savedSimulation = RSGlobalConfig.simulateInFlex;
  MRClusterType savedTopology = clusterConfig.type;
  decltype(RedisModule_Call) savedCall = RedisModule_Call;
  decltype(RedisModule_CallReplyType) savedReplyType = RedisModule_CallReplyType;
  decltype(RedisModule_CallReplyStringPtr) savedReplyString = RedisModule_CallReplyStringPtr;
  decltype(RedisModule_FreeCallReply) savedFreeReply = RedisModule_FreeCallReply;
  decltype(RedisModule_GetContextFlags) savedFlags = RedisModule_GetContextFlags;
  static inline int contextFlags;
  static inline const char *info;

  static RedisModuleCallReply *replyInfo(RedisModuleCtx *, const char *, const char *, ...) {
    return reinterpret_cast<RedisModuleCallReply *>(const_cast<char *>(info));
  }

  void SetUp() override {
    RedisModule_GetContextFlags = [](RedisModuleCtx *) { return contextFlags; };
    RedisModule_Call = replyInfo;
    RedisModule_CallReplyType = [](RedisModuleCallReply *) { return REDISMODULE_REPLY_STRING; };
    RedisModule_CallReplyStringPtr = [](RedisModuleCallReply *, size_t *len) {
      *len = strlen(info);
      return info;
    };
    RedisModule_FreeCallReply = [](RedisModuleCallReply *) {};

  }

  void TearDown() override {
    isEnterprise = savedEnterprise;
    isFlex = savedFlex;
    RSGlobalConfig.simulateInFlex = savedSimulation;
    clusterConfig.type = savedTopology;
    RedisModule_Call = savedCall;
    RedisModule_CallReplyType = savedReplyType;
    RedisModule_CallReplyStringPtr = savedReplyString;
    RedisModule_FreeCallReply = savedFreeReply;
    RedisModule_GetContextFlags = savedFlags;
  }
};

TEST_F(RuntimeEnvironmentTest, NativeClusterSelectionPreservesFlexDefaults) {
  isEnterprise = true;
  isFlex = true;
  info = "# Server\r\nredis_version:8.2.0\r\nrlec_version:8.0.0-1\r\n";
  for (int flags : {0, REDISMODULE_CTX_FLAGS_CLUSTER}) {
    contextFlags = flags;
    clusterConfig.type = DetectClusterType();
#ifdef RS_CLUSTER_ENTERPRISE
    EXPECT_FALSE(RS_IsOSSCoordinator());
#else
    EXPECT_EQ(RS_IsOSSCoordinator(), flags != 0);
#endif
    EXPECT_TRUE(RS_IsEnterpriseServer());
    EXPECT_TRUE(SearchDisk_IsEnabled());
    EXPECT_EQ(GetDefaultWorkerThreads(), 0);
  }
  info = "# Server\r\nredis_version:8.2.0\r\n";
  contextFlags = 0;
  EXPECT_EQ(DetectClusterType(), ClusterType_RedisOSS);
}

TEST_F(RuntimeEnvironmentTest, LocalAndReplicationNamesFollowCoordinator) {
  for (bool enterprise : {false, true}) {
    for (bool disk : {false, true}) {
      for (auto topology : {ClusterType_RedisOSS, ClusterType_RedisLabs}) {
        isEnterprise = enterprise;
        isFlex = disk;
        clusterConfig.type = topology;
        bool oss = topology == ClusterType_RedisOSS;
        EXPECT_EQ(RS_IsEnterpriseServer(), enterprise);
        EXPECT_EQ(SearchDisk_IsEnabled(), disk);
#define CHECK_COMMAND(command, publicName) \
  EXPECT_STREQ(CMD_FOR_COORDINATOR(command), oss ? "_" publicName : publicName)
        CHECK_COMMAND(RS_CREATE_CMD, "FT.CREATE");
        CHECK_COMMAND(RS_CREATE_IF_NX_CMD, "FT._CREATEIFNX");
        CHECK_COMMAND(RS_RESTORE_IF_NX, "FT._RESTOREIFNX");
        CHECK_COMMAND(RS_ALTER_IF_NX_CMD, "FT._ALTERIFNX");
        CHECK_COMMAND(RS_DROP_IF_X_CMD, "FT._DROPIFX");
        CHECK_COMMAND(RS_DROP_INDEX_IF_X_CMD, "FT._DROPINDEXIFX");
        CHECK_COMMAND(RS_ALIASADD_IF_NX, "FT._ALIASADDIFNX");
        CHECK_COMMAND(RS_ALIASDEL_IF_X, "FT._ALIASDELIFX");
        CHECK_COMMAND(RS_DICT_ADD, "FT.DICTADD");
        CHECK_COMMAND(RS_DROP_INDEX_CMD, "FT.DROPINDEX");
#undef CHECK_COMMAND
      }
    }
  }
}
