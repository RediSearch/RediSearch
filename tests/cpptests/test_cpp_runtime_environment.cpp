/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#include "gtest/gtest.h"

#include <cstring>
#include <string>

extern "C" size_t GetDefaultWorkerThreads(void);

#include "commands.h"
#include "module.h"
#include "search_disk.h"

extern "C" {
#include "coord/config.h"
}

enum class ServerType { OSS, Enterprise };
enum class DiskMode { Disabled, Enabled };
enum class Coordination { WithoutProxy, WithProxy };

class RuntimeEnvironmentTest : public ::testing::Test {
 protected:
  void SetServer(ServerType server) {
    isEnterprise = server == ServerType::Enterprise;
    serverInfo = isEnterprise ? "# Server\r\nredis_version:8.2.0\r\nrlec_version:8.0.0-1\r\n"
                              : "# Server\r\nredis_version:8.2.0\r\n";
  }

  void SetClusterEnabled(bool enabled) {
    contextFlags = enabled ? REDISMODULE_CTX_FLAGS_CLUSTER : 0;
  }

  void SetDiskMode(DiskMode mode) {
    isFlex = mode == DiskMode::Enabled;
    RSGlobalConfig.simulateInFlex = false;
  }

  void SetCoordination(Coordination mode) {
    clusterConfig.type =
        mode == Coordination::WithProxy ? ClusterType_RedisLabs : ClusterType_RedisOSS;
  }

  void DetectCoordination() {
    clusterConfig.type = DetectClusterType();
  }

  void ExpectMetadataCommandNames(const char* prefix) {
    const std::string expectedPrefix(prefix);

#define EXPECT_COMMAND_NAME(command, suffix) \
  EXPECT_EQ(std::string(CMD_FOR_COORDINATOR(command)), expectedPrefix + suffix)

    EXPECT_COMMAND_NAME(RS_CREATE_CMD, "CREATE");
    EXPECT_COMMAND_NAME(RS_CREATE_IF_NX_CMD, "_CREATEIFNX");
    EXPECT_COMMAND_NAME(RS_RESTORE_IF_NX, "_RESTOREIFNX");
    EXPECT_COMMAND_NAME(RS_ALTER_IF_NX_CMD, "_ALTERIFNX");
    EXPECT_COMMAND_NAME(RS_DROP_IF_X_CMD, "_DROPIFX");
    EXPECT_COMMAND_NAME(RS_DROP_INDEX_IF_X_CMD, "_DROPINDEXIFX");
    EXPECT_COMMAND_NAME(RS_ALIASADD_IF_NX, "_ALIASADDIFNX");
    EXPECT_COMMAND_NAME(RS_ALIASDEL_IF_X, "_ALIASDELIFX");
    EXPECT_COMMAND_NAME(RS_DICT_ADD, "DICTADD");
    EXPECT_COMMAND_NAME(RS_DROP_INDEX_CMD, "DROPINDEX");

#undef EXPECT_COMMAND_NAME
  }

  void SetUp() override {
    SetServer(ServerType::OSS);
    SetClusterEnabled(false);
    SetDiskMode(DiskMode::Disabled);

    RedisModule_GetContextFlags = [](RedisModuleCtx*) { return contextFlags; };
    RedisModule_Call = ReplyWithServerInfo;
    RedisModule_CallReplyType = [](RedisModuleCallReply*) { return REDISMODULE_REPLY_STRING; };
    RedisModule_CallReplyStringPtr = [](RedisModuleCallReply*, size_t* length) {
      *length = std::strlen(serverInfo);
      return serverInfo;
    };
    RedisModule_FreeCallReply = [](RedisModuleCallReply*) {};
  }

  void TearDown() override {
    isEnterprise = savedEnterprise;
    isFlex = savedFlex;
    RSGlobalConfig.simulateInFlex = savedSimulation;
    clusterConfig.type = savedCoordinator;
    RedisModule_Call = savedCall;
    RedisModule_CallReplyType = savedReplyType;
    RedisModule_CallReplyStringPtr = savedReplyString;
    RedisModule_FreeCallReply = savedFreeReply;
    RedisModule_GetContextFlags = savedContextFlags;
  }

 private:
  static RedisModuleCallReply* ReplyWithServerInfo(RedisModuleCtx*, const char*, const char*, ...) {
    return reinterpret_cast<RedisModuleCallReply*>(const_cast<char*>(serverInfo));
  }

  static inline int contextFlags = 0;
  static inline const char* serverInfo = "";
  const bool savedEnterprise = isEnterprise;
  const bool savedFlex = isFlex;
  const bool savedSimulation = RSGlobalConfig.simulateInFlex;
  const MRClusterType savedCoordinator = clusterConfig.type;
  decltype(RedisModule_Call) savedCall = RedisModule_Call;
  decltype(RedisModule_CallReplyType) savedReplyType = RedisModule_CallReplyType;
  decltype(RedisModule_CallReplyStringPtr) savedReplyString = RedisModule_CallReplyStringPtr;
  decltype(RedisModule_FreeCallReply) savedFreeReply = RedisModule_FreeCallReply;
  decltype(RedisModule_GetContextFlags) savedContextFlags = RedisModule_GetContextFlags;
};

#ifdef RS_CLUSTER_ENTERPRISE
TEST_F(RuntimeEnvironmentTest, ProxyBuildKeepsProxyCoordinationOnEnterpriseCluster) {
  SetServer(ServerType::Enterprise);
  SetClusterEnabled(true);

  DetectCoordination();

  EXPECT_TRUE(RS_UsesProxyCoordination());
}
#else
TEST_F(RuntimeEnvironmentTest, OSSBuildUsesNoProxyCoordinationOnEnterpriseCluster) {
  SetServer(ServerType::Enterprise);
  SetClusterEnabled(true);

  DetectCoordination();

  EXPECT_FALSE(RS_UsesProxyCoordination());
}
#endif

TEST_F(RuntimeEnvironmentTest, EnterpriseWithoutClusterKeepsProxyCoordination) {
  SetServer(ServerType::Enterprise);
  SetClusterEnabled(false);

  DetectCoordination();

  EXPECT_TRUE(RS_UsesProxyCoordination());
}

TEST_F(RuntimeEnvironmentTest, OSSServerUsesNoProxyCoordination) {
  SetServer(ServerType::OSS);
  SetClusterEnabled(false);

  DetectCoordination();

  EXPECT_FALSE(RS_UsesProxyCoordination());
}

TEST_F(RuntimeEnvironmentTest, DetectingCoordinationPreservesEnterpriseWorkerDefault) {
  SetServer(ServerType::Enterprise);
  SetDiskMode(DiskMode::Enabled);

  for (bool clusterEnabled : {false, true}) {
    SCOPED_TRACE(clusterEnabled ? "Cluster enabled" : "Cluster disabled");
    SetClusterEnabled(clusterEnabled);

    DetectCoordination();

    EXPECT_EQ(GetDefaultWorkerThreads(), 0);
  }
}

struct CommandNamingCase {
  const char* name;
  ServerType server;
  DiskMode disk;
  Coordination coordination;
  const char* expectedPrefix;
};

class CommandNamingTest : public RuntimeEnvironmentTest,
                          public ::testing::WithParamInterface<CommandNamingCase> {};

TEST_P(CommandNamingTest, CommandNamesFollowCoordinationMode) {
  const auto& scenario = GetParam();
  SetServer(scenario.server);
  SetDiskMode(scenario.disk);
  SetCoordination(scenario.coordination);

  ExpectMetadataCommandNames(scenario.expectedPrefix);
}

// Vary each input independently to catch accidental server/disk dependencies.
// These are isolated naming cases, not a list of supported deployments.
INSTANTIATE_TEST_SUITE_P(
    IndependentRuntimeDecisions, CommandNamingTest,
    ::testing::Values(CommandNamingCase{"OSSRamWithoutProxy", ServerType::OSS, DiskMode::Disabled,
                                        Coordination::WithoutProxy, "_FT."},
                      CommandNamingCase{"OSSDiskWithoutProxy", ServerType::OSS, DiskMode::Enabled,
                                        Coordination::WithoutProxy, "_FT."},
                      CommandNamingCase{"EnterpriseRamWithoutProxy", ServerType::Enterprise,
                                        DiskMode::Disabled, Coordination::WithoutProxy, "_FT."},
                      CommandNamingCase{"EnterpriseDiskWithoutProxy", ServerType::Enterprise,
                                        DiskMode::Enabled, Coordination::WithoutProxy, "_FT."},
                      CommandNamingCase{"OSSRamWithProxy", ServerType::OSS, DiskMode::Disabled,
                                        Coordination::WithProxy, "FT."},
                      CommandNamingCase{"OSSDiskWithProxy", ServerType::OSS, DiskMode::Enabled,
                                        Coordination::WithProxy, "FT."},
                      CommandNamingCase{"EnterpriseRamWithProxy", ServerType::Enterprise,
                                        DiskMode::Disabled, Coordination::WithProxy, "FT."},
                      CommandNamingCase{"EnterpriseDiskWithProxy", ServerType::Enterprise,
                                        DiskMode::Enabled, Coordination::WithProxy, "FT."}),
    [](const ::testing::TestParamInfo<CommandNamingCase>& info) {
      return std::string(info.param.name);
    });
