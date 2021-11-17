
#pragma once

#include "cluster.h"

MRClusterTopology *RedisEnterprise_ParseTopology(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
