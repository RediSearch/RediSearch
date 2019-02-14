#ifndef SRC_DEBUG_COMMADS_H_
#define SRC_DEBUG_COMMADS_H_

#include "redismodule.h"

int DebugCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);

#endif /* SRC_DEBUG_COMMADS_H_ */
