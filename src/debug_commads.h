/*
 * debug_commads.h
 *
 *  Created on: Jun 27, 2018
 *      Author: meir
 */

#ifndef SRC_DEBUG_COMMADS_H_
#define SRC_DEBUG_COMMADS_H_

#include "redismodule.h"

int DebugCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);

#endif /* SRC_DEBUG_COMMADS_H_ */
