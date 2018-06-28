/*
 * debug_commads.h
 *
 *  Created on: Jun 27, 2018
 *      Author: meir
 */

#ifndef SRC_DEBUG_COMMADS_H_
#define SRC_DEBUG_COMMADS_H_

#include "redismodule.h"
#include "index_iterator.h"
#include <stdbool.h>

#define DUMP_INVIDX_COMMAND "DUMP_INVIDX"
#define DUMP_NUMIDX_COMMAND "DUMP_NUMIDX"
#define DUMP_TAGIDX_COMMAND "DUMP_TAGIDX"

/**
 * debug command implementation
 * Currently three sub-commands available
 * 1.  DUMP_INVIDX - which dump all doc ids in an inverted index
 * 2.  DUMP_NUMIDX - which dump all doc ids in a numeric index
 * 3.  DUMP_TAGIDX - which dump all doc ids in a tag index
 *
 */
int DebugCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);

#endif /* SRC_DEBUG_COMMADS_H_ */
