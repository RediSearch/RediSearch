/*
 * spell_check.h
 *
 *  Created on: Jul 12, 2018
 *      Author: meir
 */

#ifndef SRC_SPELL_CHECK_H_
#define SRC_SPELL_CHECK_H_

#include "search_ctx.h"
#include "query.h"

#define DICT_KEY_PREFIX "dict:"
#define DICT_KEY_FMT DICT_KEY_PREFIX "%s"

typedef struct RS_Suggestions{
  double score;
  char* suggestion;
}RS_Suggestion;

void SpellCheck_Reply(RedisSearchCtx *sctx, QueryParseCtx *q, char** includeDict, char** excludeDict);

int SpellCheck_DictAdd(RedisModuleCtx *ctx, const char* dictName, RedisModuleString **values, int len, char** err);

int SpellCheck_DictDel(RedisModuleCtx *ctx, const char* dictName, RedisModuleString **values, int len, char** err);

int SpellCheck_DictDump(RedisModuleCtx *ctx, const char* dictName, char** err);



#endif /* SRC_SPELL_CHECK_H_ */
