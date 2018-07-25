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

typedef struct RS_Suggestion {
  double score;
  char* suggestion;
  size_t len;
} RS_Suggestion;

typedef struct RS_Suggestions {
  Trie* suggestionsTrie;
  RS_Suggestion** suggestions;
} RS_Suggestions;

typedef struct SpellCheckCtx {
  RedisSearchCtx* sctx;
  char** includeDict;
  char** excludeDict;
  long long distance;
} SpellCheckCtx;

void SpellCheck_Reply(SpellCheckCtx* ctx, QueryParseCtx* q);

#endif /* SRC_SPELL_CHECK_H_ */
