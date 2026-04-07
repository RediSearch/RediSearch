/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#ifndef SRC_SPELL_CHECK_H_
#define SRC_SPELL_CHECK_H_

#include <stdbool.h>         // for bool
#include <stddef.h>          // for size_t
#include <stdint.h>          // for uint64_t

#include "search_ctx.h"      // for RedisSearchCtx
#include "query.h"           // for QueryAST
#include "reply.h"           // for RedisModule_Reply
#include "trie/trie_type.h"  // for Trie

#define SPELL_CHECK_TERM_CONST "TERM"

#define SPELL_CHECK_FOUND_TERM_IN_INDEX "term exists in index"

typedef struct RS_Suggestion {
  double score;
  char *suggestion;
  size_t len;
} RS_Suggestion;

typedef struct RS_Suggestions {
  Trie *suggestionsTrie;
} RS_Suggestions;

typedef struct SpellCheckCtx {
  RedisSearchCtx *sctx;
  const char **includeDict;
  const char **excludeDict;
  long long distance;
  bool fullScoreInfo;
  size_t results;
  RedisModule_Reply *reply;
} SpellCheckCtx;

RS_Suggestions *RS_SuggestionsCreate();
void RS_SuggestionsAdd(RS_Suggestions *s, char *term, size_t len, double score, int incr);
void RS_SuggestionsFree(RS_Suggestions *s);

RS_Suggestion **spellCheck_GetSuggestions(RS_Suggestions *s);

RS_Suggestion *RS_SuggestionCreate(char *suggestion, size_t len, double score);
int RS_SuggestionCompare(const void *val1, const void *val2);
void SpellCheck_SendReplyOnTerm(RedisModule_Reply *reply, char *term, size_t len, RS_Suggestions *s,
                                uint64_t totalDocNumber);
void SpellCheck_Reply(SpellCheckCtx *ctx, QueryAST *q);

#endif /* SRC_SPELL_CHECK_H_ */
