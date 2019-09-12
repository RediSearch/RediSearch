
#ifndef SRC_SPELL_CHECK_H_
#define SRC_SPELL_CHECK_H_

#include "search_ctx.h"
#include "query.h"

#define FOUND_TERM_IN_INDEX "term exists in index"

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
} SpellCheckCtx;

RS_Suggestions *RS_SuggestionsCreate();
void RS_SuggestionsAdd(RS_Suggestions *s, char *term, size_t len, double score, int incr);
void RS_SuggestionsFree(RS_Suggestions *s);

RS_Suggestion **spellCheck_GetSuggestions(RS_Suggestions *s);

RS_Suggestion *RS_SuggestionCreate(char *suggestion, size_t len, double score);
int RS_SuggestionCompare(const void *val1, const void *val2);
void SpellCheck_SendReplyOnTerm(RedisModuleCtx *ctx, char *term, size_t len, RS_Suggestions *s,
                                uint64_t totalDocNumber);
void SpellCheck_Reply(SpellCheckCtx *ctx, QueryAST *q);

#endif /* SRC_SPELL_CHECK_H_ */
