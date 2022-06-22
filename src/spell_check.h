
#ifndef SRC_SPELL_CHECK_H_
#define SRC_SPELL_CHECK_H_

#include "search_ctx.h"
#include "query.h"

#define FOUND_TERM_IN_INDEX "term exists in index"

struct RS_Suggestion : Object {
  double score;
  char *suggestion;
  size_t len;

  RS_Suggestion();
  RS_Suggestion(char *suggestion, size_t len, double score);
  ~RS_Suggestion();

  static int Compare(const RS_Suggestion **val1, const RS_Suggestion **val2);

  int Compare(const RS_Suggestion **val) const {
    return Compare(this, val);
  }
};

struct RS_Suggestions : Object {
  Trie *suggestionsTrie;

  RS_Suggestions();
  ~RS_Suggestions();

  void Add(char *term, size_t len, double score, int incr);
  RS_Suggestion **GetSuggestions();
  void SendReplyOnTerm(RedisModuleCtx *ctx, char *term, size_t len, uint64_t totalDocNumber);
};

struct SpellCheckCtx {
  RedisSearchCtx *sctx;
  const char **includeDict;
  const char **excludeDict;
  long long distance;
  bool fullScoreInfo;
  size_t results;

  void Reply(QueryAST *q);
  bool ReplyTermSuggestions(char *term, size_t len, t_fieldMask fieldMask);

  void FindSuggestions(Trie *t, const char *term, size_t len, t_fieldMask fieldMask, RS_Suggestions *s, int incr);
  double GetScore(char *suggestion, size_t len, t_fieldMask fieldMask);

  bool CheckDictExistence(const char *dict);
  bool CheckTermDictsExistance();
};

#endif /* SRC_SPELL_CHECK_H_ */
