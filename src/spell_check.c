#include "spell_check.h"
#include "util/arr.h"
#include "dictionary.h"
#include <stdbool.h>

///////////////////////////////////////////////////////////////////////////////////////////////

static int RS_Suggestions::Compare(const RS_Suggestion **val1, const RS_Suggestion **val2) {
  if ((*val1)->score > (*val2)->score) {
    return -1;
  }
  if ((*val1)->score < (*val2)->score) {
    return 1;
  }
  return 0;
}

//---------------------------------------------------------------------------------------------

RS_Suggestion::RS_Suggestion(rune *ru, t_len ru_len, double score) :
    score(score) {
  suggestion = runesToStr(ru, ru_len, &len);
}

//---------------------------------------------------------------------------------------------

RS_Suggestion::~RS_Suggestion() {
  rm_free(suggestion);
}

//---------------------------------------------------------------------------------------------

void RS_Suggestions::Add(char *term, size_t len, double score, int incr) {
  double currScore;
  bool isExists = SpellChecker::IsTermExistsInTrie(&suggestionsTrie, term, len, &currScore);
  if (score == 0) {
    // we can not add zero score so we set it to -1 instead
    score = -1;
  }

  if (!incr) {
    if (!isExists) {
      //@@@TODO: term is coming from rule and InsertStringBuffer will convert it back to rune
      suggestionsTrie.InsertStringBuffer(term, len, score, incr, NULL);
    }
    return;
  }

  if (isExists && score == -1) {
    return;
  }

  if (!isExists || currScore == -1) {
    incr = 0;
  }

  //@@@TODO: term is coming from rule and InsertStringBuffer will convert it back to rune
  suggestionsTrie.InsertStringBuffer(term, len, score, incr, NULL);
}

///////////////////////////////////////////////////////////////////////////////////////////////

/**
 * Return the score for the given suggestion (number between 0 to 1).
 * In case the suggestion should not be added return -1.
 */

double SpellChecker::GetScore(char *suggestion, size_t len, t_fieldMask fieldMask) {
  RedisModuleKey *keyp = NULL;
  IndexReader *reader = NULL;
  IndexIterator *iter = NULL;
  IndexResult *r;
  double retVal = 0;
  InvertedIndex *invidx = Redis_OpenInvertedIndexEx(sctx, suggestion, len, 0, &keyp);
  if (!invidx) {
    // can not find inverted index key, score is 0.
    goto end;
  }

  reader = new TermIndexReader(invidx, NULL, fieldMask, NULL, 1);
  iter = reader->NewReadIterator();
  if (iter->Read(&r) != INDEXREAD_EOF) {
    // we have at least one result, the suggestion is relevant.
    if (fullScoreInfo) {
      retVal = invidx->numDocs;
    } else {
      retVal = invidx->numDocs;
    }
  } else {
    // fieldMask has filtered all docs, this suggestions should not be returned
    retVal = -1;
  }

end:
  if (keyp) {
    RedisModule_CloseKey(keyp);
  }
  return retVal;
}

//---------------------------------------------------------------------------------------------

bool SpellChecker::IsTermExistsInTrie(Trie *t, const char *term, size_t len, double *outScore) {
  rune *rstr = NULL;
  t_len slen = 0;
  float score = 0;
  int dist = 0;
  bool retVal = false;
  TrieIterator<DFAFilter> it = t->Iterate(term, len, 0, 0);
  // TrieIterator can be NULL when rune length exceed TRIE_MAX_PREFIX
  if (it == NULL) {
    return retVal;
  }
  if (it.Next(&rstr, &slen, NULL, &score, &dist)) {
    retVal = true;
  }
  if (outScore) {
    *outScore = score;
  }
  return retVal;
}

//---------------------------------------------------------------------------------------------

void SpellChecker::FindSuggestions(Trie *t, const char *term, size_t len, t_fieldMask fieldMask,
                                    RS_Suggestions &suggestions, int incr) {
  rune *rstr = NULL;
  t_len slen = 0;
  float score = 0;
  int dist = 0;
  size_t suggestionLen;

  TrieIterator<DFAFilter> it = t->Iterate(term, len, (int)distance, 0);
  // TrieIterator can be NULL when rune length exceed TRIE_MAX_PREFIX
  if (it == NULL) {
    return;
  }
  while (it.Next(&rstr, &slen, NULL, &score, &dist)) {
    char *res = runesToStr(rstr, slen, &suggestionLen);
    double score;
    if ((score = GetScore(res, suggestionLen, fieldMask)) != -1) {
      suggestions.Add(res, suggestionLen, score, incr);
    }
    rm_free(res);
  }
}

//---------------------------------------------------------------------------------------------

arrayof(RS_Suggestion*) RS_Suggestions::GetSuggestions() {
  TrieIterator<DFAFilter> iter = suggestionsTrie.Iterate("", 0, 0, 1);
  arrayof(RS_Suggestion*) ret = array_new(RS_Suggestion *, suggestionsTrie.size);
  rune *rstr = NULL;
  t_len slen = 0;
  float score = 0;
  int dist = 0;
  while (iter.Next(&rstr, &slen, NULL, &score, &dist)) {
    ret = array_append(ret, new RS_Suggestion(rstr, slen, score));
  }
  return ret;
}

//---------------------------------------------------------------------------------------------

void RS_Suggestions::SendReplyOnTerm(RedisModuleCtx *ctx, char *term, size_t len, uint64_t totalDocNumber) {
#define TERM "TERM"
  RedisModule_ReplyWithArray(ctx, 3);
  RedisModule_ReplyWithStringBuffer(ctx, TERM, strlen(TERM));
  RedisModule_ReplyWithStringBuffer(ctx, term, len);

  arrayof(RS_Suggestion*) suggestions = GetSuggestions();

  for (int i = 0; i < array_len(suggestions); ++i) {
    if (suggestions[i]->score == -1) {
      suggestions[i]->score = 0;
    } else {
      if (totalDocNumber > 0) {
        suggestions[i]->score = (suggestions[i]->score) / totalDocNumber;
      }
    }
  }

  qsort(suggestions, array_len(suggestions), sizeof(RS_Suggestion *), (__compar_fn_t) RS_Suggestion::_Compare);

  if (array_len(suggestions) == 0) {
    // no results found, we return an empty array
    RedisModule_ReplyWithArray(ctx, 0);
  } else {
    RedisModule_ReplyWithArray(ctx, array_len(suggestions));
    for (int i = 0; i < array_len(suggestions); ++i) {
      RedisModule_ReplyWithArray(ctx, 2);
      RedisModule_ReplyWithDouble(ctx, suggestions[i]->score);
      RedisModule_ReplyWithStringBuffer(ctx, suggestions[i]->suggestion, suggestions[i]->len);
    }
  }

  array_free_ex(suggestions, RS_SuggestionFree(*(RS_Suggestion **)ptr));
}

//---------------------------------------------------------------------------------------------

inline bool SpellChecker::ReplyTermSuggestions(char *term, size_t len, t_fieldMask fieldMask) {
  // searching the term on the term trie, if its there we just return false
  // because there is no need to return suggestions on it.
  if (SpellChecker::IsTermExistsInTrie(sctx->spec->terms, term, len, NULL)) {
    if (fullScoreInfo) {
      // if a full score info is requested we need to send information that
      // we found the term as is on the index
      RedisModule_ReplyWithArray(sctx->redisCtx, 3);
      RedisModule_ReplyWithStringBuffer(sctx->redisCtx, TERM, strlen(TERM));
      RedisModule_ReplyWithStringBuffer(sctx->redisCtx, term, len);
      RedisModule_ReplyWithStringBuffer(sctx->redisCtx, FOUND_TERM_IN_INDEX,
                                        strlen(FOUND_TERM_IN_INDEX));
      return true;
    } else {
      return false;
    }
  }

  // searching the term on the exclude list, if its there we just return false
  // because there is no need to return suggestions on it.
  for (int i = 0; i < array_len(excludeDict); ++i) {
    RedisModuleKey *k = NULL;
    Trie *t =
        SpellCheck_OpenDict(sctx->redisCtx, excludeDict[i], REDISMODULE_READ, &k);
    if (t == NULL) {
      continue;
    }
    if (SpellChecker::IsTermExistsInTrie(t, term, len, NULL)) {
      RedisModule_CloseKey(k);
      return false;
    }
    RedisModule_CloseKey(k);
  }

  RS_Suggestions s;

  FindSuggestions(sctx->spec->terms, term, len, fieldMask, s, 1);

  // sorting results by score

  // searching the term on the include list for more suggestions.
  for (int i = 0; i < array_len(includeDict); ++i) {
    RedisModuleKey *k = NULL;
    Trie *t =
        SpellCheck_OpenDict(sctx->redisCtx, includeDict[i], REDISMODULE_READ, &k);
    if (t == NULL) {
      continue;
    }
    FindSuggestions(t, term, len, fieldMask, s, 0);
    RedisModule_CloseKey(k);
  }

  s.SendReplyOnTerm(sctx->redisCtx, term, len, (!fullScoreInfo) ? sctx->spec->docs.size - 1 : 0);

  return true;
}

//---------------------------------------------------------------------------------------------

inline bool SpellChecker::CheckDictExistence(const char *dict) {
#define BUFF_SIZE 1000
  RedisModuleKey *k = NULL;
  Trie *t = SpellCheck_OpenDict(sctx->redisCtx, dict, REDISMODULE_READ, &k);
  if (t == NULL) {
    char buff[BUFF_SIZE];
    snprintf(buff, BUFF_SIZE, "Dict does not exist: %s", dict);
    RedisModule_ReplyWithError(sctx->redisCtx, buff);
    return false;
  }
  RedisModule_CloseKey(k);
  return true;
}

//---------------------------------------------------------------------------------------------

inline bool SpellChecker::CheckTermDictsExistance() {
  for (int i = 0; i < array_len(includeDict); ++i) {
    if (!CheckDictExistence(includeDict[i])) {
      return false;
    }
  }

  for (int i = 0; i < array_len(excludeDict); ++i) {
    if (!CheckDictExistence(excludeDict[i])) {
      return false;
    }
  }

  return true;
}

//---------------------------------------------------------------------------------------------

static int forEachCallback(QueryNode *n, SpellChecker *self) {
    if (n->type == QN_TOKEN) {
    QueryTokenNode *tn = dynamic_cast<QueryTokenNode*>(n);
    if (self->ReplyTermSuggestions(tn->tok.str, tn->tok.len, tn->opts.fieldMask)) {
      self->results++;
    }
  }

  return true;
}

//---------------------------------------------------------------------------------------------

void SpellChecker::Reply(QueryAST *q) {
  if (!CheckTermDictsExistance()) {
    return;
  }

  RedisModule_ReplyWithArray(sctx->redisCtx, REDISMODULE_POSTPONED_ARRAY_LEN);

  if (fullScoreInfo) {
    // sending the total number of docs for the ability to calculate score on cluster
    RedisModule_ReplyWithLongLong(sctx->redisCtx, sctx->spec->docs.size - 1);
  }

  q->root->ForEach(forEachCallback, this, 1);

  RedisModule_ReplySetArrayLength(sctx->redisCtx, results + (fullScoreInfo ? 1 : 0));
}

///////////////////////////////////////////////////////////////////////////////////////////////
