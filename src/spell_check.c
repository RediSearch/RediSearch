#include "spell_check.h"
#include "util/arr.h"
#include "dictionary.h"
#include <stdbool.h>

/** Forward declaration **/
static bool SpellCheck_IsTermExistsInTrie(Trie *t, const char *term, size_t len, double *outScore);

int RS_SuggestionCompare(const void *val1, const void *val2) {
  const RS_Suggestion **a = (const RS_Suggestion **)val1;
  const RS_Suggestion **b = (const RS_Suggestion **)val2;
  if ((*a)->score > (*b)->score) {
    return -1;
  }
  if ((*a)->score < (*b)->score) {
    return 1;
  }
  return 0;
}

RS_Suggestion *RS_SuggestionCreate(char *suggestion, size_t len, double score) {
  RS_Suggestion *res = rm_calloc(1, sizeof(RS_Suggestion));
  res->suggestion = suggestion;
  res->len = len;
  res->score = score;
  return res;
}

static void RS_SuggestionFree(RS_Suggestion *suggestion) {
  rm_free(suggestion->suggestion);
  rm_free(suggestion);
}

RS_Suggestions *RS_SuggestionsCreate() {
#define SUGGESTIONS_ARRAY_INITIAL_SIZE 10
  RS_Suggestions *ret = rm_calloc(1, sizeof(RS_Suggestions));
  ret->suggestionsTrie = NewTrie();
  return ret;
}

void RS_SuggestionsAdd(RS_Suggestions *s, char *term, size_t len, double score, int incr) {
  double currScore;
  bool isExists = SpellCheck_IsTermExistsInTrie(s->suggestionsTrie, term, len, &currScore);
  if (score == 0) {
    /** we can not add zero score so we set it to -1 instead :\ **/
    score = -1;
  }

  if (!incr) {
    if (!isExists) {
      Trie_InsertStringBuffer(s->suggestionsTrie, term, len, score, incr, NULL);
    }
    return;
  }

  if (isExists && score == -1) {
    return;
  }

  if (!isExists || currScore == -1) {
    incr = 0;
  }

  Trie_InsertStringBuffer(s->suggestionsTrie, term, len, score, incr, NULL);
}

void RS_SuggestionsFree(RS_Suggestions *s) {
  //  array_free_ex(s->suggestions, RS_SuggestionFree(*(RS_Suggestion **)ptr));
  TrieType_Free(s->suggestionsTrie);
  rm_free(s);
}

/**
 * Return the score for the given suggestion (number between 0 to 1).
 * In case the suggestion should not be added return -1.
 */
static double SpellCheck_GetScore(SpellCheckCtx *scCtx, char *suggestion, size_t len,
                                  t_fieldMask fieldMask) {
  RedisModuleKey *keyp = NULL;
  InvertedIndex *invidx = Redis_OpenInvertedIndexEx(scCtx->sctx, suggestion, len, 0, &keyp);
  double retVal = 0;
  if (!invidx) {
    // can not find inverted index key, score is 0.
    goto end;
  }
  IndexReader *reader = NewTermIndexReader(invidx, NULL, fieldMask, NULL, 1);
  IndexIterator *iter = NewReadIterator(reader);
  RSIndexResult *r;
  if (iter->Read(iter->ctx, &r) != INDEXREAD_EOF) {
    // we have at least one result, the suggestion is relevant.
    if (scCtx->fullScoreInfo) {
      retVal = invidx->numDocs;
    } else {
      retVal = invidx->numDocs;
    }
  } else {
    // fieldMask has filtered all docs, this suggestions should not be returned
    retVal = -1;
  }
  ReadIterator_Free(iter);

end:
  if (keyp) {
    RedisModule_CloseKey(keyp);
  }
  return retVal;
}

static bool SpellCheck_IsTermExistsInTrie(Trie *t, const char *term, size_t len, double *outScore) {
  rune *rstr = NULL;
  t_len slen = 0;
  float score = 0;
  int dist = 0;
  bool retVal = false;
  TrieIterator *it = Trie_Iterate(t, term, len, 0, 0);
  // TrieIterator can be NULL when rune length exceed TRIE_MAX_PREFIX
  if (it == NULL) {
    return retVal;
  }
  if (TrieIterator_Next(it, &rstr, &slen, NULL, &score, &dist)) {
    retVal = true;
  }
  DFAFilter_Free(it->ctx);
  rm_free(it->ctx);
  TrieIterator_Free(it);
  if (outScore) {
    *outScore = score;
  }
  return retVal;
}

static void SpellCheck_FindSuggestions(SpellCheckCtx *scCtx, Trie *t, const char *term, size_t len,
                                       t_fieldMask fieldMask, RS_Suggestions *s, int incr) {
  rune *rstr = NULL;
  t_len slen = 0;
  float score = 0;
  int dist = 0;
  size_t suggestionLen;

  TrieIterator *it = Trie_Iterate(t, term, len, (int)scCtx->distance, 0);
  // TrieIterator can be NULL when rune length exceed TRIE_MAX_PREFIX
  if (it == NULL) {
    return;
  }
  while (TrieIterator_Next(it, &rstr, &slen, NULL, &score, &dist)) {
    char *res = runesToStr(rstr, slen, &suggestionLen);
    double score;
    if ((score = SpellCheck_GetScore(scCtx, res, suggestionLen, fieldMask)) != -1) {
      RS_SuggestionsAdd(s, res, suggestionLen, score, incr);
    }
    rm_free(res);
  }
  DFAFilter_Free(it->ctx);
  rm_free(it->ctx);
  TrieIterator_Free(it);
}

RS_Suggestion **spellCheck_GetSuggestions(RS_Suggestions *s) {
  TrieIterator *iter = Trie_Iterate(s->suggestionsTrie, "", 0, 0, 1);
  RS_Suggestion **ret = array_new(RS_Suggestion *, s->suggestionsTrie->size);
  rune *rstr = NULL;
  t_len slen = 0;
  float score = 0;
  int dist = 0;
  size_t termLen;
  while (TrieIterator_Next(iter, &rstr, &slen, NULL, &score, &dist)) {
    char *res = runesToStr(rstr, slen, &termLen);
    ret = array_append(ret, RS_SuggestionCreate(res, termLen, score));
  }
  DFAFilter_Free(iter->ctx);
  rm_free(iter->ctx);
  TrieIterator_Free(iter);
  return ret;
}

void SpellCheck_SendReplyOnTerm(RedisModuleCtx *ctx, char *term, size_t len, RS_Suggestions *s,
                                uint64_t totalDocNumber) {
#define TERM "TERM"
  RedisModule_ReplyWithArray(ctx, 3);
  RedisModule_ReplyWithStringBuffer(ctx, TERM, strlen(TERM));
  RedisModule_ReplyWithStringBuffer(ctx, term, len);

  RS_Suggestion **suggestions = spellCheck_GetSuggestions(s);

  for (int i = 0; i < array_len(suggestions); ++i) {
    if (suggestions[i]->score == -1) {
      suggestions[i]->score = 0;
    } else {
      if (totalDocNumber > 0) {
        suggestions[i]->score = (suggestions[i]->score) / totalDocNumber;
      }
    }
  }

  qsort(suggestions, array_len(suggestions), sizeof(RS_Suggestion *), RS_SuggestionCompare);

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

static bool SpellCheck_ReplyTermSuggestions(SpellCheckCtx *scCtx, char *term, size_t len,
                                            t_fieldMask fieldMask) {

  // searching the term on the term trie, if its there we just return false
  // because there is no need to return suggestions on it.
  if (SpellCheck_IsTermExistsInTrie(scCtx->sctx->spec->terms, term, len, NULL)) {
    if (scCtx->fullScoreInfo) {
      // if a full score info is requested we need to send information that
      // we found the term as is on the index
      RedisModule_ReplyWithArray(scCtx->sctx->redisCtx, 3);
      RedisModule_ReplyWithStringBuffer(scCtx->sctx->redisCtx, TERM, strlen(TERM));
      RedisModule_ReplyWithStringBuffer(scCtx->sctx->redisCtx, term, len);
      RedisModule_ReplyWithStringBuffer(scCtx->sctx->redisCtx, FOUND_TERM_IN_INDEX,
                                        strlen(FOUND_TERM_IN_INDEX));
      return true;
    } else {
      return false;
    }
  }

  // searching the term on the exclude list, if its there we just return false
  // because there is no need to return suggestions on it.
  for (int i = 0; i < array_len(scCtx->excludeDict); ++i) {
    RedisModuleKey *k = NULL;
    Trie *t =
        SpellCheck_OpenDict(scCtx->sctx->redisCtx, scCtx->excludeDict[i], REDISMODULE_READ, &k);
    if (t == NULL) {
      continue;
    }
    if (SpellCheck_IsTermExistsInTrie(t, term, len, NULL)) {
      RedisModule_CloseKey(k);
      return false;
    }
    RedisModule_CloseKey(k);
  }

  RS_Suggestions *s = RS_SuggestionsCreate();

  SpellCheck_FindSuggestions(scCtx, scCtx->sctx->spec->terms, term, len, fieldMask, s, 1);

  // sorting results by score

  // searching the term on the include list for more suggestions.
  for (int i = 0; i < array_len(scCtx->includeDict); ++i) {
    RedisModuleKey *k = NULL;
    Trie *t =
        SpellCheck_OpenDict(scCtx->sctx->redisCtx, scCtx->includeDict[i], REDISMODULE_READ, &k);
    if (t == NULL) {
      continue;
    }
    SpellCheck_FindSuggestions(scCtx, t, term, len, fieldMask, s, 0);
    RedisModule_CloseKey(k);
  }

  SpellCheck_SendReplyOnTerm(scCtx->sctx->redisCtx, term, len, s,
                             (!scCtx->fullScoreInfo) ? scCtx->sctx->spec->docs.size - 1 : 0);

  RS_SuggestionsFree(s);

  return true;
}

static bool SpellCheck_CheckDictExistence(SpellCheckCtx *scCtx, const char *dict) {
#define BUFF_SIZE 1000
  RedisModuleKey *k = NULL;
  Trie *t = SpellCheck_OpenDict(scCtx->sctx->redisCtx, dict, REDISMODULE_READ, &k);
  if (t == NULL) {
    char buff[BUFF_SIZE];
    snprintf(buff, BUFF_SIZE, "Dict does not exist: %s", dict);
    RedisModule_ReplyWithError(scCtx->sctx->redisCtx, buff);
    return false;
  }
  RedisModule_CloseKey(k);
  return true;
}

static bool SpellCheck_CheckTermDictsExistance(SpellCheckCtx *scCtx) {
  for (int i = 0; i < array_len(scCtx->includeDict); ++i) {
    if (!SpellCheck_CheckDictExistence(scCtx, scCtx->includeDict[i])) {
      return false;
    }
  }

  for (int i = 0; i < array_len(scCtx->excludeDict); ++i) {
    if (!SpellCheck_CheckDictExistence(scCtx, scCtx->excludeDict[i])) {
      return false;
    }
  }

  return true;
}

static int forEachCallback(QueryNode *n, QueryNode *orig, void *arg) {
  SpellCheckCtx *scCtx = arg;
  if (n->type == QN_TOKEN &&
      SpellCheck_ReplyTermSuggestions(scCtx, n->tn.str, n->tn.len, n->opts.fieldMask)) {
    scCtx->results++;
  }
  return 1;
}

void SpellCheck_Reply(SpellCheckCtx *scCtx, QueryAST *q) {
  if (!SpellCheck_CheckTermDictsExistance(scCtx)) {
    return;
  }

  RedisModule_ReplyWithArray(scCtx->sctx->redisCtx, REDISMODULE_POSTPONED_ARRAY_LEN);

  if (scCtx->fullScoreInfo) {
    // sending the total number of docs for the ability to calculate score on cluster
    RedisModule_ReplyWithLongLong(scCtx->sctx->redisCtx, scCtx->sctx->spec->docs.size - 1);
  }

  QueryNode_ForEach(q->root, forEachCallback, scCtx, 1);

  RedisModule_ReplySetArrayLength(scCtx->sctx->redisCtx,
                                  scCtx->results + (scCtx->fullScoreInfo ? 1 : 0));
}
