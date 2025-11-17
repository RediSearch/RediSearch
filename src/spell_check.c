/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#include "spell_check.h"
#include "util/arr.h"
#include "dictionary.h"
#include "reply.h"
#include "iterators/inverted_index_iterator.h"
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
  ret->suggestionsTrie = NewTrie(NULL, Trie_Sort_Score);
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
  InvertedIndex *invidx = Redis_OpenInvertedIndex(scCtx->sctx, suggestion, len, 0, NULL);
  double retVal = 0;
  if (!invidx) {
    // can not find inverted index key, score is 0.
    goto end;
  }
  FieldMaskOrIndex fieldMaskOrIndex = {.mask_tag = FieldMaskOrIndex_Mask, .mask = fieldMask};
  QueryIterator *iter = NewInvIndIterator_TermQuery(invidx, scCtx->sctx, fieldMaskOrIndex, NULL, 1);
  if (iter->Read(iter) == ITERATOR_OK) {
    // we have at least one result, the suggestion is relevant.
    retVal = InvertedIndex_NumDocs(invidx);
  } else {
    // fieldMask has filtered all docs, this suggestions should not be returned
    retVal = -1;
  }
  iter->Free(iter);

end:
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
    array_append(ret, RS_SuggestionCreate(res, termLen, score));
  }
  TrieIterator_Free(iter);
  return ret;
}

void SpellCheck_SendReplyOnTerm(RedisModule_Reply *reply, char *term, size_t len, RS_Suggestions *s,
                                uint64_t totalDocNumber) {
  bool resp3 = RedisModule_IsRESP3(reply);

  if (totalDocNumber == 0) { // Can happen with FT.DICTADD
    totalDocNumber = 1;
  }

  RS_Suggestion **suggestions = spellCheck_GetSuggestions(s);
  qsort(suggestions, array_len(suggestions), sizeof(RS_Suggestion *), RS_SuggestionCompare);

  if (resp3) // RESP3
  {
    // we assume we're in the terms' map

    RedisModule_Reply_StringBuffer(reply, term, len);

    RedisModule_Reply_Array(reply);

      int n = array_len(suggestions);
      for (int i = 0; i < n; ++i) {
        RedisModule_Reply_Map(reply);
          RedisModule_Reply_StringBuffer(reply, suggestions[i]->suggestion, suggestions[i]->len);
          RedisModule_Reply_Double(reply, suggestions[i]->score == -1 ? 0 :
                                        suggestions[i]->score / totalDocNumber);
        RedisModule_Reply_MapEnd(reply);
      }

    RedisModule_Reply_ArrayEnd(reply);
  }
  else // RESP2
  {
    RedisModule_Reply_Array(reply);
    RedisModule_Reply_SimpleString(reply, SPELL_CHECK_TERM_CONST);

    RedisModule_Reply_StringBuffer(reply, term, len);

      RedisModule_Reply_Array(reply);

        int n = array_len(suggestions);
        for (int i = 0; i < n; ++i) {
          RedisModule_Reply_Array(reply);
            RedisModule_Reply_Double(reply, suggestions[i]->score == -1 ? 0 :
                                            suggestions[i]->score / totalDocNumber);
            RedisModule_Reply_StringBuffer(reply, suggestions[i]->suggestion, suggestions[i]->len);
          RedisModule_Reply_ArrayEnd(reply);
        }

      RedisModule_Reply_ArrayEnd(reply);

    RedisModule_Reply_ArrayEnd(reply);
  }

  array_free_ex(suggestions, RS_SuggestionFree(*(RS_Suggestion **)ptr));
}

static bool SpellCheck_ReplyTermSuggestions(SpellCheckCtx *scCtx, char *term, size_t len,
                                            t_fieldMask fieldMask) {
  RedisModule_Reply *reply = scCtx->reply;

  // searching the term on the term trie, if its there we just return false
  // because there is no need to return suggestions on it.
  if (SpellCheck_IsTermExistsInTrie(scCtx->sctx->spec->terms, term, len, NULL)) {
    if (!scCtx->fullScoreInfo) {
      return false;
    }

    // if a full score info is requested we need to send information that
    // we found the term as is on the index

    if (reply->resp3) {
      RedisModule_Reply_StringBuffer(reply, term, len);
      RedisModule_Reply_Error(reply, SPELL_CHECK_FOUND_TERM_IN_INDEX);
    } else {
      RedisModule_Reply_Array(reply);
        RedisModule_Reply_SimpleString(reply, SPELL_CHECK_TERM_CONST);
        RedisModule_Reply_StringBuffer(reply, term, len);
        RedisModule_Reply_SimpleString(reply, SPELL_CHECK_FOUND_TERM_IN_INDEX);
      RedisModule_Reply_ArrayEnd(reply);
    }
    return true;
  }

  // searching the term on the exclude list, if its there we just return false
  // because there is no need to return suggestions on it.
  for (int i = 0; i < array_len(scCtx->excludeDict); ++i) {
    Trie *t = SpellCheck_OpenDict(scCtx->sctx->redisCtx, scCtx->excludeDict[i], REDISMODULE_READ);
    if (t == NULL) {
      continue;
    }
    if (SpellCheck_IsTermExistsInTrie(t, term, len, NULL)) {
      return false;
    }
  }

  RS_Suggestions *s = RS_SuggestionsCreate();

  SpellCheck_FindSuggestions(scCtx, scCtx->sctx->spec->terms, term, len, fieldMask, s, 1);

  // sorting results by score

  // searching the term on the include list for more suggestions.
  for (int i = 0; i < array_len(scCtx->includeDict); ++i) {
    Trie *t = SpellCheck_OpenDict(scCtx->sctx->redisCtx, scCtx->includeDict[i], REDISMODULE_READ);
    if (t == NULL) {
      continue;
    }
    SpellCheck_FindSuggestions(scCtx, t, term, len, fieldMask, s, 0);
  }

  SpellCheck_SendReplyOnTerm(reply, term, len, s,
                             (!scCtx->fullScoreInfo) ? scCtx->sctx->spec->docs.size - 1 : 0);

  RS_SuggestionsFree(s);

  return true;
}

static bool SpellCheck_CheckDictExistence(SpellCheckCtx *scCtx, const char *dict) {
#define BUFF_SIZE 1000
  Trie *t = SpellCheck_OpenDict(scCtx->sctx->redisCtx, dict, REDISMODULE_READ);
  if (t == NULL) {
    char buff[BUFF_SIZE];
    snprintf(buff, BUFF_SIZE, "Dict does not exist: %s", dict);
    RedisModule_ReplyWithError(scCtx->sctx->redisCtx, buff);
    return false;
  }
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

static void SpellCheck_Reply_resp2(SpellCheckCtx *scCtx, QueryAST *q, RedisModule_Reply *reply) {
  RedisModule_Reply_Array(reply);

    if (scCtx->fullScoreInfo) {
      // sending the total number of docs for the ability to calculate score on cluster
      RedisModule_Reply_LongLong(reply, scCtx->sctx->spec->docs.size - 1);
    }

    scCtx->reply = reply; // this is stack-allocated, should be reset immediately after use
    QueryNode_ForEach(q->root, forEachCallback, scCtx, 1);
    scCtx->reply = NULL;

  RedisModule_Reply_ArrayEnd(reply);
}

static void SpellCheck_Reply_resp3(SpellCheckCtx *scCtx, QueryAST *q, RedisModule_Reply *reply) {
  RedisModule_Reply_Map(reply); // root

    if (scCtx->fullScoreInfo) {
      // sending the total number of docs for the ability to calculate score on cluster
      RedisModule_ReplyKV_LongLong(reply, "total_docs", scCtx->sctx->spec->docs.size - 1);
    }

    RedisModule_ReplyKV_Map(reply, "results"); // >results
      scCtx->reply = reply; // this is stack-allocated, should be reset immediately after use
      QueryNode_ForEach(q->root, forEachCallback, scCtx, 1);
      scCtx->reply = NULL;
    RedisModule_Reply_MapEnd(reply); // >results

  RedisModule_Reply_MapEnd(reply); // root
}

void SpellCheck_Reply(SpellCheckCtx *scCtx, QueryAST *q) {
  if (!SpellCheck_CheckTermDictsExistance(scCtx)) {
    return;
  }

  RedisModule_Reply _reply = RedisModule_NewReply(scCtx->sctx->redisCtx), *reply = &_reply;
  if (reply->resp3) {
    SpellCheck_Reply_resp3(scCtx, q, reply);
  } else {
    SpellCheck_Reply_resp2(scCtx, q, reply);
  }

  RedisModule_EndReply(reply);
}
