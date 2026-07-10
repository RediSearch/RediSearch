/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#include "spell_check.h"
#include "types_ffi.h"
#include "util/arr.h"
#include "dictionary.h"
#include "trie/trie.h"
#include "term_dictionary_ffi.h"
#include "reply.h"
#include "inverted_index.h"
#include "inverted_index_ffi.h"
#include <stdbool.h>

/** Forward declaration **/
static bool SpellCheck_IsTermExistsInTrie(Trie *t, const char *term, size_t len, double *outScore);
static bool SpellCheck_IsTermExistsInTermDict(TermDictionary *t, const char *term, size_t len,
                                              double *outScore);

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
  if (!incr) {
    if (!isExists) {
      // Payload is NULL so TRIE_ERR_PAYLOAD_OVERFLOW cannot occur.
      Trie_InsertStringBuffer(s->suggestionsTrie, term, len, score, incr, NULL, 0);
    }
    return;
  }

  if (isExists && score == 0) {
    return;
  }

  if (!isExists || currScore == 0) {
    incr = 0;
  }

  // Payload is NULL so TRIE_ERR_PAYLOAD_OVERFLOW cannot occur.
  Trie_InsertStringBuffer(s->suggestionsTrie, term, len, score, incr, NULL, 0);
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
  InvertedIndex *invidx = Redis_OpenInvertedIndex(scCtx->sctx->spec, suggestion, len, 0, NULL);
  double retVal = 0;
  IndexDecoderCtx ctx = {.fieldmask_tag = IndexDecoderCtx_FieldMask, .fieldmask = fieldMask};
  IndexReader *reader = NULL;
  RSIndexResult *res = NULL;

  if (!invidx) {
    // can not find inverted index key, score is 0.
    goto end;
  }
  reader = NewIndexReader(invidx, ctx);
  res = NewTokenRecord(NULL, 1);
  if (IndexReader_Next(reader, res)) {
    // we have at least one result, the suggestion is relevant.
    retVal = InvertedIndex_NumDocs(invidx);
  } else {
    // fieldMask has filtered all docs, this suggestions should not be returned
    retVal = -1;
  }
  IndexReader_Free(reader);
  IndexResult_Free(res);

end:
  return retVal;
}

static bool SpellCheck_IsTermExistsInTrie(Trie *t, const char *term, size_t len, double *outScore) {
  rune *rstr = NULL;
  t_len slen = 0;
  float score = 0;
  int dist = 0;
  bool retVal = false;
  TrieIterator *it = Trie_IterateFuzzy(t, term, len, 0, TRIE_MATCH_EDIT_DISTANCE);
  // TrieIterator can be NULL when rune length exceed TRIE_MAX_PREFIX
  if (it == NULL) {
    return retVal;
  }
  if (TrieIterator_Next(it, &rstr, &slen, NULL, &score, NULL, &dist)) {
    retVal = true;
  }
  TrieIterator_Free(it);
  if (outScore) {
    *outScore = score;
  }
  return retVal;
}

// Term-dictionary counterpart of SpellCheck_IsTermExistsInTrie: a case-folded
// exact existence check against the index term dictionary.
static bool SpellCheck_IsTermExistsInTermDict(TermDictionary *t, const char *term, size_t len,
                                              double *outScore) {
  float score = 0;
  bool retVal = TermDictionary_Get(t, term, len, &score, NULL);
  if (outScore) {
    *outScore = score;
  }
  return retVal;
}

static void SpellCheck_FindSuggestions(SpellCheckCtx *scCtx, TermDictionary *t, const char *term,
                                       size_t len, t_fieldMask fieldMask, RS_Suggestions *s,
                                       int incr) {
  const char *suggestion = NULL;
  size_t suggestionLen = 0;

  TermDictionaryIterator *it = TermDictionary_IterateFuzzy(t, term, len, (uint32_t)scCtx->distance);
  if (it == NULL) {
    return;
  }
  while (TermDictionaryIterator_Next(it, &suggestion, &suggestionLen, NULL, NULL)) {
    // The suggestion buffer is borrowed from the iterator and only valid until
    // the next step; RS_SuggestionsAdd copies it, so this stays sound.
    double score;
    if ((score = SpellCheck_GetScore(scCtx, (char *)suggestion, suggestionLen, fieldMask)) != -1) {
      RS_SuggestionsAdd(s, (char *)suggestion, suggestionLen, score, incr);
    }
  }
  TermDictionaryIterator_Free(it);
}

// Same as SpellCheck_FindSuggestions, but sourcing candidates from a
// user-managed spell-check dictionary (FT.DICTADD) instead of the index term
// trie. Fuzzy matching is delegated to the Rust SpellCheckDictionary.
static void SpellCheck_FindSuggestionsInDict(SpellCheckCtx *scCtx, SpellCheckDictionary *dict,
                                             const char *term, size_t len, t_fieldMask fieldMask,
                                             RS_Suggestions *s, int incr) {
  const char *suggestion = NULL;
  size_t suggestionLen = 0;

  SpellCheckDictionaryIterator *it =
      SpellCheckDictionary_IterateFuzzy(dict, term, len, (uint32_t)scCtx->distance);
  while (SpellCheckDictionaryIterator_Next(it, &suggestion, &suggestionLen)) {
    // The suggestion buffer is borrowed from the iterator and only valid until
    // the next step; RS_SuggestionsAdd copies it, so this stays sound.
    double score;
    if ((score = SpellCheck_GetScore(scCtx, (char *)suggestion, suggestionLen, fieldMask)) != -1) {
      RS_SuggestionsAdd(s, (char *)suggestion, suggestionLen, score, incr);
    }
  }
  SpellCheckDictionaryIterator_Free(it);
}

// Dict flavor of SpellCheck_FindSuggestions: same accumulation contract, but
// the suggestions come from a SpellCheckDictionary fuzzy cursor. The cursor
// lends each term only until the next advance; both consumers below
// (SpellCheck_GetScore, RS_SuggestionsAdd) copy what they keep.
static void SpellCheck_FindSuggestionsFromDict(SpellCheckCtx *scCtx, SpellCheckDictionary *d,
                                               const char *term, size_t len, t_fieldMask fieldMask,
                                               RS_Suggestions *s, int incr) {
  SpellCheckDictionaryIterator *it =
      SpellCheckDictionary_IterateFuzzy(d, term, len, (uint32_t)scCtx->distance);
  const char *suggestion;
  size_t suggestionLen;
  while (SpellCheckDictionaryIterator_Next(it, &suggestion, &suggestionLen)) {
    double score;
    if ((score = SpellCheck_GetScore(scCtx, (char *)suggestion, suggestionLen, fieldMask)) != -1) {
      RS_SuggestionsAdd(s, (char *)suggestion, suggestionLen, score, incr);
    }
  }
  SpellCheckDictionaryIterator_Free(it);
}

RS_Suggestion **spellCheck_GetSuggestions(RS_Suggestions *s) {
  TrieIterator *iter = Trie_IterateAll(s->suggestionsTrie);
  RS_Suggestion **ret = array_new(RS_Suggestion *, Trie_Size(s->suggestionsTrie));
  rune *rstr = NULL;
  t_len slen = 0;
  float score = 0;
  size_t termLen;
  while (TrieIterator_Next(iter, &rstr, &slen, NULL, &score, NULL, NULL)) {
    char *res = runesToStr(rstr, slen, &termLen);
    array_append(ret, RS_SuggestionCreate(res, termLen, score));
  }
  TrieIterator_Free(iter);
  return ret;
}

void SpellCheck_SendReplyOnTerm(RedisModule_Reply *reply, char *term, size_t len, RS_Suggestions *s,
                                uint64_t totalDocNumber) {
  bool resp3 = RedisModule_IsRESP3(reply);

  if (totalDocNumber == 0) {  // Can happen with FT.DICTADD
    totalDocNumber = 1;
  }

  RS_Suggestion **suggestions = spellCheck_GetSuggestions(s);
  qsort(suggestions, array_len(suggestions), sizeof(RS_Suggestion *), RS_SuggestionCompare);

  if (resp3)  // RESP3
  {
    // we assume we're in the terms' map

    RedisModule_Reply_StringBuffer(reply, term, len);

    RedisModule_Reply_Array(reply);

    int n = array_len(suggestions);
    for (int i = 0; i < n; ++i) {
      RedisModule_Reply_Map(reply);
      RedisModule_Reply_StringBuffer(reply, suggestions[i]->suggestion, suggestions[i]->len);
      RedisModule_Reply_Double(reply, suggestions[i]->score / totalDocNumber);
      RedisModule_Reply_MapEnd(reply);
    }

    RedisModule_Reply_ArrayEnd(reply);
  } else  // RESP2
  {
    RedisModule_Reply_Array(reply);
    RedisModule_Reply_SimpleString(reply, SPELL_CHECK_TERM_CONST);

    RedisModule_Reply_StringBuffer(reply, term, len);

    RedisModule_Reply_Array(reply);

    int n = array_len(suggestions);
    for (int i = 0; i < n; ++i) {
      RedisModule_Reply_Array(reply);
      RedisModule_Reply_Double(reply, suggestions[i]->score / totalDocNumber);
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
  if (SpellCheck_IsTermExistsInTermDict(scCtx->sctx->spec->terms, term, len, NULL)) {
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

  // The Rust dictionaries only ever hold valid UTF-8, so an invalid term can
  // neither be excluded by one nor gain suggestions from one — and it must
  // not reach the SpellCheckDictionary_* calls (see Dictionary_IsValidTerm).
  bool termIsValidUtf8 = Dictionary_IsValidTerm(term, len);

  // searching the term on the exclude list, if its there we just return false
  // because there is no need to return suggestions on it.
  for (int i = 0; termIsValidUtf8 && i < array_len(scCtx->excludeDict); ++i) {
    SpellCheckDictionary *d =
        SpellCheck_OpenDict(scCtx->sctx->redisCtx, scCtx->excludeDict[i], REDISMODULE_READ);
    if (d == NULL) {
      continue;
    }
    if (SpellCheckDictionary_Contains(d, term, len)) {
      return false;
    }
  }

  RS_Suggestions *s = RS_SuggestionsCreate();

  SpellCheck_FindSuggestions(scCtx, scCtx->sctx->spec->terms, term, len, fieldMask, s, 1);

  // sorting results by score

  // searching the term on the include list for more suggestions.
  for (int i = 0; termIsValidUtf8 && i < array_len(scCtx->includeDict); ++i) {
    SpellCheckDictionary *d =
        SpellCheck_OpenDict(scCtx->sctx->redisCtx, scCtx->includeDict[i], REDISMODULE_READ);
    if (d == NULL) {
      continue;
    }
    SpellCheck_FindSuggestionsFromDict(scCtx, d, term, len, fieldMask, s, 0);
  }

  SpellCheck_SendReplyOnTerm(reply, term, len, s,
                             (!scCtx->fullScoreInfo) ? scCtx->sctx->spec->docs.size - 1 : 0);

  RS_SuggestionsFree(s);

  return true;
}

static bool SpellCheck_CheckDictExistence(SpellCheckCtx *scCtx, const char *dict) {
#define BUFF_SIZE 1000
  SpellCheckDictionary *d = SpellCheck_OpenDict(scCtx->sctx->redisCtx, dict, REDISMODULE_READ);
  if (d == NULL) {
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

  scCtx->reply = reply;  // this is stack-allocated, should be reset immediately after use
  QueryNode_ForEach(q->root, forEachCallback, scCtx, 1);
  scCtx->reply = NULL;

  RedisModule_Reply_ArrayEnd(reply);
}

static void SpellCheck_Reply_resp3(SpellCheckCtx *scCtx, QueryAST *q, RedisModule_Reply *reply) {
  RedisModule_Reply_Map(reply);  // root

  if (scCtx->fullScoreInfo) {
    // sending the total number of docs for the ability to calculate score on cluster
    RedisModule_ReplyKV_LongLong(reply, "total_docs", scCtx->sctx->spec->docs.size - 1);
  }

  RedisModule_ReplyKV_Map(reply, "results");  // >results
  scCtx->reply = reply;  // this is stack-allocated, should be reset immediately after use
  QueryNode_ForEach(q->root, forEachCallback, scCtx, 1);
  scCtx->reply = NULL;
  RedisModule_Reply_MapEnd(reply);  // >results

  RedisModule_Reply_MapEnd(reply);  // root
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
