/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#include "spell_check.h"

#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>

#include "types_ffi.h"
#include "dictionary.h"
#include "reply.h"
#include "inverted_index.h"
#include "inverted_index_ffi.h"
#include "query_node.h"
#include "query_types.h"
#include "redis_index.h"
#include "redismodule.h"
#include "rmalloc.h"
#include "rqe_core.h"
#include "trie/levenshtein.h"
#include "trie/rune_util.h"
#include "trie/trie_node.h"
#include "util/arr/arr.h"

/** Forward declaration **/
static bool SpellCheck_IsTermExistsInTrie(Trie *t, const char *term, size_t len, double *outScore);


int SpellCheckCandidate_Compare(const void *val1, const void *val2) {
  const SpellCheckCandidate **a = (const SpellCheckCandidate **)val1;
  const SpellCheckCandidate **b = (const SpellCheckCandidate **)val2;
  if ((*a)->score > (*b)->score) {
    return -1;
  }
  if ((*a)->score < (*b)->score) {
    return 1;
  }
  return 0;
}

SpellCheckCandidate *SpellCheckCandidate_Create(char *candidate, size_t len, double score) {
  SpellCheckCandidate *res = rm_calloc(1, sizeof(SpellCheckCandidate));
  res->candidate = candidate;
  res->len = len;
  res->score = score;
  return res;
}

static void SpellCheckCandidate_Free(SpellCheckCandidate *candidate) {
  rm_free(candidate->candidate);
  rm_free(candidate);
}

SpellCheckCandidates *SpellCheckCandidates_Create() {
  SpellCheckCandidates *ret = rm_calloc(1, sizeof(SpellCheckCandidates));
  ret->candidatesTrie = NewTrie(NULL, Trie_Sort_Score);
  return ret;
}

void SpellCheckCandidates_Add(SpellCheckCandidates *s, char *term, size_t len, double score,
                              int incr) {
  double currScore;
  bool isExists = SpellCheck_IsTermExistsInTrie(s->candidatesTrie, term, len, &currScore);
  if (!incr) {
    if (!isExists) {
      // Payload is NULL so TRIE_ERR_PAYLOAD_OVERFLOW cannot occur.
      Trie_InsertStringBuffer(s->candidatesTrie, term, len, score, incr, NULL, 0);
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
  Trie_InsertStringBuffer(s->candidatesTrie, term, len, score, incr, NULL, 0);
}

void SpellCheckCandidates_Free(SpellCheckCandidates *s) {
  TrieType_Free(s->candidatesTrie);
  rm_free(s);
}

/**
 * Return the score for the given candidate (number between 0 to 1).
 * In case the candidate should not be added return -1.
 */
static double SpellCheck_GetScore(SpellCheckCtx *scCtx, char *candidate, size_t len,
                                  t_fieldMask fieldMask) {
  InvertedIndex *invidx = Redis_OpenInvertedIndex(scCtx->sctx->spec, candidate, len, 0, NULL);
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
    // we have at least one result, the candidate is relevant.
    retVal = InvertedIndex_NumDocs(invidx);
  } else {
    // fieldMask has filtered all docs, this candidate should not be returned
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

static void SpellCheck_FindCandidates(SpellCheckCtx *scCtx, Trie *t, const char *term, size_t len,
                                      t_fieldMask fieldMask, SpellCheckCandidates *s, int incr) {
  rune *rstr = NULL;
  t_len slen = 0;
  float score = 0;
  int dist = 0;
  size_t candidateLen;

  TrieIterator *it = Trie_IterateFuzzy(t, term, len, (int)scCtx->distance, TRIE_MATCH_EDIT_DISTANCE);
  // TrieIterator can be NULL when rune length exceed TRIE_MAX_PREFIX
  if (it == NULL) {
    return;
  }
  while (TrieIterator_Next(it, &rstr, &slen, NULL, &score, NULL, &dist)) {
    char *res = runesToStr(rstr, slen, &candidateLen);
    double score;
    if ((score = SpellCheck_GetScore(scCtx, res, candidateLen, fieldMask)) != -1) {
      SpellCheckCandidates_Add(s, res, candidateLen, score, incr);
    }
    rm_free(res);
  }
  TrieIterator_Free(it);
}

SpellCheckCandidate **SpellCheckCandidates_GetSorted(SpellCheckCandidates *s) {
  TrieIterator *iter = Trie_IterateAll(s->candidatesTrie);
  SpellCheckCandidate **ret = array_new(SpellCheckCandidate *, Trie_Size(s->candidatesTrie));
  rune *rstr = NULL;
  t_len slen = 0;
  float score = 0;
  size_t termLen;
  while (TrieIterator_Next(iter, &rstr, &slen, NULL, &score, NULL, NULL)) {
    char *res = runesToStr(rstr, slen, &termLen);
    array_append(ret, SpellCheckCandidate_Create(res, termLen, score));
  }
  TrieIterator_Free(iter);
  return ret;
}

void SpellCheck_SendReplyOnTerm(RedisModule_Reply *reply, char *term, size_t len,
                                SpellCheckCandidates *s, uint64_t totalDocNumber) {
  bool resp3 = RedisModule_IsRESP3(reply);

  if (totalDocNumber == 0) { // Can happen with FT.DICTADD
    totalDocNumber = 1;
  }

  SpellCheckCandidate **candidates = SpellCheckCandidates_GetSorted(s);
  qsort(candidates, array_len(candidates), sizeof(SpellCheckCandidate *),
        SpellCheckCandidate_Compare);

  if (resp3) // RESP3
  {
    // we assume we're in the terms' map

    RedisModule_Reply_StringBuffer(reply, term, len);

    RedisModule_Reply_Array(reply);

      int n = array_len(candidates);
      for (int i = 0; i < n; ++i) {
        RedisModule_Reply_Map(reply);
          RedisModule_Reply_StringBuffer(reply, candidates[i]->candidate, candidates[i]->len);
          RedisModule_Reply_Double(reply, candidates[i]->score / totalDocNumber);
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

        int n = array_len(candidates);
        for (int i = 0; i < n; ++i) {
          RedisModule_Reply_Array(reply);
            RedisModule_Reply_Double(reply, candidates[i]->score / totalDocNumber);
            RedisModule_Reply_StringBuffer(reply, candidates[i]->candidate, candidates[i]->len);
          RedisModule_Reply_ArrayEnd(reply);
        }

      RedisModule_Reply_ArrayEnd(reply);

    RedisModule_Reply_ArrayEnd(reply);
  }

  array_free_ex(candidates, SpellCheckCandidate_Free(*(SpellCheckCandidate **)ptr));
}

static bool SpellCheck_ReplyTermCandidates(SpellCheckCtx *scCtx, char *term, size_t len,
                                            t_fieldMask fieldMask) {
  RedisModule_Reply *reply = scCtx->reply;

  // searching the term on the term trie, if its there we just return false
  // because there is no need to return candidates for it.
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
  // because there is no need to return candidates for it.
  for (int i = 0; i < array_len(scCtx->excludeDict); ++i) {
    Trie *t = SpellCheck_OpenDict(scCtx->sctx->redisCtx, scCtx->excludeDict[i], REDISMODULE_READ);
    if (t == NULL) {
      continue;
    }
    if (SpellCheck_IsTermExistsInTrie(t, term, len, NULL)) {
      return false;
    }
  }

  SpellCheckCandidates *s = SpellCheckCandidates_Create();

  SpellCheck_FindCandidates(scCtx, scCtx->sctx->spec->terms, term, len, fieldMask, s, 1);

  // sorting results by score

  // searching the term on the include list for more candidates.
  for (int i = 0; i < array_len(scCtx->includeDict); ++i) {
    Trie *t = SpellCheck_OpenDict(scCtx->sctx->redisCtx, scCtx->includeDict[i], REDISMODULE_READ);
    if (t == NULL) {
      continue;
    }
    SpellCheck_FindCandidates(scCtx, t, term, len, fieldMask, s, 0);
  }

  SpellCheck_SendReplyOnTerm(reply, term, len, s,
                             (!scCtx->fullScoreInfo) ? scCtx->sctx->spec->docs.size - 1 : 0);

  SpellCheckCandidates_Free(s);

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
      SpellCheck_ReplyTermCandidates(scCtx, n->tn.str, n->tn.len, n->opts.fieldMask)) {
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
