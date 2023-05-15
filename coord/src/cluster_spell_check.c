/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */


#include "cluster_spell_check.h"
#include "redismodule.h"
#include "spell_check.h"
#include "util/arr.h"
#include "query_error.h"

#include <stdbool.h>
#include <stddef.h>

typedef struct {
  char* term;
  RS_Suggestions* suggestions;
  bool foundInIndex;
} spellCheckReducerTerm;

typedef struct {
  spellCheckReducerTerm** terms;
} spellcheckReducerCtx;

static spellCheckReducerTerm* spellCheckReducerTerm_Create(const char* termStr) {
  spellCheckReducerTerm* ret = rm_malloc(sizeof(spellCheckReducerTerm));
  ret->term = rm_strdup(termStr);
  ret->suggestions = RS_SuggestionsCreate();
  ret->foundInIndex = false;
  return ret;
}

static void spellCheckReducerTerm_Free(spellCheckReducerTerm* t) {
  rm_free(t->term);
  RS_SuggestionsFree(t->suggestions);
  rm_free(t);
}

static void spellCheckReducerTerm_AddSuggestion(spellCheckReducerTerm* t,
                                                const char* suggestionsStr, double score) {
  RS_SuggestionsAdd(t->suggestions, (char*)suggestionsStr, strlen(suggestionsStr), score, 1);
}

static spellcheckReducerCtx* spellcheckReducerCtx_Create() {
#define TERMS_INITIAL_SIZE 5
  spellcheckReducerCtx* ret = rm_malloc(sizeof(spellcheckReducerCtx));
  ret->terms = array_new(spellCheckReducerTerm*, TERMS_INITIAL_SIZE);
  return ret;
}

static void spellcheckReducerCtx_Free(spellcheckReducerCtx* ctx) {
  array_free_ex(ctx->terms, spellCheckReducerTerm_Free(*(spellCheckReducerTerm**)ptr));
  rm_free(ctx);
}

static spellCheckReducerTerm* spellcheckReducerCtx_GetOrCreateTermSuggerstions(
    spellcheckReducerCtx* ctx, const char* termStr) {
  spellCheckReducerTerm* term = NULL;
  for (int i = 0; i < array_len(ctx->terms); ++i) {
    if (strcmp(ctx->terms[i]->term, termStr) == 0) {
      term = ctx->terms[i];
    }
  }

  if (term == NULL) {
    term = spellCheckReducerTerm_Create(termStr);
    ctx->terms = array_append(ctx->terms, term);
  }
  return term;
}

static void spellcheckReducerCtx_AddTermSuggestion(spellcheckReducerCtx* ctx, const char* termStr,
                                                   const char* suggestionStr, double score) {
  spellCheckReducerTerm* term = spellcheckReducerCtx_GetOrCreateTermSuggerstions(ctx, termStr);
  spellCheckReducerTerm_AddSuggestion(term, suggestionStr, score);
}

static void spellcheckReducerCtx_AddTermAsFoundInIndex(spellcheckReducerCtx* ctx,
                                                       const char* termStr) {
  spellCheckReducerTerm* term = spellcheckReducerCtx_GetOrCreateTermSuggerstions(ctx, termStr);
  term->foundInIndex = true;
}

static bool spellCheckReplySanity(int count, MRReply** replies, uint64_t* totalDocNum,
                                  QueryError* qerr) {
  for (int i = 0; i < count; ++i) {
    if (MRReply_Type(replies[i]) == MR_REPLY_ERROR) {
      QueryError_SetError(qerr, QUERY_EGENERIC, MRReply_String(replies[i], NULL));
      return false;
    }

    if (MRReply_Type(replies[i]) != MR_REPLY_ARRAY) {
      QueryError_SetErrorFmt(qerr, QUERY_EGENERIC, "wrong reply type. Expected array. Got %d",
                             MRReply_Type(replies[i]));
      return false;
    }

    MRReply* numOfDocReply = MRReply_ArrayElement(replies[i], 0);

    if (MRReply_Type(numOfDocReply) != MR_REPLY_INTEGER) {
      QueryError_SetErrorFmt(qerr, QUERY_EGENERIC, "Expected first reply as integer. Have %d",
                             MRReply_Type(numOfDocReply));
      return false;
    }

    (*totalDocNum) += MRReply_Integer(numOfDocReply);
  }

  return true;
}

static bool spellCheckAnalizeResult(spellcheckReducerCtx* ctx, MRReply* reply) {
  if (MRReply_Length(reply) != 3) {
    return false;
  }

  MRReply* termStrReply = MRReply_ArrayElement(reply, 0);
  const char* termStr = MRReply_String(termStrReply, NULL);
  if (strcmp(termStr, "TERM") != 0) {
    return false;
  }

  MRReply* termValueReply = MRReply_ArrayElement(reply, 1);
  const char* termValue = MRReply_String(termValueReply, NULL);

  MRReply* termSuggestionsReply = MRReply_ArrayElement(reply, 2);
  if (MRReply_Type(termSuggestionsReply) == MR_REPLY_STRING) {
    const char* msg = MRReply_String(termSuggestionsReply, NULL);
    if (strcmp(msg, FOUND_TERM_IN_INDEX) == 0) {
      spellcheckReducerCtx_AddTermAsFoundInIndex(ctx, termValue);
      return true;
    }
    return true;
  }

  if (MRReply_Type(termSuggestionsReply) != MR_REPLY_ARRAY) {
    return false;
  }

  int i;
  for (i = 0; i < MRReply_Length(termSuggestionsReply); ++i) {
    MRReply* termSuggestionReply = MRReply_ArrayElement(termSuggestionsReply, i);
    if (MRReply_Type(termSuggestionReply) != MR_REPLY_ARRAY) {
      return false;
    }
    if (MRReply_Length(termSuggestionReply) != 2) {
      return false;
    }

    MRReply* scoreReply = MRReply_ArrayElement(termSuggestionReply, 0);
    MRReply* suggestionReply = MRReply_ArrayElement(termSuggestionReply, 1);

    if (MRReply_Type(scoreReply) != MR_REPLY_STRING) {
      return false;
    }
    if (MRReply_Type(suggestionReply) != MR_REPLY_STRING) {
      return false;
    }

    double score;
    if (!MRReply_ToDouble(scoreReply, &score)) {
      return false;
    }

    const char* suggestionStr = MRReply_String(suggestionReply, NULL);

    spellcheckReducerCtx_AddTermSuggestion(ctx, termValue, suggestionStr, score);
  }

  if (i == 0) {
    spellcheckReducerCtx_GetOrCreateTermSuggerstions(ctx, termValue);
  }

  return true;
}

void spellCheckSendResult(RedisModuleCtx* ctx, spellcheckReducerCtx* spellCheckCtx,
                          uint64_t totalDocNum) {

  RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
  size_t numOfTerms = 0;
  for (int i = 0; i < array_len(spellCheckCtx->terms); ++i) {
    if (spellCheckCtx->terms[i]->foundInIndex) {
      continue;
    }
    ++numOfTerms;

    SpellCheck_SendReplyOnTerm(ctx, spellCheckCtx->terms[i]->term,
                               strlen(spellCheckCtx->terms[i]->term),
                               spellCheckCtx->terms[i]->suggestions, totalDocNum);
  }
  RedisModule_ReplySetArrayLength(ctx, numOfTerms);
}

int spellCheckReducer(struct MRCtx* mc, int count, MRReply** replies) {
  RedisModuleCtx* ctx = MRCtx_GetRedisCtx(mc);
  if (count == 0) {
    RedisModule_ReplyWithError(ctx, "Could not distribute command");
    return REDISMODULE_OK;
  }

  uint64_t totalDocNum = 0;
  QueryError qerr = {0};
  if (!spellCheckReplySanity(count, replies, &totalDocNum, &qerr)) {
    QueryError_ReplyAndClear(ctx, &qerr);
    return REDISMODULE_OK;
  }

  spellcheckReducerCtx* spellcheckCtx = spellcheckReducerCtx_Create();

  for (int i = 0; i < count; ++i) {
    for (int j = 1; j < MRReply_Length(replies[i]); ++j) {
      MRReply* termReply = MRReply_ArrayElement(replies[i], j);
      if (MRReply_Type(termReply) != MR_REPLY_ARRAY) {
        spellcheckReducerCtx_Free(spellcheckCtx);
        RedisModule_ReplyWithError(ctx, "bad reply returned");
        return REDISMODULE_OK;
      }
      if (!spellCheckAnalizeResult(spellcheckCtx, termReply)) {
        spellcheckReducerCtx_Free(spellcheckCtx);
        RedisModule_ReplyWithError(ctx, "could not analyze term result");
        return REDISMODULE_OK;
      }
    }
  }

  spellCheckSendResult(ctx, spellcheckCtx, totalDocNum);

  spellcheckReducerCtx_Free(spellcheckCtx);

  return REDISMODULE_OK;
}
