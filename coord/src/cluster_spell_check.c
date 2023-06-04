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
                                  QueryError* qerr, bool resp3) {
  for (int i = 0; i < count; ++i) {
    if (MRReply_Type(replies[i]) == MR_REPLY_ERROR) {
      QueryError_SetError(qerr, QUERY_EGENERIC, MRReply_String(replies[i], NULL));
      return false;
    }

    int type = MRReply_Type(replies[i]);
    const char *expected = resp3 ? "map" : "array";
    if (resp3 && type != MR_REPLY_MAP || !resp3 && type != MR_REPLY_ARRAY) {
      QueryError_SetErrorFmt(qerr, QUERY_EGENERIC, "wrong reply type. Expected %s. Got %d", expected, type);
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
  int type = MRReply_Type(termSuggestionsReply);
  if (type == MR_REPLY_STRING || type == MR_REPLY_STATUS) { //@@
    const char* msg = MRReply_String(termSuggestionsReply, NULL);
    if (strcmp(msg, FOUND_TERM_IN_INDEX) == 0) {
      spellcheckReducerCtx_AddTermAsFoundInIndex(ctx, termValue);
      return true;
    }
    return true;
  }

  if (type != MR_REPLY_ARRAY) { // RESP2/3
    return false;
  }

  int i;
  for (i = 0; i < MRReply_Length(termSuggestionsReply); ++i) {
    MRReply* termSuggestionReply = MRReply_ArrayElement(termSuggestionsReply, i);
    if (MRReply_Type(termSuggestionReply) != MR_REPLY_ARRAY) { // RESP2/3 - @@ verify!
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

static bool spellCheckAnalizeResult_resp3(spellcheckReducerCtx* ctx, MRReply* termValueReply, MRReply* suggestionArray) {
  const char* termValue = MRReply_String(termValueReply, NULL);

  int type = MRReply_Type(suggestionArray);

  // TODO: check if the following section correct, not sure why it's needed
  if (type == MR_REPLY_STRING || type == MR_REPLY_STATUS) {
    const char* msg = MRReply_String(suggestionArray, NULL);
    if (strcmp(msg, FOUND_TERM_IN_INDEX) == 0) {
      spellcheckReducerCtx_AddTermAsFoundInIndex(ctx, termValue);
      return true;
    }
    return true;
  }

  if (MRReply_Type(suggestionArray) != MR_REPLY_ARRAY) {
    return false;
  }

  int i;
  for (i = 0; i < MRReply_Length(suggestionArray); ++i) {
    MRReply* termSuggestion = MRReply_ArrayElement(suggestionArray, i);
    if (MRReply_Type(termSuggestion) != MR_REPLY_MAP) {
      return false;
    }
    if (MRReply_Length(termSuggestion) != 2) {
      return false;
    }

    MRReply* suggestionReply = MRReply_ArrayElement(termSuggestion, 0);
    MRReply* scoreReply = MRReply_ArrayElement(termSuggestion, 1);
  
    if (MRReply_Type(scoreReply) != MR_REPLY_DOUBLE) {
      return false;
    }
    if (MRReply_Type(suggestionReply) != MR_REPLY_STRING) {
      return false;
    }

    double score = MRReply_Double(scoreReply);
    const char* suggestion = MRReply_String(suggestionReply, NULL);

    spellcheckReducerCtx_AddTermSuggestion(ctx, termValue, suggestion, score);
  }

  if (i == 0) {
    spellcheckReducerCtx_GetOrCreateTermSuggerstions(ctx, termValue);
  }

  return true;
}

void spellCheckSendResult(RedisModule_Reply* reply, spellcheckReducerCtx* spellCheckCtx,
                          uint64_t totalDocNum) {
  RedisModule_Reply_Map(reply);
  size_t numOfTerms = 0;
  for (int i = 0; i < array_len(spellCheckCtx->terms); ++i) {
    if (spellCheckCtx->terms[i]->foundInIndex) {
      continue;
    }
    ++numOfTerms;

    SpellCheck_SendReplyOnTerm(reply, spellCheckCtx->terms[i]->term,
                               strlen(spellCheckCtx->terms[i]->term),
                               spellCheckCtx->terms[i]->suggestions, totalDocNum);
  }
  RedisModule_Reply_MapEnd(reply);
}

int spellCheckReducer_resp2(struct MRCtx* mc, int count, MRReply** replies) {
  RedisModuleCtx* ctx = MRCtx_GetRedisCtx(mc);
  if (count == 0) {
    RedisModule_ReplyWithError(ctx, "Could not distribute command");
    return REDISMODULE_OK;
  }

  uint64_t totalDocNum = 0;
  QueryError qerr = {0};
  if (!spellCheckReplySanity(count, replies, &totalDocNum, &qerr, false)) {
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

  RedisModule_Reply _reply = RedisModule_NewReply(ctx), *reply = &_reply;
  spellCheckSendResult(reply, spellcheckCtx, totalDocNum);
  RedisModule_EndReply(reply);

  spellcheckReducerCtx_Free(spellcheckCtx);

  return REDISMODULE_OK;
}

int spellCheckReducer_resp3(struct MRCtx* mc, int count, MRReply** replies) {
  RedisModuleCtx* ctx = MRCtx_GetRedisCtx(mc);
  if (count == 0) {
    RedisModule_ReplyWithError(ctx, "Could not distribute command");
    return REDISMODULE_OK;
  }

  uint64_t totalDocNum = 0;
  QueryError qerr = {0};
  if (!spellCheckReplySanity(count, replies, &totalDocNum, &qerr, true)) {
    QueryError_ReplyAndClear(ctx, &qerr);
    return REDISMODULE_OK;
  }

  spellcheckReducerCtx* spellcheckCtx = spellcheckReducerCtx_Create();

  for (int i = 0; i < count; ++i) {
    int j = 0;
    MRReply* dictReply = replies[i];

    if (MRReply_Type(dictReply) != MR_REPLY_MAP) {
      spellcheckReducerCtx_Free(spellcheckCtx);
      RedisModule_ReplyWithError(ctx, "bad reply returned");
      return REDISMODULE_OK;
    }

    // ignore garbage field if exist
    if (MRReply_Type(MRReply_ArrayElement(dictReply, 0)) == MR_REPLY_INTEGER) {
      j+=2;
    }

    for (; j < MRReply_Length(dictReply); j += 2) {
      MRReply* termReply = MRReply_ArrayElement(dictReply, j);
      MRReply* suggestionArray = MRReply_ArrayElement(dictReply, j+1);
      if (MRReply_Type(termReply) != MR_REPLY_STRING || MRReply_Type(suggestionArray) != MR_REPLY_ARRAY) {
        spellcheckReducerCtx_Free(spellcheckCtx);
        RedisModule_ReplyWithError(ctx, "bad reply returned");
        return REDISMODULE_OK;
      }

      if (!spellCheckAnalizeResult_resp3(spellcheckCtx, termReply, suggestionArray)) {
        spellcheckReducerCtx_Free(spellcheckCtx);
        RedisModule_ReplyWithError(ctx, "could not analyze term result");
        return REDISMODULE_OK;
      }
    }
  }

  RedisModule_Reply _reply = RedisModule_NewReply(ctx), *reply = &_reply;
  spellCheckSendResult(reply, spellcheckCtx, totalDocNum);
  RedisModule_EndReply(reply);

  spellcheckReducerCtx_Free(spellcheckCtx);

  return REDISMODULE_OK;
}
