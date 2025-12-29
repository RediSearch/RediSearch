/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
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

static spellCheckReducerTerm* spellCheckReducerTerm_Create(const char* term) {
  spellCheckReducerTerm* ret = rm_malloc(sizeof(spellCheckReducerTerm));
  ret->term = rm_strdup(term);
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

static spellCheckReducerTerm *spellcheckReducerCtx_GetOrCreateTermSuggestions(
    spellcheckReducerCtx *ctx, const char *term) {
  spellCheckReducerTerm *reducer_term = NULL;
  for (int i = 0; i < array_len(ctx->terms); ++i) {
    if (strcmp(ctx->terms[i]->term, term) == 0) {
      reducer_term = ctx->terms[i];
    }
  }

  if (reducer_term == NULL) {
    reducer_term = spellCheckReducerTerm_Create(term);
    array_append(ctx->terms, reducer_term);
  }
  return reducer_term;
}

static void spellcheckReducerCtx_AddTermSuggestion(spellcheckReducerCtx* ctx, const char* term,
                                                   const char* suggestion, double score) {
  spellCheckReducerTerm *reducer_term = spellcheckReducerCtx_GetOrCreateTermSuggestions(ctx, term);
  spellCheckReducerTerm_AddSuggestion(reducer_term, suggestion, score);
}

static void spellcheckReducerCtx_AddTermAsFoundInIndex(spellcheckReducerCtx* ctx,
                                                       const char* term) {
  spellCheckReducerTerm* reducer_term = spellcheckReducerCtx_GetOrCreateTermSuggestions(ctx, term);
  reducer_term->foundInIndex = true;
}

static bool spellCheckReplySanity_resp2(MRReply *reply, uint64_t *totalDocNum, QueryError *qerr) {
  int type = MRReply_Type(reply);

  if (type == MR_REPLY_ERROR) {
    QueryError_SetError(qerr, QUERY_ERROR_CODE_GENERIC, MRReply_String(reply, NULL));
    return false;
  }

  if (type != MR_REPLY_ARRAY) {
    QueryError_SetWithoutUserDataFmt(qerr, QUERY_ERROR_CODE_GENERIC, "wrong reply type. Expected array. Got %d",
                               MRReply_Type(reply));
    return false;
  }

  MRReply *ndocs = MRReply_ArrayElement(reply, 0);

  if (MRReply_Type(ndocs) != MR_REPLY_INTEGER) {
    QueryError_SetWithoutUserDataFmt(qerr, QUERY_ERROR_CODE_GENERIC, "Expected first reply as integer. Have %d",
                               MRReply_Type(ndocs));
    return false;
  }

  *totalDocNum += MRReply_Integer(ndocs);
  return true;
}

static bool spellCheckReplySanity_resp3(MRReply *reply, uint64_t *totalDocNum, QueryError *qerr) {
  int type = MRReply_Type(reply);

  if (type == MR_REPLY_ERROR) {
    QueryError_SetError(qerr, QUERY_ERROR_CODE_GENERIC, MRReply_String(reply, NULL));
    return false;
  }

  if (type != MR_REPLY_MAP) {
    QueryError_SetWithoutUserDataFmt(qerr, QUERY_ERROR_CODE_GENERIC, "wrong reply type. Expected map. Got %d",
                               MRReply_Type(reply));
    return false;
  }

  MRReply *ndocs = MRReply_MapElement(reply, "total_docs");

  if (MRReply_Type(ndocs) != MR_REPLY_INTEGER) {
    QueryError_SetWithoutUserDataFmt(qerr, QUERY_ERROR_CODE_GENERIC, "Expected total_docs as integer. Have %d",
                               MRReply_Type(ndocs));
    return false;
  }

  *totalDocNum += MRReply_Integer(ndocs);

  return true;
}

static bool spellCheckAnalyzeResult_resp2(spellcheckReducerCtx *ctx, MRReply *reply) {
  if (MRReply_Length(reply) != 3) {
    return false;
  }

  MRReply* termConstReply = MRReply_ArrayElement(reply, 0);
  const char* termConst = MRReply_String(termConstReply, NULL);
  if (strcmp(termConst, SPELL_CHECK_TERM_CONST) != 0) {
    return false;
  }

  MRReply *termReply = MRReply_ArrayElement(reply, 1);
  const char *term = MRReply_String(termReply, NULL);

  MRReply *termSuggestionsReply = MRReply_ArrayElement(reply, 2);
  int type = MRReply_Type(termSuggestionsReply);
  if (type == MR_REPLY_STRING || type == MR_REPLY_STATUS) { //@@
    const char* msg = MRReply_String(termSuggestionsReply, NULL);
    if (strcmp(msg, SPELL_CHECK_FOUND_TERM_IN_INDEX) == 0) {
      spellcheckReducerCtx_AddTermAsFoundInIndex(ctx, term);
      return true;
    }
    return true;
  }

  if (type != MR_REPLY_ARRAY) {
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

    const char* suggestion = MRReply_String(suggestionReply, NULL);

    spellcheckReducerCtx_AddTermSuggestion(ctx, term, suggestion, score);
  }

  if (i == 0) {
    spellcheckReducerCtx_GetOrCreateTermSuggestions(ctx, term);
  }

  return true;
}

static bool spellCheckAnalyzeResult_resp3(spellcheckReducerCtx *ctx, MRReply *termReply, MRReply *suggestions) {
  const char* term = MRReply_String(termReply, NULL);

  int type = MRReply_Type(suggestions);

  if (type == MR_REPLY_ERROR) {
    const char*msg = MRReply_String(suggestions, NULL);
    if (strcmp(msg, SPELL_CHECK_FOUND_TERM_IN_INDEX) == 0) {
      spellcheckReducerCtx_AddTermAsFoundInIndex(ctx, term);
      return true;
    }
    return false;
  }

  if (MRReply_Type(suggestions) != MR_REPLY_ARRAY) {
    return false;
  }

  int i;
  for (i = 0; i < MRReply_Length(suggestions); ++i) {
    MRReply *termSuggestion = MRReply_ArrayElement(suggestions, i);
    if (MRReply_Type(termSuggestion) != MR_REPLY_MAP && MRReply_Length(termSuggestion) != 2) {
      return false;
    }

    MRReply *suggestionReply = MRReply_ArrayElement(termSuggestion, 0);
    MRReply *scoreReply = MRReply_ArrayElement(termSuggestion, 1);

    if (MRReply_Type(scoreReply) != MR_REPLY_DOUBLE) {
      return false;
    }
    if (MRReply_Type(suggestionReply) != MR_REPLY_STRING) {
      return false;
    }

    double score = MRReply_Double(scoreReply);
    const char* suggestion = MRReply_String(suggestionReply, NULL);

    spellcheckReducerCtx_AddTermSuggestion(ctx, term, suggestion, score);
  }

  if (i == 0) {
    spellcheckReducerCtx_GetOrCreateTermSuggestions(ctx, term);
  }

  return true;
}

void spellCheckSendResult(RedisModule_Reply *reply, spellcheckReducerCtx* spellCheckCtx,
                          uint64_t totalDocNum) {
  if (reply->resp3) {
    RedisModule_Reply_Map(reply); // terms' map
  }
  for (int i = 0; i < array_len(spellCheckCtx->terms); ++i) {
    if (spellCheckCtx->terms[i]->foundInIndex) {
      continue;
    }

    SpellCheck_SendReplyOnTerm(reply, spellCheckCtx->terms[i]->term,
                               strlen(spellCheckCtx->terms[i]->term),
                               spellCheckCtx->terms[i]->suggestions, totalDocNum);
  }
  if (reply->resp3) {
    RedisModule_Reply_MapEnd(reply);  // terms' map
  }
}

int spellCheckReducer_resp2(struct MRCtx* mc, int count, MRReply** replies) {
  RedisModuleCtx* ctx = MRCtx_GetRedisCtx(mc);
  if (count == 0) {
    RedisModule_ReplyWithError(ctx, "Could not distribute command");
    return REDISMODULE_OK;
  }

  uint64_t totalDocNum = 0;
  QueryError qerr = QueryError_Default();
  for (int i = 0; i < count; ++i) {
    if (!spellCheckReplySanity_resp2(replies[i], &totalDocNum, &qerr)) {
      QueryError_ReplyAndClear(ctx, &qerr);
      return REDISMODULE_OK;
    }
  }

  const char *error = NULL;
  spellcheckReducerCtx *spellcheckCtx = spellcheckReducerCtx_Create();

  for (int i = 0; i < count; ++i) {
    for (int j = 1; j < MRReply_Length(replies[i]); ++j) {
      MRReply* term = MRReply_ArrayElement(replies[i], j);
      if (MRReply_Type(term) != MR_REPLY_ARRAY) {
        error = "bad reply returned";
        goto finish;
      }

      if (!spellCheckAnalyzeResult_resp2(spellcheckCtx, term)) {
        error = "could not analyze term result";
        goto finish;
      }
    }
  }

  RedisModule_Reply _reply = RedisModule_NewReply(ctx), *reply = &_reply;
  RedisModule_Reply_Array(reply);
    spellCheckSendResult(reply, spellcheckCtx, totalDocNum);
  RedisModule_Reply_ArrayEnd(reply);
  RedisModule_EndReply(reply);

finish:
  spellcheckReducerCtx_Free(spellcheckCtx);
  if (error) {
    RedisModule_ReplyWithError(ctx, error);
  }

  return REDISMODULE_OK;
}

int spellCheckReducer_resp3(struct MRCtx* mc, int count, MRReply** replies) {
  RedisModuleCtx* ctx = MRCtx_GetRedisCtx(mc);
  if (count == 0) {
    RedisModule_ReplyWithError(ctx, "Could not distribute command");
    return REDISMODULE_OK;
  }

  uint64_t totalDocNum = 0;
  QueryError qerr = QueryError_Default();
  for (int i = 0; i < count; ++i) {
    if (!spellCheckReplySanity_resp3(replies[i], &totalDocNum, &qerr)) {
      QueryError_ReplyAndClear(ctx, &qerr);
      return REDISMODULE_OK;
    }
  }

  const char *error = NULL;
  spellcheckReducerCtx *spellcheckCtx = spellcheckReducerCtx_Create();

  for (int i = 0; i < count; ++i) {
    int j = 0;
    MRReply *dictReply = replies[i];

    if (MRReply_Type(dictReply) != MR_REPLY_MAP) {
      error = "bad reply returned";
      goto finish;
    }

    MRReply *termMap = MRReply_MapElement(dictReply, "results");
    if (!termMap || MRReply_Type(termMap) != MR_REPLY_MAP) {
      error = "bad reply returned";
      goto finish;
    }

    for (int j = 0; j < MRReply_Length(termMap); j += 2) {
      MRReply *term = MRReply_ArrayElement(termMap, j);
      MRReply *suggestions = MRReply_ArrayElement(termMap, j + 1);
      int sug_type = MRReply_Type(suggestions); // either an array of ERR(SPELL_CHECK_FOUND_TERM_IN_INDEX)
      if (MRReply_Type(term) != MR_REPLY_STRING
         || (sug_type != MR_REPLY_ARRAY && sug_type != MR_REPLY_ERROR)) {
        error = "bad reply returned";
        goto finish;
      }

      if (!spellCheckAnalyzeResult_resp3(spellcheckCtx, term, suggestions)) {
        error = "could not analyze term result";
        goto finish;
      }
    }
  }

  RedisModule_Reply _reply = RedisModule_NewReply(ctx), *reply = &_reply;
  RedisModule_Reply_Map(reply);
    RedisModule_Reply_SimpleString(reply, "results");
    spellCheckSendResult(reply, spellcheckCtx, totalDocNum);
  RedisModule_Reply_MapEnd(reply);
  RedisModule_EndReply(reply);

finish:
  spellcheckReducerCtx_Free(spellcheckCtx);
  if (error) {
    RedisModule_ReplyWithError(ctx, error);  // error already contains RQE_ prefix from other sources
  }

  return REDISMODULE_OK;
}
