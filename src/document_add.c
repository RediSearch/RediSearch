/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#include "document.h"
#include "err.h"
#include "util/logging.h"
#include "module.h"
#include "rmutil/rm_assert.h"
#include "info/info_redis/threads/current_thread.h"

// Forward declaration.
bool ACLUserMayAccessIndex(RedisModuleCtx *ctx, IndexSpec *sp);

/*
## FT.ADD <index> <docId> <score> [NOSAVE] [REPLACE] [PARTIAL] [IF <expr>] [LANGUAGE <lang>]
[PAYLOAD {payload}] FIELDS <field> <text> ....] Add a document to the index.

## Parameters:

    - index: The Fulltext index name. The index must be first created with
FT.CREATE

    - docId: The document's id that will be returned from searches. Note that
the same docId cannot
be
    added twice to the same index

    - score: The document's rank based on the user's ranking. This must be
between 0.0 and 1.0.
    If you don't have a score just set it to 1

    - NOSAVE: If set to true, we will not save the actual document in the index
and only index it.

    - REPLACE: If set, we will do an update and delete an older version of the document if it
exists

    - FIELDS: Following the FIELDS specifier, we are looking for pairs of
<field> <text> to be
indexed.
    Each field will be scored based on the index spec given in FT.CREATE.
    Passing fields that are not in the index spec will make them be stored as
part of the document,
    or ignored if NOSAVE is set

    - LANGUAGE lang: If set, we use a stemmer for the supplied language during
indexing. Defaults to
English.
   If an unsupported language is sent, the command returns an error.
   The supported languages are:

   > "arabic",  "armenian",  "danish",    "dutch",     "english",   "finnish",    "french",
   > "german",  "hindi",     "hungarian", "italian",   "norwegian", "portuguese", "romanian",
   > "russian", "serbian",   "spanish",   "swedish",   "tamil",     "turkish",    "yiddish"


Returns OK on success, NOADD if the document was not added due to an IF expression not evaluating to
true or an error if something went wrong.
*/

static int parseDocumentOptions(AddDocumentOptions *opts, ArgsCursor *ac, QueryError *status) {
  // Assume argc and argv are at proper indices
  int nosave = 0, replace = 0, partial = 0, foundFields = 0;
  opts->fieldsArray = NULL;
  opts->numFieldElems = 0;
  opts->options = 0;

  char *languageStr = NULL;
  ACArgSpec argList[] = {
      {AC_MKBITFLAG("REPLACE", &opts->options, DOCUMENT_ADD_REPLACE)},
      {AC_MKBITFLAG("PARTIAL", &opts->options, DOCUMENT_ADD_PARTIAL)},
      {AC_MKBITFLAG("NOCREATE", &opts->options, DOCUMENT_ADD_NOCREATE)},
      {.name = "PAYLOAD", .type = AC_ARGTYPE_RSTRING, .target = &opts->payload},
      {.name = "LANGUAGE", .type = AC_ARGTYPE_RSTRING, .target = &opts->languageStr},
      {.name = "IF", .type = AC_ARGTYPE_STRING, .target = &opts->evalExpr},
      {.name = NULL}};

  while (!AC_IsAtEnd(ac)) {
    int rv = 0;
    ACArgSpec *errArg = NULL;

    if ((rv = AC_ParseArgSpec(ac, argList, &errArg)) == AC_OK) {
      continue;
    } else if (rv == AC_ERR_ENOENT) {
      size_t narg;
      const char *s = AC_GetStringNC(ac, &narg);
      if (STR_EQCASE(s, narg, "FIELDS")) {
        size_t numRemaining = AC_NumRemaining(ac);
        if (numRemaining % 2 != 0) {
          QueryError_SetError(status, QUERY_ERROR_CODE_ADD_ARGS,
                              "Fields must be specified in FIELD VALUE pairs");
          return REDISMODULE_ERR;
        } else {
          opts->fieldsArray = (RedisModuleString **)ac->objs + ac->offset;
          opts->numFieldElems = numRemaining;
          foundFields = 1;
        }
        break;

      } else {
        QueryError_SetWithUserDataFmt(status, QUERY_ERROR_CODE_ADD_ARGS, "Unknown keyword", " `%.*s` provided", (int)narg, s);
        return REDISMODULE_ERR;
      }
      // Argument not found, that's ok. We'll handle it below
    } else {
      char message[1024];
      QueryError_SetWithoutUserDataFmt(status, QUERY_ERROR_CODE_ADD_ARGS, "Parsing error for document option %s: %s", errArg->name, AC_Strerror(rv));
      return REDISMODULE_ERR;
    }
  }

  if (!foundFields) {
    // If we've reached here, there is no fields list. This is an error??
    QueryError_SetError(status, QUERY_ERROR_CODE_ADD_ARGS, "No field list found");
    return REDISMODULE_ERR;
  }

  if (opts->languageStr != NULL) {
    size_t len;
    const char *lang = RedisModule_StringPtrLen(opts->languageStr, &len);
    opts->language = RSLanguage_Find(lang, len);
    if (opts->language == RS_LANG_UNSUPPORTED) {
      QueryError_SetError(status, QUERY_ERROR_CODE_ADD_ARGS, "Unsupported language");
      return REDISMODULE_ERR;
    }
  }

  if (QueryError_HasError(status)) {
    return REDISMODULE_ERR;
  }
  if (partial) {
    opts->options |= DOCUMENT_ADD_PARTIAL;
  }
  if (replace) {
    opts->options |= DOCUMENT_ADD_REPLACE;
  }
  return REDISMODULE_OK;
}

int RS_AddDocument(RedisSearchCtx *sctx, RedisModuleString *name, const AddDocumentOptions *opts,
                   QueryError *status) {
  int rc = REDISMODULE_ERR;

  int exists = -1;
  RedisModuleKey *k = RedisModule_OpenKey(sctx->redisCtx, name, REDISMODULE_READ);
  if (k == NULL || RedisModule_KeyType(k) == REDISMODULE_KEYTYPE_EMPTY) {
    exists = 0;
  } else if (RedisModule_KeyType(k) == REDISMODULE_KEYTYPE_HASH) {
    exists = 1;
  }

  if (k) {
    RedisModule_CloseKey(k);
  }

  if (exists == -1) {
    QueryError_SetCode(status, QUERY_ERROR_CODE_REDIS_KEY_TYPE);
    goto done;
  }

  if (!exists && (opts->options & DOCUMENT_ADD_NOCREATE)) {
    QueryError_SetError(status, QUERY_ERROR_CODE_NO_DOC, "Document does not exist");
    goto done;
  }

  if (exists && !(opts->options & DOCUMENT_ADD_REPLACE)) {
    QueryError_SetCode(status, QUERY_ERROR_CODE_DOC_EXISTS);
    goto done;
  }

  // handle update condition, only if the document exists
  if (exists && opts->evalExpr) {
    int res = 0;
    HiddenString* expr = NewHiddenString(opts->evalExpr, strlen(opts->evalExpr), false);
    const int rc = Document_EvalExpression(sctx, name, expr, &res, status);
    HiddenString_Free(expr, false);
    if (rc == REDISMODULE_OK) {
      if (res == 0) {
        QueryError_SetCode(status, QUERY_ERROR_CODE_DOC_NOT_ADDED);
        goto done;
      }
    } else {
      if (QueryError_GetCode(status) == QUERY_ERROR_CODE_NO_PROP_VAL) {
        QueryError_ClearError(status);
        QueryError_SetCode(status, QUERY_ERROR_CODE_DOC_NOT_ADDED);
      }
      goto done;
    }
  }

  // remove doc entirely if not partial update
  if (exists && opts->options & DOCUMENT_ADD_REPLACE && !(opts->options & DOCUMENT_ADD_PARTIAL)) {
    RedisModuleCallReply *reply = RedisModule_Call(sctx->redisCtx, "DEL", "s", opts->keyStr);
    if (reply) {
      RedisModule_FreeCallReply(reply);
    }
  }

  rc = Redis_SaveDocument(sctx, opts, status);

done:
  return rc;
}

static void replyCallback(RSAddDocumentCtx *aCtx, RedisModuleCtx *ctx, void *unused) {
  if (QueryError_HasError(&aCtx->status)) {
    if (QueryError_GetCode(&aCtx->status) == QUERY_ERROR_CODE_DOC_NOT_ADDED) {
      RedisModule_ReplyWithError(ctx, "NOADD");
    } else {
      RedisModule_ReplyWithError(ctx, QueryError_GetUserError(&aCtx->status));
    }
  } else {
    RedisModule_ReplyWithSimpleString(ctx, "OK");
  }
}

int RSAddDocumentCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  if (argc < 4) {
    // cmd, index, document, [arg] ...
    return RedisModule_WrongArity(ctx);
  }

  StrongRef ref = IndexSpec_LoadUnsafe(RedisModule_StringPtrLen(argv[1], NULL));
  IndexSpec *sp = StrongRef_Get(ref);
  if (!sp) {
    return RedisModule_ReplyWithError(ctx, "RQE_INDEX_NOT_FOUND: Unknown index name");
  }

  // Validate ACL permission to the index
  if (!ACLUserMayAccessIndex(ctx, sp)) {
    return RedisModule_ReplyWithError(ctx, NOPERM_ERR);
  }

  ArgsCursor ac;
  AddDocumentOptions opts = {.keyStr = argv[2], .scoreStr = argv[3], .donecb = replyCallback};
  QueryError status = QueryError_Default();

  ArgsCursor_InitRString(&ac, argv + 3, argc - 3);

  int rv = 0;
  if ((rv = AC_GetDouble(&ac, &opts.score, 0) != AC_OK)) {
    QueryError_SetError(&status, QUERY_ERROR_CODE_ADD_ARGS, "Could not parse document score");
  } else if (opts.score < 0 || opts.score > 1.0) {
    QueryError_SetError(&status, QUERY_ERROR_CODE_ADD_ARGS, "Score must be between 0 and 1");
  } else if (parseDocumentOptions(&opts, &ac, &status) != REDISMODULE_OK) {
    QueryError_MaybeSetCode(&status, QUERY_ERROR_CODE_ADD_ARGS);
  }

  if (QueryError_HasError(&status)) {
    RedisModule_ReplyWithError(ctx, QueryError_GetUserError(&status));
    goto cleanup;
  }

  CurrentThread_SetIndexSpec(ref);

  RedisSearchCtx sctx = SEARCH_CTX_STATIC(ctx, sp);
  rv = RS_AddDocument(&sctx, argv[2], &opts, &status);
  if (rv != REDISMODULE_OK) {
    if (QueryError_GetCode(&status) == QUERY_ERROR_CODE_DOC_NOT_ADDED) {
      RedisModule_ReplyWithSimpleString(ctx, "NOADD");
    } else {
      RedisModule_ReplyWithError(ctx, QueryError_GetUserError(&status));
    }
  } else {
    // Replicate *here*
    // note: we inject the index name manually so that we eliminate alias
    // lookups on smaller documents
    // RedisModule_Replicate(ctx, RS_SAFEADD_CMD, "cv", sp->name, argv + 2, argc - 2);

    // RS 2.0 - HSET replicates using `!v`
    RedisModule_ReplyWithSimpleString(ctx, "OK");
  }

  CurrentThread_ClearIndexSpec();

cleanup:
  QueryError_ClearError(&status);
  return REDISMODULE_OK;
}
