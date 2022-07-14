#include "document.h"
#include "err.h"
#include "util/logging.h"
#include "commands.h"
#include "rmutil/rm_assert.h"

/*
## FT.ADD <index> <docId> <score> [NOSAVE] [REPLACE] [PARTIAL] [IF <expr>] [LANGUAGE <lang>]
[PAYLOAD {payload}] FIELDS <field> <text> ....] Add a documet to the index.

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

    - LANGUAGE lang: If set, we use a stemmer for the supplied langauge during
indexing. Defaults to
English.
   If an unsupported language is sent, the command returns an error.
   The supported languages are:

   > "arabic",  "danish",    "dutch",     "english",   "finnish",    "french",
   > "german",  "hindi",     "hungarian", "italian",   "norwegian",  "portuguese", "romanian",
   > "russian", "spanish",   "swedish",   "tamil",     "turkish"


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
  ACArgSpec argList[] = {{AC_MKBITFLAG("NOSAVE", &opts->options, DOCUMENT_ADD_NOSAVE)},
                         {AC_MKBITFLAG("REPLACE", &opts->options, DOCUMENT_ADD_REPLACE)},
                         {AC_MKBITFLAG("PARTIAL", &opts->options, DOCUMENT_ADD_PARTIAL)},
                         {AC_MKBITFLAG("NOCREATE", &opts->options, DOCUMENT_ADD_NOCREATE)},
                         {.name = "PAYLOAD", .type = AC_ARGTYPE_RSTRING, .target = &opts->payload},
                         {.name = "LANGUAGE", .type = AC_ARGTYPE_STRING, .target = &languageStr},
                         {.name = "IF", .type = AC_ARGTYPE_STRING, .target = &opts->evalExpr},
                         {.name = NULL}};

  while (!ac->IsAtEnd()) {
    int rv = 0;
    ACArgSpec *errArg = NULL;

    if ((rv = ac->ParseArgSpec(argList, &errArg)) == AC_OK) {
      continue;
    } else if (rv == AC_ERR_ENOENT) {
      size_t narg;
      const char *s = ac->GetStringNC(&narg);
      if (!strncasecmp("FIELDS", s, narg)) {
        size_t numRemaining = ac->NumRemaining();
        if (numRemaining % 2 != 0) {
          status->SetError(QUERY_EADDARGS,
                              "Fields must be specified in FIELD VALUE pairs");
          return REDISMODULE_ERR;
        } else {
          opts->fieldsArray = (RedisModuleString **)ac->objs + ac->offset;
          opts->numFieldElems = numRemaining;
          foundFields = 1;
        }
        break;

      } else {
        status->SetErrorFmt(QUERY_EADDARGS, "Unknown keyword `%.*s` provided", (int)narg,
                               s);
        return REDISMODULE_ERR;
      }
      // Argument not found, that's ok. We'll handle it below
    } else {
      status->SetErrorFmt(QUERY_EADDARGS, "%s: %s", errArg->name, AC_Strerror(rv));
      return REDISMODULE_ERR;
    }
  }

  if (!foundFields) {
    // If we've reached here, there is no fields list. This is an error??
    status->SetError(QUERY_EADDARGS, "No field list found");
    return REDISMODULE_ERR;
  }

  opts->language = RSLanguage_Find(languageStr);
  if (opts->language == RS_LANG_UNSUPPORTED) {
    status->SetError(QUERY_EADDARGS, "Unsupported language");
    return REDISMODULE_ERR;
  }

  if (status->HasError()) {
    return REDISMODULE_ERR;
  }
  if (partial) {
    opts->options |= DOCUMENT_ADD_PARTIAL;
  }
  if (nosave) {
    opts->options |= DOCUMENT_ADD_NOSAVE;
  }
  if (replace) {
    opts->options |= DOCUMENT_ADD_REPLACE;
  }
  return REDISMODULE_OK;
}

int RedisSearchCtx::AddDocument(RedisModuleString *name, const AddDocumentOptions *opts,
                                QueryError *status) {
  int rc = REDISMODULE_ERR;
  // If the ID is 0, then the document does not exist.
  IndexSpec *sp = spec;
  int exists = !!sp->docs.GetIdR(name);

  AddDocumentCtx *aCtx = NULL;
  RedisModuleCtx *ctx = redisCtx;
  Document doc = *new Document(name, opts->score, opts->language);
  uint32_t addOptions;

  if (exists && !(opts->options & DOCUMENT_ADD_REPLACE)) {
    status->SetError(QUERY_EDOCEXISTS, NULL);
    goto error;
  }

  // handle update condition, only if the document exists
  if (exists && opts->evalExpr) {
    int res = 0;
    if (Document::EvalExpression(this, name, opts->evalExpr, &res, status) == REDISMODULE_OK) {
      if (res == 0) {
        status->SetError(QUERY_EDOCNOTADDED, NULL);
        goto error;
      }
    } else {
      printf("Eval failed! (%s)\n", opts->evalExpr);
      if (status->code == QUERY_ENOPROPVAL) {
        status->ClearError();
        status->SetCode(QUERY_EDOCNOTADDED);
      }
      goto error;
    }
  }

  if (opts->payload) {
    size_t npayload = 0;
    const char *payload = RedisModule_StringPtrLen(opts->payload, &npayload);
    doc.SetPayload(payload, npayload);
  }
  doc.LoadPairwiseArgs(opts->fieldsArray, opts->numFieldElems);

  if (!(opts->options & DOCUMENT_ADD_NOSAVE)) {
    int saveopts = 0;
    if (opts->options & DOCUMENT_ADD_NOCREATE) {
      saveopts |= REDIS_SAVEDOC_NOCREATE;
    }
    RedisSearchCtx sctx = SEARCH_CTX_STATIC(redisCtx, sp);
    if (Redis_SaveDocument(&sctx, &doc, saveopts, status) != REDISMODULE_OK) {
      goto error;
    }
  }

  LG_DEBUG("Adding doc %s with %d fields\n", RedisModule_StringPtrLen(doc.docKey, NULL),
           doc.numFields);
  aCtx = new AddDocumentCtx(sp, &doc, status);
  if (aCtx == NULL) {
    goto error;
  }

  addOptions = opts->options;

  if (!exists) {
    // If the document does not exist, remove replace/partial settings
    addOptions &= ~(DOCUMENT_ADD_REPLACE | DOCUMENT_ADD_PARTIAL);
  }
  if (addOptions & DOCUMENT_ADD_CURTHREAD) {
    aCtx->stateFlags |= ACTX_F_NOBLOCK;
  }

  aCtx->donecb = opts->donecb;
  aCtx->Submit(this, addOptions);
  return REDISMODULE_OK;

error:
  return REDISMODULE_ERR;
}

static void replyCallback(AddDocumentCtx *aCtx, RedisModuleCtx *ctx, void *unused) {
  if (aCtx->status.HasError()) {
    if (aCtx->status.code == QUERY_EDOCNOTADDED) {
      RedisModule_ReplyWithError(ctx, "NOADD");
    } else {
      RedisModule_ReplyWithError(ctx, aCtx->status.GetError());
    }
  } else {
    RedisModule_ReplyWithSimpleString(ctx, "OK");
  }
}

static int doAddDocument(RedisModuleCtx *ctx, RedisModuleString **argv, int argc, int canBlock) {
  if (argc < 4) {
    // cmd, index, document, [arg] ...
    return RedisModule_WrongArity(ctx);
  }

  ArgsCursor ac;
  AddDocumentOptions opts = {.donecb = replyCallback};
  QueryError status;
  RedisSearchCtx *sctx = NULL;
  IndexSpec *sp;

  ac.InitRString(argv + 3, argc - 3);

  int rv = 0;
  if ((rv = ac.GetDouble(&opts.score, 0) != AC_OK)) {
    status.SetError(QUERY_EADDARGS, "Could not parse document score");
  } else if (opts.score < 0 || opts.score > 1.0) {
    status.SetError(QUERY_EADDARGS, "Score must be between 0 and 1");
  } else if (parseDocumentOptions(&opts, &ac, &status) != REDISMODULE_OK) {
    status.MaybeSetCode(QUERY_EADDARGS);
  }

  if (status.HasError()) {
    RedisModule_ReplyWithError(ctx, status.GetError());
    goto cleanup;
  }

  sp = IndexSpec::Load(ctx, RedisModule_StringPtrLen(argv[1], NULL), 0);
  if (!sp) {
    RedisModule_ReplyWithError(ctx, "Unknown index name");
    goto cleanup;
  }

  if (!CheckConcurrentSupport(ctx) || (sp->flags & Index_Temporary) || !canBlock) {
    opts.options |= DOCUMENT_ADD_CURTHREAD;
  }
  sctx = new RedisSearchCtx(ctx, sp);
  rv = RS_AddDocument(sctx, argv[2], &opts, &status);
  if (rv != REDISMODULE_OK) {
    if (status.code == QUERY_EDOCNOTADDED) {
      RedisModule_ReplyWithSimpleString(ctx, "NOADD");
    } else {
      RedisModule_ReplyWithError(ctx, status.GetError());
    }
  } else {
    // Replicate *here*
    // note: we inject the index name manually so that we eliminate alias
    // lookups on smaller documents
    RedisModule_Replicate(ctx, RS_SAFEADD_CMD, "cv", sp->name, argv + 2, argc - 2);
  }

cleanup:
  status.ClearError();
  return REDISMODULE_OK;
}

/* FT.ADDHASH <index> <docId> <score> [LANGUAGE <lang>] [REPLACE]
*  Index a document that's already saved in redis as a HASH object, unrelated to
* the module.
*  This will not modify the document, just add it to the index if it is not
* already there.

## Parameters:

      - index: The Fulltext index name. The index must be first created with
        FT.CREATE

      - docId: The document's id, that must be a HASH key already in redis.

      - score: The document's rank based on the user's ranking. This must be
        between 0.0 and 1.0.
        If you don't have a score just set it to 1

      - REPLACE: If set, we will do an update and delete an older version of the document if it
exists

      - LANGUAGE lang: If set, we use a stemmer for the supplied langauge during
      indexing. Defaults to
      English.
        If an unsupported language is sent, the command returns an error.
        The supported languages are:

        > "arabic",  "danish",    "dutch",   "english",   "finnish", "french",
        > "german",  "hungarian",  "hindi",  "italian", "norwegian", "portuguese",
"romanian",
        > "russian", "spanish",   "swedish", "tamil",     "turkish"


  Returns OK on success, or an error if something went wrong.
*/
static int doAddHashCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc,
                            int isBlockable) {
  if (argc < 4 || argc > 7) {
    return RedisModule_WrongArity(ctx);
  }

  QueryError status;
  ArgsCursor ac;
  ac.InitRString(argv + 3, argc - 3);

  double ds;
  int rv = 0, replace = 0;
  RSLanguage language;
  const char *languageStr = NULL;
  IndexSpec *sp = NULL;
  Document *doc = NULL;
  AddDocumentCtx *aCtx = NULL;
  RedisSearchCtx *sctx = NULL;

  ACArgSpec specs[] =  // Comment to force newline
      {{name: "LANGUAGE", type: AC_ARGTYPE_STRING, target: &languageStr},
       {name: "REPLACE", type: AC_ARGTYPE_BOOLFLAG, target: &replace},
       {name: NULL}};
  ACArgSpec *errArg = NULL;

  if ((rv = ac.GetDouble(&ds, 0)) != AC_OK) {
    status.SetError(QUERY_EADDARGS, "Could not parse document score");
    goto cleanup;
  } else if (ds < 0 || ds > 1.0) {
    status.SetError(QUERY_EADDARGS, "Score must be between 0 and 1");
    goto cleanup;
  }

  rv = ac.ParseArgSpec(specs, &errArg);
  if (rv == AC_OK) {
    // OK. No error
  } else if (rv == AC_ERR_ENOENT) {
    const char *keyword = ac.GetStringNC(NULL);
    status.SetErrorFmt(QUERY_EADDARGS, "Unknown keyword: `%s`", keyword);
    goto cleanup;
  } else {
    status.SetErrorFmt(QUERY_EADDARGS, "Error parsing arguments for `%s`: %s",
                       errArg ? errArg->name : "", AC_Strerror(rv));
    goto cleanup;
  }

  language = RSLanguage_Find(languageStr);
  if (language == RS_LANG_UNSUPPORTED) {
    status.SetErrorFmt(QUERY_EADDARGS, "Unknown language: `%s`", languageStr);
    goto cleanup;
  }

  sp = IndexSpec::Load(ctx, RedisModule_StringPtrLen(argv[1], NULL), 1);
  if (sp == NULL) {
    status.SetErrorFmt(QUERY_EGENERIC, "Unknown Index name");
    goto cleanup;
  }

  // Load the document score
  doc = new Document(argv[2], ds, language);
  sctx = &SEARCH_CTX_STATIC(ctx, sp);
  if (doc->LoadAllFields(ctx) != REDISMODULE_OK) {
    return RedisModule_ReplyWithError(ctx, "Could not load document");
  }

  LG_DEBUG("Adding doc %s with %d fields\n", RedisModule_StringPtrLen(doc->docKey, NULL),
           doc->numFields);

  aCtx = new AddDocumentCtx(sp, doc, &status);
  if (aCtx == NULL) {
    return QueryError_ReplyAndClear(ctx, &status);
  }

  aCtx->donecb = replyCallback;

  if (isBlockable) {
    isBlockable = CheckConcurrentSupport(ctx);
  }

  if (!isBlockable) {
    aCtx->stateFlags |= ACTX_F_NOBLOCK;
  }

  RedisModule_Replicate(ctx, RS_SAFEADDHASH_CMD, "v", argv + 1, argc - 1);
  aCtx->Submit(sctx, replace ? DOCUMENT_ADD_REPLACE : 0);
  return REDISMODULE_OK;

cleanup:
  RS_LOG_ASSERT_FMT(status.HasError(), "%s%s", "Hash addition failed: ", status.detail);
  RedisModule_ReplyWithError(ctx, status.GetError());
  status.ClearError();
  return REDISMODULE_OK;
}

int RSAddDocumentCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  return doAddDocument(ctx, argv, argc, 1);
}
int RSSafeAddDocumentCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  return doAddDocument(ctx, argv, argc, 0);
}

int RSAddHashCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  return doAddHashCommand(ctx, argv, argc, 1);
}

int RSSafeAddHashCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  return doAddHashCommand(ctx, argv, argc, 0);
}
