#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/param.h>
#include <time.h>

#include "commands.h"
#include "document.h"
#include "tag_index.h"
#include "index.h"
#include "query.h"
#include "redis_index.h"
#include "redismodule.h"
#include "rmutil/strings.h"
#include "rmutil/util.h"
#include "rmutil/args.h"
#include "spec.h"
#include "util/logging.h"
#include "config.h"
#include "aggregate/aggregate.h"
#include "aggregate/expr/attribute.h"
#include "rmalloc.h"
#include "cursor.h"
#include "version.h"
#include "debug_commads.h"
#include "spell_check.h"
#include "dictionary.h"
#include "suggest.h"
#include "numeric_index.h"
#include "redisearch_api.h"
#include "alias.h"
#include "module.h"
#include "info_command.h"
#include "result_processor.h"
#include "rules/rules.h"

pthread_rwlock_t RWLock = PTHREAD_RWLOCK_INITIALIZER;

typedef int (*RSCommandHandler)(RedisModuleCtx *, IndexSpec *, ArgsCursor *);

static int specLockHandler(RedisModuleCtx *ctx, RSCommandHandler handler, RedisModuleString **argv,
                           int argc, uint32_t loadflags) {
  if (argc < 2) {
    return RedisModule_WrongArity(ctx);
  }
  IndexLoadOptions lopts = {.flags = INDEXSPEC_LOAD_KEY_RSTRING | INDEXSPEC_LOAD_LOCKED | loadflags,
                            .name = {.rstring = argv[1]}};
  IndexSpec *sp = IndexSpec_LoadEx(NULL, &lopts);
  if (!sp) {
    return RedisModule_ReplyWithError(ctx, "Unknown Index name");
  }
  ArgsCursor ac = {0};
  ArgsCursor_InitRString(&ac, argv + 1, argc - 1);
  AC_Seek(&ac, 1);
  IndexSpec_Incref(sp);
  handler(ctx, sp, &ac);
  pthread_rwlock_unlock(&sp->idxlock);
  IndexSpec_Decref(sp);
  return REDISMODULE_OK;
}

#define CREATE_WRAPPER_EX(name, target, flags, minargs, maxargs)             \
  static int name(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) { \
    if (minargs && argc < minargs + 2) {                                     \
      return RedisModule_WrongArity(ctx);                                    \
    }                                                                        \
    if (maxargs && argc - 2 > maxargs) {                                     \
      return RedisModule_WrongArity(ctx);                                    \
    }                                                                        \
    return specLockHandler(ctx, target, argv, argc, flags);                  \
  }

#define CREATE_WRAPPER(name, target, flags) CREATE_WRAPPER_EX(name, target, flags, 0, 0)

#define COMMAND_HANDLER(name) static int name(RedisModuleCtx *ctx, IndexSpec *sp, ArgsCursor *ac)

/* FT.SETPAYLOAD {index} {docId} {payload} */
COMMAND_HANDLER(handleSetPayload) {
  if (AC_NumRemaining(ac) != 2) {
    return RedisModule_WrongArity(ctx);
  }
  size_t didLen = 0;
  size_t plLen = 0;
  const char *did = AC_GetStringNC(ac, &didLen);
  const char *payload = AC_GetStringNC(ac, &plLen);

  /* Find the document by its key */
  RSDocumentMetadata *dmd = DocTable_GetByKey(&sp->docs, did, didLen);
  if (dmd == NULL) {
    return RedisModule_ReplyWithError(ctx, "Document not in index");
  }

  if (DocTable_SetPayload(&sp->docs, dmd->id, payload, plLen) == 0) {
    return RedisModule_ReplyWithError(ctx, "Could not set payload ¯\\_(ツ)_/¯");
  }

  RedisModule_ReplyWithSimpleString(ctx, "OK");
  return REDISMODULE_OK;
}
CREATE_WRAPPER(SetPayloadCommand, handleSetPayload, 0)

static void replyDoc(RedisModuleCtx *ctx, IndexSpec *sp, RedisModuleString *docname) {
  const DocTable *dt = &sp->docs;
  if (!DocTable_GetByKeyR(dt, docname)) {
    RedisModule_ReplyWithNull(ctx);
    return;
  }
  Document doc = {0};
  Document_Init(&doc, docname, 0, DEFAULT_LANGUAGE);
  if (Document_LoadAllFields(&doc, ctx) == REDISMODULE_ERR) {
    RedisModule_ReplyWithNull(ctx);
  } else {
    Document_ReplyFields(ctx, &doc);
    Document_Free(&doc);
  }
}

/* FT.MGET {index} {key} ...
 * Get document(s) by their id.
 * Currentlt it just performs HGETALL, but it's a future proof alternative allowing us to later on
 * replace the internal representation of the documents.
 *
 * If referred docs are missing or not HASH keys, we simply reply with Null, but the result will
 * be an array the same size of the ids list
 */
COMMAND_HANDLER(handleMget) {
  const DocTable *dt = &sp->docs;
  RedisModule_ReplyWithArray(ctx, AC_NumRemaining(ac));
  assert(ac->type == AC_TYPE_RSTRING);
  while (AC_NumRemaining(ac)) {
    RedisModuleString *did = AC_GetRStringNC(ac);
    replyDoc(ctx, sp, did);
  }
  return REDISMODULE_OK;
}
CREATE_WRAPPER_EX(GetDocumentsCommand, handleMget, 0, 1, 0)

/* FT.GET {index} {key} ...
 * Get a single document by their id.
 * Currentlt it just performs HGETALL, but it's a future proof alternative allowing us to later on
 * replace the internal representation of the documents.
 *
 * If referred docs are missing or not HASH keys, we simply reply with Null
 */
COMMAND_HANDLER(handleGet) {
  if (AC_NumRemaining(ac) != 1) {
    return RedisModule_WrongArity(ctx);
  }
  replyDoc(ctx, sp, AC_GetRStringNC(ac));
  return REDISMODULE_OK;
}
CREATE_WRAPPER_EX(GetSingleDocumentCommand, handleGet, 0, 1, 1)

COMMAND_HANDLER(handleSpellcheck) {
#define DICT_INITIAL_SIZE 5
#define DEFAULT_LEV_DISTANCE 1
#define MAX_LEV_DISTANCE 100
  if (!AC_NumRemaining(ac)) {
    return RedisModule_WrongArity(ctx);
  }
  QueryError status = {0};
  size_t len;
  const char *rawQuery = AC_GetStringNC(ac, &len);
  const char **includeDict = NULL, **excludeDict = NULL;
  RSSearchOptions opts = {0};
  QueryAST qast = {0};
  int rc = QAST_Parse(&qast, sp, &opts, rawQuery, len, &status);
  int replied = 0;

  if (rc != REDISMODULE_OK) {
    RedisModule_ReplyWithError(ctx, QueryError_GetError(&status));
    goto end;
  }

  includeDict = array_new(const char *, DICT_INITIAL_SIZE);
  excludeDict = array_new(const char *, DICT_INITIAL_SIZE);

  long long distance = DEFAULT_LEV_DISTANCE;
  bool fullScoreInfo = false;

  while (AC_NumRemaining(ac)) {
    if (AC_AdvanceIfMatch(ac, "DISTANCE")) {
      int rv;
      if ((rv = AC_GetLongLong(ac, &distance, AC_F_GE1)) != AC_OK) {
        QERR_MKBADARGS_AC(&status, "DISTANCE", rv);
        goto end;
      }
      if (distance > MAX_LEV_DISTANCE) {
        QERR_MKBADARGS_FMT(&status, "Distance must be a number between 1 and %u", MAX_LEV_DISTANCE);
        goto end;
      }
    }  // LCOV_EXCL_LINE

    if (AC_AdvanceIfMatch(ac, "TERMS")) {
      if (AC_NumRemaining(ac) < 2) {
        QERR_MKBADARGS_FMT(&status, "TERM arg is given but no TERM params comes after");
        goto end;
      }
      int include = 0;
      if (AC_AdvanceIfMatch(ac, "INCLUDE")) {
        include = 1;
      } else if (AC_AdvanceIfMatch(ac, "EXCLUDE")) {
        // include = 0
      } else {
        QERR_MKBADARGS_FMT(&status, "Expected INCLUDE or EXCLUDE");
      }
      const char *dictName = AC_GetStringNC(ac, NULL);
      if (include) {
        includeDict = array_append(includeDict, dictName);
      } else {
        excludeDict = array_append(excludeDict, dictName);
      }
    } else if (AC_AdvanceIfMatch(ac, "FULLSCOREINFO")) {
      fullScoreInfo = true;
    } else {
      QERR_MKBADARGS_FMT(&status, "Unknown option %s provided", AC_GetStringNC(ac, NULL));
    }
  }
  RedisSearchCtx sctx = SEARCH_CTX_STATIC(ctx, sp);
  SpellCheckCtx scCtx = {.sctx = &sctx,
                         .includeDict = includeDict,
                         .excludeDict = excludeDict,
                         .distance = distance,
                         .fullScoreInfo = fullScoreInfo};

  SpellCheck_Reply(&scCtx, &qast);
  replied = 1;

end:
  if (!replied) {
    RedisModule_ReplyWithError(ctx, QueryError_GetError(&status));
  }
  QueryError_ClearError(&status);
  if (includeDict != NULL) {
    array_free(includeDict);
  }
  if (excludeDict != NULL) {
    array_free(excludeDict);
  }
  QAST_Destroy(&qast);
  return REDISMODULE_OK;
}
CREATE_WRAPPER(SpellCheckCommand, handleSpellcheck, 0)

char *RS_GetExplainOutput(RedisModuleCtx *ctx, RedisModuleString **argv, int argc,
                          QueryError *status);

static int queryExplainCommon(RedisModuleCtx *ctx, RedisModuleString **argv, int argc,
                              int newlinesAsElements) {
  QueryError status = {0};
  char *explainRoot = RS_GetExplainOutput(ctx, argv, argc, &status);
  if (!explainRoot) {
    return QueryError_ReplyAndClear(ctx, &status);
  }
  if (newlinesAsElements) {
    size_t numElems = 0;
    RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
    char *explain = explainRoot;
    char *curLine = NULL;
    while ((curLine = strsep(&explain, "\n")) != NULL) {
      RedisModule_ReplyWithSimpleString(ctx, curLine);
      numElems++;
    }
    RedisModule_ReplySetArrayLength(ctx, numElems);
  } else {
    RedisModule_ReplyWithStringBuffer(ctx, explainRoot, strlen(explainRoot));
  }

  rm_free(explainRoot);
  return REDISMODULE_OK;
}

/* FT.EXPLAIN {index_name} {query} */
int QueryExplainCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  return queryExplainCommon(ctx, argv, argc, 0);
}
int QueryExplainCLICommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  return queryExplainCommon(ctx, argv, argc, 1);
}

int RSAggregateCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int RSSearchCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int RSCursorCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);

/* FT.DEL {index} {doc_id}
 *  Delete a document from the index. Returns 1 if the document was in the index, or 0 if not.
 *
 *  **NOTE**: This does not actually delete the document from the index, just marks it as
 * deleted If DD (Delete Document) is set, we also delete the document.
 */
COMMAND_HANDLER(handleDel) {
  if (sp->flags & Index_UseRules) {
    return RedisModule_ReplyWithError(
        ctx, "Cannot manually remove documents from index declared using `WITHRULES`");
  }
  RedisModuleString *docKey = AC_GetRStringNC(ac);
  int delDoc = AC_AdvanceIfMatch(ac, "DD");

  RedisSearchCtx sctx = SEARCH_CTX_STATIC(ctx, sp);

  // Get the doc ID
  t_docId id = DocTable_GetIdR(&sp->docs, docKey);
  if (id == 0) {
    return RedisModule_ReplyWithLongLong(ctx, 0);
    // ID does not exist.
  }

  for (size_t i = 0; i < sp->numFields; ++i) {
    FieldSpec *fs = sp->fields + i;
    if (!FIELD_IS(fs, INDEXFLD_T_GEO)) {
      continue;
    }
    GeoIndex *gi = IDX_LoadGeo(sp, fs, REDISMODULE_WRITE);
    GeoIndex_RemoveEntries(gi, sctx.spec, id);
  }

  int rc = DocTable_DeleteR(&sp->docs, docKey);
  if (rc) {
    sp->stats.numDocuments--;

    // If needed - delete the actual doc
    if (delDoc) {

      RedisModuleKey *dk = RedisModule_OpenKey(ctx, docKey, REDISMODULE_WRITE);
      if (dk && RedisModule_KeyType(dk) == REDISMODULE_KEYTYPE_HASH) {
        RedisModule_DeleteKey(dk);
      } else {
        RedisModule_Log(ctx, "warning", "Document %s doesn't exist",
                        RedisModule_StringPtrLen(docKey, NULL));
      }
    }

    // Increment the index's garbage collector's scanning frequency after document deletions
    if (sp->gc) {
      GCContext_OnDelete(sp->gc);
    }
    if (!delDoc) {
      RedisModule_Replicate(ctx, RS_DEL_CMD, "cs", sp->name, docKey);
    } else {
      RedisModule_Replicate(ctx, RS_DEL_CMD, "csc", sp->name, docKey, "dd");
    }
  }
  return RedisModule_ReplyWithLongLong(ctx, rc);
}
CREATE_WRAPPER_EX(DeleteCommand, handleDel, INDEXSPEC_LOAD_WRITEABLE, 1, 2)

/* FT.TAGVALS {idx} {field}
 * Return all the values of a tag field.
 * There is no sorting or paging, so be careful with high-cradinality tag fields */
COMMAND_HANDLER(handleTagvals) {
  // at least one field, and number of field/text args must be even
  if (!AC_NumRemaining(ac)) {
    return RedisModule_WrongArity(ctx);
  }
  const char *field = AC_GetStringNC(ac, NULL);
  const FieldSpec *fs = IndexSpec_GetField(sp, field, strlen(field));
  if (!fs) {
    return RedisModule_ReplyWithError(ctx, "No such field");
  }
  if (!FIELD_IS(fs, INDEXFLD_T_TAG)) {
    return RedisModule_ReplyWithError(ctx, "Not a tag field");
  }
  TagIndex *idx = IDX_LoadTags(sp, fs, REDISMODULE_READ);
  if (!idx) {
    return RedisModule_ReplyWithArray(ctx, 0);
  }

  TagIndex_SerializeValues(idx, ctx);
  return REDISMODULE_OK;
}
CREATE_WRAPPER(TagValsCommand, handleTagvals, 0)
/*
## FT.CREATE {index} [NOOFFSETS] [NOFIELDS]
    SCHEMA {field} [TEXT [NOSTEM] [WEIGHT {weight}]] | [NUMERIC] ...

Creates an index with the given spec. The index name will be used in all the
key
names
so keep it short!

### Parameters:

    - index: the index name to create. If it exists the old spec will be
overwritten

    - NOOFFSETS: If set, we do not store term offsets for documents (saves memory, does not
allow exact searches)

    - NOFIELDS: If set, we do not store field bits for each term. Saves memory, does not allow
      filtering by specific fields.

    - SCHEMA: After the SCHEMA keyword we define the index fields. They can be either numeric or
      textual.
      For textual fields we optionally specify a weight. The default weight is 1.0
      The weight is a double, but does not need to be normalized.

### Returns:

    OK or an error
*/
int CreateIndexCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  // at least one field, the SCHEMA keyword, and number of field/text args must be even
  if (argc < 5) {
    return RedisModule_WrongArity(ctx);
  }

  if (RedisModule_GetSelectedDb(ctx) != 0) {
    return RedisModule_ReplyWithError(ctx, "Cannot create index on db != 0");
  }

  QueryError status = {0};
  IndexCreateOptions opts = {0};
  ArgsCursor ac = {0};
  ArgsCursor_InitRString(&ac, argv + 2, argc - 2);
  const char *name = RedisModule_StringPtrLen(argv[1], NULL);
  IndexSpec *sp = IndexSpec_ParseArgs(name, &ac, &opts, &status);
  if (sp == NULL) {
    return QueryError_ReplyAndClear(ctx, &status);
  }
  if (IndexSpec_Register(sp, &opts, &status) != REDISMODULE_OK) {
    IndexSpec_Free(sp);
    return QueryError_ReplyAndClear(ctx, &status);
  }

  RedisModule_Replicate(ctx, RS_CREATE_CMD, "ccv", sp->name, "REPLACE", argv + 2, (size_t)argc - 2);
  return RedisModule_ReplyWithSimpleString(ctx, "OK");
}

/* FT.OPTIMIZE <index>
 *  After the index is built (and doesn't need to be updated again withuot a
 * complete rebuild)
 *  we can optimize memory consumption by trimming all index buffers to their
 * actual size.
 *
 *  Warning 1: This will delete score indexes for small words (n < 5000), so
 * updating the index
 * after
 *  optimizing it might lead to screwed up results (TODO: rebuild score indexes
 * if needed).
 *  The simple solution to that is to call optimize again after adding
 * documents
 * to the index.
 *
 *  Warning 2: This blocks redis for a long time. Do not run it on production
 * instances
 *
 */
int OptimizeIndexCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  // at least one field, and number of field/text args must be even
  if (argc != 2) {
    return RedisModule_WrongArity(ctx);
  }

  RedisModule_AutoMemory(ctx);

  IndexSpec *sp = IndexSpec_Load(ctx, RedisModule_StringPtrLen(argv[1], NULL), 0);
  if (sp == NULL) {
    return RedisModule_ReplyWithError(ctx, "Unknown Index name");
  }

  // DEPRECATED - we now don't do anything.  The GC optimizes the index in the background
  return RedisModule_ReplyWithLongLong(ctx, 0);
}

/*
 * FT.DROP <index> [KEEPDOCS]
 * Deletes all the keys associated with the index.
 * If no other data is on the redis instance, this is equivalent to FLUSHDB,
 * apart from the fact that the index specification is not deleted.
 *
 * If KEEPDOCS exists, we do not delete the actual docs
 */
COMMAND_HANDLER(handleDrop) {
  int keepDocs = AC_AdvanceIfMatch(ac, "KEEPDOCS");
  int options = !keepDocs ? IDXFREE_F_DELDOCS : 0;
  if (AC_NumRemaining(ac)) {
    return RedisModule_WrongArity(ctx);
  }
  IndexSpec_FreeEx(sp, options);
  return RedisModule_ReplyWithSimpleString(ctx, "OK");
}
CREATE_WRAPPER(DropIndexCommand, handleDrop, INDEXSPEC_LOAD_WRITEABLE)

/**
 * FT.SYNADD <index> <term1> <term2> ...
 *
 * Add a synonym group to the given index. The synonym data structure is compose of synonyms
 * groups. Each Synonym group has a unique id. The SYNADD command creates a new synonym group
 * with the given terms and return its id.
 */
COMMAND_HANDLER(handleSynAdd) {
  RedisModule_ReplicateVerbatim(ctx);
  IndexSpec_InitializeSynonym(sp);

  uint32_t id = SynonymMap_AddRedisStr(sp->smap, (RedisModuleString **)ac->objs + ac->offset,
                                       ac->argc - ac->offset);

  RedisModule_ReplyWithLongLong(ctx, id);

  return REDISMODULE_OK;
}
CREATE_WRAPPER(SynAddCommand, handleSynAdd, INDEXSPEC_LOAD_WRITEABLE)

static int synUpdateCommon(RedisModuleCtx *ctx, IndexSpec *sp, ArgsCursor *ac, bool checkIdSanity) {
  uint32_t id;
  if (!AC_NumRemaining(ac)) {
    return RedisModule_WrongArity(ctx);
  }
  int rc = AC_GetU32(ac, &id, 0);
  if (rc != AC_OK) {
    return RedisModule_ReplyWithError(ctx, AC_Strerror(rc));
  }
  RedisModule_ReplicateVerbatim(ctx);

  if (checkIdSanity && (!sp->smap || id >= SynonymMap_GetMaxId(sp->smap))) {
    RedisModule_ReplyWithError(ctx, "given id does not exists");
    return REDISMODULE_OK;
  }

  IndexSpec_InitializeSynonym(sp);

  SynonymMap_UpdateRedisStr(sp->smap, (RedisModuleString **)ac->objs + ac->offset,
                            ac->argc - ac->offset, id);

  RedisModule_ReplyWithSimpleString(ctx, "OK");

  return REDISMODULE_OK;
}

/**
 * FT.SYNUPDATE <index> <id> <term1> <term2> ...
 *
 * Update an already existing synonym group with the given terms.
 * Its only to add new terms to a synonym group.
 * return true on success.
 */
COMMAND_HANDLER(handleSynUpdate) {
  return synUpdateCommon(ctx, sp, ac, true);
}
CREATE_WRAPPER(SynUpdateCommand, handleSynUpdate, INDEXSPEC_LOAD_WRITEABLE)

COMMAND_HANDLER(handleSynForceUpdate) {
  return synUpdateCommon(ctx, sp, ac, false);
}
CREATE_WRAPPER(SynForceUpdateCommand, handleSynForceUpdate, INDEXSPEC_LOAD_WRITEABLE)

/**
 * FT.SYNDUMP <index>
 *
 * Dump the synonym data structure in the following format:
 *    - term1
 *        - id1
 *        - id2
 *    - term2
 *        - id3
 *    - term3
 *        - id4
 */
COMMAND_HANDLER(handleSynDump) {
  if (!sp->smap) {
    RedisModule_ReplyWithArray(ctx, 0);
    return REDISMODULE_OK;
  }

  size_t size;
  TermData **terms_data = SynonymMap_DumpAllTerms(sp->smap, &size);

  RedisModule_ReplyWithArray(ctx, size * 2);

  for (int i = 0; i < size; ++i) {
    TermData *t_data = terms_data[i];
    RedisModule_ReplyWithStringBuffer(ctx, t_data->term, strlen(t_data->term));
    RedisModule_ReplyWithArray(ctx, array_len(t_data->ids));
    for (size_t j = 0; j < array_len(t_data->ids); ++j) {
      RedisModule_ReplyWithLongLong(ctx, t_data->ids[j]);
    }
  }

  rm_free(terms_data);

  return REDISMODULE_OK;
}
CREATE_WRAPPER(SynDumpCommand, handleSynDump, 0)

COMMAND_HANDLER(handleAlter) {
  // Need at least <subcommand> <args...>
  if (!AC_NumRemaining(ac)) {
    return RedisModule_WrongArity(ctx);
  }
  QueryError status = {0};

  if (AC_AdvanceIfMatch(ac, "SCHEMA")) {
    if (!AC_AdvanceIfMatch(ac, "ADD")) {
      return RedisModule_ReplyWithError(ctx, "Unknown action passed to ALTER SCHEMA");
    }
    if (!AC_NumRemaining(ac)) {
      return RedisModule_ReplyWithError(ctx, "No fields provided");
    }
    IndexSpec_AddFields(sp, ac, &status);
  }

  if (QueryError_HasError(&status)) {
    return QueryError_ReplyAndClear(ctx, &status);
  } else {
    RedisModule_ReplicateVerbatim(ctx);
    return RedisModule_ReplyWithSimpleString(ctx, "OK");
  }
}
CREATE_WRAPPER(AlterIndexCommand, handleAlter, INDEXSPEC_LOAD_WRITEABLE)

static int aliasAddCommon(RedisModuleCtx *ctx, RedisModuleString **argv, int argc,
                          QueryError *error) {
  ArgsCursor ac = {0};
  ArgsCursor_InitRString(&ac, argv + 1, argc - 1);
  IndexLoadOptions loadOpts = {.name = {.rstring = argv[2]},
                               .flags = INDEXSPEC_LOAD_NOALIAS | INDEXSPEC_LOAD_KEY_RSTRING |
                                        INDEXSPEC_LOAD_WRITEABLE | INDEXSPEC_LOAD_LOCKED};
  IndexSpec *sptmp = IndexSpec_LoadEx(ctx, &loadOpts);
  if (!sptmp) {
    QueryError_SetError(error, QUERY_ENOINDEX, "Unknown index name (or name is an alias itself)");
    return REDISMODULE_ERR;
  }
  int rc = IndexAlias_Add(RedisModule_StringPtrLen(argv[1], NULL), sptmp, 0, error);
  pthread_rwlock_unlock(&sptmp->idxlock);
  return rc;
}

// FT.ALIASADD <NAME> <TARGET>
static int AliasAddCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  if (argc != 3) {
    return RedisModule_WrongArity(ctx);
  }
  QueryError e = {0};
  if (aliasAddCommon(ctx, argv, argc, &e) != REDISMODULE_OK) {
    return QueryError_ReplyAndClear(ctx, &e);
  } else {
    RedisModule_ReplicateVerbatim(ctx);
    return RedisModule_ReplyWithSimpleString(ctx, "OK");
  }
}

COMMAND_HANDLER(handleAliasDel) {
  if (AC_NumArgs(ac) != 1) {
    return RedisModule_WrongArity(ctx);
  }
  AC_Seek(ac, 0);
  const char *alias = AC_GetStringNC(ac, NULL);
  QueryError status = {0};
  if (IndexAlias_Del(alias, sp, 0, &status) != REDISMODULE_OK) {
    return QueryError_ReplyAndClear(ctx, &status);
  } else {
    RedisModule_ReplicateVerbatim(ctx);
    return RedisModule_ReplyWithSimpleString(ctx, "OK");
  }
}
CREATE_WRAPPER(AliasDelCommand, handleAliasDel, INDEXSPEC_LOAD_WRITEABLE)

static int AliasUpdateCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  if (argc != 3) {
    return RedisModule_WrongArity(ctx);
  }

  QueryError status = {0};
  IndexLoadOptions lOpts = {
      .name = {.rstring = argv[1]},
      .flags = INDEXSPEC_LOAD_KEY_RSTRING | INDEXSPEC_LOAD_LOCKED | INDEXSPEC_LOAD_WRITEABLE};
  IndexSpec *spOrig = IndexSpec_LoadEx(ctx, &lOpts);
  if (spOrig) {
    int rc = IndexAlias_Del(RedisModule_StringPtrLen(argv[1], NULL), spOrig, 0, &status);
    if (rc != REDISMODULE_OK) {
      QueryError_ReplyAndClear(ctx, &status);
      goto cleanup;
    }
  }

  if (aliasAddCommon(ctx, argv, argc, &status) != REDISMODULE_OK) {
    // Add back the previous index.. this shouldn't fail
    if (spOrig) {
      QueryError e2 = {0};
      const char *alias = RedisModule_StringPtrLen(argv[1], NULL);
      IndexAlias_Add(alias, spOrig, 0, &e2);
      QueryError_ClearError(&e2);
    }
    QueryError_ReplyAndClear(ctx, &status);
  } else {
    RedisModule_ReplicateVerbatim(ctx);
    RedisModule_ReplyWithSimpleString(ctx, "OK");
  }

cleanup:
  if (spOrig) {
    pthread_rwlock_unlock(&spOrig->idxlock);
  }
  return REDISMODULE_OK;
}

static int RuleAddCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  ArgsCursor ac = {0};
  ArgsCursor_InitRString(&ac, argv + 1, argc - 1);
  // INDEX RULENAME MATCHTYPE MATCHEXPR [ACTION ACTION-PARAMS]
  if (AC_NumRemaining(&ac) < 4) {
    return RedisModule_WrongArity(ctx);
  }
  const char *idxstr = AC_GetStringNC(&ac, NULL);
  const char *name = AC_GetStringNC(&ac, NULL);
  QueryError err = {0};
  int rc = SchemaRules_AddArgs(idxstr, name, &ac, &err);
  if (rc != REDISMODULE_OK) {
    RedisModule_ReplyWithError(ctx, QueryError_GetError(&err));
    QueryError_ClearError(&err);
  } else {
    RedisModule_ReplyWithSimpleString(ctx, "OK");
  }
  return REDISMODULE_OK;
}

static int RulesSetCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  ArgsCursor ac = {0};
  ArgsCursor_InitRString(&ac, argv + 1, argc - 1);
  QueryError status = {0};
  int rc = SchemaRules_SetArgs(&ac, &status);
  if (rc != REDISMODULE_OK) {
    QueryError_ReplyAndClear(ctx, &status);
  }
  return REDISMODULE_OK;
}

int ConfigCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  // Not bound to a specific index, so...
  RedisModule_AutoMemory(ctx);
  QueryError status = {0};

  // CONFIG <GET|SET> <NAME> [value]
  if (argc < 3) {
    return RedisModule_WrongArity(ctx);
  }
  const char *action = RedisModule_StringPtrLen(argv[1], NULL);
  const char *name = RedisModule_StringPtrLen(argv[2], NULL);
  if (!strcasecmp(action, "GET")) {
    RSConfig_DumpProto(&RSGlobalConfig, &RSGlobalConfigOptions, name, ctx, 0);
  } else if (!strcasecmp(action, "HELP")) {
    RSConfig_DumpProto(&RSGlobalConfig, &RSGlobalConfigOptions, name, ctx, 1);
  } else if (!strcasecmp(action, "SET")) {
    size_t offset = 3;  // Might be == argc. SetOption deals with it.
    if (RSConfig_SetOption(&RSGlobalConfig, &RSGlobalConfigOptions, name, argv, argc, &offset,
                           &status) == REDISMODULE_ERR) {
      RedisModule_ReplyWithSimpleString(ctx, QueryError_GetError(&status));
      return REDISMODULE_OK;
    }
    if (offset != argc) {
      RedisModule_ReplyWithSimpleString(ctx, "EXCESSARGS");
    } else {
      RedisModule_Log(ctx, "notice", "Successfully changed configuration for `%s`", name);
      RedisModule_ReplyWithSimpleString(ctx, "OK");
    }
    return REDISMODULE_OK;
  } else {
    RedisModule_ReplyWithSimpleString(ctx, "No such configuration action");
    return REDISMODULE_OK;
  }

  return REDISMODULE_OK;
}

void ShardingEvent(RedisModuleCtx *ctx, RedisModuleEvent eid, uint64_t subevent, void *data) {
  /**
   * On sharding event we need to do couple of things depends on the subevent given:
   *
   * 1. REDISMODULE_SUBEVENT_SHARDING_SLOT_RANGE_CHANGED
   *    On this event we know that the slot range changed and we might have data
   *    which are no longer belong to this shard, we must ignore it on searches
   *
   * 2. REDISMODULE_SUBEVENT_SHARDING_TRIMMING_STARTED
   *    This event tells us that the trimming process has started and keys will start to be
   *    deleted, we do not need to do anything on this event
   *
   * 3. REDISMODULE_SUBEVENT_SHARDING_TRIMMING_ENDED
   *    This event tells us that the trimming process has finished, we are not longer
   *    have data that are not belong to us and its safe to stop checking this on searches.
   */
  if (eid.id != REDISMODULE_EVENT_SHARDING) {
    RedisModule_Log(RSDummyContext, "warning", "Bad event given, ignored.");
    return;
  }

  switch (subevent) {
    case REDISMODULE_SUBEVENT_SHARDING_SLOT_RANGE_CHANGED:
      verifyDocumentSlotRange = true;
      break;
    case REDISMODULE_SUBEVENT_SHARDING_TRIMMING_STARTED:
      break;
    case REDISMODULE_SUBEVENT_SHARDING_TRIMMING_ENDED:
      verifyDocumentSlotRange = false;
      break;
    default:
      RedisModule_Log(RSDummyContext, "warning", "Bad subevent given, ignored.");
  }
}

#define RM_TRY(f, ...)                                                         \
  if (f(__VA_ARGS__) == REDISMODULE_ERR) {                                     \
    RedisModule_Log(ctx, "warning", "Could not run " #f "(" #__VA_ARGS__ ")"); \
    return REDISMODULE_ERR;                                                    \
  } else {                                                                     \
    RedisModule_Log(ctx, "verbose", "Successfully executed " #f);              \
  }

int RediSearch_InitModuleInternal(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  char *err;
  if (ReadConfig(argv, argc, &err) == REDISMODULE_ERR) {
    RedisModule_Log(ctx, "warning", "Invalid Configurations: %s", err);
    rm_free(err);
    return REDISMODULE_ERR;
  }
  if (RediSearch_Init(ctx, REDISEARCH_INIT_MODULE) != REDISMODULE_OK) {
    return REDISMODULE_ERR;
  }

  if (RedisModule_SubscribeToServerEvent) {
    // we have server events support, lets subscribe to relevan events.
    RedisModule_SubscribeToServerEvent(ctx, RedisModuleEvent_Sharding, ShardingEvent);
  }

  // register trie type
  RM_TRY(TrieType_Register, ctx);

  RM_TRY(IndexSpec_RegisterType, ctx);

  RM_TRY(RedisModule_CreateCommand, ctx, RS_ADD_CMD, RSAddDocumentCommand, "write deny-oom", 1, 1,
         1);

  RM_TRY(RedisModule_CreateCommand, ctx, RS_SAFEADD_CMD, RSSafeAddDocumentCommand, "write deny-oom",
         1, 1, 1);

  RM_TRY(RedisModule_CreateCommand, ctx, RS_SETPAYLOAD_CMD, SetPayloadCommand, "write deny-oom", 1,
         1, 1);

  RM_TRY(RedisModule_CreateCommand, ctx, RS_ADDHASH_CMD, RSAddHashCommand, "write deny-oom", 1, 1,
         1);

  RM_TRY(RedisModule_CreateCommand, ctx, RS_SAFEADDHASH_CMD, RSSafeAddHashCommand, "write deny-oom",
         1, 1, 1);

  RM_TRY(RedisModule_CreateCommand, ctx, RS_DEL_CMD, DeleteCommand, "write", 1, 1, 1);

  RM_TRY(RedisModule_CreateCommand, ctx, RS_SEARCH_CMD, RSSearchCommand, "readonly", 1, 1, 1);
  RM_TRY(RedisModule_CreateCommand, ctx, RS_AGGREGATE_CMD, RSAggregateCommand, "readonly", 1, 1, 1);

  RM_TRY(RedisModule_CreateCommand, ctx, RS_GET_CMD, GetSingleDocumentCommand, "readonly", 1, 1, 1);

  RM_TRY(RedisModule_CreateCommand, ctx, RS_MGET_CMD, GetDocumentsCommand, "readonly", 0, 0, -1);

  RM_TRY(RedisModule_CreateCommand, ctx, RS_CREATE_CMD, CreateIndexCommand, "write deny-oom", 1, 1,
         1);
  RM_TRY(RedisModule_CreateCommand, ctx, RS_CMD_PREFIX ".OPTIMIZE", OptimizeIndexCommand,
         "write deny-oom", 1, 1, 1);
  RM_TRY(RedisModule_CreateCommand, ctx, RS_DROP_CMD, DropIndexCommand, "write", 1, 1, 1);

  RM_TRY(RedisModule_CreateCommand, ctx, RS_INFO_CMD, IndexInfoCommand, "readonly", 1, 1, 1);

  RM_TRY(RedisModule_CreateCommand, ctx, RS_TAGVALS_CMD, TagValsCommand, "readonly", 1, 1, 1);

  RM_TRY(RedisModule_CreateCommand, ctx, RS_EXPLAIN_CMD, QueryExplainCommand, "readonly", 1, 1, 1);
  RM_TRY(RedisModule_CreateCommand, ctx, RS_EXPLAINCLI_CMD, QueryExplainCLICommand, "readonly", 1,
         1, 1);

  RM_TRY(RedisModule_CreateCommand, ctx, RS_SUGADD_CMD, RSSuggestAddCommand, "write deny-oom", 1, 1,
         1);

  RM_TRY(RedisModule_CreateCommand, ctx, RS_SUGDEL_CMD, RSSuggestDelCommand, "write", 1, 1, 1);

  RM_TRY(RedisModule_CreateCommand, ctx, RS_SUGLEN_CMD, RSSuggestLenCommand, "readonly", 1, 1, 1);

  RM_TRY(RedisModule_CreateCommand, ctx, RS_SUGGET_CMD, RSSuggestGetCommand, "readonly", 1, 1, 1);

  RM_TRY(RedisModule_CreateCommand, ctx, RS_CURSOR_CMD, RSCursorCommand, "readonly", 2, 2, 1);

  RM_TRY(RedisModule_CreateCommand, ctx, RS_SYNADD_CMD, SynAddCommand, "write", 1, 1, 1);

  RM_TRY(RedisModule_CreateCommand, ctx, RS_SYNUPDATE_CMD, SynUpdateCommand, "write", 1, 1, 1);

  RM_TRY(RedisModule_CreateCommand, ctx, RS_SYNFORCEUPDATE_CMD, SynForceUpdateCommand, "write", 1,
         1, 1);

  RM_TRY(RedisModule_CreateCommand, ctx, RS_SYNDUMP_CMD, SynDumpCommand, "readonly", 1, 1, 1);

  RM_TRY(RedisModule_CreateCommand, ctx, RS_ALTER_CMD, AlterIndexCommand, "write", 1, 1, 1);

  RM_TRY(RedisModule_CreateCommand, ctx, RS_DEBUG, DebugCommand, "readonly", 0, 0, 0);

  RM_TRY(RedisModule_CreateCommand, ctx, RS_SPELL_CHECK, SpellCheckCommand, "readonly", 1, 1, 1);

  RM_TRY(RedisModule_CreateCommand, ctx, RS_DICT_ADD, DictAddCommand, "readonly", 1, 1, 1);

  RM_TRY(RedisModule_CreateCommand, ctx, RS_DICT_DEL, DictDelCommand, "readonly", 1, 1, 1);

  RM_TRY(RedisModule_CreateCommand, ctx, RS_DICT_DUMP, DictDumpCommand, "readonly", 1, 1, 1);

  RM_TRY(RedisModule_CreateCommand, ctx, RS_CONFIG, ConfigCommand, "readonly", 1, 1, 1);

  RM_TRY(RedisModule_CreateCommand, ctx, RS_RULEADD, RuleAddCommand, "readonly", 1, 1, 1);

  RM_TRY(RedisModule_CreateCommand, ctx, RS_RULESET, RulesSetCommand, "readonly", 1, 1, 1);

#ifndef RS_COORDINATOR
  // we are running in a normal mode so we should raise cross slot error on alias commands
  RM_TRY(RedisModule_CreateCommand, ctx, RS_ALIASADD, AliasAddCommand, "readonly", 1, 2, 1);
  RM_TRY(RedisModule_CreateCommand, ctx, RS_ALIASUPDATE, AliasUpdateCommand, "readonly", 1, 2, 1);

  RM_TRY(RedisModule_CreateCommand, ctx, RS_ALIASDEL, AliasDelCommand, "readonly", 1, 1, 1);
#else
  // Cluster is manage outside of module lets trust it and not raise cross slot error.
  RM_TRY(RedisModule_CreateCommand, ctx, RS_ALIASADD, AliasAddCommand, "readonly", 2, 2, 1);
  RM_TRY(RedisModule_CreateCommand, ctx, RS_ALIASUPDATE, AliasUpdateCommand, "readonly", 2, 2, 1);

  RM_TRY(RedisModule_CreateCommand, ctx, RS_ALIASDEL, AliasDelCommand, "readonly", 0, 0, -1);
#endif
  return REDISMODULE_OK;
}

void __attribute__((destructor)) RediSearch_CleanupModule(void) {
  static int invoked = 0;
  if (!getenv("RS_GLOBAL_DTORS") || invoked || !RS_Initialized) {
    return;
  }
  invoked = 1;
  IndexSpec_CleanAll();
  SchemaRules_ShutdownGlobal();
  CursorList_Destroy(&RSCursors);
  Extensions_Free();
  StopWordList_FreeGlobals();
  FunctionRegistry_Free();
  mempool_free_global();
  IndexAlias_DestroyGlobal();
  Expr_AttributesDestroy();
  RedisModule_FreeThreadSafeContext(RSDummyContext);
}
