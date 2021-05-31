
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
#include "rmalloc.h"
#include "cursor.h"
#include "debug_commads.h"
#include "spell_check.h"
#include "dictionary.h"
#include "suggest.h"
#include "numeric_index.h"
#include "redisearch_api.h"
#include "alias.h"
#include "module.h"
#include "rwlock.h"
#include "info_command.h"
#include "rejson_api.h"

#define LOAD_INDEX(ctx, srcname, write)                                                     \
  ({                                                                                        \
    IndexSpec *sptmp = IndexSpec_Load(ctx, RedisModule_StringPtrLen(srcname, NULL), write); \
    if (sptmp == NULL) {                                                                    \
      return RedisModule_ReplyWithError(ctx, "Unknown index name");                         \
    }                                                                                       \
    sptmp;                                                                                  \
  })

/* FT.MGET {index} {key} ...
 * Get document(s) by their id.
 * Currentlt it just performs HGETALL, but it's a future proof alternative allowing us to later on
 * replace the internal representation of the documents.
 *
 * If referred docs are missing or not HASH keys, we simply reply with Null, but the result will
 * be an array the same size of the ids list
 */
int GetDocumentsCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  if (argc < 3) {
    return RedisModule_WrongArity(ctx);
  }

  RedisSearchCtx *sctx = NewSearchCtx(ctx, argv[1], true);
  if (sctx == NULL) {
    return RedisModule_ReplyWithError(ctx, "Unknown Index name");
  }

  const DocTable *dt = &sctx->spec->docs;
  RedisModule_ReplyWithArray(ctx, argc - 2);
  for (size_t i = 2; i < argc; i++) {

    if (DocTable_GetIdR(dt, argv[i]) == 0) {
      // Document does not exist in index; even though it exists in keyspace
      RedisModule_ReplyWithNull(ctx);
      continue;
    }
    Document_ReplyAllFields(ctx, sctx->spec, argv[i]);
  }

  SearchCtx_Free(sctx);

  return REDISMODULE_OK;
}

/* FT.GET {index} {key} ...
 * Get a single document by their id.
 * Currentlt it just performs HGETALL, but it's a future proof alternative allowing us to later on
 * replace the internal representation of the documents.
 *
 * If referred docs are missing or not HASH keys, we simply reply with Null
 */
int GetSingleDocumentCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  if (argc != 3) {
    return RedisModule_WrongArity(ctx);
  }

  RedisSearchCtx *sctx = NewSearchCtx(ctx, argv[1], true);
  if (sctx == NULL) {
    return RedisModule_ReplyWithError(ctx, "Unknown Index name");
  }

  if (DocTable_GetIdR(&sctx->spec->docs, argv[2]) == 0) {
    RedisModule_ReplyWithNull(ctx);
  } else {
    Document_ReplyAllFields(ctx, sctx->spec, argv[2]);
  }
  SearchCtx_Free(sctx);
  return REDISMODULE_OK;
}

int SpellCheckCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
#define DICT_INITIAL_SIZE 5
#define DEFAULT_LEV_DISTANCE 1
#define MAX_LEV_DISTANCE 100
#define STRINGIFY(s) #s
  if (argc < 3) {
    return RedisModule_WrongArity(ctx);
  }

  RedisModule_AutoMemory(ctx);
  RedisSearchCtx *sctx = NewSearchCtx(ctx, argv[1], true);
  if (sctx == NULL) {
    return RedisModule_ReplyWithError(ctx, "Unknown Index name");
  }

  QueryError status = {0};
  size_t len;
  const char *rawQuery = RedisModule_StringPtrLen(argv[2], &len);
  const char **includeDict = NULL, **excludeDict = NULL;
  RSSearchOptions opts = {0};
  QueryAST qast = {0};
  int rc = QAST_Parse(&qast, sctx, &opts, rawQuery, len, &status);

  if (rc != REDISMODULE_OK) {
    RedisModule_ReplyWithError(ctx, QueryError_GetError(&status));
    goto end;
  }

  includeDict = array_new(const char *, DICT_INITIAL_SIZE);
  excludeDict = array_new(const char *, DICT_INITIAL_SIZE);

  int distanceArgPos = 0;
  long long distance = DEFAULT_LEV_DISTANCE;
  if ((distanceArgPos = RMUtil_ArgExists("DISTANCE", argv, argc, 0))) {
    if (distanceArgPos + 1 >= argc) {
      RedisModule_ReplyWithError(ctx, "DISTANCE arg is given but no DISTANCE comes after");
      goto end;
    }
    if (RedisModule_StringToLongLong(argv[distanceArgPos + 1], &distance) != REDISMODULE_OK ||
        distance < 1 || distance > MAX_LEV_DISTANCE) {
      RedisModule_ReplyWithError(
          ctx, "bad distance given, distance must be a natural number between 1 to " STRINGIFY(
                   MAX_LEV_DISTANCE));
      goto end;
    }
  }  // LCOV_EXCL_LINE

  int nextPos = 0;
  while ((nextPos = RMUtil_ArgExists("TERMS", argv, argc, nextPos + 1))) {
    if (nextPos + 2 >= argc) {
      RedisModule_ReplyWithError(ctx, "TERM arg is given but no TERM params comes after");
      goto end;
    }
    const char *operation = RedisModule_StringPtrLen(argv[nextPos + 1], NULL);
    const char *dictName = RedisModule_StringPtrLen(argv[nextPos + 2], NULL);
    if (strcasecmp(operation, "INCLUDE") == 0) {
      includeDict = array_append(includeDict, (char *)dictName);
    } else if (strcasecmp(operation, "EXCLUDE") == 0) {
      excludeDict = array_append(excludeDict, (char *)dictName);
    } else {
      RedisModule_ReplyWithError(ctx, "bad format, exlude/include operation was not given");
      goto end;
    }
  }

  bool fullScoreInfo = false;
  if (RMUtil_ArgExists("FULLSCOREINFO", argv, argc, 0)) {
    fullScoreInfo = true;
  }

  SpellCheckCtx scCtx = {.sctx = sctx,
                         .includeDict = includeDict,
                         .excludeDict = excludeDict,
                         .distance = distance,
                         .fullScoreInfo = fullScoreInfo};

  SpellCheck_Reply(&scCtx, &qast);

end:
  QueryError_ClearError(&status);
  if (includeDict != NULL) {
    array_free(includeDict);
  }
  if (excludeDict != NULL) {
    array_free(excludeDict);
  }
  QAST_Destroy(&qast);
  SearchCtx_Free(sctx);
  return REDISMODULE_OK;
}

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
int RSProfileCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);

/* FT.DEL {index} {doc_id}
 *  Delete a document from the index. Returns 1 if the document was in the index, or 0 if not.
 *
 *  **NOTE**: This does not actually delete the document from the index, just marks it as deleted
 *  If DD (Delete Document) is set, we also delete the document.
 *  Since v2.0, document is deleted by default.
 */
int DeleteCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  RedisModule_AutoMemory(ctx);
  // allow 'DD' for back support and ignore it.
  if (argc < 3 || argc > 4) return RedisModule_WrongArity(ctx);
  IndexSpec *sp = IndexSpec_Load(ctx, RedisModule_StringPtrLen(argv[1], NULL), 1);
  if (sp == NULL) {
    return RedisModule_ReplyWithError(ctx, "Unknown Index name");
  }

  RedisModuleCallReply *rep = NULL;
  RedisModuleString *doc_id = argv[2];
  rep = RedisModule_Call(ctx, "DEL", "!s", doc_id);
  if (rep == NULL || RedisModule_CallReplyType(rep) != REDISMODULE_REPLY_INTEGER ||
      RedisModule_CallReplyInteger(rep) != 1) {
    return RedisModule_ReplyWithLongLong(ctx, 0);
  }
  return RedisModule_ReplyWithLongLong(ctx, 1);
}

/* FT.TAGVALS {idx} {field}
 * Return all the values of a tag field.
 * There is no sorting or paging, so be careful with high-cradinality tag fields */
int TagValsCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  // at least one field, and number of field/text args must be even
  if (argc != 3) {
    return RedisModule_WrongArity(ctx);
  }

  RedisModule_AutoMemory(ctx);
  RedisSearchCtx *sctx = NewSearchCtx(ctx, argv[1], true);
  if (sctx == NULL) {
    return RedisModule_ReplyWithError(ctx, "Unknown Index name");
  }

  size_t len;
  const char *field = RedisModule_StringPtrLen(argv[2], &len);
  const FieldSpec *sp = IndexSpec_GetField(sctx->spec, field, len);
  if (!sp) {
    RedisModule_ReplyWithError(ctx, "No such field");
    goto cleanup;
  }
  if (!FIELD_IS(sp, INDEXFLD_T_TAG)) {
    RedisModule_ReplyWithError(ctx, "Not a tag field");
    goto cleanup;
  }

  TagIndex *idx = TagIndex_Open(sctx, TagIndex_FormatName(sctx, field), 0, NULL);
  if (!idx) {
    RedisModule_ReplyWithArray(ctx, 0);
    goto cleanup;
  }

  TagIndex_SerializeValues(idx, ctx);

cleanup:
  SearchCtx_Free(sctx);
  return REDISMODULE_OK;
}
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

    - NOOFFSETS: If set, we do not store term offsets for documents (saves memory, does not allow
      exact searches)

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

  IndexSpec *sp = IndexSpec_CreateNew(ctx, argv, argc, &status);
  if (sp == NULL) {
    RedisModule_ReplyWithError(ctx, QueryError_GetError(&status));
    QueryError_ClearError(&status);
    return REDISMODULE_OK;
  }

  /*
   * We replicate CreateIfNotExists command for replica of support.
   * On replica of the destination will get the ft.create command from
   * all the src shards and not need to recreate it.
   */
  RedisModule_Replicate(ctx, RS_CREATE_IF_NX_CMD, "v", argv + 1, argc - 1);

  return RedisModule_ReplyWithSimpleString(ctx, "OK");
}

int CreateIndexIfNotExistsCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  // at least one field, the SCHEMA keyword, and number of field/text args must be even
  if (argc < 5) {
    return RedisModule_WrongArity(ctx);
  }

  const char *specName = RedisModule_StringPtrLen(argv[1], NULL);
  if (dictFetchValue(specDict_g, specName)) {
    return RedisModule_ReplyWithSimpleString(ctx, "OK");
  }

  return CreateIndexCommand(ctx, argv, argc);
}

/*
 * FT.DROP <index> [KEEPDOCS]
 * FT.DROPINDEX <index> [DD]
 * Deletes index and possibly all the keys associated with the index.
 * If no other data is on the redis instance, this is equivalent to FLUSHDB,
 * apart from the fact that the index specification is not deleted.
 *
 * FT.DROP, deletes all keys by default. If KEEPDOCS exists, we do not delete the actual docs
 * FT.DROPINDEX, keeps all keys by default. If DD exists, we delete the actual docs
 */
int DropIndexCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  // at least one field, and number of field/text args must be even
  if (argc < 2 || argc > 3) {
    return RedisModule_WrongArity(ctx);
  }

  RedisModule_AutoMemory(ctx);
  IndexSpec *sp = IndexSpec_Load(ctx, RedisModule_StringPtrLen(argv[1], NULL), 0);
  if (sp == NULL) {
    return RedisModule_ReplyWithError(ctx, "Unknown Index name");
  }

  int delDocs;
  if (RMUtil_StringEqualsCaseC(argv[0], "FT.DROP") ||
      RMUtil_StringEqualsCaseC(argv[0], "_FT.DROP")) {
    delDocs = 1;
    if (argc == 3 && RMUtil_StringEqualsCaseC(argv[2], "KEEPDOCS")) {
      delDocs = 0;
    }
  } else {  // FT.DROPINDEX
    delDocs = 0;
    if (argc == 3 && RMUtil_StringEqualsCaseC(argv[2], "DD")) {
      delDocs = 1;
    }
  }

  RedisSearchCtx sctx = SEARCH_CTX_STATIC(ctx, sp);
  Redis_DropIndex(&sctx, delDocs);

  if (RMUtil_StringEqualsCaseC(argv[0], "FT.DROP") ||
      RMUtil_StringEqualsCaseC(argv[0], "_FT.DROP")) {
    RedisModule_Replicate(ctx, RS_DROP_IF_X_CMD, "v", argv + 1, argc - 1);
  } else {
    RedisModule_Replicate(ctx, RS_DROP_INDEX_IF_X_CMD, "v", argv + 1, argc - 1);
  }

  return RedisModule_ReplyWithSimpleString(ctx, "OK");
}

int DropIfExistsIndexCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  // at least one field, and number of field/text args must be even
  if (argc < 2 || argc > 3) {
    return RedisModule_WrongArity(ctx);
  }

  IndexSpec *sp = IndexSpec_Load(ctx, RedisModule_StringPtrLen(argv[1], NULL), 0);
  if (!sp) {
    return RedisModule_ReplyWithSimpleString(ctx, "OK");
  }
  RedisModuleString *oldCommand = argv[0];
  if (RMUtil_StringEqualsCaseC(argv[0], RS_DROP_IF_X_CMD)) {
    argv[0] = RedisModule_CreateString(ctx, RS_DROP_CMD, strlen(RS_DROP_CMD));
  } else {
    argv[0] = RedisModule_CreateString(ctx, RS_DROP_INDEX_CMD, strlen(RS_DROP_INDEX_CMD));
  }
  int ret = DropIndexCommand(ctx, argv, argc);
  RedisModule_FreeString(ctx, argv[0]);
  argv[0] = oldCommand;
  return ret;
}

/**
 * FT.SYNADD <index> <term1> <term2> ...
 *
 * Add a synonym group to the given index. The synonym data structure is compose of synonyms
 * groups. Each Synonym group has a unique id. The SYNADD command creates a new synonym group with
 * the given terms and return its id.
 */
int SynAddCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  RedisModule_ReplyWithError(ctx, "No longer suppoted, use FT.SYNUPDATE");
  return REDISMODULE_OK;
}

/**
 * FT.SYNUPDATE <index> <group id> [SKIPINITIALSCAN] <term1> <term2> ...
 *
 * Update an already existing synonym group with the given terms.
 * It can be used only to add new terms to a synonym group.
 * Returns `OK` on success.
 */
int SynUpdateCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  if (argc < 4) return RedisModule_WrongArity(ctx);

  const char *id = RedisModule_StringPtrLen(argv[2], NULL);

  IndexSpec *sp = IndexSpec_Load(ctx, RedisModule_StringPtrLen(argv[1], NULL), 0);
  if (!sp) {
    RedisModule_ReplyWithError(ctx, "Unknown index name");
    return REDISMODULE_OK;
  }

  bool initialScan = true;
  int offset = 3;
  int loc = RMUtil_ArgIndex(SPEC_SKIPINITIALSCAN_STR, &argv[3], 1);
  if (loc == 0) {  // if doesn't exist, `-1` is returned
    initialScan = false;
    offset = 4;
  }

  IndexSpec_InitializeSynonym(sp);

  SynonymMap_UpdateRedisStr(sp->smap, argv + offset, argc - offset, id);

  if (initialScan) {
    IndexSpec_ScanAndReindex(ctx, sp);
  }

  RedisModule_ReplyWithSimpleString(ctx, "OK");

  RedisModule_ReplicateVerbatim(ctx);

  return REDISMODULE_OK;
}

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
int SynDumpCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  if (argc != 2) return RedisModule_WrongArity(ctx);

  IndexSpec *sp = IndexSpec_Load(ctx, RedisModule_StringPtrLen(argv[1], NULL), 0);
  if (!sp) {
    RedisModule_ReplyWithError(ctx, "Unknown index name");
    return REDISMODULE_OK;
  }

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
    RedisModule_ReplyWithArray(ctx, array_len(t_data->groupIds));
    for (size_t j = 0; j < array_len(t_data->groupIds); ++j) {
      // do not return the ~
      RedisModule_ReplyWithStringBuffer(ctx, t_data->groupIds[j] + 1,
                                        strlen(t_data->groupIds[j] + 1));
    }
  }

  rm_free(terms_data);

  return REDISMODULE_OK;
}

static int AlterIndexInternalCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc,
                                     bool ifnx) {
  ArgsCursor ac = {0};
  ArgsCursor_InitRString(&ac, argv + 1, argc - 1);

  // Need at least <cmd> <index> <subcommand> <args...>
  RedisModule_AutoMemory(ctx);

  if (argc < 5) {
    return RedisModule_WrongArity(ctx);
  }
  QueryError status = {0};

  const char *ixname = AC_GetStringNC(&ac, NULL);
  IndexSpec *sp = IndexSpec_Load(ctx, ixname, 1);
  if (!sp) {
    return RedisModule_ReplyWithError(ctx, "Unknown index name");
  }

  bool initialScan = true;
  if (AC_AdvanceIfMatch(&ac, SPEC_SKIPINITIALSCAN_STR)) {
    initialScan = false;
  }

  if (!AC_AdvanceIfMatch(&ac, "SCHEMA")) {
    return RedisModule_ReplyWithError(ctx, "ALTER must be followed by SCHEMA");
  }

  if (!AC_AdvanceIfMatch(&ac, "ADD")) {
    return RedisModule_ReplyWithError(ctx, "Unknown action passed to ALTER SCHEMA");
  }

  if (!AC_NumRemaining(&ac)) {
    return RedisModule_ReplyWithError(ctx, "No fields provided");
  }

  if (ifnx) {
    const char *fieldName;
    size_t fieldNameSize;

    int rv = AC_GetString(&ac, &fieldName, &fieldNameSize, AC_F_NOADVANCE);
    if (IndexSpec_GetField(sp, fieldName, fieldNameSize)) {
      RedisModule_Replicate(ctx, RS_ALTER_IF_NX_CMD, "v", argv + 1, argc - 1);
      return RedisModule_ReplyWithSimpleString(ctx, "OK");
    }
  }
  IndexSpec_AddFields(sp, ctx, &ac, initialScan, &status);

  if (QueryError_HasError(&status)) {
    return QueryError_ReplyAndClear(ctx, &status);
  } else {
    RedisModule_Replicate(ctx, RS_ALTER_IF_NX_CMD, "v", argv + 1, argc - 1);
    return RedisModule_ReplyWithSimpleString(ctx, "OK");
  }
}

/* FT.ALTER */
int AlterIndexIfNXCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  return AlterIndexInternalCommand(ctx, argv, argc, true);
}

int AlterIndexCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  return AlterIndexInternalCommand(ctx, argv, argc, false);
}

static int aliasAddCommon(RedisModuleCtx *ctx, RedisModuleString **argv, int argc,
                          QueryError *error, bool skipIfExists) {
  ArgsCursor ac = {0};
  ArgsCursor_InitRString(&ac, argv + 1, argc - 1);
  IndexLoadOptions loadOpts = {
      .name = {.rstring = argv[2]},
      .flags = INDEXSPEC_LOAD_NOALIAS | INDEXSPEC_LOAD_KEYLESS | INDEXSPEC_LOAD_KEY_RSTRING};
  IndexSpec *sptmp = IndexSpec_LoadEx(ctx, &loadOpts);
  if (!sptmp) {
    QueryError_SetError(error, QUERY_ENOINDEX, "Unknown index name (or name is an alias itself)");
    return REDISMODULE_ERR;
  }
  const char *alias = RedisModule_StringPtrLen(argv[1], NULL);
  IndexSpec *sp = IndexAlias_Get(alias);
  if (skipIfExists && sptmp == sp) {
    return REDISMODULE_OK;
  }
  return IndexAlias_Add(alias, sptmp, 0, error);
}

static int AliasAddCommandCommon(RedisModuleCtx *ctx, RedisModuleString **argv, int argc,
                                 bool ifNx) {
  if (argc != 3) {
    return RedisModule_WrongArity(ctx);
  }
  QueryError e = {0};
  if (aliasAddCommon(ctx, argv, argc, &e, ifNx) != REDISMODULE_OK) {
    return QueryError_ReplyAndClear(ctx, &e);
  } else {
    RedisModule_Replicate(ctx, RS_ALIASADD_IF_NX, "v", argv + 1, argc - 1);
    return RedisModule_ReplyWithSimpleString(ctx, "OK");
  }
}

static int AliasAddCommandIfNX(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  return AliasAddCommandCommon(ctx, argv, argc, true);
}

// FT.ALIASADD <NAME> <TARGET>
static int AliasAddCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  return AliasAddCommandCommon(ctx, argv, argc, false);
}

static int AliasDelCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  if (argc != 2) {
    return RedisModule_WrongArity(ctx);
  }
  IndexLoadOptions lOpts = {.name = {.rstring = argv[1]},
                            .flags = INDEXSPEC_LOAD_KEYLESS | INDEXSPEC_LOAD_KEY_RSTRING};
  IndexSpec *sp = IndexSpec_LoadEx(ctx, &lOpts);
  if (!sp) {
    return RedisModule_ReplyWithError(ctx, "Alias does not exist");
  }
  QueryError status = {0};
  if (IndexAlias_Del(RedisModule_StringPtrLen(argv[1], NULL), sp, 0, &status) != REDISMODULE_OK) {
    return QueryError_ReplyAndClear(ctx, &status);
  } else {
    RedisModule_Replicate(ctx, RS_ALIASDEL_IF_EX, "v", argv + 1, argc - 1);
    return RedisModule_ReplyWithSimpleString(ctx, "OK");
  }
}

static int AliasDelIfExCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  if (argc != 2) {
    return RedisModule_WrongArity(ctx);
  }
  IndexLoadOptions lOpts = {.name = {.rstring = argv[1]},
                            .flags = INDEXSPEC_LOAD_KEYLESS | INDEXSPEC_LOAD_KEY_RSTRING};
  IndexSpec *sp = IndexSpec_LoadEx(ctx, &lOpts);
  if (!sp) {
    return RedisModule_ReplyWithSimpleString(ctx, "OK");
  }
  return AliasDelCommand(ctx, argv, argc);
}

static int AliasUpdateCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  if (argc != 3) {
    return RedisModule_WrongArity(ctx);
  }

  QueryError status = {0};
  IndexLoadOptions lOpts = {.name = {.rstring = argv[1]},
                            .flags = INDEXSPEC_LOAD_KEYLESS | INDEXSPEC_LOAD_KEY_RSTRING};
  IndexSpec *spOrig = IndexSpec_LoadEx(ctx, &lOpts);
  if (spOrig) {
    if (IndexAlias_Del(RedisModule_StringPtrLen(argv[1], NULL), spOrig, 0, &status) !=
        REDISMODULE_OK) {
      return QueryError_ReplyAndClear(ctx, &status);
    }
  }
  if (aliasAddCommon(ctx, argv, argc, &status, false) != REDISMODULE_OK) {
    // Add back the previous index.. this shouldn't fail
    if (spOrig) {
      QueryError e2 = {0};
      const char *alias = RedisModule_StringPtrLen(argv[1], NULL);
      IndexAlias_Add(alias, spOrig, 0, &e2);
      QueryError_ClearError(&e2);
    }
    return QueryError_ReplyAndClear(ctx, &status);
  } else {
    RedisModule_ReplicateVerbatim(ctx);
    return RedisModule_ReplyWithSimpleString(ctx, "OK");
  }
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
      return QueryError_ReplyAndClear(ctx, &status);
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

int IndexList(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  if (argc != 1) {
    return RedisModule_WrongArity(ctx);
  }

  RedisModule_ReplyWithArray(ctx, dictSize(specDict_g));

  dictIterator *iter = dictGetIterator(specDict_g);
  dictEntry *entry = NULL;
  while ((entry = dictNext(iter))) {
    IndexSpec *spec = dictGetVal(entry);
    RedisModule_ReplyWithCString(ctx, spec->name);
  }
  dictReleaseIterator(iter);

  return REDISMODULE_OK;
}

#define RM_TRY(f, ...)                                                         \
  if (f(__VA_ARGS__) == REDISMODULE_ERR) {                                     \
    RedisModule_Log(ctx, "warning", "Could not run " #f "(" #__VA_ARGS__ ")"); \
    return REDISMODULE_ERR;                                                    \
  } else {                                                                     \
    RedisModule_Log(ctx, "verbose", "Successfully executed " #f);              \
  }

Version supportedVersion = {
    .majorVersion = 6,
    .minorVersion = 0,
    .patchVersion = 0,
};

static void GetRedisVersion() {
  RedisModuleCtx *ctx = RedisModule_GetThreadSafeContext(NULL);
  RedisModuleCallReply *reply = RedisModule_Call(ctx, "info", "c", "server");
  if (!reply) {
    // could not get version, it can only happened when running the tests.
    // set redis version to supported version.
    redisVersion = supportedVersion;
    RedisModule_FreeThreadSafeContext(ctx);
    return;
  }
  RedisModule_Assert(RedisModule_CallReplyType(reply) == REDISMODULE_REPLY_STRING);
  size_t len;
  const char *replyStr = RedisModule_CallReplyStringPtr(reply, &len);

  int n = sscanf(replyStr, "# Server\nredis_version:%d.%d.%d", &redisVersion.majorVersion,
                 &redisVersion.minorVersion, &redisVersion.patchVersion);

  RedisModule_Assert(n == 3);

  rlecVersion.majorVersion = -1;
  rlecVersion.minorVersion = -1;
  rlecVersion.patchVersion = -1;
  rlecVersion.buildVersion = -1;
  char *enterpriseStr = strstr(replyStr, "rlec_version:");
  if (enterpriseStr) {
    n = sscanf(enterpriseStr, "rlec_version:%d.%d.%d-%d", &rlecVersion.majorVersion,
               &rlecVersion.minorVersion, &rlecVersion.buildVersion, &rlecVersion.patchVersion);
    if (n != 4) {
      RedisModule_Log(NULL, "warning", "Could not extract enterprise version");
    }
  }

  RedisModule_FreeCallReply(reply);

  isCrdt = true;
  reply = RedisModule_Call(ctx, "CRDT.CONFIG", "cc", "GET", "active-gc");
  if (!reply || RedisModule_CallReplyType(reply) == REDISMODULE_REPLY_ERROR) {
    isCrdt = false;
  }

  if (reply) {
    RedisModule_FreeCallReply(reply);
  }

  RedisModule_FreeThreadSafeContext(ctx);
}

static inline int IsEnterprise() {
  return rlecVersion.majorVersion != -1;
}

int CheckSupportedVestion() {
  if (CompareVestions(redisVersion, supportedVersion) < 0) {
    return REDISMODULE_ERR;
  }
  return REDISMODULE_OK;
}

int RediSearch_InitModuleInternal(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  char *err;

  legacySpecRules = dictCreate(&dictTypeHeapStrings, NULL);

  if (ReadConfig(argv, argc, &err) == REDISMODULE_ERR) {
    RedisModule_Log(ctx, "warning", "Invalid Configurations: %s", err);
    rm_free(err);
    return REDISMODULE_ERR;
  }

  GetRedisVersion();

  RedisModule_Log(ctx, "notice", "Redis version found by RedisSearch : %d.%d.%d - %s",
                  redisVersion.majorVersion, redisVersion.minorVersion, redisVersion.patchVersion,
                  IsEnterprise() ? (isCrdt ? "enterprise-crdt" : "enterprise") : "oss");
  if (IsEnterprise()) {
    RedisModule_Log(ctx, "notice", "Redis Enterprise version found by RedisSearch : %d.%d.%d-%d",
                    rlecVersion.majorVersion, rlecVersion.minorVersion, rlecVersion.patchVersion,
                    rlecVersion.buildVersion);
  }

  if (CheckSupportedVestion() != REDISMODULE_OK) {
    RedisModule_Log(ctx, "warning",
                    "Redis version is too old, please upgrade to redis %d.%d.%d and above.",
                    supportedVersion.majorVersion, supportedVersion.minorVersion,
                    supportedVersion.patchVersion);

    // On memory sanity check do not failed the start
    // because our redis version there is old.
    if (!getenv("RS_GLOBAL_DTORS")) {
      return REDISMODULE_ERR;
    }
  }

  if (RediSearch_Init(ctx, REDISEARCH_INIT_MODULE) != REDISMODULE_OK) {
    return REDISMODULE_ERR;
  }

  // register trie type
  RM_TRY(DictRegister, ctx);

  RM_TRY(TrieType_Register, ctx);

  RM_TRY(IndexSpec_RegisterType, ctx);

  RM_TRY(TagIndex_RegisterType, ctx);

  RM_TRY(InvertedIndex_RegisterType, ctx);

  RM_TRY(NumericIndexType_Register, ctx);

#ifndef RS_COORDINATOR
// on a none coordinator version (for RS light/lite) we want to raise cross slot if
// the index and the document do not go to the same shard
#define INDEX_ONLY_CMD_ARGS 1, 1, 1
#define INDEX_DOC_CMD_ARGS 1, 2, 1
#else
// on coordinator we do not want to raise a move error so we do not specify any key
#define INDEX_ONLY_CMD_ARGS 0, 0, 0
#define INDEX_DOC_CMD_ARGS 2, 2, 1
#endif

  RM_TRY(RedisModule_CreateCommand, ctx, RS_INDEX_LIST_CMD, IndexList, "readonly", 0, 0, 0);

  RM_TRY(RedisModule_CreateCommand, ctx, RS_ADD_CMD, RSAddDocumentCommand, "write deny-oom",
         INDEX_DOC_CMD_ARGS);

#ifdef RS_CLUSTER_ENTERPRISE
  // on enterprise cluster we need to keep the _ft.safeadd/_ft.del command
  // to be able to replicate from an old RediSearch version.
  // If this is the light version then the _ft.safeadd/_ft.del does not exists
  // and we will get the normal ft.safeadd/ft.del command.
  RM_TRY(RedisModule_CreateCommand, ctx, LEGACY_RS_SAFEADD_CMD, RSSafeAddDocumentCommand,
         "write deny-oom", INDEX_DOC_CMD_ARGS);
  RM_TRY(RedisModule_CreateCommand, ctx, LEGACY_RS_DEL_CMD, DeleteCommand, "write",
         INDEX_DOC_CMD_ARGS);
#endif

  RM_TRY(RedisModule_CreateCommand, ctx, RS_SAFEADD_CMD, RSSafeAddDocumentCommand, "write deny-oom",
         INDEX_DOC_CMD_ARGS);

  RM_TRY(RedisModule_CreateCommand, ctx, RS_DEL_CMD, DeleteCommand, "write", INDEX_DOC_CMD_ARGS);

  RM_TRY(RedisModule_CreateCommand, ctx, RS_SEARCH_CMD, RSSearchCommand, "readonly",
         INDEX_ONLY_CMD_ARGS);
  RM_TRY(RedisModule_CreateCommand, ctx, RS_AGGREGATE_CMD, RSAggregateCommand, "readonly",
         INDEX_ONLY_CMD_ARGS);

  RM_TRY(RedisModule_CreateCommand, ctx, RS_GET_CMD, GetSingleDocumentCommand, "readonly",
         INDEX_DOC_CMD_ARGS);

#ifndef RS_COORDINATOR
  // in case not coordinator is defined, all docs and index name should go to the same slot
  RM_TRY(RedisModule_CreateCommand, ctx, RS_MGET_CMD, GetDocumentsCommand, "readonly", 1, -1, 1);
#else
  // in case coordinator is defined, do not force cross slot validation
  RM_TRY(RedisModule_CreateCommand, ctx, RS_MGET_CMD, GetDocumentsCommand, "readonly", 0, 0, 0);
#endif

  RM_TRY(RedisModule_CreateCommand, ctx, RS_CREATE_CMD, CreateIndexCommand, "write deny-oom",
         INDEX_ONLY_CMD_ARGS);
  RM_TRY(RedisModule_CreateCommand, ctx, RS_CREATE_IF_NX_CMD, CreateIndexIfNotExistsCommand,
         "write deny-oom", INDEX_ONLY_CMD_ARGS);

  RM_TRY(RedisModule_CreateCommand, ctx, RS_DROP_CMD, DropIndexCommand, "write",
         INDEX_ONLY_CMD_ARGS);
  RM_TRY(RedisModule_CreateCommand, ctx, RS_DROP_INDEX_CMD, DropIndexCommand, "write",
         INDEX_ONLY_CMD_ARGS);
  RM_TRY(RedisModule_CreateCommand, ctx, RS_DROP_IF_X_CMD, DropIfExistsIndexCommand, "write",
         INDEX_ONLY_CMD_ARGS);
  RM_TRY(RedisModule_CreateCommand, ctx, RS_DROP_INDEX_IF_X_CMD, DropIfExistsIndexCommand, "write",
         INDEX_ONLY_CMD_ARGS);

  RM_TRY(RedisModule_CreateCommand, ctx, RS_INFO_CMD, IndexInfoCommand, "readonly",
         INDEX_ONLY_CMD_ARGS);

  RM_TRY(RedisModule_CreateCommand, ctx, RS_TAGVALS_CMD, TagValsCommand, "readonly",
         INDEX_ONLY_CMD_ARGS);

  RM_TRY(RedisModule_CreateCommand, ctx, RS_PROFILE_CMD, RSProfileCommand, "readonly",
         INDEX_ONLY_CMD_ARGS);
  RM_TRY(RedisModule_CreateCommand, ctx, RS_EXPLAIN_CMD, QueryExplainCommand, "readonly",
         INDEX_ONLY_CMD_ARGS);
  RM_TRY(RedisModule_CreateCommand, ctx, RS_EXPLAINCLI_CMD, QueryExplainCLICommand, "readonly",
         INDEX_ONLY_CMD_ARGS);

  RM_TRY(RedisModule_CreateCommand, ctx, RS_SUGADD_CMD, RSSuggestAddCommand, "write deny-oom", 1, 1,
         1);

  RM_TRY(RedisModule_CreateCommand, ctx, RS_SUGDEL_CMD, RSSuggestDelCommand, "write", 1, 1, 1);

  RM_TRY(RedisModule_CreateCommand, ctx, RS_SUGLEN_CMD, RSSuggestLenCommand, "readonly", 1, 1, 1);

  RM_TRY(RedisModule_CreateCommand, ctx, RS_SUGGET_CMD, RSSuggestGetCommand, "readonly", 1, 1, 1);

#ifndef RS_COORDINATOR
  RM_TRY(RedisModule_CreateCommand, ctx, RS_CURSOR_CMD, RSCursorCommand, "readonly", 2, 2, 1);
#else
  // we do not want to raise a move error on cluster with coordinator
  RM_TRY(RedisModule_CreateCommand, ctx, RS_CURSOR_CMD, RSCursorCommand, "readonly", 0, 0, 0);
#endif

  // todo: what to do with this?
  RM_TRY(RedisModule_CreateCommand, ctx, RS_SYNADD_CMD, SynAddCommand, "write",
         INDEX_ONLY_CMD_ARGS);

  RM_TRY(RedisModule_CreateCommand, ctx, RS_SYNUPDATE_CMD, SynUpdateCommand, "write",
         INDEX_ONLY_CMD_ARGS);

  RM_TRY(RedisModule_CreateCommand, ctx, RS_SYNDUMP_CMD, SynDumpCommand, "readonly",
         INDEX_ONLY_CMD_ARGS);

  RM_TRY(RedisModule_CreateCommand, ctx, RS_ALTER_CMD, AlterIndexCommand, "write",
         INDEX_ONLY_CMD_ARGS);
  RM_TRY(RedisModule_CreateCommand, ctx, RS_ALTER_IF_NX_CMD, AlterIndexIfNXCommand, "write",
         INDEX_ONLY_CMD_ARGS);

  RM_TRY(RedisModule_CreateCommand, ctx, RS_DEBUG, DebugCommand, "readonly", 0, 0, 0);

  RM_TRY(RedisModule_CreateCommand, ctx, RS_SPELL_CHECK, SpellCheckCommand, "readonly",
         INDEX_ONLY_CMD_ARGS);

  RM_TRY(RedisModule_CreateCommand, ctx, RS_DICT_ADD, DictAddCommand, "readonly", 0, 0, 0);

  RM_TRY(RedisModule_CreateCommand, ctx, RS_DICT_DEL, DictDelCommand, "readonly", 0, 0, 0);

  RM_TRY(RedisModule_CreateCommand, ctx, RS_DICT_DUMP, DictDumpCommand, "readonly", 0, 0, 0);

  RM_TRY(RedisModule_CreateCommand, ctx, RS_CONFIG, ConfigCommand, "readonly", 0, 0, 0);

// alias is a special case, we can not use the INDEX_ONLY_CMD_ARGS/INDEX_DOC_CMD_ARGS macros
#ifndef RS_COORDINATOR
  // we are running in a normal mode so we should raise cross slot error on alias commands
  RM_TRY(RedisModule_CreateCommand, ctx, RS_ALIASADD, AliasAddCommand, "readonly", 1, 2, 1);
  RM_TRY(RedisModule_CreateCommand, ctx, RS_ALIASADD_IF_NX, AliasAddCommandIfNX, "readonly", 1, 2,
         1);
  RM_TRY(RedisModule_CreateCommand, ctx, RS_ALIASUPDATE, AliasUpdateCommand, "readonly", 1, 2, 1);

  RM_TRY(RedisModule_CreateCommand, ctx, RS_ALIASDEL, AliasDelCommand, "readonly", 1, 1, 1);
  RM_TRY(RedisModule_CreateCommand, ctx, RS_ALIASDEL_IF_EX, AliasDelIfExCommand, "readonly", 1, 1,
         1);
#else
  // Cluster is manage outside of module lets trust it and not raise cross slot error.
  RM_TRY(RedisModule_CreateCommand, ctx, RS_ALIASADD, AliasAddCommand, "readonly", 0, 0, 0);
  RM_TRY(RedisModule_CreateCommand, ctx, RS_ALIASADD_IF_NX, AliasAddCommandIfNX, "readonly", 0, 0,
         0);
  RM_TRY(RedisModule_CreateCommand, ctx, RS_ALIASUPDATE, AliasUpdateCommand, "readonly", 0, 0, 0);

  RM_TRY(RedisModule_CreateCommand, ctx, RS_ALIASDEL, AliasDelCommand, "readonly", 0, 0, 0);
  RM_TRY(RedisModule_CreateCommand, ctx, RS_ALIASDEL_IF_EX, AliasDelIfExCommand, "readonly", 0, 0,
         0);
#endif
  return REDISMODULE_OK;
}

void ReindexPool_ThreadPoolDestroy();

void __attribute__((destructor)) RediSearch_CleanupModule(void) {
  if (getenv("RS_GLOBAL_DTORS")) {  // used in sanitizer
    static int invoked = 0;
    if (invoked || !RS_Initialized) {
      return;
    }
    invoked = 1;
    CursorList_Destroy(&RSCursors);
    Extensions_Free();
    StopWordList_FreeGlobals();
    FunctionRegistry_Free();
    mempool_free_global();
    ConcurrentSearch_ThreadPoolDestroy();
    ReindexPool_ThreadPoolDestroy();
    GC_ThreadPoolDestroy();
    IndexAlias_DestroyGlobal();
    freeGlobalAddStrings();
    SchemaPrefixes_Free();
    RedisModule_FreeThreadSafeContext(RSDummyContext);
    Dictionary_Free();
    RediSearch_LockDestory();
  }
}
