/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#define REDISMODULE_MAIN

#include <stdio.h>
#include <string.h>
#include <sys/param.h>
#include <time.h>

#include "commands.h"
#include "document.h"
#include "tag_index.h"
#include "index.h"
#include "triemap.h"
#include "query.h"
#include "redis_index.h"
#include "redismodule.h"
#include "rmutil/strings.h"
#include "rmutil/util.h"
#include "rmutil/args.h"
#include "spec.h"
#include "util/logging.h"
#include "util/workers.h"
#include "util/references.h"
#include "config.h"
#include "aggregate/aggregate.h"
#include "rmalloc.h"
#include "cursor.h"
#include "debug_commands.h"
#include "spell_check.h"
#include "dictionary.h"
#include "suggest.h"
#include "numeric_index.h"
#include "redisearch_api.h"
#include "alias.h"
#include "module.h"
#include "rwlock.h"
#include "info/info_command.h"
#include "rejson_api.h"
#include "geometry/geometry_api.h"
#include "reply.h"
#include "resp3.h"
#include "coord/rmr/rmr.h"
#include "hiredis/async.h"
#include "coord/rmr/reply.h"
#include "coord/rmr/redis_cluster.h"
#include "coord/rmr/redise.h"
#include "coord/config.h"
#include "coord/debug_commands.h"
#include "libuv/include/uv.h"
#include "profile.h"
#include "coord/dist_profile.h"
#include "coord/cluster_spell_check.h"
#include "coord/info_command.h"
#include "info/global_stats.h"
#include "util/units.h"
#include "fast_float/fast_float_strtod.h"
#include "aggregate/aggregate_debug.h"
#include "info/info_redis/threads/current_thread.h"
#include "info/info_redis/threads/main_thread.h"
#include "hybrid/hybrid_exec.h"

#define VERIFY_ACL(ctx, idxR)                                                                     \
  do {                                                                                                      \
    const char *idxName = RedisModule_StringPtrLen(idxR, NULL);                                             \
    IndexLoadOptions lopts =                                                                                \
      {.nameC = idxName, .flags = INDEXSPEC_LOAD_NOCOUNTERINC};                                             \
    StrongRef spec_ref = IndexSpec_LoadUnsafeEx(&lopts);                                                    \
    IndexSpec *sp = StrongRef_Get(spec_ref);                                                                \
    if (!sp) {                                                                                              \
      return RedisModule_ReplyWithErrorFormat(ctx, "%s: no such index", idxName);                           \
    }                                                                                                       \
    if (!ACLUserMayAccessIndex(ctx, sp)) {                                                                  \
      return RedisModule_ReplyWithError(ctx, NOPERM_ERR);                                                   \
    }                                                                                                       \
  } while(0);


extern RSConfig RSGlobalConfig;

extern RedisModuleCtx *RSDummyContext;

redisearch_thpool_t *depleterPool = NULL;

static int DIST_THREADPOOL = -1;

// Number of shards in the cluster. Hint we can read and modify from the main thread
size_t NumShards = 0;

// Strings returned by CONFIG GET functions
RedisModuleString *config_ext_load = NULL;
RedisModuleString *config_friso_ini = NULL;

/* ======================= DEBUG ONLY DECLARATIONS ======================= */
static void DEBUG_DistSearchCommandHandler(void* pd);
/* ======================= DEBUG ONLY DECLARATIONS ======================= */

static inline bool SearchCluster_Ready() {
  return NumShards != 0;
}

bool ACLUserMayAccessIndex(RedisModuleCtx *ctx, IndexSpec *sp) {
  if (RedisModule_ACLCheckKeyPrefixPermissions == NULL) {
    // API not supported -> allow access (ACL will not be enforced).
    return true;
  }
  RedisModuleString *user_name = RedisModule_GetCurrentUserName(ctx);
  RedisModuleUser *user = RedisModule_GetModuleUserFromUserName(user_name);

  if (!user) {
    RedisModule_Log(ctx, "verbose", "No user found");
    RedisModule_FreeString(ctx, user_name);
    return false;
  }

  bool ret = true;
  HiddenUnicodeString **prefixes = sp->rule->prefixes;
  RedisModuleString *prefix;
  for (uint i = 0; i < array_len(prefixes); i++) {
    prefix = HiddenUnicodeString_CreateRedisModuleString(prefixes[i], ctx);
    if (RedisModule_ACLCheckKeyPrefixPermissions(user, prefix, REDISMODULE_CMD_KEY_ACCESS) != REDISMODULE_OK) {
      ret = false;
      RedisModule_FreeString(ctx, prefix);
      break;
    }
    RedisModule_FreeString(ctx, prefix);
  }

  RedisModule_FreeModuleUser(user);
  RedisModule_FreeString(ctx, user_name);
  return ret;
}

// Validates ACL key-space permissions w.r.t the given index spec for Redis
// Enterprise environments only.
static inline bool checkEnterpriseACL(RedisModuleCtx *ctx, IndexSpec *sp) {
  return !IsEnterprise() || ACLUserMayAccessIndex(ctx, sp);
}

// Returns true if the current context has permission to execute debug commands
// See redis docs regarding `enable-debug-command` for more information
// Falls back to true when the redis version is below the one we started
// supporting this feature
bool debugCommandsEnabled(RedisModuleCtx *ctx) {
  int flags = RedisModule_GetContextFlags(ctx);
  int allFlags = RedisModule_GetContextFlagsAll();
  return (!(allFlags & REDISMODULE_CTX_FLAGS_DEBUG_ENABLED)) || (flags & REDISMODULE_CTX_FLAGS_DEBUG_ENABLED);
}

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

  CurrentThread_SetIndexSpec(sctx->spec->own_ref);

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

  CurrentThread_ClearIndexSpec();

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

  if (!checkEnterpriseACL(ctx, sctx->spec)) {
    SearchCtx_Free(sctx);
    return RedisModule_ReplyWithError(ctx, NOPERM_ERR);
  }

  CurrentThread_SetIndexSpec(sctx->spec->own_ref);

  if (DocTable_GetIdR(&sctx->spec->docs, argv[2]) == 0) {
    RedisModule_ReplyWithNull(ctx);
  } else {
    Document_ReplyAllFields(ctx, sctx->spec, argv[2]);
  }
  SearchCtx_Free(sctx);
  CurrentThread_ClearIndexSpec();
  return REDISMODULE_OK;
}

#define __STRINGIFY(x) #x
#define STRINGIFY(x) __STRINGIFY(x)

int SpellCheckCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {

#define DICT_INITIAL_SIZE 5
#define DEFAULT_LEV_DISTANCE 1
#define MAX_LEV_DISTANCE 4

  if (argc < 3) {
    return RedisModule_WrongArity(ctx);
  }

  int argvOffset = 3;
  unsigned int dialect = RSGlobalConfig.requestConfigParams.dialectVersion;
  int dialectArgIndex = RMUtil_ArgExists("DIALECT", argv, argc, argvOffset);
  if(dialectArgIndex > 0) {
    dialectArgIndex++;
    ArgsCursor ac;
    ArgsCursor_InitRString(&ac, argv+dialectArgIndex, argc-dialectArgIndex);
    QueryError status = {0};
    if(parseDialect(&dialect, &ac, &status) != REDISMODULE_OK) {
      RedisModule_ReplyWithError(ctx, QueryError_GetUserError(&status));
      QueryError_ClearError(&status);
      return REDISMODULE_OK;
    }
  }

  RedisSearchCtx *sctx = NewSearchCtx(ctx, argv[1], true);
  if (sctx == NULL) {
    return RedisModule_ReplyWithError(ctx, "Unknown Index name");
  }
  CurrentThread_SetIndexSpec(sctx->spec->own_ref);
  QueryError status = {0};
  size_t len;
  const char *rawQuery = RedisModule_StringPtrLen(argv[2], &len);
  const char **includeDict = NULL, **excludeDict = NULL;
  RSSearchOptions opts = {0};
  QueryAST qast = {0};
  int rc = QAST_Parse(&qast, sctx, &opts, rawQuery, len, dialect, &status);

  if (rc != REDISMODULE_OK) {
    RedisModule_ReplyWithError(ctx, QueryError_GetUserError(&status));
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
      array_append(includeDict, (char *)dictName);
    } else if (strcasecmp(operation, "EXCLUDE") == 0) {
      array_append(excludeDict, (char *)dictName);
    } else {
      RedisModule_ReplyWithError(ctx, "bad format, exclude/include operation was not given");
      goto end;
    }
  }

  SET_DIALECT(sctx->spec->used_dialects, dialect);
  SET_DIALECT(RSGlobalStats.totalStats.used_dialects, dialect);

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
  CurrentThread_ClearIndexSpec();
  SearchCtx_Free(sctx);
  return REDISMODULE_OK;
}

char *RS_GetExplainOutput(RedisModuleCtx *ctx, RedisModuleString **argv, int argc,
                          QueryError *status);

static int queryExplainCommon(RedisModuleCtx *ctx, RedisModuleString **argv, int argc,
                              int newlinesAsElements) {
  if (argc < 3) {
    return RedisModule_WrongArity(ctx);
  }
  VERIFY_ACL(ctx, argv[1])

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
      char *line = isUnsafeForSimpleString(curLine) ? escapeSimpleString(curLine): curLine;
      RedisModule_ReplyWithSimpleString(ctx, line);
      if (line != curLine) rm_free(line);
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
  // allow 'DD' for back support and ignore it.
  if (argc < 3 || argc > 4) return RedisModule_WrongArity(ctx);
  StrongRef ref = IndexSpec_LoadUnsafe(RedisModule_StringPtrLen(argv[1], NULL));
  IndexSpec *sp = StrongRef_Get(ref);
  if (sp == NULL) {
    return RedisModule_ReplyWithError(ctx, "Unknown Index name");
  }

  // On Enterprise, we validate ACL permission to the index
  if (!checkEnterpriseACL(ctx, sp)) {
    return RedisModule_ReplyWithError(ctx, NOPERM_ERR);
  }

  CurrentThread_SetIndexSpec(ref);

  RedisModuleCallReply *rep = NULL;
  RedisModuleString *doc_id = argv[2];
  rep = RedisModule_Call(ctx, "DEL", "!s", doc_id);
  if (rep == NULL || RedisModule_CallReplyType(rep) != REDISMODULE_REPLY_INTEGER ||
      RedisModule_CallReplyInteger(rep) != 1) {
    RedisModule_ReplyWithLongLong(ctx, 0);
  } else {
    RedisModule_ReplyWithLongLong(ctx, 1);
  }

  if (rep) {
    RedisModule_FreeCallReply(rep);
  }

  CurrentThread_ClearIndexSpec();

  return REDISMODULE_OK;
}

/* FT.TAGVALS {idx} {field}
 * Return all the values of a tag field.
 * There is no sorting or paging, so be careful with high-cradinality tag fields */

int TagValsCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  // at least one field, and number of field/text args must be even
  if (argc != 3) {
    return RedisModule_WrongArity(ctx);
  }

  RedisSearchCtx *sctx = NewSearchCtx(ctx, argv[1], true);
  if (sctx == NULL) {
    return RedisModule_ReplyWithError(ctx, "Unknown Index name");
  }

  CurrentThread_SetIndexSpec(sctx->spec->own_ref);

  size_t len;
  const char *field = RedisModule_StringPtrLen(argv[2], &len);
  const FieldSpec *fs = IndexSpec_GetFieldWithLength(sctx->spec, field, len);
  if (!fs) {
    RedisModule_ReplyWithError(ctx, "No such field");
    goto cleanup;
  }
  if (!FIELD_IS(fs, INDEXFLD_T_TAG)) {
    RedisModule_ReplyWithError(ctx, "Not a tag field");
    goto cleanup;
  }

  RedisModuleString *rstr = TagIndex_FormatName(sctx->spec, fs->fieldName);
  TagIndex *idx = TagIndex_Open(sctx->spec, rstr, DONT_CREATE_INDEX);
  RedisModule_FreeString(ctx, rstr);
  if (!idx) {
    RedisModule_ReplyWithSetOrArray(ctx, 0);
    goto cleanup;
  }

  TagIndex_SerializeValues(idx, ctx);

cleanup:
  CurrentThread_ClearIndexSpec();
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
    RedisModule_ReplyWithError(ctx, QueryError_GetUserError(&status));
    QueryError_ClearError(&status);
    return REDISMODULE_OK;
  }

  /*
   * We replicate CreateIfNotExists command for replica of support.
   * On replica of the destination will get the ft.create command from
   * all the src shards and not need to recreate it.
   */
  RedisModule_Replicate(ctx, RS_CREATE_IF_NX_CMD, "v", argv + 1, (size_t)argc - 1);

  return RedisModule_ReplyWithSimpleString(ctx, "OK");
}

int CreateIndexIfNotExistsCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  // at least one field, the SCHEMA keyword, and number of field/text args must be even
  if (argc < 5) {
    return RedisModule_WrongArity(ctx);
  }

  const char *rawSpecName = RedisModule_StringPtrLen(argv[1], NULL);
  HiddenString *specName = NewHiddenString(rawSpecName, strlen(rawSpecName), false);
  const bool found = dictFetchValue(specDict_g, specName);
  HiddenString_Free(specName, false);
  if (found) {
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

  const char* spec_name = RedisModule_StringPtrLen(argv[1], NULL);
  StrongRef global_ref = IndexSpec_LoadUnsafe(spec_name);
  IndexSpec *sp = StrongRef_Get(global_ref);
  if (!sp) {
    return RedisModule_ReplyWithError(ctx, "Unknown Index name");
  }

  if (!checkEnterpriseACL(ctx, sp)) {
    return RedisModule_ReplyWithError(ctx, NOPERM_ERR);
  }

  bool dropCommand = RMUtil_StringEqualsCaseC(argv[0], "FT.DROP") ||
               RMUtil_StringEqualsCaseC(argv[0], "_FT.DROP");
  bool delDocs = dropCommand;
  if (argc == 3){
    if (RMUtil_StringEqualsCaseC(argv[2], "_FORCEKEEPDOCS")) {
      delDocs = false;
    } else if (dropCommand && RMUtil_StringEqualsCaseC(argv[2], "KEEPDOCS")) {
      delDocs = false;
    } else if (!dropCommand && RMUtil_StringEqualsCaseC(argv[2], "DD")) {
      delDocs = true;
    } else {
      return RedisModule_ReplyWithError(ctx, "Unknown argument");
    }
  }

  CurrentThread_SetIndexSpec(global_ref);

  if((delDocs || sp->flags & Index_Temporary)) {
    // We take a strong reference to the index, so it will not be freed
    // and we can still use it's doc table to delete the keys.
    StrongRef own_ref = StrongRef_Clone(global_ref);
    // We remove the index from the globals first, so it will not be found by the
    // delete key notification callbacks.
    IndexSpec_RemoveFromGlobals(global_ref, false);

    DocTable *dt = &sp->docs;
    DOCTABLE_FOREACH(dt, Redis_DeleteKeyC(ctx, dmd->keyPtr));

    // Return call's references
    CurrentThread_ClearIndexSpec();
    StrongRef_Release(own_ref);
  } else {
    // If we don't delete the docs, we just remove the index from the global dict
    IndexSpec_RemoveFromGlobals(global_ref, true);
  }

  RedisModule_Replicate(ctx, RS_DROP_INDEX_IF_X_CMD, "sc", argv[1], "_FORCEKEEPDOCS");

  return RedisModule_ReplyWithSimpleString(ctx, "OK");
}

int DropIfExistsIndexCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  // at least one field, and number of field/text args must be even
  if (argc < 2 || argc > 3) {
    return RedisModule_WrongArity(ctx);
  }

  StrongRef ref = IndexSpec_LoadUnsafe(RedisModule_StringPtrLen(argv[1], NULL));
  IndexSpec *sp = StrongRef_Get(ref);
  if (!sp) {
    return RedisModule_ReplyWithSimpleString(ctx, "OK");
  }

  if (!checkEnterpriseACL(ctx, sp)) {
    return RedisModule_ReplyWithError(ctx, NOPERM_ERR);
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
  RedisModule_ReplyWithError(ctx, "No longer supported, use FT.SYNUPDATE");
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

  StrongRef ref = IndexSpec_LoadUnsafe(RedisModule_StringPtrLen(argv[1], NULL));
  IndexSpec *sp = StrongRef_Get(ref);
  if (!sp) {
    return RedisModule_ReplyWithError(ctx, "Unknown index name");
  }

  if (!checkEnterpriseACL(ctx, sp)) {
    return RedisModule_ReplyWithError(ctx, NOPERM_ERR);
  }

  CurrentThread_SetIndexSpec(ref);

  bool initialScan = true;
  int offset = 3;
  int loc = RMUtil_ArgIndex(SPEC_SKIPINITIALSCAN_STR, &argv[3], 1);
  if (loc == 0) {  // if doesn't exist, `-1` is returned
    initialScan = false;
    offset = 4;
  }

  RedisSearchCtx sctx = SEARCH_CTX_STATIC(ctx, sp);
  RedisSearchCtx_LockSpecWrite(&sctx);

  IndexSpec_InitializeSynonym(sp);

  SynonymMap_UpdateRedisStr(sp->smap, argv + offset, argc - offset, id);

  if (initialScan) {
    IndexSpec_ScanAndReindex(ctx, ref);
  }

  RedisSearchCtx_UnlockSpec(&sctx);
  CurrentThread_ClearIndexSpec();

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

  const char *idx = RedisModule_StringPtrLen(argv[1], NULL);
  StrongRef ref = IndexSpec_LoadUnsafe(idx);
  IndexSpec *sp = StrongRef_Get(ref);
  if (!sp) {
    return RedisModule_ReplyWithErrorFormat(ctx, "%s: no such index", idx);
  }

  // Verify ACL keys permission
  if (!ACLUserMayAccessIndex(ctx, sp)) {
    return RedisModule_ReplyWithError(ctx, NOPERM_ERR);
  }

  CurrentThread_SetIndexSpec(ref);

  if (!sp->smap) {
    return RedisModule_ReplyWithMapOrArray(ctx, 0, false);
  }

  RedisSearchCtx sctx = SEARCH_CTX_STATIC(ctx, sp);
  RedisSearchCtx_LockSpecRead(&sctx);

  size_t size;
  TermData **terms_data = SynonymMap_DumpAllTerms(sp->smap, &size);

  RedisModule_ReplyWithMapOrArray(ctx, size * 2, true);

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

  RedisSearchCtx_UnlockSpec(&sctx);
  CurrentThread_ClearIndexSpec();

  rm_free(terms_data);
  return REDISMODULE_OK;
}

static int AlterIndexInternalCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc,
                                     bool ifnx) {
  ArgsCursor ac = {0};
  ArgsCursor_InitRString(&ac, argv + 1, argc - 1);

  // Need at least <cmd> <index> <subcommand> <args...>

  if (argc < 5) {
    return RedisModule_WrongArity(ctx);
  }
  QueryError status = {0};

  const char *ixname = AC_GetStringNC(&ac, NULL);
  StrongRef ref = IndexSpec_LoadUnsafe(ixname);
  IndexSpec *sp = StrongRef_Get(ref);
  if (!sp) {
    return RedisModule_ReplyWithError(ctx, "Unknown index name");
  }

  if (!checkEnterpriseACL(ctx, sp)) {
    return RedisModule_ReplyWithError(ctx, NOPERM_ERR);
  }

  RedisSearchCtx sctx = SEARCH_CTX_STATIC(ctx, sp);

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

  CurrentThread_SetIndexSpec(ref);

  if (ifnx) {
    const char *fieldName;
    size_t fieldNameSize;

    AC_GetString(&ac, &fieldName, &fieldNameSize, AC_F_NOADVANCE);
    RedisSearchCtx_LockSpecRead(&sctx);
    const FieldSpec *field_exists = IndexSpec_GetFieldWithLength(sp, fieldName, fieldNameSize);
    RedisSearchCtx_UnlockSpec(&sctx);

    if (field_exists) {
      RedisModule_Replicate(ctx, RS_ALTER_IF_NX_CMD, "v", argv + 1, (size_t)argc - 1);
      CurrentThread_ClearIndexSpec();
      return RedisModule_ReplyWithSimpleString(ctx, "OK");
    }
  }
  RedisSearchCtx_LockSpecWrite(&sctx);
  IndexSpec_AddFields(ref, sp, ctx, &ac, initialScan, &status);

  // if adding the fields has failed we return without updating statistics.
  if (QueryError_HasError(&status)) {
    RedisSearchCtx_UnlockSpec(&sctx);
    CurrentThread_ClearIndexSpec();
    return QueryError_ReplyAndClear(ctx, &status);
  }

  RedisSearchCtx_UnlockSpec(&sctx);
  CurrentThread_ClearIndexSpec();

  RedisModule_Replicate(ctx, RS_ALTER_IF_NX_CMD, "v", argv + 1, (size_t)argc - 1);
  return RedisModule_ReplyWithSimpleString(ctx, "OK");

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
      .nameR = argv[2],
      .flags = INDEXSPEC_LOAD_NOALIAS | INDEXSPEC_LOAD_KEY_RSTRING};
  StrongRef ref = IndexSpec_LoadUnsafeEx(&loadOpts);
  IndexSpec *sp = StrongRef_Get(ref);
  if (!sp) {
    QueryError_SetError(error, QUERY_ENOINDEX, "Unknown index name (or name is an alias itself)");
    return REDISMODULE_ERR;
  }

  if (!checkEnterpriseACL(ctx, sp)) {
    QueryError_SetError(error, QUERY_EGENERIC, NOPERM_ERR);
    return REDISMODULE_ERR;
  }

  CurrentThread_SetIndexSpec(ref);

  size_t length = 0;
  const char *rawAlias = RedisModule_StringPtrLen(argv[1], &length);
  HiddenString *alias = NewHiddenString(rawAlias, length, false);
  if (dictFetchValue(specDict_g, alias)) {
    HiddenString_Free(alias, false);
    QueryError_SetCode(error, QUERY_EALIASCONFLICT);
    CurrentThread_ClearIndexSpec();
    return REDISMODULE_ERR;
  }
  StrongRef alias_ref = IndexAlias_Get(alias);
  int rc = REDISMODULE_OK;
  if (!skipIfExists || !StrongRef_Equals(alias_ref, ref)) {
    rc = IndexAlias_Add(alias, ref, 0, error);
  }
  HiddenString_Free(alias, false);
  CurrentThread_ClearIndexSpec();
  return rc;
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
    RedisModule_Replicate(ctx, RS_ALIASADD_IF_NX, "v", argv + 1, (size_t)argc - 1);
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
  IndexLoadOptions lOpts = {.nameR = argv[1],
                            .flags = INDEXSPEC_LOAD_KEY_RSTRING};
  StrongRef ref = IndexSpec_LoadUnsafeEx(&lOpts);
  IndexSpec *sp = StrongRef_Get(ref);
  if (!sp) {
    return RedisModule_ReplyWithError(ctx, "Alias does not exist");
  }

  // On Enterprise, we validate ACL permission to the index
  if (!checkEnterpriseACL(ctx, sp)) {
    return RedisModule_ReplyWithError(ctx, NOPERM_ERR);
  }

  CurrentThread_SetIndexSpec(ref);

  size_t length = 0;
  const char *rawAlias = RedisModule_StringPtrLen(argv[1], &length);
  HiddenString *alias = NewHiddenString(rawAlias, length, false);
  QueryError status = {0};
  const int rc = IndexAlias_Del(alias, ref, 0, &status);
  HiddenString_Free(alias, false);
  if (rc != REDISMODULE_OK) {
    CurrentThread_ClearIndexSpec();
    return QueryError_ReplyAndClear(ctx, &status);
  } else {
    RedisModule_Replicate(ctx, RS_ALIASDEL_IF_EX, "v", argv + 1, (size_t)argc - 1);
    CurrentThread_ClearIndexSpec();
    return RedisModule_ReplyWithSimpleString(ctx, "OK");
  }
}

static int AliasDelIfExCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  if (argc != 2) {
    return RedisModule_WrongArity(ctx);
  }
  IndexLoadOptions lOpts = {.nameR = argv[1],
                            .flags = INDEXSPEC_LOAD_KEY_RSTRING};
  StrongRef ref = IndexSpec_LoadUnsafeEx(&lOpts);
  if (!StrongRef_Get(ref)) {
    return RedisModule_ReplyWithSimpleString(ctx, "OK");
  }
  return AliasDelCommand(ctx, argv, argc);
}

static int AliasUpdateCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  if (argc != 3) {
    return RedisModule_WrongArity(ctx);
  }

  QueryError status = {0};
  IndexLoadOptions lOpts = {.nameR = argv[1],
                            .flags = INDEXSPEC_LOAD_KEY_RSTRING};
  StrongRef Orig_ref = IndexSpec_LoadUnsafeEx(&lOpts);
  IndexSpec *spOrig = StrongRef_Get(Orig_ref);
  size_t length = 0;
  const char* rawAlias = RedisModule_StringPtrLen(argv[1], &length);
  HiddenString *alias = NewHiddenString(rawAlias, length, false);
  if (spOrig) {
    // On Enterprise, we validate ACL permission to the index
    if (!checkEnterpriseACL(ctx, spOrig)) {
      HiddenString_Free(alias, false);
      return RedisModule_ReplyWithError(ctx, NOPERM_ERR);
    }
    CurrentThread_SetIndexSpec(Orig_ref);
    if (IndexAlias_Del(alias, Orig_ref, 0, &status) != REDISMODULE_OK) {
      HiddenString_Free(alias, false);
      CurrentThread_ClearIndexSpec();
      return QueryError_ReplyAndClear(ctx, &status);
    }
    CurrentThread_ClearIndexSpec();
  }
  int rc = 0;
  if (aliasAddCommon(ctx, argv, argc, &status, false) != REDISMODULE_OK) {
    // Add back the previous index. this shouldn't fail
    if (spOrig) {
      QueryError e2 = {0};
      IndexAlias_Add(alias, Orig_ref, 0, &e2);
      QueryError_ClearError(&e2);
    }
    rc = QueryError_ReplyAndClear(ctx, &status);
  } else {
    RedisModule_ReplicateVerbatim(ctx);
    rc = RedisModule_ReplyWithSimpleString(ctx, "OK");
  }
  HiddenString_Free(alias, false);
  return rc;
}

int ConfigCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  // Not bound to a specific index, so...
  QueryError status = {0};

  // CONFIG <GET|SET> <NAME> [value]
  if (argc < 3) {
    return RedisModule_WrongArity(ctx);
  }

  RedisModule_Reply _reply = RedisModule_NewReply(ctx), *reply = &_reply;

  const char *action = RedisModule_StringPtrLen(argv[1], NULL);
  const char *name = RedisModule_StringPtrLen(argv[2], NULL);
  if (!strcasecmp(action, "GET")) {
    LogWarningDeprecatedFTConfig(ctx, "GET", name);
    RSConfig_DumpProto(&RSGlobalConfig, &RSGlobalConfigOptions, name, reply, false);
  } else if (!strcasecmp(action, "HELP")) {
    RSConfig_DumpProto(&RSGlobalConfig, &RSGlobalConfigOptions, name, reply, true);
  } else if (!strcasecmp(action, "SET")) {
    LogWarningDeprecatedFTConfig(ctx, "SET", name);
    size_t offset = 3;  // Might be == argc. SetOption deals with it.
    int rc = RSConfig_SetOption(&RSGlobalConfig, &RSGlobalConfigOptions, name, argv, argc,
                                &offset, &status);
    if (rc == REDISMODULE_ERR) {
      RedisModule_Reply_QueryError(reply, &status);
      QueryError_ClearError(&status);
      RedisModule_EndReply(reply);
      return REDISMODULE_OK;
    }
    if (offset != argc) {
      RedisModule_Reply_SimpleString(reply, "EXCESSARGS");
    } else {
      RedisModule_Log(ctx, "notice", "Successfully changed configuration for `%s`", name);
      RedisModule_Reply_SimpleString(reply, "OK");
    }
  } else {
    RedisModule_Reply_SimpleString(reply, "No such configuration action");
  }

  RedisModule_EndReply(reply);
  return REDISMODULE_OK;
}

int IndexList(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  if (argc > 2) {
    return RedisModule_WrongArity(ctx);
  }

  RedisModule_Reply _reply = RedisModule_NewReply(ctx);
  Indexes_List(&_reply, false);
  return REDISMODULE_OK;
}

#define RM_TRY_F(f, ...)                                                       \
  if (f(__VA_ARGS__) == REDISMODULE_ERR) {                                     \
    RedisModule_Log(ctx, "warning", "Could not run " #f "(" #__VA_ARGS__ ")"); \
    return REDISMODULE_ERR;                                                    \
  } else {                                                                     \
    RedisModule_Log(ctx, "verbose", "Successfully executed " #f);              \
  }

Version supportedVersion = {
    .majorVersion = 7,
    .minorVersion = 1,
    .patchVersion = 0,
};

static void GetRedisVersion(RedisModuleCtx *ctx) {
  RedisModuleCallReply *reply = RedisModule_Call(ctx, "info", "c", "server");
  if (!reply) {
    // could not get version, it can only happened when running the tests.
    // set redis version to supported version.
    redisVersion = supportedVersion;
    return;
  }
  RS_ASSERT(RedisModule_CallReplyType(reply) == REDISMODULE_REPLY_STRING);
  size_t len;
  const char *replyStr = RedisModule_CallReplyStringPtr(reply, &len);

  int n = sscanf(replyStr, "# Server\nredis_version:%d.%d.%d", &redisVersion.majorVersion,
                 &redisVersion.minorVersion, &redisVersion.patchVersion);

  RS_ASSERT(n == 3);

  rlecVersion.majorVersion = -1;
  rlecVersion.minorVersion = -1;
  rlecVersion.patchVersion = -1;
  rlecVersion.buildVersion = -1;
  char *enterpriseStr = strstr(replyStr, "rlec_version:");
  if (enterpriseStr) {
    n = sscanf(enterpriseStr, "rlec_version:%d.%d.%d-%d", &rlecVersion.majorVersion,
               &rlecVersion.minorVersion, &rlecVersion.buildVersion, &rlecVersion.patchVersion);
    if (n != 4) {
      RedisModule_Log(ctx, "warning", "Could not extract enterprise version");
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

}

void GetFormattedRedisVersion(char *buf, size_t len) {
    snprintf(buf, len, "%d.%d.%d - %s",
             redisVersion.majorVersion, redisVersion.minorVersion, redisVersion.patchVersion,
             IsEnterprise() ? (isCrdt ? "enterprise-crdt" : "enterprise") : "oss");
}

void GetFormattedRedisEnterpriseVersion(char *buf, size_t len) {
    snprintf(buf, len, "%d.%d.%d-%d",
             rlecVersion.majorVersion, rlecVersion.minorVersion, rlecVersion.patchVersion,
             rlecVersion.buildVersion);
}

int IsMaster() {
  if (RedisModule_GetContextFlags(RSDummyContext) & REDISMODULE_CTX_FLAGS_MASTER) {
    return 1;
  } else {
    return 0;
  }
}

bool IsEnterprise() {
  return rlecVersion.majorVersion != -1;
}

int CheckSupportedVestion() {
  if (CompareVersions(redisVersion, supportedVersion) < 0) {
    return REDISMODULE_ERR;
  }
  return REDISMODULE_OK;
}

// Creates a command and registers it to its corresponding ACL categories
static int RMCreateSearchCommand(RedisModuleCtx *ctx, const char *name,
                  RedisModuleCmdFunc callback, const char *flags, int firstkey,
                  int lastkey, int keystep, const char *aclCategories,
                  bool internalCommand) {
  int rc = REDISMODULE_OK;
  char *internalFlags;
  char *categories;

  if (internalCommand) {
    // Do not register to ANY ACL command category
    categories = "";
    // We don't want the user running internal commands. For that, we mark the
    // command internal on OSS, or exclude it from the proxy on Enterprise.
    if (IsEnterprise()) {
        rm_asprintf(&internalFlags, "%s %s", flags, CMD_PROXY_FILTERED);
    } else {
        rm_asprintf(&internalFlags, "%s %s", flags, CMD_INTERNAL);
    }
  } else {
    // Flags are not enhanced.
    internalFlags = (char *)flags;
    // Register non-internal commands to the `search` ACL category.
    rm_asprintf(&categories, strcmp(aclCategories, "") != 0 ? "%s %s" : "%.0s%s", aclCategories, SEARCH_ACL_CATEGORY);
  }

  if (RedisModule_CreateCommand(ctx, name, callback, internalFlags, firstkey, lastkey, keystep) == REDISMODULE_ERR) {
    RedisModule_Log(ctx, "warning", "Could not create command: %s", name);
    rc = REDISMODULE_ERR;
    goto cleanup;
  }

  RedisModuleCommand *command = RedisModule_GetCommand(ctx, name);
  if (!command) {
    RedisModule_Log(ctx, "warning", "Could not find command: %s", name);
    rc = REDISMODULE_ERR;
    goto cleanup;
  }

  if (RedisModule_SetCommandACLCategories(command, categories) == REDISMODULE_ERR) {
    RedisModule_Log(ctx, "warning", "Failed to set ACL categories for command: %s. Got error code: %d", name, errno);
    rc = REDISMODULE_ERR;
  }

cleanup:
  if (internalCommand) {
    rm_free(internalFlags);
  } else {
    rm_free(categories);
  }
  return rc;
}

int RSHybridCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  return hybridCommandHandler(ctx, argv, argc);
}

int RediSearch_InitModuleInternal(RedisModuleCtx *ctx) {
  GetRedisVersion(ctx);

  // Prepare thread local storage for storing active queries/cursors
  int error = MainThread_InitBlockedQueries();
  if (error) {
    RedisModule_Log(ctx, "warning", "Failed to initialize thread local data, error: %d", error);
    return REDISMODULE_ERR;
  }

  char ver[64];
  GetFormattedRedisVersion(ver, sizeof(ver));
  RedisModule_Log(ctx, "notice", "Redis version found by RedisSearch : %s", ver);
  if (IsEnterprise()) {
    GetFormattedRedisEnterpriseVersion(ver, sizeof(ver));
    RedisModule_Log(ctx, "notice", "Redis Enterprise version found by RedisSearch : %s", ver);
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
  RM_TRY_F(DictRegister, ctx);

  RM_TRY_F(TrieType_Register, ctx);

  RM_TRY_F(IndexSpec_RegisterType, ctx);

  RM_TRY_F(TagIndex_RegisterType, ctx);

  RM_TRY_F(InvertedIndex_RegisterType, ctx);

  RM_TRY_F(NumericIndexType_Register, ctx);


// With coordinator we do not want to raise a move error for index commands so we do not specify
// any key.
#define INDEX_ONLY_CMD_ARGS 0, 0, 0
#define INDEX_DOC_CMD_ARGS 2, 2, 1

  // Create the `search` ACL command category
  if (RedisModule_AddACLCategory(ctx, SEARCH_ACL_CATEGORY) == REDISMODULE_ERR) {
      RedisModule_Log(ctx, "warning", "Could not add " SEARCH_ACL_CATEGORY " ACL category, errno: %d\n", errno);
      return REDISMODULE_ERR;
  }

  RM_TRY(RMCreateSearchCommand(ctx, RS_INDEX_LIST_CMD, IndexList, "readonly",
         0, 0, 0, "slow admin", false))

  RM_TRY(RMCreateSearchCommand(ctx, RS_ADD_CMD, RSAddDocumentCommand,
         "write deny-oom", INDEX_DOC_CMD_ARGS, "write", !IsEnterprise()))

#ifdef RS_CLUSTER_ENTERPRISE
  // on enterprise cluster we need to keep the _ft.safeadd/_ft.del command
  // to be able to replicate from an old RediSearch version.
  // If this is the light version then the _ft.safeadd/_ft.del does not exist
  // and we will get the normal ft.safeadd/ft.del command.
  RM_TRY(RMCreateSearchCommand(ctx, LEGACY_RS_SAFEADD_CMD, RSAddDocumentCommand,
         "write deny-oom", INDEX_DOC_CMD_ARGS, "write", true))
  RM_TRY(RMCreateSearchCommand(ctx, LEGACY_RS_DEL_CMD, DeleteCommand,
         "write", INDEX_DOC_CMD_ARGS, "write", true))
#endif

  RM_TRY(RMCreateSearchCommand(ctx, RS_SAFEADD_CMD, RSAddDocumentCommand,
        "write deny-oom", INDEX_DOC_CMD_ARGS, "write", false))

  RM_TRY(RMCreateSearchCommand(ctx, RS_DEL_CMD, DeleteCommand, "write",
         INDEX_DOC_CMD_ARGS, "write", !IsEnterprise()))

  RM_TRY(RMCreateSearchCommand(ctx, RS_SEARCH_CMD, RSSearchCommand, "readonly",
         INDEX_ONLY_CMD_ARGS, "", true))

  RM_TRY(RMCreateSearchCommand(ctx, RS_HYBRID_CMD, RSHybridCommand, "readonly",
         INDEX_ONLY_CMD_ARGS, "", true))

  RM_TRY(RMCreateSearchCommand(ctx, RS_AGGREGATE_CMD, RSAggregateCommand,
         "readonly", INDEX_ONLY_CMD_ARGS, "read", true))

  RM_TRY(RMCreateSearchCommand(ctx, RS_GET_CMD, GetSingleDocumentCommand,
         "readonly", INDEX_DOC_CMD_ARGS, "read", !IsEnterprise()))

  // Do not force cross slot validation since coordinator will handle it.
  RM_TRY(RMCreateSearchCommand(ctx, RS_MGET_CMD, GetDocumentsCommand,
         "readonly", 0, 0, 0, "read", true))

  RM_TRY(RMCreateSearchCommand(ctx, RS_CREATE_CMD, CreateIndexCommand,
         "write deny-oom", INDEX_ONLY_CMD_ARGS, "", !IsEnterprise()))

  RM_TRY(RMCreateSearchCommand(ctx, RS_CREATE_IF_NX_CMD, CreateIndexIfNotExistsCommand,
         "write deny-oom", INDEX_ONLY_CMD_ARGS, "", !IsEnterprise()))

  RM_TRY(RMCreateSearchCommand(ctx, RS_DROP_CMD, DropIndexCommand, "write",
         INDEX_ONLY_CMD_ARGS, "write slow dangerous", !IsEnterprise()))

  RM_TRY(RMCreateSearchCommand(ctx, RS_DROP_INDEX_CMD, DropIndexCommand,
         "write", INDEX_ONLY_CMD_ARGS, "write slow dangerous", !IsEnterprise()))

  RM_TRY(RMCreateSearchCommand(ctx, RS_DROP_IF_X_CMD, DropIfExistsIndexCommand,
         "write", INDEX_ONLY_CMD_ARGS, "write slow dangerous", !IsEnterprise()))

  RM_TRY(RMCreateSearchCommand(ctx, RS_DROP_INDEX_IF_X_CMD, DropIfExistsIndexCommand,
         "write", INDEX_ONLY_CMD_ARGS, "write slow dangerous", !IsEnterprise()))

  RM_TRY(RMCreateSearchCommand(ctx, RS_INFO_CMD, IndexInfoCommand,
         "readonly", INDEX_ONLY_CMD_ARGS, "", true))

  RM_TRY(RMCreateSearchCommand(ctx, RS_TAGVALS_CMD, TagValsCommand,
         "readonly", INDEX_ONLY_CMD_ARGS, "read slow dangerous", true))

  RM_TRY(RMCreateSearchCommand(ctx, RS_PROFILE_CMD, RSProfileCommand,
         "readonly", INDEX_ONLY_CMD_ARGS, "read", true))

  RM_TRY(RMCreateSearchCommand(ctx, RS_EXPLAIN_CMD, QueryExplainCommand,
         "readonly", INDEX_ONLY_CMD_ARGS, "", false))

  RM_TRY(RMCreateSearchCommand(ctx, RS_EXPLAINCLI_CMD, QueryExplainCLICommand,
         "readonly", INDEX_ONLY_CMD_ARGS, "", false))

  RM_TRY(RMCreateSearchCommand(ctx, RS_SUGADD_CMD, RSSuggestAddCommand,
         "write deny-oom", 1, 1, 1, "write", false))

  RM_TRY(RMCreateSearchCommand(ctx, RS_SUGDEL_CMD, RSSuggestDelCommand, "write",
         1, 1, 1, "write", false))

  RM_TRY(RMCreateSearchCommand(ctx, RS_SUGLEN_CMD, RSSuggestLenCommand,
         "readonly", 1, 1, 1, "read", false))

  RM_TRY(RMCreateSearchCommand(ctx, RS_SUGGET_CMD, RSSuggestGetCommand,
         "readonly", 1, 1, 1, "read", false))

  // Do not force cross slot validation since coordinator will handle it.
  RM_TRY(RMCreateSearchCommand(ctx, RS_CURSOR_CMD, RSCursorCommand, "readonly",
         0, 0, 0, "read", true));

  // todo: what to do with this?
  RM_TRY(RMCreateSearchCommand(ctx, RS_SYNADD_CMD, SynAddCommand,
         "write deny-oom", INDEX_ONLY_CMD_ARGS, "", false))

  RM_TRY(RMCreateSearchCommand(ctx, RS_SYNUPDATE_CMD, SynUpdateCommand,
         "write deny-oom", INDEX_ONLY_CMD_ARGS, "", !IsEnterprise()))

  RM_TRY(RMCreateSearchCommand(ctx, RS_SYNDUMP_CMD, SynDumpCommand, "readonly",
         INDEX_ONLY_CMD_ARGS, "", false))

  RM_TRY(RMCreateSearchCommand(ctx, RS_ALTER_CMD, AlterIndexCommand,
         "write deny-oom", INDEX_ONLY_CMD_ARGS, "", !IsEnterprise()))

  RM_TRY(RMCreateSearchCommand(ctx, RS_ALTER_IF_NX_CMD, AlterIndexIfNXCommand,
         "write deny-oom", INDEX_ONLY_CMD_ARGS, "", !IsEnterprise()))

  // "Special" case - we do not allow debug commands from the user on RE, while
  // we also don't want them to be internal on OSS.
  RM_TRY(RMCreateSearchCommand(ctx, RS_DEBUG, NULL,
         IsEnterprise() ? "readonly " CMD_PROXY_FILTERED : "readonly",
         RS_DEBUG_FLAGS, "admin", false))
  RM_TRY_F(RegisterDebugCommands, RedisModule_GetCommand(ctx, RS_DEBUG))

  RM_TRY(RMCreateSearchCommand(ctx, RS_SPELL_CHECK, SpellCheckCommand,
         "readonly", INDEX_ONLY_CMD_ARGS, "", true))

  RM_TRY(RMCreateSearchCommand(ctx, RS_DICT_ADD, DictAddCommand,
         "write deny-oom", 0, 0, 0, "", !IsEnterprise()))

  RM_TRY(RMCreateSearchCommand(ctx, RS_DICT_DEL, DictDelCommand, "write", 0, 0,
         0, "", !IsEnterprise()))

  RM_TRY(RMCreateSearchCommand(ctx, RS_DICT_DUMP, DictDumpCommand, "readonly",
         0, 0, 0, "", false))

  // "Special" case - similar to `_FT.DEBUG` (see above).
  RM_TRY(RMCreateSearchCommand(ctx, RS_CONFIG, ConfigCommand,
         IsEnterprise() ? "readonly " CMD_PROXY_FILTERED : "readonly",
         0, 0, 0, "admin", false))

  // Alias is a special case, we can not use the INDEX_ONLY_CMD_ARGS/INDEX_DOC_CMD_ARGS macros
  // Cluster is managed outside of module lets trust it and not raise cross slot error.
  RM_TRY(RMCreateSearchCommand(ctx, RS_ALIASADD, AliasAddCommand,
         "write deny-oom", 0, 0, 0, "", !IsEnterprise()))
  RM_TRY(RMCreateSearchCommand(ctx, RS_ALIASADD_IF_NX, AliasAddCommandIfNX,
         "write deny-oom", 0, 0, 0, "", !IsEnterprise()))
  RM_TRY(RMCreateSearchCommand(ctx, RS_ALIASUPDATE, AliasUpdateCommand,
         "write deny-oom", 0, 0, 0, "", !IsEnterprise()))

  RM_TRY(RMCreateSearchCommand(ctx, RS_ALIASDEL, AliasDelCommand, "write", 0, 0,
         0, "", !IsEnterprise()))
  RM_TRY(RMCreateSearchCommand(ctx, RS_ALIASDEL_IF_EX, AliasDelIfExCommand,
         "write", 0, 0, 0, "", !IsEnterprise()))
  return REDISMODULE_OK;
}

extern dict *legacySpecDict, *legacySpecRules;

void RediSearch_CleanupModule(void) {
  static int invoked = 0;
  if (invoked || !RS_Initialized) {
    return;
  }
  invoked = 1;

  // First free all indexes
  Indexes_Free(specDict_g);
  dictRelease(specDict_g);
  specDict_g = NULL;

  // Let the workers finish BEFORE we call CursorList_Destroy, since it frees a global
  // data structure that is accessed upon releasing the spec (and running thread might hold
  // a reference to the spec bat this time).
  workersThreadPool_Drain(RSDummyContext, 0);
  workersThreadPool_Destroy();

  // At this point, the thread local storage is no longer needed, since all threads
  // finished their work.
  MainThread_DestroyBlockedQueries();

  if (legacySpecDict) {
    dictRelease(legacySpecDict);
    legacySpecDict = NULL;
  }
  LegacySchemaRulesArgs_Free(RSDummyContext);

  // free thread pools
  GC_ThreadPoolDestroy();
  CleanPool_ThreadPoolDestroy();
  ReindexPool_ThreadPoolDestroy();
  ConcurrentSearch_ThreadPoolDestroy();

  // free global structures
  Extensions_Free();
  StopWordList_FreeGlobals();
  FunctionRegistry_Free();
  mempool_free_global();
  IndexAlias_DestroyGlobal(&AliasTable_g);
  freeGlobalAddStrings();
  SchemaPrefixes_Free(SchemaPrefixes_g);
  // GeometryApi_Free();

  Dictionary_Free();
  RediSearch_LockDestory();

  IndexError_GlobalCleanup();
}

// A reducer that just merges N sets of strings by chaining them into one big array with no
// duplicates

int uniqueStringsReducer(struct MRCtx *mc, int count, MRReply **replies) {
  RedisModuleCtx *ctx = MRCtx_GetRedisCtx(mc);
  RedisModule_Reply _reply = RedisModule_NewReply(ctx), *reply = &_reply;

  MRReply *err = NULL;

  TrieMap *dict = NewTrieMap();
  int nArrs = 0;
  // Add all the set elements into the dedup dict
  for (int i = 0; i < count; i++) {
    if (replies[i] && (MRReply_Type(replies[i]) == MR_REPLY_ARRAY
    || MRReply_Type(replies[i]) == MR_REPLY_SET)) {
      nArrs++;
      for (size_t j = 0; j < MRReply_Length(replies[i]); j++) {
        size_t sl = 0;
        const char *s = MRReply_String(MRReply_ArrayElement(replies[i], j), &sl);
        if (s && sl) {
          TrieMap_Add(dict, s, sl, NULL, NULL);
        }
      }
    } else if (MRReply_Type(replies[i]) == MR_REPLY_ERROR && err == NULL) {
      err = replies[i];
    }
  }

  // if there are no values - either reply with an empty set or an error
  if (TrieMap_NUniqueKeys(dict) == 0) {

    if (nArrs > 0) {
      // the sets were empty - return an empty set
      RedisModule_Reply_Set(reply);
      RedisModule_Reply_SetEnd(reply);
    } else {
      RedisModule_ReplyWithError(ctx, err ? (const char *)err : "Could not perform query");
    }
    goto cleanup;
  }

  // Iterate the dict and reply with all values
  RedisModule_Reply_Set(reply);
    char *s;
    tm_len_t sl;
    void *p;
    TrieMapIterator *it = TrieMap_Iterate(dict);
    while (TrieMapIterator_Next(it, &s, &sl, &p)) {
      RedisModule_Reply_StringBuffer(reply, s, sl);
    }
    TrieMapIterator_Free(it);
  RedisModule_Reply_SetEnd(reply);

cleanup:
  TrieMap_Free(dict, NULL);
  RedisModule_EndReply(reply);

  return REDISMODULE_OK;
}

// A reducer that just merges N arrays of the same length, selecting the first non NULL reply from
// each

int mergeArraysReducer(struct MRCtx *mc, int count, MRReply **replies) {
  RedisModuleCtx *ctx = MRCtx_GetRedisCtx(mc);
  RedisModule_Reply _reply = RedisModule_NewReply(ctx), *reply = &_reply;

  for (size_t i = 0; i < count; ++i) {
    if (MRReply_Type(replies[i]) == MR_REPLY_ERROR) {
      // we got an error reply, something goes wrong so we return the error to the user.
      int rc = MR_ReplyWithMRReply(reply, replies[i]);
      RedisModule_EndReply(reply);
      return rc;
    }
  }

  int j = 0;
  int stillValid;
  do {
    // the number of still valid arrays in the response
    stillValid = 0;

    for (int i = 0; i < count; i++) {
      // if this is not an array - ignore it
      if (MRReply_Type(replies[i]) != MR_REPLY_ARRAY) continue;
      // if we've overshot the array length - ignore this one
      if (MRReply_Length(replies[i]) <= j) continue;
      // increase the number of valid replies
      stillValid++;

      // get the j element of array i
      MRReply *ele = MRReply_ArrayElement(replies[i], j);
      // if it's a valid response OR this is the last array we are scanning -
      // add this element to the merged array
      if (MRReply_Type(ele) != MR_REPLY_NIL || i + 1 == count) {
        // if this is the first reply - we need to crack open a new array reply
        if (j == 0) {
          RedisModule_Reply_Array(reply);
        }

        MR_ReplyWithMRReply(reply, ele);
        j++;
        break;
      }
    }
  } while (stillValid > 0);

  // j 0 means we could not process a single reply element from any reply
  if (j == 0) {
    int rc = RedisModule_Reply_Error(reply, "Could not process replies");
    RedisModule_EndReply(reply);
    return rc;
  }
  RedisModule_Reply_ArrayEnd(reply);

  RedisModule_EndReply(reply);
  return REDISMODULE_OK;
}

int singleReplyReducer(struct MRCtx *mc, int count, MRReply **replies) {
  RedisModuleCtx *ctx = MRCtx_GetRedisCtx(mc);
  RedisModule_Reply _reply = RedisModule_NewReply(ctx), *reply = &_reply;

  if (count == 0) {
    RedisModule_Reply_Null(reply);
  } else {
    MR_ReplyWithMRReply(reply, replies[0]);
  }

  RedisModule_EndReply(reply);
  return REDISMODULE_OK;
}

// a reducer that expects "OK" reply for all replies, and stops at the first error and returns it
int allOKReducer(struct MRCtx *mc, int count, MRReply **replies) {
  RedisModuleCtx *ctx = MRCtx_GetRedisCtx(mc);
  RedisModule_Reply _reply = RedisModule_NewReply(ctx), *reply = &_reply;

  if (count == 0) {
    RedisModule_Reply_Error(reply, "Could not distribute command");
    goto end;
  }

  bool isIntegerReply = false, isDoubleReply = false;
  long long integerReply = 0;
  double doubleReply = 0;
  for (int i = 0; i < count; i++) {
    if (MRReply_Type(replies[i]) == MR_REPLY_ERROR) {
      MR_ReplyWithMRReply(reply, replies[i]);
      goto end;
    }
    if (MRReply_Type(replies[i]) == MR_REPLY_INTEGER) {
      long long n = MRReply_Integer(replies[i]);
      if (!isIntegerReply) {
        integerReply = n;
        isIntegerReply = true;
      } else if (n != integerReply) {
        RedisModule_Reply_SimpleString(reply, "not all results are the same");
        goto end;
      }
    } else if (MRReply_Type(replies[i]) == MR_REPLY_DOUBLE) {
      double n = MRReply_Double(replies[i]);
      if (!isDoubleReply) {
        doubleReply = n;
        isDoubleReply = true;
      } else if (n != doubleReply) {
        RedisModule_Reply_SimpleString(reply, "not all results are the same");
        goto end;
      }
    }
  }

  if (isIntegerReply) {
    RedisModule_Reply_LongLong(reply, integerReply);
  } else if (isDoubleReply) {
    RedisModule_Reply_Double(reply, doubleReply);
  } else {
    RedisModule_Reply_SimpleString(reply, "OK");
  }

end:
  RedisModule_EndReply(reply);
  return REDISMODULE_OK;
}

typedef struct {
  char *id;
  size_t idLen;
  double score;
  MRReply *explainScores;
  MRReply *fields;
  MRReply *payload;
  const char *sortKey;
  size_t sortKeyLen;
  double sortKeyNum;
} searchResult;

struct searchReducerCtx; // Predecleration
typedef void (*processReplyCB)(MRReply *arr, struct searchReducerCtx *rCtx, RedisModuleCtx *ctx);
typedef void (*postProcessReplyCB)( struct searchReducerCtx *rCtx);

typedef struct {
  int step;  // offset for next reply
  int score;
  int firstField;
  int payload;
  int sortKey;
} searchReplyOffsets;

typedef struct{
  MRReply *fieldNames;
  MRReply *lastError;
  searchResult *cachedResult;
  searchRequestCtx *searchCtx;
  heap_t *pq;
  size_t totalReplies;
  bool errorOccurred;
  searchReplyOffsets offsets;

  processReplyCB processReply;
  postProcessReplyCB postProcess;
  specialCaseCtx* reduceSpecialCaseCtxKnn;
  specialCaseCtx* reduceSpecialCaseCtxSortby;

  MRReply *warning;
} searchReducerCtx;

typedef struct {
  searchResult* result;
  double score;
} scoredSearchResultWrapper;

specialCaseCtx* SpecialCaseCtx_New() {
  specialCaseCtx* ctx = rm_calloc(1, sizeof(specialCaseCtx));
  return ctx;
}

void SpecialCaseCtx_Free(specialCaseCtx* ctx) {
  if (!ctx) return;
  if(ctx->specialCaseType == SPECIAL_CASE_KNN) {
    QueryNode_Free(ctx->knn.queryNode);
  } else if(ctx->specialCaseType == SPECIAL_CASE_SORTBY) {
    rm_free((void*)ctx->sortby.sortKey);
  }
  rm_free(ctx);
}

static searchRequestCtx* searchRequestCtx_New(void) {
  return rm_calloc(1, sizeof(searchRequestCtx));
}

static void searchRequestCtx_Free(searchRequestCtx *r) {
  if(r->queryString) {
    rm_free(r->queryString);
  }
  if(r->specialCases) {
    size_t specialCasesLen = array_len(r->specialCases);
    for(size_t i = 0; i< specialCasesLen; i ++) {
      specialCaseCtx* ctx = r->specialCases[i];
      SpecialCaseCtx_Free(ctx);
    }
    array_free(r->specialCases);
  }
  if(r->requiredFields) {
    array_free(r->requiredFields);
  }
  rm_free(r);
}

static int searchResultReducer(struct MRCtx *mc, int count, MRReply **replies);

static int rscParseProfile(searchRequestCtx *req, RedisModuleString **argv) {
  req->profileArgs = 0;
  if (RMUtil_ArgIndex("FT.PROFILE", argv, 1) != -1) {
    req->profileArgs += 2;
    req->profileClock = clock();
    if (RMUtil_ArgIndex("LIMITED", argv + 3, 1) != -1) {
      req->profileLimited = 1;
      req->profileArgs++;
    }
    if (RMUtil_ArgIndex("QUERY", argv + 3, 2) == -1) {
      return REDISMODULE_ERR;
    }
  }
  return REDISMODULE_OK;
}

void setKNNSpecialCase(searchRequestCtx *req, specialCaseCtx *knn_ctx) {
  if(!req->specialCases) {
    req->specialCases = array_new(specialCaseCtx*, 1);
  }
  array_append(req->specialCases, knn_ctx);
  // Default: No SORTBY is given, or SORTBY is given by other field
  // When first sorting by different field, the topk vectors should be passed to the coordinator heap
  knn_ctx->knn.shouldSort = true;
  // We need to get K results from the shards
  // For example the command request SORTBY text_field LIMIT 2 3
  // In this case the top 5 results relevant for this sort might be the in the last 5 results of the TOPK
  long long requestedResultsCount = req->requestedResultsCount;
  req->requestedResultsCount = MAX(knn_ctx->knn.k, requestedResultsCount);
  if(array_len(req->specialCases) > 1) {
    specialCaseCtx* optionalSortCtx = req->specialCases[0];
    if(optionalSortCtx->specialCaseType == SPECIAL_CASE_SORTBY) {
      if(strcmp(optionalSortCtx->sortby.sortKey, knn_ctx->knn.fieldName) == 0){
        // If SORTBY is done by the vector score field, the coordinator will do it and no special operation is needed.
        knn_ctx->knn.shouldSort = false;
        // The requested results should be at most K
        req->requestedResultsCount = MIN(knn_ctx->knn.k, requestedResultsCount);
      }
    }
  }
}


// Prepare a TOPK special case, return a context with the required KNN fields if query is
// valid and contains KNN section, NULL otherwise (and set proper error in *status* if error
// was found).
specialCaseCtx *prepareOptionalTopKCase(const char *query_string, RedisModuleString **argv, int argc, uint dialectVersion,
                                        QueryError *status) {

  // First, parse the query params if exists, to set the params in the query parser ctx.
  dict *params = NULL;
  QueryNode* queryNode = NULL;
  int paramsOffset = RMUtil_ArgExists("PARAMS", argv, argc, 1);
  if (paramsOffset > 0) {
    ArgsCursor ac;
    ArgsCursor_InitRString(&ac, argv+paramsOffset+1, argc-(paramsOffset+1));
    if (parseParams(&params, &ac, status) != REDISMODULE_OK) {
        return NULL;
    }
  }
  RedisSearchCtx sctx = {0};
  RSSearchOptions opts = {0};
  opts.params = params;
  QueryParseCtx qpCtx = {
      .raw = query_string,
      .len = strlen(query_string),
      .sctx = &sctx,
      .opts = &opts,
      .status = status,
#ifdef PARSER_DEBUG
      .trace_log = NULL
#endif
  };

  // KNN queries are parsed only on dialect versions >=2
  queryNode = RSQuery_ParseRaw_v2(&qpCtx);
  if (QueryError_GetCode(status) != QUERY_OK || queryNode == NULL) {
    // Query parsing failed.
    goto cleanup;
  }
  if (QueryNode_NumParams(queryNode) > 0 && paramsOffset == 0) {
    // Query expects params, but no params were given.
    goto cleanup;
  }
  if (QueryNode_NumParams(queryNode) > 0) {
      int ret = QueryNode_EvalParamsCommon(params, queryNode, dialectVersion, status);
      if (ret != REDISMODULE_OK || QueryError_GetCode(status) != QUERY_OK) {
        // Params evaluation failed.
        goto cleanup;
      }
      Param_DictFree(params);
      params = NULL;
  }

  if (queryNode->type == QN_VECTOR) {
    QueryVectorNode queryVectorNode = queryNode->vn;
    size_t k = queryVectorNode.vq->knn.k;
    if (k > MAX_KNN_K) {
      QueryError_SetWithoutUserDataFmt(status, QUERY_ELIMIT, VECSIM_KNN_K_TOO_LARGE_ERR_MSG ", max supported K value is %zu", MAX_KNN_K);
      goto cleanup;
    }
    specialCaseCtx *ctx = SpecialCaseCtx_New();
    ctx->knn.k = k;
    ctx->knn.fieldName = queryNode->opts.distField ? queryNode->opts.distField : queryVectorNode.vq->scoreField;
    ctx->knn.pq = NULL;
    ctx->knn.queryNode = queryNode;  // take ownership
    ctx->specialCaseType = SPECIAL_CASE_KNN;
    return ctx;
  }

cleanup:
  if (params) {
    Param_DictFree(params);
  }
  if (queryNode) {
    QueryNode_Free(queryNode);
  }
  return NULL;
}

// Prepare a sortby special case.
void prepareSortbyCase(searchRequestCtx *req, RedisModuleString **argv, int argc, int sortByIndex) {
  const char* sortkey = RedisModule_StringPtrLen(argv[sortByIndex + 1], NULL);
  specialCaseCtx *ctx = SpecialCaseCtx_New();
  ctx->specialCaseType = SPECIAL_CASE_SORTBY;
  ctx->sortby.sortKey = rm_strdup(sortkey);
  ctx->sortby.asc = true;
  req->sortAscending = true;
  if (req->withSortby && sortByIndex + 2 < argc) {
    if (RMUtil_StringEqualsCaseC(argv[sortByIndex + 2], "DESC")) {
      ctx->sortby.asc = false;
      req->sortAscending = false;
    }
  }
  if(!req->specialCases) {
      req->specialCases = array_new(specialCaseCtx*, 1);
    }
  array_append(req->specialCases, ctx);
}

searchRequestCtx *rscParseRequest(RedisModuleString **argv, int argc, QueryError* status) {

  searchRequestCtx *req = searchRequestCtx_New();

  req->initClock = clock();

  if (rscParseProfile(req, argv) != REDISMODULE_OK) {
    searchRequestCtx_Free(req);
    return NULL;
  }

  int argvOffset = 2 + req->profileArgs;
  req->queryString = rm_strdup(RedisModule_StringPtrLen(argv[argvOffset++], NULL));
  req->limit = 10;
  req->offset = 0;
  // marks the user set WITHSCORES. internally it's always set
  req->withScores = RMUtil_ArgExists("WITHSCORES", argv, argc, argvOffset) != 0;
  req->withExplainScores = RMUtil_ArgExists("EXPLAINSCORE", argv, argc, argvOffset) != 0;
  req->specialCases = NULL;
  req->requiredFields = NULL;

  req->withSortingKeys = RMUtil_ArgExists("WITHSORTKEYS", argv, argc, argvOffset) != 0;
  // fprintf(stderr, "Sortby: %d, asc: %d withsort: %d\n", req->withSortby, req->sortAscending,
  //         req->withSortingKeys);

  // Detect "NOCONTENT"
  req->noContent = RMUtil_ArgExists("NOCONTENT", argv, argc, argvOffset) != 0;

  // if RETURN exists - make sure we don't have RETURN 0
  if (!req->noContent && RMUtil_ArgExists("RETURN", argv, argc, argvOffset)) {
    long long numReturns = -1;
    RMUtil_ParseArgsAfter("RETURN", argv, argc, "l", &numReturns);
    // RETURN 0 equals NOCONTENT
    if (numReturns <= 0) {
      req->noContent = 1;
    }
  }

  req->withPayload = RMUtil_ArgExists("WITHPAYLOADS", argv, argc, argvOffset) != 0;

  // Parse LIMIT argument
  RMUtil_ParseArgsAfter("LIMIT", argv + argvOffset, argc - argvOffset, "ll", &req->offset, &req->limit);
  if (req->limit < 0 || req->offset < 0) {
    searchRequestCtx_Free(req);
    return NULL;
  }
  req->requestedResultsCount = req->limit + req->offset;

  // Handle special cases
  // Parse SORTBY ... ASC.
  // Parse it ALWAYS first so the sortkey will be send first
  int sortByIndex = RMUtil_ArgIndex("SORTBY", argv, argc);
  if (sortByIndex > 2) {
    req->withSortby = true;
    // Check for command error where no sortkey is given.
    if(sortByIndex + 1 >= argc) {
      searchRequestCtx_Free(req);
      return NULL;
    }
    prepareSortbyCase(req, argv, argc, sortByIndex);
  } else {
    req->withSortby = false;
  }

  unsigned int dialect = RSGlobalConfig.requestConfigParams.dialectVersion;
  int argIndex = RMUtil_ArgExists("DIALECT", argv, argc, argvOffset);
  if(argIndex > 0) {
      argIndex++;
      ArgsCursor ac;
      ArgsCursor_InitRString(&ac, argv+argIndex, argc-argIndex);
      if (parseDialect(&dialect, &ac, status) != REDISMODULE_OK) {
        searchRequestCtx_Free(req);
        return NULL;
      }
  }

  if(dialect >= 2) {
    // Note: currently there is only one single case. For extending those cases we should use a trie here.
    if(strcasestr(req->queryString, "KNN")) {
      specialCaseCtx *knnCtx = prepareOptionalTopKCase(req->queryString, argv, argc, dialect, status);
      if (QueryError_HasError(status)) {
        searchRequestCtx_Free(req);
        return NULL;
      }
      if (knnCtx != NULL) {
        setKNNSpecialCase(req, knnCtx);
      }
    }
  }

  req->format = QEXEC_FORMAT_DEFAULT;
  argIndex = RMUtil_ArgExists("FORMAT", argv, argc, argvOffset);
  if(argIndex > 0) {
    argIndex++;
    ArgsCursor ac;
    ArgsCursor_InitRString(&ac, argv+argIndex, argc-argIndex);
    if (parseValueFormat(&req->format, &ac, status) != REDISMODULE_OK) {
      searchRequestCtx_Free(req);
      return NULL;
    }
  }

  // Get timeout parameter, if set in the command
  argIndex = RMUtil_ArgIndex("TIMEOUT", argv, argc);
  if (argIndex > -1) {
    argIndex++;
    ArgsCursor ac;
    ArgsCursor_InitRString(&ac, argv+argIndex, argc-argIndex);
    if (parseTimeout(&req->timeout, &ac, status)) {
      searchRequestCtx_Free(req);
      return NULL;
    }
  } else {
    req->timeout = RSGlobalConfig.requestConfigParams.queryTimeoutMS;
  }

  return req;
}

static int cmpStrings(const char *s1, size_t l1, const char *s2, size_t l2) {
  int cmp = memcmp(s1, s2, MIN(l1, l2));
  if (l1 == l2) {
    // if the strings are the same length, just return the result of strcmp
    return cmp;
  }

  // if the strings are identical but the lengths aren't, return the longer string
  if (cmp == 0) {
    return l1 > l2 ? 1 : -1;
  } else {  // the strings are lexically different, just return that
    return cmp;
  }
}

static int cmp_results(const void *p1, const void *p2, const void *udata) {

  const searchResult *r1 = p1, *r2 = p2;
  const searchRequestCtx *req = udata;
  // Compary by sorting keys
  if (req->withSortby) {
    int cmp = 0;
    if ((r1->sortKey || r2->sortKey)) {
      // Sort by numeric sorting keys
      if (r1->sortKeyNum != HUGE_VAL && r2->sortKeyNum != HUGE_VAL) {
        double diff = r2->sortKeyNum - r1->sortKeyNum;
        cmp = diff < 0 ? -1 : (diff > 0 ? 1 : 0);
      } else if (r1->sortKey && r2->sortKey) {

        // Sort by string sort keys
        cmp = cmpStrings(r2->sortKey, r2->sortKeyLen, r1->sortKey, r1->sortKeyLen);
        // printf("Using sortKey!! <N=%lu> %.*s vs <N=%lu> %.*s. Result=%d\n", r2->sortKeyLen,
        //        (int)r2->sortKeyLen, r2->sortKey, r1->sortKeyLen, (int)r1->sortKeyLen, r1->sortKey,
        //        cmp);
      } else {
        // If at least one of these has no sort key, it gets high value regardless of asc/desc
        return r2->sortKey ? 1 : -1;
      }
    }
    // in case of a tie or missing both sorting keys - compare ids
    if (!cmp) {
      // printf("It's a tie! Comparing <N=%lu> %.*s vs <N=%lu> %.*s\n", r2->idLen, (int)r2->idLen,
      //        r2->id, r1->idLen, (int)r1->idLen, r1->id);
      cmp = cmpStrings(r2->id, r2->idLen, r1->id, r1->idLen);
    }
    return (req->sortAscending ? -cmp : cmp);
  }

  double s1 = r1->score, s2 = r2->score;
  // printf("Scores: %lf vs %lf. WithSortBy: %d. SK1=%p. SK2=%p\n", s1, s2, req->withSortby,
  //        r1->sortKey, r2->sortKey);
  if (s1 < s2) {
    return 1;
  } else if (s1 > s2) {
    return -1;
  } else {
    // printf("Scores are tied. Will compare ID Strings instead\n");

    // This was reversed to be more compatible with OSS version where tie breaker was changed
    // to return the lower doc ID to reduce sorting heap work. Doc name might not be ascending
    // or descending but this still may reduce heap work.
    // Our tests are usually ascending so this will create similarity between RS and RSC.
    int rv = -cmpStrings(r2->id, r2->idLen, r1->id, r1->idLen);

    // printf("ID Strings: Comparing <N=%lu> %.*s vs <N=%lu> %.*s => %d\n", r2->idLen,
    // (int)r2->idLen,
    //        r2->id, r1->idLen, (int)r1->idLen, r1->id, rv);
    return rv;
  }
}

searchResult *newResult_resp2(searchResult *cached, MRReply *arr, int j, searchReplyOffsets* offsets, int explainScores) {
  int scoreOffset = offsets->score;
  int fieldsOffset = offsets->firstField;
  int payloadOffset = offsets->payload;
  int sortKeyOffset = offsets->sortKey;
  searchResult *res = cached ? cached : rm_malloc(sizeof *res);
  res->sortKey = NULL;
  res->sortKeyNum = HUGE_VAL;
  if (MRReply_Type(MRReply_ArrayElement(arr, j)) != MR_REPLY_STRING) {
    res->id = NULL;
    return res;
  }
  res->id = (char*)MRReply_String(MRReply_ArrayElement(arr, j), &res->idLen);
  if (!res->id) {
    return res;
  }
  // parse score
  if (explainScores) {
    MRReply *scoreReply = MRReply_ArrayElement(arr, j + scoreOffset);
    if (MRReply_Type(scoreReply) != MR_REPLY_ARRAY) {
      res->id = NULL;
      return res;
    }
    if (MRReply_Length(scoreReply) != 2) {
      res->id = NULL;
      return res;
    }
    if (!MRReply_ToDouble(MRReply_ArrayElement(scoreReply, 0), &res->score)) {
      res->id = NULL;
      return res;
    }
    res->explainScores = MRReply_ArrayElement(scoreReply, 1);
    // Parse scores only if they were are part of the shard's response.
  } else if (scoreOffset > 0 &&
             !MRReply_ToDouble(MRReply_ArrayElement(arr, j + scoreOffset), &res->score)) {
      res->id = NULL;
      return res;
  }
  // get fields
  res->fields = fieldsOffset > 0 ? MRReply_ArrayElement(arr, j + fieldsOffset) : NULL;
  // get payloads
  res->payload = payloadOffset > 0 ? MRReply_ArrayElement(arr, j + payloadOffset) : NULL;
  if (sortKeyOffset > 0) {
    res->sortKey = MRReply_String(MRReply_ArrayElement(arr, j + sortKeyOffset), &res->sortKeyLen);
  }
  if (res->sortKey) {
    if (res->sortKey[0] == '#') {
      char *endptr;
      res->sortKeyNum = fast_float_strtod(res->sortKey + 1, &endptr);
      RS_ASSERT(endptr == res->sortKey + res->sortKeyLen);
    }
    // fprintf(stderr, "Sort key string '%s', num '%f\n", res->sortKey, res->sortKeyNum);
  }
  return res;
}

searchResult *newResult_resp3(searchResult *cached, MRReply *results, int j, searchReplyOffsets* offsets, bool explainScores, specialCaseCtx *reduceSpecialCaseCtxSortBy) {
  searchResult *res = cached ? cached : rm_malloc(sizeof *res);
  res->sortKey = NULL;
  res->sortKeyNum = HUGE_VAL;

  MRReply *result_j = MRReply_ArrayElement(results, j);
  if (MRReply_Type(result_j) != MR_REPLY_MAP) {
    res->id = NULL;
    return res;
  }

  MRReply *result_id = MRReply_MapElement(result_j, "id");
  if (!result_id || !MRReply_Type(result_id) == MR_REPLY_STRING) {
    // We crash in development env, and return NULL (such that an error is raised)
    // in production.
    RS_LOG_ASSERT_FMT(false, "Expected id %d to exist, and be a string", j);
    res->id = NULL;
    return res;
  }
  res->id = (char*)MRReply_String(result_id, &res->idLen);
  if (!res->id) {
    return res;
  }

  // parse score
  MRReply *score = MRReply_MapElement(result_j, "score");
  if (explainScores) {
    if (MRReply_Type(score) != MR_REPLY_ARRAY) {
      res->id = NULL;
      return res;
    }
    if (!MRReply_ToDouble(MRReply_ArrayElement(score, 0), &res->score)) {
      res->id = NULL;
      return res;
    }
    res->explainScores = MRReply_ArrayElement(score, 1);

  } else if (offsets->score > 0 && !MRReply_ToDouble(score, &res->score)) {
      res->id = NULL;
      return res;
  }

  // get fields
  res->fields = MRReply_MapElement(result_j, "extra_attributes");

  // get payloads
  res->payload = MRReply_MapElement(result_j, "payload");

  if (offsets->sortKey > 0) {
    MRReply *sortkey = NULL;
    if (reduceSpecialCaseCtxSortBy) {
      MRReply *require_fields = MRReply_MapElement(result_j, "required_fields");
      if (require_fields) {
        sortkey = MRReply_MapElement(require_fields, reduceSpecialCaseCtxSortBy->sortby.sortKey);
      }
    }
    if (!sortkey) {
      // If sortkey is the only special case, it will not be in the required_fields map
      sortkey = MRReply_MapElement(result_j, "sortkey");
    }
    if (!sortkey) {
      // Fail if sortkey is required but not found
      res->id = NULL;
      return res;
    }
    if (sortkey) {
      res->sortKey = MRReply_String(sortkey, &res->sortKeyLen);
      if (res->sortKey) {
        if (res->sortKey[0] == '#') {
          char *endptr;
          res->sortKeyNum = fast_float_strtod(res->sortKey + 1, &endptr);
          RS_ASSERT(endptr == res->sortKey + res->sortKeyLen);
        }
        // fprintf(stderr, "Sort key string '%s', num '%f\n", res->sortKey, res->sortKeyNum);
      }
    }
  }

  return res;
}

static void getReplyOffsets(const searchRequestCtx *ctx, searchReplyOffsets *offsets) {

  /**
   * Reply format
   *
   * ID
   * SCORE         ---| optional - only if WITHSCORES was given, or SORTBY section was not given.
   * Payload
   * Sort field    ---|
   * ...              | special cases - SORTBY, TOPK. Sort key is always first for backwards compatibility.
   * ...           ---|
   * First field
   *
   *
   */

  if (ctx->withScores || !ctx->withSortby) {
    offsets->step = 3;  // 1 for key, 1 for score, 1 for fields
    offsets->score = 1;
    offsets->firstField = 2;
  } else {
    offsets->score = -1;
    offsets->step = 2;  // 1 for key, 1 for fields
    offsets->firstField = 1;
  }
  offsets->payload = -1;
  offsets->sortKey = -1;

  if (ctx->withPayload) {  // save an extra step for payloads
    offsets->step++;
    offsets->payload = offsets->firstField;
    offsets->firstField++;
  }

  // Update the offsets for the special case after determining score, payload, field.
  size_t specialCaseStartOffset = offsets->firstField;
  size_t specialCasesMaxOffset = 0;
  if (ctx->specialCases) {
    size_t nSpecialCases = array_len(ctx->specialCases);
    for(size_t i = 0; i < nSpecialCases; i++) {
      switch (ctx->specialCases[i]->specialCaseType)
      {
      case SPECIAL_CASE_KNN: {
        ctx->specialCases[i]->knn.offset += specialCaseStartOffset;
        specialCasesMaxOffset = MAX(specialCasesMaxOffset, ctx->specialCases[i]->knn.offset);
        break;
      }
      case SPECIAL_CASE_SORTBY: {
        ctx->specialCases[i]->sortby.offset += specialCaseStartOffset;
        offsets->sortKey = ctx->specialCases[i]->sortby.offset;
        specialCasesMaxOffset = MAX(specialCasesMaxOffset, ctx->specialCases[i]->sortby.offset);
        break;
      }
      case SPECIAL_CASE_NONE:
      default:
        break;
      }
    }
  }

  if(specialCasesMaxOffset > 0) {
    offsets->firstField=specialCasesMaxOffset+1;
    offsets->step=offsets->firstField+1;
  }
  else if(ctx->withSortingKeys) {
    offsets->step++;
    offsets->sortKey = offsets->firstField++;
  }

  // nocontent - one less field, and the offset is -1 to avoid parsing it
  if (ctx->noContent) {
    offsets->step--;
    offsets->firstField = -1;
  }
}


/************************** Result processing callbacks **********************/

static int cmp_scored_results(const void *p1, const void *p2, const void *udata) {
  const scoredSearchResultWrapper* s1= p1;
  const scoredSearchResultWrapper* s2 = p2;
  double score1 = s1->score;
  double score2 = s2->score;
  if (score1 < score2) {
    return -1;
  } else if (score1 > score2) {
    return 1;
  }
  return cmpStrings(s1->result->id, s1->result->idLen, s2->result->id, s2->result->idLen);
}

static double parseNumeric(const char *str, const char *sortKey) {
    RS_ASSERT(str[0] == '#');
    char *eptr;
    double d = fast_float_strtod(str + 1, &eptr);
    RS_ASSERT(eptr != sortKey + 1 && *eptr == 0);
    return d;
}

#define GET_NUMERIC_SCORE(d, searchResult_var, score_exp) \
  do {                                                    \
    if (res->sortKeyNum != HUGE_VAL) {                    \
      d = searchResult_var->sortKeyNum;                   \
    } else {                                              \
      const char *score = (score_exp);                    \
      d = parseNumeric(score, res->sortKey);              \
    }                                                     \
  } while (0);

static void ProcessKNNSearchResult(searchResult *res, searchReducerCtx *rCtx, double score, knnContext *knnCtx) {
  // As long as we don't have k results, keep insert
    if (heap_count(knnCtx->pq) < knnCtx->k) {
      scoredSearchResultWrapper* resWrapper = rm_malloc(sizeof(scoredSearchResultWrapper));
      resWrapper->result = res;
      resWrapper->score = score;
      heap_offerx(knnCtx->pq, resWrapper);
    } else {
      // Check for upper bound
      scoredSearchResultWrapper tmpWrapper;
      tmpWrapper.result = res;
      tmpWrapper.score = score;
      scoredSearchResultWrapper *largest = heap_peek(knnCtx->pq);
      int c = cmp_scored_results(&tmpWrapper, largest, rCtx->searchCtx);
      if (c < 0) {
        scoredSearchResultWrapper* resWrapper = rm_malloc(sizeof(scoredSearchResultWrapper));
        resWrapper->result = res;
        resWrapper->score = score;
        // Current result is smaller then upper bound, replace them.
        largest = heap_poll(knnCtx->pq);
        heap_offerx(knnCtx->pq, resWrapper);
        rCtx->cachedResult = largest->result;
        rm_free(largest);
      } else {
        rCtx->cachedResult = res;
      }
    }
}

static void ProcessKNNSearchReply(MRReply *arr, searchReducerCtx *rCtx, RedisModuleCtx *ctx) {
  if (arr == NULL) {
    return;
  }
  if (MRReply_Type(arr) == MR_REPLY_ERROR) {
    return;
  }

  bool resp3 = MRReply_Type(arr) == MR_REPLY_MAP;
  if (!resp3 && (MRReply_Type(arr) != MR_REPLY_ARRAY || MRReply_Length(arr) == 0)) {
    // Empty reply??
    return;
  }

  searchRequestCtx *req = rCtx->searchCtx;
  specialCaseCtx* reduceSpecialCaseCtxKnn = rCtx->reduceSpecialCaseCtxKnn;
  specialCaseCtx* reduceSpecialCaseCtxSortBy = rCtx->reduceSpecialCaseCtxSortby;
  searchResult *res;
  if (resp3) {
    // Check for a warning
    MRReply *warning = MRReply_MapElement(arr, "warning");
    RS_LOG_ASSERT(warning && MRReply_Type(warning) == MR_REPLY_ARRAY, "invalid warning record");
    if (!rCtx->warning && MRReply_Length(warning) > 0) {
      rCtx->warning = warning;
    }

    MRReply *results = MRReply_MapElement(arr, "results");
    RS_LOG_ASSERT(results && MRReply_Type(results) == MR_REPLY_ARRAY, "invalid results record");
    size_t len = MRReply_Length(results);
    for (int j = 0; j < len; ++j) {
      res = newResult_resp3(rCtx->cachedResult, results, j, &rCtx->offsets, rCtx->searchCtx->withExplainScores, reduceSpecialCaseCtxSortBy);
      if (res && res->id) {
        rCtx->cachedResult = NULL;
      } else {
        RedisModule_Log(ctx, "warning", "missing required_field when parsing redisearch results");
        goto error;
      }
      MRReply *require_fields = MRReply_MapElement(MRReply_ArrayElement(results, j), "required_fields");
      if (!require_fields) {
        RedisModule_Log(ctx, "warning", "missing required_fields when parsing redisearch results");
        goto error;
      }
      MRReply *score_value = MRReply_MapElement(require_fields, reduceSpecialCaseCtxKnn->knn.fieldName);
      if (!score_value) {
        RedisModule_Log(ctx, "warning", "missing knn required_field when parsing redisearch results");
        goto error;
      }
      double d;
      GET_NUMERIC_SCORE(d, res, MRReply_String(score_value, NULL));
      ProcessKNNSearchResult(res, rCtx, d, &reduceSpecialCaseCtxKnn->knn);
    }
    processResultFormat(&req->format, arr);

  } else {
    size_t len = MRReply_Length(arr);

    int step = rCtx->offsets.step;
    int scoreOffset = reduceSpecialCaseCtxKnn->knn.offset;
    for (int j = 1; j < len; j += step) {
      if (j + step > len) {
        RedisModule_Log(
            ctx, "warning",
            "got a bad reply from redisearch, reply contains less parameters then expected");
        rCtx->errorOccurred = true;
        break;
      }
      res = newResult_resp2(rCtx->cachedResult, arr, j, &rCtx->offsets, rCtx->searchCtx->withExplainScores);
      if (res && res->id) {
        rCtx->cachedResult = NULL;
      } else {
        RedisModule_Log(ctx, "warning", "missing required_field when parsing redisearch results");
        goto error;
      }

      double d;
      GET_NUMERIC_SCORE(d, res, MRReply_String(MRReply_ArrayElement(arr, j + scoreOffset), NULL));
      ProcessKNNSearchResult(res, rCtx, d, &reduceSpecialCaseCtxKnn->knn);
    }
  }
  return;

error:
  rCtx->errorOccurred = true;
  // invalid result - usually means something is off with the response, and we should just
  // quit this response
  rCtx->cachedResult = res;
}

static void processSearchReplyResult(searchResult *res, searchReducerCtx *rCtx, RedisModuleCtx *ctx) {
  if (!res || !res->id) {
    RedisModule_Log(ctx, "warning", "got an unexpected argument when parsing redisearch results");
    rCtx->errorOccurred = true;
    // invalid result - usually means something is off with the response, and we should just
    // quit this response
    rCtx->cachedResult = res;
    return;
  }

  rCtx->cachedResult = NULL;

  // fprintf(stderr, "Result %d Reply docId %s score: %f sortkey %f\n", i, res->id, res->score, res->sortKeyNum);

  // TODO: minmax_heap?
  if (heap_count(rCtx->pq) < heap_size(rCtx->pq)) {
    // printf("Offering result score %f\n", res->score);
    heap_offerx(rCtx->pq, res);
  } else {
    searchResult *smallest = heap_peek(rCtx->pq);
    int c = cmp_results(res, smallest, rCtx->searchCtx);
    if (c < 0) {
      smallest = heap_poll(rCtx->pq);
      heap_offerx(rCtx->pq, res);
      rCtx->cachedResult = smallest;
    } else {
      rCtx->cachedResult = res;
      if (rCtx->searchCtx->withSortby) {
        // If the result is lower than the last result in the heap,
        // AND there is a user-defined sort order - we can stop now
        return;
      }
    }
  }
}

static void processSearchReply(MRReply *arr, searchReducerCtx *rCtx, RedisModuleCtx *ctx) {
  if (arr == NULL) {
    return;
  }
  if (MRReply_Type(arr) == MR_REPLY_ERROR) {
    return;
  }

  bool resp3 = MRReply_Type(arr) == MR_REPLY_MAP;
  if (!resp3 && (MRReply_Type(arr) != MR_REPLY_ARRAY || MRReply_Length(arr) == 0)) {
    // Empty reply??
    return;
  }

  searchRequestCtx *req = rCtx->searchCtx;

  if (resp3) // RESP3
  {
    // Check for a warning
    MRReply *warning = MRReply_MapElement(arr, "warning");
    RS_LOG_ASSERT(warning && MRReply_Type(warning) == MR_REPLY_ARRAY, "invalid warning record");
    if (!rCtx->warning && MRReply_Length(warning) > 0) {
      rCtx->warning = warning;
    }

    MRReply *total_results = MRReply_MapElement(arr, "total_results");
    if (!total_results) {
      rCtx->errorOccurred = true;
      return;
    }
    rCtx->totalReplies += MRReply_Integer(total_results);
    MRReply *results = MRReply_MapElement(arr, "results");
    if (!results) {
      rCtx->errorOccurred = true;
      return;
    }
    size_t len = MRReply_Length(results);

    bool needScore = rCtx->offsets.score > 0;
    for (int i = 0; i < len; ++i) {
      searchResult *res = newResult_resp3(rCtx->cachedResult, results, i, &rCtx->offsets, rCtx->searchCtx->withExplainScores, rCtx->reduceSpecialCaseCtxSortby);
      processSearchReplyResult(res, rCtx, ctx);
    }
    processResultFormat(&rCtx->searchCtx->format, arr);
  }
  else // RESP2
  {
    size_t len = MRReply_Length(arr);

    // first element is the total count
    rCtx->totalReplies += MRReply_Integer(MRReply_ArrayElement(arr, 0));

    int step = rCtx->offsets.step;
    // fprintf(stderr, "Step %d, scoreOffset %d, fieldsOffset %d, sortKeyOffset %d\n", step,
    //         scoreOffset, fieldsOffset, sortKeyOffset);

    for (int j = 1; j < len; j += step) {
      if (j + step > len) {
        RedisModule_Log(ctx, "warning",
          "got a bad reply from redisearch, reply contains less parameters then expected");
        rCtx->errorOccurred = true;
        break;
      }
      searchResult *res = newResult_resp2(rCtx->cachedResult, arr, j, &rCtx->offsets , rCtx->searchCtx->withExplainScores);
      processSearchReplyResult(res, rCtx, ctx);
    }
  }
}

/************************ Result post processing callbacks ********************/


static void noOpPostProcess(searchReducerCtx *rCtx){
  return;
}

static void knnPostProcess(searchReducerCtx *rCtx) {
  specialCaseCtx* reducerSpecialCaseCtx = rCtx->reduceSpecialCaseCtxKnn;
  RS_ASSERT(reducerSpecialCaseCtx->specialCaseType == SPECIAL_CASE_KNN);
  if(reducerSpecialCaseCtx->knn.pq) {
    size_t numberOfResults = heap_count(reducerSpecialCaseCtx->knn.pq);
    for (size_t i = 0; i < numberOfResults; i++) {
      scoredSearchResultWrapper* wrappedResult = heap_poll(reducerSpecialCaseCtx->knn.pq);
      searchResult* res = wrappedResult->result;
      rm_free(wrappedResult);
      if(heap_count(rCtx->pq) < heap_size(rCtx->pq)) {
        heap_offerx(rCtx->pq, res);
      }
      else {
        searchResult *smallest = heap_peek(rCtx->pq);
        int c = cmp_results(res, smallest, rCtx->searchCtx);
        if (c < 0) {
          smallest = heap_poll(rCtx->pq);
          heap_offerx(rCtx->pq, res);
          rm_free(smallest);
        } else {
          rm_free(res);
        }
      }
    }
  }
  // We can always get at most K results
  rCtx->totalReplies = heap_count(rCtx->pq);

}

static void sendSearchResults(RedisModule_Reply *reply, searchReducerCtx *rCtx) {
  // Reverse the top N results

  rCtx->postProcess((struct searchReducerCtx *)rCtx);

  searchRequestCtx *req = rCtx->searchCtx;

  // Number of results to actually return
  size_t num = req->requestedResultsCount;

  size_t qlen = heap_count(rCtx->pq);
  size_t pos = qlen;

  // Load the results from the heap into a sorted array. Free the items in
  // the heap one-by-one so that we don't have to go through them again
  searchResult **results = rm_malloc(sizeof(*results) * qlen);
  while (pos) {
    results[--pos] = heap_poll(rCtx->pq);
  }
  heap_free(rCtx->pq);
  rCtx->pq = NULL;

  //-------------------------------------------------------------------------------------------
  RedisModule_Reply_Map(reply);
  if (reply->resp3) // RESP3
  {
    RedisModule_Reply_SimpleString(reply, "attributes");
    if (rCtx->fieldNames) {
      MR_ReplyWithMRReply(reply, rCtx->fieldNames);
    } else {
      RedisModule_Reply_EmptyArray(reply);
    }

    RedisModule_Reply_SimpleString(reply, "warning"); // >warning
    if (rCtx->warning) {
      MR_ReplyWithMRReply(reply, rCtx->warning);
    } else {
      RedisModule_Reply_EmptyArray(reply);
    }

    RedisModule_ReplyKV_LongLong(reply, "total_results", rCtx->totalReplies);

    if (rCtx->searchCtx->format & QEXEC_FORMAT_EXPAND) {
      RedisModule_ReplyKV_SimpleString(reply, "format", "EXPAND"); // >format
    } else {
      RedisModule_ReplyKV_SimpleString(reply, "format", "STRING"); // >format
    }

    RedisModule_ReplyKV_Array(reply, "results"); // >results

    for (int i = 0; i < qlen && i < num; ++i) {
      RedisModule_Reply_Map(reply); // >> result
        searchResult *res = results[i];

        RedisModule_ReplyKV_StringBuffer(reply, "id", res->id, res->idLen);

        if (req->withScores) {
          RedisModule_Reply_SimpleString(reply, "score");

          if (req->withExplainScores) {
            RedisModule_Reply_Array(reply);
              RedisModule_Reply_Double(reply, res->score);
              MR_ReplyWithMRReply(reply, res->explainScores);
            RedisModule_Reply_ArrayEnd(reply);
          } else {
            RedisModule_Reply_Double(reply, res->score);
          }
        }

        if (req->withPayload) {
          RedisModule_Reply_SimpleString(reply, "payload");
          MR_ReplyWithMRReply(reply, res->payload);
        }

        if (req->withSortingKeys && req->withSortby) {
          RedisModule_Reply_SimpleString(reply, "sortkey");
          if (res->sortKey) {
            RedisModule_Reply_StringBuffer(reply, res->sortKey, res->sortKeyLen);
          } else {
            RedisModule_Reply_Null(reply);
          }
        }
        if (!req->noContent) {
          RedisModule_ReplyKV_MRReply(reply, "extra_attributes", res->fields); // >> extra_attributes
        }

        RedisModule_Reply_SimpleString(reply, "values");
        RedisModule_Reply_EmptyArray(reply);
      RedisModule_Reply_MapEnd(reply); // >>result
    }

    RedisModule_Reply_ArrayEnd(reply); // >results
  }
  //-------------------------------------------------------------------------------------------
  else // RESP2
  {
    RedisModule_Reply_LongLong(reply, rCtx->totalReplies);

    for (pos = rCtx->searchCtx->offset; pos < qlen && pos < num; pos++) {
      searchResult *res = results[pos];
      RedisModule_Reply_StringBuffer(reply, res->id, res->idLen);
      if (req->withScores) {
        if (req->withExplainScores) {
          RedisModule_Reply_Array(reply);
            RedisModule_Reply_Double(reply, res->score);
            MR_ReplyWithMRReply(reply, res->explainScores);
          RedisModule_Reply_ArrayEnd(reply);
        } else {
          RedisModule_Reply_Double(reply, res->score);
        }
      }
      if (req->withPayload) {
        MR_ReplyWithMRReply(reply, res->payload);
      }
      if (req->withSortingKeys && req->withSortby) {
        if (res->sortKey) {
          RedisModule_Reply_StringBuffer(reply, res->sortKey, res->sortKeyLen);
        } else {
          RedisModule_Reply_Null(reply);
        }
      }
      if (!req->noContent) {
        MR_ReplyWithMRReply(reply, res->fields);
      }
    }
  }
  RedisModule_Reply_MapEnd(reply);
  //-------------------------------------------------------------------------------------------

  // Free the sorted results
  for (pos = 0; pos < qlen; pos++) {
    rm_free(results[pos]);
  }
  rm_free(results);
}

/**
 * This function is used to print profiles received from the shards.
 * It is used by both SEARCH and AGGREGATE.
 */
static void PrintShardProfile_resp2(RedisModule_Reply *reply, int count, MRReply **replies, bool isSearch) {
  // The 1st location always stores the results. On FT.AGGREGATE, the next place stores the
  // cursor ID. The last location (2nd for FT.SEARCH and 3rd for FT.AGGREGATE) stores the
  // profile information of the shard.
  const int profile_data_idx = isSearch ? 1 : 2;
  for (int i = 0; i < count; ++i) {
    MRReply *shards_reply = MRReply_ArrayElement(replies[i], profile_data_idx);
    MRReply *shards_array_profile = MRReply_ArrayElement(shards_reply, 1);
    MRReply *shard_profile = MRReply_ArrayElement(shards_array_profile, 0);
    MR_ReplyWithMRReply(reply, shard_profile);
  }
}

static void PrintShardProfile_resp3(RedisModule_Reply *reply, int count, MRReply **replies, bool isSearch) {
  for (int i = 0; i < count; ++i) {
    MRReply *profile;
    if (!isSearch) {
      // On aggregate commands, take the results from the response (second component is the cursor-id)
      MRReply *results = MRReply_ArrayElement(replies[i], 0);
      profile = MRReply_MapElement(results, PROFILE_STR);
    } else {
      profile = MRReply_MapElement(replies[i], PROFILE_STR);
    }
    MRReply *shards = MRReply_MapElement(profile, PROFILE_SHARDS_STR);
    MRReply *shard = MRReply_ArrayElement(shards, 0);

    MR_ReplyWithMRReply(reply, shard);
  }
}

void PrintShardProfile(RedisModule_Reply *reply, void *ctx) {
  PrintShardProfile_ctx *pCtx = ctx;
  if (reply->resp3) {
    PrintShardProfile_resp3(reply, pCtx->count, pCtx->replies, pCtx->isSearch);
  } else {
    PrintShardProfile_resp2(reply, pCtx->count, pCtx->replies, pCtx->isSearch);
  }
}

struct PrintCoordProfile_ctx {
  clock_t totalTime;
  clock_t postProcessTime;
};
static void profileSearchReplyCoordinator(RedisModule_Reply *reply, void *ctx) {
  struct PrintCoordProfile_ctx *pCtx = ctx;
  RedisModule_Reply_Map(reply);
  RedisModule_ReplyKV_Double(reply, "Total Coordinator time", (double)(clock() - pCtx->totalTime) / CLOCKS_PER_MILLISEC);
  RedisModule_ReplyKV_Double(reply, "Post Processing time", (double)(clock() - pCtx->postProcessTime) / CLOCKS_PER_MILLISEC);
  RedisModule_Reply_MapEnd(reply);
}

static void profileSearchReply(RedisModule_Reply *reply, searchReducerCtx *rCtx,
                               int count, MRReply **replies,
                               clock_t totalTime, clock_t postProcessTime) {
  bool has_map = RedisModule_HasMap(reply);
  RedisModule_Reply_Map(reply); // root
    // Have a named map for the results for RESP3
    if (has_map) {
      RedisModule_Reply_SimpleString(reply, "Results"); // >results
    }
    sendSearchResults(reply, rCtx);

    // print profile of shards & coordinator
    PrintShardProfile_ctx shardsCtx = {
        .count = count,
        .replies = replies,
        .isSearch = true,
    };
    struct PrintCoordProfile_ctx coordCtx = {
        .totalTime = totalTime,
        .postProcessTime = postProcessTime,
    };
    Profile_PrintInFormat(reply, PrintShardProfile, &shardsCtx, profileSearchReplyCoordinator, &coordCtx);

    RedisModule_Reply_MapEnd(reply); // >root
}

static void searchResultReducer_wrapper(void *mc_v) {
  struct MRCtx *mc = mc_v;
  searchResultReducer(mc, MRCtx_GetNumReplied(mc), MRCtx_GetReplies(mc));
}

static int searchResultReducer_background(struct MRCtx *mc, int count, MRReply **replies) {
  ConcurrentSearch_ThreadPoolRun(searchResultReducer_wrapper, mc, DIST_THREADPOOL);
  return REDISMODULE_OK;
}

static bool should_return_error(MRReply *reply) {
  // TODO: Replace third condition with a var instead of hard-coded string
  const char *errStr = MRReply_String(reply, NULL);
  return (!errStr
          || RSGlobalConfig.requestConfigParams.timeoutPolicy == TimeoutPolicy_Fail
          || strcmp(errStr, "Timeout limit was reached"));
}

static bool should_return_timeout_error(searchRequestCtx *req) {
  return RSGlobalConfig.requestConfigParams.timeoutPolicy == TimeoutPolicy_Fail
         && req->timeout != 0
         && ((double)(clock() - req->initClock) / CLOCKS_PER_MILLISEC) > req->timeout;
}

static int searchResultReducer(struct MRCtx *mc, int count, MRReply **replies) {
  clock_t postProcessTime;
  RedisModuleBlockedClient *bc = MRCtx_GetBlockedClient(mc);
  RedisModuleCtx *ctx = RedisModule_GetThreadSafeContext(bc);
  searchRequestCtx *req = MRCtx_GetPrivData(mc);
  searchReducerCtx rCtx = {NULL};
  int profile = req->profileArgs > 0;
  RedisModule_Reply _reply = RedisModule_NewReply(ctx), *reply = &_reply;

  int res = REDISMODULE_OK;
  // got no replies - this means timeout
  if (count == 0 || req->limit < 0) {
    res = RedisModule_Reply_Error(reply, "Could not send query to cluster");
    goto cleanup;
  }

  // Traverse the replies, check for early bail-out which we want for all errors
  // but timeout+non-strict timeout policy.
  for (int i = 0; i < count; i++) {
    MRReply *curr_rep = replies[i];
    if (MRReply_Type(curr_rep) == MR_REPLY_ERROR) {
      rCtx.errorOccurred = true;
      rCtx.lastError = curr_rep;
      if (should_return_error(curr_rep)) {
        res = MR_ReplyWithMRReply(reply, curr_rep);
        goto cleanup;
      }
    }
  }

  rCtx.searchCtx = req;

  // Get reply offsets
  getReplyOffsets(rCtx.searchCtx, &rCtx.offsets);

  // Init results heap.
  size_t num = req->requestedResultsCount;
  rCtx.pq = rm_malloc(heap_sizeof(num));
  heap_init(rCtx.pq, cmp_results, req, num);

  // Default result process and post process operations
  rCtx.processReply = (processReplyCB) processSearchReply;
  rCtx.postProcess = (postProcessReplyCB) noOpPostProcess;

  if (req->specialCases) {
    size_t nSpecialCases = array_len(req->specialCases);
    for (size_t i = 0; i < nSpecialCases; ++i) {
      if (req->specialCases[i]->specialCaseType == SPECIAL_CASE_KNN) {
        specialCaseCtx* knnCtx = req->specialCases[i];
        rCtx.postProcess = (postProcessReplyCB) knnPostProcess;
        rCtx.reduceSpecialCaseCtxKnn = knnCtx;
        if (knnCtx->knn.shouldSort) {
          knnCtx->knn.pq = rm_malloc(heap_sizeof(knnCtx->knn.k));
          heap_init(knnCtx->knn.pq, cmp_scored_results, NULL, knnCtx->knn.k);
          rCtx.processReply = (processReplyCB) ProcessKNNSearchReply;
          break;
        }
      } else if (req->specialCases[i]->specialCaseType == SPECIAL_CASE_SORTBY) {
        rCtx.reduceSpecialCaseCtxSortby = req->specialCases[i];
      }
    }
  }

  if (!profile) {
    for (int i = 0; i < count; ++i) {
      rCtx.processReply(replies[i], (struct searchReducerCtx *)&rCtx, ctx);

      // If we timed out on strict timeout policy, return a timeout error
      if (should_return_timeout_error(req)) {
        RedisModule_Reply_Error(reply, QueryError_Strerror(QUERY_ETIMEDOUT));
        goto cleanup;
      }
    }
  } else {
    for (int i = 0; i < count; ++i) {
      MRReply *mr_reply;
      if (reply->resp3) {
        mr_reply = MRReply_MapElement(replies[i], "Results");
      } else {
        mr_reply = MRReply_ArrayElement(replies[i], 0);
      }
      rCtx.processReply(mr_reply, (struct searchReducerCtx *)&rCtx, ctx);

      // If we timed out on strict timeout policy, return a timeout error
      if (should_return_timeout_error(req)) {
        RedisModule_Reply_Error(reply, QueryError_Strerror(QUERY_ETIMEDOUT));
        goto cleanup;
      }
    }
  }

  if (rCtx.cachedResult) {
    rm_free(rCtx.cachedResult);
  }

  if (rCtx.errorOccurred && !rCtx.lastError) {
    RedisModule_Reply_Error(reply, "could not parse redisearch results");
    goto cleanup;
  }

  if (!profile) {
    sendSearchResults(reply, &rCtx);
  } else {
    profileSearchReply(reply, &rCtx, count, replies, req->profileClock, clock());
  }

  TotalGlobalStats_CountQuery(QEXEC_F_IS_SEARCH, clock() - req->initClock);

cleanup:
  RedisModule_EndReply(reply);

  if (rCtx.pq) {
    heap_destroy(rCtx.pq);
  }
  if (rCtx.reduceSpecialCaseCtxKnn &&
      rCtx.reduceSpecialCaseCtxKnn->knn.pq) {
    heap_destroy(rCtx.reduceSpecialCaseCtxKnn->knn.pq);
  }

  RedisModule_BlockedClientMeasureTimeEnd(bc);
  RedisModule_UnblockClient(bc, NULL);
  RedisModule_FreeThreadSafeContext(ctx);
  // We could pass `mc` to the unblock function to perform the next 3 cleanup steps, but
  // this way we free the memory from the background after the client is unblocked,
  // which is a bit more efficient.
  // The unblocking callback also replies with error if there was 0 replies from the shards,
  // and since we already replied with error in this case (in the beginning of this function),
  // we can't pass `mc` to the unblock function.
  searchRequestCtx_Free(req);
  MR_requestCompleted();
  MRCtx_Free(mc);
  return res;
}

static inline bool cannotBlockCtx(RedisModuleCtx *ctx) {
  return RedisModule_GetContextFlags(ctx) & REDISMODULE_CTX_FLAGS_DENY_BLOCKING;
}

static inline int ReplyBlockDeny(RedisModuleCtx *ctx, const RedisModuleString *cmd) {
  return RMUtil_ReplyWithErrorFmt(ctx, "Cannot perform `%s`: Cannot block", RedisModule_StringPtrLen(cmd, NULL));
}

static int genericCallUnderscoreVariant(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  size_t len;
  const char *cmd = RedisModule_StringPtrLen(argv[0], &len);
  RS_ASSERT(!strncasecmp(cmd, "FT.", 3));
  char *localCmd;
  rm_asprintf(&localCmd, "_%.*s", len, cmd);
  /*
   * v - argv input array of RedisModuleString
   * E - return errors as RedisModuleCallReply object (instead of NULL)
   * M - respect OOM
   * 0 - same RESP protocol
   * ! - replicate the command if needed (allows for replication)
   * NOTICE: We don't add the `C` flag, such that the user that runs the internal
   * command is the unrestricted user. Such that it can execute internal commands
   * even if the dispatching user does not have such permissions (we reach here
   * only on OSS with 1 shard due to the mechanism of this function).
   * This is OK because the user already passed the ACL command validation (keys - TBD)
   * before reaching the non-underscored command command-handler.
   */

  RedisModuleCallReply *r = RedisModule_Call(ctx, localCmd, "vEM0!", argv + 1, argc - 1);
  RedisModule_ReplyWithCallReply(ctx, r); // Pass the reply to the client
  rm_free(localCmd);
  RedisModule_FreeCallReply(r);
  return REDISMODULE_OK;
}

/* FT.MGET {idx} {key} ... */
int MGetCommandHandler(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  if (argc < 3) {
    return RedisModule_WrongArity(ctx);
  } else if (!SearchCluster_Ready()) {
    // Check that the cluster state is valid
    return RedisModule_ReplyWithError(ctx, CLUSTERDOWN_ERR);
  }
  RS_AutoMemory(ctx);

  VERIFY_ACL(ctx, argv[1])

  if (NumShards == 1) {
    return genericCallUnderscoreVariant(ctx, argv, argc);
  } else if (cannotBlockCtx(ctx)) {
    return ReplyBlockDeny(ctx, argv[0]);
  }

  MRCommand cmd = MR_NewCommandFromRedisStrings(argc, argv);
  MRCommand_SetProtocol(&cmd, ctx);
  /* Replace our own FT command with _FT. command */
  MRCommand_SetPrefix(&cmd, "_FT");

  struct MRCtx *mrctx = MR_CreateCtx(ctx, 0, NULL, NumShards);
  MR_Fanout(mrctx, mergeArraysReducer, cmd, true);
  return REDISMODULE_OK;
}

int SpellCheckCommandHandler(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  if (NumShards == 0) {
    // Cluster state is not ready
    return RedisModule_ReplyWithError(ctx, CLUSTERDOWN_ERR);
  } else if (argc < 3) {
    return RedisModule_WrongArity(ctx);
  }
  RS_AutoMemory(ctx);

  VERIFY_ACL(ctx, argv[1])

  if (NumShards == 1) {
    return SpellCheckCommand(ctx, argv, argc);
  } else if (cannotBlockCtx(ctx)) {
    return ReplyBlockDeny(ctx, argv[0]);
  }

  MRCommand cmd = MR_NewCommandFromRedisStrings(argc, argv);
  MRCommand_SetProtocol(&cmd, ctx);
  /* Replace our own FT command with _FT. command */
  MRCommand_SetPrefix(&cmd, "_FT");

  MRCommand_Insert(&cmd, 3, "FULLSCOREINFO", sizeof("FULLSCOREINFO") - 1);

  struct MRCtx *mrctx = MR_CreateCtx(ctx, 0, NULL, NumShards);
  MR_Fanout(mrctx, is_resp3(ctx) ? spellCheckReducer_resp3 : spellCheckReducer_resp2, cmd, true);
  return REDISMODULE_OK;
}

static int MastersFanoutCommandHandler(RedisModuleCtx *ctx,
  RedisModuleString **argv, int argc, int indexNamePos) {
  if (argc < 2) {
    return RedisModule_WrongArity(ctx);
  } else if (!SearchCluster_Ready()) {
    // Check that the cluster state is valid
    return RedisModule_ReplyWithError(ctx, CLUSTERDOWN_ERR);
  }
  RS_AutoMemory(ctx);

  // Validate ACL key permissions if needed (for commands that access an index)
  if (indexNamePos != -1) {
    if (indexNamePos >= argc) {
      return RedisModule_WrongArity(ctx);
    }
    VERIFY_ACL(ctx, argv[indexNamePos])
  }

  if (NumShards == 1) {
    // There is only one shard in the cluster. We can handle the command locally.
    return genericCallUnderscoreVariant(ctx, argv, argc);
  } else if (cannotBlockCtx(ctx)) {
    return ReplyBlockDeny(ctx, argv[0]);
  }

  MRCommand cmd = MR_NewCommandFromRedisStrings(argc, argv);
  MRCommand_SetProtocol(&cmd, ctx);
  /* Replace our own FT command with _FT. command */
  MRCommand_SetPrefix(&cmd, "_FT");
  struct MRCtx *mrctx = MR_CreateCtx(ctx, 0, NULL, NumShards);

  MR_Fanout(mrctx, allOKReducer, cmd, true);
  return REDISMODULE_OK;
}

static int FanoutCommandHandlerWithIndexAtFirstArg(RedisModuleCtx *ctx,
  RedisModuleString **argv, int argc) {
  return MastersFanoutCommandHandler(ctx, argv, argc, 1);
}

static int FanoutCommandHandlerWithIndexAtSecondArg(RedisModuleCtx *ctx,
  RedisModuleString **argv, int argc) {
  return MastersFanoutCommandHandler(ctx, argv, argc, 2);
}

static int FanoutCommandHandlerIndexless(RedisModuleCtx *ctx,
  RedisModuleString **argv, int argc) {
  return MastersFanoutCommandHandler(ctx, argv, argc, -1);
}

// Supports FT.ADD, FT.DEL, FT.GET, FT.SUGADD, FT.SUGGET, FT.SUGDEL, FT.SUGLEN.
// If needed for more commands, make sure `MRCommand_GetShardingKey` is implemented for them.
// Notice that only OSS cluster should deal with such redirections.
static int SingleShardCommandHandler(RedisModuleCtx *ctx,
  RedisModuleString **argv, int argc, int indexNamePos) {
  if (argc < 2) {
    return RedisModule_WrongArity(ctx);
  } else if (!SearchCluster_Ready()) {
    return RedisModule_ReplyWithError(ctx, CLUSTERDOWN_ERR);
  }
  RS_AutoMemory(ctx);

  // Validate ACL key permissions if needed (for commands that access an index)
  if (indexNamePos != -1) {
    if (indexNamePos >= argc) {
      return RedisModule_WrongArity(ctx);
    }
    VERIFY_ACL(ctx, argv[indexNamePos])
  }

  if (NumShards == 1) {
    // There is only one shard in the cluster. We can handle the command locally.
    return genericCallUnderscoreVariant(ctx, argv, argc);
  } else if (cannotBlockCtx(ctx)) {
    return ReplyBlockDeny(ctx, argv[0]);
  }

  MRCommand cmd = MR_NewCommandFromRedisStrings(argc, argv);
  MRCommand_SetProtocol(&cmd, ctx);
  /* Replace our own FT command with _FT. command */
  MRCommand_SetPrefix(&cmd, "_FT");

  MR_MapSingle(MR_CreateCtx(ctx, 0, NULL, NumShards), singleReplyReducer, cmd);

  return REDISMODULE_OK;
}

static int SingleShardCommandHandlerWithIndexAtFirstArg(RedisModuleCtx *ctx,
  RedisModuleString **argv, int argc) {
  return SingleShardCommandHandler(ctx, argv, argc, 1);
}

void RSExecDistAggregate(RedisModuleCtx *ctx, RedisModuleString **argv, int argc,
                         struct ConcurrentCmdCtx *cmdCtx);
int RSAggregateCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);

/** Debug */
void DEBUG_RSExecDistAggregate(RedisModuleCtx *ctx, RedisModuleString **argv, int argc,
                         struct ConcurrentCmdCtx *cmdCtx);

int DistAggregateCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  if (NumShards == 0) {
    return RedisModule_ReplyWithError(ctx, CLUSTERDOWN_ERR);
  } else if (argc < 3) {
    return RedisModule_WrongArity(ctx);
  }

  // Coord callback
  ConcurrentCmdHandler dist_callback = RSExecDistAggregate;

  bool isDebug = (RMUtil_ArgIndex("_FT.DEBUG", argv, 1) != -1);
  if (isDebug) {
    argv++;
    argc--;
    dist_callback = DEBUG_RSExecDistAggregate;
  }

  // Prepare the spec ref for the background thread
  const char *idx = RedisModule_StringPtrLen(argv[1], NULL);
  IndexLoadOptions lopts = {.nameC = idx, .flags = INDEXSPEC_LOAD_NOCOUNTERINC};
  StrongRef spec_ref = IndexSpec_LoadUnsafeEx(&lopts);
  IndexSpec *sp = StrongRef_Get(spec_ref);
  if (!sp) {
    // Reply with error
    return RedisModule_ReplyWithErrorFormat(ctx, "No such index %s", idx);
  }

  bool isProfile = (RMUtil_ArgIndex("FT.PROFILE", argv, 1) != -1);
  // Check the ACL key permissions of the user w.r.t the queried index (only if
  // not profiling, as it was already checked earlier).
  if (!isProfile && !ACLUserMayAccessIndex(ctx, sp)) {
    return RedisModule_ReplyWithError(ctx, NOPERM_ERR);
  }

  if (NumShards == 1) {
    // There is only one shard in the cluster. We can handle the command locally.
    return RSAggregateCommand(ctx, argv, argc);
  } else if (cannotBlockCtx(ctx)) {
    return ReplyBlockDeny(ctx, argv[0]);
  }

  return ConcurrentSearch_HandleRedisCommandEx(DIST_THREADPOOL, CMDCTX_NO_GIL,
                                               dist_callback, ctx, argv, argc,
                                               StrongRef_Demote(spec_ref));
}

static void CursorCommandInternal(RedisModuleCtx *ctx, RedisModuleString **argv, int argc, struct ConcurrentCmdCtx *cmdCtx) {
  RSCursorCommand(ctx, argv, argc);
}

static int CursorCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  if (argc < 4) {
    return RedisModule_WrongArity(ctx);
  } else if (!SearchCluster_Ready()) {
    return RedisModule_ReplyWithError(ctx, CLUSTERDOWN_ERR);
  }

  VERIFY_ACL(ctx, argv[2])

  if (NumShards == 1) {
    // There is only one shard in the cluster. We can handle the command locally.
    return RSCursorCommand(ctx, argv, argc);
  } else if (cannotBlockCtx(ctx)) {
    return ReplyBlockDeny(ctx, argv[0]);
  }

  return ConcurrentSearch_HandleRedisCommandEx(DIST_THREADPOOL, CMDCTX_NO_GIL,
                                               CursorCommandInternal, ctx, argv, argc,
                                               (WeakRef){0});
}

int TagValsCommandHandler(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  if (argc < 3) {
    return RedisModule_WrongArity(ctx);
  } else if (!SearchCluster_Ready()) {
    // Check that the cluster state is valid
    return RedisModule_ReplyWithError(ctx, CLUSTERDOWN_ERR);
  }
  RS_AutoMemory(ctx);

  VERIFY_ACL(ctx, argv[1])

  if (NumShards == 1) {
    return genericCallUnderscoreVariant(ctx, argv, argc);
  } else if (cannotBlockCtx(ctx)) {
    return ReplyBlockDeny(ctx, argv[0]);
  }

  MRCommand cmd = MR_NewCommandFromRedisStrings(argc, argv);
  MRCommand_SetProtocol(&cmd, ctx);
  /* Replace our own FT command with _FT. command */
  MRCommand_SetPrefix(&cmd, "_FT");

  MR_Fanout(MR_CreateCtx(ctx, 0, NULL, NumShards), uniqueStringsReducer, cmd, true);
  return REDISMODULE_OK;
}

int InfoCommandHandler(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  if (argc != 2) {
    // FT.INFO {index}
    return RedisModule_WrongArity(ctx);
  } else if (!SearchCluster_Ready()) {
    // Check that the cluster state is valid
    return RedisModule_ReplyWithError(ctx, CLUSTERDOWN_ERR);
  }
  RS_AutoMemory(ctx);

  VERIFY_ACL(ctx, argv[1])

  if (NumShards == 1) {
    // There is only one shard in the cluster. We can handle the command locally.
    return IndexInfoCommand(ctx, argv, argc);
  } else if (cannotBlockCtx(ctx)) {
    return ReplyBlockDeny(ctx, argv[0]);
  }

  MRCommand cmd = MR_NewCommandFromRedisStrings(argc, argv);
  MRCommand_Append(&cmd, WITH_INDEX_ERROR_TIME, strlen(WITH_INDEX_ERROR_TIME));
  MRCommand_SetProtocol(&cmd, ctx);
  MRCommand_SetPrefix(&cmd, "_FT");

  struct MRCtx *mctx = MR_CreateCtx(ctx, 0, NULL, NumShards);
  MR_SetCoordinationStrategy(mctx, false); // send to all shards (not just the masters)
  MR_Fanout(mctx, InfoReplyReducer, cmd, true);
  return REDISMODULE_OK;
}

void sendRequiredFields(searchRequestCtx *req, MRCommand *cmd) {
  size_t specialCasesLen = array_len(req->specialCases);
  size_t offset = 0;
  for(size_t i=0; i < specialCasesLen; i++) {
    specialCaseCtx* ctx = req->specialCases[i];
    switch (ctx->specialCaseType) {
      // Handle sortby
      case SPECIAL_CASE_SORTBY: {
        // Sort by is always the first case.
        RS_ASSERT(i==0);
        if(req->requiredFields == NULL) {
          req->requiredFields = array_new(const char*, 1);
        }
        array_append(req->requiredFields, ctx->sortby.sortKey);
        // Sortkey is the first required key value to return
        ctx->sortby.offset = 0;
        offset++;
        break;
      }
      case SPECIAL_CASE_KNN: {
        // Before requesting for a new field, see if it is not the sortkey.
        if(!ctx->knn.shouldSort) {
            // We have already requested this field, we will not append it.
            ctx->knn.offset = 0;
            break;;
        }
        // Fall back into appending new required field.
        if(req->requiredFields == NULL) {
          req->requiredFields = array_new(const char*, 1);
        }
        array_append(req->requiredFields, ctx->knn.fieldName);
        ctx->knn.offset = offset++;
        break;
      }
      default:
        break;
    }
  }

  if(req->requiredFields) {
    MRCommand_Append(cmd, "_REQUIRED_FIELDS", strlen("_REQUIRED_FIELDS"));
    int numberOfFields = array_len(req->requiredFields);
    char snum[8];
    int len = sprintf(snum, "%d", numberOfFields);
    MRCommand_Append(cmd, snum, len);
    for(size_t i = 0; i < numberOfFields; i++) {
        MRCommand_Append(cmd, req->requiredFields[i], strlen(req->requiredFields[i]));
    }
  }
}

static void bailOut(RedisModuleBlockedClient *bc, QueryError *status) {
  RedisModuleCtx* clientCtx = RedisModule_GetThreadSafeContext(bc);
  QueryError_ReplyAndClear(clientCtx, status);
  RedisModule_BlockedClientMeasureTimeEnd(bc);
  RedisModule_UnblockClient(bc, NULL);
  RedisModule_FreeThreadSafeContext(clientCtx);
}

static int prepareCommand(MRCommand *cmd, searchRequestCtx *req, RedisModuleBlockedClient *bc, int protocol,
  RedisModuleString **argv, int argc, WeakRef spec_ref, QueryError *status) {

  cmd->protocol = protocol;

  // replace the LIMIT {offset} {limit} with LIMIT 0 {limit}, because we need all top N to merge
  int limitIndex = RMUtil_ArgExists("LIMIT", argv, argc, 3);
  if (limitIndex && req->limit > 0 && limitIndex < argc - 2) {
    size_t k =0;
    MRCommand_ReplaceArg(cmd, limitIndex + 1, "0", 1);
    char buf[32];
    snprintf(buf, sizeof(buf), "%lld", req->requestedResultsCount);
    MRCommand_ReplaceArg(cmd, limitIndex + 2, buf, strlen(buf));
  }

  /* Replace our own FT command with _FT. command */
  if (req->profileArgs == 0) {
    MRCommand_ReplaceArg(cmd, 0, "_FT.SEARCH", sizeof("_FT.SEARCH") - 1);
  } else {
    MRCommand_ReplaceArg(cmd, 0, "_FT.PROFILE", sizeof("_FT.PROFILE") - 1);
  }

  // adding the WITHSCORES option only if there is no SORTBY (hence the score is the default sort key)
  if (!req->withSortby) {
    MRCommand_Insert(cmd, 3 + req->profileArgs, "WITHSCORES", sizeof("WITHSCORES") - 1);
  }

  if(req->specialCases) {
    sendRequiredFields(req, cmd);
  }

  // Append the prefixes of the index to the command
  StrongRef strong_ref = IndexSpecRef_Promote(spec_ref);
  IndexSpec *sp = StrongRef_Get(strong_ref);
  if (!sp) {
    MRCommand_Free(cmd);
    QueryError_SetCode(status, QUERY_EDROPPEDBACKGROUND);

    bailOut(bc, status);
    return REDISMODULE_ERR;
  }

  uint16_t arg_pos = 3 + req->profileArgs;
  MRCommand_Insert(cmd, arg_pos++, "_INDEX_PREFIXES", sizeof("_INDEX_PREFIXES") - 1);
  arrayof(HiddenUnicodeString*) prefixes = sp->rule->prefixes;
  char *n_prefixes;
  int string_len = rm_asprintf(&n_prefixes, "%u", array_len(prefixes));
  MRCommand_Insert(cmd, arg_pos++, n_prefixes, string_len);
  rm_free(n_prefixes);

  for (uint i = 0; i < array_len(prefixes); i++) {
    size_t len;
    const char* prefix = HiddenUnicodeString_GetUnsafe(prefixes[i], &len);
    MRCommand_Insert(cmd, arg_pos++, prefix, len);
  }

  // Return spec references, no longer needed
  IndexSpecRef_Release(strong_ref);
  WeakRef_Release(spec_ref);


  return REDISMODULE_OK;
}

static searchRequestCtx *createReq(RedisModuleString **argv, int argc, RedisModuleBlockedClient *bc, QueryError *status) {
  searchRequestCtx *req = rscParseRequest(argv, argc, status);

  if (!req) {
    bailOut(bc, status);
    return NULL;
  }
  return req;
}

int FlatSearchCommandHandler(RedisModuleBlockedClient *bc, int protocol,
  RedisModuleString **argv, int argc, WeakRef spec_ref) {
  QueryError status = {0};

  searchRequestCtx *req = createReq(argv, argc, bc, &status);

  if (!req) {
    return REDISMODULE_OK;
  }

  MRCommand cmd = MR_NewCommandFromRedisStrings(argc, argv);
  int rc = prepareCommand(&cmd, req, bc, protocol, argv, argc, spec_ref, &status);
  if (!(rc == REDISMODULE_OK)) {
    return REDISMODULE_OK;
  }
  // Here we have an unsafe read of `NumShards`. This is fine because its just a hint.
  struct MRCtx *mrctx = MR_CreateCtx(0, bc, req, NumShards);

  MRCtx_SetReduceFunction(mrctx, searchResultReducer_background);
  MR_Fanout(mrctx, NULL, cmd, false);
  return REDISMODULE_OK;
}

typedef struct SearchCmdCtx {
  RedisModuleString **argv;
  int argc;
  RedisModuleBlockedClient* bc;
  int protocol;
  WeakRef spec_ref;
} SearchCmdCtx;

static void DistSearchCommandHandler(void* pd) {
  SearchCmdCtx* sCmdCtx = pd;
  FlatSearchCommandHandler(sCmdCtx->bc, sCmdCtx->protocol, sCmdCtx->argv, sCmdCtx->argc, sCmdCtx->spec_ref);
  for (size_t i = 0 ; i < sCmdCtx->argc ; ++i) {
    RedisModule_FreeString(NULL, sCmdCtx->argv[i]);
  }
  rm_free(sCmdCtx->argv);
  rm_free(sCmdCtx);
}

// If the client is unblocked with a private data, we have to free it.
// This currently happens only when the client is unblocked without calling its reduce function,
// because we expect 0 replies. This function handles this case as well.
static int DistSearchUnblockClient(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  struct MRCtx *mrctx = RedisModule_GetBlockedClientPrivateData(ctx);
  if (mrctx) {
    if (MRCtx_GetNumReplied(mrctx) == 0) {
      RedisModule_ReplyWithError(ctx, "Could not send query to cluster");
    }
    searchRequestCtx_Free(MRCtx_GetPrivData(mrctx));
    MR_requestCompleted();
    MRCtx_Free(mrctx);
  }
  return REDISMODULE_OK;
}

int RSSearchCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);

int DistSearchCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  if (NumShards == 0) {
    return RedisModule_ReplyWithError(ctx, CLUSTERDOWN_ERR);
  } else if (argc < 3) {
    return RedisModule_WrongArity(ctx);
  }

  // Coord callback
  void (*dist_callback)(void *) = DistSearchCommandHandler;

  bool isDebug = (RMUtil_ArgIndex("_FT.DEBUG", argv, 1) != -1);
  if (isDebug) {
    argv++;
    argc--;
    dist_callback = DEBUG_DistSearchCommandHandler;
  }

  // Prepare spec ref for the background thread
  const char *idx = RedisModule_StringPtrLen(argv[1], NULL);
  IndexLoadOptions lopts = {.nameC = idx, .flags = INDEXSPEC_LOAD_NOCOUNTERINC};
  StrongRef spec_ref = IndexSpec_LoadUnsafeEx(&lopts);
  IndexSpec *sp = StrongRef_Get(spec_ref);
  if (!sp) {
    // Reply with error
    return RedisModule_ReplyWithErrorFormat(ctx, "No such index %s", idx);
  }

  bool isProfile = (RMUtil_ArgIndex("FT.PROFILE", argv, 1) != -1);
  // Check the ACL key permissions of the user w.r.t the queried index (only if
  // not profiling, as it was already checked).
  if (!isProfile && !ACLUserMayAccessIndex(ctx, sp)) {
    return RedisModule_ReplyWithError(ctx, NOPERM_ERR);
  }

  if (NumShards == 1) {
    // There is only one shard in the cluster. We can handle the command locally.
    return RSSearchCommand(ctx, argv, argc);
  } else if (cannotBlockCtx(ctx)) {
    return ReplyBlockDeny(ctx, argv[0]);
  }

  SearchCmdCtx* sCmdCtx = rm_malloc(sizeof(*sCmdCtx));
  sCmdCtx->spec_ref = StrongRef_Demote(spec_ref);

  RedisModuleBlockedClient* bc = RedisModule_BlockClient(ctx, DistSearchUnblockClient, NULL, NULL, 0);
  sCmdCtx->argv = rm_malloc(sizeof(RedisModuleString*) * argc);
  for (size_t i = 0 ; i < argc ; ++i) {
    // We need to copy the argv because it will be freed in the callback (from another thread).
    sCmdCtx->argv[i] = RedisModule_CreateStringFromString(ctx, argv[i]);
  }
  sCmdCtx->argc = argc;
  sCmdCtx->bc = bc;
  sCmdCtx->protocol = is_resp3(ctx) ? 3 : 2;
  RedisModule_BlockedClientMeasureTimeStart(bc);

  ConcurrentSearch_ThreadPoolRun(dist_callback, sCmdCtx, DIST_THREADPOOL);

  return REDISMODULE_OK;
}

int RSProfileCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int ProfileCommandHandler(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  if (argc < 5) {
    return RedisModule_WrongArity(ctx);
  }

  if (RMUtil_ArgExists("WITHCURSOR", argv, argc, 3)) {
    return RedisModule_ReplyWithError(ctx, "FT.PROFILE does not support cursor");
  }

  VERIFY_ACL(ctx, argv[1])

  if (NumShards == 1) {
    // There is only one shard in the cluster. We can handle the command locally.
    // We must first check that we don't have a cursor, as the local command handler allows cursors
    // for multi-shard clusters support.
    return RSProfileCommand(ctx, argv, argc);
  }

  if (RMUtil_ArgExists("SEARCH", argv, 3, 2)) {
    return DistSearchCommand(ctx, argv, argc);
  }
  if (RMUtil_ArgExists("AGGREGATE", argv, 3, 2)) {
    return DistAggregateCommand(ctx, argv, argc);
  }
  return RedisModule_ReplyWithError(ctx, "No `SEARCH` or `AGGREGATE` provided");
}

int ClusterInfoCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  if (MR_CurrentTopologyExists()) {
    // If we have a topology, we must read it from the uv thread
    MR_uvReplyClusterInfo(ctx);
  } else {
    // If we don't have a topology, we can reply immediately
    MR_ReplyClusterInfo(ctx, NULL);
  }
  return REDISMODULE_OK;
}

// A special command for redis cluster OSS, that refreshes the cluster state
int RefreshClusterCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  UpdateTopology(ctx);
  return RedisModule_ReplyWithSimpleString(ctx, "OK");
}

int SetClusterCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  MRClusterTopology *topo = RedisEnterprise_ParseTopology(ctx, argv, argc);
  // this means a parsing error, the parser already sent the explicit error to the client
  if (!topo) {
    return REDISMODULE_ERR;
  }

  RedisModule_Log(ctx, "debug", "Setting number of partitions to %ld", topo->numShards);
  NumShards = topo->numShards;

  // send the topology to the cluster
  MR_UpdateTopology(topo);
  return RedisModule_ReplyWithSimpleString(ctx, "OK");
}

/* Perform basic configurations and init all threads and global structures */
static int initSearchCluster(RedisModuleCtx *ctx, RedisModuleString **argv, int argc, bool isClusterEnabled) {
  RedisModule_Log(ctx, "notice",
                  "Cluster configuration: AUTO partitions, type: %d, coordinator timeout: %dms",
                  clusterConfig.type, clusterConfig.timeoutMS);

  if (clusterConfig.type == ClusterType_RedisOSS) {
    if (isClusterEnabled) {
      // Init the topology updater cron loop.
      InitRedisTopologyUpdater(ctx);
    } else {
      // We are not in cluster mode. No need to init the topology updater cron loop.
      // Set the number of shards to 1 to indicate the topology is "set"
      NumShards = 1;
    }
  }

  size_t num_connections_per_shard;
  if (clusterConfig.connPerShard) {
    num_connections_per_shard = clusterConfig.connPerShard;
  } else {
    // default
    num_connections_per_shard = RSGlobalConfig.numWorkerThreads + 1;
  }

  MRCluster *cl = MR_NewCluster(NULL, num_connections_per_shard);
  MR_Init(cl, clusterConfig.timeoutMS);

  return REDISMODULE_OK;
}

size_t GetNumShards_UnSafe() {
  return NumShards;
}

/** A dummy command handler, for commands that are disabled when running the module in OSS
 * clusters
 * when it is not an internal OSS build. */
int DisabledCommandHandler(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  return RedisModule_ReplyWithError(ctx, "Module Disabled in Open Source Redis");
}

/** A wrapper function that safely checks whether we are running in OSS cluster when registering
 * commands.
 * If we are, and the module was not compiled for oss clusters, this wrapper will return a pointer
 * to a dummy function disabling the actual handler.
 *
 * If we are running in RLEC or in a special OSS build - we simply return the original command.
 *
 * All coordinator handlers must be wrapped in this decorator.
 */
static RedisModuleCmdFunc SafeCmd(RedisModuleCmdFunc f) {
  if (IsEnterprise() && clusterConfig.type != ClusterType_RedisLabs) {
    /* If we are running inside OSS cluster and not built for oss, we return the dummy handler */
    return DisabledCommandHandler;
  }

  /* Valid - we return the original function */
  return f;
}

/**
 * A wrapper function to override hiredis allocators with redis allocators.
 * It should be called after RedisModule_Init.
 */
void setHiredisAllocators(){
  hiredisAllocFuncs ha = {
    .mallocFn = rm_malloc,
    .callocFn = rm_calloc,
    .reallocFn = rm_realloc,
    .strdupFn = rm_strdup,
    .freeFn = rm_free,
  };

  hiredisSetAllocators(&ha);
}

void Coordinator_ShutdownEvent(RedisModuleCtx *ctx, RedisModuleEvent eid, uint64_t subevent, void *data) {
  RedisModule_Log(ctx, "notice", "%s", "Begin releasing RediSearch resources on shutdown");
  RediSearch_CleanupModule();
  RedisModule_Log(ctx, "notice", "%s", "End releasing RediSearch resources");
}

void Initialize_CoordKeyspaceNotifications(RedisModuleCtx *ctx) {
  // To be called after `Initialize_ServerEventNotifications` as callbacks are overridden.
  if (RedisModule_SubscribeToServerEvent && getenv("RS_GLOBAL_DTORS")) {
    // clear resources when the server exits
    // used only with sanitizer or valgrind
    RedisModule_Log(ctx, "notice", "%s", "Subscribe to clear resources on shutdown");
    RedisModule_SubscribeToServerEvent(ctx, RedisModuleEvent_Shutdown, Coordinator_ShutdownEvent);
  }
}

static bool checkClusterEnabled(RedisModuleCtx *ctx) {
  RedisModuleCallReply *rep = RedisModule_Call(ctx, "CONFIG", "cc", "GET", "cluster-enabled");
  RS_ASSERT_ALWAYS(rep && RedisModule_CallReplyType(rep) == REDISMODULE_REPLY_ARRAY &&
                     RedisModule_CallReplyLength(rep) == 2);
  size_t len;
  const char *isCluster = RedisModule_CallReplyStringPtr(RedisModule_CallReplyArrayElement(rep, 1), &len);
  bool isClusterEnabled = STR_EQCASE(isCluster, len, "yes");
  RedisModule_FreeCallReply(rep);
  return isClusterEnabled;
}

int ConfigCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);

int RediSearch_InitModuleConfig(RedisModuleCtx *ctx, RedisModuleString **argv, int argc, int registerConfiguration, int isClusterEnabled) {
  // register the module configuration with redis, use loaded values from command line as defaults
  if (registerConfiguration) {
    if (RegisterModuleConfig(ctx) == REDISMODULE_ERR) {
      RedisModule_Log(ctx, "warning", "Error registering module configuration");
      return REDISMODULE_ERR;
    }
    if (isClusterEnabled) {
      // Register module configuration parameters for cluster
      RM_TRY_F(RegisterClusterModuleConfig, ctx);
    }
  }

  // Load default values
  RM_TRY_F(RedisModule_LoadDefaultConfigs, ctx);

  char *err = NULL;
  // Read module configuration from module ARGS
  if (ReadConfig(argv, argc, &err) == REDISMODULE_ERR) {
    RedisModule_Log(ctx, "warning", "Invalid Configurations: %s", err);
    rm_free(err);
    return REDISMODULE_ERR;
  }
  // Apply configuration redis has loaded from the configuration file
  RM_TRY_F(RedisModule_LoadConfigs, ctx);
  return REDISMODULE_OK;
}

int __attribute__((visibility("default")))
RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {

  if (RedisModule_Init(ctx, REDISEARCH_MODULE_NAME, REDISEARCH_MODULE_VERSION,
                       REDISMODULE_APIVER_1) == REDISMODULE_ERR) {
    return REDISMODULE_ERR;
  }

  setHiredisAllocators();
  uv_replace_allocator(rm_malloc, rm_realloc, rm_calloc, rm_free);

  if (!RSDummyContext) {
    RSDummyContext = RedisModule_GetDetachedThreadSafeContext(ctx);
  }

  // Chain the config into RediSearch's global config and set the default values
  clusterConfig = DEFAULT_CLUSTER_CONFIG;
  RSConfigOptions_AddConfigs(&RSGlobalConfigOptions, GetClusterConfigOptions());
  ClusterConfig_RegisterTriggers();

  // Register the module configuration parameters
  GetRedisVersion(ctx);

  // Check if we are actually in cluster mode
  const bool isClusterEnabled = checkClusterEnabled(ctx);
  const Version unstableRedis = {7, 9, 227};
  const bool unprefixedConfigSupported = (CompareVersions(redisVersion, unstableRedis) >= 0) ? true : false;

  legacySpecRules = dictCreate(&dictTypeHeapHiddenStrings, NULL);

  if (RediSearch_InitModuleConfig(ctx, argv, argc, unprefixedConfigSupported, isClusterEnabled) == REDISMODULE_ERR) {
    return REDISMODULE_ERR;
  }

  // Init RediSearch internal search
  if (RediSearch_InitModuleInternal(ctx) == REDISMODULE_ERR) {
    RedisModule_Log(ctx, "warning", "Could not init search library...");
    return REDISMODULE_ERR;
  }

  // Init the global cluster structs
  if (initSearchCluster(ctx, argv, argc, isClusterEnabled) == REDISMODULE_ERR) {
    RedisModule_Log(ctx, "warning", "Could not init MR search cluster");
    return REDISMODULE_ERR;
  }

  // Init the aggregation thread pool
  DIST_THREADPOOL = ConcurrentSearch_CreatePool(clusterConfig.coordinatorPoolSize);

  Initialize_CoordKeyspaceNotifications(ctx);

  if (RedisModule_ACLCheckKeyPrefixPermissions == NULL) {
    // Running against a Redis version that does not support module ACL protection
    RedisModule_Log(ctx, "warning", "Redis version does not support ACL API necessary for index protection");
  }

  // read commands
  if (clusterConfig.type == ClusterType_RedisLabs) {
    RM_TRY(RMCreateSearchCommand(ctx, "FT.AGGREGATE",
           SafeCmd(DistAggregateCommand), "readonly", 0, 1, -2, "read", false))
  } else {
    RM_TRY(RMCreateSearchCommand(ctx, "FT.AGGREGATE",
           SafeCmd(DistAggregateCommand), "readonly", 0, 0, -1, "read", false))
  }
  RM_TRY(RMCreateSearchCommand(ctx, "FT.HYBRID",
    SafeCmd(hybridCommandHandler), "readonly", 0, 0, -1, "read", false))
  RM_TRY(RMCreateSearchCommand(ctx, "FT.INFO", SafeCmd(InfoCommandHandler), "readonly", 0, 0, -1, "", false))
  RM_TRY(RMCreateSearchCommand(ctx, "FT.SEARCH", SafeCmd(DistSearchCommand), "readonly", 0, 0, -1, "read", false))
  RM_TRY(RMCreateSearchCommand(ctx, "FT.PROFILE", SafeCmd(ProfileCommandHandler), "readonly", 0, 0, -1, "read", false))
  if (clusterConfig.type == ClusterType_RedisLabs) {
    RM_TRY(RMCreateSearchCommand(ctx, "FT.CURSOR", SafeCmd(CursorCommand), "readonly", 3, 1, -3, "read", false))
  } else {
    RM_TRY(RMCreateSearchCommand(ctx, "FT.CURSOR", SafeCmd(CursorCommand), "readonly", 0, 0, -1, "read", false))
  }
  RM_TRY(RMCreateSearchCommand(ctx, "FT.SPELLCHECK", SafeCmd(SpellCheckCommandHandler), "readonly", 0, 0, -1, "", false))
  // Assumes "_FT.DEBUG" is registered (from `RediSearch_InitModuleInternal`)
  RM_TRY(RegisterCoordDebugCommands(RedisModule_GetCommand(ctx, "_FT.DEBUG")));

// OSS commands (registered via proxy in Enterprise)
#ifndef RS_CLUSTER_ENTERPRISE
    if (!isClusterEnabled) {
      // Register the config command with `FT.` prefix only if we are not in cluster mode as an alias
      RM_TRY(RMCreateSearchCommand(ctx, "FT.CONFIG", SafeCmd(ConfigCommand), "readonly", 0, 0, 0, "admin", false));
    }
    RedisModule_Log(ctx, "notice", "Register write commands");
    // write commands (on enterprise we do not define them, the dmc take care of them)
    RM_TRY(RMCreateSearchCommand(ctx, "FT.CREATE", SafeCmd(FanoutCommandHandlerIndexless), "write deny-oom", 0, 0, -1, "", false))
    RM_TRY(RMCreateSearchCommand(ctx, "FT._CREATEIFNX", SafeCmd(FanoutCommandHandlerIndexless), "write deny-oom", 0, 0, -1, "", false))
    RM_TRY(RMCreateSearchCommand(ctx, "FT.ALTER", SafeCmd(FanoutCommandHandlerWithIndexAtFirstArg), "write deny-oom", 0, 0, -1, "", false))
    RM_TRY(RMCreateSearchCommand(ctx, "FT._ALTERIFNX", SafeCmd(FanoutCommandHandlerWithIndexAtFirstArg), "write deny-oom", 0, 0, -1, "", false))
    RM_TRY(RMCreateSearchCommand(ctx, "FT.DROPINDEX", SafeCmd(FanoutCommandHandlerWithIndexAtFirstArg), "write",0, 0, -1, "write slow dangerous", false))
    // TODO: Either make ALL replication commands internal (such that no need for ACL check), or add ACL check.
    RM_TRY(RMCreateSearchCommand(ctx, "FT._DROPINDEXIFX", SafeCmd(FanoutCommandHandlerIndexless), "write",0, 0, -1, "write slow dangerous", false))
    // search write slow dangerous
    RM_TRY(RMCreateSearchCommand(ctx, "FT.DICTADD", SafeCmd(FanoutCommandHandlerIndexless), "write deny-oom", 0, 0, -1, "", false))
    RM_TRY(RMCreateSearchCommand(ctx, "FT.DICTDEL", SafeCmd(FanoutCommandHandlerIndexless), "write", 0, 0, -1, "", false))
    RM_TRY(RMCreateSearchCommand(ctx, "FT.ALIASADD", SafeCmd(FanoutCommandHandlerWithIndexAtSecondArg), "write deny-oom", 0, 0, -1, "", false))
    RM_TRY(RMCreateSearchCommand(ctx, "FT._ALIASADDIFNX", SafeCmd(FanoutCommandHandlerIndexless), "write deny-oom", 0, 0, -1, "", false))
    RM_TRY(RMCreateSearchCommand(ctx, "FT.ALIASDEL", SafeCmd(FanoutCommandHandlerIndexless), "write", 0, 0, -1, "", false))
    RM_TRY(RMCreateSearchCommand(ctx, "FT._ALIASDELIFX", SafeCmd(FanoutCommandHandlerIndexless), "write", 0, 0, -1, "", false))
    RM_TRY(RMCreateSearchCommand(ctx, "FT.ALIASUPDATE", SafeCmd(FanoutCommandHandlerWithIndexAtSecondArg), "write deny-oom", 0, 0, -1, "", false))
    RM_TRY(RMCreateSearchCommand(ctx, "FT.SYNUPDATE", SafeCmd(FanoutCommandHandlerWithIndexAtFirstArg),"write deny-oom", 0, 0, -1, "", false))

    // Deprecated OSS commands
    RM_TRY(RMCreateSearchCommand(ctx, "FT.GET", SafeCmd(SingleShardCommandHandlerWithIndexAtFirstArg), "readonly", 0, 0, -1, "read", false))
    RM_TRY(RMCreateSearchCommand(ctx, "FT.ADD", SafeCmd(SingleShardCommandHandlerWithIndexAtFirstArg), "write deny-oom", 0, 0, -1, "write", false))
    RM_TRY(RMCreateSearchCommand(ctx, "FT.DEL", SafeCmd(SingleShardCommandHandlerWithIndexAtFirstArg), "write", 0, 0, -1, "write", false))
    RM_TRY(RMCreateSearchCommand(ctx, "FT.DROP", SafeCmd(FanoutCommandHandlerWithIndexAtFirstArg), "write", 0, 0, -1, "write slow dangerous", false))
    RM_TRY(RMCreateSearchCommand(ctx, "FT._DROPIFX", SafeCmd(FanoutCommandHandlerIndexless), "write", 0, 0, -1, "write", false))
#endif

  // cluster set commands. We filter from the proxy, but do not mark them as internal.
  RM_TRY(RMCreateSearchCommand(ctx, REDISEARCH_MODULE_NAME".CLUSTERSET",
         SafeCmd(SetClusterCommand),
         IsEnterprise() ? "readonly allow-loading deny-script " CMD_PROXY_FILTERED : "readonly allow-loading deny-script",
         0, 0, -1, "", false))
  RM_TRY(RMCreateSearchCommand(ctx, REDISEARCH_MODULE_NAME".CLUSTERREFRESH",
         SafeCmd(RefreshClusterCommand),
         IsEnterprise() ? "readonly deny-script " CMD_PROXY_FILTERED : "readonly deny-script",
         0, 0, -1, "", false))
  RM_TRY(RMCreateSearchCommand(ctx, REDISEARCH_MODULE_NAME".CLUSTERINFO",
         SafeCmd(ClusterInfoCommand),
         IsEnterprise() ? "readonly allow-loading deny-script " CMD_PROXY_FILTERED : "readonly allow-loading deny-script",
         0, 0, -1, "", false))

  // Deprecated commands. Grouped here for easy tracking
  RM_TRY(RMCreateSearchCommand(ctx, "FT.MGET", SafeCmd(MGetCommandHandler), "readonly", 0, 0, -1, "read", false))
  RM_TRY(RMCreateSearchCommand(ctx, "FT.TAGVALS", SafeCmd(TagValsCommandHandler), "readonly", 0, 0, -1, "read slow dangerous", false))

  return REDISMODULE_OK;
}

int RedisModule_OnUnload(RedisModuleCtx *ctx) {
  if (config_ext_load) {
    RedisModule_FreeString(ctx, config_ext_load);
    config_ext_load = NULL;
  }
  if (config_friso_ini) {
    RedisModule_FreeString(ctx, config_friso_ini);
    config_friso_ini = NULL;
  }
  if (RSGlobalConfig.extLoad) {
    rm_free((void *)RSGlobalConfig.extLoad);
    RSGlobalConfig.extLoad = NULL;
  }
  if (RSGlobalConfig.frisoIni) {
    rm_free((void *)RSGlobalConfig.frisoIni);
    RSGlobalConfig.frisoIni = NULL;
  }

  return REDISMODULE_OK;
}
/* ======================= DEBUG ONLY ======================= */

static int DEBUG_FlatSearchCommandHandler(RedisModuleBlockedClient *bc, int protocol,
  RedisModuleString **argv, int argc, WeakRef spec_ref) {
  QueryError status = {0};
  AREQ_Debug_params debug_params = parseDebugParamsCount(argv, argc, &status);

  if (debug_params.debug_params_count == 0) {
    bailOut(bc, &status);
    return REDISMODULE_OK;
  }

  int debug_argv_count = debug_params.debug_params_count + 2;
  int base_argc = argc - debug_argv_count;
  searchRequestCtx *req = createReq(argv, base_argc, bc, &status);

  if (!req) {
    return REDISMODULE_OK;
  }

  MRCommand cmd = MR_NewCommandFromRedisStrings(base_argc, argv);
  int rc = prepareCommand(&cmd, req, bc, protocol, argv, argc, spec_ref, &status);
  if (!(rc == REDISMODULE_OK)) {
    return REDISMODULE_OK;
  }

  MRCommand_Insert(&cmd, 0, "_FT.DEBUG", sizeof("_FT.DEBUG") - 1);
  // insert also debug params at the end
  for (size_t i = 0; i < debug_argv_count; i++) {
    size_t n;
    const char *arg = RedisModule_StringPtrLen(debug_params.debug_argv[i], &n);
    MRCommand_Append(&cmd, arg, n);
  }

  struct MRCtx *mrctx = MR_CreateCtx(0, bc, req, NumShards);

  MRCtx_SetReduceFunction(mrctx, searchResultReducer_background);
  MR_Fanout(mrctx, NULL, cmd, false);
  return REDISMODULE_OK;
}

static void DEBUG_DistSearchCommandHandler(void* pd) {
  SearchCmdCtx* sCmdCtx = pd;
  // send argv not including the _FT.DEBUG
  DEBUG_FlatSearchCommandHandler(sCmdCtx->bc, sCmdCtx->protocol, sCmdCtx->argv, sCmdCtx->argc, sCmdCtx->spec_ref);
  for (size_t i = 0 ; i < sCmdCtx->argc ; ++i) {
    RedisModule_FreeString(NULL, sCmdCtx->argv[i]);
  }
  rm_free(sCmdCtx->argv);
  rm_free(sCmdCtx);
}

// Structure to pass context cleanup data to main thread
typedef struct ContextCleanupData{
  RedisModuleCtx *thctx;
  RedisSearchCtx *sctx;
} ContextCleanupData;

// Callback to safely free contexts from main thread
static void freeContextsCallback(void *data) {
  ContextCleanupData *cleanup = (ContextCleanupData *)data;

  if (cleanup->sctx) {
    SearchCtx_Free(cleanup->sctx);
  }

  if (cleanup->thctx) {
    RedisModule_FreeThreadSafeContext(cleanup->thctx);
  }

  rm_free(cleanup);
}

// Public function to schedule context cleanup
void ScheduleContextCleanup(RedisModuleCtx *thctx, struct RedisSearchCtx *sctx) {
  ContextCleanupData *cleanup = rm_malloc(sizeof(ContextCleanupData));
  cleanup->thctx = thctx;
  cleanup->sctx = sctx;

  ConcurrentSearch_ThreadPoolRun(freeContextsCallback, cleanup, DIST_THREADPOOL);
}
