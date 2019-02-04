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
#include "extension.h"
#include "ext/default.h"
#include "config.h"
#include "aggregate/aggregate.h"
#include "rmalloc.h"
#include "cursor.h"
#include "version.h"
#include "debug_commads.h"
#include "spell_check.h"
#include "dictionary.h"
#include "suggest.h"
#include "numeric_index.h"

#define LOAD_INDEX(ctx, srcname, write)                                                     \
  ({                                                                                        \
    IndexSpec *sptmp = IndexSpec_Load(ctx, RedisModule_StringPtrLen(srcname, NULL), write); \
    if (sptmp == NULL) {                                                                    \
      return RedisModule_ReplyWithError(ctx, "Unknown index name");                         \
    }                                                                                       \
    sptmp;                                                                                  \
  })

/* FT.SETPAYLOAD {index} {docId} {payload} */
int SetPayloadCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {

  // nosave must be at place 4 and we must have at least 7 fields
  if (argc != 4) {
    return RedisModule_WrongArity(ctx);
  }
  RedisModule_ReplicateVerbatim(ctx);

  RedisModule_AutoMemory(ctx);

  IndexSpec *sp = IndexSpec_Load(ctx, RedisModule_StringPtrLen(argv[1], NULL), 1);
  if (sp == NULL) {
    RedisModule_ReplyWithError(ctx, "Unknown Index name");
    goto cleanup;
  }

  /* Find the document by its key */
  t_docId docId = DocTable_GetIdR(&sp->docs, argv[2]);
  if (docId == 0) {
    RedisModule_ReplyWithError(ctx, "Document not in index");
    goto cleanup;
  }

  size_t mdlen;
  const char *md = RedisModule_StringPtrLen(argv[3], &mdlen);

  if (DocTable_SetPayload(&sp->docs, docId, md, mdlen) == 0) {
    RedisModule_ReplyWithError(ctx, "Could not set payload ¯\\_(ツ)_/¯");
    goto cleanup;
  }

  RedisModule_ReplyWithSimpleString(ctx, "OK");
cleanup:

  return REDISMODULE_OK;
}

#define REPLY_KVNUM(n, k, v)                   \
  RedisModule_ReplyWithSimpleString(ctx, k);   \
  RedisModule_ReplyWithDouble(ctx, (double)v); \
  n += 2
#define REPLY_KVSTR(n, k, v)                 \
  RedisModule_ReplyWithSimpleString(ctx, k); \
  RedisModule_ReplyWithSimpleString(ctx, v); \
  n += 2

static int renderIndexOptions(RedisModuleCtx *ctx, IndexSpec *sp) {

#define ADD_NEGATIVE_OPTION(flag, str)                        \
  if (!(sp->flags & flag)) {                                  \
    RedisModule_ReplyWithStringBuffer(ctx, str, strlen(str)); \
    n++;                                                      \
  }

  RedisModule_ReplyWithSimpleString(ctx, "index_options");
  RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
  int n = 0;
  ADD_NEGATIVE_OPTION(Index_StoreFreqs, SPEC_NOFREQS_STR);
  ADD_NEGATIVE_OPTION(Index_StoreFieldFlags, SPEC_NOFIELDS_STR);
  ADD_NEGATIVE_OPTION(Index_StoreTermOffsets, SPEC_NOOFFSETS_STR);
  if (sp->flags & Index_WideSchema) {
    RedisModule_ReplyWithSimpleString(ctx, SPEC_SCHEMA_EXPANDABLE_STR);
    n++;
  }
  RedisModule_ReplySetArrayLength(ctx, n);
  return 2;
}

/* FT.INFO {index}
 *  Provide info and stats about an index
 */
int IndexInfoCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  RedisModule_AutoMemory(ctx);
  if (argc < 2) return RedisModule_WrongArity(ctx);

  IndexSpec *sp = IndexSpec_Load(ctx, RedisModule_StringPtrLen(argv[1], NULL), 1);
  if (sp == NULL) {
    return RedisModule_ReplyWithError(ctx, "Unknown Index name");
  }

  RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
  int n = 0;

  REPLY_KVSTR(n, "index_name", sp->name);

  n += renderIndexOptions(ctx, sp);

  RedisModule_ReplyWithSimpleString(ctx, "fields");
  RedisModule_ReplyWithArray(ctx, sp->numFields);
  for (int i = 0; i < sp->numFields; i++) {
    RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
    RedisModule_ReplyWithSimpleString(ctx, sp->fields[i].name);
    int nn = 1;
    REPLY_KVSTR(nn, "type", SpecTypeNames[sp->fields[i].type]);
    if (sp->fields[i].type == FIELD_FULLTEXT) {
      REPLY_KVNUM(nn, SPEC_WEIGHT_STR, sp->fields[i].textOpts.weight);
    }

    if (sp->fields[i].type == FIELD_TAG) {
      char buf[2];
      sprintf(buf, "%c", sp->fields[i].tagOpts.separator);
      REPLY_KVSTR(nn, SPEC_SEPARATOR_STR, buf);
    }
    if (FieldSpec_IsSortable(&sp->fields[i])) {
      RedisModule_ReplyWithSimpleString(ctx, SPEC_SORTABLE_STR);
      ++nn;
    }
    if (FieldSpec_IsNoStem(&sp->fields[i])) {
      RedisModule_ReplyWithSimpleString(ctx, SPEC_NOSTEM_STR);
      ++nn;
    }
    if (!FieldSpec_IsIndexable(&sp->fields[i])) {
      RedisModule_ReplyWithSimpleString(ctx, SPEC_NOINDEX_STR);
      ++nn;
    }
    RedisModule_ReplySetArrayLength(ctx, nn);
  }
  n += 2;

  REPLY_KVNUM(n, "num_docs", sp->stats.numDocuments);
  REPLY_KVNUM(n, "max_doc_id", sp->docs.maxDocId);
  REPLY_KVNUM(n, "num_terms", sp->stats.numTerms);
  REPLY_KVNUM(n, "num_records", sp->stats.numRecords);
  REPLY_KVNUM(n, "inverted_sz_mb", sp->stats.invertedSize / (float)0x100000);
  REPLY_KVNUM(n, "total_inverted_index_blocks", TotalIIBlocks);
  // REPLY_KVNUM(n, "inverted_cap_mb", sp->stats.invertedCap / (float)0x100000);

  // REPLY_KVNUM(n, "inverted_cap_ovh", 0);
  //(float)(sp->stats.invertedCap - sp->stats.invertedSize) / (float)sp->stats.invertedCap);

  REPLY_KVNUM(n, "offset_vectors_sz_mb", sp->stats.offsetVecsSize / (float)0x100000);
  // REPLY_KVNUM(n, "skip_index_size_mb", sp->stats.skipIndexesSize / (float)0x100000);
  //  REPLY_KVNUM(n, "score_index_size_mb", sp->stats.scoreIndexesSize / (float)0x100000);

  REPLY_KVNUM(n, "doc_table_size_mb", sp->docs.memsize / (float)0x100000);
  REPLY_KVNUM(n, "sortable_values_size_mb", sp->docs.sortablesSize / (float)0x100000);

  REPLY_KVNUM(n, "key_table_size_mb", TrieMap_MemUsage(sp->docs.dim.tm) / (float)0x100000);
  REPLY_KVNUM(n, "records_per_doc_avg",
              (float)sp->stats.numRecords / (float)sp->stats.numDocuments);
  REPLY_KVNUM(n, "bytes_per_record_avg",
              (float)sp->stats.invertedSize / (float)sp->stats.numRecords);
  REPLY_KVNUM(n, "offsets_per_term_avg",
              (float)sp->stats.offsetVecRecords / (float)sp->stats.numRecords);
  REPLY_KVNUM(n, "offset_bits_per_record_avg",
              8.0F * (float)sp->stats.offsetVecsSize / (float)sp->stats.offsetVecRecords);

  if (sp->gc) {
    RedisModule_ReplyWithSimpleString(ctx, "gc_stats");
    GCContext_RenderStats(sp->gc, ctx);
    n += 2;
  }

  RedisModule_ReplyWithSimpleString(ctx, "cursor_stats");
  Cursors_RenderStats(&RSCursors, sp->name, ctx);
  n += 2;

  RedisModule_ReplySetArrayLength(ctx, n);
  return REDISMODULE_OK;
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

  RedisModule_AutoMemory(ctx);
  RedisSearchCtx *sctx = NewSearchCtx(ctx, argv[1], true);
  if (sctx == NULL) {
    return RedisModule_ReplyWithError(ctx, "Unknown Index name");
  }

  RedisModule_ReplyWithArray(ctx, argc - 2);
  for (int i = 2; i < argc; i++) {
    Document doc;

    if (Redis_LoadDocument(sctx, argv[i], &doc) == REDISMODULE_ERR) {
      RedisModule_ReplyWithNull(ctx);
    } else {
      Document_ReplyFields(ctx, &doc);
      Document_Free(&doc);
    }
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

  RedisModule_AutoMemory(ctx);
  RedisSearchCtx *sctx = NewSearchCtx(ctx, argv[1], true);
  if (sctx == NULL) {
    return RedisModule_ReplyWithError(ctx, "Unknown Index name");
  }

  Document doc;

  if (Redis_LoadDocument(sctx, argv[2], &doc) == REDISMODULE_ERR) {
    RedisModule_ReplyWithNull(ctx);
  } else {
    Document_ReplyFields(ctx, &doc);
    Document_Free(&doc);
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
  }

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

  free(explainRoot);
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
 *  **NOTE**: This does not actually delete the document from the index, just marks it as deleted
 * If DD (Delete Document) is set, we also delete the document.
 */
int DeleteCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  RedisModule_AutoMemory(ctx);

  if (argc < 3 || argc > 4) return RedisModule_WrongArity(ctx);
  RedisModule_ReplicateVerbatim(ctx);

  IndexSpec *sp = IndexSpec_Load(ctx, RedisModule_StringPtrLen(argv[1], NULL), 1);
  if (sp == NULL) {
    return RedisModule_ReplyWithError(ctx, "Unknown Index name");
  }

  int delDoc = 0;
  if (argc == 4 && RMUtil_StringEqualsCaseC(argv[3], "DD")) {
    delDoc = 1;
  }

  RedisSearchCtx sctx = SEARCH_CTX_STATIC(ctx, sp);
  RedisModuleString *docKey = argv[2];

  // Get the doc ID
  t_docId id = DocTable_GetIdR(&sp->docs, docKey);
  if (id == 0) {
    return RedisModule_ReplyWithLongLong(ctx, 0);
    // ID does not exist.
  }

  for (size_t i = 0; i < sp->numFields; ++i) {
    FieldSpec *fs = sp->fields + i;
    if (fs->type != FIELD_GEO) {
      continue;
    }
    GeoIndex gi = {.ctx = &sctx, .sp = fs};
    GeoIndex_RemoveEntries(&gi, sctx.spec, id);
  }

  int rc = DocTable_DeleteR(&sp->docs, docKey);
  if (rc) {
    sp->stats.numDocuments--;

    // If needed - delete the actual doc
    if (delDoc) {

      RedisModuleKey *dk = RedisModule_OpenKey(ctx, argv[2], REDISMODULE_WRITE);
      if (dk && RedisModule_KeyType(dk) == REDISMODULE_KEYTYPE_HASH) {
        RedisModule_DeleteKey(dk);
      } else {
        RedisModule_Log(ctx, "warning", "Document %s doesn't exist",
                        RedisModule_StringPtrLen(argv[2], NULL));
      }
    }

    // Increment the index's garbage collector's scanning frequency after document deletions
    if (sp->gc) {
      GCContext_OnDelete(sp->gc);
    }
  }

  return RedisModule_ReplyWithLongLong(ctx, rc);
}

/*
## FT.SEARCH <index> <query> [NOCONTENT] [LIMIT offset num]
    [INFIELDS <num> field ...]
    [LANGUAGE lang] [VERBATIM]
    [FILTER {property} {min} {max}]
    [SLOP {slop}] [INORDER]
    [GEOFILTER {property} {lon} {lat} {radius} {unit}]

Seach the index with a textual query, returning either documents or just ids.

### Parameters:
   - index: The Fulltext index name. The index must be first created with
FT.CREATE

   - query: the text query to search. If it's more than a single word, put it
in
quotes.
   Basic syntax like quotes for exact matching is supported.

   - NOCONTENT: If it appears after the query, we only return the document ids
and not
   the content. This is useful if rediseach is only an index on an external
document collection

   - LIMIT fist num: If the parameters appear after the query, we limit the
results to the offset and number of results given. The default is 0 10

   - FILTER: Apply a numeric filter to a numeric field, with a minimum and maximum

   - GEOFILTER: Apply a radius filter to a geo field, with a given lon, lat, radius and radius
units (m, km, mi, or ft)

   - PAYLOAD: Add a payload to the query that will be exposed to custrom scoring functions.

   - INFIELDS num field1 field2 ...: If set, filter the results to ones
appearing only in specific
   fields of the document, like title or url. num is the number of specified
field arguments

   - VERBATIM: If set, we turn off stemming for the query processing. Faster
    but will yield less results

   - WITHSCORES: If set, we also return the relative internal score of each
    document. this can be used to merge results from multiple instances

   - WITHPAYLOADS: If set, we return document payloads as they were inserted, or nil if no payload
    exists.

   - NOSTOPWORDS: If set, we do not check the query for stopwords

   - SLOP slop: If set, we allow a maximal intervening number of unmatched offsets between phrase
terms.

   - INORDER: Phrase terms must appear in the document in the same order as in the query.

   - LANGUAGE lang: If set, we use a stemmer for the supplied langauge.
Defaults
to English.
   If an unsupported language is sent, the command returns an error. The
supported languages are:

   > "arabic",  "danish",    "dutch",   "english",   "finnish",    "french",
   > "german",  "hungarian", "italian", "norwegian", "portuguese", "romanian",
   > "russian", "spanish",   "swedish", "tamil",     "turkish"

### Returns:

    An array reply, where the first element is the total number of results,
and
then pairs of
    document id, and a nested array of field/value, unless NOCONTENT was given
*/
#if 0
void _SearchCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc,
                    struct ConcurrentCmdCtx *cmdCtx) {
  // at least one field, and number of field/text args must be even

  RedisModule_AutoMemory(ctx);
  RedisSearchCtx *sctx = NewSearchCtx(ctx, argv[1], true);
  if (sctx == NULL) {
    RedisModule_ReplyWithError(ctx, "Unknown Index name");
    return;
  }

  QueryError status = {0};
  RSSearchRequest *req = NULL;
  QueryParseCtx *q = NULL;
  QueryPlan *plan = NULL;

  req = ParseRequest(sctx, argv, argc, &status);
  if (req == NULL) {
    RedisModule_Log(ctx, "warning", "Error parsing request: %s", status.detail);
    RedisModule_ReplyWithError(ctx, QueryError_GetError(&status));
    goto end;
  }

  q = SearchRequest_ParseQuery(sctx, req, &status);
  if (!q && status.code != QUERY_OK) {
    RedisModule_Log(ctx, "warning", "Error parsing query: %s", QueryError_GetError(&status));
    RedisModule_ReplyWithError(ctx, QueryError_GetError(&status));
    goto end;
  }

  plan = SearchRequest_BuildPlan(sctx, req, q, &status);
  if (!plan) {
    if (QueryError_HasError(&status) && status.code != QUERY_ENORESULTS) {
      RedisModule_Log(ctx, "debug", "Error parsing query: %s", QueryError_GetError(&status));
      RedisModule_ReplyWithError(ctx, QueryError_GetError(&status));
    } else {
      /* Simulate an empty response - this means an empty query */
      RedisModule_ReplyWithArray(ctx, 1);
      RedisModule_ReplyWithLongLong(ctx, 0);
    }
    goto end;
  }

  QueryPlan_Run(plan, ctx);
  if (QueryError_HasError(&status)) {
    RedisModule_ReplyWithError(ctx, QueryError_GetError(&status));
  }

end:
  QueryError_ClearError(&status);

  if (plan) QueryPlan_Free(plan);
  if (sctx) SearchCtx_Free(sctx);
  if (req) RSSearchRequest_Free(req);
  if (q) Query_Free(q);
}

GEN_CONCURRENT_WRAPPER(SearchCommand, argc >= 3, _SearchCommand, CONCURRENT_POOL_SEARCH)
#endif

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
  if (sp->type != FIELD_TAG) {
    RedisModule_ReplyWithError(ctx, "Not a tag field");
    goto cleanup;
  }

  TagIndex *idx = TagIndex_Open(ctx, TagIndex_FormatName(sctx, field), 0, NULL);
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

  RedisModule_AutoMemory(ctx);
  RedisModule_ReplicateVerbatim(ctx);
  QueryError status = {0};

  IndexSpec *sp = IndexSpec_CreateNew(ctx, argv, argc, &status);
  if (sp == NULL) {
    RedisModule_ReplyWithError(ctx, QueryError_GetError(&status));
    QueryError_ClearError(&status);
    return REDISMODULE_OK;
  }

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
int DropIndexCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  // at least one field, and number of field/text args must be even
  if (argc < 2 || argc > 3) {
    return RedisModule_WrongArity(ctx);
  }
  RedisModule_ReplicateVerbatim(ctx);

  RedisModule_AutoMemory(ctx);

  IndexSpec *sp = IndexSpec_Load(ctx, RedisModule_StringPtrLen(argv[1], NULL), 0);
  if (sp == NULL) {
    return RedisModule_ReplyWithError(ctx, "Unknown Index name");
  }

  // Optional KEEPDOCS
  int delDocs = 1;
  if (argc == 3 && RMUtil_StringEqualsCaseC(argv[2], "KEEPDOCS")) {
    delDocs = 0;
  }

  RedisSearchCtx sctx = SEARCH_CTX_STATIC(ctx, sp);
  Redis_DropIndex(&sctx, delDocs, true);
  return RedisModule_ReplyWithSimpleString(ctx, "OK");
}

/**
 * FT.SYNADD <index> <term1> <term2> ...
 *
 * Add a synonym group to the given index. The synonym data structure is compose of synonyms
 * groups. Each Synonym group has a unique id. The SYNADD command creates a new synonym group with
 * the given terms and return its id.
 */
int SynAddCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  if (argc < 3) return RedisModule_WrongArity(ctx);

  IndexSpec *sp = IndexSpec_Load(ctx, RedisModule_StringPtrLen(argv[1], NULL), 0);
  if (!sp) {
    RedisModule_ReplyWithError(ctx, "Unknown index name");
    return REDISMODULE_OK;
  }

  RedisModule_ReplicateVerbatim(ctx);

  IndexSpec_InitializeSynonym(sp);

  uint32_t id = SynonymMap_AddRedisStr(sp->smap, argv + 2, argc - 2);

  RedisModule_ReplyWithLongLong(ctx, id);

  return REDISMODULE_OK;
}

int SynUpdateCommandInternal(RedisModuleCtx *ctx, RedisModuleString *indexName, long long id,
                             RedisModuleString **synonyms, size_t size, bool checkIdSanity) {
  IndexSpec *sp = IndexSpec_Load(ctx, RedisModule_StringPtrLen(indexName, NULL), 0);
  if (!sp) {
    RedisModule_ReplyWithError(ctx, "Unknown index name");
    return REDISMODULE_OK;
  }

  if (checkIdSanity && (!sp->smap || id >= SynonymMap_GetMaxId(sp->smap))) {
    RedisModule_ReplyWithError(ctx, "given id does not exists");
    return REDISMODULE_OK;
  }

  IndexSpec_InitializeSynonym(sp);

  SynonymMap_UpdateRedisStr(sp->smap, synonyms, size, id);

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
int SynUpdateCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  if (argc < 4) return RedisModule_WrongArity(ctx);

  long long id;
  if (RedisModule_StringToLongLong(argv[2], &id) != REDISMODULE_OK) {
    RedisModule_ReplyWithError(ctx, "wrong parameters, id is not an integer");
    return REDISMODULE_OK;
  }

  if (id < 0 || id > UINT32_MAX) {
    RedisModule_ReplyWithError(ctx, "wrong parameters, id out of range");
    return REDISMODULE_OK;
  }

  RedisModule_ReplicateVerbatim(ctx);

  return SynUpdateCommandInternal(ctx, argv[1], id, argv + 3, argc - 3, true);
}

int SynForceUpdateCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  if (argc < 4) return RedisModule_WrongArity(ctx);

  long long id;
  if (RedisModule_StringToLongLong(argv[2], &id) != REDISMODULE_OK) {
    RedisModule_ReplyWithError(ctx, "wrong parameters, id is not an integer");
    return REDISMODULE_OK;
  }

  if (id < 0 || id > UINT32_MAX) {
    RedisModule_ReplyWithError(ctx, "wrong parameters, id out of range");
    return REDISMODULE_OK;
  }

  RedisModule_ReplicateVerbatim(ctx);

  return SynUpdateCommandInternal(ctx, argv[1], id, argv + 3, argc - 3, false);
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
  if (argc < 2) return RedisModule_WrongArity(ctx);

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
    RedisModule_ReplyWithArray(ctx, array_len(t_data->ids));
    for (size_t j = 0; j < array_len(t_data->ids); ++j) {
      RedisModule_ReplyWithLongLong(ctx, t_data->ids[j]);
    }
  }

  rm_free(terms_data);

  return REDISMODULE_OK;
}

int AlterIndexCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  // Need at least <cmd> <index> <subcommand> <args...>
  RedisModule_AutoMemory(ctx);

  if (argc < 5) {
    return RedisModule_WrongArity(ctx);
  }
  // I'd like to use CmdSchema, but want to avoid the ugly <N> <list of N> stuff..
  if (!RMUtil_StringEqualsCaseC(argv[2], "SCHEMA") || !RMUtil_StringEqualsCaseC(argv[3], "ADD")) {
    return RedisModule_ReplyWithError(ctx, "Unknown command");
  }
  IndexSpec *sp = IndexSpec_Load(ctx, RedisModule_StringPtrLen(argv[1], NULL), 1);
  if (!sp) {
    return RedisModule_ReplyWithError(ctx, "Unknown index name");
  }

  const int schemaOffset = 4;
  QueryError status = {0};

  if (argc - schemaOffset == 0) {
    return RedisModule_ReplyWithError(ctx, "No fields provided");
  }

  int rc = IndexSpec_AddFieldsRedisArgs(sp, argv + schemaOffset, argc - schemaOffset, &status);
  if (!rc) {
    RedisModule_ReplyWithError(ctx, QueryError_GetError(&status));
    QueryError_ClearError(&status);
  } else {
    RedisModule_ReplyWithSimpleString(ctx, "OK");
  }

  RedisModule_ReplicateVerbatim(ctx);
  return REDISMODULE_OK;
}

int ConfigCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  // Not bound to a specific index, so...
  RedisModule_AutoMemory(ctx);
  QueryError status;

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

#define RM_TRY(f, ...)                                                         \
  if (f(__VA_ARGS__) == REDISMODULE_ERR) {                                     \
    RedisModule_Log(ctx, "warning", "Could not run " #f "(" #__VA_ARGS__ ")"); \
    return REDISMODULE_ERR;                                                    \
  } else {                                                                     \
    RedisModule_Log(ctx, "verbose", "Successfully executed " #f);              \
  }

/**
 * Check if we can run under the current AOF configuration. Returns true
 * or false
 */
static int validateAofSettings(RedisModuleCtx *ctx) {
  int rc = 1;

  if (RedisModule_GetContextFlags == NULL) {
    RedisModule_Log(ctx, "warning",
                    "Could not determine if AOF is in use. AOF Rewrite will crash!");
    return 1;
  }

  if ((RedisModule_GetContextFlags(ctx) & REDISMODULE_CTX_FLAGS_AOF) == 0) {
    // AOF disabled. All is OK, and no further checks needed
    return rc;
  }

  // Can't exexcute commands on the loading context, so make a new one
  RedisModuleCtx *confCtx = RedisModule_GetThreadSafeContext(NULL);
  RedisModuleCallReply *reply =
      RedisModule_Call(confCtx, "CONFIG", "cc", "GET", "aof-use-rdb-preamble");
  assert(reply);
  assert(RedisModule_CallReplyType(reply) == REDISMODULE_REPLY_ARRAY);
  assert(RedisModule_CallReplyLength(reply) == 2);
  const char *value =
      RedisModule_CallReplyStringPtr(RedisModule_CallReplyArrayElement(reply, 1), NULL);

  // I tried using strcasecmp, but it seems that the yes/no replies have a trailing
  // embedded newline in them
  if (tolower(*value) == 'n') {
    RedisModule_Log(ctx, "warning", "FATAL: aof-use-rdb-preamble required if AOF is used!");
    rc = 0;
  }
  RedisModule_FreeCallReply(reply);
  RedisModule_FreeThreadSafeContext(confCtx);
  return rc;
}

int RediSearch_InitModuleInternal(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {

  // Check that redis supports thread safe context. RC3 or below doesn't
  if (RedisModule_GetThreadSafeContext == NULL) {
    RedisModule_Log(ctx, "warning",
                    "***** FATAL: Incompatible version of redis 4.0 detected. *****\n"
                    "\t\t\t\tPlease use Redis 4.0.0 or later from https://redis.io/download\n"
                    "\t\t\t\tRedis will exit now!");
    return REDISMODULE_ERR;
  }

  // Print version string!
  RedisModule_Log(ctx, "notice", "RediSearch version %d.%d.%d (Git=%s)", REDISEARCH_VERSION_MAJOR,
                  REDISEARCH_VERSION_MINOR, REDISEARCH_VERSION_PATCH, RS_GetExtraVersion());

  char *err;
  if (ReadConfig(argv, argc, &err) == REDISMODULE_ERR) {
    RedisModule_Log(ctx, "warning", "Invalid Configurations: %s", err);
    free(err);
    return REDISMODULE_ERR;
  }
  sds confstr = RSConfig_GetInfoString(&RSGlobalConfig);
  RedisModule_Log(ctx, "notice", confstr);
  sdsfree(confstr);

  if (RedisModule_GetContextFlags == NULL && RSGlobalConfig.concurrentMode) {
    RedisModule_Log(ctx, "warning",
                    "GetContextFlags unsupported (need Redis >= 4.0.6). Commands executed in "
                    "MULTI or LUA will "
                    "malfunction unless 'safe' functions are used or SAFEMODE is enabled.");
  }

  if (!validateAofSettings(ctx)) {
    return REDISMODULE_ERR;
  }

  // Init extension mechanism
  Extensions_Init();

  if (RSGlobalConfig.concurrentMode) {
    ConcurrentSearch_ThreadPoolStart();
  }

  // Init cursors mechanism
  CursorList_Init(&RSCursors);

  // Register aggregation functions
  RegisterAllFunctions();

  RedisModule_Log(ctx, "notice", "Initialized thread pool!");

  /* Load extensions if needed */
  if (RSGlobalConfig.extLoad != NULL) {

    char *errMsg = NULL;
    // Load the extension so TODO: pass with param
    if (Extension_LoadDynamic(RSGlobalConfig.extLoad, &errMsg) == REDISMODULE_ERR) {
      RedisModule_Log(ctx, "warning", "Could not load extension %s: %s", RSGlobalConfig.extLoad,
                      errMsg);
      free(errMsg);
      return REDISMODULE_ERR;
    }
    RedisModule_Log(ctx, "notice", "Loaded RediSearch extension '%s'", RSGlobalConfig.extLoad);
  }

  // Register the default hard coded extension
  if (Extension_Load("DEFAULT", DefaultExtensionInit) == REDISEARCH_ERR) {
    RedisModule_Log(ctx, "warning", "Could not register default extension");
    return REDISMODULE_ERR;
  }

  // register trie type
  RM_TRY(TrieType_Register, ctx);

  RM_TRY(IndexSpec_RegisterType, ctx);

  RM_TRY(TagIndex_RegisterType, ctx);

  RM_TRY(InvertedIndex_RegisterType, ctx);

  RM_TRY(NumericIndexType_Register, ctx);

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

  RM_TRY(RedisModule_CreateCommand, ctx, RS_DEBUG, DebugCommand, "readonly", 1, 1, 1);

  RM_TRY(RedisModule_CreateCommand, ctx, RS_SPELL_CHECK, SpellCheckCommand, "readonly", 1, 1, 1);

  RM_TRY(RedisModule_CreateCommand, ctx, RS_DICT_ADD, DictAddCommand, "readonly", 1, 1, 1);

  RM_TRY(RedisModule_CreateCommand, ctx, RS_DICT_DEL, DictDelCommand, "readonly", 1, 1, 1);

  RM_TRY(RedisModule_CreateCommand, ctx, RS_DICT_DUMP, DictDumpCommand, "readonly", 1, 1, 1);

  RM_TRY(RedisModule_CreateCommand, ctx, RS_CONFIG, ConfigCommand, "readonly", 1, 1, 1);
  return REDISMODULE_OK;
}
