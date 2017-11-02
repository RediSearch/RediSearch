#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/param.h>
#include <time.h>

#include "commands.h"
#include "document.h"
#include "version.h"
#include "forward_index.h"
#include "geo_index.h"
#include "tag_index.h"
#include "index.h"
#include "numeric_filter.h"
#include "numeric_index.h"
#include "query.h"
#include "query_node.h"
#include "redis_index.h"
#include "redismodule.h"
#include "rmutil/strings.h"
#include "rmutil/util.h"
#include "spec.h"
#include "stopwords.h"
#include "trie/trie_type.h"
#include "util/logging.h"
#include "varint.h"
#include "extension.h"
#include "ext/default.h"
#include "search_request.h"
#include "config.h"
#include "gc.h"
#include "rmalloc.h"

#define LOAD_INDEX(ctx, srcname, write)                                                     \
  ({                                                                                        \
    IndexSpec *sptmp = IndexSpec_Load(ctx, RedisModule_StringPtrLen(srcname, NULL), write); \
    if (sptmp == NULL) {                                                                    \
      return RedisModule_ReplyWithError(ctx, "Unknown index name");                         \
    }                                                                                       \
    sptmp;                                                                                  \
  })

/*
## FT.ADD <index> <docId> <score> [NOSAVE] [REPLACE] [LANGUAGE <lang>] [PAYLOAD {payload}] FIELDS
<field>
<text> ....]
Add a documet to the index.

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

    - REPLACE: If set, we will do an update and delete an older version of the document if it exists

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

   > "arabic",  "danish",    "dutch",   "english",   "finnish",    "french",
   > "german",  "hungarian", "italian", "norwegian", "portuguese", "romanian",
   > "russian", "spanish",   "swedish", "tamil",     "turkish"


Returns OK on success, or an error if something went wrong.
*/

static int doAddDocument(RedisModuleCtx *ctx, RedisModuleString **argv, int argc, int canBlock) {
  int nosave = RMUtil_ArgExists("NOSAVE", argv, argc, 1);
  int fieldsIdx = RMUtil_ArgExists("FIELDS", argv, argc, 1);
  int replace = RMUtil_ArgExists("REPLACE", argv, argc, 1);
  int partial = RMUtil_ArgExists("PARTIAL", argv, argc, 1);
  // printf("argc: %d, fieldsIdx: %d, argc - fieldsIdx: %d, nosave: %d\n", argc,
  // fieldsIdx,
  // argc-fieldsIdx, nosave);
  // nosave must be at place 4 and we must have at least 7 fields
  if (argc < 7 || fieldsIdx == 0 || (argc - fieldsIdx) % 2 == 0 || (nosave && nosave != 4)) {
    return RedisModule_WrongArity(ctx);
  }

  RedisModule_Replicate(ctx, RS_SAFEADD_CMD, "v", argv + 1, argc - 1);
  RedisModule_AutoMemory(ctx);
  // Load the document score
  double ds = 0;
  if (RedisModule_StringToDouble(argv[3], &ds) == REDISMODULE_ERR) {
    RedisModule_ReplyWithError(ctx, "Could not parse document score");
    goto cleanup;
  }

  if (ds > 1 || ds < 0) {
    RedisModule_ReplyWithError(ctx, "Document scores must be normalized between 0.0 ... 1.0");
    goto cleanup;
  }

  // Parse the optional LANGUAGE flag
  const char *lang = NULL;
  RMUtil_ParseArgsAfter("LANGUAGE", argv, argc, "c", &lang);
  if (lang && !IsSupportedLanguage(lang, strlen(lang))) {
    RedisModule_ReplyWithError(ctx, "Unsupported Language");
    goto cleanup;
  }

  // Parse the optional payload field
  RedisModuleString *payload = NULL;
  RMUtil_ParseArgsAfter("PAYLOAD", argv, argc, "s", &payload);

  IndexSpec *sp = IndexSpec_Load(ctx, RedisModule_StringPtrLen(argv[1], NULL), 0);
  if (!sp) {
    RedisModule_ReplyWithError(ctx, "Unknown index name");
    goto cleanup;
  }

  Document doc;
  Document_PrepareForAdd(&doc, argv[2], ds, argv, fieldsIdx, argc, lang, payload, ctx);
  if (!Document_CanAdd(&doc, sp, replace)) {
    RedisModule_ReplyWithError(ctx, "Document already in index");
    goto cleanup;
  }
  if (!nosave) {
    RedisSearchCtx sctx = SEARCH_CTX_STATIC(ctx, sp);
    if (Redis_SaveDocument(&sctx, &doc) != REDISMODULE_OK) {
      Document_FreeDetached(&doc, ctx);
      return RedisModule_ReplyWithError(ctx, "ERR couldn't save document");
    }
  }

  LG_DEBUG("Adding doc %s with %d fields\n", RedisModule_StringPtrLen(doc.docKey, NULL),
           doc.numFields);
  RSAddDocumentCtx *aCtx = NewAddDocumentCtx(sp, &doc);

  // in partial mode
  uint32_t options = 0;
  if (replace) {
    options |= DOCUMENT_ADD_REPLACE;
  }
  if (partial) {
    options |= DOCUMENT_ADD_PARTIAL;
  }
  if (nosave) {
    options |= DOCUMENT_ADD_NOSAVE;
  }
  RedisSearchCtx sctx = {.redisCtx = ctx, .spec = sp};
  if (!canBlock) {
    aCtx->stateFlags |= ACTX_F_NOBLOCK;
  }
  AddDocumentCtx_Submit(aCtx, &sctx, options);

cleanup:
  return REDISMODULE_OK;
}

int AddDocumentCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  return doAddDocument(ctx, argv, argc, 1);
}

int SafeAddDocumentCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  return doAddDocument(ctx, argv, argc, 0);
}

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
  t_docId docId = DocTable_GetId(&sp->docs, RedisModule_StringPtrLen(argv[2], NULL));
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
  ADD_NEGATIVE_OPTION(Index_StoreFreqs, "NOFREQS");
  ADD_NEGATIVE_OPTION(Index_StoreFieldFlags, "NOFIELDS");
  ADD_NEGATIVE_OPTION(Index_StoreTermOffsets, "NOOFFSETS");
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
  // REPLY_KVNUM(n, "inverted_cap_mb", sp->stats.invertedCap / (float)0x100000);

  // REPLY_KVNUM(n, "inverted_cap_ovh", 0);
  //(float)(sp->stats.invertedCap - sp->stats.invertedSize) / (float)sp->stats.invertedCap);

  REPLY_KVNUM(n, "offset_vectors_sz_mb", sp->stats.offsetVecsSize / (float)0x100000);
  // REPLY_KVNUM(n, "skip_index_size_mb", sp->stats.skipIndexesSize / (float)0x100000);
  //  REPLY_KVNUM(n, "score_index_size_mb", sp->stats.scoreIndexesSize / (float)0x100000);

  REPLY_KVNUM(n, "doc_table_size_mb", sp->docs.memsize / (float)0x100000);
  REPLY_KVNUM(n, "key_table_size_mb", TrieMap_MemUsage(sp->docs.dim.tm) / (float)0x100000);
  REPLY_KVNUM(n, "records_per_doc_avg",
              (float)sp->stats.numRecords / (float)sp->stats.numDocuments);
  REPLY_KVNUM(n, "bytes_per_record_avg",
              (float)sp->stats.invertedSize / (float)sp->stats.numRecords);
  REPLY_KVNUM(n, "offsets_per_term_avg",
              (float)sp->stats.offsetVecRecords / (float)sp->stats.numRecords);
  REPLY_KVNUM(n, "offset_bits_per_record_avg",
              8.0F * (float)sp->stats.offsetVecsSize / (float)sp->stats.offsetVecRecords);

  RedisModule_ReplyWithSimpleString(ctx, "gc_stats");
  GC_RenderStats(ctx, sp->gc);
  n += 2;

  RedisModule_ReplySetArrayLength(ctx, n);
  return REDISMODULE_OK;
}

/* FT.MGET {index} {key} ...
* Get document(s) by their id.
* Currentlt it just performs HGETALL, but it's a future proof alternative allowing us to later on
* replace the internal representation of the documents.
*
* If referred docs are missing or not HASH keys, we simply reply with Null, but the result will be
* an array the same size of the ids list
*/
int GetDocumentsCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  if (argc < 3) {
    return RedisModule_WrongArity(ctx);
  }

  RedisModule_AutoMemory(ctx);
  RedisSearchCtx *sctx = NewSearchCtx(ctx, argv[1]);
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
  RedisSearchCtx *sctx = NewSearchCtx(ctx, argv[1]);
  if (sctx == NULL) {
    return RedisModule_ReplyWithError(ctx, "Unknown Index name");
  }

  Document doc;

  if (Redis_LoadDocument(sctx, argv[2], &doc) == REDISMODULE_ERR) {
    RedisModule_ReplyWithNull(ctx);
  } else {
    Document_ReplyFields(ctx, &doc);
  }
  SearchCtx_Free(sctx);

  return REDISMODULE_OK;
}

/* FT.EXPLAIN {index_name} {query} */
int QueryExplainCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {

  // at least one field, and number of field/text args must be even
  if (argc < 3) {
    return RedisModule_WrongArity(ctx);
  }

  RedisModule_AutoMemory(ctx);
  RedisSearchCtx *sctx = NewSearchCtx(ctx, argv[1]);
  if (sctx == NULL) {
    return RedisModule_ReplyWithError(ctx, "Unknown Index name");
  }

  char *err;

  RSSearchRequest *req = ParseRequest(sctx, argv, argc, &err);
  if (req == NULL) {
    RedisModule_Log(ctx, "warning", "Error parsing request: %s", err);
    SearchCtx_Free(sctx);
    return RedisModule_ReplyWithError(ctx, err);
  }
  req->sctx = sctx;

  QueryParseCtx *q = NewQueryParseCtx(req);
  if (!q) {
    SearchCtx_Free(sctx);
    return RedisModule_ReplyWithError(ctx, "Error parsing query");
  }

  char *errMsg = NULL;
  if (!Query_Parse(q, &errMsg)) {

    if (errMsg) {
      RedisModule_Log(ctx, "debug", "Error parsing query: %s", errMsg);
      RedisModule_ReplyWithError(ctx, errMsg);
      free(errMsg);
    } else {
      /* Simulate an empty response - this means an empty query */
      RedisModule_ReplyWithArray(ctx, 1);
      RedisModule_ReplyWithLongLong(ctx, 0);
    }
    goto end;
  }
  if (!(req->flags & Search_Verbatim)) {
    Query_Expand(q, req->expander);
  }
  if (req->geoFilter) {
    Query_SetGeoFilter(q, req->geoFilter);
  }

  if (req->idFilter) {
    Query_SetIdFilter(q, req->idFilter);
  }
  // set numeric filters if possible
  if (req->numericFilters) {
    for (int i = 0; i < Vector_Size(req->numericFilters); i++) {
      NumericFilter *nf;
      Vector_Get(req->numericFilters, i, &nf);
      if (nf) {
        Query_SetNumericFilter(q, nf);
      }
    }

    Vector_Free(req->numericFilters);
    req->numericFilters = NULL;
  }

  char *explain = (char *)Query_DumpExplain(q);
  RedisModule_ReplyWithStringBuffer(ctx, explain, strlen(explain));
  free(explain);

end:

  Query_Free(q);
  RSSearchRequest_Free(req);
  return REDISMODULE_OK;
}

/* FT.DTADD {index} {key} {flags} {score} {payload} {byteOffsets}
*
*  **WARNING**:  Do NOT use this command, it is for internal use in AOF rewriting only!!!!
*
*  This command is used only for AOF rewrite and makes sure the document table is rebuilt in the
*  same order as as in memory
*
*  Returns the docId on success or 0 on failure
*/
int DTAddCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  RedisModule_AutoMemory(ctx);
  if (argc != 7) return RedisModule_WrongArity(ctx);

  IndexSpec *sp = IndexSpec_Load(ctx, RedisModule_StringPtrLen(argv[1], NULL), 1);
  if (sp == NULL) {
    return RedisModule_ReplyWithError(ctx, "Unknown Index name");
  }

  long long flags;
  double score;
  if (RMUtil_ParseArgs(argv, argc, 3, "ld", &flags, &score) == REDISMODULE_ERR) {
    return RedisModule_ReplyWithError(ctx, "Could not parse flags and score");
  }

  size_t payloadSize = 0, offsetsSize = 0;
  const char *payload = RedisModule_StringPtrLen(argv[5], &payloadSize);
  const char *serOffsets = RedisModule_StringPtrLen(argv[6], &offsetsSize);

  t_docId d = DocTable_Put(&sp->docs, RedisModule_StringPtrLen(argv[2], NULL), (float)score,
                           (u_char)flags, payload, payloadSize);

  if (offsetsSize) {
    Buffer *b = Buffer_Wrap((char *)serOffsets, offsetsSize);
    RSByteOffsets *offsets = LoadByteOffsets(b);
    free(b);
    if (offsets) {
      DocTable_SetByteOffsets(&sp->docs, d, offsets);
    }
  }

  return RedisModule_ReplyWithLongLong(ctx, d);
}

/**
 * FT.TERMADD {index} {term} {score}
 *
 * **WARNING** Do NOT use this command. It is for internal AOF rewriting only
 *
 * This command is used to incrementally transfer terms (for prefix expansion)
 * over to the trie.
 *
 * This might change once the internal structure of the trie changes
 */
int TermAddCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  RedisModule_AutoMemory(ctx);
  if (argc != 4) {
    return RedisModule_WrongArity(ctx);
  }

  IndexSpec *sp = LOAD_INDEX(ctx, argv[1], 1);

  // Add the term to the spec
  size_t termLen = 0;
  const char *termStr = RedisModule_StringPtrLen(argv[2], &termLen);

  double score;
  if (RedisModule_StringToDouble(argv[3], &score) != REDISMODULE_OK) {
    return RedisModule_ReplyWithError(ctx, "ERR bad score");
  }

  IndexSpec_RestoreTerm(sp, termStr, termLen, score);
  return REDISMODULE_OK;
}

/* FT.DEL {index} {doc_id}
*  Delete a document from the index. Returns 1 if the document was in the index, or 0 if not.
*
*  **NOTE**: This does not actually delete the document from the index, just marks it as deleted
*/
int DeleteCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  RedisModule_AutoMemory(ctx);

  if (argc != 3) return RedisModule_WrongArity(ctx);
  RedisModule_ReplicateVerbatim(ctx);

  IndexSpec *sp = IndexSpec_Load(ctx, RedisModule_StringPtrLen(argv[1], NULL), 1);
  if (sp == NULL) {
    return RedisModule_ReplyWithError(ctx, "Unknown Index name");
  }

  int rc = DocTable_Delete(&sp->docs, RedisModule_StringPtrLen(argv[2], NULL));
  if (rc == 1) {
    sp->stats.numDocuments--;
    // Increment the index's garbage collector's scanning frequency after document deletions
    GC_OnDelete(sp->gc);
  }

  return RedisModule_ReplyWithLongLong(ctx, rc);
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
        > "german",  "hungarian", "italian", "norwegian", "portuguese",
"romanian",
        > "russian", "spanish",   "swedish", "tamil",     "turkish"


  Returns OK on success, or an error if something went wrong.
*/
static int doAddHashCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc,
                            int isBlockable) {
  if (argc < 4 || argc > 7) {
    return RedisModule_WrongArity(ctx);
  }

  RedisModule_AutoMemory(ctx);
  RedisModule_Replicate(ctx, RS_SAFEADDHASH_CMD, "v", argv + 1, argc - 1);

  IndexSpec *sp = IndexSpec_Load(ctx, RedisModule_StringPtrLen(argv[1], NULL), 1);
  if (sp == NULL) {
    RedisModule_ReplyWithError(ctx, "Unknown Index name");
    goto cleanup;
  }

  int replace = RMUtil_ArgExists("REPLACE", argv, argc, 1);

  // Load the document score
  double ds = 0;
  if (RedisModule_StringToDouble(argv[3], &ds) == REDISMODULE_ERR) {
    RedisModule_ReplyWithError(ctx, "Could not parse document score");
    goto cleanup;
  }
  if (ds > 1 || ds < 0) {
    RedisModule_ReplyWithError(ctx, "Document scores must be normalized between 0.0 ... 1.0");
    goto cleanup;
  }

  // Parse the optional LANGUAGE flag
  const char *lang = NULL;
  RMUtil_ParseArgsAfter("LANGUAGE", &argv[3], argc - 4, "c", &lang);
  if (lang && !IsSupportedLanguage(lang, strlen(lang))) {
    RedisModule_ReplyWithError(ctx, "Unsupported Language");
    goto cleanup;
  }

  Document doc;
  RedisSearchCtx sctx = SEARCH_CTX_STATIC(ctx, sp);
  if (Redis_LoadDocument(&sctx, argv[2], &doc) != REDISMODULE_OK) {
    return RedisModule_ReplyWithError(ctx, "Could not load document");
  }

  doc.docKey = argv[2];
  doc.score = ds;
  doc.language = lang ? lang : DEFAULT_LANGUAGE;
  doc.payload = NULL;
  doc.payloadSize = 0;
  Document_Detach(&doc, ctx);

  LG_DEBUG("Adding doc %s with %d fields\n", RedisModule_StringPtrLen(doc.docKey, NULL),
           doc.numFields);
  RSAddDocumentCtx *aCtx = NewAddDocumentCtx(sp, &doc);
  if (!isBlockable) {
    aCtx->stateFlags |= ACTX_F_NOBLOCK;
  }
  AddDocumentCtx_Submit(aCtx, &sctx, replace ? DOCUMENT_ADD_REPLACE : 0);

cleanup:
  return REDISMODULE_OK;
}

int AddHashCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  return doAddHashCommand(ctx, argv, argc, 1);
}

int SafeAddHashCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  return doAddHashCommand(ctx, argv, argc, 0);
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
int SearchCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  // at least one field, and number of field/text args must be even
  if (argc < 3) {
    return RedisModule_WrongArity(ctx);
  }

  RedisModule_AutoMemory(ctx);
  RedisSearchCtx *sctx = NewSearchCtx(ctx, argv[1]);
  if (sctx == NULL) {
    return RedisModule_ReplyWithError(ctx, "Unknown Index name");
  }

  char *err;

  RSSearchRequest *req = ParseRequest(sctx, argv, argc, &err);
  if (req == NULL) {
    RedisModule_Log(ctx, "warning", "Error parsing request: %s", err);
    SearchCtx_Free(sctx);
    return RedisModule_ReplyWithError(ctx, err);
  }

  // in concurrent mode - process the request in the thread pool
  if (RSGlobalConfig.concurrentMode) {
    int rc = RSSearchRequest_ProcessInThreadpool(ctx, req);
    SearchCtx_Free(sctx);
    return rc;
  } else {  // "safe" mode - process the request in the main thread
    return RSSearchRequest_ProcessMainThread(sctx, req);
  }
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
  RedisSearchCtx *sctx = NewSearchCtx(ctx, argv[1]);
  if (sctx == NULL) {
    return RedisModule_ReplyWithError(ctx, "Unknown Index name");
  }
  size_t len;
  const char *field = RedisModule_StringPtrLen(argv[2], &len);
  FieldSpec *sp = IndexSpec_GetField(sctx->spec, field, len);
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
    RedisModule_ReplyWithError(ctx, "Could not open tag index");
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
  RedisModule_AutoMemory(ctx);
  RedisModule_ReplicateVerbatim(ctx);
  char *err;

  IndexSpec *sp = IndexSpec_CreateNew(ctx, argv, argc, &err);
  if (sp == NULL) {
    return RedisModule_ReplyWithError(ctx, err ? err : "Could not create new index");
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
* FT.DROP <index>
* Deletes all the keys associated with the index.
* If no other data is on the redis instance, this is equivalent to FLUSHDB,
* apart from the fact
* that the index specification is not deleted.
*/
int DropIndexCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  // at least one field, and number of field/text args must be even
  if (argc != 2) {
    return RedisModule_WrongArity(ctx);
  }
  RedisModule_ReplicateVerbatim(ctx);

  RedisModule_AutoMemory(ctx);

  IndexSpec *sp = IndexSpec_Load(ctx, RedisModule_StringPtrLen(argv[1], NULL), 0);
  if (sp == NULL) {
    return RedisModule_ReplyWithError(ctx, "Unknown Index name");
  }

  RedisSearchCtx sctx = SEARCH_CTX_STATIC(ctx, sp);
  Redis_DropIndex(&sctx, 1);
  return RedisModule_ReplyWithSimpleString(ctx, "OK");
}

/*
## FT.SUGGADD key string score [INCR] [PAYLOAD {payload}]

Add a suggestion string to an auto-complete suggestion dictionary. This is
disconnected from the
index definitions, and leaves creating and updating suggestino dictionaries to
the user.

### Parameters:

   - key: the suggestion dictionary key.

   - string: the suggestion string we index

   - score: a floating point number of the suggestion string's weight

   -INCR: if set, we increment the existing entry of the suggestion by the
given
score, instead
of
    replacing the score. This is useful for updating the dictionary based on
user queries in
real
    time

   - PAYLOAD: Add a payload to the suggestion string that will be used as additional information.

### Returns:

Integer reply: the current size of the suggestion dictionary.
*/
int SuggestAddCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  if (argc < 4 || argc > 7) return RedisModule_WrongArity(ctx);

  RedisModule_AutoMemory(ctx); /* Use automatic memory management. */
  RedisModule_ReplicateVerbatim(ctx);

  RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ | REDISMODULE_WRITE);
  int type = RedisModule_KeyType(key);
  if (type != REDISMODULE_KEYTYPE_EMPTY && RedisModule_ModuleTypeGetType(key) != TrieType) {
    return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
  }

  RedisModuleString *val = argv[2];
  double score;
  if ((RedisModule_StringToDouble(argv[3], &score) != REDISMODULE_OK)) {
    return RedisModule_ReplyWithError(ctx, "ERR invalid score");
  }

  int incr = RMUtil_ArgExists("INCR", argv, argc, 4);

  // Parse the optional payload field
  RSPayload payload = {.data = NULL, .len = 0};
  if (argc > 4) {
    RMUtil_ParseArgsAfter("PAYLOAD", &argv[3], argc - 3, "b", &payload.data, &payload.len);
  }

  /* Create an empty value object if the key is currently empty. */
  Trie *tree;
  if (type == REDISMODULE_KEYTYPE_EMPTY) {
    tree = NewTrie();
    RedisModule_ModuleTypeSetValue(key, TrieType, tree);
  } else {
    tree = RedisModule_ModuleTypeGetValue(key);
  }

  /* Insert the new element. */
  Trie_Insert(tree, val, score, incr, &payload);

  RedisModule_ReplyWithLongLong(ctx, tree->size);
  RedisModule_ReplicateVerbatim(ctx);
  return REDISMODULE_OK;
}

/*
## FT.SUGLEN key

Get the size of an autoc-complete suggestion dictionary

### Parameters:

   - key: the suggestion dictionary key.

### Returns:

Integer reply: the current size of the suggestion dictionary.
*/
int SuggestLenCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  RedisModule_AutoMemory(ctx); /* Use automatic memory management. */

  if (argc != 2) return RedisModule_WrongArity(ctx);
  RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ);
  int type = RedisModule_KeyType(key);
  if (type != REDISMODULE_KEYTYPE_EMPTY && RedisModule_ModuleTypeGetType(key) != TrieType) {
    return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
  }

  Trie *tree = RedisModule_ModuleTypeGetValue(key);
  return RedisModule_ReplyWithLongLong(ctx, tree ? tree->size : 0);
}

/*
## FT.SUGDEL key str

Delete a string from a suggestion index.

### Parameters:

   - key: the suggestion dictionary key.

   - str: the string to delete

### Returns:

Integer reply: 1 if the string was found and deleted, 0 otherwise.
*/
int SuggestDelCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  RedisModule_AutoMemory(ctx); /* Use automatic memory management. */

  if (argc != 3) return RedisModule_WrongArity(ctx);
  RedisModule_ReplicateVerbatim(ctx);

  RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ);
  int type = RedisModule_KeyType(key);
  if (type != REDISMODULE_KEYTYPE_EMPTY && RedisModule_ModuleTypeGetType(key) != TrieType) {
    return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
  }

  Trie *tree = RedisModule_ModuleTypeGetValue(key);
  if (!tree) {
    return RedisModule_ReplyWithLongLong(ctx, 0);
  }
  size_t len;
  const char *str = RedisModule_StringPtrLen(argv[2], &len);
  return RedisModule_ReplyWithLongLong(ctx, Trie_Delete(tree, (char *)str, len));
}

/*
## FT.SUGGET key prefix [FUZZY] [MAX num] [WITHSCORES] [TRIM] [OPTIMIZE] [WITHPAYLOADS]

Get completion suggestions for a prefix

### Parameters:

   - key: the suggestion dictionary key

   - prefix: the prefix to complete on

   - FUZZY: if set,we do a fuzzy prefix search, including prefixes at
     levenshtein distance of 1  from the prefix sent

   - MAX num: If set, we limit the results to a maximum of `num`. The default
     is 5, and the number   cannot be greater than 10.

   - WITHSCORES: If set, we also return each entry's score

   - TRIM: If set, we remove very unlikely results

   - WITHPAYLOADS: If set, we also return each entry's payload as they were inserted, or nil if no
payload
    exists.
### Returns:

Array reply: a list of the top suggestions matching the prefix

*/
int SuggestGetCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  RedisModule_AutoMemory(ctx); /* Use automatic memory management. */

  if (argc < 3 || argc > 10) return RedisModule_WrongArity(ctx);

  RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ);
  // make sure the key is a trie
  int type = RedisModule_KeyType(key);
  if (type != REDISMODULE_KEYTYPE_EMPTY && RedisModule_ModuleTypeGetType(key) != TrieType) {
    return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
  }

  Trie *tree = RedisModule_ModuleTypeGetValue(key);
  if (tree == NULL) {
    return RedisModule_ReplyWithNull(ctx);
  }

  // get the string to search for
  size_t len;
  char *s = (char *)RedisModule_StringPtrLen(argv[2], &len);

  // get optional FUZZY argument
  long maxDist = 0;
  if (RMUtil_ArgExists("FUZZY", argv, argc, 3)) {
    maxDist = 1;
  }
  // RedisModuleString **argv, int argc,const char *fmt
  long num = 5;
  RMUtil_ParseArgsAfter("MAX", argv, argc, "l", &num);
  if (num <= 0 || num > 10) {
    num = 5;
  }
  // detect WITHSCORES
  int withScores = RMUtil_ArgExists("WITHSCORES", argv, argc, 3);

  // detect TRIM
  int trim = RMUtil_ArgExists("TRIM", argv, argc, 3);

  int optimize = RMUtil_ArgExists("OPTIMIZE", argv, argc, 3);

  // detect WITHPAYLOADS
  int withPayloads = RMUtil_ArgExists("WITHPAYLOADS", argv, argc, 3);

  Vector *res = Trie_Search(tree, s, len, num, maxDist, 1, trim, optimize);

  // if we also need to return scores, we need double the records
  int mul = 1;
  mul = withScores ? mul + 1 : mul;
  mul = withPayloads ? mul + 1 : mul;
  RedisModule_ReplyWithArray(ctx, Vector_Size(res) * mul);

  for (int i = 0; i < Vector_Size(res); i++) {
    TrieSearchResult *e;
    Vector_Get(res, i, &e);

    RedisModule_ReplyWithStringBuffer(ctx, e->str, e->len);
    if (withScores) {
      RedisModule_ReplyWithDouble(ctx, e->score);
    }
    if (withPayloads) {
      if (e->payload)
        RedisModule_ReplyWithStringBuffer(ctx, e->payload, e->plen);
      else
        RedisModule_ReplyWithNull(ctx);
    }

    TrieSearchResult_Free(e);
  }
  Vector_Free(res);

  return REDISMODULE_OK;
}

#define RM_TRY(f, ...)                                                                  \
  if (f(__VA_ARGS__) == REDISMODULE_ERR) {                                              \
    RedisModule_Log(ctx, "warning", "Could not run " __STRING(f) "(" #__VA_ARGS__ ")"); \
    return REDISMODULE_ERR;                                                             \
  } else {                                                                              \
    RedisModule_Log(ctx, "verbose", "Successfully executed " __STRING(f));              \
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
  RedisModule_Log(ctx, "verbose", "Loading RediSearch module!");

  const char *err;
  if (ReadConfig(argv, argc, &err) == REDISMODULE_ERR) {
    RedisModule_Log(ctx, "warning", "Invalid Configurations: %s", err);
    return REDISMODULE_ERR;
  }
  RedisModule_Log(
      ctx, "notice",
      "Configuration: concurrent mode: %d, ext load: %s, min prefix: %d, max expansions: %d,",
      RSGlobalConfig.concurrentMode, RSGlobalConfig.extLoad, RSGlobalConfig.minTermPrefix,
      RSGlobalConfig.maxPrefixExpansions);

  // Init extension mechanism
  Extensions_Init();

  ConcurrentSearch_ThreadPoolStart();
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

  RM_TRY(RedisModule_CreateCommand, ctx, RS_ADD_CMD, AddDocumentCommand, "write deny-oom", 1, 1, 1);

  RM_TRY(RedisModule_CreateCommand, ctx, RS_SAFEADD_CMD, SafeAddDocumentCommand, "write deny-oom",
         1, 1, 1);

  RM_TRY(RedisModule_CreateCommand, ctx, RS_SETPAYLOAD_CMD, SetPayloadCommand, "write deny-oom", 1,
         1, 1);

  RM_TRY(RedisModule_CreateCommand, ctx, RS_ADDHASH_CMD, AddHashCommand, "write deny-oom", 1, 1, 1);

  RM_TRY(RedisModule_CreateCommand, ctx, RS_SAFEADDHASH_CMD, SafeAddHashCommand, "write deny-oom",
         1, 1, 1);

  RM_TRY(RedisModule_CreateCommand, ctx, RS_DTADD_CMD, DTAddCommand, "write deny-oom", 1, 1, 1);

  RM_TRY(RedisModule_CreateCommand, ctx, RS_ADDTERM_CMD, TermAddCommand, "write deny-oom", 1, 1, 1);

  RM_TRY(RedisModule_CreateCommand, ctx, RS_DEL_CMD, DeleteCommand, "write", 1, 1, 1);

  RM_TRY(RedisModule_CreateCommand, ctx, RS_SEARCH_CMD, SearchCommand, "readonly", 1, 1, 1);

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

  RM_TRY(RedisModule_CreateCommand, ctx, RS_SUGADD_CMD, SuggestAddCommand, "write deny-oom", 1, 1,
         1);

  RM_TRY(RedisModule_CreateCommand, ctx, RS_SUGDEL_CMD, SuggestDelCommand, "write", 1, 1, 1);

  RM_TRY(RedisModule_CreateCommand, ctx, RS_SUGLEN_CMD, SuggestLenCommand, "readonly", 1, 1, 1);

  RM_TRY(RedisModule_CreateCommand, ctx, RS_SUGGET_CMD, SuggestGetCommand, "readonly", 1, 1, 1);

  return REDISMODULE_OK;
}

/* Only compile the entry point if compiling the module as a standalone module */
#ifndef RS_STATIC

int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {

  if (RedisModule_Init(ctx, "ft", REDISEARCH_MODULE_VERSION, REDISMODULE_APIVER_1) ==
      REDISMODULE_ERR)
    return REDISMODULE_ERR;
  return RediSearch_InitModuleInternal(ctx, argv, argc);
}

#endif
