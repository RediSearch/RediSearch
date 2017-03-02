
#include "forward_index.h"
#include "geo_index.h"
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
#include "tokenize.h"
#include "trie/trie_type.h"
#include "util/logging.h"
#include "varint.h"
#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/param.h>
#include <time.h>

/* Add a parsed document to the index. If replace is set, we will add it be deleting an older
 * version of it first */
int AddDocument(RedisSearchCtx *ctx, Document doc, const char **errorString, int nosave,
                int replace) {
  int isnew = 1;

  // if we're in replace mode, first we need to try and delete the older version of the document
  if (replace) {
    DocTable_Delete(&ctx->spec->docs, RedisModule_StringPtrLen(doc.docKey, NULL));
  }

  doc.docId = DocTable_Put(&ctx->spec->docs, RedisModule_StringPtrLen(doc.docKey, NULL), doc.score,
                           0, doc.payload, doc.payloadSize);

  // Make sure the document is not already in the index - it needs to be
  // incremental!
  if (doc.docId == 0 || !isnew) {
    *errorString = "Document already in index";
    return REDISMODULE_ERR;
  }

  // first save the document as hash
  if (nosave == 0 && Redis_SaveDocument(ctx, &doc) != REDISMODULE_OK) {
    *errorString = "Could not save document data";
    return REDISMODULE_ERR;
  }

  ForwardIndex *idx = NewForwardIndex(doc);

  int totalTokens = 0;

  for (int i = 0; i < doc.numFields; i++) {
    // printf("Tokenizing %s: %s\n",
    // RedisModule_StringPtrLen(doc.fields[i].name, NULL),
    //        RedisModule_StringPtrLen(doc.fields[i].text, NULL));

    size_t len;
    const char *f = RedisModule_StringPtrLen(doc.fields[i].name, &len);
    const char *c = RedisModule_StringPtrLen(doc.fields[i].text, NULL);

    FieldSpec *fs = IndexSpec_GetField(ctx->spec, f, len);
    if (fs == NULL) {
      LG_DEBUG("Skipping field %s not in index!", c);
      continue;
    }

    switch (fs->type) {
      case F_FULLTEXT:
        totalTokens =
            tokenize(c, fs->weight, fs->id, idx, forwardIndexTokenFunc, idx->stemmer, totalTokens);
        break;
      case F_NUMERIC: {
        double score;

        if (RedisModule_StringToDouble(doc.fields[i].text, &score) == REDISMODULE_ERR) {
          *errorString = "Could not parse numeric index value";
          goto error;
        }

        NumericRangeTree *rt = OpenNumericIndex(ctx, fs->name);
        NumericRangeTree_Add(rt, doc.docId, score);

        break;
        case F_GEO: {

          char *pos = strpbrk(c, " ,");
          if (!pos) {
            *errorString = "Invalid lon/lat format. Use \"lon lat\" or \"lon,lat\"";
            goto error;
          }
          *pos = '\0';
          pos++;
          char *slon = (char *)c, *slat = (char *)pos;

          GeoIndex gi = {.ctx = ctx, .sp = fs};
          if (GeoIndex_AddStrings(&gi, doc.docId, slon, slat) == REDISMODULE_ERR) {
            *errorString = "Could not index geo value";
            goto error;
          }
        }

        break;

        default:
          break;
      }
    }
  }

  DocumentMetadata *md = DocTable_Get(&ctx->spec->docs, doc.docId);
  md->maxFreq = idx->maxFreq;

  // printf("totaltokens :%d\n", totalTokens);
  if (totalTokens > 0) {
    ForwardIndexIterator it = ForwardIndex_Iterate(idx);

    ForwardIndexEntry *entry = ForwardIndexIterator_Next(&it);
    while (entry != NULL) {
      // ForwardIndex_NormalizeFreq(idx, entry);
      InvertedIndex *invidx = Redis_OpenInvertedIndex(ctx, entry->term, entry->len, 1);
      // IndexWriter *w = Redis_OpenWriter(ctx, entry->term, entry->len);
      int isNew = invidx->lastId == 0;

      // size_t cap = isNew ? 0 : w->bw.buf->cap;
      // size_t skcap = isNew ? 0 : w->skipIndexWriter.buf->cap;
      // size_t sccap = isNew ? 0 : w->scoreWriter.bw.buf->cap;
      size_t sz = InvertedIndex_WriteEntry(invidx, entry);

      /*******************************************
      * update stats for the index
      ********************************************/

      /* record the change in capacity of the buffer */
      // ctx->spec->stats.invertedCap += w->bw.buf->cap - cap;
      // ctx->spec->stats.skipIndexesSize += w->skipIndexWriter.buf->cap - skcap;
      // ctx->spec->stats.scoreIndexesSize += w->scoreWriter.bw.buf->cap - sccap;
      /* record the actual size consumption change */
      ctx->spec->stats.invertedSize += sz;

      ctx->spec->stats.numRecords++;
      /* increment the number of terms if this is a new term*/
      if (isNew) {
        ctx->spec->stats.numTerms++;
        ctx->spec->stats.termsSize += entry->len;
      }
      /* Record the space saved for offset vectors */
      if (ctx->spec->flags & Index_StoreTermOffsets) {
        ctx->spec->stats.offsetVecsSize += entry->vw->bw.buf->offset;
        ctx->spec->stats.offsetVecRecords += entry->vw->nmemb;
      }
      // Redis_CloseWriter(w);

      entry = ForwardIndexIterator_Next(&it);
    }
    // ctx->spec->stats->numDocuments += 1;
  }
  ctx->spec->stats.numDocuments += 1;
  ForwardIndexFree(idx);
  return REDISMODULE_OK;

error:
  ForwardIndexFree(idx);

  return REDISMODULE_ERR;
}

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
int AddDocumentCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  int nosave = RMUtil_ArgExists("NOSAVE", argv, argc, 1);
  int fieldsIdx = RMUtil_ArgExists("FIELDS", argv, argc, 1);
  int replace = RMUtil_ArgExists("REPLACE", argv, argc, 1);

  // printf("argc: %d, fieldsIdx: %d, argc - fieldsIdx: %d, nosave: %d\n", argc,
  // fieldsIdx,
  // argc-fieldsIdx, nosave);
  // nosave must be at place 4 and we must have at least 7 fields
  if (argc < 7 || fieldsIdx == 0 || (argc - fieldsIdx) % 2 == 0 || (nosave && nosave != 4)) {
    return RedisModule_WrongArity(ctx);
  }

  RedisModule_AutoMemory(ctx);

  IndexSpec *sp = IndexSpec_Load(ctx, RedisModule_StringPtrLen(argv[1], NULL), 1);
  if (sp == NULL) {
    RedisModule_ReplyWithError(ctx, "Unknown Index name");
    goto cleanup;
  }

  RedisSearchCtx sctx = {ctx, sp};

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
  const char *payload = NULL;
  size_t payloadSize = 0;
  RMUtil_ParseArgsAfter("PAYLOAD", argv, argc, "b", &payload, &payloadSize);

  Document doc = NewDocument(argv[2], ds, (argc - fieldsIdx) / 2, lang ? lang : DEFAULT_LANGUAGE,
                             payload, payloadSize);

  size_t len;
  int n = 0;
  for (int i = fieldsIdx + 1; i < argc - 1; i += 2, n++) {
    // printf ("indexing '%s' => '%s'\n", RedisModule_StringPtrLen(argv[i],
    // NULL),
    // RedisModule_StringPtrLen(argv[i+1], NULL));
    doc.fields[n].name = argv[i];
    doc.fields[n].text = argv[i + 1];
  }

  LG_DEBUG("Adding doc %s with %d fields\n", RedisModule_StringPtrLen(doc.docKey, NULL),
           doc.numFields);
  const char *msg = NULL;
  int rc = AddDocument(&sctx, doc, &msg, nosave, replace);
  if (rc == REDISMODULE_ERR) {
    RedisModule_ReplyWithError(ctx, msg ? msg : "Could not index document");
  } else {
    RedisModule_ReplyWithSimpleString(ctx, "OK");
  }
  free(doc.fields);

cleanup:

  return REDISMODULE_OK;
}

/* FT.REPAIR {index} {term} {offset}
 * Repair a key or select a random key for repair.
 *
 * If term is set, we repair the key for the given term. If not, we select a random term key.
 * If offset is set, we start repairing at the given block offset.
 * The returned values are the term repaired, and the block offset we stopped at.
 * In order not to block redis for too long, we work at 10 blocks at most.
 * If we did not finish covering the entire block range, we return the block we stopped at, a-la
 * SCAN. If we finished all the term's blocks, we return 0, which means we can go on to the next
 * term
 */
int RepairCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  RedisModule_AutoMemory(ctx);
  if (argc < 2 || argc > 4) return RedisModule_WrongArity(ctx);

  IndexSpec *sp = IndexSpec_Load(ctx, RedisModule_StringPtrLen(argv[1], NULL), 1);
  if (sp == NULL) {
    return RedisModule_ReplyWithError(ctx, "Unknown Index name");
  }
  RedisSearchCtx sctx = {ctx, sp};

  long long startBlock = 0;
  if (argc == 4) {
    if (RedisModule_StringToLongLong(argv[3], &startBlock) == REDISMODULE_ERR || startBlock < 0) {
      return RedisModule_ReplyWithError(ctx, "Invalid start offset");
    }
  }
  size_t len = 0;
  const char *term = NULL;
  if (argc > 2) {
    term = RedisModule_StringPtrLen(argv[2], &len);
  } else {
    term = Redis_SelectRandomTerm(&sctx, &len);
  }
  printf("Selected term %.*s\n", (int)len, term);
  if (!term) {
    return RedisModule_ReplyWithError(ctx, "Could not find a term");
  }
  RedisModule_Log(ctx, "debug", "Repairing term %.*s", (int)len, term);

  InvertedIndex *idx = Redis_OpenInvertedIndex(&sctx, term, len, 1);
  if (idx == NULL) {
    return RedisModule_ReplyWithError(ctx, "Could not open term index");
  }

  int rc = InvertedIndex_Repair(idx, &sp->docs, startBlock, 10);
  RedisModule_ReplyWithArray(ctx, 2);
  RedisModule_ReplyWithStringBuffer(ctx, term, len);
  return RedisModule_ReplyWithLongLong(ctx, rc);
}

#define __reply_kvnum(n, k, v)                 \
  RedisModule_ReplyWithSimpleString(ctx, k);   \
  RedisModule_ReplyWithDouble(ctx, (double)v); \
  n += 2
#define __reply_kvstr(n, k, v)               \
  RedisModule_ReplyWithSimpleString(ctx, k); \
  RedisModule_ReplyWithSimpleString(ctx, v); \
  n += 2

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
  int n = 0, _ = 0;

  __reply_kvstr(n, "index_name", sp->name);

  RedisModule_ReplyWithSimpleString(ctx, "fields");
  RedisModule_ReplyWithArray(ctx, sp->numFields);
  for (int i = 0; i < sp->numFields; i++) {
    RedisModule_ReplyWithArray(ctx, 5);
    RedisModule_ReplyWithSimpleString(ctx, sp->fields[i].name);
    __reply_kvstr(_, "type", SpecTypeNames[sp->fields[i].type]);
    __reply_kvnum(_, "weight", sp->fields[i].weight);
  }
  n += 2;

  __reply_kvnum(n, "num_docs", sp->stats.numDocuments);
  __reply_kvnum(n, "max_doc_id", sp->docs.maxDocId);
  __reply_kvnum(n, "num_terms", sp->stats.numTerms);
  __reply_kvnum(n, "num_records", sp->stats.numRecords);
  __reply_kvnum(n, "inverted_sz_mb", sp->stats.invertedSize / (float)0x100000);
  __reply_kvnum(n, "inverted_cap_mb", sp->stats.invertedCap / (float)0x100000);

  __reply_kvnum(n, "inverted_cap_ovh", (float)(sp->stats.invertedCap - sp->stats.invertedSize) /
                                           (float)sp->stats.invertedCap);

  __reply_kvnum(n, "offset_vectors_sz_mb", sp->stats.offsetVecsSize / (float)0x100000);
  __reply_kvnum(n, "skip_index_size_mb", sp->stats.skipIndexesSize / (float)0x100000);
  __reply_kvnum(n, "score_index_size_mb", sp->stats.scoreIndexesSize / (float)0x100000);

  __reply_kvnum(n, "doc_table_size_mb", sp->docs.memsize / (float)0x100000);
  __reply_kvnum(n, "key_table_size_mb", TrieMapNode_MemUsage(sp->docs.dim.tm) / (float)0x100000);
  __reply_kvnum(n, "records_per_doc_avg",
                (float)sp->stats.numRecords / (float)sp->stats.numDocuments);
  __reply_kvnum(n, "bytes_per_record_avg",
                (float)sp->stats.invertedSize / (float)sp->stats.numRecords);
  __reply_kvnum(n, "offsets_per_term_avg",
                (float)sp->stats.offsetVecRecords / (float)sp->stats.numRecords);
  __reply_kvnum(n, "offset_bits_per_record_avg",
                8.0F * (float)sp->stats.offsetVecsSize / (float)sp->stats.offsetVecRecords);

  RedisModule_ReplySetArrayLength(ctx, n);
  return REDISMODULE_OK;
}

/* FT.DTADD {index} {key} {flags} {score} [{payload}]
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
  if (argc < 5 || argc > 6) return RedisModule_WrongArity(ctx);

  IndexSpec *sp = IndexSpec_Load(ctx, RedisModule_StringPtrLen(argv[1], NULL), 1);
  if (sp == NULL) {
    return RedisModule_ReplyWithError(ctx, "Unknown Index name");
  }

  long long flags;
  double score;
  if (RMUtil_ParseArgs(argv, argc, 3, "ld", &flags, &score) == REDISMODULE_ERR) {
    return RedisModule_ReplyWithError(ctx, "Could not parse flags and score");
  }

  const char *payload = NULL;
  size_t payloadSize = 0;
  // optionally take the payload
  if (argc == 6) {
    payload = RedisModule_StringPtrLen(argv[5], &payloadSize);
  }
  t_docId d = DocTable_Put(&sp->docs, RedisModule_StringPtrLen(argv[2], NULL), (float)score,
                           (u_char)flags, payload, payloadSize);

  return RedisModule_ReplyWithLongLong(ctx, d);
}

/* FT.DEL {index} {doc_id}
*  Delete a document from the index. Returns 1 if the document was in the index, or 0 if not.
*
*  **NOTE**: This does not actually delete the document from the index, just marks it as deleted
*/
int DeleteCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  RedisModule_AutoMemory(ctx);

  if (argc != 3) return RedisModule_WrongArity(ctx);

  IndexSpec *sp = IndexSpec_Load(ctx, RedisModule_StringPtrLen(argv[1], NULL), 1);
  if (sp == NULL) {
    return RedisModule_ReplyWithError(ctx, "Unknown Index name");
  }

  int rc = DocTable_Delete(&sp->docs, RedisModule_StringPtrLen(argv[2], NULL));
  if (rc == 1) {
    sp->stats.numDocuments--;
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
int AddHashCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  if (argc < 4 || argc > 7) {
    return RedisModule_WrongArity(ctx);
  }

  RedisModule_AutoMemory(ctx);

  IndexSpec *sp = IndexSpec_Load(ctx, RedisModule_StringPtrLen(argv[1], NULL), 1);
  if (sp == NULL) {
    RedisModule_ReplyWithError(ctx, "Unknown Index name");
    goto cleanup;
  }

  int replace = RMUtil_ArgExists("REPLACE", argv, argc, 1);

  RedisSearchCtx sctx = {ctx, sp};

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
  if (Redis_LoadDocument(&sctx, argv[2], &doc) != REDISMODULE_OK) {
    return RedisModule_ReplyWithError(ctx, "Could not load document");
  }
  doc.docKey = argv[2];
  doc.score = ds;
  doc.language = lang ? lang : DEFAULT_LANGUAGE;
  doc.payload = NULL;
  doc.payloadSize = 0;

  LG_DEBUG("Adding doc %s with %d fields\n", RedisModule_StringPtrLen(doc.docKey, NULL),
           doc.numFields);
  const char *msg = NULL;
  int rc = AddDocument(&sctx, doc, &msg, 1, replace);
  if (rc == REDISMODULE_ERR) {
    RedisModule_ReplyWithError(ctx, msg ? msg : "Could not index document");
  } else {
    RedisModule_ReplyWithSimpleString(ctx, "OK");
  }
  free(doc.fields);

cleanup:
  return REDISMODULE_OK;
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
units
(m, km, mi, or ft)

   - INFIELDS num field1 field2 ...: If set, filter the results to ones
appearing only in specific
   fields of the document, like title or url. num is the number of specified
field arguments

   - VERBATIM: If set, we turn off stemming for the query processing. Faster
but
will yield less
    results

   - WITHSCORES: If set, we also return the relative internal score of each
document. this can be
   used to merge results from multiple instances

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

  // Detect "NOCONTENT"
  int nocontent = RMUtil_ArgExists("nocontent", argv, argc, 3);

  // Parse LIMIT argument
  long long first = 0, limit = 10;
  RMUtil_ParseArgsAfter("LIMIT", argv, argc, "ll", &first, &limit);
  if (limit <= 0) {
    return RedisModule_WrongArity(ctx);
  }

  IndexSpec *sp = IndexSpec_Load(ctx, RedisModule_StringPtrLen(argv[1], NULL), 0);
  if (sp == NULL) {
    return RedisModule_ReplyWithError(ctx, "Unknown Index name");
  }

  // if INFIELDS exists, parse the field mask
  int inFieldsIdx = RMUtil_ArgExists("INFIELDS", argv, argc, 3);
  long long numFields = 0;
  u_char fieldMask = 0xff;
  if (inFieldsIdx > 0) {
    RMUtil_ParseArgs(argv, argc, inFieldsIdx + 1, "l", &numFields);
    if (numFields > 0 && inFieldsIdx + 1 + numFields < argc) {
      fieldMask = IndexSpec_ParseFieldMask(sp, &argv[inFieldsIdx + 2], numFields);
    }
    LG_DEBUG("Parsed field mask: 0x%x\n", fieldMask);
  }

  // Parse numeric filter. currently only one supported
  RedisSearchCtx sctx = {ctx, sp};
  Vector *numericFilters = NULL;
  int filterIdx = RMUtil_ArgExists("FILTER", argv, argc, 3);
  if (filterIdx > 0) {
    numericFilters = ParseMultipleFilters(&sctx, &argv[filterIdx], argc - filterIdx);
    if (numericFilters == NULL) {
      RedisModule_ReplyWithError(ctx, "Invalid numeric filter");
      goto end;
    }
  }

  // parse geo filter if present
  GeoFilter gf;
  int filterGeo = 0;
  int gfIdx = RMUtil_ArgExists("GEOFILTER", argv, argc, 3);
  if (gfIdx > 0 && filterIdx + 6 <= argc) {
    if (GeoFilter_Parse(&gf, &argv[gfIdx + 1], 5) == REDISMODULE_ERR) {
      RedisModule_ReplyWithError(ctx, "Invalid geo filter");
      goto end;
    }
    filterGeo = 1;
  }

  // parse WISTHSCORES
  int withscores = RMUtil_ArgExists("WITHSCORES", argv, argc, 3);

  // parse WITHPAYLOADS
  int withpaylaods = RMUtil_ArgExists("WITHPAYLOADS", argv, argc, 3);

  // Parse VERBATIM and LANGUAGE arguments
  int verbatim = RMUtil_ArgExists("VERBATIM", argv, argc, 3);

  // Parse INORDER and SLOP arguments
  int inOrder = RMUtil_ArgIndex("INORDER", argv, argc) >= 0;
  long long slop = -1;
  RMUtil_ParseArgsAfter("SLOP", argv, argc, "l", &slop);

  if (inOrder && slop < 0) {
    slop = __INT_MAX__;
  }

  const char *lang = NULL;

  // make sure we search for "language" only after the query
  if (argc > 3) {
    RMUtil_ParseArgsAfter("LANGUAGE", &argv[3], argc - 3, "c", &lang);
    if (lang && !IsSupportedLanguage(lang, strlen(lang))) {
      RedisModule_ReplyWithError(ctx, "Unsupported Stemmer Language");
      goto end;
    }
  }

  // parse the optional expander argument
  const char *expander = NULL;
  if (argc > 3) {
    RMUtil_ParseArgsAfter("EXPANDER", &argv[2], argc - 2, "c", &expander);
  }
  if (!expander) {
    expander = STEMMER_EXPANDER_NAME;
  }

  int nostopwords = RMUtil_ArgExists("NOSTOPWORDS", argv, argc, 3);

  // parse the id filter arguments
  long long numFilteredIds = 0;
  IdFilter idf;
  RMUtil_ParseArgsAfter("INKEYS", &argv[2], argc - 2, "l", &numFilteredIds);
  if (numFilteredIds > 0 && numFilteredIds < argc - 3) {

    RedisModule_Log(ctx, "debug", "Filtering %d keys", numFilteredIds);
    int pos = RMUtil_ArgIndex("INKEYS", argv, argc);
    idf = NewIdFilter(&argv[pos + 2], MIN(argc - pos - 2, numFilteredIds), &sctx.spec->docs);
  } else {
    numFilteredIds = 0;
  }

  size_t len;
  const char *qs = RedisModule_StringPtrLen(argv[2], &len);
  Query *q = NewQuery(&sctx, (char *)qs, len, first, limit, fieldMask, verbatim, lang,
                      nostopwords ? NULL : DEFAULT_STOPWORDS, expander, slop, inOrder);

  char *errMsg = NULL;
  if (!Query_Parse(q, &errMsg)) {

    RedisModule_Log(ctx, "debug", "Error parsing query: %s", errMsg);
    RedisModule_ReplyWithError(ctx, errMsg);
    free(errMsg);
    Query_Free(q);
    goto end;
  }

  Query_Expand(q);

  // set numeric filters if possible
  if (numericFilters) {
    for (int i = 0; i < Vector_Size(numericFilters); i++) {
      NumericFilter *nf;
      Vector_Get(numericFilters, i, &nf);
      if (nf) {
        Query_SetNumericFilter(q, nf);
      }
    }

    Vector_Free(numericFilters);
  }

  if (filterGeo) {
    Query_SetGeoFilter(q, &gf);
  }

  if (numFilteredIds > 0) {
    Query_SetIdFilter(q, &idf);
  }
  q->docTable = &sp->docs;

  // Execute the query
  QueryResult *r = Query_Execute(q);
  if (r == NULL) {
    RedisModule_ReplyWithError(ctx, QUERY_ERROR_INTERNAL_STR);
    goto end;
  }

  QueryResult_Serialize(r, &sctx, nocontent, withscores, withpaylaods);

  QueryResult_Free(r);
  Query_Free(q);
end:
  return REDISMODULE_OK;
}

/*
## FT.CREATE {index} [NOOFFSETS] [NOFIELDS] [NOSCOREIDX]
    SCHEMA {field} [TEXT [WEIGHT {weight}]] | [NUMERIC] ...

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

    - NOSCOREIDX: If set, we avoid saving the top results for single words. Saves a lot of memory,
      slows down searches for common single word queries

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

  char *err;

  IndexSpec *sp = IndexSpec_ParseRedisArgs(ctx, argv[1], &argv[2], argc - 2, &err);
  if (sp == NULL) {
    return RedisModule_ReplyWithError(ctx, err ? err : "Could not parse index spec");
  }

  RedisModuleKey *k =
      RedisModule_OpenKey(ctx, RedisModule_CreateStringPrintf(ctx, INDEX_SPEC_KEY_FMT, sp->name),
                          REDISMODULE_READ | REDISMODULE_WRITE);

  // check that the key is empty
  if (k == NULL || (RedisModule_KeyType(k) != REDISMODULE_KEYTYPE_EMPTY)) {
    if (RedisModule_ModuleTypeGetType(k) != IndexSpecType)
      return RedisModule_ReplyWithError(ctx, "Wrong type for index key");
    else
      return RedisModule_ReplyWithError(ctx, "Index already exists. Drop it first!");
  }

  RedisModule_ModuleTypeSetValue(k, IndexSpecType, sp);

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

  /* Zero the stats in sp that are affected by optimization */
  sp->stats.invertedCap = 0;
  sp->stats.invertedSize = 0;
  sp->stats.scoreIndexesSize = 0;
  sp->stats.skipIndexesSize = 0;

  RedisSearchCtx sctx = {ctx, sp};

  RedisModuleString *pf = fmtRedisTermKey(&sctx, "*", 1);
  size_t len;
  const char *prefix = RedisModule_StringPtrLen(pf, &len);

  // RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
  int num = Redis_ScanKeys(ctx, prefix, Redis_OptimizeScanHandler, &sctx);
  return RedisModule_ReplyWithLongLong(ctx, num);
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

  RedisModule_AutoMemory(ctx);

  IndexSpec *sp = IndexSpec_Load(ctx, RedisModule_StringPtrLen(argv[1], NULL), 0);
  if (sp == NULL) {
    return RedisModule_ReplyWithError(ctx, "Unknown Index name");
  }

  RedisSearchCtx sctx = {ctx, sp};

  Redis_DropIndex(&sctx, 1);
  return RedisModule_ReplyWithSimpleString(ctx, "OK");
}

/*
## FT.SUGGADD key string score [INCR]

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

### Returns:

Integer reply: the current size of the suggestion dictionary.
*/
int SuggestAddCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  if (argc < 4 || argc > 5) return RedisModule_WrongArity(ctx);

  RedisModule_AutoMemory(ctx); /* Use automatic memory management. */

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

  /* Create an empty value object if the key is currently empty. */
  Trie *tree;
  if (type == REDISMODULE_KEYTYPE_EMPTY) {
    tree = NewTrie();
    RedisModule_ModuleTypeSetValue(key, TrieType, tree);
  } else {
    tree = RedisModule_ModuleTypeGetValue(key);
  }

  /* Insert the new element. */
  Trie_Insert(tree, val, score, incr);

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
  RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ);
  int type = RedisModule_KeyType(key);
  if (type != REDISMODULE_KEYTYPE_EMPTY && RedisModule_ModuleTypeGetType(key) != TrieType) {
    return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
  }

  Trie *tree = RedisModule_ModuleTypeGetValue(key);
  if (!tree) {
    RedisModule_ReplyWithLongLong(ctx, 0);
  }
  size_t len;
  const char *str = RedisModule_StringPtrLen(argv[2], &len);
  return RedisModule_ReplyWithLongLong(ctx, Trie_Delete(tree, (char *)str, len));
}

/*
## FT.SUGGET key prefix [FUZZY] [MAX num] [WITHSCORES] [TRIM]

Get completion suggestions for a prefix

### Parameters:

   - key: the suggestion dictionary key

   - prefix: the prefix to complete on

   - FUZZY: if set,we do a fuzzy prefix search, including prefixes at
levenshtein distance of 1
    from the prefix sent

   - MAX num: If set, we limit the results to a maximum of `num`. The default
is
5, and the number
    cannot be greater than 10.

   - WITHSCORES: If set, we also return each entry's score

   - TRIM: If set, we remove very unlikely results

### Returns:

Array reply: a list of the top suggestions matching the prefix

*/
int SuggestGetCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  RedisModule_AutoMemory(ctx); /* Use automatic memory management. */

  if (argc < 3 || argc > 8) return RedisModule_WrongArity(ctx);

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

  Vector *res = Trie_Search(tree, s, len, num, maxDist, 1, trim, optimize);

  // if we also need to return scores, we need double the records
  RedisModule_ReplyWithArray(ctx, Vector_Size(res) * (withScores ? 2 : 1));

  for (int i = 0; i < Vector_Size(res); i++) {
    TrieSearchResult *e;
    Vector_Get(res, i, &e);

    RedisModule_ReplyWithStringBuffer(ctx, e->str, e->len);
    if (withScores) {
      RedisModule_ReplyWithDouble(ctx, e->score);
    }

    TrieSearchResult_Free(e);
  }
  Vector_Free(res);

  return REDISMODULE_OK;
}

int RedisModule_OnLoad(RedisModuleCtx *ctx) {

  // LOGGING_INIT(0xFFFFFFFF);
  if (RedisModule_Init(ctx, "ft", 1, REDISMODULE_APIVER_1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;

  /* Self initialization */
  RegisterStemmerExpander();

  // register trie type
  if (TrieType_Register(ctx) == REDISMODULE_ERR) return REDISMODULE_ERR;

  if (IndexSpec_RegisterType(ctx) == REDISMODULE_ERR) return REDISMODULE_ERR;

  if (InvertedIndex_RegisterType(ctx) == REDISMODULE_ERR) return REDISMODULE_ERR;

  if (NumericIndexType_Register(ctx) == REDISMODULE_ERR) return REDISMODULE_ERR;

  if (RedisModule_CreateCommand(ctx, "ft.add", AddDocumentCommand, "write deny-oom no-cluster", 1,
                                1, 1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;

  if (RedisModule_CreateCommand(ctx, "ft.addhash", AddHashCommand, "write deny-oom no-cluster", 1,
                                1, 1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;
  if (RedisModule_CreateCommand(ctx, "ft.dtadd", DTAddCommand, "write deny-oom no-cluster", 1, 1,
                                1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;

  if (RedisModule_CreateCommand(ctx, "ft.del", DeleteCommand, "write no-cluster", 1, 1, 1) ==
      REDISMODULE_ERR)
    return REDISMODULE_ERR;

  if (RedisModule_CreateCommand(ctx, "ft.repair", RepairCommand, "write no-cluster", 1, 1, 1) ==
      REDISMODULE_ERR)
    return REDISMODULE_ERR;

  if (RedisModule_CreateCommand(ctx, "ft.search", SearchCommand, "readonly deny-oom no-cluster", 1,
                                1, 1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;

  if (RedisModule_CreateCommand(ctx, "ft.create", CreateIndexCommand, "write no-cluster", 1, 1,
                                1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;

  if (RedisModule_CreateCommand(ctx, "ft.optimize", OptimizeIndexCommand, "write no-cluster", 1, 1,
                                1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;

  if (RedisModule_CreateCommand(ctx, "ft.drop", DropIndexCommand, "write no-cluster", 1, 1, 1) ==
      REDISMODULE_ERR)
    return REDISMODULE_ERR;

  if (RedisModule_CreateCommand(ctx, "ft.info", IndexInfoCommand, "readonly no-cluster", 1, 1, 1) ==
      REDISMODULE_ERR)
    return REDISMODULE_ERR;

  if (RedisModule_CreateCommand(ctx, TRIE_ADD_CMD, SuggestAddCommand, "write no-cluster", 1, 1,
                                1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;

  if (RedisModule_CreateCommand(ctx, TRIE_DEL_CMD, SuggestDelCommand, "write no-cluster", 1, 1,
                                1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;

  if (RedisModule_CreateCommand(ctx, TRIE_LEN_CMD, SuggestLenCommand, "readonly no-cluster", 1, 1,
                                1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;

  if (RedisModule_CreateCommand(ctx, TRIE_SEARCH_CMD, SuggestGetCommand, "readonly no-cluster", 1,
                                1, 1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;

  return REDISMODULE_OK;
}
