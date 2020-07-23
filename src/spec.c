#include "spec.h"

#include <math.h>
#include <ctype.h>

#include "util/logging.h"
#include "util/misc.h"
#include "rmutil/vector.h"
#include "rmutil/util.h"
#include "rmutil/rm_assert.h"
#include "trie/trie_type.h"
#include "rmalloc.h"
#include "config.h"
#include "cursor.h"
#include "tag_index.h"
#include "redis_index.h"
#include "indexer.h"
#include "alias.h"
#include "module.h"
#include "aggregate/expr/expression.h"
#include "rules.h"
#include "commands.h"
#include "dictionary.h"

///////////////////////////////////////////////////////////////////////////////////////////////

void (*IndexSpec_OnCreate)(const IndexSpec *) = NULL;
const char *(*IndexAlias_GetUserTableName)(RedisModuleCtx *, const char *) = NULL;

RedisModuleType *IndexSpecType;
static uint64_t spec_unique_ids = 1;

dict *specDict;

//---------------------------------------------------------------------------------------------

static const FieldSpec *getFieldCommon(const IndexSpec *spec, const char *name, size_t len) {
  for (size_t i = 0; i < spec->numFields; i++) {
    if (len != strlen(spec->fields[i].name)) {
      continue;
    }
    const FieldSpec *fs = spec->fields + i;
    if (!strncmp(fs->name, name, len)) {
      return fs;
    }
  }
  return NULL;
}

/*
 * Get a field spec by field name. Case sensetive!
 * Return the field spec if found, NULL if not
 */
const FieldSpec *IndexSpec_GetField(const IndexSpec *spec, const char *name, size_t len) {
  return getFieldCommon(spec, name, len);
};

t_fieldMask IndexSpec_GetFieldBit(IndexSpec *spec, const char *name, size_t len) {
  const FieldSpec *fs = IndexSpec_GetField(spec, name, len);
  if (!fs || !FIELD_IS(fs, INDEXFLD_T_FULLTEXT) || !FieldSpec_IsIndexable(fs)) return 0;

  return FIELD_BIT(fs);
}

int IndexSpec_CheckPhoneticEnabled(const IndexSpec *sp, t_fieldMask fm) {
  if (!(sp->flags & Index_HasPhonetic)) {
    return 0;
  }

  if (fm == 0 || fm == (t_fieldMask)-1) {
    // No fields -- implicit phonetic match!
    return 1;
  }

  for (size_t ii = 0; ii < sp->numFields; ++ii) {
    if (fm & ((t_fieldMask)1 << ii)) {
      const FieldSpec *fs = sp->fields + ii;
      if (FIELD_IS(fs, INDEXFLD_T_FULLTEXT) && (FieldSpec_IsPhonetics(fs))) {
        return 1;
      }
    }
  }
  return 0;
}

int IndexSpec_GetFieldSortingIndex(IndexSpec *sp, const char *name, size_t len) {
  if (!sp->sortables) return -1;
  return RSSortingTable_GetFieldIdx(sp->sortables, name);
}

const FieldSpec *IndexSpec_GetFieldBySortingIndex(const IndexSpec *sp, uint16_t idx) {
  for (size_t ii = 0; ii < sp->numFields; ++ii) {
    if (sp->fields[ii].options & FieldSpec_Sortable && sp->fields[ii].sortIdx == idx) {
      return sp->fields + ii;
    }
  }
  return NULL;
}

const char *GetFieldNameByBit(const IndexSpec *sp, t_fieldMask id) {
  for (int i = 0; i < sp->numFields; i++) {
    if (FIELD_BIT(&sp->fields[i]) == id && FIELD_IS(&sp->fields[i], INDEXFLD_T_FULLTEXT) &&
        FieldSpec_IsIndexable(&sp->fields[i])) {
      return sp->fields[i].name;
    }
  }
  return NULL;
}

//---------------------------------------------------------------------------------------------

/*
* Parse an index spec from redis command arguments.
* Returns REDISMODULE_ERR if there's a parsing error.
* The command only receives the relevant part of argv.
*
* The format currently is FT.CREATE {index} [NOOFFSETS] [NOFIELDS] [NOFREQS]
    SCHEMA {field} [TEXT [WEIGHT {weight}]] | [NUMERIC]
*/
IndexSpec *IndexSpec_ParseRedisArgs(RedisModuleCtx *ctx, RedisModuleString *name,
                                    RedisModuleString **argv, int argc, QueryError *status) {

  const char *args[argc];
  for (int i = 0; i < argc; i++) {
    args[i] = RedisModule_StringPtrLen(argv[i], NULL);
  }

  IndexSpec *sp = IndexSpec_Parse(RedisModule_StringPtrLen(name, NULL), args, argc, status);
  return sp;
}

FieldSpec **getFieldsByType(IndexSpec *spec, FieldType type) {
#define FIELDS_ARRAY_CAP 2
  FieldSpec **fields = array_new(FieldSpec *, FIELDS_ARRAY_CAP);
  for (int i = 0; i < spec->numFields; ++i) {
    if (FIELD_IS(spec->fields + i, type)) {
      fields = array_append(fields, &(spec->fields[i]));
    }
  }
  return fields;
}

/* Check if Redis is currently loading from RDB. Our thread starts before RDB loading is finished */
int isRdbLoading(RedisModuleCtx *ctx) {
  long long isLoading = 0;
  RMUtilInfo *info = RMUtil_GetRedisInfo(ctx);
  if (!info) {
    return 0;
  }

  if (!RMUtilInfo_GetInt(info, "loading", &isLoading)) {
    isLoading = 0;
  }

  RMUtilRedisInfo_Free(info);
  return isLoading == 1;
}

static void IndexSpec_TimedOutProc(RedisModuleCtx *ctx, IndexSpec *sp) {
  // we need to delete the spec from the specDict, as far as the user see it,
  // this spec was deleted and its memory will be freed in a background thread.
  dictDelete(specDict, sp->name);
  sp->isTimerSet = false;
  IndexSpec_Free(sp);
}

static void IndexSpec_SetTimeoutTimer(IndexSpec *sp) {
  if (sp->isTimerSet) {
    RedisModule_StopTimer(RSDummyContext, sp->timerId, NULL);
  }
  sp->timerId = RedisModule_CreateTimer(RSDummyContext, sp->timeout, 
                                        (RedisModuleTimerProc) IndexSpec_TimedOutProc, sp);
  sp->isTimerSet = true;
}

static void Indexes_SetTempSpecsTimers() {
  dictIterator *iter = dictGetIterator(specDict);
  dictEntry *entry = NULL;
  while ((entry = dictNext(iter))) {
    IndexSpec *sp = dictGetVal(entry);
    if (sp->flags & Index_Temporary) {
      IndexSpec_SetTimeoutTimer(sp);
    }
  }
  dictReleaseIterator(iter);
}

//---------------------------------------------------------------------------------------------

static void IndexSpec_ScanAndReindex(IndexSpec *sp);

IndexSpec *IndexSpec_CreateNew(RedisModuleCtx *ctx, RedisModuleString **argv, int argc,
                               QueryError *status) {
  const char *specName = RedisModule_StringPtrLen(argv[1], NULL);
  if (dictFetchValue(specDict, specName)) {
    QueryError_SetCode(status, QUERY_EINDEXEXISTS);
    return NULL;
  }
  IndexSpec *sp = IndexSpec_ParseRedisArgs(ctx, argv[1], &argv[2], argc - 2, status);
  if (sp == NULL) {
    return NULL;
  }

  dictAdd(specDict, (char *)specName, sp);

  sp->uniqueId = spec_unique_ids++;
  // Start the garbage collector
  IndexSpec_StartGC(ctx, sp, GC_DEFAULT_HZ);

  CursorList_AddSpec(&RSCursors, sp->name, RSCURSORS_DEFAULT_CAPACITY);

  // Create the indexer
  sp->indexer = NewIndexer(sp);
  if (IndexSpec_OnCreate) {
    IndexSpec_OnCreate(sp);
  }

  // TODO: decide whether to start counting before or after reindexing (after would lead 
  // to different timeouts in sync/async)
  if (sp->flags & Index_Temporary) {
    IndexSpec_SetTimeoutTimer(sp);
  }

  IndexSpec_ScanAndReindex(sp);

  return sp;
}

//---------------------------------------------------------------------------------------------

static void FieldSpec_RdbLoad(RedisModuleIO *rdb, FieldSpec *f, int encver);

static bool checkPhoneticAlgorithmAndLang(const char *matcher) {
  if (strlen(matcher) != 5) {
    return false;
  }
  if (matcher[0] != 'd' || matcher[1] != 'm' || matcher[2] != ':') {
    return false;
  }

#define LANGUAGES_SIZE 4
  char *languages[] = {"en", "pt", "fr", "es"};

  bool langauge_found = false;
  for (int i = 0; i < LANGUAGES_SIZE; ++i) {
    if (matcher[3] == languages[i][0] && matcher[4] == languages[i][1]) {
      langauge_found = true;
    }
  }

  return langauge_found;
}

static int parseTextField(FieldSpec *fs, ArgsCursor *ac, QueryError *status) {
  int rc;
  // this is a text field
  // init default weight and type
  while (!AC_IsAtEnd(ac)) {
    if (AC_AdvanceIfMatch(ac, SPEC_NOSTEM_STR)) {
      fs->options |= FieldSpec_NoStemming;
      continue;

    } else if (AC_AdvanceIfMatch(ac, SPEC_WEIGHT_STR)) {
      double d;
      if ((rc = AC_GetDouble(ac, &d, 0)) != AC_OK) {
        QERR_MKBADARGS_AC(status, "weight", rc);
        return 0;
      }
      fs->ftWeight = d;
      continue;

    } else if (AC_AdvanceIfMatch(ac, SPEC_PHONETIC_STR)) {
      if (AC_IsAtEnd(ac)) {
        QueryError_SetError(status, QUERY_EPARSEARGS, SPEC_PHONETIC_STR " requires an argument");
        return 0;
      }

      const char *matcher = AC_GetStringNC(ac, NULL);
      // try and parse the matcher
      // currently we just make sure algorithm is double metaphone (dm)
      // and language is one of the following : English (en), French (fr), Portuguese (pt) and
      // Spanish (es)
      // in the future we will support more algorithms and more languages
      if (!checkPhoneticAlgorithmAndLang(matcher)) {
        QueryError_SetError(
            status, QUERY_EINVAL,
            "Matcher Format: <2 chars algorithm>:<2 chars language>. Support algorithms: "
            "double metaphone (dm). Supported languages: English (en), French (fr), "
            "Portuguese (pt) and Spanish (es)");
        return 0;
      }
      fs->options |= FieldSpec_Phonetics;
      continue;

    } else {
      break;
    }
  }
  return 1;
}

void FieldSpec_Initialize(FieldSpec *fs, FieldType types) {
  fs->types |= types;
  if (FIELD_IS(fs, INDEXFLD_T_TAG)) {
    fs->tagFlags = TAG_FIELD_DEFAULT_FLAGS;
    fs->tagSep = TAG_FIELD_DEFAULT_SEP;
  }
}

/* Parse a field definition from argv, at *offset. We advance offset as we progress.
 *  Returns 1 on successful parse, 0 otherwise */
static int parseFieldSpec(ArgsCursor *ac, FieldSpec *fs, QueryError *status) {
  if (AC_IsAtEnd(ac)) {
    QueryError_SetErrorFmt(status, QUERY_EPARSEARGS, "Field `%s` does not have a type", fs->name);
    return 0;
  }

  if (AC_AdvanceIfMatch(ac, SPEC_TEXT_STR)) {
    FieldSpec_Initialize(fs, INDEXFLD_T_FULLTEXT);
    if (!parseTextField(fs, ac, status)) {
      goto error;
    }
  } else if (AC_AdvanceIfMatch(ac, NUMERIC_STR)) {
    FieldSpec_Initialize(fs, INDEXFLD_T_NUMERIC);
  } else if (AC_AdvanceIfMatch(ac, GEO_STR)) {  // geo field
    FieldSpec_Initialize(fs, INDEXFLD_T_GEO);
  } else if (AC_AdvanceIfMatch(ac, SPEC_TAG_STR)) {  // tag field
    FieldSpec_Initialize(fs, INDEXFLD_T_TAG);
    if (AC_AdvanceIfMatch(ac, SPEC_SEPARATOR_STR)) {
      if (AC_IsAtEnd(ac)) {
        QueryError_SetError(status, QUERY_EPARSEARGS, SPEC_SEPARATOR_STR " requires an argument");
        goto error;
      }
      const char *sep = AC_GetStringNC(ac, NULL);
      if (strlen(sep) != 1) {
        QueryError_SetErrorFmt(status, QUERY_EPARSEARGS,
                               "Tag separator must be a single character. Got `%s`", sep);
        goto error;
      }
      fs->tagSep = *sep;
    }
  } else {  // not numeric and not text - nothing more supported currently
    QueryError_SetErrorFmt(status, QUERY_EPARSEARGS, "Invalid field type for field `%s`", fs->name);
    goto error;
  }

  while (!AC_IsAtEnd(ac)) {
    if (AC_AdvanceIfMatch(ac, SPEC_SORTABLE_STR)) {
      FieldSpec_SetSortable(fs);
      continue;
    } else if (AC_AdvanceIfMatch(ac, SPEC_NOINDEX_STR)) {
      fs->options |= FieldSpec_NotIndexable;
      continue;
    } else {
      break;
    }
  }
  return 1;

error:
  if (!QueryError_HasError(status)) {
    QueryError_SetErrorFmt(status, QUERY_EPARSEARGS, "Could not parse schema for field `%s`",
                           fs->name);
  }
  FieldSpec_Cleanup(fs);
  return 0;
}

int IndexSpec_CreateTextId(const IndexSpec *sp) {
  int maxId = -1;
  for (size_t ii = 0; ii < sp->numFields; ++ii) {
    const FieldSpec *fs = sp->fields + ii;
    if (FIELD_IS(fs, INDEXFLD_T_FULLTEXT)) {
      if (fs->ftId == (t_fieldId)-1) {
        // ignore
        continue;
      }
      maxId = MAX(fs->ftId, maxId);
    }
  }

  if (maxId + 1 >= SPEC_MAX_FIELD_ID) {
    return -1;
  }
  return maxId + 1;
}

/**
 * Add fields to an existing (or newly created) index. If the addition fails,
 */
static int IndexSpec_AddFieldsInternal(IndexSpec *sp, ArgsCursor *ac, QueryError *status,
                                       int isNew) {
  if (sp->spcache) {
    IndexSpecCache_Decref(sp->spcache);
    sp->spcache = NULL;
  }
  const size_t prevNumFields = sp->numFields;
  const size_t prevSortLen = sp->sortables->len;
  FieldSpec *fs = NULL;

  while (!AC_IsAtEnd(ac)) {
    size_t nfieldName = 0;
    const char *fieldName = AC_GetStringNC(ac, &nfieldName);
    if (IndexSpec_GetField(sp, fieldName, nfieldName)) {
      QueryError_SetError(status, QUERY_EINVAL, "Duplicate field in schema");
      goto reset;
    }

    fs = IndexSpec_CreateField(sp, fieldName);

    if (!parseFieldSpec(ac, fs, status)) {
      goto reset;
    }

    if (FIELD_IS(fs, INDEXFLD_T_FULLTEXT) && FieldSpec_IsIndexable(fs)) {
      int textId = IndexSpec_CreateTextId(sp);
      if (textId < 0) {
        QueryError_SetError(status, QUERY_ELIMIT, "Too many TEXT fields in schema");
        goto reset;
      }

      // If we need to store field flags and we have over 32 fields, we need to switch to wide
      // schema encoding
      if (textId >= SPEC_WIDEFIELD_THRESHOLD && (sp->flags & Index_StoreFieldFlags)) {
        if (isNew) {
          sp->flags |= Index_WideSchema;
        } else if ((sp->flags & Index_WideSchema) == 0) {
          QueryError_SetError(
              status, QUERY_ELIMIT,
              "Cannot add more fields. Declare index with wide fields to allow adding "
              "unlimited fields");
          goto reset;
        }
      }
      fs->ftId = textId;
    }

    if (FieldSpec_IsSortable(fs)) {
      if (fs->options & FieldSpec_Dynamic) {
        QueryError_SetError(status, QUERY_EBADOPTION, "Cannot set dynamic field to sortable");
        goto reset;
      }

      fs->sortIdx = RSSortingTable_Add(sp->sortables, fs->name, fieldTypeToValueType(fs->types));
    } else {
      fs->sortIdx = -1;
    }
    if (FieldSpec_IsPhonetics(fs)) {
      sp->flags |= Index_HasPhonetic;
    }
    fs = NULL;
  }
  return 1;

reset:
  // If the current field spec exists, but was not added (i.e. we got an error)
  // and reached this block, then free it
  if (fs) {
    // if we have a field spec it means that we increased the number of fields, so we need to
    // decreas it.
    --sp->numFields;
    FieldSpec_Cleanup(fs);
  }
  for (size_t ii = prevNumFields; ii < sp->numFields; ++ii) {
    FieldSpec_Cleanup(&sp->fields[ii]);
  }

  sp->numFields = prevNumFields;
  sp->sortables->len = prevSortLen;
  return 0;
}

int IndexSpec_AddFields(IndexSpec *sp, ArgsCursor *ac, QueryError *status) {
  int rc = IndexSpec_AddFieldsInternal(sp, ac, status, 0);
  if (rc) {
    IndexSpec_ScanAndReindex(sp);
  }
  return rc;
}

/* The format currently is FT.CREATE {index} [NOOFFSETS] [NOFIELDS]
    SCHEMA {field} [TEXT [WEIGHT {weight}]] | [NUMERIC]
  */
IndexSpec *IndexSpec_Parse(const char *name, const char **argv, int argc, QueryError *status) {
  IndexSpec *spec = NewIndexSpec(name);

  IndexSpec_MakeKeyless(spec);

  ArgsCursor ac = {0};
  ArgsCursor acStopwords = {0};

  ArgsCursor_InitCString(&ac, argv, argc);
  long long timeout = -1;
  int dummy;
  size_t dummy2;
  SchemaRuleArgs rule_args = {0};
  ArgsCursor rule_prefixes = {0};

  ACArgSpec argopts[] = {
      {AC_MKUNFLAG(SPEC_NOOFFSETS_STR, &spec->flags,
                   Index_StoreTermOffsets | Index_StoreByteOffsets)},
      {AC_MKUNFLAG(SPEC_NOHL_STR, &spec->flags, Index_StoreByteOffsets)},
      {AC_MKUNFLAG(SPEC_NOFIELDS_STR, &spec->flags, Index_StoreFieldFlags)},
      {AC_MKUNFLAG(SPEC_NOFREQS_STR, &spec->flags, Index_StoreFreqs)},
      {AC_MKBITFLAG(SPEC_SCHEMA_EXPANDABLE_STR, &spec->flags, Index_WideSchema)},
      {AC_MKBITFLAG(SPEC_ASYNC_STR, &spec->flags, Index_Async)},

      // For compatibility
      {.name = "NOSCOREIDX", .target = &dummy, .type = AC_ARGTYPE_BOOLFLAG},
      {.name = "ON", .target = &rule_args.type, .len = &dummy2, .type = AC_ARGTYPE_STRING},
      {.name = "PREFIX", .target = &rule_prefixes, .type = AC_ARGTYPE_SUBARGS},
      {.name = "FILTER",
       .target = &rule_args.filter_exp_str,
       .len = &dummy2,
       .type = AC_ARGTYPE_STRING},
      {.name = "SCORE",
       .target = &rule_args.score_field,
       .len = &dummy2,
       .type = AC_ARGTYPE_STRING},
      {.name = "LANGUAGE",
       .target = &rule_args.lang_field,
       .len = &dummy2,
       .type = AC_ARGTYPE_STRING},
      {.name = "PAYLOAD",
       .target = &rule_args.payload_field,
       .len = &dummy2,
       .type = AC_ARGTYPE_STRING},
      {.name = SPEC_TEMPORARY_STR, .target = &timeout, .type = AC_ARGTYPE_LLONG},
      {.name = SPEC_STOPWORDS_STR, .target = &acStopwords, .type = AC_ARGTYPE_SUBARGS},
      {.name = NULL}};

  ACArgSpec *errarg = NULL;
  int rc = AC_ParseArgSpec(&ac, argopts, &errarg);
  if (rc != AC_OK) {
    if (rc != AC_ERR_ENOENT) {
      QERR_MKBADARGS_AC(status, errarg->name, rc);
      goto failure;
    }
  }

  if (timeout != -1) {
    spec->flags |= Index_Temporary;
  }
  spec->timeout = timeout * 1000;  // convert to ms

  if (rule_prefixes.argc > 0) {
    rule_args.nprefixes = rule_prefixes.argc;
    rule_args.prefixes = (const char **)rule_prefixes.objs;
  } else {
    rule_args.nprefixes = 1;
    static const char *empty_prefix[] = {""};
    rule_args.prefixes = empty_prefix;
  }

  spec->rule = SchemaRule_Create(&rule_args, spec, status);
  if (!spec->rule) {
    goto failure;
  }

  if (AC_IsInitialized(&acStopwords)) {
    if (spec->stopwords) {
      StopWordList_Unref(spec->stopwords);
    }
    spec->stopwords = NewStopWordListCStr((const char **)acStopwords.objs, acStopwords.argc);
    spec->flags |= Index_HasCustomStopwords;
  }

  if (!AC_AdvanceIfMatch(&ac, SPEC_SCHEMA_STR)) {
    if (AC_NumRemaining(&ac)) {
      const char *badarg = AC_GetStringNC(&ac, NULL);
      QueryError_SetErrorFmt(status, QUERY_EPARSEARGS, "Unknown argument `%s`", badarg);
    } else {
      QueryError_SetError(status, QUERY_EPARSEARGS, "No schema found");
    }
    goto failure;
  }

  if (!IndexSpec_AddFieldsInternal(spec, &ac, status, 1)) {
    goto failure;
  }

  return spec;

failure:  // on failure free the spec fields array and return an error
  IndexSpec_Free(spec);
  return NULL;
}

/* Initialize some index stats that might be useful for scoring functions */
void IndexSpec_GetStats(IndexSpec *sp, RSIndexStats *stats) {
  stats->numDocs = sp->stats.numDocuments;
  stats->numTerms = sp->stats.numTerms;
  stats->avgDocLen =
      stats->numDocs ? (double)sp->stats.numRecords / (double)sp->stats.numDocuments : 0;
}

int IndexSpec_AddTerm(IndexSpec *sp, const char *term, size_t len) {
  int isNew = Trie_InsertStringBuffer(sp->terms, (char *)term, len, 1, 1, NULL);
  if (isNew) {
    sp->stats.numTerms++;
    sp->stats.termsSize += len;
  }
  return isNew;
}

IndexSpecCache *IndexSpec_GetSpecCache(const IndexSpec *spec) {
  if (!spec->spcache) {
    ((IndexSpec *)spec)->spcache = IndexSpec_BuildSpecCache(spec);
  }

  spec->spcache->refcount++;
  return spec->spcache;
}

IndexSpecCache *IndexSpec_BuildSpecCache(const IndexSpec *spec) {
  IndexSpecCache *ret = rm_calloc(1, sizeof(*ret));
  ret->nfields = spec->numFields;
  ret->fields = rm_malloc(sizeof(*ret->fields) * ret->nfields);
  ret->refcount = 1;
  for (size_t ii = 0; ii < spec->numFields; ++ii) {
    ret->fields[ii] = spec->fields[ii];
    ret->fields[ii].name = rm_strdup(ret->fields[ii].name);
  }
  return ret;
}

void IndexSpecCache_Decref(IndexSpecCache *c) {
  if (--c->refcount) {
    return;
  }
  for (size_t ii = 0; ii < c->nfields; ++ii) {
    rm_free(c->fields[ii].name);
  }
  rm_free(c->fields);
  rm_free(c);
}

/// given an array of random weights, return the a weighted random selection, as the index in the
/// array
size_t weightedRandom(double weights[], size_t len) {

  double totalWeight = 0;
  for (size_t i = 0; i < len; i++) {
    totalWeight += weights[i];
  }
  double selection = totalWeight * ((double)rand() / (double)(RAND_MAX));

  totalWeight = 0;
  for (size_t i = 0; i < len; i++) {
    if (selection >= totalWeight && selection <= (totalWeight + weights[i])) {
      return i;
    }
    totalWeight += weights[i];
  }
  // fallback
  return 0;
}

/* Get a random term from the index spec using weighted random. Weighted random is done by
 * sampling N terms from the index and then doing weighted random on them. A sample size of 10-20
 * should be enough. Returns NULL if the index is empty */
char *IndexSpec_GetRandomTerm(IndexSpec *sp, size_t sampleSize) {

  if (sampleSize > sp->terms->size) {
    sampleSize = sp->terms->size;
  }
  if (!sampleSize) return NULL;

  char *samples[sampleSize];
  double weights[sampleSize];
  for (int i = 0; i < sampleSize; i++) {
    char *ret = NULL;
    t_len len = 0;
    double d = 0;
    if (!Trie_RandomKey(sp->terms, &ret, &len, &d) || len == 0) {
      return NULL;
    }
    samples[i] = ret;
    weights[i] = d;
  }

  size_t selection = weightedRandom(weights, sampleSize);
  for (int i = 0; i < sampleSize; i++) {
    if (i != selection) {
      rm_free(samples[i]);
    }
  }
  // printf("Selected %s --> %f\n", samples[selection], weights[selection]);
  return samples[selection];
}

///////////////////////////////////////////////////////////////////////////////////////////////

#if 0

void IndexSpec_FreeWithKey(IndexSpec *sp, RedisModuleCtx *ctx) {
  RedisModuleString *s = RedisModule_CreateStringPrintf(ctx, INDEX_SPEC_KEY_FMT, sp->name);
  RedisModuleKey *kk = RedisModule_OpenKey(ctx, s, REDISMODULE_WRITE);
  RedisModule_FreeString(ctx, s);
  if (kk == NULL || RedisModule_KeyType(kk) != REDISMODULE_KEYTYPE_MODULE ||
      RedisModule_ModuleTypeGetType(kk) != IndexSpecType) {
    if (kk != NULL) {
      RedisModule_CloseKey(kk);
    }
    IndexSpec_Free(sp);
    return;
  }
  RS_LOG_ASSERT(RedisModule_ModuleTypeGetValue(kk) == sp, "IndexSpecs should be identical");
  RedisModule_DeleteKey(kk);
  RedisModule_CloseKey(kk);
}

#endif // 0

//---------------------------------------------------------------------------------------------

void IndexSpec_FreeInternals(IndexSpec *spec) {
  //RS_LOG_ASSERT(!spec->isReindexing, "Cannot free index while it is being scanned");

  dictDelete(specDict, spec->name);
  SchemaRules_RemoveSpecRules(spec);
  SchemaPrefixes_RemoveSpec(spec);

  if (spec->isTimerSet) {
    RedisModule_StopTimer(RSDummyContext, spec->timerId, NULL);
    spec->isTimerSet = false;
  }

  if (spec->indexer) {
    Indexer_Free(spec->indexer);
  }
  if (spec->gc) {
    GCContext_Stop(spec->gc);
  }

  if (spec->terms) {
    TrieType_Free(spec->terms);
  }
  DocTable_Free(&spec->docs);

  if (spec->uniqueId) {
    // If uniqueid is 0, it means the index was not initialized
    // and is being freed now during an error.
    Cursors_PurgeWithName(&RSCursors, spec->name);
    CursorList_RemoveSpec(&RSCursors, spec->name);
  }

  if (spec->rule) {
    SchemaRule_Free(spec->rule);
    spec->rule = NULL;
  }

  rm_free(spec->name);
  if (spec->sortables) {
    SortingTable_Free(spec->sortables);
    spec->sortables = NULL;
  }
  if (spec->stopwords) {
    StopWordList_Unref(spec->stopwords);
    spec->stopwords = NULL;
  }

  if (spec->smap) {
    SynonymMap_Free(spec->smap);
  }
  if (spec->spcache) {
    IndexSpecCache_Decref(spec->spcache);
    spec->spcache = NULL;
  }

  if (spec->indexStrs) {
    for (size_t ii = 0; ii < spec->numFields; ++ii) {
      IndexSpecFmtStrings *fmts = spec->indexStrs + ii;
      for (size_t jj = 0; jj < INDEXFLD_NUM_TYPES; ++jj) {
        if (fmts->types[jj]) {
          RedisModule_FreeString(RSDummyContext, fmts->types[jj]);
        }
      }
    }
    rm_free(spec->indexStrs);
  }
  if (spec->fields != NULL) {
    for (size_t i = 0; i < spec->numFields; i++) {
      rm_free(spec->fields[i].name);
    }
    rm_free(spec->fields);
  }
  IndexSpec_ClearAliases(spec);

  if (spec->keysDict) {
    dictRelease(spec->keysDict);
  }

  rm_free(spec);
}

//---------------------------------------------------------------------------------------------

static void IndexSpec_FreeTask(IndexSpec *spec) {
  RedisModule_Log(NULL, "notice", "Freeing index %s in background", spec->name);

  while (spec->isReindexing) {
    sched_yield();
    sleep(0.1);
  }
  IndexSpec_FreeSync(spec);

  RedisModuleCtx *threadCtx = RedisModule_GetThreadSafeContext(NULL);
  RedisSearchCtx sctx = SEARCH_CTX_STATIC(threadCtx, spec);
  RedisModule_AutoMemory(threadCtx);
  RedisModule_ThreadSafeContextLock(threadCtx);

  Redis_DropIndex(&sctx, true);

  RedisModule_ThreadSafeContextUnlock(threadCtx);
  RedisModule_FreeThreadSafeContext(threadCtx);
}

static struct thpool_ *cleanPool = NULL;

void IndexSpec_Free(IndexSpec *spec) {
  if (!!(spec->flags & Index_Temporary) || spec->isReindexing) {
    if (!cleanPool) {
      cleanPool = thpool_init(1);
    }
    thpool_add_work(cleanPool, (thpool_proc) IndexSpec_FreeTask, spec);
    return;
  }

  IndexSpec_FreeSync(spec);
}

//---------------------------------------------------------------------------------------------

void IndexSpec_FreeSync(IndexSpec *spec) {
  //  todo:
  //  mark I think we only need IndexSpec_FreeInternals, this is called only from the
  //  LLAPI and there is no need to drop keys cause its out of the key space.
  //  Let me know what you think

  //   Need a context for this:
  RedisModuleCtx *ctx = RedisModule_GetThreadSafeContext(NULL);
  RedisSearchCtx sctx = SEARCH_CTX_STATIC(ctx, spec);
  RedisModule_AutoMemory(ctx);

  Redis_DropIndex(&sctx, spec->cascadeDelete);
  RedisModule_FreeThreadSafeContext(ctx);
}

//---------------------------------------------------------------------------------------------

void Indexes_Free() {
  arrayof(IndexSpec *) specs = array_new(IndexSpec *, 10);
  dictIterator *iter = dictGetIterator(specDict);
  dictEntry *entry = NULL;
  while ((entry = dictNext(iter))) {
    IndexSpec *sp = dictGetVal(entry);
    specs = array_append(specs, sp);
  }
  dictReleaseIterator(iter);

  for (size_t i = 0; i < array_len(specs); ++i) {
    IndexSpec_FreeInternals(specs[i]);
  }
  array_free(specs);
}

///////////////////////////////////////////////////////////////////////////////////////////////

IndexSpec *IndexSpec_LoadEx(RedisModuleCtx *ctx, IndexLoadOptions *options) {
  const char *ixname = NULL;
  if (options->flags & INDEXSPEC_LOAD_KEY_RSTRING) {
    ixname = RedisModule_StringPtrLen(options->name.rstring, NULL);
  } else {
    ixname = options->name.cstring;
  }

  IndexSpec *sp = dictFetchValue(specDict, ixname);
  if (!sp) {
    if (!(options->flags & INDEXSPEC_LOAD_NOALIAS)) {
      sp = IndexAlias_Get(ixname);
    }
    if (!sp) {
      return NULL;
    }
  }

  if ((sp->flags & Index_Temporary) && !(options->flags & INDEXSPEC_LOAD_NOTIMERUPDATE)) {
    if (sp->isTimerSet) {
      RedisModule_StopTimer(RSDummyContext, sp->timerId, NULL);
    }
    IndexSpec_SetTimeoutTimer(sp);
  }

  return sp;
}

/* Load the spec from the saved version */
IndexSpec *IndexSpec_Load(RedisModuleCtx *ctx, const char *name, int openWrite) {
  IndexLoadOptions lopts = {.flags = openWrite ? INDEXSPEC_LOAD_WRITEABLE : 0,
                            .name = {.cstring = name}};
  lopts.flags |= INDEXSPEC_LOAD_KEYLESS;
  return IndexSpec_LoadEx(ctx, &lopts);
}

RedisModuleString *IndexSpec_GetFormattedKey(IndexSpec *sp, const FieldSpec *fs,
                                             FieldType forType) {
  if (!sp->indexStrs) {
    sp->indexStrs = rm_calloc(SPEC_MAX_FIELDS, sizeof(*sp->indexStrs));
  }

  size_t typeix = INDEXTYPE_TO_POS(forType);

  RedisModuleString *ret = sp->indexStrs[fs->index].types[typeix];
  if (!ret) {
    RedisSearchCtx sctx = {.redisCtx = RSDummyContext, .spec = sp};
    switch (forType) {
      case INDEXFLD_T_NUMERIC:
      case INDEXFLD_T_GEO:  // TODO?? change the name
        ret = fmtRedisNumericIndexKey(&sctx, fs->name);
        break;
      case INDEXFLD_T_TAG:
        ret = TagIndex_FormatName(&sctx, fs->name);
        break;
      case INDEXFLD_T_FULLTEXT:  // Text fields don't get a per-field index
      default:
        ret = NULL;
        abort();
        break;
    }
    RS_LOG_ASSERT(ret, "Failed to create index string");
    sp->indexStrs[fs->index].types[typeix] = ret;
  }
  return ret;
}

RedisModuleString *IndexSpec_GetFormattedKeyByName(IndexSpec *sp, const char *s,
                                                   FieldType forType) {
  const FieldSpec *fs = IndexSpec_GetField(sp, s, strlen(s));
  if (!fs) {
    return NULL;
  }
  return IndexSpec_GetFormattedKey(sp, fs, forType);
}

t_fieldMask IndexSpec_ParseFieldMask(IndexSpec *sp, RedisModuleString **argv, int argc) {
  t_fieldMask ret = 0;

  for (int i = 0; i < argc; i++) {
    size_t len;
    const char *p = RedisModule_StringPtrLen(argv[i], &len);

    ret |= IndexSpec_GetFieldBit(sp, p, len);
  }

  return ret;
}

void IndexSpec_InitializeSynonym(IndexSpec *sp) {
  if (!sp->smap) {
    sp->smap = SynonymMap_New(false);
    sp->flags |= Index_HasSmap;
  }
}

int IndexSpec_ParseStopWords(IndexSpec *sp, RedisModuleString **strs, size_t len) {
  // if the index already has custom stopwords, let us free them first
  if (sp->stopwords) {
    StopWordList_Unref(sp->stopwords);
    sp->stopwords = NULL;
  }

  sp->stopwords = NewStopWordList(strs, len);
  // on failure we revert to the default stopwords list
  if (sp->stopwords == NULL) {
    sp->stopwords = DefaultStopWordList();
    sp->flags &= ~Index_HasCustomStopwords;
    return 0;
  } else {
    sp->flags |= Index_HasCustomStopwords;
  }
  return 1;
}

int IndexSpec_IsStopWord(IndexSpec *sp, const char *term, size_t len) {
  if (!sp->stopwords) {
    return 0;
  }
  return StopWordList_Contains(sp->stopwords, term, len);
}

///////////////////////////////////////////////////////////////////////////////////////////////

IndexSpec *NewIndexSpec(const char *name) {
  IndexSpec *sp = rm_calloc(1, sizeof(IndexSpec));
  sp->fields = rm_calloc(sizeof(FieldSpec), SPEC_MAX_FIELDS);
  sp->sortables = NewSortingTable();
  sp->flags = INDEX_DEFAULT_FLAGS;
  sp->name = rm_strdup(name);
  sp->docs = DocTable_New(100);
  sp->stopwords = DefaultStopWordList();
  sp->terms = NewTrie();
  sp->keysDict = NULL;
  sp->minPrefix = RSGlobalConfig.minTermPrefix;
  sp->maxPrefixExpansions = RSGlobalConfig.maxPrefixExpansions;
  sp->getValue = NULL;
  sp->getValueCtx = NULL;

  sp->timeout = 0;
  sp->isTimerSet = false;
  sp->timerId = 0;

  sp->isReindexing = false;
  sp->keysIndexed = 0;
  sp->keysTotal = 0;

  //sp->isDropped = false;
  sp->cascadeDelete = true; //@@ TODO: true for temp indexed

  memset(&sp->stats, 0, sizeof(sp->stats));
  return sp;
}

FieldSpec *IndexSpec_CreateField(IndexSpec *sp, const char *name) {
  sp->fields = rm_realloc(sp->fields, sizeof(*sp->fields) * (sp->numFields + 1));
  FieldSpec *fs = sp->fields + sp->numFields;
  memset(fs, 0, sizeof(*fs));
  fs->index = sp->numFields++;
  fs->name = rm_strdup(name);
  fs->ftId = (t_fieldId)-1;
  fs->ftWeight = 1.0;
  fs->sortIdx = -1;
  fs->tagFlags = TAG_FIELD_DEFAULT_FLAGS;
  fs->tagFlags = TAG_FIELD_DEFAULT_SEP;
  return fs;
}

static dictType invidxDictType = {0};

static void valFreeCb(void *unused, void *p) {
  KeysDictValue *kdv = p;
  if (kdv->dtor) {
    kdv->dtor(kdv->p);
  }
  rm_free(kdv);
}

void IndexSpec_MakeKeyless(IndexSpec *sp) {
  // Initialize only once:
  if (!invidxDictType.valDestructor) {
    invidxDictType = dictTypeHeapRedisStrings;
    invidxDictType.valDestructor = valFreeCb;
  }
  sp->keysDict = dictCreate(&invidxDictType, NULL);
}

void IndexSpec_StartGCFromSpec(IndexSpec *sp, float initialHZ, uint32_t gcPolicy) {
  sp->gc = GCContext_CreateGCFromSpec(sp, initialHZ, sp->uniqueId, gcPolicy);
  GCContext_Start(sp->gc);
}

/* Start the garbage collection loop on the index spec. The GC removes garbage data left on the
 * index after removing documents */
void IndexSpec_StartGC(RedisModuleCtx *ctx, IndexSpec *sp, float initialHZ) {
  RS_LOG_ASSERT(!sp->gc, "GC already exists");
  // we will not create a gc thread on temporary index
  if (RSGlobalConfig.enableGC && !(sp->flags & Index_Temporary)) {
    RedisModuleString *keyName = RedisModule_CreateString(ctx, sp->name, strlen(sp->name));
    sp->gc = GCContext_CreateGC(keyName, initialHZ, sp->uniqueId);
    GCContext_Start(sp->gc);
    RedisModule_Log(ctx, "verbose", "Starting GC for index %s", sp->name);
  }
}

// given a field mask with one bit lit, it returns its offset
int bit(t_fieldMask id) {
  for (int i = 0; i < sizeof(t_fieldMask) * 8; i++) {
    if (((id >> i) & 1) == 1) {
      return i;
    }
  }
  return 0;
}

///////////////////////////////////////////////////////////////////////////////////////////////

// Backwards compat version of load for rdbs with version < 8
static void FieldSpec_RdbLoadCompat8(RedisModuleIO *rdb, FieldSpec *f, int encver) {

  f->name = RedisModule_LoadStringBuffer(rdb, NULL);
  char *tmpName = rm_strdup(f->name);
  RedisModule_Free(f->name);
  f->name = tmpName;
  // the old versions encoded the bit id of the field directly
  // we convert that to a power of 2
  if (encver < INDEX_MIN_WIDESCHEMA_VERSION) {
    f->ftId = bit(RedisModule_LoadUnsigned(rdb));
  } else {
    // the new version encodes just the power of 2 of the bit
    f->ftId = RedisModule_LoadUnsigned(rdb);
  }
  f->types = RedisModule_LoadUnsigned(rdb);
  f->ftWeight = RedisModule_LoadDouble(rdb);
  f->tagFlags = TAG_FIELD_DEFAULT_FLAGS;
  f->tagSep = TAG_FIELD_DEFAULT_SEP;
  if (encver >= 4) {
    f->options = RedisModule_LoadUnsigned(rdb);
    f->sortIdx = RedisModule_LoadSigned(rdb);
  }
}

static void FieldSpec_RdbSave(RedisModuleIO *rdb, FieldSpec *f) {
  RedisModule_SaveStringBuffer(rdb, f->name, strlen(f->name) + 1);
  RedisModule_SaveUnsigned(rdb, f->types);
  RedisModule_SaveUnsigned(rdb, f->options);
  RedisModule_SaveSigned(rdb, f->sortIdx);
  // Save text specific options
  if (FIELD_IS(f, INDEXFLD_T_FULLTEXT) || (f->options & FieldSpec_Dynamic)) {
    RedisModule_SaveUnsigned(rdb, f->ftId);
    RedisModule_SaveDouble(rdb, f->ftWeight);
  }
  if (FIELD_IS(f, INDEXFLD_T_TAG) || (f->options & FieldSpec_Dynamic)) {
    RedisModule_SaveUnsigned(rdb, f->tagFlags);
    RedisModule_SaveStringBuffer(rdb, &f->tagSep, 1);
  }
}

static const FieldType fieldTypeMap[] = {[IDXFLD_LEGACY_FULLTEXT] = INDEXFLD_T_FULLTEXT,
                                         [IDXFLD_LEGACY_NUMERIC] = INDEXFLD_T_NUMERIC,
                                         [IDXFLD_LEGACY_GEO] = INDEXFLD_T_GEO,
                                         [IDXFLD_LEGACY_TAG] = INDEXFLD_T_TAG};

static void FieldSpec_RdbLoad(RedisModuleIO *rdb, FieldSpec *f, int encver) {

  // Fall back to legacy encoding if needed
  if (encver < INDEX_MIN_TAGFIELD_VERSION) {
    return FieldSpec_RdbLoadCompat8(rdb, f, encver);
  }

  f->name = RedisModule_LoadStringBuffer(rdb, NULL);
  char *tmpName = rm_strdup(f->name);
  RedisModule_Free(f->name);
  f->name = tmpName;

  f->types = RedisModule_LoadUnsigned(rdb);
  f->options = RedisModule_LoadUnsigned(rdb);
  f->sortIdx = RedisModule_LoadSigned(rdb);

  if (encver < INDEX_MIN_MULTITYPE_VERSION) {
    RS_LOG_ASSERT(f->types <= IDXFLD_LEGACY_MAX, "field type should be string or numeric");
    f->types = fieldTypeMap[f->types];
  }

  // Load text specific options
  if (FIELD_IS(f, INDEXFLD_T_FULLTEXT) || (f->options & FieldSpec_Dynamic)) {
    f->ftId = RedisModule_LoadUnsigned(rdb);
    f->ftWeight = RedisModule_LoadDouble(rdb);
  }
  // Load tag specific options
  if (FIELD_IS(f, INDEXFLD_T_TAG) || (f->options & FieldSpec_Dynamic)) {
    f->tagFlags = RedisModule_LoadUnsigned(rdb);
    // Load the separator
    size_t l;
    char *s = RedisModule_LoadStringBuffer(rdb, &l);
    RS_LOG_ASSERT(l == 1, "buffer length should be 1");
    f->tagSep = *s;
    RedisModule_Free(s);
  }
}

#if 0

static void IndexStats_RdbLoad(RedisModuleIO *rdb, IndexStats *stats) {
  stats->numDocuments = RedisModule_LoadUnsigned(rdb);
  stats->numTerms = RedisModule_LoadUnsigned(rdb);
  stats->numRecords = RedisModule_LoadUnsigned(rdb);
  stats->invertedSize = RedisModule_LoadUnsigned(rdb);
  stats->invertedCap = RedisModule_LoadUnsigned(rdb);
  stats->skipIndexesSize = RedisModule_LoadUnsigned(rdb);
  stats->scoreIndexesSize = RedisModule_LoadUnsigned(rdb);
  stats->offsetVecsSize = RedisModule_LoadUnsigned(rdb);
  stats->offsetVecRecords = RedisModule_LoadUnsigned(rdb);
  stats->termsSize = RedisModule_LoadUnsigned(rdb);
}

static void IndexStats_RdbSave(RedisModuleIO *rdb, IndexStats *stats) {
  RedisModule_SaveUnsigned(rdb, stats->numDocuments);
  RedisModule_SaveUnsigned(rdb, stats->numTerms);
  RedisModule_SaveUnsigned(rdb, stats->numRecords);
  RedisModule_SaveUnsigned(rdb, stats->invertedSize);
  RedisModule_SaveUnsigned(rdb, stats->invertedCap);
  RedisModule_SaveUnsigned(rdb, stats->skipIndexesSize);
  RedisModule_SaveUnsigned(rdb, stats->scoreIndexesSize);
  RedisModule_SaveUnsigned(rdb, stats->offsetVecsSize);
  RedisModule_SaveUnsigned(rdb, stats->offsetVecRecords);
  RedisModule_SaveUnsigned(rdb, stats->termsSize);
}

#endif // 0

///////////////////////////////////////////////////////////////////////////////////////////////

static threadpool reindexPool = NULL;

typedef struct IndexesScanner {
  IndexSpec *spec_opt;
  size_t scannedKeys, totalKeys;
} IndexesScanner;

static IndexesScanner *IndexesScanner_New(IndexSpec *spec_opt) {
  IndexesScanner *scanner = rm_calloc(1, sizeof(IndexesScanner));
  scanner->spec_opt = spec_opt;
  scanner->scannedKeys = 0;
  RedisModuleCtx *ctx = RedisModule_GetThreadSafeContext(NULL);
  scanner->totalKeys = RedisModule_DbSize(ctx);
  RedisModule_FreeThreadSafeContext(ctx);
  if (spec_opt) {
    spec_opt->isReindexing = true;
    spec_opt->keysIndexed = 0;
    spec_opt->keysTotal = scanner->totalKeys;
  }
  return scanner;
}

//---------------------------------------------------------------------------------------------

static void IndexSpec_DoneIndexingCallabck(struct RSAddDocumentCtx *docCtx, RedisModuleCtx *ctx,
                                           void *pd) {
}

//---------------------------------------------------------------------------------------------

void IndexSpec_UpdateMatchingWithSchemaRules(IndexSpec *sp, RedisModuleCtx *ctx, RedisModuleString *key);

static void Indexes_ScanProc(RedisModuleCtx *ctx, RedisModuleString *keyname,
                                 RedisModuleKey *key, IndexesScanner *scanner) {
  if (!key) {
    // todo: on ROF the key might not be in the ram and we will not get it here, we will need to
    // hanlde it.
    return;
  }

  IndexSpec *sp = scanner->spec_opt;
  if (sp) {
    IndexSpec_UpdateMatchingWithSchemaRules(sp, ctx, keyname);
  } else {
    Indexes_UpdateMatchingWithSchemaRules(ctx, keyname);
  }
  ++scanner->scannedKeys;
}

//---------------------------------------------------------------------------------------------

#define REINDEX_BATCH_SIZE 100

static void Indexes_ScanAndReindexTask(IndexesScanner *scanner) {
  RS_LOG_ASSERT(scanner, "invalid IndexesScanner");

  IndexSpec *sp = scanner->spec_opt; // can be NULL

  RedisModuleCtx *ctx = RedisModule_GetThreadSafeContext(NULL);
  RedisModuleScanCursor *cursor = RedisModule_ScanCursorCreate();
  RedisModule_ThreadSafeContextLock(ctx);

  if (sp) {
    RedisModule_Log(ctx, "notice", "Scanning index %s in background", sp->name);
  } else {
    RedisModule_Log(ctx, "notice", "Scanning indexes in background");
  }

  if (!sp) {
    Indexes_SetTempSpecsTimers();
  }

  while (RedisModule_Scan(ctx, cursor, (RedisModuleScanCB) Indexes_ScanProc, scanner)) {
    if (scanner->scannedKeys % REINDEX_BATCH_SIZE >= 0) {
      continue;
    }

    if (sp) {
      sp->keysIndexed = scanner->scannedKeys;
    } else {
      dictIterator *iter = dictGetIterator(specDict);
      dictEntry *entry = NULL;
      while ((entry = dictNext(iter))) {
        IndexSpec *sp = dictGetVal(entry);
        sp->keysIndexed = scanner->scannedKeys;
      }
      dictReleaseIterator(iter);
    }

    RedisModule_ThreadSafeContextUnlock(ctx);
    sched_yield();
    RedisModule_ThreadSafeContextLock(ctx);

/*    if (sp && sp->isDropped) {
      RedisModule_Log(ctx, "notice", "Scanning index %s in background: aborted - index dropped", sp->name);
      sp->isReindexing = false;
      sp->keysTotal = sp->keysIndexed = 0;
      goto end;
    }*/
  }

  if (sp) {
    sp->keysTotal = sp->keysIndexed = scanner->totalKeys;
    sp->isReindexing = false;
  } else {
    dictIterator *iter = dictGetIterator(specDict);
    dictEntry *entry = NULL;
    while ((entry = dictNext(iter))) {
      IndexSpec *sp = dictGetVal(entry);
      sp->keysTotal = sp->keysIndexed = scanner->totalKeys;
      sp->isReindexing = false;
    }
    dictReleaseIterator(iter);
  }

  RedisModule_Log(ctx, "notice", "Scanning indexes in background: done (scanned=%ld)", scanner->totalKeys);

//end:
  RedisModule_ThreadSafeContextUnlock(ctx);
  RedisModule_ScanCursorDestroy(cursor);

  rm_free(scanner);
  RedisModule_FreeThreadSafeContext(ctx);
}

//---------------------------------------------------------------------------------------------

static void IndexSpec_ScanAndReindexAsync(IndexSpec *sp) {
  //if (sp->isDropped) {
  //  return;
  //}
  if (!reindexPool) {
    reindexPool = thpool_init(1);
  }
  RedisModule_Log(NULL, "notice", "Register index %s for async scan", sp->name);
  IndexesScanner *scanner = IndexesScanner_New(sp);
  thpool_add_work(reindexPool, (thpool_proc) Indexes_ScanAndReindexTask, scanner);
}

//---------------------------------------------------------------------------------------------

static void IndexSpec_ScanCallback(RedisModuleCtx *ctx, RedisModuleString *keyname,
                                   RedisModuleKey *key, void *_sp) {
  IndexSpec *sp = _sp;

  if (!key) {
    // todo: on ROF the key might not be in the ram and we will not get it here, we will need to
    // hanlde it.
    return;
  }

  IndexSpec_UpdateMatchingWithSchemaRules(sp, ctx, keyname);
}

static void IndexSpec_ScanAndReindexSync(IndexSpec *sp) {
  RedisModuleCtx *ctx = RedisModule_GetThreadSafeContext(NULL);

  sp->isReindexing = true;
  RedisModuleScanCursor *cursor = RedisModule_ScanCursorCreate();
  while (RedisModule_Scan(ctx, cursor, IndexSpec_ScanCallback, sp)) {
  }
  RedisModule_ScanCursorDestroy(cursor);
  sp->keysTotal = sp->keysIndexed = RedisModule_DbSize(ctx);;
  sp->isReindexing = false;

  RedisModule_FreeThreadSafeContext(ctx);
}

//---------------------------------------------------------------------------------------------

#if 0

void Indexes_ScanAndReindexSync() {
  IndexesScanner *scanner = IndexesScanner_New(NULL);
  RedisModuleCtx *ctx = RedisModule_GetThreadSafeContext(NULL);
  RedisModuleScanCursor *cursor = RedisModule_ScanCursorCreate();

  Indexes_SetTempSpecsTimers();

  while (RedisModule_Scan(ctx, cursor, (RedisModuleScanCB) Indexes_ScanProc, scanner)) {
    dictIterator *iter = dictGetIterator(specDict);
    dictEntry *entry = NULL;
    while ((entry = dictNext(iter))) {
      IndexSpec *sp = dictGetVal(entry);
      sp->keysIndexed = scanner->scannedKeys;
    }
    dictReleaseIterator(iter);
  }

  dictIterator *iter = dictGetIterator(specDict);
  dictEntry *entry = NULL;
  while ((entry = dictNext(iter))) {
    IndexSpec *sp = dictGetVal(entry);
    sp->keysTotal = sp->keysIndexed = scanner->totalKeys;
    sp->isReindexing = false;
  }
  dictReleaseIterator(iter);

  RedisModule_Log(ctx, "notice", "Scanning indexes in background: done (scanned=%ld)", scanner->totalKeys);

  RedisModule_ScanCursorDestroy(cursor);

  rm_free(scanner);
  RedisModule_FreeThreadSafeContext(ctx);
}

#endif // 0

//---------------------------------------------------------------------------------------------

static void IndexSpec_ScanAndReindex(IndexSpec *sp) {
  if (sp->flags & Index_Async) {
    IndexSpec_ScanAndReindexAsync(sp);
  }
  else {
    IndexSpec_ScanAndReindexSync(sp);
  }
}

void Indexes_ScanAndReindex() {
  if (!reindexPool) {
    reindexPool = thpool_init(1);
  }

  RedisModule_Log(NULL, "notice", "Scanning all indexes");
  IndexesScanner *scanner = IndexesScanner_New(NULL);
  thpool_add_work(reindexPool, (thpool_proc) Indexes_ScanAndReindexTask, scanner);
}

///////////////////////////////////////////////////////////////////////////////////////////////

IndexSpec *IndexSpec_CreateFromRdb(RedisModuleCtx *ctx, RedisModuleIO *rdb, int encver,
                               QueryError *status) {
  IndexSpec *sp = rm_calloc(1, sizeof(IndexSpec));
  IndexSpec_MakeKeyless(sp);

  sp->sortables = NewSortingTable();
  sp->terms = NULL;
  sp->docs = DocTable_New(1000);
  sp->name = RedisModule_LoadStringBuffer(rdb, NULL);
  char *tmpName = rm_strdup(sp->name);
  RedisModule_Free(sp->name);
  sp->name = tmpName;
  sp->flags = (IndexFlags)RedisModule_LoadUnsigned(rdb);
  sp->maxPrefixExpansions = RSGlobalConfig.maxPrefixExpansions;
  sp->minPrefix = RSGlobalConfig.minTermPrefix;
  if (encver < INDEX_MIN_NOFREQ_VERSION) {
    sp->flags |= Index_StoreFreqs;
  }

  sp->numFields = RedisModule_LoadUnsigned(rdb);
  sp->fields = rm_calloc(sp->numFields, sizeof(FieldSpec));
  int maxSortIdx = -1;
  for (int i = 0; i < sp->numFields; i++) {
    FieldSpec *fs = sp->fields + i;
    FieldSpec_RdbLoad(rdb, sp->fields + i, encver);
    sp->fields[i].index = i;
    if (FieldSpec_IsSortable(fs)) {
      RS_LOG_ASSERT(fs->sortIdx < RS_SORTABLES_MAX, "sorting index is too large");
      sp->sortables->fields[fs->sortIdx].name = fs->name;
      sp->sortables->fields[fs->sortIdx].type = fieldTypeToValueType(fs->types);
      sp->sortables->len = MAX(sp->sortables->len, fs->sortIdx + 1);
    }
  }

  //    IndexStats_RdbLoad(rdb, &sp->stats);

  if (SchemaRule_RdbLoad(sp, rdb, encver) != REDISMODULE_OK) {
    QueryError_SetErrorFmt(status, QUERY_EPARSEARGS, "Failed to load schema rule");
    IndexSpec_Free(sp);
    return NULL;
  }

  //    DocTable_RdbLoad(&sp->docs, rdb, encver);
  sp->terms = NewTrie();
  /* For version 3 or up - load the generic trie */
  //  if (encver >= 3) {
  //    sp->terms = TrieType_GenericLoad(rdb, 0);
  //  } else {
  //    sp->terms = NewTrie();
  //  }

  if (sp->flags & Index_HasCustomStopwords) {
    sp->stopwords = StopWordList_RdbLoad(rdb, encver);
  } else {
    sp->stopwords = DefaultStopWordList();
  }

  sp->uniqueId = spec_unique_ids++;

  IndexSpec_StartGC(ctx, sp, GC_DEFAULT_HZ);
  RedisModuleString *specKey = RedisModule_CreateStringPrintf(ctx, INDEX_SPEC_KEY_FMT, sp->name);
  CursorList_AddSpec(&RSCursors, sp->name, RSCURSORS_DEFAULT_CAPACITY);
  RedisModule_FreeString(ctx, specKey);

  sp->smap = NULL;
  if (sp->flags & Index_HasSmap) {
    sp->smap = SynonymMap_RdbLoad(rdb, encver);
  }
  if (IndexSpec_OnCreate) {
    IndexSpec_OnCreate(sp);
  }

  sp->timeout = RedisModule_LoadUnsigned(rdb);

  size_t narr = RedisModule_LoadUnsigned(rdb);
  for (size_t ii = 0; ii < narr; ++ii) {
    QueryError status;
    size_t dummy;
    char *s = RedisModule_LoadStringBuffer(rdb, &dummy);
    int rc = IndexAlias_Add(s, sp, 0, &status);
    RedisModule_Free(s);
    RS_LOG_ASSERT(rc == REDISMODULE_OK, "adding alias to index failed");
  }

  sp->indexer = NewIndexer(sp);
  dictAdd(specDict, sp->name, sp);

  sp->isReindexing = true;
  sp->keysIndexed = 0;
  sp->keysTotal = 0;

  //sp->isDropped = false;
  sp->cascadeDelete = true;

  return sp;
}

int Indexes_RdbLoad(RedisModuleIO *rdb, int encver, int when) {
  if (when == REDISMODULE_AUX_BEFORE_RDB) {
    return REDISMODULE_OK;
  }

  if (encver < INDEX_MIN_COMPAT_VERSION) {
    return REDISMODULE_OK;
  }

  size_t nIndexes = RedisModule_LoadUnsigned(rdb);
  RedisModuleCtx *ctx = RedisModule_GetContextFromIO(rdb);
  QueryError status = {0};
  for (size_t i = 0; i < nIndexes; ++i) {
    IndexSpec *sp = IndexSpec_CreateFromRdb(ctx, rdb, encver, &status);
    if (!sp) {
      RedisModule_Log(ctx, "error", "RDB Load: %s", status.detail ? status.detail : "general failure");
      return REDISMODULE_ERR;
    }
  }
  return REDISMODULE_OK;
}

void Indexes_RdbSave(RedisModuleIO *rdb, int when) {
  if (when == REDISMODULE_AUX_BEFORE_RDB) {
    return;
  }

  RedisModule_SaveUnsigned(rdb, dictSize(specDict));

  dictIterator *iter = dictGetIterator(specDict);
  dictEntry *entry = NULL;
  while ((entry = dictNext(iter))) {
    IndexSpec *sp = dictGetVal(entry);
    // we save the name plus the null terminator
    RedisModule_SaveStringBuffer(rdb, sp->name, strlen(sp->name) + 1);
    RedisModule_SaveUnsigned(rdb, (uint)sp->flags);

    RedisModule_SaveUnsigned(rdb, sp->numFields);
    for (int i = 0; i < sp->numFields; i++) {
      FieldSpec_RdbSave(rdb, &sp->fields[i]);
    }

    SchemaRule_RdbSave(sp->rule, rdb);

    //    IndexStats_RdbSave(rdb, &sp->stats);
    //    DocTable_RdbSave(&sp->docs, rdb);
    //    // save trie of terms
    //    TrieType_GenericSave(rdb, sp->terms, 0);

    // If we have custom stopwords, save them
    if (sp->flags & Index_HasCustomStopwords) {
      StopWordList_RdbSave(rdb, sp->stopwords);
    }

    if (sp->flags & Index_HasSmap) {
      SynonymMap_RdbSave(rdb, sp->smap);
    }

    RedisModule_SaveUnsigned(rdb, sp->timeout);

    if (sp->aliases) {
      RedisModule_SaveUnsigned(rdb, array_len(sp->aliases));
      for (size_t ii = 0; ii < array_len(sp->aliases); ++ii) {
        RedisModule_SaveStringBuffer(rdb, sp->aliases[ii], strlen(sp->aliases[ii]) + 1);
      }
    } else {
      RedisModule_SaveUnsigned(rdb, 0);
    }
  }

  dictReleaseIterator(iter);
}

void IndexSpec_Digest(RedisModuleDigest *digest, void *value) {
}

static void Indexes_LoadingEvent(RedisModuleCtx *ctx, RedisModuleEvent eid, uint64_t subevent,
                                   void *data) {
  if (subevent == REDISMODULE_SUBEVENT_LOADING_RDB_START ||
      subevent == REDISMODULE_SUBEVENT_LOADING_AOF_START ||
      subevent == REDISMODULE_SUBEVENT_LOADING_REPL_START) {
    Indexes_Free();
  } else if (subevent == REDISMODULE_SUBEVENT_LOADING_ENDED) {
    Indexes_ScanAndReindex();
  }
}

int IndexSpec_RegisterType(RedisModuleCtx *ctx) {
  RedisModuleTypeMethods tm = {
      .version = REDISMODULE_TYPE_METHOD_VERSION,
      .aux_load = Indexes_RdbLoad,
      .aux_save = Indexes_RdbSave,
      .aof_rewrite = GenericAofRewrite_DisabledHandler,
      .aux_save_triggers = REDISMODULE_AUX_BEFORE_RDB | REDISMODULE_AUX_AFTER_RDB,
  };

  IndexSpecType = RedisModule_CreateDataType(ctx, "ft_index0", INDEX_CURRENT_VERSION, &tm);
  if (IndexSpecType == NULL) {
    RedisModule_Log(ctx, "error", "Could not create index spec type");
    return REDISMODULE_ERR;
  }

  RedisModule_SubscribeToServerEvent(ctx, RedisModuleEvent_Loading, Indexes_LoadingEvent);

  return REDISMODULE_OK;
}

int IndexSpec_DeleteHash(IndexSpec *spec, RedisModuleCtx *ctx, RedisModuleString *key);

int IndexSpec_UpdateWithHash(IndexSpec *spec, RedisModuleCtx *ctx, RedisModuleString *key) {
  if (!spec->rule) {
    RedisModule_Log(ctx, "warning", "Index spec %s: no rule found", spec->name);
    return REDISMODULE_ERR;
  }
  RedisSearchCtx sctx = SEARCH_CTX_STATIC(ctx, spec);
  Document doc = {0};
  Document_Init(&doc, key, 1.0, DEFAULT_LANGUAGE);
  if (Document_LoadSchemaFields(&doc, &sctx) != REDISMODULE_OK) {
    Document_Free(&doc);
    return RedisModule_ReplyWithError(ctx, "Could not load document");
  }
  QueryError status = {0};
  RSAddDocumentCtx *aCtx = NewAddDocumentCtx(spec, &doc, &status);
  aCtx->stateFlags |= ACTX_F_NOBLOCK;
  AddDocumentCtx_Submit(aCtx, &sctx, DOCUMENT_ADD_REPLACE);

  // doc was set DEAD in Document_Moved and was not freed since it set as NOFREEDOC
  doc.flags &= ~DOCUMENT_F_DEAD;
  Document_Free(&doc);
  return REDISMODULE_OK;
}

int IndexSpec_DeleteHash(IndexSpec *spec, RedisModuleCtx *ctx, RedisModuleString *key) {
  RedisSearchCtx sctx = SEARCH_CTX_STATIC(ctx, spec);

  // Get the doc ID
  t_docId id = DocTable_GetIdR(&spec->docs, key);
  if (id == 0) {
    return RedisModule_ReplyWithLongLong(ctx, 0);
    // ID does not exist.
  }

  int rc = DocTable_DeleteR(&spec->docs, key);
  if (rc) {
    spec->stats.numDocuments--;

    // Increment the index's garbage collector's scanning frequency after document deletions
    if (spec->gc) {
      GCContext_OnDelete(spec->gc);
    }
    RedisModule_Replicate(ctx, RS_DEL_CMD, "cs", spec->name, key);
  }
  return REDISMODULE_OK;
}

///////////////////////////////////////////////////////////////////////////////////////////////

void IndexSpec_CleanAll(void) {
  dictIterator *it = dictGetSafeIterator(specDict);
  dictEntry *e = NULL;
  while ((e = dictNext(it))) {
    IndexSpec *sp = e->v.val;
    IndexSpec_Free(sp);
  }
  dictReleaseIterator(it);
}

static void onFlush(RedisModuleCtx *ctx, RedisModuleEvent eid, uint64_t subevent, void *data) {
  if (subevent != REDISMODULE_SUBEVENT_FLUSHDB_START) {
    return;
  }
  IndexSpec_CleanAll();
  Dictionary_Clear();
}

void Indexes_Init(RedisModuleCtx *ctx) {
  specDict = dictCreate(&dictTypeHeapStrings, NULL);
  RedisModule_SubscribeToServerEvent(ctx, RedisModuleEvent_FlushDB, onFlush);
  SchemaPrefixes_Create();
  SchemaRules_Create();
}

dict *Indexes_FindMatchingSchemaRules(RedisModuleCtx *ctx, RedisModuleString *key) {
  EvalCtx *r = EvalCtx_Create();
  // check r for null?
  EvalCtx_AddHash(r, ctx, key);
  RSValue *keyRSV = RS_RedisStringVal(key);
  EvalCtx_Set(r, "__key", keyRSV);

#ifdef DEBUG
  RLookupKey *k = RLookup_GetKey(&r->lk, "__key", 0);
  RSValue *v = RLookup_GetItem(k, &r->row);
  const char *x = RSValue_StringPtrLen(v, NULL);
  k = RLookup_GetKey(&r->lk, "name", 0);
  v = RLookup_GetItem(k, &r->row);
  x = RSValue_StringPtrLen(v, NULL);
#endif  // DEBUG

  dict *specs = dictCreate(&dictTypeHeapStrings, NULL);

  size_t n;
  const char *key_p = RedisModule_StringPtrLen(key, &n);
  arrayof(SchemaPrefixNode *) prefixes = array_new(SchemaPrefixNode *, 1);
  int nprefixes = TrieMap_FindPrefixes(ScemaPrefixes_g, key_p, n, (arrayof(void *) *)&prefixes);
  for (int i = 0; i < array_len(prefixes); ++i) {
    SchemaPrefixNode *node = prefixes[i];
    for (int j = 0; j < array_len(node->index_specs); ++j) {
      IndexSpec *spec = node->index_specs[j];
      if (!dictFind(specs, node->prefix) /*&& !spec->isDropped*/) {
        dictAdd(specs, spec->name, spec);
      }
    }
  }
  array_free(prefixes);

  for (size_t i = 0; i < array_len(SchemaRules_g); i++) {
    SchemaRule *rule = SchemaRules_g[i];
    if (!rule->filter_exp /*|| rule->spec->isDropped*/) {
      continue;
    }
    if (EvalCtx_EvalExpr(r, rule->filter_exp) == EXPR_EVAL_OK) {
      IndexSpec *spec = rule->spec;
      if (RSValue_BoolTest(&r->res) && !dictFind(specs, spec->name)) {
        dictAdd(specs, spec->name, spec);
      }
    }
  }

  EvalCtx_Destroy(r);

  return specs;
}

void Indexes_UpdateMatchingWithSchemaRules(RedisModuleCtx *ctx, RedisModuleString *key) {
  dict *specs = Indexes_FindMatchingSchemaRules(ctx, key);

  dictIterator *di = dictGetIterator(specs);
  dictEntry *ent = dictNext(di);
  while (ent) {
    IndexSpec *spec = (IndexSpec *)ent->v.val;
    IndexSpec_UpdateWithHash(spec, ctx, key);
    ent = dictNext(di);
  }
  dictReleaseIterator(di);

  dictRelease(specs);
}

void IndexSpec_UpdateMatchingWithSchemaRules(IndexSpec *sp, RedisModuleCtx *ctx, RedisModuleString *key) {
  //if (sp->isDropped) {
  //  return;
  //}
  dict *specs = Indexes_FindMatchingSchemaRules(ctx, key);
  if (! dictFind(specs, sp->name)) {
    return;
  }

  dictIterator *di = dictGetIterator(specs);
  dictEntry *ent = dictNext(di);
  while (ent) {
    IndexSpec *spec = (IndexSpec *)ent->v.val;
    if (spec == sp) {
      IndexSpec_UpdateWithHash(spec, ctx, key);
    }
    ent = dictNext(di);
  }
  dictReleaseIterator(di);

  dictRelease(specs);
}

void Indexes_DeleteMatchingWithSchemaRules(RedisModuleCtx *ctx, RedisModuleString *key) {
  dict *specs = Indexes_FindMatchingSchemaRules(ctx, key);

  dictIterator *di = dictGetIterator(specs);
  dictEntry *ent = dictNext(di);
  while (ent) {
    IndexSpec *spec = (IndexSpec *)ent->v.val;
    IndexSpec_DeleteHash(spec, ctx, key);
    ent = dictNext(di);
  }
  dictReleaseIterator(di);

  dictRelease(specs);
}

///////////////////////////////////////////////////////////////////////////////////////////////
