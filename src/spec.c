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
#include "dictionary.h"
#include "doc_types.h"
#include "rdb.h"
#include "commands.h"

#define INITIAL_DOC_TABLE_SIZE 1000

///////////////////////////////////////////////////////////////////////////////////////////////

static int FieldSpec_RdbLoad(RedisModuleIO *rdb, FieldSpec *f, int encver);
void IndexSpec_UpdateMatchingWithSchemaRules(IndexSpec *sp, RedisModuleCtx *ctx,
                                             RedisModuleString *key, DocumentType type);
int IndexSpec_DeleteDoc(IndexSpec *spec, RedisModuleCtx *ctx, RedisModuleString *key);

void (*IndexSpec_OnCreate)(const IndexSpec *) = NULL;
const char *(*IndexAlias_GetUserTableName)(RedisModuleCtx *, const char *) = NULL;

RedisModuleType *IndexSpecType;
static uint64_t spec_unique_ids = 1;

dict *specDict_g;
IndexesScanner *global_spec_scanner = NULL;
size_t pending_global_indexing_ops = 0;
dict *legacySpecDict;
dict *legacySpecRules;

Version redisVersion;
Version rlecVersion;
bool isCrdt;
bool isTrimming = false;

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

const char *IndexSpec_GetFieldNameByBit(const IndexSpec *sp, t_fieldMask id) {
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
  // we need to delete the spec from the specDict_g, as far as the user see it,
  // this spec was deleted and its memory will be freed in a background thread.
#ifdef _DEBUG
  RedisModule_Log(NULL, "notice", "Freeing index %s by timer", sp->name);
#endif

  sp->isTimerSet = false;
  IndexSpec_Free(sp);

#ifdef _DEBUG
  RedisModule_Log(NULL, "notice", "Freeing index by timer: done");
#endif
}

static void IndexSpec_SetTimeoutTimer(IndexSpec *sp) {
  if (sp->isTimerSet) {
    RedisModule_StopTimer(RSDummyContext, sp->timerId, NULL);
  }
  sp->timerId = RedisModule_CreateTimer(RSDummyContext, sp->timeout,
                                        (RedisModuleTimerProc)IndexSpec_TimedOutProc, sp);
  sp->isTimerSet = true;
}

static void IndexSpec_ResetTimeoutTimer(IndexSpec *sp) {
  if (sp->isTimerSet) {
    RedisModule_StopTimer(RSDummyContext, sp->timerId, NULL);
  }
  sp->timerId = 0;
  sp->isTimerSet = false;
}

void Indexes_SetTempSpecsTimers(TimerOp op) {
  dictIterator *iter = dictGetIterator(specDict_g);
  dictEntry *entry = NULL;
  while ((entry = dictNext(iter))) {
    IndexSpec *sp = dictGetVal(entry);
    if (sp->flags & Index_Temporary) {
      switch (op) {
        case TimerOp_Add: IndexSpec_SetTimeoutTimer(sp);    break;
        case TimerOp_Del: IndexSpec_ResetTimeoutTimer(sp);  break;
      }
    }
  }
  dictReleaseIterator(iter);
}

//---------------------------------------------------------------------------------------------

IndexSpec *IndexSpec_CreateNew(RedisModuleCtx *ctx, RedisModuleString **argv, int argc,
                               QueryError *status) {
  const char *specName = RedisModule_StringPtrLen(argv[1], NULL);
  if (dictFetchValue(specDict_g, specName)) {
    QueryError_SetCode(status, QUERY_EINDEXEXISTS);
    return NULL;
  }
  IndexSpec *sp = IndexSpec_ParseRedisArgs(ctx, argv[1], &argv[2], argc - 2, status);
  if (sp == NULL) {
    return NULL;
  }

  dictAdd(specDict_g, (char *)specName, sp);

  sp->uniqueId = spec_unique_ids++;
  // Start the garbage collector
  IndexSpec_StartGC(ctx, sp, GC_DEFAULT_HZ);

  CursorList_AddSpec(&RSCursors, sp->name, RSCURSORS_DEFAULT_CAPACITY);

  // Create the indexer
  sp->indexer = NewIndexer(sp);
  if (IndexSpec_OnCreate) {
    IndexSpec_OnCreate(sp);
  }

  // set timeout for temporary index on master
  if ((sp->flags & Index_Temporary) && IsMaster()) {
    IndexSpec_SetTimeoutTimer(sp);
  }

  if (!(sp->flags & Index_SkipInitialScan)) {
    IndexSpec_ScanAndReindex(ctx, sp);
  }
  return sp;
}

//---------------------------------------------------------------------------------------------

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

/* Parse a field definition from argv, at *offset. We advance offset as we progress.
 *  Returns 1 on successful parse, 0 otherwise */
static int parseFieldSpec(ArgsCursor *ac, FieldSpec *fs, QueryError *status) {
  if (AC_IsAtEnd(ac)) {
    QueryError_SetErrorFmt(status, QUERY_EPARSEARGS, "Field `%s` does not have a type", fs->name);
    return 0;
  }

  if (AC_AdvanceIfMatch(ac, SPEC_TEXT_STR)) {
    fs->types |= INDEXFLD_T_FULLTEXT;
    if (!parseTextField(fs, ac, status)) {
      goto error;
    }
  } else if (AC_AdvanceIfMatch(ac, NUMERIC_STR)) {
    fs->types |= INDEXFLD_T_NUMERIC;
  } else if (AC_AdvanceIfMatch(ac, GEO_STR)) {  // geo field
    fs->types |= INDEXFLD_T_GEO;
  } else if (AC_AdvanceIfMatch(ac, SPEC_TAG_STR)) {  // tag field
    fs->types |= INDEXFLD_T_TAG;
    while (!AC_IsAtEnd(ac)) {
      if (AC_AdvanceIfMatch(ac, SPEC_TAG_SEPARATOR_STR)) {
        if (AC_IsAtEnd(ac)) {
          QueryError_SetError(status, QUERY_EPARSEARGS, SPEC_TAG_SEPARATOR_STR " requires an argument");
          goto error;
        }
        const char *sep = AC_GetStringNC(ac, NULL);
        if (strlen(sep) != 1) {
          QueryError_SetErrorFmt(status, QUERY_EPARSEARGS,
                                "Tag separator must be a single character. Got `%s`", sep);
          goto error;
        }
        fs->tagSep = *sep;
      } else if (AC_AdvanceIfMatch(ac, SPEC_TAG_CASE_SENSITIVE_STR)) {
        fs->tagFlags |= TagField_CaseSensitive;
      } else {
        break;
      }
   }
  } else {  // not numeric and not text - nothing more supported currently
    QueryError_SetErrorFmt(status, QUERY_EPARSEARGS, "Invalid field type for field `%s`", fs->name);
    goto error;
  }

  while (!AC_IsAtEnd(ac)) {
    if (AC_AdvanceIfMatch(ac, SPEC_SORTABLE_STR)) {
      FieldSpec_SetSortable(fs);
      if (AC_AdvanceIfMatch(ac, SPEC_UNF_STR)) {
        fs->options |= FieldSpec_UNF;
      }
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
  if (ac->offset == ac->argc) {
    QueryError_SetErrorFmt(status, QUERY_EPARSEARGS, "Fields arguments are missing");
    return 0;
  }

  if (sp->spcache) {
    IndexSpecCache_Decref(sp->spcache);
    sp->spcache = NULL;
  }
  const size_t prevNumFields = sp->numFields;
  const size_t prevSortLen = sp->sortables->len;
  FieldSpec *fs = NULL;

  while (!AC_IsAtEnd(ac)) {
    if (sp->numFields == SPEC_MAX_FIELDS) {
      QueryError_SetErrorFmt(status, QUERY_ELIMIT, "Schema is limited to %d fields",
                             SPEC_MAX_FIELDS);
      goto reset;
    }

    // Parse path and name of field
    size_t pathlen, namelen;
    const char *fieldPath = AC_GetStringNC(ac, &pathlen);
    const char *fieldName = fieldPath;
    if (AC_AdvanceIfMatch(ac, SPEC_AS_STR)) {
      if (AC_IsAtEnd(ac)) {
        QueryError_SetError(status, QUERY_EPARSEARGS, SPEC_AS_STR " requires an argument");
        goto reset;
      }
      fieldName = AC_GetStringNC(ac, &namelen);
      sp->flags |= Index_HasFieldAlias;
    } else {
      // if `AS` is not used, set the path as name
      fieldName = fieldPath;
      namelen= pathlen;
      fieldPath = NULL;
    }

    if (IndexSpec_GetField(sp, fieldName, namelen)) {
      QueryError_SetErrorFmt(status, QUERY_EINVAL, "Duplicate field in schema - %s", fieldName);
      goto reset;
    }

    fs = IndexSpec_CreateField(sp, fieldName, fieldPath);
    if (!parseFieldSpec(ac, fs, status)) {
      goto reset;
    }

    if (FIELD_IS(fs, INDEXFLD_T_FULLTEXT) && FieldSpec_IsIndexable(fs)) {
      int textId = IndexSpec_CreateTextId(sp);
      if (textId < 0) {
        QueryError_SetErrorFmt(status, QUERY_ELIMIT, "Schema is limited to %d TEXT fields",
                               SPEC_MAX_FIELD_ID);
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
      if (fs->types & INDEXFLD_T_TAG && isSpecJson(sp)) {
        QueryError_SetErrorFmt(status, QUERY_EBADOPTION,
                               "On JSON, cannot set tag field to sortable - %s", fieldName);
        goto reset;
      }

      if (fs->options & FieldSpec_Dynamic) {
        QueryError_SetErrorFmt(status, QUERY_EBADOPTION,
                               "Cannot set dynamic field to sortable - %s", fieldName);
        goto reset;
      }

      fs->sortIdx = RSSortingTable_Add(&sp->sortables, fs->name, fieldTypeToValueType(fs->types));
      if (fs->sortIdx == -1) {
        QueryError_SetErrorFmt(status, QUERY_ELIMIT, "Schema is limited to %d Sortable fields",
                               SPEC_MAX_FIELDS);
        goto reset;
      }
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

int IndexSpec_AddFields(IndexSpec *sp, RedisModuleCtx *ctx, ArgsCursor *ac, bool initialScan,
                        QueryError *status) {
  int rc = IndexSpec_AddFieldsInternal(sp, ac, status, 0);
  if (rc && initialScan) {
    IndexSpec_ScanAndReindex(ctx, sp);
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
      {AC_MKBITFLAG(SPEC_SKIPINITIALSCAN_STR, &spec->flags, Index_SkipInitialScan)},

      // For compatibility
      {.name = "NOSCOREIDX", .target = &dummy, .type = AC_ARGTYPE_BOOLFLAG},
      {.name = "ON", .target = &rule_args.type, .len = &dummy2, .type = AC_ARGTYPE_STRING},
      SPEC_FOLLOW_HASH_ARGS_DEF(&rule_args){
          .name = SPEC_TEMPORARY_STR, .target = &timeout, .type = AC_ARGTYPE_LLONG},
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

  if (spec->rule->filter_exp) {
    SchemaRule_FilterFields(spec->rule);
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

void Spec_AddToDict(const IndexSpec *sp) {
  dictAdd(specDict_g, sp->name, (void *)sp);
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
    ret->fields[ii].name = rm_strdup(spec->fields[ii].name);
    // if name & path are pointing to the same string, copy pointer
    if (ret->fields[ii].path && (spec->fields[ii].name != spec->fields[ii].path)) {
      ret->fields[ii].path = rm_strdup(spec->fields[ii].path);
    } else {
      // use the same pointer for both name and path
      ret->fields[ii].path = ret->fields[ii].name;
    }
  }
  return ret;
}

void IndexSpecCache_Decref(IndexSpecCache *c) {
  if (--c->refcount) {
    return;
  }
  for (size_t ii = 0; ii < c->nfields; ++ii) {
    if (c->fields[ii].name != c->fields[ii].path) {
      rm_free(c->fields[ii].name);
    }
    rm_free(c->fields[ii].path);
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

static redisearch_threadpool cleanPool = NULL;

void CleanPool_ThreadPoolStart() {
  if (!cleanPool) {
    cleanPool = redisearch_thpool_init(1);
  }
}

void CleanPool_ThreadPoolDestroy() {
  if (cleanPool) {
    RedisModule_ThreadSafeContextUnlock(RSDummyContext);
    if (RSGlobalConfig.freeResourcesThread) {
      redisearch_thpool_wait(cleanPool);
    }
    redisearch_thpool_destroy(cleanPool);
    cleanPool = NULL;
    RedisModule_ThreadSafeContextLock(RSDummyContext);
  }
}

/*
 * Free resources of unlinked index spec
 */
static void IndexSpec_FreeUnlinkedData(IndexSpec *spec) {
  // Free all documents metadata
  DocTable_Free(&spec->docs);
  // Free TEXT field trie and inverted indexes
  if (spec->terms) {
    TrieType_Free(spec->terms);
  }
  // Free NUMERIC, TAG and GEO fields trie and inverted indexes
  if (spec->keysDict) {
    dictRelease(spec->keysDict);
  }
  // Free synonym data
  if (spec->smap) {
    SynonymMap_Free(spec->smap);
  }
  // Destroy spec rule
  if (spec->rule) {
    SchemaRule_Free(spec->rule);
    spec->rule = NULL;
  }
  // Free fields cache data
  if (spec->spcache) {
    IndexSpecCache_Decref(spec->spcache);
    spec->spcache = NULL;
  }
  // Free fields formatted names
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
  // Free fields data
  if (spec->fields != NULL) {
    for (size_t i = 0; i < spec->numFields; i++) {
      if (spec->fields[i].name != spec->fields[i].path) {
        rm_free(spec->fields[i].name);
      }
      rm_free(spec->fields[i].path);
    }
    rm_free(spec->fields);
  }
  // Free spec name
  rm_free(spec->name);
  // Free sortable list
  if (spec->sortables) {
    SortingTable_Free(spec->sortables);
    spec->sortables = NULL;
  }
  // Free spec struct
  rm_free(spec);
}

/*
 * This function unlinks the index spec from any global structures and frees
 * all struct that requires acquiring the GIL.
 * Other resources are freed using IndexSpec_FreeData.
 */
void IndexSpec_FreeInternals(IndexSpec *spec) {
  // Remove spec from global index list
  if (spec->name && dictFetchValue(specDict_g, spec->name) == spec) {
    dictDelete(specDict_g, spec->name);
  }
  // Stop scanner
  if (spec->scanner) {
    spec->scanner->cancelled = true;
    spec->scanner->spec = NULL;
  }
  // Remove spec from global aliases list
  if (spec->uniqueId) {
    // If uniqueid is 0, it means the index was not initialized
    // and is being freed now during an error.
    IndexSpec_ClearAliases(spec);
  }
  // Remove spec prefix from global index list
  SchemaPrefixes_RemoveSpec(spec);
  // For temporary index
  if (spec->isTimerSet) {
    RedisModule_StopTimer(RSDummyContext, spec->timerId, NULL);
    spec->isTimerSet = false;
  }
  // Stop and destroy indexer
  if (spec->indexer) {
    Indexer_Free(spec->indexer);
  }
  // Stop and destroy garbage collector
  if (spec->gc) {
    GCContext_Stop(spec->gc);
  }
  // Remove existing cursors from global list
  if (spec->uniqueId) {
    // If uniqueid is 0, it means the index was not initialized
    // and is being freed now during an error.
    Cursors_PurgeWithName(&RSCursors, spec->name);
    CursorList_RemoveSpec(&RSCursors, spec->name);
  }
  // Free stopwords list (might use global pointer to default list)
  if (spec->stopwords) {
    StopWordList_Unref(spec->stopwords);
    spec->stopwords = NULL;
  }
  // Free unlinked index spec on a second thread
  if (RSGlobalConfig.freeResourcesThread == false) {
    IndexSpec_FreeUnlinkedData(spec);
  } else {
    redisearch_thpool_add_work(cleanPool, (redisearch_thpool_proc)IndexSpec_FreeUnlinkedData, spec);
  }
}

//---------------------------------------------------------------------------------------------

// called on master shard for temporary indexes and deletes all documents by defaults
static void IndexSpec_FreeTask(char *specName) {
#ifdef _DEBUG
  RedisModule_Log(NULL, "notice", "Freeing index %s in background", specName);
#endif
  RedisModule_ThreadSafeContextLock(RSDummyContext);

  // pass FT.DROPINDEX with "DD" flag to slef.
  RedisModuleCallReply *rep = RedisModule_Call(RSDummyContext, RS_DROP_INDEX_CMD, "cc!", specName, "DD");
  if (rep) {
    RedisModule_FreeCallReply(rep);
  }

  RedisModule_ThreadSafeContextUnlock(RSDummyContext);

  rm_free(specName);
}

void IndexSpec_LegacyFree(void *spec) {
  // free legacy index do nothing, it will be called only
  // when the index key will be deleted and we keep the legacy
  // index pointer in the legacySpecDict so we will free it when needed
}

void IndexSpec_Free(IndexSpec *spec) {
  if (spec->flags & Index_Temporary) {
    if (spec->isTimerSet) {
      RedisModule_StopTimer(RSDummyContext, spec->timerId, NULL);
      spec->isTimerSet = false;
    }
    redisearch_thpool_add_work(cleanPool, (redisearch_thpool_proc)IndexSpec_FreeTask, rm_strdup(spec->name));
    return;
  }

  IndexSpec_FreeInternals(spec);
}

//---------------------------------------------------------------------------------------------

void Indexes_Free(dict *d) {
  // free the schema dictionary this way avoid iterating over it for each combination of
  // spec<-->prefix
  SchemaPrefixes_Free(ScemaPrefixes_g);
  SchemaPrefixes_Create();
  // cursor list is iterating through the list as well and consuming a lot of CPU
  CursorList_Empty(&RSCursors);

  arrayof(IndexSpec *) specs = array_new(IndexSpec *, dictSize(d));
  dictIterator *iter = dictGetIterator(d);
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

  IndexSpec *sp = dictFetchValue(specDict_g, ixname);
  if (!sp) {
    if (!(options->flags & INDEXSPEC_LOAD_NOALIAS)) {
      sp = IndexAlias_Get(ixname);
    }
    if (!sp) {
      return NULL;
    }
  }

  // Increament the number of uses. 
  // When we move to multi readers this counter needs to be atomic.
  ++sp->counter;

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
  sp->docs = DocTable_New(INITIAL_DOC_TABLE_SIZE);
  sp->stopwords = DefaultStopWordList();
  sp->terms = NewTrie();
  sp->keysDict = NULL;
  sp->getValue = NULL;
  sp->getValueCtx = NULL;

  sp->timeout = 0;
  sp->isTimerSet = false;
  sp->timerId = 0;

  sp->scanner = NULL;
  sp->scan_in_progress = false;

  memset(&sp->stats, 0, sizeof(sp->stats));
  return sp;
}

FieldSpec *IndexSpec_CreateField(IndexSpec *sp, const char *name, const char *path) {
  sp->fields = rm_realloc(sp->fields, sizeof(*sp->fields) * (sp->numFields + 1));
  FieldSpec *fs = sp->fields + sp->numFields;
  memset(fs, 0, sizeof(*fs));
  fs->index = sp->numFields++;
  fs->name = rm_strdup(name);
  fs->path = (path) ? rm_strdup(path) : fs->name;
  fs->ftId = (t_fieldId)-1;
  fs->ftWeight = 1.0;
  fs->sortIdx = -1;
  fs->tagFlags = TAG_FIELD_DEFAULT_FLAGS;
  if (!(sp->flags & Index_FromLLAPI)) {
    RS_LOG_ASSERT((sp->rule), "index w/o a rule?");
    switch (sp->rule->type) {
      case DocumentType_Hash:
        fs->tagSep = TAG_FIELD_DEFAULT_HASH_SEP; break;
      case DocumentType_Json:
        fs->tagSep = TAG_FIELD_DEFAULT_JSON_SEP; break;
      case DocumentType_Unsupported:
        RS_LOG_ASSERT(0, "shouldn't get here");
    }
  }
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
static int FieldSpec_RdbLoadCompat8(RedisModuleIO *rdb, FieldSpec *f, int encver) {
  LoadStringBufferAlloc_IOErrors(rdb, f->name, NULL, goto fail);

  // the old versions encoded the bit id of the field directly
  // we convert that to a power of 2
  if (encver < INDEX_MIN_WIDESCHEMA_VERSION) {
    f->ftId = bit(LoadUnsigned_IOError(rdb, goto fail));
  } else {
    // the new version encodes just the power of 2 of the bit
    f->ftId = LoadUnsigned_IOError(rdb, goto fail);
  }
  f->types = LoadUnsigned_IOError(rdb, goto fail);
  f->ftWeight = LoadDouble_IOError(rdb, goto fail);
  f->tagFlags = TAG_FIELD_DEFAULT_FLAGS;
  f->tagSep = TAG_FIELD_DEFAULT_HASH_SEP;
  if (encver >= 4) {
    f->options = LoadUnsigned_IOError(rdb, goto fail);
    f->sortIdx = LoadSigned_IOError(rdb, goto fail);
  }
  return REDISMODULE_OK;

fail:
  return REDISMODULE_ERR;
}

static void FieldSpec_RdbSave(RedisModuleIO *rdb, FieldSpec *f) {
  RedisModule_SaveStringBuffer(rdb, f->name, strlen(f->name) + 1);
  if (f->path != f->name) {
    RedisModule_SaveUnsigned(rdb, 1);
    RedisModule_SaveStringBuffer(rdb, f->path, strlen(f->path) + 1);
  } else {
    RedisModule_SaveUnsigned(rdb, 0);
  }
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

static int FieldSpec_RdbLoad(RedisModuleIO *rdb, FieldSpec *f, int encver) {

  // Fall back to legacy encoding if needed
  if (encver < INDEX_MIN_TAGFIELD_VERSION) {
    return FieldSpec_RdbLoadCompat8(rdb, f, encver);
  }

  LoadStringBufferAlloc_IOErrors(rdb, f->name, NULL, goto fail);
  f->path = f->name;
  if (encver >= INDEX_JSON_VERSION) {
    if (LoadUnsigned_IOError(rdb, goto fail) == 1) {
      LoadStringBufferAlloc_IOErrors(rdb, f->path, NULL,goto fail);
    }
  }

  f->types = LoadUnsigned_IOError(rdb, goto fail);
  f->options = LoadUnsigned_IOError(rdb, goto fail);
  f->sortIdx = LoadSigned_IOError(rdb, goto fail);

  if (encver < INDEX_MIN_MULTITYPE_VERSION) {
    RS_LOG_ASSERT(f->types <= IDXFLD_LEGACY_MAX, "field type should be string or numeric");
    f->types = fieldTypeMap[f->types];
  }

  // Load text specific options
  if (FIELD_IS(f, INDEXFLD_T_FULLTEXT) || (f->options & FieldSpec_Dynamic)) {
    f->ftId = LoadUnsigned_IOError(rdb, goto fail);
    f->ftWeight = LoadDouble_IOError(rdb, goto fail);
  }
  // Load tag specific options
  if (FIELD_IS(f, INDEXFLD_T_TAG) || (f->options & FieldSpec_Dynamic)) {
    f->tagFlags = LoadUnsigned_IOError(rdb, goto fail);
    // Load the separator
    size_t l;
    char *s = LoadStringBuffer_IOError(rdb, &l, goto fail);
    RS_LOG_ASSERT(l == 1, "buffer length should be 1");
    f->tagSep = *s;
    RedisModule_Free(s);
  }
  return REDISMODULE_OK;

fail:
  return REDISMODULE_ERR;
}

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

///////////////////////////////////////////////////////////////////////////////////////////////

static redisearch_threadpool reindexPool = NULL;

static IndexesScanner *IndexesScanner_New(IndexSpec *spec) {
  if (!spec && global_spec_scanner) {
    return NULL;
  }
  IndexesScanner *scanner = rm_calloc(1, sizeof(IndexesScanner));
  scanner->global = !spec;
  scanner->spec = spec;
  scanner->scannedKeys = 0;
  scanner->cancelled = false;
  scanner->totalKeys = RedisModule_DbSize(RSDummyContext);

  if (spec) {
    // scan already in progress?
    if (spec->scanner) {
      // cancel ongoing scan, keep on_progress indicator on
      IndexesScanner_Cancel(spec->scanner, true);
      RedisModule_Log(RSDummyContext, "notice", "Scanning index %s in background: cancelled and restarted",
                  spec->name);
    }
    spec->scanner = scanner;
    spec->scan_in_progress = true;
  } else {
    global_spec_scanner = scanner;
    RedisModule_Log(RSDummyContext, "notice", "Global scanner created");    
  }

  return scanner;
}

void IndexesScanner_Free(IndexesScanner *scanner) {
  if (global_spec_scanner == scanner) {
    global_spec_scanner = NULL;
  } else if (!scanner->cancelled) {
    if (scanner->spec && scanner->spec->scanner == scanner) {
      scanner->spec->scanner = NULL;
      scanner->spec->scan_in_progress = false;
    }
  }

  rm_free(scanner);
}

void IndexesScanner_Cancel(IndexesScanner *scanner, bool still_in_progress) {
  if (scanner->cancelled) {
    return;
  }
  if (!scanner->global && scanner->spec) {
    scanner->spec->scan_in_progress = still_in_progress;
    scanner->spec->scanner = NULL;
    scanner->spec = NULL;
  }
  scanner->cancelled = true;
}

//---------------------------------------------------------------------------------------------

static void IndexSpec_DoneIndexingCallabck(struct RSAddDocumentCtx *docCtx, RedisModuleCtx *ctx,
                                           void *pd) {
}

//---------------------------------------------------------------------------------------------

static void Indexes_ScanProc(RedisModuleCtx *ctx, RedisModuleString *keyname, RedisModuleKey *key,
                             IndexesScanner *scanner) {
  // RMKey it is provided as best effort but in some cases it might be NULL
  bool keyOpened = false;
  if (!key) {
    key = RedisModule_OpenKey(ctx, keyname, REDISMODULE_READ);
    keyOpened = true;
  }

  // check type of document is support and document is not empty
  DocumentType type = getDocType(key);
  if (type == DocumentType_Unsupported) {
    return;
  }

  if (keyOpened) {
    RedisModule_CloseKey(key);
  }

  if (scanner->cancelled) {
    return;
  }
  if (scanner->global) {
    Indexes_UpdateMatchingWithSchemaRules(ctx, keyname, type, NULL);
  } else {
    IndexSpec_UpdateMatchingWithSchemaRules(scanner->spec, ctx, keyname, type);
  }
  ++scanner->scannedKeys;
}

//---------------------------------------------------------------------------------------------

static void Indexes_ScanAndReindexTask(IndexesScanner *scanner) {
  RS_LOG_ASSERT(scanner, "invalid IndexesScanner");

  RedisModuleCtx *ctx = RedisModule_GetThreadSafeContext(NULL);
  RedisModuleScanCursor *cursor = RedisModule_ScanCursorCreate();
  RedisModule_ThreadSafeContextLock(ctx);

  if (scanner->cancelled) {
    goto end;
  }
  if (scanner->global) {
    RedisModule_Log(ctx, "notice", "Scanning indexes in background");
  } else {
    RedisModule_Log(ctx, "notice", "Scanning index %s in background", scanner->spec->name);
  }

  while (RedisModule_Scan(ctx, cursor, (RedisModuleScanCB)Indexes_ScanProc, scanner)) {
    RedisModule_ThreadSafeContextUnlock(ctx);
    sched_yield();
    RedisModule_ThreadSafeContextLock(ctx);

    if (scanner->cancelled) {
      RedisModule_Log(ctx, "notice", "Scanning indexes in background: cancelled (scanned=%ld)",
                  scanner->totalKeys);
      goto end;
    }
  }

  if (scanner->global) {
    RedisModule_Log(ctx, "notice", "Scanning indexes in background: done (scanned=%ld)",
                  scanner->totalKeys);
  } else {
    RedisModule_Log(ctx, "notice", "Scanning index %s in background: done (scanned=%ld)",
                  scanner->spec->name, scanner->totalKeys);
  }

end:
  if (!scanner->cancelled && scanner->global) {
    Indexes_SetTempSpecsTimers(TimerOp_Add);
  }

  IndexesScanner_Free(scanner);

  RedisModule_ThreadSafeContextUnlock(ctx);
  RedisModule_ScanCursorDestroy(cursor);
  RedisModule_FreeThreadSafeContext(ctx);
}

//---------------------------------------------------------------------------------------------

static void IndexSpec_ScanAndReindexAsync(IndexSpec *sp) {
  if (!reindexPool) {
    reindexPool = redisearch_thpool_init(1);
  }
#ifdef _DEBUG
  RedisModule_Log(NULL, "notice", "Register index %s for async scan", sp->name);
#endif
  IndexesScanner *scanner = IndexesScanner_New(sp);
  redisearch_thpool_add_work(reindexPool, (redisearch_thpool_proc)Indexes_ScanAndReindexTask, scanner);
}

void ReindexPool_ThreadPoolDestroy() {
  if (reindexPool != NULL) {
    RedisModule_ThreadSafeContextUnlock(RSDummyContext);
    redisearch_thpool_destroy(reindexPool);
    reindexPool = NULL;
    RedisModule_ThreadSafeContextLock(RSDummyContext);
  }
}

//---------------------------------------------------------------------------------------------

void IndexSpec_ScanAndReindex(RedisModuleCtx *ctx, IndexSpec *sp) {
  size_t nkeys = RedisModule_DbSize(ctx);
  if (nkeys > 0) {
    IndexSpec_ScanAndReindexAsync(sp);
  }
}

void IndexSpec_DropLegacyIndexFromKeySpace(IndexSpec *sp) {
  RedisSearchCtx ctx = SEARCH_CTX_STATIC(RSDummyContext, sp);

  rune *rstr = NULL;
  t_len slen = 0;
  float score = 0;
  int dist = 0;
  size_t termLen;

  TrieIterator *it = Trie_Iterate(ctx.spec->terms, "", 0, 0, 1);
  while (TrieIterator_Next(it, &rstr, &slen, NULL, &score, &dist)) {
    char *res = runesToStr(rstr, slen, &termLen);
    RedisModuleString *keyName = fmtRedisTermKey(&ctx, res, strlen(res));
    Redis_DropScanHandler(ctx.redisCtx, keyName, &ctx);
    RedisModule_FreeString(ctx.redisCtx, keyName);
    rm_free(res);
  }
  DFAFilter_Free(it->ctx);
  rm_free(it->ctx);
  TrieIterator_Free(it);

  // Delete the numeric, tag, and geo indexes which reside on separate keys
  for (size_t i = 0; i < ctx.spec->numFields; i++) {
    const FieldSpec *fs = ctx.spec->fields + i;
    if (FIELD_IS(fs, INDEXFLD_T_NUMERIC)) {
      Redis_DeleteKey(ctx.redisCtx, IndexSpec_GetFormattedKey(ctx.spec, fs, INDEXFLD_T_NUMERIC));
    }
    if (FIELD_IS(fs, INDEXFLD_T_TAG)) {
      Redis_DeleteKey(ctx.redisCtx, IndexSpec_GetFormattedKey(ctx.spec, fs, INDEXFLD_T_TAG));
    }
    if (FIELD_IS(fs, INDEXFLD_T_GEO)) {
      Redis_DeleteKey(ctx.redisCtx, IndexSpec_GetFormattedKey(ctx.spec, fs, INDEXFLD_T_GEO));
    }
  }
  RedisModuleString *str =
      RedisModule_CreateStringPrintf(ctx.redisCtx, INDEX_SPEC_KEY_FMT, ctx.spec->name);
  Redis_DeleteKey(ctx.redisCtx, str);
  RedisModule_FreeString(ctx.redisCtx, str);
}

void Indexes_UpgradeLegacyIndexes() {
  dictIterator *iter = dictGetIterator(legacySpecDict);
  dictEntry *entry = NULL;
  while ((entry = dictNext(iter))) {
    IndexSpec *sp = dictGetVal(entry);
    IndexSpec_DropLegacyIndexFromKeySpace(sp);

    // recreate the doctable
    DocTable_Free(&sp->docs);
    sp->docs = DocTable_New(INITIAL_DOC_TABLE_SIZE);

    // clear index stats
    memset(&sp->stats, 0, sizeof(sp->stats));

    // put the new index in the specDict_g
    dictAdd(specDict_g, sp->name, sp);
  }
  dictReleaseIterator(iter);
}

void Indexes_ScanAndReindex() {
  if (!reindexPool) {
    reindexPool = redisearch_thpool_init(1);
  }

  RedisModule_Log(NULL, "notice", "Scanning all indexes");
  IndexesScanner *scanner = IndexesScanner_New(NULL);
  // check no global scan is in progress
  if (scanner) {
    redisearch_thpool_add_work(reindexPool, (redisearch_thpool_proc)Indexes_ScanAndReindexTask, scanner);
  }
}

///////////////////////////////////////////////////////////////////////////////////////////////

IndexSpec *IndexSpec_CreateFromRdb(RedisModuleCtx *ctx, RedisModuleIO *rdb, int encver,
                                   QueryError *status) {
  IndexSpec *sp = rm_calloc(1, sizeof(IndexSpec));
  IndexSpec_MakeKeyless(sp);

  sp->sortables = NewSortingTable();
  sp->docs = DocTable_New(INITIAL_DOC_TABLE_SIZE);
  sp->name = LoadStringBuffer_IOError(rdb, NULL, goto cleanup);
  char *tmpName = rm_strdup(sp->name);
  RedisModule_Free(sp->name);
  sp->name = tmpName;
  sp->flags = (IndexFlags)LoadUnsigned_IOError(rdb, goto cleanup);
  if (encver < INDEX_MIN_NOFREQ_VERSION) {
    sp->flags |= Index_StoreFreqs;
  }

  sp->numFields = LoadUnsigned_IOError(rdb, goto cleanup);
  sp->fields = rm_calloc(sp->numFields, sizeof(FieldSpec));
  int maxSortIdx = -1;
  for (int i = 0; i < sp->numFields; i++) {
    FieldSpec *fs = sp->fields + i;
    if (FieldSpec_RdbLoad(rdb, sp->fields + i, encver) != REDISMODULE_OK) {
      QueryError_SetErrorFmt(status, QUERY_EPARSEARGS, "Failed to load index field");
      goto cleanup;
    }
    sp->fields[i].index = i;
    if (FieldSpec_IsSortable(fs)) {
      RSSortingTable_Add(&sp->sortables, fs->name, fieldTypeToValueType(fs->types));
    }
  }

  //    IndexStats_RdbLoad(rdb, &sp->stats);

  if (SchemaRule_RdbLoad(sp, rdb, encver) != REDISMODULE_OK) {
    QueryError_SetErrorFmt(status, QUERY_EPARSEARGS, "Failed to load schema rule");
    goto cleanup;
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
    if (sp->stopwords == NULL)
      goto cleanup;
  } else {
    sp->stopwords = DefaultStopWordList();
  }

  sp->uniqueId = spec_unique_ids++;

  IndexSpec_StartGC(ctx, sp, GC_DEFAULT_HZ);
  RedisModuleString *specKey = RedisModule_CreateStringPrintf(ctx, INDEX_SPEC_KEY_FMT, sp->name);
  CursorList_AddSpec(&RSCursors, sp->name, RSCURSORS_DEFAULT_CAPACITY);
  RedisModule_FreeString(ctx, specKey);

  if (sp->flags & Index_HasSmap) {
    sp->smap = SynonymMap_RdbLoad(rdb, encver);
    if (sp->smap == NULL)
      goto cleanup;
  }
  if (IndexSpec_OnCreate) {
    IndexSpec_OnCreate(sp);
  }

  sp->timeout = LoadUnsigned_IOError(rdb, goto cleanup);

  size_t narr = LoadUnsigned_IOError(rdb, goto cleanup);
  for (size_t ii = 0; ii < narr; ++ii) {
    QueryError _status;
    size_t dummy;
    char *s = LoadStringBuffer_IOError(rdb, &dummy, goto cleanup);
    int rc = IndexAlias_Add(s, sp, 0, &_status);
    RedisModule_Free(s);
    if (rc != REDISMODULE_OK) {
      RedisModule_Log(NULL, "notice", "Loading existing alias failed");
    }
  }

  sp->indexer = NewIndexer(sp);

  sp->scan_in_progress = false;

  IndexSpec *oldSpec = dictFetchValue(specDict_g, sp->name);
  if (oldSpec) {
    // spec already exists lets just free this one
    RedisModule_Log(NULL, "notice", "Loading an already existing index, will just ignore.");
    // setting unique id to zero will make sure index will not be removed from global
    // cursor map and aliases.
    sp->uniqueId = 0;
    IndexSpec_FreeInternals(sp);
    sp = oldSpec;
  } else {
    dictAdd(specDict_g, sp->name, sp);
  }

  return sp;

cleanup:
  IndexSpec_Free(sp);
  QueryError_SetErrorFmt(status, QUERY_EPARSEARGS, "while reading an index");
  return NULL;
}

void *IndexSpec_LegacyRdbLoad(RedisModuleIO *rdb, int encver) {
  if (encver < LEGACY_INDEX_MIN_VERSION || encver > LEGACY_INDEX_MAX_VERSION) {
    return NULL;
  }
  char *name = RedisModule_LoadStringBuffer(rdb, NULL);

  RedisModuleCtx *ctx = RedisModule_GetContextFromIO(rdb);
  IndexSpec *sp = rm_calloc(1, sizeof(IndexSpec));
  IndexSpec_MakeKeyless(sp);
  sp->sortables = NewSortingTable();
  sp->terms = NULL;
  sp->docs = DocTable_New(INITIAL_DOC_TABLE_SIZE);
  sp->name = rm_strdup(name);
  RedisModule_Free(name);
  sp->flags = (IndexFlags)RedisModule_LoadUnsigned(rdb);
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
      RSSortingTable_Add(&sp->sortables, fs->name, fieldTypeToValueType(fs->types));
    }
  }

  IndexStats_RdbLoad(rdb, &sp->stats);

  DocTable_LegacyRdbLoad(&sp->docs, rdb, encver);
  /* For version 3 or up - load the generic trie */
  if (encver >= 3) {
    sp->terms = TrieType_GenericLoad(rdb, 0);
  } else {
    sp->terms = NewTrie();
  }

  if (sp->flags & Index_HasCustomStopwords) {
    sp->stopwords = StopWordList_RdbLoad(rdb, encver);
  } else {
    sp->stopwords = DefaultStopWordList();
  }

  sp->uniqueId = spec_unique_ids++;

  sp->smap = NULL;
  if (sp->flags & Index_HasSmap) {
    sp->smap = SynonymMap_RdbLoad(rdb, encver);
  }
  if (IndexSpec_OnCreate) {
    IndexSpec_OnCreate(sp);
  }
  if (encver < INDEX_MIN_EXPIRE_VERSION) {
    sp->timeout = -1;
  } else {
    sp->timeout = RedisModule_LoadUnsigned(rdb);
  }

  if (encver >= INDEX_MIN_ALIAS_VERSION) {
    size_t narr = RedisModule_LoadUnsigned(rdb);
    for (size_t ii = 0; ii < narr; ++ii) {
      QueryError status;
      size_t dummy;
      char *s = RedisModule_LoadStringBuffer(rdb, &dummy);
      int rc = IndexAlias_Add(s, sp, 0, &status);
      RedisModule_Free(s);
      assert(rc == REDISMODULE_OK);
    }
  }
  sp->indexer = NewIndexer(sp);

  SchemaRuleArgs *rule_args = dictFetchValue(legacySpecRules, sp->name);
  if (!rule_args) {
    RedisModule_LogIOError(rdb, "warning",
                           "Could not find upgrade definition for legacy index '%s'", sp->name);
    IndexSpec_Free(sp);
    return NULL;
  }

  QueryError status;
  sp->rule = SchemaRule_Create(rule_args, sp, &status);

  dictDelete(legacySpecRules, sp->name);
  SchemaRuleArgs_Free(rule_args);

  if (!sp->rule) {
    RedisModule_LogIOError(rdb, "warning", "Failed creating rule for legacy index '%s', error='%s'",
                           sp->name, QueryError_GetError(&status));
    IndexSpec_Free(sp);
    return NULL;
  }

  // start the gc and add the spec to the cursor list
  IndexSpec_StartGC(RSDummyContext, sp, GC_DEFAULT_HZ);
  CursorList_AddSpec(&RSCursors, sp->name, RSCURSORS_DEFAULT_CAPACITY);

  dictAdd(legacySpecDict, sp->name, sp);
  return sp;
}

void IndexSpec_LegacyRdbSave(RedisModuleIO *rdb, void *value) {
  // we do not save legacy indexes
  return;
}

int Indexes_RdbLoad(RedisModuleIO *rdb, int encver, int when) {

  if (encver < INDEX_MIN_COMPAT_VERSION) {
    return REDISMODULE_ERR;
  }

  size_t nIndexes = LoadUnsigned_IOError(rdb, goto cleanup);
  RedisModuleCtx *ctx = RedisModule_GetContextFromIO(rdb);
  QueryError status = {0};
  for (size_t i = 0; i < nIndexes; ++i) {
    IndexSpec *sp = IndexSpec_CreateFromRdb(ctx, rdb, encver, &status);
    if (!sp) {
      RedisModule_Log(ctx, "error", "RDB Load: %s",
                      status.detail ? status.detail : "general failure");
      return REDISMODULE_ERR;
    }
  }
  return REDISMODULE_OK;

cleanup:
  return REDISMODULE_ERR;
}

void Indexes_RdbSave(RedisModuleIO *rdb, int when) {

  RedisModule_SaveUnsigned(rdb, dictSize(specDict_g));

  dictIterator *iter = dictGetIterator(specDict_g);
  dictEntry *entry = NULL;
  while ((entry = dictNext(iter))) {
    IndexSpec *sp = dictGetVal(entry);
    // we save the name plus the null terminator
    RedisModule_SaveStringBuffer(rdb, sp->name, strlen(sp->name) + 1);
    RedisModule_SaveUnsigned(rdb, (uint64_t)sp->flags);
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

// from this version we will have the loaded notification which means that scan
// will no longer be needed
Version noScanVersion = {
    .majorVersion = 6,
    .minorVersion = 0,
    .patchVersion = 7,
};

int CompareVestions(Version v1, Version v2) {
  if (v1.majorVersion < v2.majorVersion) {
    return -1;
  } else if (v1.majorVersion > v2.majorVersion) {
    return 1;
  }

  if (v1.minorVersion < v2.minorVersion) {
    return -1;
  } else if (v1.minorVersion > v2.minorVersion) {
    return 1;
  }

  if (v1.patchVersion < v2.patchVersion) {
    return -1;
  } else if (v1.patchVersion > v2.patchVersion) {
    return 1;
  }

  return 0;
}

static void Indexes_LoadingEvent(RedisModuleCtx *ctx, RedisModuleEvent eid, uint64_t subevent,
                                 void *data) {
  if (subevent == REDISMODULE_SUBEVENT_LOADING_RDB_START ||
      subevent == REDISMODULE_SUBEVENT_LOADING_AOF_START ||
      subevent == REDISMODULE_SUBEVENT_LOADING_REPL_START) {
    Indexes_Free(specDict_g);
    if (legacySpecDict) {
      dictEmpty(legacySpecDict, NULL);
    } else {
      legacySpecDict = dictCreate(&dictTypeHeapStrings, NULL);
    }
    RedisModule_Log(RSDummyContext, "notice", "Loading event starts");
  } else if (subevent == REDISMODULE_SUBEVENT_LOADING_ENDED) {
    int hasLegacyIndexes = dictSize(legacySpecDict);
    Indexes_UpgradeLegacyIndexes();

    // we do not need the legacy dict specs anymore
    dictRelease(legacySpecDict);
    legacySpecDict = NULL;

    LegacySchemaRulesArgs_Free(ctx);

    if (hasLegacyIndexes || CompareVestions(redisVersion, noScanVersion) < 0) {
      Indexes_ScanAndReindex();
    } else {
      RedisModule_Log(ctx, "warning",
                      "Skip background reindex scan, redis version contains loaded event.");
    }
    RedisModule_Log(RSDummyContext, "notice", "Loading event ends");
  }
}

int IndexSpec_RegisterType(RedisModuleCtx *ctx) {
  RedisModuleTypeMethods tm = {
      .version = REDISMODULE_TYPE_METHOD_VERSION,
      .rdb_load = IndexSpec_LegacyRdbLoad,
      .rdb_save = IndexSpec_LegacyRdbSave,
      .aux_load = Indexes_RdbLoad,
      .aux_save = Indexes_RdbSave,
      .free = IndexSpec_LegacyFree,
      .aof_rewrite = GenericAofRewrite_DisabledHandler,
      .aux_save_triggers = REDISMODULE_AUX_BEFORE_RDB,
  };

  IndexSpecType = RedisModule_CreateDataType(ctx, "ft_index0", INDEX_CURRENT_VERSION, &tm);
  if (IndexSpecType == NULL) {
    RedisModule_Log(ctx, "error", "Could not create index spec type");
    return REDISMODULE_ERR;
  }

  RedisModule_SubscribeToServerEvent(ctx, RedisModuleEvent_Loading, Indexes_LoadingEvent);

  return REDISMODULE_OK;
}

int Document_LoadSchemaFieldJson(Document *doc, RedisSearchCtx *sctx);

int IndexSpec_UpdateDoc(IndexSpec *spec, RedisModuleCtx *ctx, RedisModuleString *key, DocumentType type) {
  if (!spec->rule) {
    RedisModule_Log(ctx, "warning", "Index spec %s: no rule found", spec->name);
    return REDISMODULE_ERR;
  }

  RedisSearchCtx sctx = SEARCH_CTX_STATIC(ctx, spec);
  Document doc = {0};
  Document_Init(&doc, key, DEFAULT_SCORE, DEFAULT_LANGUAGE, type);
  // if a key does not exit, is not a hash or has no fields in index schema

  int rv = REDISMODULE_ERR;
  switch (type) {
  case DocumentType_Hash:
    rv = Document_LoadSchemaFieldHash(&doc, &sctx);
    break;
  case DocumentType_Json:
    rv = Document_LoadSchemaFieldJson(&doc, &sctx);
    break;
  case DocumentType_Unsupported:
    RS_LOG_ASSERT(0, "Should receieve valid type");
  }

  if (rv != REDISMODULE_OK) {
    spec->stats.indexingFailures++;

    // if a document did not load properly, it is deleted
    // to prevent mismatch of index and hash
    IndexSpec_DeleteDoc(spec, ctx, key);
    Document_Free(&doc);
    return REDISMODULE_ERR;
  }

  QueryError status = {0};
  RSAddDocumentCtx *aCtx = NewAddDocumentCtx(spec, &doc, &status);
  aCtx->stateFlags |= ACTX_F_NOBLOCK | ACTX_F_NOFREEDOC;
  AddDocumentCtx_Submit(aCtx, &sctx, DOCUMENT_ADD_REPLACE);

  Document_Free(&doc);
  return REDISMODULE_OK;
}

int IndexSpec_DeleteDoc(IndexSpec *spec, RedisModuleCtx *ctx, RedisModuleString *key) {

  // Get the doc ID
  t_docId id = DocTable_GetIdR(&spec->docs, key);
  if (id == 0) {
    return REDISMODULE_ERR;
    // ID does not exist.
  }

  int rc = DocTable_DeleteR(&spec->docs, key);
  if (rc) {
    spec->stats.numDocuments--;

    // Increment the index's garbage collector's scanning frequency after document deletions
    if (spec->gc) {
      GCContext_OnDelete(spec->gc);
    }
  }
  return REDISMODULE_OK;
}

///////////////////////////////////////////////////////////////////////////////////////////////

static void onFlush(RedisModuleCtx *ctx, RedisModuleEvent eid, uint64_t subevent, void *data) {
  if (subevent != REDISMODULE_SUBEVENT_FLUSHDB_START) {
    return;
  }
  Indexes_Free(specDict_g);
  Dictionary_Clear();
}

void Indexes_Init(RedisModuleCtx *ctx) {
  specDict_g = dictCreate(&dictTypeHeapStrings, NULL);
  RedisModule_SubscribeToServerEvent(ctx, RedisModuleEvent_FlushDB, onFlush);
  SchemaPrefixes_Create();
}

SpecOpIndexingCtx *Indexes_FindMatchingSchemaRules(RedisModuleCtx *ctx, RedisModuleString *key,
                                                   bool runFilters,
                                                   RedisModuleString *keyToReadData) {
  if (!keyToReadData) {
    keyToReadData = key;
  }
  SpecOpIndexingCtx *res = rm_malloc(sizeof(*res));
  res->specs = dictCreate(&dictTypeHeapStrings, NULL);
  res->specsOps = array_new(SpecOpCtx, 10);
  if (dictSize(specDict_g) == 0) {
    return res;
  }
  dict *specs = res->specs;

#if defined(_DEBUG) && 0
  RLookupKey *k = RLookup_GetKey(&r->lk, UNDERSCORE_KEY, 0);
  RSValue *v = RLookup_GetItem(k, &r->row);
  const char *x = RSValue_StringPtrLen(v, NULL);
  RedisModule_Log(NULL, "notice", "Indexes_FindMatchingSchemaRules: x=%s", x);
  const char *f = "name";
  k = RLookup_GetKey(&r->lk, f, 0);
  if (k) {
    v = RLookup_GetItem(k, &r->row);
    x = RSValue_StringPtrLen(v, NULL);
  }
#endif  // _DEBUG

  size_t n;
  const char *key_p = RedisModule_StringPtrLen(key, &n);
  arrayof(SchemaPrefixNode *) prefixes = array_new(SchemaPrefixNode *, 1);
  // collect specs that their name is prefixed by the key name
  // `prefixes` includes list of arrays of specs, one for each prefix of key name
  int nprefixes = TrieMap_FindPrefixes(ScemaPrefixes_g, key_p, n, (arrayof(void *) *)&prefixes);
  for (int i = 0; i < array_len(prefixes); ++i) {
    SchemaPrefixNode *node = prefixes[i];
    for (int j = 0; j < array_len(node->index_specs); ++j) {
      IndexSpec *spec = node->index_specs[j];
      if (!dictFind(specs, spec->name)) {
        SpecOpCtx specOp = {
            .spec = spec,
            .op = SpecOp_Add,
        };
        res->specsOps = array_append(res->specsOps, specOp);
        dictEntry *entry = dictAddRaw(specs, spec->name, NULL);
        // put the location on the specsOps array so we can get it
        // fast using index name
        entry->v.u64 = array_len(res->specsOps) - 1;
      }
    }
  }
  array_free(prefixes);

  if (runFilters) {

    EvalCtx *r = NULL;
    for (size_t i = 0; i < array_len(res->specsOps); ++i) {
      SpecOpCtx *specOp = res->specsOps + i;
      SchemaRule *rule = specOp->spec->rule;
      if (!rule->filter_exp) {
        continue;
      }

      if (!r) {
        // load hash only if required
        r = EvalCtx_Create();

        RLookup_LoadRuleFields(ctx, &r->lk, &r->row, rule, key_p);
      }

      if (EvalCtx_EvalExpr(r, rule->filter_exp) == EXPR_EVAL_OK) {
        IndexSpec *spec = rule->spec;
        if (!RSValue_BoolTest(&r->res) && dictFind(specs, spec->name)) {
          specOp->op = SpecOp_Del;
        }
      }
      QueryError_ClearError(r->ee.err);
    }

    if (r) {
      EvalCtx_Destroy(r);
    }
  }
  return res;
}

static bool hashFieldChanged(IndexSpec *spec, RedisModuleString **hashFields) {
  if (hashFields == NULL) {
    return true;
  }

  // TODO: improve implementation to avoid O(n^2)
  for (size_t i = 0; hashFields[i] != NULL; ++i) {
    const char *field = RedisModule_StringPtrLen(hashFields[i], NULL);
    for (size_t j = 0; j < spec->numFields; ++j) {
      if (!strcmp(field, spec->fields[j].name)) {
        return true;
      }
    }
    // optimize. change of score and payload fields just require an update of the doc table
    if ((spec->rule->lang_field && !strcmp(field, spec->rule->lang_field)) ||
        (spec->rule->score_field && !strcmp(field, spec->rule->score_field)) ||
        (spec->rule->payload_field && !strcmp(field, spec->rule->payload_field))) {
      return true;
    }
  }
  return false;
}

void Indexes_SpecOpsIndexingCtxFree(SpecOpIndexingCtx *specs) {
  dictRelease(specs->specs);
  array_free(specs->specsOps);
  rm_free(specs);
}

void Indexes_UpdateMatchingWithSchemaRules(RedisModuleCtx *ctx, RedisModuleString *key, DocumentType type,
                                           RedisModuleString **hashFields) {
  if (type == DocumentType_Unsupported) {
    // COPY could overwrite a hash/json with other types so we must try and remove old doc
    Indexes_DeleteMatchingWithSchemaRules(ctx, key, hashFields);
    return;
  }

  SpecOpIndexingCtx *specs = Indexes_FindMatchingSchemaRules(ctx, key, true, NULL);

  for (size_t i = 0; i < array_len(specs->specsOps); ++i) {
    SpecOpCtx *specOp = specs->specsOps + i;

    // skip if document type does not match the index type
    if (type != specOp->spec->rule->type) {
      continue;
    }

    if (!hashFields || hashFieldChanged(specOp->spec, hashFields)) {
      if (specOp->op == SpecOp_Add) {
        IndexSpec_UpdateDoc(specOp->spec, ctx, key, type);
      } else {
        IndexSpec_DeleteDoc(specOp->spec, ctx, key);
      }
    }
  }

  Indexes_SpecOpsIndexingCtxFree(specs);
}

void IndexSpec_UpdateMatchingWithSchemaRules(IndexSpec *sp, RedisModuleCtx *ctx,
                                             RedisModuleString *key, DocumentType type) {
  if (type != sp->rule->type) {
    return;
  }

  SpecOpIndexingCtx *specs = Indexes_FindMatchingSchemaRules(ctx, key, true, NULL);
  if (!dictFind(specs->specs, sp->name)) {
    goto end;
  }

  for (size_t i = 0; i < array_len(specs->specsOps); ++i) {
    SpecOpCtx *specOp = specs->specsOps + i;
    if (specOp->spec == sp) {
      if (specOp->op == SpecOp_Add) {
        IndexSpec_UpdateDoc(specOp->spec, ctx, key, type);
      } else {
        IndexSpec_DeleteDoc(specOp->spec, ctx, key);
      }
    }
  }
end:
  Indexes_SpecOpsIndexingCtxFree(specs);
}

void Indexes_DeleteMatchingWithSchemaRules(RedisModuleCtx *ctx, RedisModuleString *key,
                                           RedisModuleString **hashFields) {
  SpecOpIndexingCtx *specs = Indexes_FindMatchingSchemaRules(ctx, key, false, NULL);

  for (size_t i = 0; i < array_len(specs->specsOps); ++i) {
    SpecOpCtx *specOp = specs->specsOps + i;
    if (!hashFields || hashFieldChanged(specOp->spec, hashFields)) {
      IndexSpec_DeleteDoc(specOp->spec, ctx, key);
    }
  }

  Indexes_SpecOpsIndexingCtxFree(specs);
}

void Indexes_ReplaceMatchingWithSchemaRules(RedisModuleCtx *ctx, RedisModuleString *from_key,
                                            RedisModuleString *to_key) {
  DocumentType type = getDocTypeFromString(to_key);
  if (type == DocumentType_Unsupported) {
    return;
  }

  SpecOpIndexingCtx *from_specs = Indexes_FindMatchingSchemaRules(ctx, from_key, true, to_key);
  SpecOpIndexingCtx *to_specs = Indexes_FindMatchingSchemaRules(ctx, to_key, true, NULL);

  size_t from_len, to_len;
  const char *from_str = RedisModule_StringPtrLen(from_key, &from_len);
  const char *to_str = RedisModule_StringPtrLen(to_key, &to_len);

  for (size_t i = 0; i < array_len(from_specs->specsOps); ++i) {
    SpecOpCtx *specOp = from_specs->specsOps + i;
    IndexSpec *spec = specOp->spec;
    if (specOp->op == SpecOp_Del) {
      // the document is not in the index from the first place
      continue;
    }
    dictEntry *entry = dictFind(to_specs->specs, spec->name);
    if (entry) {
      DocTable_Replace(&spec->docs, from_str, from_len, to_str, to_len);
      size_t index = entry->v.u64;
      dictDelete(to_specs->specs, spec->name);
      array_del_fast(to_specs->specsOps, index);
    } else {
      IndexSpec_DeleteDoc(spec, ctx, from_key);
    }
  }

  // add to a different index
  for (size_t i = 0; i < array_len(to_specs->specsOps); ++i) {
    SpecOpCtx *specOp = to_specs->specsOps + i;
    if (specOp->op == SpecOp_Del) {
      // not need to index
      // also no need to delete because we know that the document is
      // not in the index because if it was there we would handle it
      // on the spec from section.
      continue;
    }
    IndexSpec_UpdateDoc(specOp->spec, ctx, to_key, type);
  }
  Indexes_SpecOpsIndexingCtxFree(from_specs);
  Indexes_SpecOpsIndexingCtxFree(to_specs);
}
///////////////////////////////////////////////////////////////////////////////////////////////
