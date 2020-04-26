#include "rmutil/util.h"
#include "spec.h"
#include "util/logging.h"
#include "util/misc.h"
#include "rmutil/vector.h"
#include "trie/trie_type.h"
#include <math.h>
#include <ctype.h>
#include "rmalloc.h"
#include "config.h"
#include "cursor.h"
#include "tag_index.h"
#include "redis_index.h"
#include "indexer.h"
#include "alias.h"
#include "module.h"
#include "rules/rules.h"
#include "numeric_index.h"
#include "rmutil/rm_assert.h"

void (*IndexSpec_OnCreate)(const IndexSpec *) = NULL;
const char *(*IndexAlias_GetUserTableName)(RedisModuleCtx *, const char *) = NULL;
static void freeLegacyParams(IndexSpec *sp);

static uint64_t spec_unique_ids = 1;

static const FieldSpec *getFieldCommon(const IndexSpec *spec, const char *name, size_t len,
                                       int useCase) {
  for (size_t i = 0; i < spec->numFields; i++) {
    if (len != strlen(spec->fields[i].name)) {
      continue;
    }
    const FieldSpec *fs = spec->fields + i;
    if (useCase) {
      if (!strncmp(fs->name, name, len)) {
        return fs;
      }
    } else {
      if (!strncasecmp(fs->name, name, len)) {
        return fs;
      }
    }
  }
  return NULL;
}

static void expiredCallback(RedisModuleCtx *ctx, void *arg) {
  IndexSpec *spec = arg;
  spec->timer = 0;
  spec->state |= IDX_S_EXPIRED;
  IndexSpec_FreeEx(spec, 0);
}

static void resetTimer(IndexSpec *sp) {
  if (!(sp->flags & Index_Temporary)) {
    return;
  }

  if (sp->timer) {
    RedisModule_StopTimer(RSDummyContext, sp->timer, NULL);
  }
  sp->timer = RedisModule_CreateTimer(RSDummyContext, sp->timeout * 1000, expiredCallback, sp);
}

/*
 * Get a field spec by field name. Case insensitive!
 * Return the field spec if found, NULL if not
 */
const FieldSpec *IndexSpec_GetField(const IndexSpec *spec, const char *name, size_t len) {
  return getFieldCommon(spec, name, len, 0);
};

const FieldSpec *IndexSpec_GetFieldCase(const IndexSpec *spec, const char *name, size_t n) {
  return getFieldCommon(spec, name, n, 1);
}

t_fieldMask IndexSpec_GetFieldBit(IndexSpec *spec, const char *name, size_t len) {
  const FieldSpec *sp = IndexSpec_GetField(spec, name, len);
  if (!sp || !FIELD_IS(sp, INDEXFLD_T_FULLTEXT) || !FieldSpec_IsIndexable(sp)) return 0;

  return FIELD_BIT(sp);
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

static void prepareGeoIndexes(IndexSpec *sp) {
  for (size_t ii = 0; ii < sp->numFields; ++ii) {
    if (FIELD_IS(sp->fields + ii, INDEXFLD_T_GEO)) {
      GeoIndex *geo = IDX_LoadGeo(sp, sp->fields + ii, REDISMODULE_WRITE);
      GeoIndex_PrepareKey(RSDummyContext, geo);
    }
  }
}
static void IndexSpec_Unregister(IndexSpec *spec);

int IndexSpec_Register(IndexSpec *sp, const IndexCreateOptions *options, QueryError *status) {
  dictEntry *e = dictFind(RSIndexes_g, sp->name);
  if (e) {
    if (options && options->replace) {
      IndexSpec_Unregister(e->v.val);
    } else {
      QueryError_SetCode(status, QUERY_EINDEXEXISTS);
      return REDISMODULE_ERR;
    }
  }

  sp->uniqueId = spec_unique_ids++;
  dictAdd(RSIndexes_g, sp->name, sp);
  // Start the garbage collector
  IndexSpec_StartGC(RSDummyContext, sp, GC_DEFAULT_HZ);
  CursorList_AddSpec(&RSCursors, sp->name, RSCURSORS_DEFAULT_CAPACITY);
  prepareGeoIndexes(sp);
  resetTimer(sp);
  if (IndexSpec_OnCreate) {
    IndexSpec_OnCreate(sp);
  }

  sp->state |= IDX_S_REGISTERED;  // Already created
  return REDISMODULE_OK;
}

char *strtolower(char *str) {
  char *p = str;
  while (*p) {
    *p = tolower(*p);
    p++;
  }
  return str;
}

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

static int parseTextField(FieldSpec *sp, ArgsCursor *ac, QueryError *status) {
  int rc;
  // this is a text field
  // init default weight and type
  while (!AC_IsAtEnd(ac)) {
    if (AC_AdvanceIfMatch(ac, SPEC_NOSTEM_STR)) {
      sp->options |= FieldSpec_NoStemming;
      continue;

    } else if (AC_AdvanceIfMatch(ac, SPEC_WEIGHT_STR)) {
      double d;
      if ((rc = AC_GetDouble(ac, &d, 0)) != AC_OK) {
        QERR_MKBADARGS_AC(status, "weight", rc);
        return 0;
      }
      sp->ftWeight = d;
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
      sp->options |= FieldSpec_Phonetics;
      continue;

    } else {
      break;
    }
  }
  return 1;
}

void FieldSpec_Initialize(FieldSpec *sp, FieldType types) {
  sp->types |= types;
  if (FIELD_IS(sp, INDEXFLD_T_TAG)) {
    sp->tagFlags = TAG_FIELD_DEFAULT_FLAGS;
    sp->tagSep = TAG_FIELD_DEFAULT_SEP;
  }
}

/* Parse a field definition from argv, at *offset. We advance offset as we progress.
 *  Returns 1 on successful parse, 0 otherwise */
static int parseFieldSpec(ArgsCursor *ac, FieldSpec *sp, QueryError *status) {
  if (AC_IsAtEnd(ac)) {
    QueryError_SetErrorFmt(status, QUERY_EPARSEARGS, "Field `%s` does not have a type", sp->name);
    return 0;
  }

  if (AC_AdvanceIfMatch(ac, SPEC_TEXT_STR)) {
    FieldSpec_Initialize(sp, INDEXFLD_T_FULLTEXT);
    if (!parseTextField(sp, ac, status)) {
      goto error;
    }
  } else if (AC_AdvanceIfMatch(ac, NUMERIC_STR)) {
    FieldSpec_Initialize(sp, INDEXFLD_T_NUMERIC);
  } else if (AC_AdvanceIfMatch(ac, GEO_STR)) {  // geo field
    FieldSpec_Initialize(sp, INDEXFLD_T_GEO);
  } else if (AC_AdvanceIfMatch(ac, SPEC_TAG_STR)) {  // tag field
    FieldSpec_Initialize(sp, INDEXFLD_T_TAG);
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
      sp->tagSep = *sep;
    }
  } else {  // not numeric and not text - nothing more supported currently
    QueryError_SetErrorFmt(status, QUERY_EPARSEARGS, "Invalid field type for field `%s`", sp->name);
    goto error;
  }

  while (!AC_IsAtEnd(ac)) {
    if (AC_AdvanceIfMatch(ac, SPEC_SORTABLE_STR)) {
      FieldSpec_SetSortable(sp);
      continue;
    } else if (AC_AdvanceIfMatch(ac, SPEC_NOINDEX_STR)) {
      sp->options |= FieldSpec_NotIndexable;
      continue;
    } else {
      break;
    }
  }
  return 1;

error:
  if (!QueryError_HasError(status)) {
    QueryError_SetErrorFmt(status, QUERY_EPARSEARGS, "Could not parse schema for field `%s`",
                           sp->name);
  }
  FieldSpec_Cleanup(sp);
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
  return IndexSpec_AddFieldsInternal(sp, ac, status, 0);
}

typedef struct {
  char *expr;
  char *score;
  char *lang;
} ruleSettings;

static int applyRuleSettings(IndexSpec *spec, ruleSettings *settings, QueryError *status) {
#define APPEND_ARG(a) ruleargs = array_append(ruleargs, a)
  // Create an expression rule type...
  char **ruleargs = array_new(char *, 6);
  APPEND_ARG("EXPR");
  APPEND_ARG(settings->expr);
  APPEND_ARG("INDEX");
  if (settings->lang || settings->score) {
    APPEND_ARG("LOADATTRS");
    if (settings->lang) {
      APPEND_ARG("LANGUAGE");
      APPEND_ARG(settings->lang);
    }
    if (settings->score) {
      APPEND_ARG("SCORE");
      APPEND_ARG(settings->score);
    }
  }
  ArgsCursor ruleAc = {0};
  ArgsCursor_InitCString(&ruleAc, (const char **)ruleargs, array_len(ruleargs));
  int rc = SchemaRules_AddArgsInternal(SchemaRules_g, spec, "__generated__", &ruleAc, status);
  array_free(ruleargs);
  ruleargs = NULL;
  if (rc == REDISMODULE_OK) {
    spec->minRulesVersion = SchemaRules_IncrRevision();
    spec->state |= IDX_S_SCANNING;
    SchemaRules_StartScan(0);
  }
  return rc;
}

IndexSpec *IndexSpec_ParseArgs(const char *name, ArgsCursor *ac, IndexCreateOptions *options,
                               QueryError *status) {
  IndexSpec *spec = NewIndexSpec(name);
  ArgsCursor acStopwords = {0};
  IndexCreateOptions opts_s = {0};
  ruleSettings rulesopts = {0};
  if (!options) {
    options = &opts_s;
  }

  long long timeout = -1;
  int dummy, replace = 0;
  size_t dummy2;

  ACArgSpec argopts[] = {
      {AC_MKUNFLAG(SPEC_NOOFFSETS_STR, &spec->flags,
                   Index_StoreTermOffsets | Index_StoreByteOffsets)},
      {AC_MKUNFLAG(SPEC_NOHL_STR, &spec->flags, Index_StoreByteOffsets)},
      {AC_MKUNFLAG(SPEC_NOFIELDS_STR, &spec->flags, Index_StoreFieldFlags)},
      {AC_MKUNFLAG(SPEC_NOFREQS_STR, &spec->flags, Index_StoreFreqs)},
      {AC_MKBITFLAG(SPEC_SCHEMA_EXPANDABLE_STR, &spec->flags, Index_WideSchema)},

      {AC_MKBITFLAG(SPEC_WITHRULES_STR, &spec->flags, Index_UseRules)},
      {AC_MKBITFLAG(SPEC_ASYNC_STR, &spec->flags, Index_Async)},

      // For compatibility
      {.name = "NOSCOREIDX", .target = &dummy, .type = AC_ARGTYPE_BOOLFLAG},
      {.name = "REPLACE", .target = &options->replace, .type = AC_ARGTYPE_BOOLFLAG},
      {.name = "EXPRESSION", .target = &rulesopts.expr, .len = &dummy2, .type = AC_ARGTYPE_STRING},
      {.name = "SCORE", .target = &rulesopts.score, .len = &dummy2, .type = AC_ARGTYPE_STRING},
      {.name = "LANGUAGE", .target = &rulesopts.lang, .len = &dummy2, .type = AC_ARGTYPE_STRING},
      {.name = SPEC_TEMPORARY_STR, .target = &timeout, .type = AC_ARGTYPE_LLONG},
      {.name = SPEC_STOPWORDS_STR, .target = &acStopwords, .type = AC_ARGTYPE_SUBARGS},
      {.name = NULL}};

  ACArgSpec *errarg = NULL;
  int rc = AC_ParseArgSpec(ac, argopts, &errarg);
  if (rc != AC_OK) {
    if (rc != AC_ERR_ENOENT) {
      QERR_MKBADARGS_AC(status, errarg->name, rc);
      goto failure;
    }
  }

  if (timeout != -1) {
    spec->flags |= Index_Temporary;
  }
  spec->timeout = timeout;

  if (AC_IsInitialized(&acStopwords)) {
    if (spec->stopwords) {
      StopWordList_Unref(spec->stopwords);
    }
    spec->stopwords = StopWordList_FromArgs(&acStopwords);
    spec->flags |= Index_HasCustomStopwords;
  }

  if (!AC_AdvanceIfMatch(ac, SPEC_SCHEMA_STR)) {
    if (AC_NumRemaining(ac)) {
      const char *badarg = AC_GetStringNC(ac, NULL);
      QueryError_SetErrorFmt(status, QUERY_EPARSEARGS, "Unknown argument `%s`", badarg);
    } else {
      QueryError_SetError(status, QUERY_EPARSEARGS, "No schema found");
    }
    goto failure;
  }

  if (!IndexSpec_AddFieldsInternal(spec, ac, status, 1)) {
    goto failure;
  }
  if (rulesopts.expr) {
    spec->flags |= Index_UseRules;
  }

  if (spec->flags & Index_UseRules) {
    SchemaRules_RegisterIndex(spec);
    if (rulesopts.expr && applyRuleSettings(spec, &rulesopts, status) != REDISMODULE_OK) {
      goto failure;
    }
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

InvertedIndex *IDX_LoadTerm(IndexSpec *sp, const char *term, size_t n, int flags) {
  if ((flags & REDISMODULE_WRITE) == 0) {
    void *idx = raxFind(sp->termsIdx, (unsigned char *)term, n);
    if (idx == raxNotFound) {
      return NULL;
    } else {
      // printf("Returning %p for term %.*s (flags=%d)\n", idx, (int)n, term, flags);
      return idx;
    }
  }

  InvertedIndex *newix = sp->cachedInvidx;
  if (!newix) {
    newix = sp->cachedInvidx = NewInvertedIndex(sp->flags, 1);
  }
  InvertedIndex *old = NULL;
  int isNew = raxTryInsert(sp->termsIdx, (unsigned char *)term, n, newix, (void **)&old);
  if (isNew) {
    assert(!old);
    sp->stats.numTerms++;
    Trie_InsertStringBuffer(sp->terms, term, n, 1, 1, NULL);
    sp->cachedInvidx = NULL;
    return newix;
  } else {
    return old;
  }
}

#define DECLARE_IDXACC_COMMON(base, fnames, type, ctor, fld, T)     \
  T base(IndexSpec *sp, const FieldSpec *fs, int options) {         \
    if (!FIELD_IS(fs, type)) {                                      \
      return NULL;                                                  \
    }                                                               \
    T r = sp->fld[fs->index];                                       \
    if (r || (options & REDISMODULE_WRITE) == 0) {                  \
      return r;                                                     \
    }                                                               \
    r = sp->fld[fs->index] = ctor;                                  \
    return r;                                                       \
  }                                                                 \
  T fnames(IndexSpec *sp, const char *s, int options) {             \
    const FieldSpec *fs = IndexSpec_GetFieldCase(sp, s, strlen(s)); \
    if (!fs) {                                                      \
      return NULL;                                                  \
    }                                                               \
    return base(sp, fs, options);                                   \
  }

DECLARE_IDXACC_COMMON(IDX_LoadRange, IDX_LoadRangeFieldname, INDEXFLD_T_NUMERIC,
                      NewNumericRangeTree(), nums, NumericRangeTree *)

DECLARE_IDXACC_COMMON(IDX_LoadTags, IDX_LoadTagsFieldname, INDEXFLD_T_TAG, NewTagIndex(), tags,
                      TagIndex *)

DECLARE_IDXACC_COMMON(IDX_LoadGeo, IDX_LoadGeoFieldname, INDEXFLD_T_GEO, GeoIndex_Create(sp->name),
                      geos, GeoIndex *)

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
  if (sampleSize > raxSize(sp->termsIdx)) {
    sampleSize = raxSize(sp->termsIdx);
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

void IndexSpec_FreeWithKey(IndexSpec *sp, RedisModuleCtx *ctx) {
  IndexSpec_Free(sp);
}

static void IndexSpec_Unregister(IndexSpec *spec) {
  freeLegacyParams(spec);

  if (spec->flags & Index_UseRules) {
    SchemaRules_UnregisterIndex(spec);
  }

  if (spec->gc) {
    GCContext_Stop(spec->gc);
    spec->gc = NULL;
  }

  if (spec->timer) {
    RedisModule_StopTimer(RSDummyContext, spec->timer, NULL);
    spec->timer = 0;
  }

  IndexSpec_ClearAliases(spec);

  // This block relies on name-based lookups, in this case we should
  // ensure that our index really exists, and is not an errored index because
  // of someone trying to create a new index that already exists
  if (spec->state & IDX_S_REGISTERED) {
    Cursors_PurgeWithName(&RSCursors, spec->name);
    CursorList_RemoveSpec(&RSCursors, spec->name);
    dictDelete(RSIndexes_g, spec->name);
    // Remove the geo key
    for (size_t ii = 0; ii < spec->numFields; ++ii) {
      if (FIELD_IS(spec->fields + ii, INDEXFLD_T_GEO)) {
        GeoIndex_RemoveKey(RSDummyContext, spec->geos[ii]);
      }
    }
    spec->state &= ~IDX_S_REGISTERED;
  }
}

static void IndexSpec_FreeInternals(IndexSpec *spec) {
  if (spec->terms) {
    TrieType_Free(spec->terms);
  }
  DocTable_Free(&spec->docs);

  IndexSpec_Unregister(spec);

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

  if (spec->fields != NULL) {
    for (size_t i = 0; i < spec->numFields; i++) {
      rm_free(spec->fields[i].name);
    }
    rm_free(spec->fields);
  }
  for (size_t ii = 0; ii < spec->numFields; ++ii) {
    if (spec->nums[ii]) {
      NumericRangeTree_Free(spec->nums[ii]);
    }
    if (spec->geos[ii]) {
      GeoIndex_Free(spec->geos[ii]);
    }
    if (spec->tags[ii]) {
      TagIndex_Free(spec->tags[ii]);
    }
  }
  array_free(spec->nums);
  array_free(spec->tags);
  array_free(spec->geos);

  // Free the term indexes
  raxIterator it = {0};
  raxStart(&it, spec->termsIdx);
  unsigned char e = 0;
  raxSeek(&it, "^", &e, 0);
  while (raxNext(&it)) {
    InvertedIndex_Free(it.data);
  }

  raxFree(spec->termsIdx);

  SpecDocQueue_Free(spec->queue);

  if (spec->cachedInvidx) {
    InvertedIndex_Free(spec->cachedInvidx);
  }
  pthread_rwlock_destroy(&spec->idxlock);
  rm_free(spec);
}

static void IndexSpec_FreeAsync(void *data) {
  IndexSpec *spec = data;
  RedisModuleCtx *threadCtx = RedisModule_GetThreadSafeContext(NULL);
  RedisModule_AutoMemory(threadCtx);
  RedisModule_ThreadSafeContextLock(threadCtx);

  IndexSpec_FreeInternals(spec);

  RedisModule_ThreadSafeContextUnlock(threadCtx);
  RedisModule_FreeThreadSafeContext(threadCtx);
}

static struct thpool_ *cleanPool = NULL;

size_t IndexSpec_Decref(IndexSpec *spec) {
  size_t n = --spec->refcount;
  if (n) {
    return n;
  }

  if (spec->flags & Index_Temporary) {
    if (!cleanPool) {
      cleanPool = thpool_init(1);
    }
    thpool_add_work(cleanPool, IndexSpec_FreeAsync, spec);
    return 0;
  }

  IndexSpec_FreeInternals(spec);
  return 0;
}

void IndexSpec_Free(void *ctx) {
  IndexSpec_FreeEx(ctx, 0);
}

static void deleteRedisKey(RedisModuleCtx *ctx, RSDocumentMetadata *dmd) {
  RedisModuleString *s = RedisModule_CreateString(ctx, dmd->keyPtr, sdslen(dmd->keyPtr));
  RedisModuleKey *k = RedisModule_OpenKey(ctx, s, REDISMODULE_WRITE);
  if (k != NULL) {
    RedisModule_DeleteKey(k);
    RedisModule_CloseKey(k);
  }
  RedisModule_FreeString(ctx, s);
}

void IndexSpec_FreeEx(IndexSpec *ctx, int options) {
  IndexSpec *sp = ctx;
  sp->state |= IDX_S_DELETED;
  if (!(sp->state & IDX_S_REGISTERED)) {
    IndexSpec_Decref(ctx);
    return;
  }

  if ((options & IDXFREE_F_DELDOCS) && !(sp->flags & Index_UseRules)) {
    DOCTABLE_FOREACH((&sp->docs), deleteRedisKey(RSDummyContext, dmd));
  }

  IndexSpec_Unregister(sp);

  // Let's delete ourselves from the global index list as well
  IndexSpec_Decref(ctx);
}

dict *RSIndexes_g = NULL;

IndexSpec *IndexSpec_LoadEx(void *unused, IndexLoadOptions *options) {
  IndexSpec *ret = NULL;

  const char *ixname = NULL;
  if (options->flags & INDEXSPEC_LOAD_KEY_RSTRING) {
    ixname = RedisModule_StringPtrLen(options->name.rstring, NULL);
  } else {
    ixname = options->name.cstring;
  }
  dictEntry *ent = dictFind(RSIndexes_g, ixname);
  if (!ent) {
    if ((options->flags & INDEXSPEC_LOAD_NOALIAS) || ixname == NULL) {
      return NULL;
    }
    IndexSpec *aliasTarget = ret = IndexAlias_Get(ixname);
  } else {
    ret = ent->v.val;
  }

  if (ret && !(options->flags & INDEXSPEC_LOAD_NOTOUCH)) {
    resetTimer(ret);
  }

  return ret;
}

/* Load the spec from the saved version */
IndexSpec *IndexSpec_Load(void *unused, const char *name, int openWrite) {
  IndexLoadOptions lopts = {.flags = openWrite ? INDEXSPEC_LOAD_WRITEABLE : 0,
                            .name = {.cstring = name}};
  return IndexSpec_LoadEx(NULL, &lopts);
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

int IndexSpec_IsStopWord(IndexSpec *sp, const char *term, size_t len) {
  if (!sp->stopwords) {
    return 0;
  }
  return StopWordList_Contains(sp->stopwords, term, len);
}

int IDX_IsAlive(const IndexSpec *sp) {
  return !(sp->state & IDX_S_DELETED);
}
int IDX_YieldWrite(IndexSpec *sp) {
  return 1;
}
int IDX_YieldRead(IndexSpec *sp) {
  return 1;
}

IndexSpec *IDX_CreateEmpty(void) {
  IndexSpec *sp = rm_calloc(1, sizeof(*sp));
  sp->docs = DocTable_New(100);
  sp->refcount = 1;

  sp->flags = INDEX_DEFAULT_FLAGS;
  sp->maxPrefixExpansions = RSGlobalConfig.maxPrefixExpansions;
  sp->minPrefix = RSGlobalConfig.minTermPrefix;

  sp->fields = rm_calloc(sizeof(FieldSpec), SPEC_MAX_FIELDS);
  sp->nums = array_new(NumericRangeTree *, 8);
  sp->tags = array_new(TagIndex *, 8);
  sp->geos = array_new(GeoIndex *, 8);

  sp->termsIdx = raxNew();
  sp->terms = NewTrie();
  sp->queue = SpecDocQueue_Create(sp);
  sp->sortables = NewSortingTable();
  sp->stopwords = DefaultStopWordList();
  pthread_rwlock_init(&sp->idxlock, NULL);
  return sp;
}

IndexSpec *NewIndexSpec(const char *name) {
  IndexSpec *sp = IDX_CreateEmpty();
  sp->name = rm_strdup(name);
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

  // Once everything is done, extend the list of geo/num/tag
  if (array_len(sp->tags) < sp->numFields) {
    *array_ensure_tail(&sp->geos, GeoIndex *) = NULL;
    *array_ensure_tail(&sp->tags, TagIndex *) = NULL;
    *array_ensure_tail(&sp->nums, NumericRangeTree *) = NULL;
  }
  return fs;
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
    sp->gc = GCContext_CreateGC(sp, initialHZ, sp->uniqueId);
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

void *IndexSpec_RdbLoad(RedisModuleIO *rdb, int encver) {
  if (encver < INDEX_MIN_COMPAT_VERSION) {
    return NULL;
  }

  RedisModuleCtx *ctx = RedisModule_GetContextFromIO(rdb);
  IndexSpec *sp = IDX_CreateEmpty();
  sp->name = RedisModule_LoadStringBuffer(rdb, NULL);
  sp->flags = (IndexFlags)RedisModule_LoadUnsigned(rdb);

  if (encver < INDEX_MIN_NOFREQ_VERSION) {
    sp->flags |= Index_StoreFreqs;
  }

  sp->numFields = RedisModule_LoadUnsigned(rdb);
  assert(sp->numFields < SPEC_MAX_FIELDS);
  sp->nums = array_ensure_len(sp->nums, sp->numFields);
  sp->tags = array_ensure_len(sp->tags, sp->numFields);
  sp->geos = array_ensure_len(sp->geos, sp->numFields);
  for (size_t ii = 0; ii < sp->numFields; ++ii) {
    sp->nums[ii] = NULL;
    sp->tags[ii] = NULL;
    sp->geos[ii] = NULL;
  }

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

  // TODO: maybe just call _Register()?

  if (sp->flags & Index_HasCustomStopwords) {
    sp->stopwords = StopWordList_RdbLoad(rdb, encver);
  }

  sp->uniqueId = spec_unique_ids++;

  IndexSpec_StartGC(ctx, sp, GC_DEFAULT_HZ);
  CursorList_AddSpec(&RSCursors, sp->name, RSCURSORS_DEFAULT_CAPACITY);

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
      RS_LOG_ASSERT(rc == REDISMODULE_OK, "adding alias to index failed");
    }
  }
  if (sp->flags & Index_UseRules) {
    SchemaRules_RegisterIndex(sp);
  }
  sp->state |= IDX_S_REGISTERED;
  return sp;
}

void IndexSpec_RdbSave(RedisModuleIO *rdb, void *value) {

  IndexSpec *sp = value;
  // we save the name plus the null terminator
  RedisModule_SaveStringBuffer(rdb, sp->name, strlen(sp->name) + 1);
  RedisModule_SaveUnsigned(rdb, (uint)sp->flags);

  RedisModule_SaveUnsigned(rdb, sp->numFields);
  for (int i = 0; i < sp->numFields; i++) {
    FieldSpec_RdbSave(rdb, &sp->fields[i]);
  }

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
  if ((sp->flags & Index_UseRules) == 0) {
    DocTable_RdbSave(&sp->docs, rdb);
  }
}

static void specAuxSave(RedisModuleIO *rdb, int when) {
  // Save the actual index, rules, etc. before RDB
  if (when == REDISMODULE_AUX_BEFORE_RDB) {
    return;
  }
  // handle all this _after_ rdb...
  RedisModule_SaveUnsigned(rdb, dictSize(RSIndexes_g));
  dictIterator *it = dictGetIterator(RSIndexes_g);
  dictEntry *e = NULL;
  while ((e = dictNext(it))) {
    IndexSpec_RdbSave(rdb, e->v.val);
  }
  dictReleaseIterator(it);
  SchemaRules_Save(rdb, when);
}

typedef struct SpecLegacyInfo {
  SchemaCustomRule *rule;
  DocTable docs;
  IndexSpec *spec;
} SpecLegacyInfo;

static int customRuleCb(RedisModuleCtx *ctx, RuleKeyItem *item, void *arg, SchemaCustomCtx *cc) {
  SpecLegacyInfo *lrp = arg;
  RSDocumentMetadata *dmd = DocTable_GetByKeyR(&lrp->docs, item->kstr);
  if (!dmd) {
    return 0;
  }
  IndexItemAttrs attrs = {0};
  if (dmd->score) {
    attrs.score = dmd->score;
    attrs.predefMask |= SCATTR_TYPE_SCORE;
  }
  if (dmd->payload) {
    attrs.predefMask |= SCATTR_TYPE_PAYLOAD;
    attrs.payload = RedisModule_CreateString(RSDummyContext, dmd->payload->data, dmd->payload->len);
  }
  SchemaCustomCtx_Index(cc, lrp->spec, &attrs);
  return 1;
}

static SpecLegacyInfo *createLegacyInfo(IndexSpec *parent) {
  SpecLegacyInfo *sli = rm_calloc(1, sizeof(*sli));
  sli->docs = DocTable_New(100);
  sli->spec = parent;
  sli->rule = SchemaRules_AddCustomRule(customRuleCb, sli, SCHEMA_CUSTOM_LAST);
  return sli;
}

static void freeLegacyParams(IndexSpec *sp) {
  if (!sp->legacy) {
    return;
  }
  SpecLegacyInfo *sli = sp->legacy;
  SchemaRules_RemoveCustomRule(sli->rule);
  DocTable_Free(&sli->docs);
  rm_free(sli);
  sp->legacy = NULL;
}

static int specAuxLoad(RedisModuleIO *rdb, int encver, int when) {
  if (when == REDISMODULE_AUX_BEFORE_RDB) {
    return REDISMODULE_OK;
  }

  size_t n = RedisModule_LoadUnsigned(rdb);
  for (size_t ii = 0; ii < n; ++ii) {
    IndexSpec *sp = IndexSpec_RdbLoad(rdb, encver);
    if (!sp) {
      return REDISMODULE_ERR;
    }
    prepareGeoIndexes(sp);
    dictAdd(RSIndexes_g, sp->name, sp);
    if (!(sp->flags & Index_UseRules)) {
      sp->legacy = createLegacyInfo(sp);
      DocTable_RdbLoad(&sp->legacy->docs, rdb, encver);
    }
  }
  return SchemaRules_Load(rdb, encver, when);
}

void Indexes_OnScanDone(uint64_t revision) {
  dictIterator *it = dictGetIterator(RSIndexes_g);
  dictEntry *e;
  while ((e = dictNext(it))) {
    IndexSpec *sp = e->v.val;
    if (revision >= sp->minRulesVersion) {
      sp->state &= ~IDX_S_SCANNING;
    }
    freeLegacyParams(sp);
  }
  dictReleaseIterator(it);
}

int IndexSpec_RegisterType(RedisModuleCtx *ctx) {
  RedisModuleTypeMethods tm = {
      .version = 2,
      .aux_save = specAuxSave,
      .aux_load = specAuxLoad,
      .aux_save_triggers = REDISMODULE_AUX_AFTER_RDB | REDISMODULE_AUX_BEFORE_RDB};

  RedisModuleType *t = RedisModule_CreateDataType(ctx, "ft_index2", INDEX_CURRENT_VERSION, &tm);
  if (t == NULL) {
    RedisModule_Log(ctx, "error", "Could not create index spec type");
    return REDISMODULE_ERR;
  }

  return REDISMODULE_OK;
}

void IndexSpec_CleanAll(void) {
  dictIterator *it = dictGetSafeIterator(RSIndexes_g);
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
}

void Indexes_Init(RedisModuleCtx *ctx) {
  RSIndexes_g = dictCreate(&dictTypeHeapStrings, NULL);
  RedisModule_SubscribeToServerEvent(ctx, RedisModuleEvent_FlushDB, onFlush);
}