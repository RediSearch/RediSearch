#include "rmutil/strings.h"
#include "rmutil/util.h"
#include "spec.h"
#include "util/logging.h"
#include "util/misc.h"
#include "rmutil/vector.h"
#include "trie/trie_type.h"
#include <math.h>
#include <ctype.h>
#include <assert.h>
#include "rmalloc.h"
#include "config.h"
#include "cursor.h"
#include "tag_index.h"

void (*IndexSpec_OnCreate)(const IndexSpec *) = NULL;

RedisModuleType *IndexSpecType;
uint64_t spec_unique_ids = 0;

/*
 * Get a field spec by field name. Case insensitive!
 * Return the field spec if found, NULL if not
 */
FieldSpec *IndexSpec_GetField(IndexSpec *spec, const char *name, size_t len) {
  for (int i = 0; i < spec->numFields; i++) {
    if (len != strlen(spec->fields[i].name)) continue;
    if (!strncasecmp(spec->fields[i].name, name, len)) {
      return &spec->fields[i];
    }
  }

  return NULL;
};

t_fieldMask IndexSpec_GetFieldBit(IndexSpec *spec, const char *name, size_t len) {
  FieldSpec *sp = IndexSpec_GetField(spec, name, len);
  if (!sp || sp->type != FIELD_FULLTEXT || !FieldSpec_IsIndexable(sp)) return 0;

  return FIELD_BIT(sp);
}

int IndexSpec_GetFieldSortingIndex(IndexSpec *sp, const char *name, size_t len) {
  if (!sp->sortables) return -1;
  return RSSortingTable_GetFieldIdx(sp->sortables, name);
}

char *GetFieldNameByBit(IndexSpec *sp, t_fieldMask id) {
  for (int i = 0; i < sp->numFields; i++) {
    if (FIELD_BIT(&sp->fields[i]) == id && sp->fields[i].type == FIELD_FULLTEXT &&
        FieldSpec_IsIndexable(&sp->fields[i])) {
      return sp->fields[i].name;
    }
  }
  return NULL;
}

/*
* Parse an index spec from redis command arguments.
* Returns REDISMODULE_ERR if there's a parsing error.
* The command only receives the relevant part of argv.
*
* The format currently is FT.CREATE {index} [NOOFFSETS] [NOFIELDS] [NOFREQS]
    SCHEMA {field} [TEXT [WEIGHT {weight}]] | [NUMERIC]
*/
IndexSpec *IndexSpec_ParseRedisArgs(RedisModuleCtx *ctx, RedisModuleString *name,
                                    RedisModuleString **argv, int argc, char **err) {

  const char *args[argc];
  for (int i = 0; i < argc; i++) {
    args[i] = RedisModule_StringPtrLen(argv[i], NULL);
  }

  IndexSpec *ret = IndexSpec_Parse(RedisModule_StringPtrLen(name, NULL), args, argc, err);

  return ret;
}

FieldSpec **getFieldsByType(IndexSpec *spec, FieldType type) {
#define FIELDS_ARRAY_CAP 2
  FieldSpec **fields = array_new(FieldSpec *, FIELDS_ARRAY_CAP);
  for (int i = 0; i < spec->numFields; ++i) {
    if (spec->fields[i].type == type) {
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

IndexSpec *IndexSpec_CreateNew(RedisModuleCtx *ctx, RedisModuleString **argv, int argc,
                               char **err) {
  IndexSpec *sp = IndexSpec_ParseRedisArgs(ctx, argv[1], &argv[2], argc - 2, err);
  if (sp == NULL) {
    SET_ERR(err, "Could not parse index spec");
    return NULL;
  }

  RedisModuleString *keyString = RedisModule_CreateStringPrintf(ctx, INDEX_SPEC_KEY_FMT, sp->name);
  RedisModuleKey *k = RedisModule_OpenKey(ctx, keyString, REDISMODULE_READ | REDISMODULE_WRITE);

  // check that the key is empty
  if (k == NULL || (RedisModule_KeyType(k) != REDISMODULE_KEYTYPE_EMPTY)) {
    if (RedisModule_ModuleTypeGetType(k) != IndexSpecType) {
      SET_ERR(err, "Wrong type for index key");
    } else {
      SET_ERR(err, "Index already exists. Drop it first!");
    }
    IndexSpec_Free(sp);
    return NULL;
  }

  sp->uniqueId = spec_unique_ids++;
  // Start the garbage collector
  IndexSpec_StartGC(ctx, sp, GC_DEFAULT_HZ);

  CursorList_AddSpec(&RSCursors, sp->name, RSCURSORS_DEFAULT_CAPACITY);

  // set the value in redis
  RedisModule_ModuleTypeSetValue(k, IndexSpecType, sp);

  if (IndexSpec_OnCreate) {
    IndexSpec_OnCreate(sp);
  }

  return sp;
}

static int findOffset(const char *arg, const char **argv, int argc) {
  for (int i = 0; i < argc; i++) {
    if (!strcasecmp(arg, argv[i])) {
      return i;
    }
  }
  return -1;
}

static int argExists(const char *arg, const char **argv, int argc, int maxIdx) {
  int idx = findOffset(arg, argv, argc);
  // printf("pos for %s: %d\n", arg, idx);
  return idx >= 0 && idx < maxIdx;
}

char *strtolower(char *str) {
  char *p = str;
  while (*p) {
    *p = tolower(*p);
    p++;
  }
  return str;
}

static bool checkPhoneticAlgorithmAndLang(char *matcher) {
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

/* Parse a field definition from argv, at *offset. We advance offset as we progress.
 *  Returns 1 on successful parse, 0 otherwise */
static int parseFieldSpec(const char **argv, int *offset, int argc, FieldSpec *sp, char **err) {

  // if we're at the end - fail
  if (*offset >= argc) return 0;
  sp->sortIdx = -1;
  sp->options = 0;
  sp->name = rm_strdup(argv[*offset]);

  // we can't be at the end
  if (++*offset == argc) {
    goto error;
  }

  // this is a text field
  if (!strcasecmp(argv[*offset], SPEC_TEXT_STR)) {

    // init default weight and type
    sp->type = FIELD_FULLTEXT;
    sp->textOpts.weight = 1.0;

    while (++*offset < argc) {
      if (!strcasecmp(argv[*offset], SPEC_NOSTEM_STR)) {
        sp->options |= FieldSpec_NoStemming;

      } else if (!strcasecmp(argv[*offset], SPEC_WEIGHT_STR)) {
        // weight with no value is invalid
        if (++*offset == argc) {
          goto error;
        }
        // try and parse the weight
        double d = strtod(argv[*offset], NULL);
        if (d == 0 || d == HUGE_VAL || d == -HUGE_VAL || d < 0) {
          goto error;
        }
        sp->textOpts.weight = d;

      } else if (!strcasecmp(argv[*offset], SPEC_PHONETIC_STR)) {
        // phonetic with no matcher
        if (++*offset == argc) {
          return 0;
        }
        // try and parse the matcher
        char *matcher = strdup(argv[*offset]);
        // currently we just make sure algorithm is double metaphone (dm)
        // and language is one of the following : English (en), French (fr), Portuguese (pt) and
        // Spanish (es)
        // in the future we will support more algorithms and more languages
        if (!checkPhoneticAlgorithmAndLang(matcher)) {
          SET_ERR(err,
                  "Matcher Format: <2 chars algorithm>:<2 chars language>. Support algorithms: "
                  "double metaphone (dm). Supported languages: English (en), French (fr), "
                  "Portuguese (pt) and Spanish (es)");
          return 0;
        }

        sp->options |= FieldSpec_Phonetics;
      } else {
        break;
      }
    }

    if (*offset == argc) {
      return 1;
    }

  } else if (!strcasecmp(argv[*offset], NUMERIC_STR)) {
    sp->type = FIELD_NUMERIC;
    ++*offset;

  } else if (!strcasecmp(argv[*offset], GEO_STR)) {  // geo field
    sp->type = FIELD_GEO;
    ++*offset;
  } else if (!strcasecmp(argv[*offset], SPEC_TAG_STR)) {  // tag field
    sp->type = FIELD_TAG;
    sp->tagOpts.separator = ',';
    sp->tagOpts.flags = TAG_FIELD_DEFAULT_FLAGS;
    ++*offset;
    // Detectet SEPARATOR Argument
    if (*offset + 1 < argc && !strcasecmp(argv[*offset], SPEC_SEPARATOR_STR)) {
      ++*offset;
      if (strlen(argv[*offset]) == 1) {
        sp->tagOpts.separator = argv[*offset][0];
      } else {
        SET_ERR(err, "Invalid separator, only 1 byte ascii characters allowed");
        goto error;
      }
      ++*offset;
    }
  } else {  // not numeric and not text - nothing more supported currently
    goto error;
  }

  while (*offset < argc) {
    if (!strcasecmp(argv[*offset], SPEC_SORTABLE_STR)) {
      // cannot sort by geo fields
      if (sp->type == FIELD_GEO) {
        SET_ERR(err, "Geo fields cannot be sortable");
        goto error;
      }
      sp->options |= FieldSpec_Sortable;
      ++*offset;
    } else if (!strcasecmp(argv[*offset], SPEC_NOINDEX_STR)) {
      sp->options |= FieldSpec_NotIndexable;
      ++*offset;
    } else {
      break;
    }
  }

  return 1;

error:
  if (sp->name) {
    rm_free(sp->name);
    sp->name = NULL;
  }
  return 0;
}

/* Convert field type rsvalue type */
RSValueType fieldTypeToValueType(FieldType ft) {
  switch (ft) {
    case FIELD_NUMERIC:
      return RSValue_Number;
    case FIELD_FULLTEXT:
    case FIELD_TAG:
      return RSValue_String;
    case FIELD_GEO:
    default:
      // geo is not sortable so we don't care as of now...
      return RSValue_Null;
  }
}

/**
 * Add fields to an existing (or newly created) index. If the addition fails,
 *
 */
static int IndexSpec_AddFieldsInternal(IndexSpec *sp, const char **argv, int argc, char **err,
                                       int isNew) {

  const size_t prevNumFields = sp->numFields;
  const size_t prevSortLen = sp->sortables->len;

  int textId = -1;

  for (size_t ii = 0; ii < sp->numFields; ++ii) {
    const FieldSpec *fs = sp->fields + ii;
    if (fs->type == FIELD_FULLTEXT) {
      textId = MAX(textId, fs->textOpts.id);
    }
  }

  for (int offset = 0; offset < argc && sp->numFields < SPEC_MAX_FIELDS;) {
    sp->fields = rm_realloc(sp->fields, sizeof(*sp->fields) * (sp->numFields + 1));
    FieldSpec *fs = sp->fields + sp->numFields;
    memset(fs, 0, sizeof(*fs));

    fs->index = sp->numFields;

    if (!parseFieldSpec(argv, &offset, argc, fs, err)) {
      SET_ERR(err, "Could not parse field spec");
      goto reset;
    }

    if (fs->type == FIELD_FULLTEXT && FieldSpec_IsIndexable(fs)) {
      // make sure we don't have too many indexable fields
      textId++;  // Explicit
      if (textId == SPEC_MAX_FIELD_ID) {
        SET_ERR(err, "Too many TEXT fields in schema");
        goto reset;
      }

      // If we need to store field flags and we have over 32 fields, we need to switch to wide
      // schema encoding
      if (textId >= SPEC_WIDEFIELD_THRESHOLD && (sp->flags & Index_StoreFieldFlags)) {
        if (isNew) {
          sp->flags |= Index_WideSchema;
        } else if ((sp->flags & Index_WideSchema) == 0) {
          SET_ERR(err,
                  "Cannot add more fields. Declare index with wide fields to allow adding "
                  "unlimited fields");
          goto reset;
        }
      }
      fs->textOpts.id = textId;
    }

    if (IndexSpec_GetField(sp, fs->name, strlen(fs->name))) {
      SET_ERR(err, "Duplicate field in schema");
      goto reset;
    }

    if (FieldSpec_IsSortable(fs)) {
      fs->sortIdx = RSSortingTable_Add(sp->sortables, fs->name, fieldTypeToValueType(fs->type));
    }
    sp->numFields++;
  }
  return 1;

reset:
  sp->numFields = prevNumFields;
  sp->sortables->len = prevSortLen;
  return 0;
}

int IndexSpec_AddFields(IndexSpec *sp, const char **argv, int argc, char **err) {
  return IndexSpec_AddFieldsInternal(sp, argv, argc, err, 0);
}

int IndexSpec_AddFieldsRedisArgs(IndexSpec *sp, RedisModuleString **argv, int argc, char **err) {
  const char *args[argc];
  for (int i = 0; i < argc; i++) {
    args[i] = RedisModule_StringPtrLen(argv[i], NULL);
  }
  return IndexSpec_AddFields(sp, args, argc, err);
}

/* The format currently is FT.CREATE {index} [NOOFFSETS] [NOFIELDS]
    SCHEMA {field} [TEXT [WEIGHT {weight}]] | [NUMERIC]
  */
IndexSpec *IndexSpec_Parse(const char *name, const char **argv, int argc, char **err) {
  *err = NULL;
  int schemaOffset = findOffset(SPEC_SCHEMA_STR, argv, argc);
  // no schema or schema towrards the end
  if (schemaOffset == -1) {
    SET_ERR(err, "schema not found");
    return NULL;
  }
  IndexSpec *spec = NewIndexSpec(name, 0);

  if (argExists(SPEC_NOOFFSETS_STR, argv, argc, schemaOffset)) {
    spec->flags &= ~(Index_StoreTermOffsets | Index_StoreByteOffsets);
  }

  if (argExists(SPEC_NOHL_STR, argv, argc, schemaOffset)) {
    spec->flags &= ~Index_StoreByteOffsets;
  }

  if (argExists(SPEC_NOFIELDS_STR, argv, argc, schemaOffset)) {
    spec->flags &= ~Index_StoreFieldFlags;
  }

  if (argExists(SPEC_NOFREQS_STR, argv, argc, schemaOffset)) {
    spec->flags &= ~Index_StoreFreqs;
  }

  if (argExists(SPEC_SCHEMA_EXPANDABLE_STR, argv, argc, schemaOffset)) {
    spec->flags |= Index_WideSchema;
  }

  int swIndex = findOffset(SPEC_STOPWORDS_STR, argv, argc);
  if (swIndex >= 0 && swIndex + 1 < schemaOffset) {
    int listSize = atoi(argv[swIndex + 1]);
    if (listSize < 0 || (swIndex + 2 + listSize > schemaOffset)) {
      SET_ERR(err, "Invalid stopword list size");
      goto failure;
    }
    spec->stopwords = NewStopWordListCStr(&argv[swIndex + 2], listSize);
    spec->flags |= Index_HasCustomStopwords;
  } else {
    spec->stopwords = DefaultStopWordList();
  }
  schemaOffset++;
  if (!IndexSpec_AddFieldsInternal(spec, argv + schemaOffset, argc - schemaOffset, err, 1)) {
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
      free(samples[i]);
    }
  }
  // printf("Selected %s --> %f\n", samples[selection], weights[selection]);
  return samples[selection];
}

void IndexSpec_Free(void *ctx) {
  IndexSpec *spec = ctx;

  if (spec->gc.gcCtx) {
    spec->gc.stop(spec->gc.gcCtx);
  }

  if (spec->terms) {
    TrieType_Free(spec->terms);
  }
  DocTable_Free(&spec->docs);
  if (spec->fields != NULL) {
    for (int i = 0; i < spec->numFields; i++) {
      rm_free(spec->fields[i].name);
    }
    rm_free(spec->fields);
  }

  Cursors_PurgeWithName(&RSCursors, spec->name);

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

  if (spec->indexStrs) {
    for (size_t ii = 0; ii < spec->numFields; ++ii) {
      if (spec->indexStrs[ii]) {
        RedisModule_FreeString(spec->strCtx, spec->indexStrs[ii]);
      }
    }
    rm_free(spec->indexStrs);
    RedisModule_FreeThreadSafeContext(spec->strCtx);
  }
  rm_free(spec);
}

IndexSpec *IndexSpec_LoadEx(RedisModuleCtx *ctx, RedisModuleString *formattedKey, int openWrite,
                            RedisModuleKey **keyp) {
  RedisModuleKey *key_s = NULL;
  if (!keyp) {
    keyp = &key_s;
  }

  *keyp = RedisModule_OpenKey(ctx, formattedKey,
                              REDISMODULE_READ | (openWrite ? REDISMODULE_WRITE : 0));

  // we do not allow empty indexes when loading an existing index
  if (*keyp == NULL || RedisModule_KeyType(*keyp) == REDISMODULE_KEYTYPE_EMPTY ||
      RedisModule_ModuleTypeGetType(*keyp) != IndexSpecType) {
    return NULL;
  }

  IndexSpec *ret = RedisModule_ModuleTypeGetValue(*keyp);
  return ret;
}

/* Load the spec from the saved version */
IndexSpec *IndexSpec_Load(RedisModuleCtx *ctx, const char *name, int openWrite) {
  RedisModuleString *s = RedisModule_CreateStringPrintf(ctx, INDEX_SPEC_KEY_FMT, name);
  return IndexSpec_LoadEx(ctx, s, openWrite, NULL);
}

RedisModuleString *IndexSpec_GetFormattedKey(IndexSpec *sp, const FieldSpec *fs) {
  if (!sp->indexStrs) {
    sp->indexStrs = rm_calloc(SPEC_MAX_FIELDS, sizeof(*sp->indexStrs));
    sp->strCtx = RedisModule_GetThreadSafeContext(NULL);
  }
  RedisModuleString *ret = sp->indexStrs[fs->index];
  if (!ret) {
    RedisSearchCtx sctx = {.redisCtx = sp->strCtx, .spec = sp};
    if (fs->type == FIELD_NUMERIC) {
      ret = fmtRedisNumericIndexKey(&sctx, fs->name);
    } else if (fs->type == FIELD_TAG) {
      ret = TagIndex_FormatName(&sctx, fs->name);
    } else {
      // Unknown
      ret = NULL;
    }
  }
  if (!ret) {
    return NULL;
  }
  sp->indexStrs[fs->index] = ret;
  return ret;
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

IndexSpec *NewIndexSpec(const char *name, size_t numFields) {
  IndexSpec *sp = rm_calloc(1, sizeof(IndexSpec));
  sp->fields = rm_calloc(sizeof(FieldSpec), numFields ? numFields : SPEC_MAX_FIELDS);
  sp->sortables = NewSortingTable();
  sp->flags = INDEX_DEFAULT_FLAGS;
  sp->name = rm_strdup(name);
  sp->docs = DocTable_New(100);
  sp->stopwords = DefaultStopWordList();
  sp->terms = NewTrie();
  memset(&sp->stats, 0, sizeof(sp->stats));
  return sp;
}

static GCContext IndexSpec_CreateGarbageCollection(RedisModuleString *keyName, float initialHZ, uint64_t uniqueId){
  switch (RSGlobalConfig.gcPolicy) {
    case GCPolicy_Fork:
      return NewForkGC(keyName, uniqueId);
      break;
    case GCPolicy_Default:
    default:
      return NewGarbageCollector(keyName, initialHZ, uniqueId);
      break;
  }
}

/* Start the garbage collection loop on the index spec. The GC removes garbage data left on the
 * index after removing documents */
void IndexSpec_StartGC(RedisModuleCtx *ctx, IndexSpec *sp, float initialHZ) {
  assert(sp->gc.gcCtx == NULL);
  if (RSGlobalConfig.enableGC) {
    RedisModuleString *keyName = RedisModule_CreateString(ctx, sp->name, strlen(sp->name));
    RedisModule_RetainString(ctx, keyName);
    sp->gc = IndexSpec_CreateGarbageCollection(keyName, initialHZ, sp->uniqueId);
    sp->gc.start(sp->gc.gcCtx);
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
  // the old versions encoded the bit id of the field directly
  // we convert that to a power of 2
  if (encver < INDEX_MIN_WIDESCHEMA_VERSION) {
    f->textOpts.id = bit(RedisModule_LoadUnsigned(rdb));
  } else {
    // the new version encodes just the power of 2 of the bit
    f->textOpts.id = RedisModule_LoadUnsigned(rdb);
  }
  f->type = RedisModule_LoadUnsigned(rdb);
  f->textOpts.weight = RedisModule_LoadDouble(rdb);
  f->tagOpts.flags = TAG_FIELD_DEFAULT_FLAGS;
  f->tagOpts.separator = ',';
  if (encver >= 4) {
    f->options = RedisModule_LoadUnsigned(rdb);
    f->sortIdx = RedisModule_LoadSigned(rdb);
  }
}

static void FieldSpec_RdbSave(RedisModuleIO *rdb, FieldSpec *f) {
  RedisModule_SaveStringBuffer(rdb, f->name, strlen(f->name) + 1);
  RedisModule_SaveUnsigned(rdb, f->type);
  RedisModule_SaveUnsigned(rdb, f->options);
  RedisModule_SaveSigned(rdb, f->sortIdx);
  // Save text specific options
  if (f->type == FIELD_FULLTEXT) {
    RedisModule_SaveUnsigned(rdb, f->textOpts.id);
    RedisModule_SaveDouble(rdb, f->textOpts.weight);
  } else if (f->type == FIELD_TAG) {
    RedisModule_SaveUnsigned(rdb, f->tagOpts.flags);
    RedisModule_SaveStringBuffer(rdb, &f->tagOpts.separator, 1);
  }
}

static void FieldSpec_RdbLoad(RedisModuleIO *rdb, FieldSpec *f, int encver) {

  // Fall back to legacy encoding if needed
  if (encver < INDEX_MIN_TAGFIELD_VERSION) {
    return FieldSpec_RdbLoadCompat8(rdb, f, encver);
  }

  f->name = RedisModule_LoadStringBuffer(rdb, NULL);
  f->type = RedisModule_LoadUnsigned(rdb);
  f->options = RedisModule_LoadUnsigned(rdb);
  f->sortIdx = RedisModule_LoadSigned(rdb);

  // Load text specific options
  if (f->type == FIELD_FULLTEXT) {
    f->textOpts.id = RedisModule_LoadUnsigned(rdb);
    f->textOpts.weight = RedisModule_LoadDouble(rdb);
  }
  // Load tag specific options
  if (f->type == FIELD_TAG) {
    f->tagOpts.flags = RedisModule_LoadUnsigned(rdb);
    // Load the separator
    size_t l;
    char *s = RedisModule_LoadStringBuffer(rdb, &l);
    assert(l == 1);

    f->tagOpts.separator = *s;
    rm_free(s);
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
  IndexSpec *sp = rm_calloc(1, sizeof(IndexSpec));
  sp->sortables = NewSortingTable();
  sp->terms = NULL;
  sp->docs = DocTable_New(1000);
  sp->name = RedisModule_LoadStringBuffer(rdb, NULL);
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
      assert(fs->sortIdx < RS_SORTABLES_MAX);
      sp->sortables->fields[fs->sortIdx].name = fs->name;
      sp->sortables->fields[fs->sortIdx].type = fieldTypeToValueType(fs->type);
      sp->sortables->len = MAX(sp->sortables->len, fs->sortIdx + 1);
    }
  }

  IndexStats_RdbLoad(rdb, &sp->stats);

  DocTable_RdbLoad(&sp->docs, rdb, encver);
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

  IndexStats_RdbSave(rdb, &sp->stats);
  DocTable_RdbSave(&sp->docs, rdb);
  // save trie of terms
  TrieType_GenericSave(rdb, sp->terms, 0);

  // If we have custom stopwords, save them
  if (sp->flags & Index_HasCustomStopwords) {
    StopWordList_RdbSave(rdb, sp->stopwords);
  }

  if (sp->flags & Index_HasSmap) {
    SynonymMap_RdbSave(rdb, sp->smap);
  }
}

void IndexSpec_Digest(RedisModuleDigest *digest, void *value) {
}

int IndexSpec_RegisterType(RedisModuleCtx *ctx) {
  RedisModuleTypeMethods tm = {.version = REDISMODULE_TYPE_METHOD_VERSION,
                               .rdb_load = IndexSpec_RdbLoad,
                               .rdb_save = IndexSpec_RdbSave,
                               .aof_rewrite = GenericAofRewrite_DisabledHandler,
                               .free = IndexSpec_Free};

  IndexSpecType = RedisModule_CreateDataType(ctx, "ft_index0", INDEX_CURRENT_VERSION, &tm);
  if (IndexSpecType == NULL) {
    RedisModule_Log(ctx, "error", "Could not create index spec type");
    return REDISMODULE_ERR;
  }

  return REDISMODULE_OK;
}
