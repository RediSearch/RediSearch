#include "rmutil/strings.h"
#include "rmutil/util.h"
#include "spec.h"
#include "util/logging.h"
#include "rmutil/vector.h"
#include "trie/trie_type.h"
#include <math.h>
#include <ctype.h>
#include <assert.h>
#include "rmalloc.h"
#include "config.h"

RedisModuleType *IndexSpecType;

/*
* Get a field spec by field name. Case insensitive!
* Return the field spec if found, NULL if not
*/
inline FieldSpec *IndexSpec_GetField(IndexSpec *spec, const char *name, size_t len) {
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

IndexSpec *IndexSpec_CreateNew(RedisModuleCtx *ctx, RedisModuleString **argv, int argc,
                               char **err) {
  IndexSpec *sp = IndexSpec_ParseRedisArgs(ctx, argv[1], &argv[2], argc - 2, err);
  if (sp == NULL) {
    if (!*err) *err = "Could not parse index spec";
    return NULL;
  }

  RedisModuleKey *k =
      RedisModule_OpenKey(ctx, RedisModule_CreateStringPrintf(ctx, INDEX_SPEC_KEY_FMT, sp->name),
                          REDISMODULE_READ | REDISMODULE_WRITE);

  // check that the key is empty
  if (k == NULL || (RedisModule_KeyType(k) != REDISMODULE_KEYTYPE_EMPTY)) {
    if (RedisModule_ModuleTypeGetType(k) != IndexSpecType) {
      *err = "Wrong type for index key";
    } else {
      *err = "Index already exists. Drop it first!";
    }
    IndexSpec_Free(sp);
    return NULL;
  }

  // Start the garbage collector
  IndexSpec_StartGC(ctx, sp, GC_DEFAULT_HZ);

  // set the value in redis
  RedisModule_ModuleTypeSetValue(k, IndexSpecType, sp);

  return sp;
}

int __findOffset(const char *arg, const char **argv, int argc) {
  for (int i = 0; i < argc; i++) {
    if (!strcasecmp(arg, argv[i])) {
      return i;
    }
  }
  return -1;
}

int __argExists(const char *arg, const char **argv, int argc, int maxIdx) {
  int idx = __findOffset(arg, argv, argc);
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
/* Parse a field definition from argv, at *offset. We advance offset as we progress.
*  Returns 1 on successful parse, 0 otherwise */
int __parseFieldSpec(const char **argv, int *offset, int argc, FieldSpec *sp, char **err) {

  // if we're at the end - fail
  if (*offset >= argc) return 0;
  sp->sortIdx = -1;
  sp->options = 0;
  // the field name comes here
  sp->name = rm_strdup(argv[*offset]);

  // we can't be at the end
  if (++*offset == argc) return 0;

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
          return 0;
        }
        // try and parse the weight
        double d = strtod(argv[*offset], NULL);
        if (d == 0 || d == HUGE_VAL || d == -HUGE_VAL || d < 0) {
          return 0;
        }
        sp->textOpts.weight = d;

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
        *err = "Invalid separator, only 1 byte ascii characters allowed";
        return 0;
      }
      ++*offset;
    }
  } else {  // not numeric and not text - nothing more supported currently
    return 0;
  }

  while (*offset < argc) {
    if (!strcasecmp(argv[*offset], SPEC_SORTABLE_STR)) {
      // cannot sort by geo fields
      if (sp->type == FIELD_GEO || sp->type == FIELD_TAG) {
        *err = "Tag and Geo fields cannot be sortable";
        return 0;
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
}

void _spec_buildSortingTable(IndexSpec *spec, int len) {
  spec->sortables = NewSortingTable(len);
  for (int i = 0; i < spec->numFields; i++) {
    if (FieldSpec_IsSortable(&spec->fields[i])) {
      // printf("Adding sortable field %s id %d\n", spec->fields[i].name, spec->fields[i].sortIdx);
      SortingTable_SetFieldName(spec->sortables, spec->fields[i].sortIdx, spec->fields[i].name);
    }
  }
}

/* The format currently is FT.CREATE {index} [NOOFFSETS] [NOFIELDS]
    SCHEMA {field} [TEXT [WEIGHT {weight}]] | [NUMERIC]
  */
IndexSpec *IndexSpec_Parse(const char *name, const char **argv, int argc, char **err) {
  *err = NULL;
  int schemaOffset = __findOffset(SPEC_SCHEMA_STR, argv, argc);
  // no schema or schema towrards the end
  if (schemaOffset == -1) {
    *err = "schema not found";
    return NULL;
  }
  IndexSpec *spec = NewIndexSpec(name, 0);

  if (__argExists(SPEC_NOOFFSETS_STR, argv, argc, schemaOffset)) {
    spec->flags &= ~(Index_StoreTermOffsets | Index_StoreByteOffsets);
  }

  if (__argExists(SPEC_NOHL_STR, argv, argc, schemaOffset)) {
    spec->flags &= ~Index_StoreByteOffsets;
  }

  if (__argExists(SPEC_NOFIELDS_STR, argv, argc, schemaOffset)) {
    spec->flags &= ~Index_StoreFieldFlags;
  }

  if (__argExists(SPEC_NOFREQS_STR, argv, argc, schemaOffset)) {
    spec->flags &= ~Index_StoreFreqs;
  }

  int swIndex = __findOffset(SPEC_STOPWORDS_STR, argv, argc);
  if (swIndex >= 0 && swIndex + 1 < schemaOffset) {
    int listSize = atoi(argv[swIndex + 1]);
    if (listSize < 0 || (swIndex + 2 + listSize > schemaOffset)) {
      *err = "Invalid stopword list size";
      goto failure;
    }
    spec->stopwords = NewStopWordListCStr(&argv[swIndex + 2], listSize);
    spec->flags |= Index_HasCustomStopwords;
  } else {
    spec->stopwords = DefaultStopWordList();
  }

  uint64_t id = 0;
  int sortIdx = 0;

  int i = schemaOffset + 1;
  while (i < argc && spec->numFields < SPEC_MAX_FIELDS) {

    FieldSpec *fs = &spec->fields[spec->numFields++];
    if (!__parseFieldSpec(argv, &i, argc, fs, err)) {
      if (!*err) {
        *err = "Could not parse field spec";
      }
      goto failure;
    }

    if (fs->type == FIELD_FULLTEXT && FieldSpec_IsIndexable(fs)) {
      // make sure we don't have too many indexable fields
      if (id == SPEC_MAX_FIELD_ID) {
        *err = "Too many TEXT fields in schema";
        goto failure;
      }

      // If we need to store field flags and we have over 32 fields, we need to switch to wide
      // schema encoding
      if (id == SPEC_WIDEFIELD_THRESHOLD && spec->flags & Index_StoreFieldFlags) {
        spec->flags |= Index_WideSchema;
      }

      fs->textOpts.id = id++;
    }
    if (FieldSpec_IsSortable(fs)) {
      fs->sortIdx = sortIdx++;
    }
    if (sortIdx > 255) {
      *err = "Too many sortable fields";

      goto failure;
    }
  }

  /* If we have sortable fields, create a sorting lookup table */
  if (sortIdx > 0) {
    _spec_buildSortingTable(spec, sortIdx);
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

void IndexSpec_RestoreTerm(IndexSpec *sp, const char *term, size_t len, double score) {
  Trie_InsertStringBuffer(sp->terms, (char *)term, len, score, 0, NULL);
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

/* Get a random term from the index spec using weighted random. Weighted random is done by sampling
 * N terms from the index and then doing weighted random on them. A sample size of 10-20 should be
 * enough. Returns NULL if the index is empty */
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

  if (spec->gc) {
    GC_Stop(spec->gc);
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

  rm_free(spec->name);
  if (spec->sortables) {
    SortingTable_Free(spec->sortables);
    spec->sortables = NULL;
  }
  if (spec->stopwords) {
    StopWordList_Unref(spec->stopwords);
    spec->stopwords = NULL;
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

t_fieldMask IndexSpec_ParseFieldMask(IndexSpec *sp, RedisModuleString **argv, int argc) {
  t_fieldMask ret = 0;

  for (int i = 0; i < argc; i++) {
    size_t len;
    const char *p = RedisModule_StringPtrLen(argv[i], &len);

    ret |= IndexSpec_GetFieldBit(sp, p, len);
  }

  return ret;
}

int IndexSpec_ParseStopWords(IndexSpec *sp, RedisModuleString **strs, size_t len) {
  // if the index already has custom stopwords, let us free them first
  if (sp->stopwords && sp->flags & Index_HasCustomStopwords) {
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
  IndexSpec *sp = rm_malloc(sizeof(IndexSpec));
  sp->fields = rm_calloc(sizeof(FieldSpec), numFields ? numFields : SPEC_MAX_FIELDS);
  sp->numFields = 0;
  sp->flags = INDEX_DEFAULT_FLAGS;
  sp->name = rm_strdup(name);
  sp->docs = NewDocTable(1000);
  sp->stopwords = DefaultStopWordList();
  sp->terms = NewTrie();
  sp->sortables = NULL;
  sp->gc = NULL;
  memset(&sp->stats, 0, sizeof(sp->stats));
  return sp;
}

/* Start the garbage collection loop on the index spec. The GC removes garbage data left on the
 * index after removing documents */
void IndexSpec_StartGC(RedisModuleCtx *ctx, IndexSpec *sp, float initialHZ) {
  assert(sp->gc == NULL);
  if (sp->gc == NULL && RSGlobalConfig.enableGC) {
    RedisModuleString *keyName = RedisModule_CreateString(ctx, sp->name, strlen(sp->name));
    RedisModule_RetainString(ctx, keyName);
    sp->gc = NewGarbageCollector(keyName, initialHZ);
    GC_Start(sp->gc);
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
void __fieldSpec_rdbLoadCompat8(RedisModuleIO *rdb, FieldSpec *f, int encver) {

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

void __fieldSpec_rdbSave(RedisModuleIO *rdb, FieldSpec *f) {
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

void __fieldSpec_rdbLoad(RedisModuleIO *rdb, FieldSpec *f, int encver) {

  // Fall back to legacy encoding if needed
  if (encver < INDEX_MIN_TAGFIELD_VERSION) {
    return __fieldSpec_rdbLoadCompat8(rdb, f, encver);
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

void __indexStats_rdbLoad(RedisModuleIO *rdb, IndexStats *stats) {
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

void __indexStats_rdbSave(RedisModuleIO *rdb, IndexStats *stats) {
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
  IndexSpec *sp = rm_malloc(sizeof(IndexSpec));
  sp->terms = NULL;
  sp->docs = NewDocTable(1000);
  sp->sortables = NULL;
  sp->name = RedisModule_LoadStringBuffer(rdb, NULL);
  sp->gc = NULL;
  sp->flags = (IndexFlags)RedisModule_LoadUnsigned(rdb);
  if (encver < INDEX_MIN_NOFREQ_VERSION) {
    sp->flags |= Index_StoreFreqs;
  }

  sp->numFields = RedisModule_LoadUnsigned(rdb);
  sp->fields = rm_calloc(sp->numFields, sizeof(FieldSpec));
  int maxSortIdx = -1;
  for (int i = 0; i < sp->numFields; i++) {

    __fieldSpec_rdbLoad(rdb, &sp->fields[i], encver);
    /* keep track of sorting indexes to rebuild the table */
    if (sp->fields[i].sortIdx > maxSortIdx) {
      maxSortIdx = sp->fields[i].sortIdx;
    }
  }
  /* if we have sortable fields - rebuild the sorting table */
  if (maxSortIdx >= 0) {
    _spec_buildSortingTable(sp, maxSortIdx + 1);
  }

  __indexStats_rdbLoad(rdb, &sp->stats);

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

  IndexSpec_StartGC(ctx, sp, GC_DEFAULT_HZ);
  return sp;
}

void IndexSpec_RdbSave(RedisModuleIO *rdb, void *value) {

  IndexSpec *sp = value;
  // we save the name plus the null terminator
  RedisModule_SaveStringBuffer(rdb, sp->name, strlen(sp->name) + 1);
  RedisModule_SaveUnsigned(rdb, (uint)sp->flags);

  RedisModule_SaveUnsigned(rdb, sp->numFields);
  for (int i = 0; i < sp->numFields; i++) {
    __fieldSpec_rdbSave(rdb, &sp->fields[i]);
  }

  __indexStats_rdbSave(rdb, &sp->stats);
  DocTable_RdbSave(&sp->docs, rdb);
  // save trie of terms
  TrieType_GenericSave(rdb, sp->terms, 0);

  // If we have custom stopwords, save them
  if (sp->flags & Index_HasCustomStopwords) {
    StopWordList_RdbSave(rdb, sp->stopwords);
  }
}

static void rewriteAofTerms(RedisModuleIO *io, const char *indexName, TrieNode *root) {
  TrieIterator *iter = TrieNode_Iterate(root, NULL, NULL, NULL);
  rune *runeStr;
  t_len runeStrLen;
  RSPayload *payload;
  float score;

  while (TrieIterator_Next(iter, &runeStr, &runeStrLen, NULL, &score, NULL)) {
    size_t bufLen;
    char *buf = runesToStr(runeStr, runeStrLen, &bufLen);

    char floatBuf[32] = {0};
    sprintf(floatBuf, "%f", score);

    RedisModule_EmitAOF(io, "FT.TERMADD", "ccc", indexName, buf, floatBuf);
    free(buf);
  }

  TrieIterator_Free(iter);
}

void IndexSpec_Digest(RedisModuleDigest *digest, void *value) {
}

#define __vpushStr(v, ctx, str) Vector_Push(v, RedisModule_CreateString(ctx, str, strlen(str)))

void IndexSpec_AofRewrite(RedisModuleIO *aof, RedisModuleString *key, void *value) {
  IndexSpec *sp = value;
  Vector *args = NewVector(RedisModuleString *, 4 + 4 * sp->numFields);
  RedisModuleCtx *ctx = RedisModule_GetContextFromIO(aof);

  // printf("sp->fags:%x\n", sp->flags);
  // serialize flags
  if (!(sp->flags & Index_StoreTermOffsets)) {
    __vpushStr(args, ctx, SPEC_NOOFFSETS_STR);
  }
  if (!(sp->flags & Index_StoreByteOffsets)) {
    __vpushStr(args, ctx, SPEC_NOHL_STR);
  }
  if (!(sp->flags & Index_StoreFieldFlags)) {
    __vpushStr(args, ctx, SPEC_NOFIELDS_STR);
  }

  // write SCHEMA keyword
  __vpushStr(args, ctx, SPEC_SCHEMA_STR);

  // serialize schema
  for (int i = 0; i < sp->numFields; i++) {

    switch (sp->fields[i].type) {
      case FIELD_FULLTEXT:
        __vpushStr(args, ctx, sp->fields[i].name);
        __vpushStr(args, ctx, SPEC_TEXT_STR);
        if (sp->fields[i].textOpts.weight != 1.0) {
          __vpushStr(args, ctx, SPEC_WEIGHT_STR);
          Vector_Push(args,
                      RedisModule_CreateStringPrintf(ctx, "%f", sp->fields[i].textOpts.weight));
        }
        if (FieldSpec_IsNoStem(&sp->fields[i])) {
          __vpushStr(args, ctx, SPEC_NOSTEM_STR);
        }
        break;
      case FIELD_NUMERIC:
        __vpushStr(args, ctx, sp->fields[i].name);
        __vpushStr(args, ctx, NUMERIC_STR);
        break;
      case FIELD_GEO:
        __vpushStr(args, ctx, sp->fields[i].name);
        __vpushStr(args, ctx, GEO_STR);
      default:

        break;
    }
    if (FieldSpec_IsSortable(&sp->fields[i])) {
      __vpushStr(args, ctx, SPEC_SORTABLE_STR);
    }
  }

  size_t offset = strlen(INDEX_SPEC_KEY_PREFIX);
  const char *indexName = RedisModule_StringPtrLen(key, NULL);
  indexName += offset;

  RedisModule_EmitAOF(aof, "FT.CREATE", "cv", indexName, (RedisModuleString *)args->data,
                      Vector_Size(args));

  DocTable_AOFRewrite(&sp->docs, indexName, aof);
  rewriteAofTerms(aof, indexName, sp->terms->root);

  Vector_Free(args);
}

int IndexSpec_RegisterType(RedisModuleCtx *ctx) {
  RedisModuleTypeMethods tm = {.version = REDISMODULE_TYPE_METHOD_VERSION,
                               .rdb_load = IndexSpec_RdbLoad,
                               .rdb_save = IndexSpec_RdbSave,
                               .aof_rewrite = IndexSpec_AofRewrite,
                               .free = IndexSpec_Free};

  IndexSpecType = RedisModule_CreateDataType(ctx, "ft_index0", INDEX_CURRENT_VERSION, &tm);
  if (IndexSpecType == NULL) {
    RedisModule_Log(ctx, "error", "Could not create index spec type");
    return REDISMODULE_ERR;
  }

  return REDISMODULE_OK;
}
