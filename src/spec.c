#include "rmutil/strings.h"
#include "rmutil/util.h"
#include "spec.h"
#include "util/logging.h"
#include "rmutil/vector.h"
#include "trie/trie_type.h"
#include <math.h>
#include <ctype.h>
#include "rmalloc.h"

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

uint32_t IndexSpec_GetFieldBit(IndexSpec *spec, const char *name, size_t len) {
  FieldSpec *sp = IndexSpec_GetField(spec, name, len);
  if (!sp) return 0;

  return sp->id;
}

int IndexSpec_GetFieldSortingIndex(IndexSpec *sp, const char *name, size_t len) {
  if (!sp->sortables) return -1;
  return RSSortingTable_GetFieldIdx(sp->sortables, name);
}

char *GetFieldNameByBit(IndexSpec *sp, uint32_t id) {
  for (int i = 0; i < sp->numFields; i++) {
    if (sp->fields[i].id == id) {
      return sp->fields[i].name;
    }
  }
  return NULL;
}

/*
* Parse an index spec from redis command arguments.
* Returns REDISMODULE_ERR if there's a parsing error.
* The command only receives the relvant part of argv.
*
* The format currently is FT.CREATE {index} [NOOFFSETS] [NOFIELDS] [NOSCOREIDX]
    SCHEMA {field} [TEXT [WEIGHT {weight}]] | [NUMERIC]
*/
IndexSpec *IndexSpec_ParseRedisArgs(RedisModuleCtx *ctx, RedisModuleString *name,
                                    RedisModuleString **argv, int argc, char **err) {

  const char *args[argc];
  for (int i = 0; i < argc; i++) {
    args[i] = RedisModule_StringPtrLen(argv[i], NULL);
  }

  return IndexSpec_Parse(RedisModule_StringPtrLen(name, NULL), args, argc, err);
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
int __parseFieldSpec(const char **argv, int *offset, int argc, FieldSpec *sp) {

  // if we're at the end - fail
  if (*offset >= argc) return 0;
  sp->sortIdx = -1;
  sp->sortable = 0;
  // the field name comes here
  sp->name = rm_strdup(argv[*offset]);

  // we can't be at the end
  if (++*offset == argc) return 0;

  // this is a text field
  if (!strcasecmp(argv[*offset], SPEC_TEXT_STR)) {

    // init default weight and type
    sp->type = F_FULLTEXT;
    sp->weight = 1.0;
    // it's legit to be at the end now
    if (++*offset == argc) return 1;

    // if we have weight - try and parse it
    if (!strcasecmp(argv[*offset], SPEC_WEIGHT_STR)) {
      // weight with no wait is invalid
      if (++*offset == argc) return 0;

      // try and parse the weight
      double d = strtod(argv[*offset], NULL);
      if (d == 0 || d == HUGE_VAL || d == -HUGE_VAL || d < 0) {
        return 0;
      }
      sp->weight = d;
      ++*offset;
    }

  } else if (!strcasecmp(argv[*offset], NUMERIC_STR)) {
    sp->type = F_NUMERIC;
    sp->weight = 0.0;
    ++*offset;

  } else if (!strcasecmp(argv[*offset], GEO_STR)) {  // geo field
    sp->type = F_GEO;
    sp->weight = 0;
    ++*offset;
  } else {  // not numeric and not text - nothing more supported currently
    return 0;
  }

  if (*offset < argc && !strcasecmp(argv[*offset], SPEC_SORTABLE_STR)) {
    // cannot sort by geo fields
    if (sp->type == F_GEO) {
      return 0;
    }
    sp->sortable = 1;
    ++*offset;
  }
  return 1;
}

void _spec_buildSortingTable(IndexSpec *spec, int len) {
  spec->sortables = NewSortingTable(len);
  for (int i = 0; i < spec->numFields; i++) {
    if (spec->fields[i].sortable) {
      // printf("Adding sortable field %s id %d\n", spec->fields[i].name, spec->fields[i].sortIdx);
      SortingTable_SetFieldName(spec->sortables, spec->fields[i].sortIdx, spec->fields[i].name);
    }
  }
}
/* The format currently is FT.CREATE {index} [NOOFFSETS] [NOFIELDS] [NOSCOREIDX]
    SCHEMA {field} [TEXT [WEIGHT {weight}]] | [NUMERIC]
  */
IndexSpec *IndexSpec_Parse(const char *name, const char **argv, int argc, char **err) {

  int schemaOffset = __findOffset(SPEC_SCHEMA_STR, argv, argc);
  // no schema or schema towrards the end
  if (schemaOffset == -1) {
    *err = "schema not found";
    return NULL;
  }
  IndexSpec *spec = NewIndexSpec(name, 0);

  if (__argExists(SPEC_NOOFFSETS_STR, argv, argc, schemaOffset)) {
    spec->flags &= ~Index_StoreTermOffsets;
  }

  if (__argExists(SPEC_NOFIELDS_STR, argv, argc, schemaOffset)) {
    spec->flags &= ~Index_StoreFieldFlags;
  }

  if (__argExists(SPEC_NOSCOREIDX_STR, argv, argc, schemaOffset)) {
    spec->flags &= ~Index_StoreScoreIndexes;
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

  t_fieldMask id = 1;
  int sortIdx = 0;

  int i = schemaOffset + 1;
  while (i < argc && spec->numFields < SPEC_MAX_FIELDS) {

    if (!__parseFieldSpec(argv, &i, argc, &spec->fields[spec->numFields])) {
      *err = "Could not parse field spec";
      goto failure;
    }

    if (spec->fields[spec->numFields].type == F_FULLTEXT) {
      spec->fields[spec->numFields].id = id;
      id *= 2;
    }
    if (spec->fields[spec->numFields].sortable) {
      spec->fields[spec->numFields].sortIdx = sortIdx++;
    }
    spec->numFields++;
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

int IndexSpec_AddTerm(IndexSpec *sp, const char *term, size_t len) {
  return Trie_InsertStringBuffer(sp->terms, (char *)term, len, 1, 1, NULL);
}

void IndexSpec_Free(void *ctx) {
  IndexSpec *spec = ctx;

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
  rm_free(spec);
}

/* Load the spec from the saved version */
IndexSpec *IndexSpec_Load(RedisModuleCtx *ctx, const char *name, int openWrite) {

  RedisModuleKey *k =
      RedisModule_OpenKey(ctx, RedisModule_CreateStringPrintf(ctx, INDEX_SPEC_KEY_FMT, name),
                          REDISMODULE_READ | (openWrite ? REDISMODULE_WRITE : 0));

  // we do not allow empty indexes when loading an existing index
  if (k == NULL || RedisModule_KeyType(k) == REDISMODULE_KEYTYPE_EMPTY ||
      RedisModule_ModuleTypeGetType(k) != IndexSpecType) {
    return NULL;
  }

  IndexSpec *ret = RedisModule_ModuleTypeGetValue(k);
  return ret;
}

t_fieldMask IndexSpec_ParseFieldMask(IndexSpec *sp, RedisModuleString **argv, int argc) {
  t_fieldMask ret = 0;

  for (int i = 0; i < argc; i++) {
    size_t len;
    const char *p = RedisModule_StringPtrLen(argv[i], &len);

    FieldSpec *fs = IndexSpec_GetField(sp, p, len);
    if (fs != NULL) {
      LG_DEBUG("Found mask for %s: %u\n", p, fs->id);
      ret |= (fs->id & RS_FIELDMASK_ALL);
    }
  }

  return ret;
}

int IndexSpec_ParseStopWords(IndexSpec *sp, RedisModuleString **strs, size_t len) {
  // if the index already has custom stopwords, let us free them first
  if (sp->stopwords && sp->flags & Index_HasCustomStopwords) {
    StopWordList_Free(sp->stopwords);
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
  memset(&sp->stats, 0, sizeof(sp->stats));
  return sp;
}

void __fieldSpec_rdbSave(RedisModuleIO *rdb, FieldSpec *f) {
  RedisModule_SaveStringBuffer(rdb, f->name, strlen(f->name) + 1);
  RedisModule_SaveUnsigned(rdb, f->id);
  RedisModule_SaveUnsigned(rdb, f->type);
  RedisModule_SaveDouble(rdb, f->weight);
  RedisModule_SaveUnsigned(rdb, f->sortable);
  RedisModule_SaveSigned(rdb, f->sortIdx);
}

void __fieldSpec_rdbLoad(RedisModuleIO *rdb, FieldSpec *f, int encver) {

  f->name = RedisModule_LoadStringBuffer(rdb, NULL);
  f->id = RedisModule_LoadUnsigned(rdb);
  f->type = RedisModule_LoadUnsigned(rdb);
  f->weight = RedisModule_LoadDouble(rdb);
  if (encver >= 4) {
    f->sortable = RedisModule_LoadUnsigned(rdb);
    f->sortIdx = RedisModule_LoadSigned(rdb);
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
  IndexSpec *sp = rm_malloc(sizeof(IndexSpec));
  sp->terms = NULL;
  sp->docs = NewDocTable(1000);
  sp->sortables = NULL;
  sp->name = RedisModule_LoadStringBuffer(rdb, NULL);
  sp->flags = (IndexFlags)RedisModule_LoadUnsigned(rdb);

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
  if (!(sp->flags & Index_StoreFieldFlags)) {
    __vpushStr(args, ctx, SPEC_NOFIELDS_STR);
  }
  if (!(sp->flags & Index_StoreScoreIndexes)) {
    __vpushStr(args, ctx, SPEC_NOSCOREIDX_STR);
  }

  // write SCHEMA keyword
  __vpushStr(args, ctx, SPEC_SCHEMA_STR);

  // serialize schema
  for (int i = 0; i < sp->numFields; i++) {

    switch (sp->fields[i].type) {
      case F_FULLTEXT:
        __vpushStr(args, ctx, sp->fields[i].name);
        __vpushStr(args, ctx, SPEC_TEXT_STR);
        if (sp->fields[i].weight != 1.0) {
          __vpushStr(args, ctx, SPEC_WEIGHT_STR);
          Vector_Push(args, RedisModule_CreateStringPrintf(ctx, "%f", sp->fields[i].weight));
        }
        break;
      case F_NUMERIC:
        __vpushStr(args, ctx, sp->fields[i].name);
        __vpushStr(args, ctx, NUMERIC_STR);
        break;
      case F_GEO:
        __vpushStr(args, ctx, sp->fields[i].name);
        __vpushStr(args, ctx, GEO_STR);
      default:

        break;
    }
    if (sp->fields[i].sortable) {
      __vpushStr(args, ctx, SPEC_SORTABLE_STR);
    }
  }

  RedisModule_EmitAOF(aof, "FT.CREATE", "sv", key, (RedisModuleString *)args->data,
                      Vector_Size(args));

  DocTable_AOFRewrite(&sp->docs, key, aof);

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
