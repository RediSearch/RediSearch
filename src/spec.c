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
#include "numeric_index.h"
#include "indexer.h"
#include "alias.h"
#include "module.h"
#include "rmutil/rm_assert.h"

///////////////////////////////////////////////////////////////////////////////////////////////

void (*IndexSpec_OnCreate)(const IndexSpec *) = NULL;
const char *(*IndexAlias_GetUserTableName)(RedisModuleCtx *, const char *) = NULL;

RedisModuleType *IndexSpecType;
static uint64_t spec_unique_ids = 1;

//---------------------------------------------------------------------------------------------

const FieldSpec *IndexSpec::getFieldCommon(const char *name, size_t len, int useCase) const {
  for (size_t i = 0; i < numFields; i++) {
    if (len != strlen(fields[i].name)) {
      continue;
    }
    const FieldSpec *fs = fields + i;
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

//---------------------------------------------------------------------------------------------

/*
 * Get a field spec by field name. Case insensitive!
 * Return the field spec if found, NULL if not
 */
const FieldSpec *IndexSpec::GetField(const char *name, size_t len) const {
  return getFieldCommon(name, len, 0);
};

//---------------------------------------------------------------------------------------------

// Case-sensitive version of GetField()
const FieldSpec *IndexSpec::GetFieldCase(const char *name, size_t n) const {
  return getFieldCommon(name, n, 1);
}

//---------------------------------------------------------------------------------------------

// Get the field bitmask id of a text field by name. Return 0 if the field is not found or is not a
// text field

t_fieldMask IndexSpec::GetFieldBit(const char *name, size_t len) {
  const FieldSpec *sp = GetField(name, len);
  if (!sp || !sp->IsFieldType(INDEXFLD_T_FULLTEXT) || !sp->IsIndexable()) return 0;

  return FIELD_BIT(sp);
}

//---------------------------------------------------------------------------------------------

// Check if phonetic matching is enabled on any field within the fieldmask.
// Returns true if any field has phonetics, and false if none of the fields require it.

bool IndexSpec::CheckPhoneticEnabled(t_fieldMask fm) const {
  if (!(flags & Index_HasPhonetic)) {
    return false;
  }

  if (fm == 0 || fm == (t_fieldMask)-1) {
    // No fields -- implicit phonetic match!
    return true;
  }

  for (size_t ii = 0; ii < numFields; ++ii) {
    if (fm & ((t_fieldMask)1 << ii)) {
      const FieldSpec *fs = fields + ii;
      if (fs->IsFieldType(INDEXFLD_T_FULLTEXT) && (fs->IsPhonetics())) {
        return true;
      }
    }
  }
  return false;
}

//---------------------------------------------------------------------------------------------

// Get a sortable field's sort table index by its name. return -1 if the field was not found or is
// not sortable.

int IndexSpec::GetFieldSortingIndex(const char *name, size_t len) {
  if (!sortables) return -1;
  return sortables->GetFieldIdx(name);
}

//---------------------------------------------------------------------------------------------

// Get the field spec from the sortable index

const FieldSpec *IndexSpec::GetFieldBySortingIndex(uint16_t idx) const {
  for (size_t ii = 0; ii < numFields; ++ii) {
    if (fields[ii].options & FieldSpec_Sortable && fields[ii].sortIdx == idx) {
      return fields + ii;
    }
  }
  return NULL;
}

//---------------------------------------------------------------------------------------------

const char *IndexSpec::GetFieldNameByBit(t_fieldMask id) const {
  for (int i = 0; i < numFields; i++) {
    if (FIELD_BIT(&fields[i]) == id && fields[i].IsFieldType(INDEXFLD_T_FULLTEXT) &&
        fields[i].IsIndexable()) {
      return fields[i].name;
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

void IndexSpec::ParseRedisArgs(RedisModuleCtx *ctx, RedisModuleString *name,
                               RedisModuleString **argv, int argc, QueryError *status) {

  const char *args[argc];
  for (int i = 0; i < argc; i++) {
    args[i] = RedisModule_StringPtrLen(argv[i], NULL);
  }

  Parse(RedisModule_StringPtrLen(name, NULL), args, argc, status);
}

//---------------------------------------------------------------------------------------------

FieldSpec **IndexSpec::getFieldsByType(FieldType type) {
#define FIELDS_ARRAY_CAP 2
  FieldSpec **fields_ = array_new(FieldSpec *, FIELDS_ARRAY_CAP);
  for (int i = 0; i < numFields; ++i) {
    if ((fields + i)->IsFieldType(type)) {
      fields_ = array_append(fields_, &fields[i]);
    }
  }
  return fields_;
}

//---------------------------------------------------------------------------------------------

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

//---------------------------------------------------------------------------------------------

IndexSpec::IndexSpec(RedisModuleCtx *ctx, RedisModuleString **argv, int argc,
                     QueryError *status) {
  ParseRedisArgs(ctx, argv[1], &argv[2], argc - 2, status);

  RedisModuleString *keyString = RedisModule_CreateStringPrintf(ctx, INDEX_SPEC_KEY_FMT, name);
  RedisModuleKey *k = RedisModule_OpenKey(ctx, keyString, REDISMODULE_READ | REDISMODULE_WRITE);
  RedisModule_FreeString(ctx, keyString);

  // check that the key is empty
  if (k == NULL || (RedisModule_KeyType(k) != REDISMODULE_KEYTYPE_EMPTY)) {
    if (RedisModule_ModuleTypeGetType(k) != IndexSpecType) {
      status->SetCode(QUERY_EREDISKEYTYPE);
    } else {
      status->SetCode(QUERY_EINDEXEXISTS);
    }
    delete this;
    if (k) {
      RedisModule_CloseKey(k);
    }
    return;
  }

  uniqueId = spec_unique_ids++;
  // Start the garbage collector
  StartGC(ctx, GC_DEFAULT_HZ);

  RSCursors->Add(name, RSCURSORS_DEFAULT_CAPACITY);

  // set the value in redis
  RedisModule_ModuleTypeSetValue(k, IndexSpecType, this);
  if (flags & Index_Temporary) {
    RedisModule_SetExpire(k, timeout * 1000);
  }
  // Create the indexer
  indexer = new DocumentIndexer(this);
  if (IndexSpec_OnCreate) {
    IndexSpec_OnCreate(this);
  }
  RedisModule_CloseKey(k);
}

//---------------------------------------------------------------------------------------------

char *strtolower(char *str) {
  char *p = str;
  while (*p) {
    *p = tolower(*p);
    p++;
  }
  return str;
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

//---------------------------------------------------------------------------------------------

static bool parseTextField(FieldSpec *sp, ArgsCursor *ac, QueryError *status) {
  int rc;
  // this is a text field
  // init default weight and type
  while (!ac->IsAtEnd()) {
    if (ac->AdvanceIfMatch(SPEC_NOSTEM_STR)) {
      sp->options |= FieldSpec_NoStemming;
      continue;
    } else if (ac->AdvanceIfMatch(SPEC_WEIGHT_STR)) {
      double d;
      if ((rc = ac->GetDouble(&d, 0)) != AC_OK) {
        QERR_MKBADARGS_AC(status, "weight", rc);
        return false;
      }
      sp->ftWeight = d;
      continue;
    } else if (ac->AdvanceIfMatch(SPEC_PHONETIC_STR)) {
      if (ac->IsAtEnd()) {
        status->SetError(QUERY_EPARSEARGS, SPEC_PHONETIC_STR " requires an argument");
        return false;
      }

      const char *matcher = ac->GetStringNC(NULL);
      // try and parse the matcher
      // currently we just make sure algorithm is double metaphone (dm)
      // and language is one of the following : English (en), French (fr), Portuguese (pt) and
      // Spanish (es)
      // in the future we will support more algorithms and more languages
      if (!checkPhoneticAlgorithmAndLang(matcher)) {
        status->SetError(QUERY_EINVAL,
        "Matcher Format: <2 chars algorithm>:<2 chars language>. Support algorithms: "
        "double metaphone (dm). Supported languages: English (en), French (fr), "
        "Portuguese (pt) and Spanish (es)");

        return false;
      }
      sp->options |= FieldSpec_Phonetics;
      continue;
    } else {
      break;
    }
  }
  return true;
}

//---------------------------------------------------------------------------------------------

/* Parse a field definition from argv, at *offset. We advance offset as we progress.
 *  Returns 1 on successful parse, 0 otherwise */
static bool parseFieldSpec(ArgsCursor *ac, FieldSpec *sp, QueryError *status) {
  if (ac->IsAtEnd()) {
    status->SetErrorFmt(QUERY_EPARSEARGS, "Field `%s` does not have a type", sp->name);
    return false;
  }

  if (ac->AdvanceIfMatch(SPEC_TEXT_STR)) {
    sp->Initialize(INDEXFLD_T_FULLTEXT);
    if (!parseTextField(sp, ac, status)) {
      goto error;
    }
  } else if (ac->AdvanceIfMatch(NUMERIC_STR)) {
    sp->Initialize(INDEXFLD_T_NUMERIC);
  } else if (ac->AdvanceIfMatch(GEO_STR)) {  // geo field
    sp->Initialize(INDEXFLD_T_GEO);
  } else if (ac->AdvanceIfMatch(SPEC_TAG_STR)) {  // tag field
    sp->Initialize(INDEXFLD_T_TAG);
    if (ac->AdvanceIfMatch(SPEC_SEPARATOR_STR)) {
      if (ac->IsAtEnd()) {
        status->SetError(QUERY_EPARSEARGS, SPEC_SEPARATOR_STR " requires an argument");
        goto error;
      }
      const char *sep = ac->GetStringNC(NULL);
      if (strlen(sep) != 1) {
        status->SetErrorFmt(QUERY_EPARSEARGS,
                               "Tag separator must be a single character. Got `%s`", sep);
        goto error;
      }
      sp->tagSep = *sep;
    }
  } else {  // not numeric and not text - nothing more supported currently
    status->SetErrorFmt(QUERY_EPARSEARGS, "Invalid field type for field `%s`", sp->name);
    goto error;
  }

  while (!ac->IsAtEnd()) {
    if (ac->AdvanceIfMatch(SPEC_SORTABLE_STR)) {
      sp->SetSortable();
      continue;
    } else if (ac->AdvanceIfMatch(SPEC_NOINDEX_STR)) {
      sp->options |= FieldSpec_NotIndexable;
      continue;
    } else {
      break;
    }
  }
  return true;

error:
  if (!status->HasError()) {
    status->SetErrorFmt(QUERY_EPARSEARGS, "Could not parse schema for field `%s`",
                           sp->name);
  }
  sp->Cleanup();
  return false;
}

//---------------------------------------------------------------------------------------------

// Gets the next text id from the index. This does not currently modify the index

int IndexSpec::CreateTextId() const {
  int maxId = -1;
  for (size_t ii = 0; ii < numFields; ++ii) {
    const FieldSpec *fs = fields + ii;
    if (fs->IsFieldType(INDEXFLD_T_FULLTEXT)) {
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

//---------------------------------------------------------------------------------------------

/**
 * Add fields to an existing (or newly created) index. If the addition fails,
 */

bool IndexSpec::AddFieldsInternal(ArgsCursor *ac, QueryError *status, int isNew) {
  if (spcache) {
    spcache->Decref();
    spcache = NULL;
  }
  const size_t prevNumFields = numFields;
  const size_t prevSortLen = sortables->len;
  FieldSpec *fs = NULL;

  while (!ac->IsAtEnd()) {
    size_t nfieldName = 0;
    const char *fieldName = ac->GetStringNC(&nfieldName);
    if (GetField(fieldName, nfieldName)) {
      status->SetError(QUERY_EINVAL, "Duplicate field in schema");
      goto reset;
    }

    fs = CreateField(fieldName);

    if (!parseFieldSpec(ac, fs, status)) {
      goto reset;
    }

    if (fs->IsFieldType(INDEXFLD_T_FULLTEXT) && fs->IsIndexable()) {
      int textId = CreateTextId();
      if (textId < 0) {
        status->SetError(QUERY_ELIMIT, "Too many TEXT fields in schema");
        goto reset;
      }

      // If we need to store field flags and we have over 32 fields, we need to switch to wide
      // schema encoding
      if (textId >= SPEC_WIDEFIELD_THRESHOLD && (flags & Index_StoreFieldFlags)) {
        if (isNew) {
          flags |= Index_WideSchema;
        } else if ((flags & Index_WideSchema) == 0) {

              status->SetError(QUERY_ELIMIT,
              "Cannot add more fields. Declare index with wide fields to allow adding "
              "unlimited fields");
          goto reset;
        }
      }
      fs->ftId = textId;
    }

    if (fs->IsSortable()) {
      if (fs->options & FieldSpec_Dynamic) {
        status->SetError(QUERY_EBADOPTION, "Cannot set dynamic field to sortable");
        goto reset;
      }

      fs->sortIdx = sortables->Add(fs->name, fieldTypeToValueType(fs->types));
    } else {
      fs->sortIdx = -1;
    }
    if (fs->IsPhonetics()) {
      flags |= Index_HasPhonetic;
    }
    fs = NULL;
  }
  return true;

reset:
  // If the current field spec exists, but was not added (i.e. we got an error)
  // and reached this block, then free it
  if (fs) {
    // if we have a field spec it means that we increased the number of fields, so we need to
    // decreas it.
    --numFields;
    fs->Cleanup();
  }
  for (size_t i = prevNumFields; i < numFields; ++i) {
    fields[i].Cleanup();
  }

  numFields = prevNumFields;
  sortables->len = prevSortLen;
  return false;
}

// Add fields to a redis schema
bool IndexSpec::AddFields(ArgsCursor *ac, QueryError *status) {
  return AddFieldsInternal(ac, status, 0);
}

//---------------------------------------------------------------------------------------------

/* The format currently is FT.CREATE {index} [NOOFFSETS] [NOFIELDS]
    SCHEMA {field} [TEXT [WEIGHT {weight}]] | [NUMERIC]
  */
void IndexSpec::Parse(const char *name, const char **argv, int argc, QueryError *status) {
  ctor(name);

  ArgsCursor ac;
  ArgsCursor acStopwords;

  ac.InitCString(argv, argc);
  long long timeout = -1;
  int dummy;

  ACArgSpec argopts[] = {
      {AC_MKUNFLAG(SPEC_NOOFFSETS_STR, &flags,
                   Index_StoreTermOffsets | Index_StoreByteOffsets)},
      {AC_MKUNFLAG(SPEC_NOHL_STR, &flags, Index_StoreByteOffsets)},
      {AC_MKUNFLAG(SPEC_NOFIELDS_STR, &flags, Index_StoreFieldFlags)},
      {AC_MKUNFLAG(SPEC_NOFREQS_STR, &flags, Index_StoreFreqs)},
      {AC_MKBITFLAG(SPEC_SCHEMA_EXPANDABLE_STR, &flags, Index_WideSchema)},
      // For compatibility
      {name: "NOSCOREIDX", type: AC_ARGTYPE_BOOLFLAG, target: &dummy},
      {name: SPEC_TEMPORARY_STR, type: AC_ARGTYPE_LLONG, target: &timeout},
      {name: SPEC_STOPWORDS_STR, type: AC_ARGTYPE_SUBARGS, target: &acStopwords},
      {name: NULL}};

  ACArgSpec *errarg = NULL;
  int rc = ac.ParseArgSpec(argopts, &errarg);
  if (rc != AC_OK) {
    if (rc != AC_ERR_ENOENT) {
      QERR_MKBADARGS_AC(status, errarg->name, rc);
      throw Error("");
    }
  }

  if (timeout != -1) {
    flags |= Index_Temporary;
  }
  timeout = timeout;

  if (acStopwords.IsInitialized()) {
    if (stopwords) {
      delete stopwords;
    }
    stopwords = new StopWordList((const char **)acStopwords.objs, acStopwords.argc);
    flags |= Index_HasCustomStopwords;
  }

  if (!ac.AdvanceIfMatch(SPEC_SCHEMA_STR)) {
    if (ac.NumRemaining()) {
      const char *badarg = ac.GetStringNC(NULL);
      status->SetErrorFmt(QUERY_EPARSEARGS, "Unknown argument `%s`", badarg);
    } else {
      status->SetError(QUERY_EPARSEARGS, "No schema found");
    }
    throw Error("");
  }

  if (!AddFieldsInternal(&ac, status, 1)) {
    throw Error("");
  }
}

//---------------------------------------------------------------------------------------------

// Initialize some index stats that might be useful for scoring functions

void IndexSpec::GetStats(RSIndexStats *stats_) {
  stats_->numDocs = stats.numDocuments;
  stats_->numTerms = stats.numTerms;
  stats_->avgDocLen =
      stats_->numDocs ? (double)stats.numRecords / (double)stats.numDocuments : 0;
}

//---------------------------------------------------------------------------------------------

int IndexSpec::AddTerm(const char *term, size_t len) {
  int isNew = terms->InsertStringBuffer((char *)term, len, 1, 1, NULL);
  if (isNew) {
    stats.numTerms++;
    stats.termsSize += len;
  }
  return isNew;
}

//---------------------------------------------------------------------------------------------

// Retrieves the current spec cache from the index, incrementing its
// reference count by 1. Use IndexSpecCache_Decref to free

IndexSpecCache *IndexSpec::GetSpecCache() const {
  if (!spcache) {
    ((IndexSpec *)this)->spcache = BuildSpecCache();
  }

  spcache->refcount++;
  return spcache;
}

//---------------------------------------------------------------------------------------------

// Create a new copy of the spec cache from the current index spec

IndexSpecCache *IndexSpec::BuildSpecCache() const {
  IndexSpecCache *ret = rm_calloc(1, sizeof(*ret));
  ret->nfields = numFields;
  ret->fields = rm_malloc(sizeof(*ret->fields) * ret->nfields);
  ret->refcount = 1;
  for (size_t ii = 0; ii < numFields; ++ii) {
    ret->fields[ii] = fields[ii];
    ret->fields[ii].name = rm_strdup(ret->fields[ii].name);
  }
  return ret;
}

//---------------------------------------------------------------------------------------------

// Decrement the reference count of the spec cache. Should be matched
// with a previous call of GetSpecCache()

void IndexSpecCache::Decref() {
  if (--refcount) {
    return;
  }
  for (size_t ii = 0; ii < nfields; ++ii) {
    rm_free(fields[ii].name);
  }
  rm_free(fields);
  rm_free(this);
}

//---------------------------------------------------------------------------------------------

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

//---------------------------------------------------------------------------------------------

// Get a random term from the index spec using weighted random. Weighted random is done by
// sampling N terms from the index and then doing weighted random on them. A sample size of 10-20
// should be enough. Returns NULL if the index is empty

char *IndexSpec::GetRandomTerm(size_t sampleSize) {

  if (sampleSize > terms->size) {
    sampleSize = terms->size;
  }
  if (!sampleSize) return NULL;

  char *samples[sampleSize];
  double weights[sampleSize];
  for (int i = 0; i < sampleSize; i++) {
    char *ret = NULL;
    t_len len = 0;
    double d = 0;
    if (!terms->RandomKey(&ret, &len, &d) || len == 0) {
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

//---------------------------------------------------------------------------------------------

// Delete the redis key from Redis

void IndexSpec::FreeWithKey(RedisModuleCtx *ctx) {
  RedisModuleString *s = RedisModule_CreateStringPrintf(ctx, INDEX_SPEC_KEY_FMT, name);
  RedisModuleKey *kk = RedisModule_OpenKey(ctx, s, REDISMODULE_WRITE);
  RedisModule_FreeString(ctx, s);
  if (kk == NULL || RedisModule_KeyType(kk) != REDISMODULE_KEYTYPE_MODULE ||
      RedisModule_ModuleTypeGetType(kk) != IndexSpecType) {
    if (kk != NULL) {
      RedisModule_CloseKey(kk);
    }
    delete this;
    return;
  }
  RS_LOG_ASSERT(RedisModule_ModuleTypeGetValue(kk) == this, "IndexSpecs should be identical");
  RedisModule_DeleteKey(kk);
  RedisModule_CloseKey(kk);
}

//---------------------------------------------------------------------------------------------

void IndexSpec::FreeInternals() {
  if (indexer) {
    delete indexer;
  }
  if (gc) {
    gc->Stop();
  }

  if (terms) {
    delete terms;
  }
  delete &docs;

  if (uniqueId) {
    // If uniqueid is 0, it means the index was not initialized
    // and is being freed now during an error.
    RSCursors->Purge(name);
    RSCursors->Remove(name);
  }

  rm_free(name);
  if (sortables) {
    delete sortables;
  }
  if (stopwords) {
    delete stopwords;
    stopwords = NULL;
  }

  if (smap) {
    delete smap;
  }
  if (spcache) {
    spcache->Decref();
    spcache = NULL;
  }

  if (indexStrs) {
    for (size_t ii = 0; ii < numFields; ++ii) {
      IndexSpecFmtStrings *fmts = indexStrs + ii;
      for (size_t jj = 0; jj < INDEXFLD_NUM_TYPES; ++jj) {
        if (fmts->types[jj]) {
          RedisModule_FreeString(RSDummyContext, fmts->types[jj]);
        }
      }
    }
    rm_free(indexStrs);
  }
  if (fields != NULL) {
    for (size_t i = 0; i < numFields; i++) {
      rm_free(fields[i].name);
    }
    rm_free(fields);
  }

  ClearAliases();

  delete &keysDict;
}

//---------------------------------------------------------------------------------------------

// Free the index synchronously. Any keys associated with the index (but not the
// documents themselves) are freed before this function returns.

static void IndexSpec_FreeAsync(void *data) {
  IndexSpec *spec = data;
  RedisModuleCtx *threadCtx = RedisModule_GetThreadSafeContext(NULL);
  RedisSearchCtx sctx = SEARCH_CTX_STATIC(threadCtx, spec);
  RedisModule_AutoMemory(threadCtx);
  RedisModule_ThreadSafeContextLock(threadCtx);

  Redis_DropIndex(&sctx, true, false);
  spec->FreeInternals();

  RedisModule_ThreadSafeContextUnlock(threadCtx);
  RedisModule_FreeThreadSafeContext(threadCtx);
}

//---------------------------------------------------------------------------------------------

static struct thpool_ *cleanPool = NULL;

// Free an indexSpec. This doesn't free the spec itself as it's not allocated by the parser
// and should be on the request's stack.

IndexSpec::~IndexSpec() {
  if (flags & Index_Temporary) {
    if (!cleanPool) {
      cleanPool = thpool_init(1);
    }
    thpool_add_work(cleanPool, IndexSpec_FreeAsync, this);
    return;
  }

  FreeInternals();
}

void IndexSpec_Free(void *ctx) {
  IndexSpec *spec = ctx;
  delete spec;
}

//---------------------------------------------------------------------------------------------

void IndexSpec::FreeSync() {
  //  todo:
  //  mark I think we only need FreeInternals, this is called only from the
  //  LLAPI and there is no need to drop keys cause its out of the key space.
  //  Let me know what you think

  //   Need a context for this:
  RedisModuleCtx *ctx = RedisModule_GetThreadSafeContext(NULL);
  RedisSearchCtx sctx = SEARCH_CTX_STATIC(ctx, this);
  RedisModule_AutoMemory(ctx);
  if (!IsKeyless()) {
    Redis_DropIndex(&sctx, 0, 1);
  }
  FreeInternals();
  RedisModule_FreeThreadSafeContext(ctx);
}

//---------------------------------------------------------------------------------------------

IndexSpec *IndexSpec::LoadEx(RedisModuleCtx *ctx, IndexLoadOptions *options) {
  IndexSpec *ret = NULL;
  int modeflags = REDISMODULE_READ | REDISMODULE_WRITE;

  if (options->flags & INDEXSPEC_LOAD_WRITEABLE) {
    modeflags |= REDISMODULE_WRITE;
  }

  RedisModuleString *formatted;
  bool isKeynameOwner = false;
  const char *ixname = NULL;

  if (options->flags & INDEXSPEC_LOAD_KEY_FORMATTED) {
    formatted = options->rstring;
  } else {
    isKeynameOwner = true;
    if (options->flags & INDEXSPEC_LOAD_KEY_RSTRING) {
      ixname = RedisModule_StringPtrLen(options->rstring, NULL);
    } else {
      ixname = options->cstring;
    }
    formatted = RedisModule_CreateStringPrintf(ctx, INDEX_SPEC_KEY_FMT, ixname);
  }

  options->keyp = RedisModule_OpenKey(ctx, formatted, modeflags);
  // we do not allow empty indexes when loading an existing index
  if (options->keyp == NULL || RedisModule_KeyType(options->keyp) == REDISMODULE_KEYTYPE_EMPTY) {
    if (options->keyp) {
      RedisModule_CloseKey(options->keyp);
      options->keyp = NULL;
    }
    if ((options->flags & INDEXSPEC_LOAD_NOALIAS) || ixname == NULL) {
      goto done;  // doesn't exist.
    }
    IndexSpec *aliasTarget = ret = IndexAlias::Get(ixname);
    if (aliasTarget && (options->flags & INDEXSPEC_LOAD_KEYLESS) == 0) {
      if (isKeynameOwner) {
        RedisModule_FreeString(ctx, formatted);
      }
      formatted = RedisModule_CreateStringPrintf(ctx, INDEX_SPEC_KEY_FMT, ret->name);
      isKeynameOwner = true;
      options->keyp = RedisModule_OpenKey(ctx, formatted, modeflags);
    }
  } else {
    if (RedisModule_ModuleTypeGetType(options->keyp) != IndexSpecType) {
      goto done;
    }
    ret = RedisModule_ModuleTypeGetValue(options->keyp);
  }

  if (!ret) {
    goto done;
  }
  if (ret->flags & Index_Temporary) {
    mstime_t exp = ret->timeout * 1000;
    if (modeflags & REDISMODULE_WRITE) {
      RedisModule_SetExpire(options->keyp, exp);
    } else {
      RedisModuleKey *temp = RedisModule_OpenKey(ctx, formatted, REDISMODULE_WRITE);
      RedisModule_SetExpire(temp, ret->timeout * 1000);
      RedisModule_CloseKey(temp);
    }
  }

done:
  if (isKeynameOwner) {
    RedisModule_FreeString(ctx, formatted);
  }
  if ((options->flags & INDEXSPEC_LOAD_KEYLESS) && options->keyp) {
    RedisModule_CloseKey(options->keyp);
    options->keyp = NULL;
  }
  return ret;
}

//---------------------------------------------------------------------------------------------

// Load the spec from the saved version

IndexSpec *IndexSpec::Load(RedisModuleCtx *ctx, const char *name, int openWrite) {
  IndexLoadOptions lopts(openWrite ? INDEXSPEC_LOAD_WRITEABLE : 0, name);
  lopts.flags |= INDEXSPEC_LOAD_KEYLESS;
  return IndexSpec::LoadEx(ctx, &lopts);
}

//---------------------------------------------------------------------------------------------

RedisModuleString *fmtRedisNumericIndexKey(RedisSearchCtx *ctx, const char *field);

// Returns a string suitable for indexes. This saves on string creation/destruction

RedisModuleString *IndexSpec::GetFormattedKey(const FieldSpec *fs, FieldType forType) {
  if (!indexStrs) {
    indexStrs = rm_calloc(SPEC_MAX_FIELDS, sizeof(*indexStrs));
  }

  size_t typeix = INDEXTYPE_TO_POS(forType);

  RedisModuleString *ret = indexStrs[fs->index].types[typeix];
  if (!ret) {
    RedisSearchCtx sctx = {.redisCtx = RSDummyContext, .spec = this};
    switch (forType) {
      case INDEXFLD_T_NUMERIC:
        ret = fmtRedisNumericIndexKey(&sctx, fs->name);
        break;
      case INDEXFLD_T_TAG:
        ret = TagIndex::FormatName(&sctx, fs->name);
        break;
      case INDEXFLD_T_GEO:
        ret = RedisModule_CreateStringPrintf(RSDummyContext, GEOINDEX_KEY_FMT, name, fs->name);
        break;
      case INDEXFLD_T_FULLTEXT:  // Text fields don't get a per-field index
      default:
        ret = NULL;
        abort();
        break;
    }
  }
  if (!ret) {
    return NULL;
  }
  indexStrs[fs->index].types[typeix] = ret;
  return ret;
}

//---------------------------------------------------------------------------------------------

RedisModuleString *IndexSpec::GetFormattedKeyByName(const char *s, FieldType forType) {
  const FieldSpec *fs = GetField(s, strlen(s));
  if (!fs) {
    return NULL;
  }
  return GetFormattedKey(fs, forType);
}

//---------------------------------------------------------------------------------------------

// Parse the field mask passed to a query, map field names to a bit mask passed down to the
// execution engine, detailing which fields the query works on. See FT.SEARCH for API details

t_fieldMask IndexSpec::ParseFieldMask(RedisModuleString **argv, int argc) {
  t_fieldMask ret = 0;

  for (int i = 0; i < argc; i++) {
    size_t len;
    const char *p = RedisModule_StringPtrLen(argv[i], &len);

    ret |= GetFieldBit(p, len);
  }

  return ret;
}

//---------------------------------------------------------------------------------------------

void IndexSpec::InitializeSynonym() {
  if (!smap) {
    smap = new SynonymMap(false);
    flags |= Index_HasSmap;
  }
}

//---------------------------------------------------------------------------------------------

// Parse a new stopword list and set it. If the parsing fails we revert to the default stopword
// list, and return 0

bool IndexSpec::ParseStopWords(RedisModuleString **strs, size_t len) {
  // if the index already has custom stopwords, let us free them first
  if (stopwords) {
    delete stopwords;
    stopwords = NULL;
  }

  stopwords = new StopWordList(strs, len);
  // on failure we revert to the default stopwords list
  if (stopwords == NULL) {
    stopwords = DefaultStopWordList();
    flags &= ~Index_HasCustomStopwords;
    return false;
  } else {
    flags |= Index_HasCustomStopwords;
  }
  return true;
}

//---------------------------------------------------------------------------------------------

bool IndexSpec::IsStopWord(const char *term, size_t len) {
  if (!stopwords) {
    return false;
  }
  return stopwords->Contains(term, len);
}

//---------------------------------------------------------------------------------------------

void IndexSpec::ctor(const char *name) {
  fields = rm_calloc(sizeof(FieldSpec), SPEC_MAX_FIELDS);
  sortables = new RSSortingTable();
  flags = INDEX_DEFAULT_FLAGS;
  name = rm_strdup(name);
  docs = new DocTable();
  stopwords = DefaultStopWordList();
  terms = new Trie();
  minPrefix = RSGlobalConfig.minTermPrefix;
  maxPrefixExpansions = RSGlobalConfig.maxPrefixExpansions;
  getValue = NULL;
  getValueCtx = NULL;
  memset(&stats, 0, sizeof(stats));
}

//---------------------------------------------------------------------------------------------

FieldSpec *IndexSpec::CreateField(const char *name) {
  fields = rm_realloc(fields, sizeof(*fields) * (numFields + 1));
  FieldSpec *fs = fields + numFields;
  memset(fs, 0, sizeof(*fs));
  fs->index = numFields++;
  fs->name = rm_strdup(name);
  fs->ftId = (t_fieldId)-1;
  fs->ftWeight = 1.0;
  fs->sortIdx = -1;
  fs->tagFlags = TAG_FIELD_DEFAULT_FLAGS;
  fs->tagFlags = TAG_FIELD_DEFAULT_SEP;
  return fs;
}

//---------------------------------------------------------------------------------------------

// Indicate the index spec should use an internal dictionary, rather than the Redis keyspace

void IndexSpec::MakeKeyless() {
  keysDict.clear();
}

//---------------------------------------------------------------------------------------------

void IndexSpec::StartGCFromSpec(float initialHZ, uint32_t gcPolicy) {
  gc = new GC(this, initialHZ, uniqueId, gcPolicy);
  gc->Start();
}

//---------------------------------------------------------------------------------------------

/* Start the garbage collection loop on the index spec. The GC removes garbage data left on the
 * index after removing documents */
void IndexSpec::StartGC(RedisModuleCtx *ctx, float initialHZ) {
  RS_LOG_ASSERT(!gc, "GC already exists");
  // we will not create a gc thread on temporary index
  if (RSGlobalConfig.enableGC && !(flags & Index_Temporary)) {
    RedisModuleString *keyName = RedisModule_CreateString(ctx, name, strlen(name));
    gc = new GC(keyName, initialHZ, uniqueId);
    gc->Start();
    RedisModule_Log(ctx, "verbose", "Starting GC for index %s", name);
  }
}

//---------------------------------------------------------------------------------------------

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

//---------------------------------------------------------------------------------------------

static void FieldSpec_RdbSave(RedisModuleIO *rdb, FieldSpec *f) {
  RedisModule_SaveStringBuffer(rdb, f->name, strlen(f->name) + 1);
  RedisModule_SaveUnsigned(rdb, f->types);
  RedisModule_SaveUnsigned(rdb, f->options);
  RedisModule_SaveSigned(rdb, f->sortIdx);
  // Save text specific options
  if (f->IsFieldType(INDEXFLD_T_FULLTEXT) || (f->options & FieldSpec_Dynamic)) {
    RedisModule_SaveUnsigned(rdb, f->ftId);
    RedisModule_SaveDouble(rdb, f->ftWeight);
  }
  if (f->IsFieldType(INDEXFLD_T_TAG) || (f->options & FieldSpec_Dynamic)) {
    RedisModule_SaveUnsigned(rdb, f->tagFlags);
    RedisModule_SaveStringBuffer(rdb, &f->tagSep, 1);
  }
}

//---------------------------------------------------------------------------------------------

extern const FieldType fieldTypeMap[];

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
  if (f->IsFieldType(INDEXFLD_T_FULLTEXT) || (f->options & FieldSpec_Dynamic)) {
    f->ftId = RedisModule_LoadUnsigned(rdb);
    f->ftWeight = RedisModule_LoadDouble(rdb);
  }
  // Load tag specific options
  if (f->IsFieldType(INDEXFLD_T_TAG) || (f->options & FieldSpec_Dynamic)) {
    f->tagFlags = RedisModule_LoadUnsigned(rdb);
    // Load the separator
    size_t l;
    char *s = RedisModule_LoadStringBuffer(rdb, &l);
    RS_LOG_ASSERT(l == 1, "buffer length should be 1");
    f->tagSep = *s;
    RedisModule_Free(s);
  }
}

//---------------------------------------------------------------------------------------------

void IndexStats::RdbLoad(RedisModuleIO *rdb) {
  numDocuments = RedisModule_LoadUnsigned(rdb);
  numTerms = RedisModule_LoadUnsigned(rdb);
  numRecords = RedisModule_LoadUnsigned(rdb);
  invertedSize = RedisModule_LoadUnsigned(rdb);
  invertedCap = RedisModule_LoadUnsigned(rdb);
  skipIndexesSize = RedisModule_LoadUnsigned(rdb);
  scoreIndexesSize = RedisModule_LoadUnsigned(rdb);
  offsetVecsSize = RedisModule_LoadUnsigned(rdb);
  offsetVecRecords = RedisModule_LoadUnsigned(rdb);
  termsSize = RedisModule_LoadUnsigned(rdb);
}

//---------------------------------------------------------------------------------------------

void IndexStats::RdbSave(RedisModuleIO *rdb) {
  RedisModule_SaveUnsigned(rdb, numDocuments);
  RedisModule_SaveUnsigned(rdb, numTerms);
  RedisModule_SaveUnsigned(rdb, numRecords);
  RedisModule_SaveUnsigned(rdb, invertedSize);
  RedisModule_SaveUnsigned(rdb, invertedCap);
  RedisModule_SaveUnsigned(rdb, skipIndexesSize);
  RedisModule_SaveUnsigned(rdb, scoreIndexesSize);
  RedisModule_SaveUnsigned(rdb, offsetVecsSize);
  RedisModule_SaveUnsigned(rdb, offsetVecRecords);
  RedisModule_SaveUnsigned(rdb, termsSize);
}

///////////////////////////////////////////////////////////////////////////////////////////////

void *IndexSpec_RdbLoad(RedisModuleIO *rdb, int encver) {
  if (encver < INDEX_MIN_COMPAT_VERSION) {
    return NULL;
  }
  RedisModuleCtx *ctx = RedisModule_GetContextFromIO(rdb);
  IndexSpec *sp;
  sp->sortables = new RSSortingTable();
  sp->terms = NULL;
  sp->docs = new DocTable(1000);
  sp->name = RedisModule_LoadStringBuffer(rdb, NULL);
  char *tmpName = rm_strdup(sp->name);
  RedisModule_Free(sp->name);
  sp->name = tmpName;
  sp->flags = (IndexFlags)RedisModule_LoadUnsigned(rdb);
  sp->keysDict.clear();
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
    if (fs->IsSortable()) {
      RS_LOG_ASSERT(fs->sortIdx < RS_SORTABLES_MAX, "sorting index is too large");
      sp->sortables->fields[fs->sortIdx].name = fs->name;
      sp->sortables->fields[fs->sortIdx].type = fieldTypeToValueType(fs->types);
      sp->sortables->len = MAX(sp->sortables->len, fs->sortIdx + 1);
    }
  }

  sp->stats.RdbLoad(rdb);

  sp->docs.RdbLoad(rdb, encver);
  /* For version 3 or up - load the generic trie */
  if (encver >= 3) {
    sp->terms = TrieType_GenericLoad(rdb, 0);
  } else {
    sp->terms = new Trie();
  }

  if (sp->flags & Index_HasCustomStopwords) {
    sp->stopwords = new StopWordList(rdb, encver);
  } else {
    sp->stopwords = DefaultStopWordList();
  }

  sp->uniqueId = spec_unique_ids++;

  sp->StartGC(ctx, GC_DEFAULT_HZ);
  RedisModuleString *specKey = RedisModule_CreateStringPrintf(ctx, INDEX_SPEC_KEY_FMT, sp->name);
  RSCursors->Add(sp->name, RSCURSORS_DEFAULT_CAPACITY);
  RedisModule_FreeString(ctx, specKey);

  sp->smap = NULL;
  if (sp->flags & Index_HasSmap) {
    sp->smap = new SynonymMap(rdb, encver);
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
      int rc = IndexAlias::Add(s, sp, 0, &status);
      RedisModule_Free(s);
      RS_LOG_ASSERT(rc == REDISMODULE_OK, "adding alias to index failed");
    }
  }
  sp->indexer = new DocumentIndexer(sp);
  return sp;
}

//---------------------------------------------------------------------------------------------

void IndexSpec_RdbSave(RedisModuleIO *rdb, void *value) {

  IndexSpec *sp = value;
  // we save the name plus the null terminator
  RedisModule_SaveStringBuffer(rdb, sp->name, strlen(sp->name) + 1);
  RedisModule_SaveUnsigned(rdb, (uint)sp->flags);

  RedisModule_SaveUnsigned(rdb, sp->numFields);
  for (int i = 0; i < sp->numFields; i++) {
    FieldSpec_RdbSave(rdb, &sp->fields[i]);
  }

  sp->stats.RdbSave(rdb);
  sp->docs.RdbSave(rdb);
  // save trie of terms
  TrieType_GenericSave(rdb, sp->terms, 0);

  // If we have custom stopwords, save them
  if (sp->flags & Index_HasCustomStopwords) {
    sp->stopwords->RdbSave(rdb);
  }

  if (sp->flags & Index_HasSmap) {
    sp->smap->RdbSave(rdb);
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

//---------------------------------------------------------------------------------------------

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

//---------------------------------------------------------------------------------------------

void IndexSpec::addAlias(const char *alias) {
  char *duped = rm_strdup(alias);
  aliases = array_ensure_append(aliases, &duped, 1, char *);
}

//---------------------------------------------------------------------------------------------

void IndexSpec::delAlias(ssize_t idx) {
  aliases = array_del_fast(aliases, idx);
}

//---------------------------------------------------------------------------------------------

void IndexSpec::ClearAliases() {
  if (!aliases) {
    return;
  }
  for (size_t i = 0; i < array_len(aliases); ++i) {
    char **pp = &aliases[i];
    QueryError e;
    int rc = IndexAlias::Del(*pp, this, INDEXALIAS_NO_BACKREF, &e);
    RS_LOG_ASSERT(rc == REDISMODULE_OK, "Alias delete has failed");
    rm_free(*pp);
    // set to NULL so IndexAlias::Del skips over this
    *pp = NULL;
  }
  array_free(aliases);
}

///////////////////////////////////////////////////////////////////////////////////////////////
