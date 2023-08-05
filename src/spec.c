#include "alias.h"
#include "config.h"
#include "cursor.h"
#include "indexer.h"
#include "module.h"
#include "numeric_index.h"
#include "redis_index.h"
#include "rmalloc.h"
#include "spec.h"
#include "tag_index.h"

#include "trie/trie_type.h"

#include "util/misc.h"
#include "util/logging.h"
#include "util/strconv.h"

#include "rmutil/util.h"
#include "rmutil/vector.h"

#include <math.h>
#include <ctype.h>
#include <assert.h>

///////////////////////////////////////////////////////////////////////////////////////////////

void (*IndexSpec_OnCreate)(const IndexSpec *) = nullptr;

RedisModuleType *IndexSpecType;
static uint64_t spec_unique_ids = 1;

//---------------------------------------------------------------------------------------------

const FieldSpec *IndexSpec::getFieldCommon(std::string_view name, bool useCase) const {
  for (size_t i = 0; i < fields.size(); i++) {
    const FieldSpec &fs = fields[i];
    if (name.length() != fs.name.length()) {
      continue;
    }
    if (useCase) {
      if (!strcmp(fs.name.c_str(), name.data())) {
        return &fs;
      }
    } else {
      if (!str_casecmp(name, fs.name)) {
        return &fs;
      }
    }
  }
  return nullptr;
}

//---------------------------------------------------------------------------------------------

// Get a field spec by field name. Case insensitive!
// Return the field spec if found, nullptr if not

const FieldSpec *IndexSpec::GetField(std::string_view name) const {
  return getFieldCommon(name, false);
};

//---------------------------------------------------------------------------------------------

// Case-sensitive version of GetField()

const FieldSpec *IndexSpec::GetFieldCase(std::string_view name) const {
  return getFieldCommon(name, true);
}

//---------------------------------------------------------------------------------------------

// Get the field bitmask id of a text field by name.
// Return 0 if the field is not found or is not a text field.

t_fieldMask IndexSpec::GetFieldBit(std::string_view name) const {
  const FieldSpec *fs = GetField(name);
  if (!fs || !fs->IsFieldType(INDEXFLD_T_FULLTEXT) || !fs->IsIndexable()) return 0;

  return fs->FieldBit();
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

  for (size_t ii = 0; ii < fields.size(); ++ii) {
    if (fm & ((t_fieldMask)1 << ii)) {
      const FieldSpec &fs = fields[ii];
      if (fs.IsFieldType(INDEXFLD_T_FULLTEXT) && (fs.IsPhonetics())) {
        return true;
      }
    }
  }
  return false;
}

//---------------------------------------------------------------------------------------------

// Get a sortable field's sort table index by its name. return -1 if the field was not found or is
// not sortable.

int IndexSpec::GetFieldSortingIndex(std::string_view name) {
  if (!sortables) return -1;
  return sortables->GetFieldIdx(name);
}

//---------------------------------------------------------------------------------------------

// Get the field spec from the sortable index

const FieldSpec *IndexSpec::GetFieldBySortingIndex(uint16_t idx) const {
  for (auto const &field : fields) {
    if (field.options & FieldSpec_Sortable && field.sortIdx == idx) {
      return &field;
    }
  }
  return nullptr;
}

//---------------------------------------------------------------------------------------------

String IndexSpec::GetFieldNameByBit(t_fieldMask id) const {
  for (auto const &field : fields) {
    if (field.FieldBit() == id && field.IsFieldType(INDEXFLD_T_FULLTEXT) && field.IsIndexable()) {
      return field.name;
    }
  }
  return "";
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
    args[i] = RedisModule_StringPtrLen(argv[i], nullptr);
  }

  Parse(RedisModule_StringPtrLen(name, nullptr), args, argc, status);
}

//---------------------------------------------------------------------------------------------

Vector<FieldSpec> IndexSpec::getFieldsByType(FieldType type) {
  Vector<FieldSpec> res;
  for (auto const &fs: fields) {
    if (fs.IsFieldType(type)) {
      res.emplace_back(fs);
    }
  }
  return res;
}

//---------------------------------------------------------------------------------------------

// Check if Redis is currently loading from RDB. Our thread starts before RDB loading is finished

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

IndexSpec::IndexSpec(
  RedisModuleCtx *ctx, RedisModuleString **argv, int argc, QueryError *status
) : IndexSpec(RedisModule_StringPtrLen(argv[1], nullptr)) {
  ParseRedisArgs(ctx, argv[1], &argv[2], argc - 2, status);

  RedisModuleString *keyString = RedisModule_CreateStringPrintf(ctx, INDEX_SPEC_KEY_FMT, name);
  RedisModuleKey *k = RedisModule_OpenKey(ctx, keyString, REDISMODULE_READ | REDISMODULE_WRITE);
  RedisModule_FreeString(ctx, keyString);

  // check that the key is empty
  if (k == nullptr || (RedisModule_KeyType(k) != REDISMODULE_KEYTYPE_EMPTY)) {
    if (RedisModule_ModuleTypeGetType(k) != IndexSpecType) {
      status->SetCode(QUERY_EREDISKEYTYPE);
    } else {
      status->SetCode(QUERY_EINDEXEXISTS);
    }
    if (k) {
      RedisModule_CloseKey(k);
    }

    throw Error("Key already exist");
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
  indexer = std::make_shared<DocumentIndexer>(*this);
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

bool checkPhoneticAlgorithmAndLang(const char *matcher) {
  if (strlen(matcher) != 5) {
    return false;
  }
  if (matcher[0] != 'd' || matcher[1] != 'm' || matcher[2] != ':') {
    return false;
  }

#define LANGUAGES_SIZE 4
  const char *languages[] = {"en", "pt", "fr", "es"};

  bool langauge_found = false;
  for (int i = 0; i < LANGUAGES_SIZE; ++i) {
    if (matcher[3] == languages[i][0] && matcher[4] == languages[i][1]) {
      langauge_found = true;
    }
  }

  return langauge_found;
}

//---------------------------------------------------------------------------------------------

// Gets the next text id from the index. This does not currently modify the index

int IndexSpec::CreateTextId() const {
  int maxId = -1;
  for (auto const &fs : fields) {
    if (fs.IsFieldType(INDEXFLD_T_FULLTEXT)) {
      if (fs.ftId == (t_fieldId)-1) {
        // ignore
        continue;
      }
      maxId = MAX(fs.ftId, maxId);
    }
  }

  if (maxId + 1 >= SPEC_MAX_FIELD_ID) {
    return -1;
  }

  return maxId + 1;
}

//---------------------------------------------------------------------------------------------

// Add fields to an existing (or newly created) index.

bool IndexSpec::AddFieldsInternal(ArgsCursor *ac, QueryError *status, bool isNew) {
  if (spcache) {
    // delete spcache;
    spcache.reset();
  }

  const size_t prevSortLen = sortables->len;

  while (!ac->IsAtEnd()) {
    size_t nfieldName = 0;
    const char *fieldName = ac->GetStringNC(&nfieldName);
    if (GetField(fieldName)) {
      status->SetError(QUERY_EINVAL, "Duplicate field in schema");
      goto reset;
    }

    try {
      fields.emplace_back(fieldName, this, ac, status, isNew);
    } catch (Error &x) {
      goto reset;
    }
  }

  return true;

reset:
  sortables->len = prevSortLen;
  return false;
}

// Add fields to a redis schema
bool IndexSpec::AddFields(ArgsCursor *ac, QueryError *status) {
  return AddFieldsInternal(ac, status, false);
}

//---------------------------------------------------------------------------------------------

/* The format currently is FT.CREATE {index} [NOOFFSETS] [NOFIELDS]
    SCHEMA {field} [TEXT [WEIGHT {weight}]] | [NUMERIC]
  */
void IndexSpec::Parse(const char *name, const char **argv, int argc, QueryError *status) {
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
      {name: nullptr}};

  ACArgSpec *errarg = nullptr;
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
    // Can't remove the global default stopwords list!
    // if (stopwords) {
    //   delete stopwords;
    //   stopwords = nullptr;
    // }
    stopwords = new StopWordList((const char **)acStopwords.objs, acStopwords.argc);
    flags |= Index_HasCustomStopwords;
  }

  if (!ac.AdvanceIfMatch(SPEC_SCHEMA_STR)) {
    if (ac.NumRemaining()) {
      const char *badarg = ac.GetStringNC(nullptr);
      status->SetErrorFmt(QUERY_EPARSEARGS, "Unknown argument `%s`", badarg);
    } else {
      status->SetError(QUERY_EPARSEARGS, "No schema found");
    }
    throw Error("");
  }

  if (!AddFieldsInternal(&ac, status, true)) {
    throw Error("Error creating FieldSpec");
  }
}

//---------------------------------------------------------------------------------------------

// Initialize some index stats that might be useful for scoring functions

ScorerArgs::Stats::Stats(const IndexStats &stats)
  : numDocs{stats.numDocuments}
  , numTerms{stats.numTerms}
  , avgDocLen{stats.numDocuments ? (double)stats.numRecords/stats.numDocuments : 0}
{}

//---------------------------------------------------------------------------------------------

int IndexSpec::AddTerm(const char *term, size_t len) {
  int isNew = terms->InsertStringBuffer((char *)term, len, 1, 1, nullptr);
  if (isNew) {
    stats.numTerms++;
    stats.termsSize += len;
  }
  return isNew;
}

//---------------------------------------------------------------------------------------------

// Retrieves the current spec cache from the index, incrementing its
// reference count by 1. Use IndexSpecCache_Decref to free

std::shared_ptr<IndexSpecFields> IndexSpec::GetSpecCache() const {
  if (!spcache) {
    ((IndexSpec *)this)->spcache = std::make_shared<IndexSpecFields>(fields);;
  }

  return spcache;
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
// should be enough. Returns nullptr if the index is empty

char *IndexSpec::GetRandomTerm(size_t sampleSize) {
  if (sampleSize > terms->size) {
    sampleSize = terms->size;
  }
  if (!sampleSize) return nullptr;

  char *samples[sampleSize];
  double weights[sampleSize];
  for (int i = 0; i < sampleSize; i++) {
    char *ret = nullptr;
    t_len len = 0;
    double d = 0;
    if (!terms->RandomKey(&ret, &len, &d) || len == 0) {
      return nullptr;
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

  return samples[selection];
}

//---------------------------------------------------------------------------------------------

// Delete the redis key from Redis

void IndexSpec::FreeWithKey(RedisModuleCtx *ctx) {
  RedisModuleString *s = RedisModule_CreateStringPrintf(ctx, INDEX_SPEC_KEY_FMT, name);
  RedisModuleKey *kk = RedisModule_OpenKey(ctx, s, REDISMODULE_WRITE);
  RedisModule_FreeString(ctx, s);
  if (kk == nullptr || RedisModule_KeyType(kk) != REDISMODULE_KEYTYPE_MODULE ||
      RedisModule_ModuleTypeGetType(kk) != IndexSpecType) {
    if (kk != nullptr) {
      RedisModule_CloseKey(kk);
    }
    delete this;
    return;
  }
  if (RedisModule_ModuleTypeGetValue(kk) != this) throw Error("IndexSpecs should be identical");
  RedisModule_DeleteKey(kk);
  RedisModule_CloseKey(kk);
}

//---------------------------------------------------------------------------------------------

void IndexSpec::FreeInternals() {
  if (!!indexer) {
    indexer->Stop();
  }
  if (gc) {
    gc->Stop();
  }

  delete terms;
  terms = nullptr;

  if (uniqueId) {
    // If uniqueid is 0, it means the index was not initialized
    // and is being freed now during an error.
    RSCursors->Purge(name);
    RSCursors->Remove(name);
  }

  rm_free(name);

  delete sortables;
  sortables = nullptr;

  if (stopwords != DefaultStopWordList()) {
    delete stopwords;
    stopwords = nullptr;
  }

  delete smap;
  smap = nullptr;

  if (spcache) {
    // delete spcache;
    spcache.reset();
  }

  if (indexStrs) {
    for (size_t ii = 0; ii < fields.size(); ++ii) {
      IndexSpecFmtStrings *fmts = indexStrs + ii;
      for (size_t jj = 0; jj < INDEXFLD_NUM_TYPES; ++jj) {
        if (fmts->types[jj]) {
          RedisModule_FreeString(RSDummyContext, fmts->types[jj]);
        }
      }
    }
    rm_free(indexStrs);
  }

  ClearAliases();

  keysDict.clear();
}

//---------------------------------------------------------------------------------------------

// Free the index synchronously. Any keys associated with the index (but not the
// documents themselves) are freed before this function returns.

static void IndexSpec_FreeAsync(void *data) {
  IndexSpec *spec = static_cast<IndexSpec *>(data);
  RedisModuleCtx *threadCtx = RedisModule_GetThreadSafeContext(nullptr);
  RedisSearchCtx sctx{threadCtx, spec};
  RedisModule_AutoMemory(threadCtx);
  RedisModule_ThreadSafeContextLock(threadCtx);

  Redis_DropIndex(&sctx, true, false);
  spec->FreeInternals();

  RedisModule_ThreadSafeContextUnlock(threadCtx);
  RedisModule_FreeThreadSafeContext(threadCtx);
}

//---------------------------------------------------------------------------------------------

static struct thpool_ *cleanPool = nullptr;

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
  IndexSpec *spec = static_cast<IndexSpec *>(ctx);
  delete spec;
}

//---------------------------------------------------------------------------------------------

void IndexSpec::FreeSync() {
  //  todo:
  //  mark I think we only need FreeInternals, this is called only from the
  //  LLAPI and there is no need to drop keys cause its out of the key space.
  //  Let me know what you think

  //   Need a context for this:
  RedisModuleCtx *ctx = RedisModule_GetThreadSafeContext(nullptr);
  RedisSearchCtx sctx{ctx, this};
  RedisModule_AutoMemory(ctx);
  if (!IsKeyless()) {
    Redis_DropIndex(&sctx, 0, 1);
  }
  FreeInternals();
  RedisModule_FreeThreadSafeContext(ctx);
}

//---------------------------------------------------------------------------------------------

IndexSpec *IndexSpec::LoadEx(RedisModuleCtx *ctx, IndexLoadOptions *options) {
  IndexSpec *ret = nullptr;
  int modeflags = REDISMODULE_READ | REDISMODULE_WRITE;

  if (options->flags & INDEXSPEC_LOAD_WRITEABLE) {
    modeflags |= REDISMODULE_WRITE;
  }

  RedisModuleString *formatted;
  bool isKeynameOwner = false;
  const char *ixname = nullptr;

  if (options->flags & INDEXSPEC_LOAD_KEY_FORMATTED) {
    formatted = options->rstring;
  } else {
    isKeynameOwner = true;
    if (options->flags & INDEXSPEC_LOAD_KEY_RSTRING) {
      ixname = RedisModule_StringPtrLen(options->rstring, nullptr);
    } else {
      ixname = options->cstring;
    }
    formatted = RedisModule_CreateStringPrintf(ctx, INDEX_SPEC_KEY_FMT, ixname);
  }

  options->keyp = RedisModule_OpenKey(ctx, formatted, modeflags);
  // we do not allow empty indexes when loading an existing index
  if (options->keyp == nullptr || RedisModule_KeyType(options->keyp) == REDISMODULE_KEYTYPE_EMPTY) {
    if (options->keyp) {
      RedisModule_CloseKey(options->keyp);
      options->keyp = nullptr;
    }
    if ((options->flags & INDEXSPEC_LOAD_NOALIAS) || ixname == nullptr) {
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
    ret = static_cast<IndexSpec *>(RedisModule_ModuleTypeGetValue(options->keyp));
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
    options->keyp = nullptr;
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

// Returns a string suitable for indexes. This saves on string creation/destruction

RedisModuleString *IndexSpec::GetFormattedKey(const FieldSpec &fs, FieldType forType) {
  if (!indexStrs) {
    indexStrs = static_cast<IndexSpecFmtStrings *>(rm_calloc(SPEC_MAX_FIELDS, sizeof *indexStrs));
  }

  size_t typeix = INDEXTYPE_TO_POS(forType);
  RedisModuleString *ret = indexStrs[fs.index].types[typeix];
  if (!ret) {
    RedisSearchCtx sctx(RSDummyContext, this);
    switch (forType) {
      case INDEXFLD_T_NUMERIC:
        ret = sctx.NumericIndexKey(fs.name);
        break;
      case INDEXFLD_T_TAG:
        ret = TagIndex::FormatName(&sctx, fs.name);
        break;
      case INDEXFLD_T_GEO:
        ret = RedisModule_CreateStringPrintf(RSDummyContext, GEOINDEX_KEY_FMT, name, fs.name.c_str());
        break;
      case INDEXFLD_T_FULLTEXT:  // Text fields don't get a per-field index
      default:
        ret = nullptr;
        abort();
        break;
    }
  }
  if (!ret) {
    return nullptr;
  }
  indexStrs[fs.index].types[typeix] = ret;
  return ret;
}

//---------------------------------------------------------------------------------------------

RedisModuleString *IndexSpec::GetFormattedKeyByName(const char *s, FieldType forType) {
  const FieldSpec *fs = GetField(s);
  if (!fs) {
    return nullptr;
  }
  return GetFormattedKey(*fs, forType);
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
    stopwords = nullptr;
  }

  stopwords = new StopWordList(strs, len);
  // on failure we revert to the default stopwords list
  if (stopwords == nullptr) {
    stopwords = DefaultStopWordList();
    flags &= ~Index_HasCustomStopwords;
    return false;
  } else {
    flags |= Index_HasCustomStopwords;
  }
  return true;
}

//---------------------------------------------------------------------------------------------

bool IndexSpec::IsStopWord(std::string_view term) {
  if (!stopwords) {
    return false;
  }
  return stopwords->Contains(term);
}

//---------------------------------------------------------------------------------------------

IndexSpec::IndexSpec(const char *name_)
  : name{rm_strdup(name_)}
  , fields{}
  , stats{}
  , flags{INDEX_DEFAULT_FLAGS}
  , terms{new Trie()}
  , sortables{new RSSortingTable()}
  , stopwords{DefaultStopWordList()}
  , getValue{nullptr}
  , getValueCtx{nullptr}
{
  fields.reserve(SPEC_MAX_FIELDS);
  // memset(&stats, 0, sizeof(stats));
}

//---------------------------------------------------------------------------------------------

FieldSpec IndexSpec::CreateField(const char *name) {
  return FieldSpec(fields.size(), name);
}

//---------------------------------------------------------------------------------------------

// Indicate the index spec should use an internal dictionary, rather than the Redis keyspace

void IndexSpec::MakeKeyless() {
  keysDict.clear();
}

//---------------------------------------------------------------------------------------------

void IndexSpec::StartGCFromSpec(float initialHZ, uint32_t gcPolicy) {
#ifndef NO_GC
  gc = new GC(this, initialHZ, uniqueId, gcPolicy);
  gc->Start();
#else
  gc = 0;
#endif
}

//---------------------------------------------------------------------------------------------

/* Start the garbage collection loop on the index spec. The GC removes garbage data left on the
 * index after removing documents */
void IndexSpec::StartGC(RedisModuleCtx *ctx, float initialHZ) {
  if (gc) throw Error("GC already exists");
  // we will not create a gc thread on temporary index
  if (RSGlobalConfig.enableGC && !(flags & Index_Temporary)) {
    RedisModuleString *keyName = RedisModule_CreateString(ctx, name, strlen(name));
#ifndef NO_GC
    gc = new GC(keyName, initialHZ, uniqueId);
    gc->Start();
#else
    gc = 0;
#endif
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
  f->name = RedisModule_LoadStringBuffer(rdb, nullptr);
  // the old versions encoded the bit id of the field directly
  // we convert that to a power of 2
  if (encver < INDEX_MIN_WIDESCHEMA_VERSION) {
    f->ftId = bit(RedisModule_LoadUnsigned(rdb));
  } else {
    // the new version encodes just the power of 2 of the bit
    f->ftId = RedisModule_LoadUnsigned(rdb);
  }
  f->types = static_cast<FieldType>(RedisModule_LoadUnsigned(rdb));
  f->ftWeight = RedisModule_LoadDouble(rdb);
  f->tagFlags = TAG_FIELD_DEFAULT_FLAGS;
  f->tagSep = TAG_FIELD_DEFAULT_SEP;
  if (encver >= 4) {
    f->options = static_cast<FieldSpecOptions>(RedisModule_LoadUnsigned(rdb));
    f->sortIdx = RedisModule_LoadSigned(rdb);
  }
}

//---------------------------------------------------------------------------------------------

static void FieldSpec_RdbSave(RedisModuleIO *rdb, FieldSpec *f) {
  RedisModule_SaveStringBuffer(rdb, f->name.c_str(), f->name.length() + 1);
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

  size_t nameLen;
  char *name = RedisModule_LoadStringBuffer(rdb, &nameLen);
  f->name = String{name, nameLen};
  RedisModule_Free(name);

  f->types = static_cast<FieldType>(RedisModule_LoadUnsigned(rdb));
  f->options = static_cast<FieldSpecOptions>(RedisModule_LoadUnsigned(rdb));
  f->sortIdx = RedisModule_LoadSigned(rdb);

  if (encver < INDEX_MIN_MULTITYPE_VERSION) {
    if (f->types > IDXFLD_LEGACY_MAX) throw Error("field type should be string or numeric");
    f->types = fieldTypeMap[f->types];
  }

  // Load text specific options
  if (f->IsFieldType(INDEXFLD_T_FULLTEXT) || (f->options & FieldSpec_Dynamic)) {
    f->ftId = RedisModule_LoadUnsigned(rdb);
    f->ftWeight = RedisModule_LoadDouble(rdb);
  }
  // Load tag specific options
  if (f->IsFieldType(INDEXFLD_T_TAG) || (f->options & FieldSpec_Dynamic)) {
    f->tagFlags = static_cast<TagFieldFlags>(RedisModule_LoadUnsigned(rdb));
    // Load the separator
    size_t l;
    char *s = RedisModule_LoadStringBuffer(rdb, &l);
    if (l != 1) throw Error("buffer length should be 1");
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
    return nullptr;
  }
  RedisModuleCtx *ctx = RedisModule_GetContextFromIO(rdb);

  char *name = RedisModule_LoadStringBuffer(rdb, nullptr);
  IndexSpec *sp = new IndexSpec(name);
  RedisModule_Free(name);

  sp->sortables = new RSSortingTable();
  sp->terms = nullptr;
  //sp->docs = new DocTable(1000);
  sp->flags = (IndexFlags)RedisModule_LoadUnsigned(rdb);
  sp->keysDict.clear();
  if (encver < INDEX_MIN_NOFREQ_VERSION) {
    sp->flags |= Index_StoreFreqs;
  }

  uint64_t numsFields = RedisModule_LoadUnsigned(rdb);
  sp->fields.reserve(numsFields);
  int maxSortIdx = -1;
  for (int i = 0; i < numsFields; i++) {
    auto fs = std::make_unique<FieldSpec>(i);
    FieldSpec_RdbLoad(rdb, fs.get(), encver);
    if (fs->IsSortable()) {
      if (fs->sortIdx >= RS_SORTABLES_MAX) throw Error("sorting index is too large");
      sp->sortables->fields[fs->sortIdx].name = fs->name;
      sp->sortables->fields[fs->sortIdx].type = fieldTypeToValueType(fs->types);
      sp->sortables->len = MAX(sp->sortables->len, fs->sortIdx + 1);
    }
    sp->fields.push_back(*fs);
  }

  sp->stats.RdbLoad(rdb);

  sp->docs.RdbLoad(rdb, encver);

  // For version 3 or up - load the generic trie
  if (encver >= 3) {
    sp->terms = static_cast<Trie *>(TrieType_GenericLoad(rdb, 0));
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

  sp->smap = nullptr;
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
      if (rc != REDISMODULE_OK) throw Error("adding alias to index failed");
    }
  }
  sp->indexer = std::make_shared<DocumentIndexer>(*sp);
  return sp;
}

//---------------------------------------------------------------------------------------------

void IndexSpec_RdbSave(RedisModuleIO *rdb, void *value) {

  IndexSpec *sp = static_cast<IndexSpec *>(value);
  // we save the name plus the null terminator
  RedisModule_SaveStringBuffer(rdb, sp->name, strlen(sp->name) + 1);
  RedisModule_SaveUnsigned(rdb, (uint)sp->flags);

  RedisModule_SaveUnsigned(rdb, sp->fields.size());
  for (int i = 0; i < sp->fields.size(); i++) {
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
  if (IndexSpecType == nullptr) {
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

///////////////////////////////////////////////////////////////////////////////////////////////
