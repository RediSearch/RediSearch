#pragma once

#include "default_gc.h"
#include "redismodule.h"
#include "doc_table.h"
#include "trie/trie_type.h"
#include "sortable.h"
#include "stopwords.h"
#include "gc.h"
#include "synonym_map.h"
#include "query_error.h"
#include "field_spec.h"
#include "util/map.h"
#include "redisearch_api.h"

#include <stdlib.h>
#include <string.h>
#include <memory>

///////////////////////////////////////////////////////////////////////////////////////////////

#define NUMERIC_STR "NUMERIC"
#define GEO_STR "GEO"

#define SPEC_NOOFFSETS_STR "NOOFFSETS"
#define SPEC_NOFIELDS_STR "NOFIELDS"
#define SPEC_NOFREQS_STR "NOFREQS"
#define SPEC_NOHL_STR "NOHL"
#define SPEC_SCHEMA_STR "SCHEMA"
#define SPEC_SCHEMA_EXPANDABLE_STR "MAXTEXTFIELDS"
#define SPEC_TEMPORARY_STR "TEMPORARY"
#define SPEC_TEXT_STR "TEXT"
#define SPEC_WEIGHT_STR "WEIGHT"
#define SPEC_NOSTEM_STR "NOSTEM"
#define SPEC_PHONETIC_STR "PHONETIC"
#define SPEC_TAG_STR "TAG"
#define SPEC_SORTABLE_STR "SORTABLE"
#define SPEC_STOPWORDS_STR "STOPWORDS"
#define SPEC_NOINDEX_STR "NOINDEX"
#define SPEC_SEPARATOR_STR "SEPARATOR"
#define SPEC_MULTITYPE_STR "MULTITYPE"

//---------------------------------------------------------------------------------------------

// If wishing to represent field types positionally, use this enum.
// Since field types are a bitmask, it's pointless to waste space like this.

extern const char *SpecTypeNames[];

#define INDEX_SPEC_KEY_PREFIX "idx:"
#define INDEX_SPEC_KEY_FMT INDEX_SPEC_KEY_PREFIX "%s"
#define INDEX_SPEC_ALIASES "$idx:aliases$"

#define SPEC_MAX_FIELDS 1024
#define SPEC_MAX_FIELD_ID (sizeof(t_fieldMask) * 8)
// The threshold after which we move to a special encoding for wide fields
#define SPEC_WIDEFIELD_THRESHOLD 32

//---------------------------------------------------------------------------------------------

struct IndexStats {
  size_t numDocuments;
  size_t numTerms;
  size_t numRecords;
  size_t invertedSize;
  size_t invertedCap;
  size_t skipIndexesSize;
  size_t scoreIndexesSize;
  size_t offsetVecsSize;
  size_t offsetVecRecords;
  size_t termsSize;

  IndexStats() {}
  IndexStats(RedisModuleIO *rdb) { RdbLoad(rdb); }

  void RdbLoad(RedisModuleIO *rdb);
  void RdbSave(RedisModuleIO *rdb);
};

//---------------------------------------------------------------------------------------------

enum IndexFlags {
  Index_StoreTermOffsets = 0x01,
  Index_StoreFieldFlags = 0x02,

  // Was StoreScoreIndexes, but these are always stored, so this option is unused
  Index__Reserved1 = 0x04,
  Index_HasCustomStopwords = 0x08,
  Index_StoreFreqs = 0x010,
  Index_StoreNumeric = 0x020,
  Index_StoreByteOffsets = 0x40,
  Index_WideSchema = 0x080,
  Index_HasSmap = 0x100,
  Index_Temporary = 0x200,
  Index_DocIdsOnly = 0x00,

  // If any of the fields has phonetics. This is just a cache for quick lookup
  Index_HasPhonetic = 0x400
};

//---------------------------------------------------------------------------------------------

/**
 * This "ID" type is independent of the field mask, and is used to distinguish
 * between one field and another field. For now, the ID is the position in
 * the array of fields - a detail we'll try to hide.
 */
struct FieldSpecDedupeArray : Vector<uint16_t> {
  FieldSpecDedupeArray() {
    assign(SPEC_MAX_FIELDS, 0);
  }
};

#define INDEX_DEFAULT_FLAGS \
  Index_StoreFreqs | Index_StoreTermOffsets | Index_StoreFieldFlags | Index_StoreByteOffsets

#define INDEX_STORAGE_MASK                                                                  \
  (Index_StoreFreqs | Index_StoreFieldFlags | Index_StoreTermOffsets | Index_StoreNumeric | \
   Index_WideSchema)

#define INDEX_CURRENT_VERSION 15

// Those versions contains doc table as array, we modified it to be array of linked lists
#define INDEX_MIN_COMPACTED_DOCTABLE_VERSION 12
#define INDEX_MIN_COMPAT_VERSION 2
// Versions below this always store the frequency
#define INDEX_MIN_NOFREQ_VERSION 6
// Versions below this encode field ids as the actual value,
// above - field ides are encoded as their exponent (bit offset)
#define INDEX_MIN_WIDESCHEMA_VERSION 7

// Versions below this didn't know tag indexes
#define INDEX_MIN_TAGFIELD_VERSION 8

// Versions below this one don't save the document len when serializing the table
#define INDEX_MIN_DOCLEN_VERSION 9

#define INDEX_MIN_BINKEYS_VERSION 10

// Versions below this one do not contains expire information
#define INDEX_MIN_EXPIRE_VERSION 13

// Versions below this contain legacy types; newer versions allow a field
// to contain multiple types
#define INDEX_MIN_MULTITYPE_VERSION 14

#define INDEX_MIN_ALIAS_VERSION 15

//---------------------------------------------------------------------------------------------

#define IDXFLD_LEGACY_FULLTEXT 0
#define IDXFLD_LEGACY_NUMERIC 1
#define IDXFLD_LEGACY_GEO 2
#define IDXFLD_LEGACY_TAG 3
#define IDXFLD_LEGACY_MAX 3

//---------------------------------------------------------------------------------------------

#define Index_SupportsHighlight(spec) \
  (((spec)->flags & Index_StoreTermOffsets) && ((spec)->flags & Index_StoreByteOffsets))

struct DocumentIndexer;

typedef uint64_t IndexSpecId;

//---------------------------------------------------------------------------------------------

struct IndexSpecFmtStrings {
  RedisModuleString *types[INDEXFLD_NUM_TYPES];
};

//---------------------------------------------------------------------------------------------

struct IndexLoadOptions {
  union {
    const char *cstring;
    RedisModuleString *rstring;
  };
  uint32_t flags;

  // key pointer. you should close this when done with the index
  RedisModuleKey *keyp;
  // name of alias lookup key to use
  const char *alookup;

  IndexLoadOptions(uint32_t flags_, const char *cstring_) : cstring(cstring_), flags(flags_) {}
  IndexLoadOptions(uint32_t flags_, RedisModuleString *rstring_) : rstring(rstring_), flags(flags_) {}
};

//---------------------------------------------------------------------------------------------

struct BaseIndex : Object {
  virtual ~BaseIndex() = default;
};

//---------------------------------------------------------------------------------------------

struct IndexSpec : Object {
  typedef IndexSpecId Id;

  char *name;
  Vector<FieldSpec> fields;

  IndexStats stats;
  IndexFlags flags;

  Trie *terms;

  RSSortingTable *sortables;

  DocTable docs;

  //@@ std::shared_ptr<StopWordList> stopwords;
  StopWordList *stopwords;

  GC *gc;

  SynonymMap *smap;

  Id uniqueId;

  // cached strings, corresponding to number of fields
  IndexSpecFmtStrings *indexStrs;
  std::shared_ptr<struct IndexSpecFields> spcache;
  long long timeout;
  UnorderedMap<RedisModuleString*, BaseIndex*> keysDict;

  RSGetValueCallback getValue; //@@TODO: check this out
  void *getValueCtx;

  char **aliases; // Aliases to self-remove when the index is deleted
  std::shared_ptr<DocumentIndexer> indexer;

  static IndexSpec *Load(RedisModuleCtx *ctx, const char *name, int openWrite);
  static IndexSpec *LoadEx(RedisModuleCtx *ctx, IndexLoadOptions *options);

  void Parse(const char *name, const char **argv, int argc, QueryError *status);
  void ParseRedisArgs(RedisModuleCtx *ctx, RedisModuleString *name,
                      RedisModuleString **argv, int argc, QueryError *status);

  IndexSpec(const char *name);
  IndexSpec(const char *name, const char **argv, int argc, QueryError *status)
    : IndexSpec(name) {
    Parse(name, argv, argc, status);
  }
  IndexSpec(
    RedisModuleCtx *ctx, RedisModuleString *name,
    RedisModuleString **argv, int argc, QueryError *status
  ) : IndexSpec(RedisModule_StringPtrLen(name, nullptr)) {
    ParseRedisArgs(ctx, name, argv, argc, status);
  }
  IndexSpec(RedisModuleCtx *ctx, RedisModuleString **argv, int argc, QueryError *status);

  void FreeInternals();
  void FreeAsync();
  void FreeSync();
  void FreeWithKey(RedisModuleCtx *ctx);

  ~IndexSpec();

  std::shared_ptr<IndexSpecFields> GetSpecCache() const;

  FieldSpec CreateField(const char *name);

  const FieldSpec *GetField(std::string_view name) const;
  const FieldSpec * GetFieldCase(std::string_view name) const;
  const FieldSpec *GetFieldBySortingIndex(uint16_t idx) const;
  String GetFieldNameByBit(t_fieldMask id) const;
  int GetFieldSortingIndex(std::string_view name);
  t_fieldMask GetFieldBit(std::string_view name) const;
  const IndexStats &GetStats() const;

  void StartGCFromSpec(float initialHZ, uint32_t gcPolicy);
  void StartGC(RedisModuleCtx *ctx, float initialHZ);

  const FieldSpec *getFieldCommon(std::string_view name, bool useCase) const;
  Vector<FieldSpec> getFieldsByType(FieldType type);

  bool CheckPhoneticEnabled(t_fieldMask fm) const;

  void MakeKeyless();
  int CreateTextId() const;
  bool IsKeyless() { return keysDict.empty(); }

  int AddTerm(const char *term, size_t len);
  char *GetRandomTerm(size_t sampleSize);

  bool AddFieldsInternal(ArgsCursor *ac, QueryError *status, bool isNew);
  bool AddFields(ArgsCursor *ac, QueryError *status);

  bool ParseStopWords(RedisModuleString **strs, size_t len);
  bool IsStopWord(std::string_view term);

  void ClearAliases();
  void InitializeSynonym();

  void addAlias(const char *alias);
  void delAlias(ssize_t idx);

  RedisModuleString *GetFormattedKey(const FieldSpec &fs, FieldType forType);
  RedisModuleString *GetFormattedKeyByName(const char *s, FieldType forType);

  void writeIndexEntry(struct InvertedIndex *idx, const struct ForwardIndexEntry &entry);
};

//---------------------------------------------------------------------------------------------

struct KeysDictValue {
  void (*dtor)(void *p);
  void *p;
};

//---------------------------------------------------------------------------------------------

extern RedisModuleType *IndexSpecType;
extern RedisModuleType *IndexAliasType;

/**
 * This lightweight object contains a COPY of the actual index spec.
 * This makes it safe for other modules to use for information such as
 * field names, WITHOUT worrying about the index schema changing.
 *
 * If the index schema changes, this object is simply recreated rather
 * than modified, making it immutable.
 *
 * It is freed when its reference count hits 0
 */

struct IndexSpecFields : Vector<FieldSpec> {
  IndexSpecFields(const Vector<FieldSpec> &fields_) {
    reserve(fields_.size());
    for (auto const &f : fields_) {
      emplace_back(f.index, f.name);
    }
  }
};

//---------------------------------------------------------------------------------------------

int isRdbLoading(RedisModuleCtx *ctx);

//---------------------------------------------------------------------------------------------

// Load the index as writeable
#define INDEXSPEC_LOAD_WRITEABLE 0x01
// Don't consult the alias table when retrieving the index
#define INDEXSPEC_LOAD_NOALIAS 0x02
// The name of the index is in the format of a redis string
#define INDEXSPEC_LOAD_KEY_RSTRING 0x04

// The redis string is formatted, and is not the "plain" index name. Impliest RSTRING.
#define INDEXSPEC_LOAD_KEY_FORMATTED 0x08

// Don't load or return the key. Should only be used in cases where the
// spec is not persisted between threads
#define INDEXSPEC_LOAD_KEYLESS 0x10

//---------------------------------------------------------------------------------------------

// Global hook called when an index spec is created
extern void (*IndexSpec_OnCreate)(const IndexSpec *sp);

//---------------------------------------------------------------------------------------------

void *IndexSpec_RdbLoad(RedisModuleIO *rdb, int encver);
void IndexSpec_RdbSave(RedisModuleIO *rdb, void *value);
int IndexSpec_RegisterType(RedisModuleCtx *ctx);

//---------------------------------------------------------------------------------------------

bool checkPhoneticAlgorithmAndLang(const char *matcher);

///////////////////////////////////////////////////////////////////////////////////////////////
