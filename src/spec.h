#ifndef __SPEC_H__
#define __SPEC_H__
#include <stdlib.h>
#include <string.h>

#include "default_gc.h"
#include "redismodule.h"
#include "doc_table.h"
#include "trie/trie_type.h"
#include "sortable.h"
#include "stopwords.h"
#include "gc.h"
#include "synonym_map.h"

#define IDXFLD_LEGACY_FULLTEXT 0
#define IDXFLD_LEGACY_NUMERIC 1
#define IDXFLD_LEGACY_GEO 2
#define IDXFLD_LEGACY_TAG 3
#define IDXFLD_LEGACY_MAX 3

// New-style fields, for compatibility with multitype in 1.6
typedef enum fieldType {
  FIELD_FULLTEXT = 0x01,
  FIELD_NUMERIC = 0x02,
  FIELD_GEO = 0x04,
  FIELD_TAG = 0x08
} FieldType;

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

static const char *SpecTypeNames[] = {[FIELD_FULLTEXT] = SPEC_TEXT_STR,
                                      [FIELD_NUMERIC] = NUMERIC_STR, [FIELD_GEO] = GEO_STR,
                                      [FIELD_TAG] = SPEC_TAG_STR};

#define INDEX_SPEC_KEY_PREFIX "idx:"
#define INDEX_SPEC_KEY_FMT INDEX_SPEC_KEY_PREFIX "%s"

#define SPEC_MAX_FIELDS 1024
#define SPEC_MAX_FIELD_ID (sizeof(t_fieldMask) * 8)
// The threshold after which we move to a special encoding for wide fields
#define SPEC_WIDEFIELD_THRESHOLD 32

typedef enum {
  FieldSpec_Sortable = 0x01,
  FieldSpec_NoStemming = 0x02,
  FieldSpec_NotIndexable = 0x04,
  FieldSpec_Phonetics = 0x08,
} FieldSpecOptions;

// Specific options for text fields
typedef struct {
  // weight in frequency calculations
  double weight;
  // bitwise id for field masks
  t_fieldId id;
} TextFieldOptions;

// Flags for tag fields
typedef enum {
  TagField_CaseSensitive = 0x01,
  TagField_TrimSpace = 0x02,
  TagField_RemoveAccents = 0x04,
} TagFieldFlags;

#define TAG_FIELD_DEFAULT_FLAGS TagField_TrimSpace &TagField_RemoveAccents;

// Specific options for tag fields
typedef struct {
  char separator;
  TagFieldFlags flags;
} TagFieldOptions;

/* The fieldSpec represents a single field in the document's field spec.
Each field has a unique id that's a power of two, so we can filter fields
by a bit mask.
Each field has a type, allowing us to add non text fields in the future */
typedef struct fieldSpec {
  char *name;
  FieldType type;
  FieldSpecOptions options;

  int sortIdx;

  /**
   * Unique field index. Each field has a unique index regardless of its type
   */
  uint16_t index;

  union {
    TextFieldOptions textOpts;
    TagFieldOptions tagOpts;
  };

  // TODO: More options here..
} FieldSpec;

#define FieldSpec_IsSortable(fs) ((fs)->options & FieldSpec_Sortable)
#define FieldSpec_IsNoStem(fs) ((fs)->options & FieldSpec_NoStemming)
#define FieldSpec_IsPhonetics(fs) ((fs)->options & FieldSpec_Phonetics)
#define FieldSpec_IsIndexable(fs) (0 == ((fs)->options & FieldSpec_NotIndexable))

typedef struct {
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
} IndexStats;

typedef enum {
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
} IndexFlags;

/**
 * This "ID" type is independent of the field mask, and is used to distinguish
 * between one field and another field. For now, the ID is the position in
 * the array of fields - a detail we'll try to hide.
 */
typedef uint16_t FieldSpecDedupeArray[SPEC_MAX_FIELDS];

#define INDEX_DEFAULT_FLAGS \
  Index_StoreFreqs | Index_StoreTermOffsets | Index_StoreFieldFlags | Index_StoreByteOffsets

#define INDEX_STORAGE_MASK                                                                  \
  (Index_StoreFreqs | Index_StoreFieldFlags | Index_StoreTermOffsets | Index_StoreNumeric | \
   Index_WideSchema)

#define INDEX_CURRENT_VERSION 15

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

// Those versions contains doc table as array, we modified it to be array of linked lists
#define INDEX_MIN_COMPACTED_DOCTABLE_VERSION 12

// Versions below this one do not contains expire information
#define INDEX_MIN_EXPIRE_VERSION 13

// Multitype; not implemented in 1.4, but needs to support new constant values
#define INDEX_MIN_MULTITYPE_VERSION 14

// Minimum version for alias support
#define INDEX_MIN_ALIAS_VERSION 15

#define Index_SupportsHighlight(spec) \
  (((spec)->flags & Index_StoreTermOffsets) && ((spec)->flags & Index_StoreByteOffsets))

#define FIELD_BIT(fs) (((t_fieldMask)1) << (fs)->textOpts.id)

typedef struct {
  char *name;
  FieldSpec *fields;
  int numFields;

  IndexStats stats;
  IndexFlags flags;

  Trie *terms;

  RSSortingTable *sortables;

  DocTable docs;

  StopWordList *stopwords;

  GCContext *gc;

  SynonymMap *smap;

  uint64_t uniqueId;

  RedisModuleCtx *strCtx;
  RedisModuleString **indexStrs;
  char **aliases;
  long long timeout;
} IndexSpec;

extern RedisModuleType *IndexSpecType;

/*
 * Get a field spec by field name. Case insensitive!
 * Return the field spec if found, NULL if not
 */
FieldSpec *IndexSpec_GetField(IndexSpec *spec, const char *name, size_t len);

char *GetFieldNameByBit(IndexSpec *sp, t_fieldMask id);

/* Get the field bitmask id of a text field by name. Return 0 if the field is not found or is not a
 * text field */
t_fieldMask IndexSpec_GetFieldBit(IndexSpec *spec, const char *name, size_t len);

/* Get a sortable field's sort table index by its name. return -1 if the field was not found or is
 * not sortable */
int IndexSpec_GetFieldSortingIndex(IndexSpec *sp, const char *name, size_t len);

/* Initialize some index stats that might be useful for scoring functions */
void IndexSpec_GetStats(IndexSpec *sp, RSIndexStats *stats);
/*
 * Parse an index spec from redis command arguments.
 * Returns REDISMODULE_ERR if there's a parsing error.
 * The command only receives the relvant part of argv.
 *
 * The format currently is <field> <weight>, <field> <weight> ...
 */
IndexSpec *IndexSpec_ParseRedisArgs(RedisModuleCtx *ctx, RedisModuleString *name,
                                    RedisModuleString **argv, int argc, char **err);

FieldSpec **getFieldsByType(IndexSpec *spec, FieldType type);
int isRdbLoading(RedisModuleCtx *ctx);

/* Create a new index spec from redis arguments, set it in a redis key and start its GC.
 * If an error occurred - we set an error string in err and return NULL.
 */
IndexSpec *IndexSpec_CreateNew(RedisModuleCtx *ctx, RedisModuleString **argv, int argc, char **err);

/* Start the garbage collection loop on the index spec */
void IndexSpec_StartGC(RedisModuleCtx *ctx, IndexSpec *sp, float initialHZ);

/* Same as above but with ordinary strings, to allow unit testing */
IndexSpec *IndexSpec_Parse(const char *name, const char **argv, int argc, char **err);

/* Add fields to a redis schema */
int IndexSpec_AddFields(IndexSpec *sp, const char **argv, int argc, char **err);
int IndexSpec_AddFieldsRedisArgs(IndexSpec *sp, RedisModuleString **argv, int argc, char **err);

IndexSpec *IndexSpec_Load(RedisModuleCtx *ctx, const char *name, int openWrite);

/** Load the index as writeable */
#define INDEXSPEC_LOAD_WRITEABLE 0x01
/** Don't consult the alias table when retrieving the index */
#define INDEXSPEC_LOAD_NOALIAS 0x02
/** The name of the index is in the format of a redis string */
#define INDEXSPEC_LOAD_KEY_RSTRING 0x04

/**
 * The redis string is formatted, and is not the "plain" index name.
 * Impliest RSTRING
 */
#define INDEXSPEC_LOAD_KEY_FORMATTED 0x08

/**
 * Don't load or return the key. Should only be used in cases where the
 * spec is not persisted between threads
 */
#define INDEXSPEC_LOAD_KEYLESS 0x10

typedef struct {
  uint32_t flags;
  union {
    const char *cstring;
    RedisModuleString *rstring;
  } name;

  /** key pointer. you should close this when done with the index */
  RedisModuleKey *keyp;
  /** name of alias lookup key to use */
  const char *alookup;
} IndexLoadOptions;


/**
 * Find and load the index using the specified parameters.
 * @return the index spec, or NULL if the index does not exist
 */
IndexSpec *IndexSpec_LoadEx(RedisModuleCtx *ctx, IndexLoadOptions *options);

// Global hook called when an index spec is created
extern void (*IndexSpec_OnCreate)(const IndexSpec *sp);

int IndexSpec_AddTerm(IndexSpec *sp, const char *term, size_t len);

/* Get a random term from the index spec using weighted random. Weighted random is done by sampling
 * N terms from the index and then doing weighted random on them. A sample size of 10-20 should be
 * enough */
char *IndexSpec_GetRandomTerm(IndexSpec *sp, size_t sampleSize);
/*
 * Free an indexSpec. This doesn't free the spec itself as it's not allocated by the parser
 * and should be on the request's stack
 */
void IndexSpec_Free(void *spec);

/* Parse a new stopword list and set it. If the parsing fails we revert to the default stopword
 * list, and return 0 */
int IndexSpec_ParseStopWords(IndexSpec *sp, RedisModuleString **strs, size_t len);

/* Return 1 if a term is a stopword for the specific index */
int IndexSpec_IsStopWord(IndexSpec *sp, const char *term, size_t len);

/**
 * Returns a string suitable for indexes. This saves on string creation/destruction
 */
RedisModuleString *IndexSpec_GetFormattedKey(IndexSpec *sp, const FieldSpec *fs);

IndexSpec *NewIndexSpec(const char *name, size_t numFields);
void *IndexSpec_RdbLoad(RedisModuleIO *rdb, int encver);
void IndexSpec_RdbSave(RedisModuleIO *rdb, void *value);
void IndexSpec_Digest(RedisModuleDigest *digest, void *value);
int IndexSpec_RegisterType(RedisModuleCtx *ctx);
// void IndexSpec_Free(void *value);

void IndexSpec_ClearAliases(IndexSpec *sp);

/*
 * Parse the field mask passed to a query, map field names to a bit mask passed down to the
 * execution engine, detailing which fields the query works on. See FT.SEARCH for API details
 */
t_fieldMask IndexSpec_ParseFieldMask(IndexSpec *sp, RedisModuleString **argv, int argc);

void IndexSpec_InitializeSynonym(IndexSpec *sp);

#endif
