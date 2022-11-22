
#pragma once

#include "object.h"
#include "score_explain.h"
#include "util/dllist.h"
#include "trie/rune_util.h"
#include "stemmer.h"
#include "rmalloc.h"
#include "rmutil/sds.h"
#include "rmutil/vector.h"

#include <stdint.h>
#include <stdlib.h>
#include <limits.h>
#include <string_view>

///////////////////////////////////////////////////////////////////////////////////////////////

template <class T = size_t>
struct IdType {
  T _id;

  explicit IdType(T id = 0): _id(id) {}
  IdType<T> &operator=(T id) { _id = id; return *this; }

  operator T() const { return _id; }
  T operator+() const { return _id; }
  IdType<T> &operator++() { ++_id; return *this; }
  //IdType<T> operator=(IdType<T> id) { _id = id; return *this; }

  bool operator==(IdType<T> id) const { return _id == id._id; }
  bool operator<(IdType<T> id) const { return _id < id._id; }
  bool operator>(IdType<T> id) const { return _id > id._id; }
};

//---------------------------------------------------------------------------------------------

#define Mask(T) unsigned int
#define Mask_i8(T) uint8_t
#define Mask_i16(T) uint16_t
#define Mask_i32(T) uint32_t

//---------------------------------------------------------------------------------------------

typedef IdType<uint64_t> t_docId;
typedef IdType<uint64_t> t_offset;

// represents id of single field. to produce field mask we calculate 2^fieldId.
typedef IdType<uint16_t> t_fieldId;

#define DOCID_MAX UINT64_MAX

#if defined(__x86_64__) && !defined(RS_NO_U128)
// 64 bit architectures use 128 bit field masks and up to 128 fields
typedef __uint128_t t_fieldMask;
// #define RS_FIELDMASK_ALL (((__uint128_t)1 << 127) - (__uint128_t)1 + ((__uint128_t)1 << 127))

#else
// 32 bit architectures use 64 bits and 64 fields only
typedef uint64_t t_fieldMask;
// #define RS_FIELDMASK_ALL 0xFFFFFFFFFFFFFFFF
#endif

#define RS_FIELDMASK_ALL (~(t_fieldMask(0)))

#define REDISEARCH_ERR 1
#define REDISEARCH_OK 0

//---------------------------------------------------------------------------------------------

struct RSSortingVector;
struct ScoreExplain;
struct ScorerArgs;
struct RedisSearchCtx;
struct QueryAST;
struct QueryError;
struct QueryNode;
struct IndexSpec;

///////////////////////////////////////////////////////////////////////////////////////////////

// payload object is set either by query expander or by user, and can be used to process scores.
// For examples, it can be a feature vector that is then compared to a feature vector
// extracted from each result or document.

struct RSPayload : Object {
  RSPayload() : data(NULL), len(0) {}
  RSPayload(const char *payload, size_t payloadSize);
  RSPayload(RedisModuleIO *rdb);
  RSPayload(struct TriePayload *payload);
  ~RSPayload();

  char *data;
  size_t len;

  size_t memsize() const { return sizeof(*this) + len; }

  operator SimpleBuff() { return SimpleBuff{data, len}; }

  void reset() {
    data = NULL;
    len = 0;
  }
};

//---------------------------------------------------------------------------------------------

// Internally used document flags

enum DocumentFlags {
  Document_DefaultFlags = 0x00,
  Document_Deleted = 0x01,
  Document_HasPayload = 0x02,
  Document_HasSortVector = 0x04,
  Document_HasOffsetVector = 0x08,

  // Whether this document has any kind of 'on-demand'
  // deletable field; this means any kind of numeric or geo
  Document_HasOnDemandDeletable = 0x10
};

//---------------------------------------------------------------------------------------------

/* DocumentMetadata describes metadata stored about a document in the index (not the document
 * itself).
 *
 * The key is the user-defined key of the document, not the incremental id.
 * It is used to convert incremental internal ids to external string keys.
 *
 * Score is the original user score as inserted to the index
 *
 * Flags is not currently used, but should be used in the future to mark documents as deleted, etc.
 */

struct DocumentMetadata : Object {
  t_docId id;

  // The actual key of the document, not the internal incremental id
  sds keyPtr;

  // The a-priory document score as given by the user on insertion
  float score;

  // The maximum frequency of any term in the index, used to normalize frequencies
  uint32_t maxFreq : 24;

  // The total weighted number of tokens in the document, weighted by field weights
  uint32_t len : 24;

  // Document flags
  Mask(DocumentFlags) flags : 8;

  // Optional user payload
  RSPayload *payload;

  struct RSSortingVector *sortVector;

  // Offsets of all terms in the document (in bytes). Used by highlighter
  struct RSByteOffsets *byteOffsets;

  List<DocumentMetadata>::iterator dmd_iter;
  //uint32_t ref_count;

  DocumentMetadata(const char *key, size_t keylen, double score, Mask(DocumentFlags) flags,
    RSPayload *payload, t_docId docId);
  DocumentMetadata(DocumentMetadata &&dmd);
  DocumentMetadata(t_docId id, RedisModuleIO *rdb, int encver);
  ~DocumentMetadata();
  //DocumentMetadata &operator=(const DocumentMetadata &dmd);

  const char *KeyPtrLen(size_t *len) const;

  RedisModuleString *CreateKeyString(RedisModuleCtx *ctx) const;

  //void Decref();
  //void Incref() { ++ref_count; }

  bool IsDeleted() const { return !!(flags & Document_Deleted); }

  size_t memsize() const { return sizeof(*this) + sdsAllocSize(keyPtr) + (payload ? payload->memsize() : 0); }

  void RdbSave(RedisModuleIO *rdb);
};

//---------------------------------------------------------------------------------------------

// We support up to 30 user given flags for each token, flags 1 and 2 are taken by the engine
typedef uint32_t RSTokenFlags;

// A token in the query.
// The expanders receive query tokens and can expand the query with more query tokens.

struct RSToken {
  String str;
  uint8_t expanded; // Is this token an expansion?
  RSTokenFlags flags; // Extension set token flags - up to 31 bits

  RSToken(const std::string_view &str, uint8_t expanded = 1, RSTokenFlags flags = 31) :
    str(str), expanded(expanded), flags(flags) {}

  RSToken(const Runes &r);
  RSToken(const rune *r, size_t n);

  size_t length() const { return str.length(); }
  const char *operator+() const { return str.c_str(); }
};

//---------------------------------------------------------------------------------------------

struct QueryExpander : virtual Object {
  QueryExpander(QueryAST *qast, RedisSearchCtx &sctx, RSLanguage lang, QueryError *status):
    qast(qast), sctx(sctx), language(lang), status(status), currentNode(NULL) {}
  virtual ~QueryExpander() = default;

  QueryAST *qast;
  RedisSearchCtx &sctx;
  QueryError *status;
  RSLanguage language;

  QueryNode *currentNode;

  virtual int Expand(RSToken *token) = 0;

  virtual void ExpandToken(std::string_view str, RSTokenFlags flags);
  virtual void ExpandTokenWithPhrase(const Vector<String> &tokens, RSTokenFlags flags, bool replace, bool exact);

  // SetPayload allows the query expander to set GLOBAL payload on the query (not unique per token)
  virtual void SetPayload(RSPayload payload);

  typedef QueryExpander *(*Factory)(QueryAST *qast, RedisSearchCtx &sctx, RSLanguage lang, QueryError *status);
};

//---------------------------------------------------------------------------------------------

#if 0

// The signature for a query expander instance
typedef int (*RSQueryTokenExpander)(QueryExpander *expander, RSToken *token);

// A free function called after the query expansion phase is over, to release per-query data
typedef void (*RSFreeFunction)(void *);

#endif // 0

//---------------------------------------------------------------------------------------------

// A single term being evaluated in query time

struct RSQueryTerm : Object {
  String str; // term string

  // Inverse document frequency of the term in the index.
  // See https://en.wikipedia.org/wiki/Tf%E2%80%93idf
  double idf;

  int id; // Each term in the query gets an incremental id
  RSTokenFlags flags; // Flags given by the engine or by the query expander

  RSQueryTerm(const RSToken &tok, int id);

  size_t length() { return str.length(); }
};

//---------------------------------------------------------------------------------------------
// Scoring Function API

// RS_OFFSETVECTOR_EOF is returned from an OffsetIterator when calling next and reaching the end.
// When calling the iterator you should check for this return value.
#define RS_OFFSETVECTOR_EOF UINT32_MAX

//---------------------------------------------------------------------------------------------

// OffsetIterator is an interface for iterating offset vectors of aggregate and token records

struct OffsetIterator {
  virtual ~OffsetIterator() {}

  virtual uint32_t Next(RSQueryTerm **term) { return RS_OFFSETVECTOR_EOF; }
  virtual void Rewind() {}

  struct Proxy {
    OffsetIterator *it;

    Proxy(OffsetIterator *it) : it(it) {}
    ~Proxy();

    OffsetIterator *operator->() { return it; }
  };
};

typedef Vector<std::unique_ptr<OffsetIterator>> RSOffsetIterators;

//---------------------------------------------------------------------------------------------

// OffsetVector represents the encoded offsets of a term in a document.
// You can read the offsets by iterating over it with OffsetVector::Iterate.

struct OffsetVector {
  OffsetVector(char *data = 0, uint32_t len = 0) : data(data), len(len) {}

  char *data;
  uint32_t len;

  // Create an offset iterator interface  from a raw offset vector
  std::unique_ptr<OffsetIterator> Iterate(RSQueryTerm *t) const;
};

//---------------------------------------------------------------------------------------------

enum RSResultType {
  RSResultType_Union = 0x1,
  RSResultType_Intersection = 0x2,
  RSResultType_Term = 0x4,
  RSResultType_Virtual = 0x8,
  RSResultType_Numeric = 0x10
};

#define RS_RESULT_AGGREGATE (RSResultType_Intersection | RSResultType_Union)

//---------------------------------------------------------------------------------------------

#pragma pack(16)

struct EmptyOffsetIterator : OffsetIterator {
};

//---------------------------------------------------------------------------------------------

struct IndexResult : Object {
  //-------------------------------------------------------------------------------------------
  // IMPORTANT: The order of the following 4 variables must remain the same, and all
  // their type aliases must remain uint32_t. The record is decoded by casting it
  // to an array of 4 uint32_t integers to avoid redundant memcpy
  //-------------------------------------------------------------------------------------------

  // The docId of the result
  t_docId docId;

  // the total frequency of all the records in this result
  uint32_t freq;

  // The aggregate field mask of all the records in this result
  t_fieldMask fieldMask;

  // For term records only.
  // This is used as an optimization, allowing the result to be loaded directly into memory.
  uint32_t offsetsSz;

  //-------------------------------------------------------------------------------------------
  // END OF the "magic 4 uints" section
  //-------------------------------------------------------------------------------------------

  RSResultType type;

  // we mark copied results so we can treat them a bit differently on deletion, and pool them if we want
  bool isCopy;

  // Relative weight for scoring calculations. This is derived from the result's iterator weight
  double weight;

  //-------------------------------------------------------------------------------------------

  //IndexResult() {}

  IndexResult(RSResultType type, t_docId docId, t_fieldMask fieldMask, uint32_t freq, double weight) :
    type(type), docId(docId), fieldMask(fieldMask), freq(freq), weight(weight), isCopy(false) {}

  IndexResult(RSResultType type, t_docId docId) : type(type), docId(docId), isCopy(false) {}

  // Create deep copy of results that is totall thread safe. Very slow so use with caution.
  IndexResult(const IndexResult &src) {
    docId = src.docId;
    freq = src.freq;
    fieldMask = src.fieldMask;
    offsetsSz = src.offsetsSz;
    type = src.type;
    weight = src.weight;
    isCopy = true;
  }

  virtual ~IndexResult() {}
  virtual IndexResult *Clone() const = 0;

  // Reset state of an existing index hit. This can be used to recycle index hits during reads
  void Reset();

  virtual void Print(int depth) const = 0; // debug print

  // Get the minimal delta between the terms in the result
  virtual int MinOffsetDelta() const { return 1; };

  size_t GetMatchedTerms(RSQueryTerm **arr, size_t cap);
  virtual void GetMatchedTerms(RSQueryTerm *arr[], size_t cap, size_t &len) {}

  // Return true if the result is within a slop range.
  // inOrder determines whether the tokens need to be ordered as in the query or not.
  virtual bool IsWithinRange(int maxSlop, bool inOrder) const { return true; }

  virtual bool HasOffsets() const { return false; };

  bool withinRangeInOrder(RSOffsetIterators &iters, uint32_t *positions, int num, int maxSlop);
  bool withinRangeUnordered(RSOffsetIterators &iters, uint32_t *positions, int num, int maxSlop);

  double TFIDFScorer(const ScorerArgs *args, const DocumentMetadata *dmd, double minScore, int normMode) const;
  virtual double TFIDFScorer(const DocumentMetadata *dmd, ScoreExplain *explain) const;

  virtual double BM25Scorer(const ScorerArgs *args, const DocumentMetadata *dmd) const;
  virtual double DisMaxScorer(const ScorerArgs *args) const;

  // Iterate an offset vector
  virtual std::unique_ptr<OffsetIterator> IterateOffsets() const {
    return std::make_unique<OffsetIterator>();
  }
};
#pragma pack()

//---------------------------------------------------------------------------------------------

// RS_SCORE_FILTEROUT is a special value (-inf) that should be returned by scoring functions in
// order to completely filter out results and disregard them in the totals count

#define RS_SCORE_FILTEROUT (-1.0 / 0.0)

//---------------------------------------------------------------------------------------------

// The context given to a scoring function.
// It includes payload set by user or expander, extension private data, and callback functions.

struct ScorerArgs {
  struct Stats {
    Stats(const struct IndexStats &stats);

    size_t numDocs;
    size_t numTerms;
    double avgDocLen;
  };

  ScorerArgs(const IndexSpec &spec, const SimpleBuff &ast_payload, bool bExplain);
  ~ScorerArgs();

  SimpleBuff payload;    // Payload set by the client or by the query expander
  Stats indexStats;      // Index statistics to be used by scoring functions
  ScoreExplain *explain; // Flags controlling scoring function

  ScoreExplain *CreateNewExplainParent();
};

//---------------------------------------------------------------------------------------------

#if 0

// ScoringFunction is a callback type for query custom scoring function modules

typedef double (*ScoringFunction)(const ScorerArgs *args, const IndexResult *res,
                                    const DocumentMetadata *dmd, double minScore);

// The extension registeration context, containing the callbacks avaliable to the extension for
// registering query expanders and scorers

struct RSExtensions {
  virtual int RegisterScoringFunction(const char *alias, ScoringFunction func, RSFreeFunction ff, void *privdata);
  virtual int RegisterQueryExpander(const char *alias, RSQueryTokenExpander exp, RSFreeFunction ff, void *privdata);
};

// An extension initialization function
typedef int (*RSExtensionInitFunc)(RSExtensions *ctx);

#endif // 0

///////////////////////////////////////////////////////////////////////////////////////////////
