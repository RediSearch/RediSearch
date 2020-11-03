
#pragma once

#include "object.h"
#include "util/dllist.h"
#include "stemmer.h"
#include "rmalloc.h"

#include <stdint.h>
#include <stdlib.h>
#include <limits.h>

///////////////////////////////////////////////////////////////////////////////////////////////

typedef uint64_t t_docId;
typedef uint64_t t_offset;
// used to represent the id of a single field.
// to produce a field mask we calculate 2^fieldId
typedef uint16_t t_fieldId;

#define DOCID_MAX UINT64_MAX

#if defined(__x86_64__) && !defined(RS_NO_U128)
// 64 bit architectures use 128 bit field masks and up to 128 fields
typedef __uint128_t t_fieldMask;
#define RS_FIELDMASK_ALL (((__uint128_t)1 << 127) - (__uint128_t)1 + ((__uint128_t)1 << 127))

#else
// 32 bit architectures use 64 bits and 64 fields only
typedef uint64_t t_fieldMask;
#define RS_FIELDMASK_ALL 0xFFFFFFFFFFFFFFFF
#endif

struct RSSortingVector;

#define REDISEARCH_ERR 1
#define REDISEARCH_OK 0

///////////////////////////////////////////////////////////////////////////////////////////////

/* A payload object is set either by a query expander or by the user, and can be used to process
 * scores. For examples, it can be a feature vector that is then compared to a feature vector
 * extracted from each result or document */

struct RSPayload {
  char *data;
  size_t len;
};

//---------------------------------------------------------------------------------------------

// Internally used document flags
enum RSDocumentFlags {
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

/* RSDocumentMetadata describes metadata stored about a document in the index (not the document
 * itself).
 *
 * The key is the actual user defined key of the document, not the incremental id. It is used to
 * convert incremental internal ids to external string keys.
 *
 * Score is the original user score as inserted to the index
 *
 * Flags is not currently used, but should be used in the future to mark documents as deleted, etc.
 */

struct RSDocumentMetadata {
  t_docId id;

  // The actual key of the document, not the internal incremental id
  char *keyPtr;

  // The a-priory document score as given by the user on insertion
  float score;

  // The maximum frequency of any term in the index, used to normalize frequencies
  uint32_t maxFreq : 24;

  // The total weighted number of tokens in the document, weighted by field weights
  uint32_t len : 24;

  // Document flags
  RSDocumentFlags flags : 8;

  // Optional user payload
  RSPayload *payload;

  struct RSSortingVector *sortVector;
  // Offsets of all terms in the document (in bytes). Used by highlighter
  struct RSByteOffsets *byteOffsets;
  DLLIST2_node llnode;
  uint32_t ref_count;
};

//---------------------------------------------------------------------------------------------

// Forward declaration of the opaque query object
struct RSQuery;

// Forward declaration of the opaque query node object
struct RSQueryNode;

// We support up to 30 user given flags for each token, flags 1 and 2 are taken by the engine
typedef uint32_t RSTokenFlags;

// A token in the query.
// The expanders receive query tokens and can expand the query with more query tokens.
struct RSToken {
  // The token string - which may or may not be NULL terminated
  char *str;
  // The token length
  size_t len;

  // Is this token an expansion?
  uint8_t expanded : 1;

  // Extension set token flags - up to 31 bits
  RSTokenFlags flags : 31;
};

//---------------------------------------------------------------------------------------------

struct QueryAST;

// RSQueryExpanderCtx is a context given to query expanders, containing callback methods and useful data

struct RSQueryExpanderCtx {

  // Opaque query object used internally by the engine, and should not be accessed
  struct QueryAST *qast;

  struct RedisSearchCtx *handle;

  // Opaque query node object used internally by the engine, and should not be accessed
  struct RSQueryNode **currentNode;

  // Error object. Can be used to signal an error to the user
  struct QueryError *status;

  // Private data of the extension, set on extension initialization or during expansion.
  // If a Free callback is provided, it will be used automatically to free this data.
  void *privdata;

  // The language of the query. Defaults to "english"
  RSLanguage language;

  /* ExpandToken allows the user to add an expansion of the token in the query, that will be
   * union-merged with the given token in query time. str is the expanded string, len is its
   * length, and flags is a 32 bit flag mask that can be used by the extension to set private
   * information on the token
   */
  void (*ExpandToken)(struct RSQueryExpanderCtx *ctx, const char *str, size_t len,
                      RSTokenFlags flags);

  /* Expand the token with a multi-word phrase, where all terms are intersected. toks is an array
   * with num its len, each member of it is a null terminated string. If replace is set to 1, we
   * replace the original token with the new phrase. If exact is 1 the expanded phrase is an exact
   * match phrase
   */
  void (*ExpandTokenWithPhrase)(struct RSQueryExpanderCtx *ctx, const char **toks, size_t num,
                                RSTokenFlags flags, int replace, int exact);

  // SetPayload allows the query expander to set GLOBAL payload on the query (not unique per token)
  void (*SetPayload)(struct RSQueryExpanderCtx *ctx, RSPayload payload);
};

//---------------------------------------------------------------------------------------------

// The signature for a query expander instance
typedef int (*RSQueryTokenExpander)(RSQueryExpanderCtx *ctx, RSToken *token);
// A free function called after the query expansion phase is over, to release per-query data
typedef void (*RSFreeFunction)(void *);

//---------------------------------------------------------------------------------------------

// A single term being evaluated in query time
struct RSQueryTerm : public Object {
  // The term string, not necessarily NULL terminated, hence the length is given as well
  char *str;
  // The term length
  size_t len;

  // Inverse document frequency of the term in the index.
  // See https://en.wikipedia.org/wiki/Tf%E2%80%93idf
  double idf;

  // Each term in the query gets an incremental id
  int id;
  // Flags given by the engine or by the query expander
  RSTokenFlags flags;

  RSQueryTerm(RSToken *tok, int id);
  ~RSQueryTerm();
};

//---------------------------------------------------------------------------------------------
// Scoring Function API

// RS_OFFSETVECTOR_EOF is returned from an RSOffsetIterator when calling next and reaching the end.
// When calling the iterator you should check for this return value */
#define RS_OFFSETVECTOR_EOF UINT32_MAX

//---------------------------------------------------------------------------------------------

// RSOffsetIterator is an interface for iterating offset vectors of aggregate and token records
struct RSOffsetIterator {
  virtual ~RSOffsetIterator() {}
  virtual uint32_t Next(RSQueryTerm **term) { return RS_OFFSETVECTOR_EOF; }
  virtual void Rewind() {}

  struct Proxy {
    RSOffsetIterator *it;

    Proxy(RSOffsetIterator *it) : it(it) {}
    ~Proxy();

    RSOffsetIterator *operator->() { return it; }
  };
};

//---------------------------------------------------------------------------------------------

// RSOffsetVector represents the encoded offsets of a term in a document.
// You can read the offsets by iterating over it with RSOffsetVector::Iterate.

struct RSOffsetVector {
  RSOffsetVector(char *data = 0, uint32_t len = 0) : data(data), len(len) {}
  
  char *data;
  uint32_t len;

  RSOffsetIterator Iterate(RSQueryTerm *t) const;
};

//---------------------------------------------------------------------------------------------

// RSIndexRecord represents a single record of a document inside a term in the inverted index
struct RSTermRecord {
  // The term that brought up this record
  RSQueryTerm *term;

  // The encoded offsets in which the term appeared in the document
  RSOffsetVector offsets;
};

//---------------------------------------------------------------------------------------------

// A virtual record represents a record that doesn't have a term or an aggregate, like numeric records
struct RSVirtualRecord {
  char dummy;
};

//---------------------------------------------------------------------------------------------

struct RSNumericRecord {
  double value;
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

struct RSAggregateResult {
  // number of child records
  int numChildren;
  // capacity of the records array. Has no use for extensions
  int childrenCap;
  // array of recods
  struct RSIndexResult **children;

  // A map of the aggregate type of the underlying results
  uint32_t typeMask;
};

//---------------------------------------------------------------------------------------------

#pragma pack(16)

struct RSIndexResult : public Object {
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

  union {
    // Aggregate record
    RSAggregateResult agg;
    // Term record
    RSTermRecord term;
    // virtual record with no values
    RSVirtualRecord virt;
    // numeric record with float value
    RSNumericRecord num;
  };

  RSResultType type;

  // we mark copied results so we can treat them a bit differently on deletion, and pool them if we want
  int isCopy;

  // Relative weight for scoring calculations. This is derived from the result's iterator weight
  double weight;

  //-------------------------------------------------------------------------------------------

  RSIndexResult() {}

  // Create deep copy of results that is totall thread safe. Very slow so use with caution.
  RSIndexResult(const RSIndexResult &res);

  RSIndexResult(t_docId docId, double value) : docId(docId), type(RSResultType_Numeric) {
    num.value = value;
  }

  // Reset state of an existing index hit. This can be used to recycle index hits during reads
  void Reset();

  // Debug print a result
  void Print(int depth) const;

  // Free an index result's internal allocations, does not free the result itself
  void Free();

  // Get the minimal delta between the terms in the result
    int MinOffsetDelta() const;

  // Fill an array of max capacity cap with all the matching text terms for the result.
  // The number of matching terms is returned.
  size_t GetMatchedTerms(RSQueryTerm **arr, size_t cap) const;
  void GetMatchedTerms(RSQueryTerm *arr[], size_t cap, size_t &len) const;

  // Return 1 if the the result is within a given slop range, inOrder determines whether the tokens
  // need to be ordered as in the query or not
  int IsWithinRange(int maxSlop, int inOrder) const;

  bool HasOffsets() const;
  bool IsAggregate() const;

  // Iterate an offset vector. The iterator object is allocated on the heap and needs to be freed
  RSOffsetIterator::Proxy IterateOffsets() const;
};

#pragma pack()

//---------------------------------------------------------------------------------------------

// RS_SCORE_FILTEROUT is a special value (-inf) that should be returned by scoring functions in
// order to completely filter out results and disregard them in the totals count
#define RS_SCORE_FILTEROUT (-1.0 / 0.0)

//---------------------------------------------------------------------------------------------

struct RSIndexStats {
  size_t numDocs;
  size_t numTerms;
  double avgDocLen;
};

//---------------------------------------------------------------------------------------------

// The context given to a scoring function. It includes the payload set by the user or expander,
// the private data set by the extensionm and callback functions

struct ScoringFunctionArgs {
  // Private data set by the extension on initialization time, or during scoring
  void *extdata;

  // Payload set by the client or by the query expander
  const void *qdata;
  size_t qdatalen;

  // Index statistics to be used by scoring functions
  RSIndexStats indexStats;

  // Flags controlling scoring function
  void *scrExp;  // scoreflags

  // The GetSlop() callback. Returns the cumulative "slop" or distance between the query terms,
  // that can be used to factor the result score */
  int (*GetSlop)(const RSIndexResult *res);
};

//---------------------------------------------------------------------------------------------

// RSScoringFunction is a callback type for query custom scoring function modules

typedef double (*RSScoringFunction)(const ScoringFunctionArgs *ctx, const RSIndexResult *res,
                                    const RSDocumentMetadata *dmd, double minScore);

// The extension registeration context, containing the callbacks avaliable to the extension for
// registering query expanders and scorers

struct RSExtensionCtx {
  int (*RegisterScoringFunction)(const char *alias, RSScoringFunction func, RSFreeFunction ff,
                                 void *privdata);
  int (*RegisterQueryExpander)(const char *alias, RSQueryTokenExpander exp, RSFreeFunction ff,
                               void *privdata);
};

// An extension initialization function
typedef int (*RSExtensionInitFunc)(RSExtensionCtx *ctx);

///////////////////////////////////////////////////////////////////////////////////////////////
