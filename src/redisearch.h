/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#ifndef REDISEARCH_H__
#define REDISEARCH_H__

#include <stdint.h>
#include <stdlib.h>
#include <limits.h>
#include <stdbool.h>
#include <time.h>
#include "util/dllist.h"
#include "stemmer.h"
#include "types_rs.h"

typedef uint64_t t_docId;
typedef uint64_t t_offset;
// used to represent the id of a single field.
// to produce a field mask we calculate 2^fieldId
typedef uint16_t t_fieldId;
#define RS_INVALID_FIELD_ID (t_fieldId)-1
// Used to identify any field index within the spec, not just textual fields
typedef uint16_t t_fieldIndex;
#define RS_INVALID_FIELD_INDEX (t_fieldIndex)0xFFFF
struct timespec;
typedef struct timespec t_expirationTimePoint;

typedef uint64_t t_uniqueId;
#define SIGN_CHAR_LENGTH 0 // t_uniqueId is unsigned
#define LOG_10_ON_256_UPPER_BOUND 3 // 2^8 = 10 ^ y, 2^16 = 2^8 * 2^8 = 10^y * 10^y = 10^2y -> y == 2.40824 -> upper bound for y is 3
#define MAX_UNIQUE_ID_TEXT_LENGTH_UPPER_BOUND ((sizeof(t_uniqueId) * LOG_10_ON_256_UPPER_BOUND) + SIGN_CHAR_LENGTH)

#define DOCID_MAX UINT64_MAX

#if (defined(__x86_64__) || defined(__aarch64__) || defined(__arm64__)) && !defined(RS_NO_U128)
/* 64 bit architectures use 128 bit field masks and up to 128 fields */
typedef __uint128_t t_fieldMask;
#define RS_FIELDMASK_ALL (((__uint128_t)1 << 127) - (__uint128_t)1 + ((__uint128_t)1 << 127))
#else
/* 32 bit architectures use 64 bits and 64 fields only */
typedef uint64_t t_fieldMask;
#define RS_FIELDMASK_ALL 0xFFFFFFFFFFFFFFFF
#endif

struct RSSortingVector;

#define REDISEARCH_ERR 1
#define REDISEARCH_OK 0
#define REDISEARCH_UNINITIALIZED -1
#define BAD_POINTER ((void *)0xBAAAAAAD)

#define RedisModule_ReplyWithPrintf(ctx, fmt, ...)                                      \
do {                                                                                    \
  RedisModuleString *str = RedisModule_CreateStringPrintf(ctx, fmt, __VA_ARGS__);       \
  RedisModule_ReplyWithString(ctx, str);                                                \
  RedisModule_FreeString(ctx, str);                                                     \
} while (0)

#ifdef __cplusplus
extern "C" {
#endif

typedef enum {
  DocumentType_Hash,
  DocumentType_Json,
  DocumentType_Unsupported,
} DocumentType;

#define isSpecHash(spec) ((spec)->rule && (spec)->rule->type == DocumentType_Hash)
#define isSpecJson(spec) ((spec)->rule && (spec)->rule->type == DocumentType_Json)
#define SpecRuleTypeName(spec) ((spec)->rule ? DocumentType_ToString((spec)->rule->type) : "Unknown")

#define RS_IsMock (!RedisModule_CreateTimer)

/* A payload object is set either by a query expander or by the user, and can be used to process
 * scores. For examples, it can be a feature vector that is then compared to a feature vector
 * extracted from each result or document */
typedef struct {
  char *data;
  size_t len;
} RSPayload;

/* Internally used document flags */
typedef enum {
  Document_DefaultFlags = 0x00,
  Document_Deleted = 0x01,
  Document_HasPayload = 0x02,
  Document_HasSortVector = 0x04,
  Document_HasOffsetVector = 0x08,
  Document_HasExpiration = 0x10, // Document and/or at least one of its fields has an expiration time
  Document_FailedToOpen = 0x20, // Document was failed to opened by a loader (might expired) but not yet marked as deleted.
                                // This is an optimization to avoid attempting opening the document for loading. May be used UN-ATOMICALLY
} RSDocumentFlags;

enum FieldExpirationPredicate {
  FIELD_EXPIRATION_DEFAULT, // one of the fields need to be valid
  FIELD_EXPIRATION_MISSING // one of the fields need to be expired for the entry to be considered missing
};

#define hasPayload(x) (x & Document_HasPayload)
#define hasExpirationTimeInformation(x) (x & Document_HasExpiration)

/* RSDocumentMetadata describes metadata stored about a document in the index (not the document
 * itself).
 *
 * The key is the actual user defined key of the document, not the incremental id. It is used to
 * convert incremental internal ids to external string keys.
 *
 * Score is the original user score as inserted to the index
 */
typedef struct RSDocumentMetadata_s {
  t_docId id;

  /* The actual key of the document, not the internal incremental id */
  char *keyPtr;

  /* The a-priory document score as given by the user on insertion */
  float score;

  /* The maximum frequency of any term in the index, used to normalize frequencies */
  uint32_t maxFreq : 24;

  /* Document flags  */
  RSDocumentFlags flags : 8;

  /* The total weighted number of tokens in the document, weighted by field weights */
  uint32_t len : 24;

  // Type of source document. Hash or JSON.
  DocumentType type : 8;

  uint16_t ref_count;

  struct RSSortingVector *sortVector;
  /* Offsets of all terms in the document (in bytes). Used by highlighter */
  struct RSByteOffsets *byteOffsets;
  DLLIST2_node llnode;

  /* Optional user payload */
  RSPayload *payload;

} RSDocumentMetadata;

/* Forward declaration of the opaque query object */
struct QueryParseCtx;

/* Forward declaration of the opaque query node object */
struct RSQueryNode;

/* We support up to 30 user given flags for each token, flags 1 and 2 are taken by the engine */
typedef uint32_t RSTokenFlags;

/* A token in the query. The expanders receive query tokens and can expand the query with more query
 * tokens */
typedef struct {
  /* The token string - which may or may not be NULL terminated */
  char *str;
  /* The token length */
  size_t len;

  /* Is this token an expansion? */
  uint8_t expanded : 1;

  /* Extension set token flags - up to 31 bits */
  RSTokenFlags flags : 31;
} RSToken;

struct QueryAST;

/* RSQueryExpanderCtx is a context given to query expanders, containing callback methods and useful
 * data */
typedef struct RSQueryExpanderCtx {

  /* Opaque query object used internally by the engine, and should not be accessed */
  struct QueryAST *qast;

  struct RedisSearchCtx *handle;

  /* Opaque query node object used internally by the engine, and should not be accessed */
  struct RSQueryNode **currentNode;

  /* Error object. Can be used to signal an error to the user */
  struct QueryError *status;

  /* Private data of the extension, set on extension initialization or during expansion. If a Free
   * callback is provided, it will be used automatically to free this data */
  void *privdata;

  /* The language of the query. Defaults to "english" */
  RSLanguage language;

  /* ExpandToken allows the user to add an expansion of the token in the query, that will be
   * union-merged with the given token in query time. str is the expanded string, len is its
   * length, and flags is a 32 bit flag mask that can be used by the extension to set private
   * information on the token
   * */
  void (*ExpandToken)(struct RSQueryExpanderCtx *ctx, const char *str, size_t len,
                      RSTokenFlags flags);

  /* Expand the token with a multi-word phrase, where all terms are intersected. toks is an array
   * with num its len, each member of it is a null terminated string. If replace is set to 1, we
   * replace the original token with the new phrase. If exact is 1 the expanded phrase is an exact
   * match phrase
   */
  void (*ExpandTokenWithPhrase)(struct RSQueryExpanderCtx *ctx, const char **toks, size_t num,
                                RSTokenFlags flags, int replace, int exact);

  /* SetPayload allows the query expander to set GLOBAL payload on the query (not unique per token)
   */
  void (*SetPayload)(struct RSQueryExpanderCtx *ctx, RSPayload payload);

} RSQueryExpanderCtx;

/* The signature for a query expander instance */
typedef int (*RSQueryTokenExpander)(RSQueryExpanderCtx *ctx, RSToken *token);
/* A free function called after the query expansion phase is over, to release per-query data */
typedef void (*RSFreeFunction)(void *);

/* A single term being evaluated in query time */
typedef struct RSQueryTerm {
  /* The term string, not necessarily NULL terminated, hence the length is given as well */
  char *str;
  /* The term length */
  size_t len;
  /* Inverse document frequency of the term in the index. See
   * https://en.wikipedia.org/wiki/Tf%E2%80%93idf */
  double idf;

  /* Each term in the query gets an incremental id */
  int id;
  /* Flags given by the engine or by the query expander */
  RSTokenFlags flags;

  /* Inverse document frequency of the term in the index for computing BM25 */
  double bm25_idf;
} RSQueryTerm;

/**************************************
 * Scoring Function API
 **************************************/

/* RS_OFFSETVECTOR_EOF is returned from an RSOffsetIterator when calling next and reaching the end.
 * When calling the iterator you should check for this return value */
#define RS_OFFSETVECTOR_EOF UINT32_MAX

/* RSOffsetIterator is an interface for iterating offset vectors of aggregate and token records */
typedef struct RSOffsetIterator {
  void *ctx;
  uint32_t (*Next)(void *ctx, RSQueryTerm **term);
  void (*Rewind)(void *ctx);
  void (*Free)(void *ctx);
} RSOffsetIterator;


/* A virtual record represents a record that doesn't have a term or an aggregate, like numeric
 * records */
typedef struct {
  char dummy;
} RSVirtualRecord;


#define RS_RESULT_NUMERIC (RSResultData_Numeric | RSResultData_Metric)

// Forward declaration of needed structs
struct RLookupKey;
struct RSValue;

// Holds a key-value pair of an `RSValue` and the `RLookupKey` to add it into.
// A result processor will write the value into the key if the result passed the AST.
typedef struct RSYieldableMetric{
  struct RLookupKey *key;
  struct RSValue *value;
} RSYieldableMetric;

#pragma pack()

RSOffsetIterator RSOffsetVector_Iterate(const RSOffsetVector *v, RSQueryTerm *t);

/* Iterate an offset vector. The iterator object is allocated on the heap and needs to be freed */
RSOffsetIterator RSIndexResult_IterateOffsets(const RSIndexResult *res);

int RSIndexResult_HasOffsets(const RSIndexResult *res);

/* RS_SCORE_FILTEROUT is a special value (-inf) that should be returned by scoring functions in
 * order to completely filter out results and disregard them in the totals count */
#define RS_SCORE_FILTEROUT (-1.0 / 0.0)

typedef struct {
  size_t numDocs;
  size_t numTerms;
  double avgDocLen;
} RSIndexStats;

/* The context given to a scoring function. It includes the payload set by the user or expander,
 * the
 * private data set by the extensionm and callback functions */
typedef struct {
  /* Private data set by the extension on initialization time, or during scoring */
  void *extdata;

  /* Payload set by the client or by the query expander */
  const void *qdata;
  size_t qdatalen;

  /* Index statistics to be used by scoring functions */
  RSIndexStats indexStats;

  /** Flags controlling scoring function */
  void *scrExp;  // scoreflags

  /* The GetSlop() callback. Returns the cumulative "slop" or distance between the query terms,
   * that can be used to factor the result score */
  int (*GetSlop)(const RSIndexResult *res);

  /* Tanh factor (used only in the `BM25STD.TANH` scorer)*/
  uint64_t tanhFactor;
} ScoringFunctionArgs;

/* RSScoringFunction is a callback type for query custom scoring function modules */
typedef double (*RSScoringFunction)(const ScoringFunctionArgs *ctx, const RSIndexResult *res,
                                    const RSDocumentMetadata *dmd, double minScore);

/* The extension registration context, containing the callbacks available to the extension for
 * registering query expanders and scorers. */
typedef struct RSExtensionCtx {
  int (*RegisterScoringFunction)(const char *alias, RSScoringFunction func, RSFreeFunction ff,
                                 void *privdata);
  int (*RegisterQueryExpander)(const char *alias, RSQueryTokenExpander exp, RSFreeFunction ff,
                               void *privdata);
} RSExtensionCtx;

/* An extension initialization function  */
typedef int (*RSExtensionInitFunc)(RSExtensionCtx *ctx);
#ifdef __cplusplus
}
#endif
#endif
