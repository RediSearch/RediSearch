#ifndef __REDISEARCH_H__
#define __REDISEARCH_H__

#include <stdint.h>
#include <stdlib.h>

typedef struct {
  char *data;
  size_t len;
} RSPayload;

/* Document flags. The only supported flag currently is deleted, but more might come later one */
typedef enum {
  Document_DefaultFlags = 0x00,
  Document_Deleted = 0x01,
  Document_HasPayload = 0x02
} RSDocumentFlags;

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
typedef struct {
  /* The actual key of the document, not the internal incremental id */
  char *key;

  /* The a-priory document score as given by the user on insertion */
  float score;

  /* The maximum frequency of any term in the index, used to normalize frequencies */
  uint32_t maxFreq : 24;

  /* The total number of tokens in the document */
  uint32_t len : 24;

  /* Document flags - currently only Deleted is used */
  RSDocumentFlags flags;

  /* Optional user payload */
  RSPayload *payload;
} RSDocumentMetadata;

/* Forward declaration of the opaque query object */
struct RSQuery;

/* Forward declaration of the opaque query node object */
struct RSQueryNode;

/* We support up to 30 user given flags for each token, flags 1 and 2 are taken by the engine */
typedef uint32_t RSTokenFlags;

/* A token in the query. The expanders receive query tokens and can expand the query with more query
 * tokens */
typedef struct {
  char *str;
  size_t len;
  RSTokenFlags flags;
} RSToken;

typedef struct RSQueryExpanderCtx {
  struct RSQuery *query;
  struct RSQueryNode *currentNode;
  void (*ExpandToken)(struct RSQueryExpanderCtx *ctx, RSToken *token);
  void (*SetPayload)(struct RSQueryExpanderCtx *ctx, RSPayload payload);
} RSQueryExpanderCtx;

typedef int (*RSQueryTokenExpander)(RSQueryExpanderCtx *ctx, RSToken *token);

/**************************************
 * Scoring Function API
 **************************************/

#define RS_OFFSETVECTOR_EOF (uint32_t) - 1;

#ifndef __RS_OFFSET_VECTOR_H__
typedef struct RSOffsetVector RSOffsetVector;

typedef struct RSOffsetIterator RSOffsetIterator;
#endif

struct RSOffsetIterator RSOffsetVector_Iterate(RSOffsetVector *v);
uint32_t RSOffsetIterator_Next(RSOffsetVector *vi);

/* A single term being evaluated in query time */
typedef struct {
  char *str;
  size_t len;
  double idf;
  RSTokenFlags flags;
} RSQueryTerm;

typedef struct {
  t_docId docId;
  RSQueryTerm *term;
  uint32_t freq;
  uint32_t fieldMask;
  RSOffsetVector *offsets;
} RSIndexRecord;

typedef struct {
  t_docId docId;
  double finalScore;
  uint32_t totalTF;
  uint32_t fieldMask;
  int numRecords;
  int recordsCap;
  RSIndexRecord *records;
} RSIndexResult;

typedef struct {
  void *privdata;
  void *payload;
  int (*GetSlop)(RSIndexResult *res);
} RSScoringFunctionCtx;

/* RSScoringFunction is a callback type for query custom scoring function modules */
typedef double (*RSScoringFunction)(RSScoringFunctionCtx *ctx, RSIndexResult *res,
                                    RSDocumentMetadata *dmd, double minScore);

typedef struct RSExtensionCtx {
  int (*RegisterScoringFunction)(const char *alias, RSScoringFunction func, void *privdata);
  int (*RegisterQueryExpander)(const char *alias, RSQueryTokenExpander exp, void *privdata);
} RSExtensionCtx;
#endif