#ifndef __REDISEARCH_H__
#define __REDISEARCH_H__

#include <stdint.h>
#include <stdlib.h>

typedef struct {
  char *data;
  size_t len;
} RSPayload;

/* DocumentMetadata describes metadata stored about a document in the index (not the document
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
  uint32_t : 24 maxFreq;

  /* The total number of tokens in the document */
  uint32_t : 24 len;

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

/* Scoring Function API */
struct RSOfffsetVector;
typedef RSOffsetIterator;

struct RSOfffsetVector RSOffsetVector_Iterate(struct RSOfffsetVector *v);
uint32_t RSOffsetIterator_Next(struct RSOfffsetVector *vi);

typedef struct {
  char *str;
  size_t len;
  double idf;
  RSTokenFlags flags;
} RSQueryTerm;

typedef u_char RSRecordFlags;

typedef struct {
  t_docId docId;
  RSRecordFlags *term;
  uint32_t freq;
  u_char flags;
  RSOffsetVector offsets;
} RSIndexRecord;

typedef struct {
  t_docId docId;
  double finalScore;
  uint32_t totalTF;
  u_char flags;
  int numRecords;
  int recordsCap;
  RSIndexRecord *records;
} RSIndexResult;

typedef struct {
  void *privdata;
  void *payload;
  int (*GetSlop)(IndexResult *res);
} RSScoringFunctionCtx;

/* RSScoringFunction is a callback type for query custom scoring function modules */
typedef double (*RSScoringFunction)(RSScoringFunctionCtx *ctx, IndexResult *res,
                                    DocumentMetadata *dmd);

typedef struct rsExtensionCtx {
  int (*RegisterScoringFunction)(struct rsExtensionCtx *ctx, const char *alias,
                                 RSScoringFunction func);
  int (*RegisterQueryExpander)(struct rsExtensionCtx *ctx, const char *alias,
                               RSQueryTokenExpander exp);
} RSExtensionCtx;
#endif