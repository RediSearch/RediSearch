#include "search_ctx.h"
#include "VectorSimilarity/src/vecsim.h"
#include "index_iterator.h"

typedef enum {
  VECTOR_TYPE_FLOAT32 = 0,
  VECTOR_TYPE_DOUBLE64 = 1,
  VECTOR_TYPE_INT32 = 2,
  VECTOR_TYPE_INT64 = 3,
  VECTOR_TYPE_BYTE = 4,
  VECTOR_TYPE_BOOL = 5,
} VectorType;

typedef enum {
  VECTOR_ALG_BF = 0,
  VECTOR_ALG_HNSW = 1,
} VectorAlgorithm;


typedef enum {
  VECTOR_TOPK = 0,
  VECTOR_RANGE = 1,
} VectorQueryType;

typedef enum {
  VECTOR_DIST_L2 = 0,
  VECTOR_DIST_COSINE = 1,
  VECTOR_DIST_IP = 2,
} VectorDistanceMetric;

typedef struct VectorFilter {
  char *property;                 // name of field
  void *vector;                   // vector data
  size_t vecLen;                  // vector length
  VectorQueryType type;           // TOPK or RANGE
  double value;                   // can hold int for TOPK or double for RANGE.

  VecSimQueryResult *results;     // array for K results
  int resultsLen;                 // length of array
} VectorFilter;

/*
typedef struct {
#if 0
  union IndexPtr {
    BFIndex *bf;
    HNSWIndex *nhsw;
  } data;
#endif
  VecSimIndex *data;

  size_t size;
  VectorType type;
  VectorAlgorithm alg;
  VectorDistanceMetric dist;

  size_t numEntries;
  t_docId lastDocId;

} RS_Vector;*/

// This function open or create a new index of type HNSW.

// TODO: remove idxKey from all OpenFooIndex functions
VecSimIndex *OpenVectorIndex(RedisSearchCtx *ctx,
  RedisModuleString *keyName/*, RedisModuleKey **idxKey*/);

IndexIterator *NewVectorIterator(RedisSearchCtx *ctx, VectorFilter *vf);

VectorFilter *NewVectorFilter(const void *vector, size_t len, char *type, double value);
void VectorFilter_Free(VectorFilter *vf);
