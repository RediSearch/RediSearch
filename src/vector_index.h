#include "search_ctx.h"
#include "VecSim/vecsim.h"
#include "index_iterator.h"

#define VECSIM_TYPE_FLOAT32 "FLOAT32"
#define VECSIM_TYPE_FLOAT64 "FLOAT64"
#define VECSIM_TYPE_INT32 "INT32"
#define VECSIM_TYPE_INT64 "INT64"

#define VECSIM_METRIC_IP "IP"
#define VECSIM_METRIC_L2 "L2"

#define VECSIM_ALGORITHM_BF "BF"
#define VECSIM_ALGORITHM_HNSW "HNSW"

#define VECSIM_INITIAL_CAP "INITIAL_CAP"
#define VECSIM_M "M"
#define VECSIM_EF "EF"

typedef enum {
  VECTOR_TOPK = 0,
  VECTOR_RANGE = 1,
} VectorQueryType;

typedef struct VectorFilter {
  char *property;                 // name of field
  void *vector;                   // vector data
  size_t vecLen;                  // vector length
  VectorQueryType type;           // TOPK or RANGE
  bool isBase64;                  // uses base64 strings
  long long efRuntime;            // efRuntime
  double value;                   // can hold int for TOPK or double for RANGE.

  VecSimQueryResult *results;     // array for K results
  int resultsLen;                 // length of array
} VectorFilter;

// TODO: remove idxKey from all OpenFooIndex functions
VecSimIndex *OpenVectorIndex(RedisSearchCtx *ctx,
  RedisModuleString *keyName/*, RedisModuleKey **idxKey*/);

IndexIterator *NewVectorIterator(RedisSearchCtx *ctx, VectorFilter *vf);

VectorFilter *NewVectorFilter(const void *vector, size_t len, char *type, double value);
void VectorFilter_Free(VectorFilter *vf);
