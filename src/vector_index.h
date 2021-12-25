#pragma once
#include "search_ctx.h"
#include "VecSim/vec_sim.h"
#include "index_iterator.h"
#include "query_node.h"

#define VECSIM_TYPE_FLOAT32 "FLOAT32"
#define VECSIM_TYPE_FLOAT64 "FLOAT64"
#define VECSIM_TYPE_INT32 "INT32"
#define VECSIM_TYPE_INT64 "INT64"

#define VECSIM_METRIC_IP "IP"
#define VECSIM_METRIC_L2 "L2"
#define VECSIM_METRIC_COSINE "COSINE"

#define VECSIM_ALGORITHM_BF "FLAT"
#define VECSIM_ALGORITHM_HNSW "HNSW"

#define VECSIM_INITIAL_CAP "INITIAL_CAP"
#define VECSIM_BLOCKSIZE "BLOCK_SIZE"
#define VECSIM_M "M"
#define VECSIM_EFCONSTRUCTION "EF_CONSTRUCTION"
#define VECSIM_EFRUNTIME "EF_RUNTIME"
#define VECSIM_TYPE "TYPE"
#define VECSIM_DIM "DIM"
#define VECSIM_DISTANCE_METRIC "DISTANCE_METRIC"

#define VECSIM_ERR_MANDATORY(status,algorithm,arg) \
  QERR_MKBADARGS_FMT(status, "Missing mandatory parameter: cannot create %s index without specifying %s argument", algorithm, arg)

typedef enum {
  VECTOR_SIM_INVALID = 0,
  VECTOR_SIM_TOPK = 1,
} VectorQueryType;

typedef struct VectorFilter {
  char *property;                 // name of field
  void *vector;                   // vector data
  size_t vecLen;                  // vector length
  VectorQueryType type;           // TOPK
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

void VectorFilter_InitValues(VectorFilter *vf);
VectorQueryType VectorFilter_ParseType(const char *s, size_t len);
int VectorFilter_Validate(const VectorFilter *vf, QueryError *status);
int VectorFilter_EvalParams(dict *params, QueryNode *node, QueryError *status);
void VectorFilter_Free(VectorFilter *vf);

const char *VecSimType_ToString(VecSimType type);
const char *VecSimMetric_ToString(VecSimMetric metric);
const char *VecSimAlgorithm_ToString(VecSimAlgo algo);
