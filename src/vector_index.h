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
  VECSIM_QT_TOPK,
} VectorQueryType;

typedef enum {
  VECSIM_RUN_KNN = 0,
  VECSIM_RUN_BATCH = 1,
} VectorQueryRunType;

typedef struct {
  const char *name;
  size_t namelen;
  const char *value;
  size_t vallen;
  bool isParam;
} VectorQueryParam;

typedef struct VectorQuery {
  char *property;                     // name of field
  char *scoreField;                   // name of score field
  union {
    struct {
      void *vector;                   // query vector data
      size_t vecLen;                  // vector length
      size_t k;                       // number of vectors to return
      VectorQueryRunType runType;     // specify how to run the query
      VecSimQueryResult_Order order;  // specify the result order.
    } topk;
  };
  VectorQueryType type;               // vector similarity query type
  VectorQueryParam *params;           // generic query params array, for the vecsim library to check

  VecSimQueryResult *results;         // array for results
  int resultsLen;                     // length of array
} VectorQuery;

// TODO: remove idxKey from all OpenFooIndex functions
VecSimIndex *OpenVectorIndex(RedisSearchCtx *ctx,
  RedisModuleString *keyName/*, RedisModuleKey **idxKey*/);

IndexIterator *NewVectorIterator(RedisSearchCtx *ctx, VectorQuery *vq);

int VectorQuery_EvalParams(dict *params, QueryNode *node, QueryError *status);
int VectorQuery_Resolve(VectorQueryParam *param, dict *params, QueryError *status);
void VectorQuery_Free(VectorQuery *vq);

const char *VecSimType_ToString(VecSimType type);
const char *VecSimMetric_ToString(VecSimMetric metric);
const char *VecSimAlgorithm_ToString(VecSimAlgo algo);
