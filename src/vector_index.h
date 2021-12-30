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

// This struct always hold parameter name and value as allocated string.
// First, the parser creates the param, and hold the key-name and value as they appear in the query,
// and marks if the value is the literal value or an attribute name.
// Second, in the parameters evaluation step, if a param was marked as an attribute, we try to resolve it,
// and free its old value and replace it with the actual value if we succeed.
// It is the VecSim library job to resolve this strings-key-value params (array) into a VecSimQueryParams struct.
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
int VectorQuery_ParamResolve(VectorQueryParam *param, dict *params, QueryError *status);
void VectorQuery_Free(VectorQuery *vq);

const char *VecSimType_ToString(VecSimType type);
const char *VecSimMetric_ToString(VecSimMetric metric);
const char *VecSimAlgorithm_ToString(VecSimAlgo algo);

void VecSim_RdbSave(RedisModuleIO *rdb, VecSimParams *vecsimParams);
int VecSim_RdbLoad(RedisModuleIO *rdb, VecSimParams *vecsimParams);
