/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#pragma once
#include "search_ctx.h"
#include "VecSim/vec_sim.h"
#include "index_iterator.h"
#include "query_node.h"
#include "query_ctx.h"

#define VECSIM_TYPE_FLOAT32 "FLOAT32"
#define VECSIM_TYPE_FLOAT64 "FLOAT64"
#define VECSIM_TYPE_INT32 "INT32"
#define VECSIM_TYPE_INT64 "INT64"

#define VECSIM_METRIC_IP "IP"
#define VECSIM_METRIC_L2 "L2"
#define VECSIM_METRIC_COSINE "COSINE"

#define VECSIM_ALGORITHM_BF "FLAT"
#define VECSIM_ALGORITHM_HNSW "HNSW"
#define VECSIM_ALGORITHM_TIERED "TIERED"

#define VECSIM_INITIAL_CAP "INITIAL_CAP"
#define VECSIM_BLOCKSIZE "BLOCK_SIZE"
#define VECSIM_M "M"
#define VECSIM_EFCONSTRUCTION "EF_CONSTRUCTION"
#define VECSIM_EFRUNTIME "EF_RUNTIME"
#define VECSIM_EPSILON "EPSILON"
#define VECSIM_HYBRID_POLICY "HYBRID_POLICY"
#define VECSIM_BATCH_SIZE "BATCH_SIZE"
#define VECSIM_TYPE "TYPE"
#define VECSIM_DIM "DIM"
#define VECSIM_DISTANCE_METRIC "DISTANCE_METRIC"

#define VECSIM_ERR_MANDATORY(status,algorithm,arg) \
  QERR_MKBADARGS_FMT(status, "Missing mandatory parameter: cannot create %s index without specifying %s argument", algorithm, arg)

typedef enum {
  VECSIM_QT_KNN,
  VECSIM_QT_RANGE
} VectorQueryType;

// This struct holds VecSimRawParam array and bool array.
// the arrays should have the same length, for testing if the param in some index needs to be evaluated.
// `params` params will always hold parameter name and value as allocated string.
// First, the parser creates the param, holds the key-name and value as they appear in the query,
// and marks if the value is the literal value or an attribute name.
// Second, in the parameters evaluation step, if a param was marked as an attribute, we try to resolve it,
// and free its old value and replace it with the actual value if we succeed.
// It is the VecSim library job to resolve this strings-key-value params (array) into a VecSimQueryParams struct.
typedef struct {
  VecSimRawParam *params;
  bool *needResolve;
} VectorQueryParams;

typedef struct {
  void *vector;                  // query vector data
  size_t vecLen;                 // vector length
  size_t k;                      // number of vectors to return
  VecSimQueryReply_Order order;  // specify the result order.
} KNNVectorQuery;

typedef struct {
  void *vector;                  // query vector data
  size_t vecLen;                 // vector length
  double radius;                 // the radius to search in
  VecSimQueryReply_Order order;  // specify the result order.
} RangeVectorQuery;

typedef struct VectorQuery {
  char *property;                     // name of field
  char *scoreField;                   // name of score field
  union {
    KNNVectorQuery knn;
    RangeVectorQuery range;
  };
  VectorQueryType type;               // vector similarity query type
  VectorQueryParams params;           // generic query params array, for the vecsim library to check

  VecSimQueryResult *results;         // array for results
  int resultsLen;                     // length of array
} VectorQuery;

// This enum should match the VecSearchMode enum in VecSim
typedef enum {
  VECSIM_EMPTY_MODE,
  VECSIM_STANDARD_KNN,               // Run k-nn query over the entire vector index.
  VECSIM_HYBRID_ADHOC_BF,            // Measure ad-hoc the distance for every result that passes the filters,
                                     //  and take the top k results.
  VECSIM_HYBRID_BATCHES,             // Get the top vector results in batches upon demand, and keep the results that
                                     //  passes the filters until we reach k results.
  VECSIM_HYBRID_BATCHES_TO_ADHOC_BF, // Start with batches and dynamically switched to ad-hoc BF.
  VECSIM_RANGE_QUERY,                // Run range query, to return all vectors that are within a given range from the
                                     //  query vector.
  VECSIM_LAST_SEARCHMODE,            // Last value of this enum. Can be used to check if a given value resides within
                                     //  this enum values range.

} VecSimSearchMode;

// External log ctx to be sent to the log callback that vecsim is using internally.
// Created upon creating a new vecsim index
typedef struct VecSimLogCtx {
    const char *index_field_name;  // should point to the field_spec name string.
} VecSimLogCtx;

// TODO: remove idxKey from all OpenFooIndex functions
VecSimIndex *OpenVectorIndex(IndexSpec *sp,
  RedisModuleString *keyName/*, RedisModuleKey **idxKey*/);

IndexIterator *NewVectorIterator(QueryEvalCtx *q, VectorQuery *vq, IndexIterator *child_it);

int VectorQuery_EvalParams(dict *params, QueryNode *node, QueryError *status);
int VectorQuery_ParamResolve(VectorQueryParams params, size_t index, dict *paramsDict, QueryError *status);
void VectorQuery_Free(VectorQuery *vq);

VecSimResolveCode VecSim_ResolveQueryParams(VecSimIndex *index, VecSimRawParam *params, size_t params_len,
                                            VecSimQueryParams *qParams, VecsimQueryType queryType, QueryError *status);
size_t VecSimType_sizeof(VecSimType type);
const char *VecSimType_ToString(VecSimType type);
const char *VecSimMetric_ToString(VecSimMetric metric);
const char *VecSimAlgorithm_ToString(VecSimAlgo algo);

void VecSimParams_Cleanup(VecSimParams *params);

void VecSim_RdbSave(RedisModuleIO *rdb, VecSimParams *vecsimParams);
int VecSim_RdbLoad(RedisModuleIO *rdb, VecSimParams *vecsimParams);
int VecSim_RdbLoad_v2(RedisModuleIO *rdb, VecSimParams *vecsimParams); // includes multi flag
int VecSim_RdbLoad_v3(RedisModuleIO *rdb, VecSimParams *vecsimParams, StrongRef spec,
                      const char *field_name); // includes tiered index

void VecSim_TieredParams_Init(TieredIndexParams *params, StrongRef sp_ref);
void VecSimLogCallback(void *ctx, const char *level, const char *message);

VecSimIndex **VecSim_GetAllTieredIndexes(StrongRef spec_ref);
void VecSim_CallTieredIndexesGC(VecSimIndex **tieredIndexes, WeakRef spRef);

#ifdef __cplusplus
extern "C" {
#endif

IndexIterator *createMetricIteratorFromVectorQueryResults(VecSimQueryReply *reply,
                                                          bool yields_metric);
#ifdef __cplusplus
}
#endif
