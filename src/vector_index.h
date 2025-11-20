/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#pragma once
#include "VecSim/vec_sim.h"
#include "iterators/iterator_api.h"
#include "query_node.h"
#include "query_ctx.h"
#include "field_spec.h"

#define VECSIM_TYPE_BFLOAT16 "BFLOAT16"
#define VECSIM_TYPE_FLOAT16 "FLOAT16"
#define VECSIM_TYPE_FLOAT32 "FLOAT32"
#define VECSIM_TYPE_FLOAT64 "FLOAT64"
#define VECSIM_TYPE_UINT8 "UINT8"
#define VECSIM_TYPE_INT8 "INT8"
#define VECSIM_TYPE_INT32 "INT32"
#define VECSIM_TYPE_INT64 "INT64"

#define VECSIM_METRIC_IP "IP"
#define VECSIM_METRIC_L2 "L2"
#define VECSIM_METRIC_COSINE "COSINE"

#define VECSIM_ALGORITHM_BF "FLAT"
#define VECSIM_ALGORITHM_HNSW "HNSW"
#define VECSIM_ALGORITHM_TIERED "TIERED"
#define VECSIM_ALGORITHM_SVS "SVS-VAMANA"

#define VECSIM_INITIAL_CAP "INITIAL_CAP"
#define VECSIM_BLOCKSIZE "BLOCK_SIZE"
#define VECSIM_M "M"
#define VECSIM_EFCONSTRUCTION "EF_CONSTRUCTION"
#define VECSIM_EFRUNTIME "EF_RUNTIME"
#define VECSIM_EPSILON "EPSILON"
#define VECSIM_HYBRID_POLICY "HYBRID_POLICY"
#define VECSIM_BATCH_SIZE "BATCH_SIZE"
#define VECSIM_SHARD_WINDOW_RATIO "SHARD_WINDOW_RATIO"
#define VECSIM_TYPE "TYPE"
#define VECSIM_DIM "DIM"
#define VECSIM_DISTANCE_METRIC "DISTANCE_METRIC"
#define VECSIM_GRAPH_DEGREE "GRAPH_MAX_DEGREE"
#define VECSIM_WINDOW_SIZE "CONSTRUCTION_WINDOW_SIZE"
#define VECSIM_NUM_THREADS "NUM_THREADS"
#define VECSIM_WSSEARCH "SEARCH_WINDOW_SIZE"
#define VECSIM_MAX_CANDIDATE_POOL_SIZE "MAX_CANDIDATE_POOL_SIZE"
#define VECSIM_PRUNE_TO "PRUNE_TO"
#define VECSIM_ALPHA "ALPHA"
#define VECSIM_USE_SEARCH_HISTORY "USE_SEARCH_HISTORY"
#define VECSIM_USE_SEARCH_HISTORY_ON "ON"
#define VECSIM_USE_SEARCH_HISTORY_OFF "OFF"
#define VECSIM_USE_SEARCH_HISTORY_DEFAULT "DEFAULT"
#define VECSIM_COMPRESSION "COMPRESSION"
#define VECSIM_NO_COMPRESSION "NO_COMPRESSION"
#define VECSIM_LVQ_SCALAR "GlobalSQ8"
#define VECSIM_LVQ_4 "LVQ4"
#define VECSIM_LVQ_8 "LVQ8"
#define VECSIM_LVQ_4X4 "LVQ4x4"
#define VECSIM_LVQ_4X8 "LVQ4x8"
#define VECSIM_LEANVEC_4X8 "LeanVec4x8"
#define VECSIM_LEANVEC_8X8 "LeanVec8x8"
#define VECSIM_TRAINING_THRESHOLD "TRAINING_THRESHOLD"
#define VECSIM_REDUCED_DIM "REDUCE"

#define VECSIM_ERR_MANDATORY(status,algorithm,arg) \
  QueryError_SetWithUserDataFmt(status, QUERY_ERROR_CODE_PARSE_ARGS, "Missing mandatory parameter: cannot create", " %s index without specifying %s argument", algorithm, arg)

#define VECSIM_KNN_K_TOO_LARGE_ERR_MSG "KNN K parameter is too large"

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
  double shardWindowRatio;       // shard window ratio for distributed queries

  // Position tracking for K value modification (shard ratio optimization)
  // For literal K (e.g., "KNN 10"): stores position and length of numeric value
  // For parameter K (e.g., "KNN $k"): stores position and length INCLUDING the '$' prefix
  size_t k_token_pos;            // Byte offset where K token starts in original query
  size_t k_token_len;            // Length of K token
} KNNVectorQuery;

typedef struct {
  void *vector;                  // query vector data
  size_t vecLen;                 // vector length
  double radius;                 // the radius to search in
  VecSimQueryReply_Order order;  // specify the result order.
} RangeVectorQuery;

typedef struct VectorQuery {
  const FieldSpec *field;             // the vector field
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

VecSimIndex *openVectorIndex(IndexSpec *spec, RedisModuleString *keyName, bool create_if_index);

QueryIterator *NewVectorIterator(QueryEvalCtx *q, VectorQuery *vq, QueryIterator *child_it);

int VectorQuery_EvalParams(dict *params, QueryNode *node, unsigned int dialectVersion, QueryError *status);
int VectorQuery_ParamResolve(VectorQueryParams params, size_t index, dict *paramsDict, QueryError *status);
void VectorQuery_Free(VectorQuery *vq);
char *VectorQuery_GetDefaultScoreFieldName(const char *fieldName, size_t fieldNameLen);
void VectorQuery_SetDefaultScoreField(VectorQuery *vq, const char *fieldName, size_t fieldNameLen);

VecSimResolveCode VecSim_ResolveQueryParams(VecSimIndex *index, VecSimRawParam *params, size_t params_len,
                                            VecSimQueryParams *qParams, VecsimQueryType queryType, QueryError *status);
size_t VecSimType_sizeof(VecSimType type);
const char *VecSimType_ToString(VecSimType type);
const char *VecSimMetric_ToString(VecSimMetric metric);
const char *VecSimAlgorithm_ToString(VecSimAlgo algo);
const char *VecSimSearchMode_ToString(VecSearchMode vecsimSearchMode);
const char *VecSimSvsCompression_ToString(VecSimSvsQuantBits quantBits);
const char *VecSimSearchHistory_ToString(VecSimOptionMode option);
bool VecSim_IsLeanVecCompressionType(VecSimSvsQuantBits quantBits);
bool isLVQSupported();

VecSimMetric getVecSimMetricFromVectorField(const FieldSpec *vectorField);

void VecSimParams_Cleanup(VecSimParams *params);

void VecSim_RdbSave(RedisModuleIO *rdb, VecSimParams *vecsimParams);
int VecSim_RdbLoad(RedisModuleIO *rdb, VecSimParams *vecsimParams);
int VecSim_RdbLoad_v2(RedisModuleIO *rdb, VecSimParams *vecsimParams); // includes multi flag
int VecSim_RdbLoad_v3(RedisModuleIO *rdb, VecSimParams *vecsimParams, StrongRef spec,
                      const char *field_name); // includes tiered index
int VecSim_RdbLoad_v4(RedisModuleIO *rdb, VecSimParams *vecsimParams, StrongRef spec,
                      const char *field_name); // includes SVS algorithm support

void VecSim_TieredParams_Init(TieredIndexParams *params, StrongRef sp_ref);
void VecSimLogCallback(void *ctx, const char *level, const char *message);

int VecSim_CallTieredIndexesGC(WeakRef spRef);

#ifdef __cplusplus
extern "C" {
#endif

QueryIterator *createMetricIteratorFromVectorQueryResults(VecSimQueryReply *reply,
                                                          bool yields_metric);
#ifdef __cplusplus
}
#endif
