/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#include "spec_field_parse.h"
#include "spec.h"
#include "config.h"
#include "rmalloc.h"
#include "suffix.h"
#include "json.h"
#include "module.h"
#include "vector_index.h"
#include "spec_cache.h"
#include "spec_scanner.h"
#include "search_disk.h"
#include "search_disk_utils.h"
#include "info/global_stats.h"
#include "info/index_error.h"
#include "util/workers.h"
#include "rmutil/rm_assert.h"

// Disk validation helper (mirrors the one in spec.c)
inline static bool isSpecOnDiskForValidation(const IndexSpec *sp) {
  return SearchDisk_IsEnabledForValidation();
}

// Externs from spec.c
extern size_t memoryLimit;

//---------------------------------------------------------------------------------------------

void initializeFieldSpec(FieldSpec *fs, t_fieldIndex index) {
  fs->index = index;
  fs->indexError = IndexError_Init();
}

//---------------------------------------------------------------------------------------------

static bool checkPhoneticAlgorithmAndLang(const char *matcher) {
  if (strlen(matcher) != 5) {
    return false;
  }
  if (matcher[0] != 'd' || matcher[1] != 'm' || matcher[2] != ':') {
    return false;
  }

#define LANGUAGES_SIZE 4
  char *languages[] = {"en", "pt", "fr", "es"};

  bool langauge_found = false;
  for (int i = 0; i < LANGUAGES_SIZE; ++i) {
    if (matcher[3] == languages[i][0] && matcher[4] == languages[i][1]) {
      langauge_found = true;
    }
  }

  return langauge_found;
}

// Tries to get vector data type from ac. This function need to stay updated with
// the supported vector data types list of VecSim.
static int parseVectorField_GetType(ArgsCursor *ac, VecSimType *type) {
  const char *typeStr;
  size_t len;
  int rc;
  if ((rc = AC_GetString(ac, &typeStr, &len, 0)) != AC_OK) {
    return rc;
  }
  // Uncomment these when support for other type is added.
  if (STR_EQCASE(typeStr, len, VECSIM_TYPE_FLOAT32))
    *type = VecSimType_FLOAT32;
  else if (STR_EQCASE(typeStr, len, VECSIM_TYPE_FLOAT64))
    *type = VecSimType_FLOAT64;
  else if (STR_EQCASE(typeStr, len, VECSIM_TYPE_FLOAT16))
    *type = VecSimType_FLOAT16;
  else if (STR_EQCASE(typeStr, len, VECSIM_TYPE_BFLOAT16))
    *type = VecSimType_BFLOAT16;
  else if (STR_EQCASE(typeStr, len, VECSIM_TYPE_UINT8))
    *type = VecSimType_UINT8;
  else if (STR_EQCASE(typeStr, len, VECSIM_TYPE_INT8))
    *type = VecSimType_INT8;
  // else if (STR_EQCASE(typeStr, len, VECSIM_TYPE_INT32))
  //   *type = VecSimType_INT32;
  // else if (STR_EQCASE(typeStr, len, VECSIM_TYPE_INT64))
  //   *type = VecSimType_INT64;
  else
    return AC_ERR_ENOENT;
  return AC_OK;
}

// Tries to get distance metric from ac. This function need to stay updated with
// the supported distance metric functions list of VecSim.
static int parseVectorField_GetMetric(ArgsCursor *ac, VecSimMetric *metric) {
  const char *metricStr;
  int rc;
  if ((rc = AC_GetString(ac, &metricStr, NULL, 0)) != AC_OK) {
    return rc;
  }
  if (!strcasecmp(VECSIM_METRIC_IP, metricStr))
    *metric = VecSimMetric_IP;
  else if (!strcasecmp(VECSIM_METRIC_L2, metricStr))
    *metric = VecSimMetric_L2;
  else if (!strcasecmp(VECSIM_METRIC_COSINE, metricStr))
    *metric = VecSimMetric_Cosine;
  else
    return AC_ERR_ENOENT;
  return AC_OK;
}

// Parsing for Quantization parameter in SVS algorithm
static int parseVectorField_GetQuantBits(ArgsCursor *ac, VecSimSvsQuantBits *quantBits) {
  const char *quantBitsStr;
  size_t len;
  int rc;
  if ((rc = AC_GetString(ac, &quantBitsStr, &len, 0)) != AC_OK) {
    return rc;
  }
  if (STR_EQCASE(quantBitsStr, len, VECSIM_LVQ_8))
    *quantBits = VecSimSvsQuant_8;
  else if (STR_EQCASE(quantBitsStr, len, VECSIM_LVQ_4))
    *quantBits = VecSimSvsQuant_4;
  else if (STR_EQCASE(quantBitsStr, len, VECSIM_LVQ_4X4))
    *quantBits = VecSimSvsQuant_4x4;
  else if (STR_EQCASE(quantBitsStr, len, VECSIM_LVQ_4X8))
    *quantBits = VecSimSvsQuant_4x8;
  else if (STR_EQCASE(quantBitsStr, len, VECSIM_LEANVEC_4X8))
    *quantBits = VecSimSvsQuant_4x8_LeanVec;
  else if (STR_EQCASE(quantBitsStr, len, VECSIM_LEANVEC_8X8))
    *quantBits = VecSimSvsQuant_8x8_LeanVec;
  else
    return AC_ERR_ENOENT;
  return AC_OK;
}

// memoryLimit / 10 - default is 10% of global memory limit
#define ACTUAL_MEMORY_LIMIT ((memoryLimit == 0) ? SIZE_MAX : memoryLimit)
#define BLOCK_MEMORY_LIMIT ((RSGlobalConfig.vssMaxResize) ? RSGlobalConfig.vssMaxResize : ACTUAL_MEMORY_LIMIT / 10)

static int parseVectorField_validate_hnsw(VecSimParams *params, QueryError *status) {
  // BLOCK_SIZE is deprecated and not respected when set by user as of INDEX_VECSIM_SVS_VAMANA_VERSION.
  size_t elementSize = VecSimIndex_EstimateElementSize(params);
  // Calculating max block size (in # of vectors), according to memory limits
  size_t maxBlockSize = BLOCK_MEMORY_LIMIT / elementSize;
  params->algoParams.hnswParams.blockSize = MIN(DEFAULT_BLOCK_SIZE, maxBlockSize);
  if (params->algoParams.hnswParams.blockSize == 0) {
    QueryError_SetWithUserDataFmt(status, QUERY_ERROR_CODE_LIMIT, "Vector index element size",
      " %zu exceeded maximum size allowed by server limit which is %zu", elementSize, maxBlockSize);
    return 0;
  }
  size_t index_size_estimation = VecSimIndex_EstimateInitialSize(params);
  index_size_estimation += elementSize * params->algoParams.hnswParams.blockSize;

  RedisModule_Log(RSDummyContext, REDISMODULE_LOGLEVEL_NOTICE,
    "Creating vector index of type HNSW. Required memory for a block of %zu vectors: %zuB",
    params->algoParams.hnswParams.blockSize,  index_size_estimation);
  return 1;
}

static int parseVectorField_validate_flat(VecSimParams *params, QueryError *status) {
  // BLOCK_SIZE is deprecated and not respected when set by user as of INDEX_VECSIM_SVS_VAMANA_VERSION.
  size_t elementSize = VecSimIndex_EstimateElementSize(params);
  // Calculating max block size (in # of vectors), according to memory limits
  size_t maxBlockSize = BLOCK_MEMORY_LIMIT / elementSize;
  params->algoParams.bfParams.blockSize = MIN(DEFAULT_BLOCK_SIZE, maxBlockSize);
  if (params->algoParams.bfParams.blockSize == 0) {
    QueryError_SetWithUserDataFmt(status, QUERY_ERROR_CODE_LIMIT, "Vector index element size",
      " %zu exceeded maximum size allowed by server limit which is %zu", elementSize, maxBlockSize);
    return 0;
  }
  // Calculating index size estimation, after first vector block was allocated.
  size_t index_size_estimation = VecSimIndex_EstimateInitialSize(params);
  index_size_estimation += elementSize * params->algoParams.bfParams.blockSize;

  RedisModule_Log(RSDummyContext, REDISMODULE_LOGLEVEL_NOTICE,
    "Creating vector index of type FLAT. Required memory for a block of %zu vectors: %zuB",
    params->algoParams.bfParams.blockSize, index_size_estimation);
  return 1;
}

static int parseVectorField_validate_svs(VecSimParams *params, QueryError *status) {
  size_t elementSize = VecSimIndex_EstimateElementSize(params);
  // Calculating max block size (in # of vectors), according to memory limits
  size_t maxBlockSize = BLOCK_MEMORY_LIMIT / elementSize;
  // Block size should be min(maxBlockSize, DEFAULT_BLOCK_SIZE)
  params->algoParams.svsParams.blockSize = MIN(DEFAULT_BLOCK_SIZE, maxBlockSize);

  // Calculating index size estimation, after first vector block was allocated.
  size_t index_size_estimation = VecSimIndex_EstimateInitialSize(params);
  index_size_estimation += elementSize * params->algoParams.svsParams.blockSize;
  if (params->algoParams.svsParams.blockSize == 0) {
    QueryError_SetWithUserDataFmt(status, QUERY_ERROR_CODE_LIMIT, "Vector index element size",
      " %zu exceeded maximum size allowed by server limit which is %zu", elementSize, maxBlockSize);
    return 0;
  }
  RedisModule_Log(RSDummyContext, REDISMODULE_LOGLEVEL_NOTICE,
    "Creating vector index of type SVS-VAMANA. Required memory for a block of %zu vectors: %zuB",
    params->algoParams.svsParams.blockSize,  index_size_estimation);
  return 1;
}

int VecSimIndex_validate_params(RedisModuleCtx *ctx, VecSimParams *params, QueryError *status) {
  setMemoryInfo(ctx);
  bool valid = false;
  if (VecSimAlgo_HNSWLIB == params->algo) {
    valid = parseVectorField_validate_hnsw(params, status);
  } else if (VecSimAlgo_BF == params->algo) {
    valid = parseVectorField_validate_flat(params, status);
  } else if (VecSimAlgo_SVS == params->algo) {
    valid = parseVectorField_validate_svs(params, status);
  } else if (VecSimAlgo_TIERED == params->algo) {
    return VecSimIndex_validate_params(ctx, params->algoParams.tieredParams.primaryIndexParams, status);
  }
  return valid ? REDISMODULE_OK : REDISMODULE_ERR;
}

#define VECSIM_ALGO_PARAM_MSG(algo, param) "vector similarity " algo " index `" param "`"

static int parseVectorField_hnsw(IndexSpec *sp, FieldSpec *fs, VecSimParams *params, ArgsCursor *ac, QueryError *status, bool *rerank) {
  int rc;

  // HNSW mandatory params.
  bool mandtype = false;
  bool mandsize = false;
  bool mandmetric = false;
  // Disk-mode mandatory params (tracked here, validated later in parseVectorField)
  bool mandM = false;
  bool mandEfConstruction = false;
  bool mandEfRuntime = false;
  *rerank = false;

  // Get number of parameters and create a sub-cursor for them
  size_t expNumParam;
  if ((rc = AC_GetSize(ac, &expNumParam, 0)) != AC_OK) {
    QueryError_SetWithUserDataFmt(status, QUERY_ERROR_CODE_PARSE_ARGS, "Bad arguments", " for vector similarity number of parameters: %s", AC_Strerror(rc));
    return 0;
  }
  // Create a sub-cursor with exactly expNumParam arguments
  ArgsCursor subAc;
  if ((rc = AC_GetSlice(ac, &subAc, expNumParam)) != AC_OK) {
    QueryError_SetWithUserDataFmt(status, QUERY_ERROR_CODE_PARSE_ARGS, "Bad arguments", " for vector similarity: not enough arguments");
    return 0;
  }

  while (!AC_IsAtEnd(&subAc)) {
    if (AC_AdvanceIfMatch(&subAc, VECSIM_TYPE)) {
      if ((rc = parseVectorField_GetType(&subAc, &params->algoParams.hnswParams.type)) != AC_OK) {
        QERR_MKBADARGS_AC(status, VECSIM_ALGO_PARAM_MSG(VECSIM_ALGORITHM_HNSW, VECSIM_TYPE), rc);
        return 0;
      }
      mandtype = true;
    } else if (AC_AdvanceIfMatch(&subAc, VECSIM_DIM)) {
      if ((rc = AC_GetSize(&subAc, &params->algoParams.hnswParams.dim, AC_F_GE1)) != AC_OK) {
        QERR_MKBADARGS_AC(status, VECSIM_ALGO_PARAM_MSG(VECSIM_ALGORITHM_HNSW, VECSIM_DIM), rc);
        return 0;
      }
      mandsize = true;
    } else if (AC_AdvanceIfMatch(&subAc, VECSIM_DISTANCE_METRIC)) {
      if ((rc = parseVectorField_GetMetric(&subAc, &params->algoParams.hnswParams.metric)) != AC_OK) {
        QERR_MKBADARGS_AC(status,  VECSIM_ALGO_PARAM_MSG(VECSIM_ALGORITHM_HNSW, VECSIM_DISTANCE_METRIC), rc);
        return 0;
      }
      mandmetric = true;
    } else if (AC_AdvanceIfMatch(&subAc, VECSIM_INITIAL_CAP)) {
      if ((rc = AC_GetSize(&subAc, &params->algoParams.hnswParams.initialCapacity, 0)) != AC_OK) {
        QERR_MKBADARGS_AC(status, VECSIM_ALGO_PARAM_MSG(VECSIM_ALGORITHM_HNSW, VECSIM_INITIAL_CAP), rc);
        return 0;
      }
    } else if (AC_AdvanceIfMatch(&subAc, VECSIM_M)) {
      if ((rc = AC_GetSize(&subAc, &params->algoParams.hnswParams.M, AC_F_GE1)) != AC_OK) {
        QERR_MKBADARGS_AC(status, VECSIM_ALGO_PARAM_MSG(VECSIM_ALGORITHM_HNSW, VECSIM_M), rc);
        return 0;
      }
      mandM = true;
    } else if (AC_AdvanceIfMatch(&subAc, VECSIM_EFCONSTRUCTION)) {
      if ((rc = AC_GetSize(&subAc, &params->algoParams.hnswParams.efConstruction, AC_F_GE1)) != AC_OK) {
        QERR_MKBADARGS_AC(status, VECSIM_ALGO_PARAM_MSG(VECSIM_ALGORITHM_HNSW, VECSIM_EFCONSTRUCTION), rc);
        return 0;
      }
      mandEfConstruction = true;
    } else if (AC_AdvanceIfMatch(&subAc, VECSIM_EFRUNTIME)) {
      if ((rc = AC_GetSize(&subAc, &params->algoParams.hnswParams.efRuntime, AC_F_GE1)) != AC_OK) {
        QERR_MKBADARGS_AC(status, VECSIM_ALGO_PARAM_MSG(VECSIM_ALGORITHM_HNSW, VECSIM_EFRUNTIME), rc);
        return 0;
      }
      mandEfRuntime = true;
    } else if (AC_AdvanceIfMatch(&subAc, VECSIM_EPSILON)) {
      if ((rc = AC_GetDouble(&subAc, &params->algoParams.hnswParams.epsilon, AC_F_GE0)) != AC_OK) {
        QERR_MKBADARGS_AC(status, VECSIM_ALGO_PARAM_MSG(VECSIM_ALGORITHM_HNSW, VECSIM_EPSILON), rc);
        return 0;
      }
    } else if (AC_AdvanceIfMatch(&subAc, VECSIM_RERANK)) {
      if (*rerank) {
        QueryError_SetWithoutUserDataFmt(status, QUERY_ERROR_CODE_INVAL,
          "Duplicate RERANK parameter");
        return 0;
      }
      if (AC_IsAtEnd(&subAc)) {
        QueryError_SetError(status, QUERY_ERROR_CODE_PARSE_ARGS, VECSIM_RERANK " requires an argument");
        return 0;
      }
      size_t rerank_len;
      const char *rerank_value = AC_GetStringNC(&subAc, &rerank_len);
      if (!STR_EQCASE(rerank_value, rerank_len, "TRUE")) {
        QueryError_SetError(status, QUERY_ERROR_CODE_PARSE_ARGS,
          "Syntax error: RERANK only supports TRUE currently");
        return 0;
      }
      *rerank = true;
    } else {
      QueryError_SetWithUserDataFmt(status, QUERY_ERROR_CODE_PARSE_ARGS, "Bad arguments for algorithm", " %s: %s", VECSIM_ALGORITHM_HNSW, AC_GetStringNC(&subAc, NULL));
      return 0;
    }
  }
  if (!mandtype) {
    VECSIM_ERR_MANDATORY(status, VECSIM_ALGORITHM_HNSW, VECSIM_TYPE);
    return 0;
  }
  if (!mandsize) {
    VECSIM_ERR_MANDATORY(status, VECSIM_ALGORITHM_HNSW, VECSIM_DIM);
    return 0;
  }
  if (!mandmetric) {
    VECSIM_ERR_MANDATORY(status, VECSIM_ALGORITHM_HNSW, VECSIM_DISTANCE_METRIC);
    return 0;
  }

  // Disk-mode validation: enforce mandatory parameters
  if (isSpecOnDiskForValidation(sp)) {
    if (params->algoParams.hnswParams.type != VecSimType_FLOAT32) {
      const char *typeName = VecSimType_ToString(params->algoParams.hnswParams.type);
      QueryError_SetWithoutUserDataFmt(status, QUERY_ERROR_CODE_INVAL,
        "Disk index does not support %s vector type", typeName);
      return 0;
    }
    if (params->algoParams.hnswParams.multi) {
      QueryError_SetWithoutUserDataFmt(status, QUERY_ERROR_CODE_INVAL,
        "Disk index does not support multi-value vectors");
      return 0;
    }
    if (!mandM) {
      QueryError_SetWithoutUserDataFmt(status, QUERY_ERROR_CODE_INVAL,
        "Disk HNSW index requires M parameter");
      return 0;
    }
    if (!mandEfConstruction) {
      QueryError_SetWithoutUserDataFmt(status, QUERY_ERROR_CODE_INVAL,
        "Disk HNSW index requires EF_CONSTRUCTION parameter");
      return 0;
    }
    if (!mandEfRuntime) {
      QueryError_SetWithoutUserDataFmt(status, QUERY_ERROR_CODE_INVAL,
        "Disk HNSW index requires EF_RUNTIME parameter");
      return 0;
    }
    if (!*rerank) {
      QueryError_SetWithoutUserDataFmt(status, QUERY_ERROR_CODE_INVAL,
        "Disk HNSW index requires RERANK parameter");
      return 0;
    }
  }

  // Calculating expected blob size of a vector in bytes.
  fs->vectorOpts.expBlobSize = params->algoParams.hnswParams.dim * VecSimType_sizeof(params->algoParams.hnswParams.type);

  return parseVectorField_validate_hnsw(params, status);
}

static int parseVectorField_flat(FieldSpec *fs, VecSimParams *params, ArgsCursor *ac, QueryError *status) {
  int rc;

  // BF mandatory params.
  bool mandtype = false;
  bool mandsize = false;
  bool mandmetric = false;

  // Get number of parameters
  size_t expNumParam, numParam = 0;
  if ((rc = AC_GetSize(ac, &expNumParam, 0)) != AC_OK) {
    QueryError_SetWithUserDataFmt(status, QUERY_ERROR_CODE_PARSE_ARGS, "Bad arguments", " for vector similarity number of parameters: %s", AC_Strerror(rc));
    return 0;
  } else if (expNumParam % 2) {
    QueryError_SetWithUserDataFmt(status, QUERY_ERROR_CODE_PARSE_ARGS, "Bad number of arguments for vector similarity index", ": got %d but expected even number as algorithm parameters (should be submitted as named arguments)", expNumParam);
    return 0;
  } else {
    expNumParam /= 2;
  }

  while (expNumParam > numParam && !AC_IsAtEnd(ac)) {
    if (AC_AdvanceIfMatch(ac, VECSIM_TYPE)) {
      if ((rc = parseVectorField_GetType(ac, &params->algoParams.bfParams.type)) != AC_OK) {
        QERR_MKBADARGS_AC(status, VECSIM_ALGO_PARAM_MSG(VECSIM_ALGORITHM_BF, VECSIM_TYPE), rc);
        return 0;
      }
      mandtype = true;
    } else if (AC_AdvanceIfMatch(ac, VECSIM_DIM)) {
      if ((rc = AC_GetSize(ac, &params->algoParams.bfParams.dim, AC_F_GE1)) != AC_OK) {
        QERR_MKBADARGS_AC(status, VECSIM_ALGO_PARAM_MSG(VECSIM_ALGORITHM_BF, VECSIM_DIM), rc);
        return 0;
      }
      mandsize = true;
    } else if (AC_AdvanceIfMatch(ac, VECSIM_DISTANCE_METRIC)) {
      if ((rc = parseVectorField_GetMetric(ac, &params->algoParams.bfParams.metric)) != AC_OK) {
        QERR_MKBADARGS_AC(status, VECSIM_ALGO_PARAM_MSG(VECSIM_ALGORITHM_BF, VECSIM_DISTANCE_METRIC), rc);
        return 0;
      }
      mandmetric = true;
    } else if (AC_AdvanceIfMatch(ac, VECSIM_INITIAL_CAP)) {
      if ((rc = AC_GetSize(ac, &params->algoParams.bfParams.initialCapacity, 0)) != AC_OK) {
        QERR_MKBADARGS_AC(status, VECSIM_ALGO_PARAM_MSG(VECSIM_ALGORITHM_BF, VECSIM_INITIAL_CAP), rc);
        return 0;
      }
    } else if (AC_AdvanceIfMatch(ac, VECSIM_BLOCKSIZE)) {
      if ((rc = AC_GetSize(ac, &params->algoParams.bfParams.blockSize, AC_F_GE1)) != AC_OK) {
        QERR_MKBADARGS_AC(status, VECSIM_ALGO_PARAM_MSG(VECSIM_ALGORITHM_BF, VECSIM_BLOCKSIZE), rc);
        return 0;
      }
    } else {
      QueryError_SetWithUserDataFmt(status, QUERY_ERROR_CODE_PARSE_ARGS, "Bad arguments for algorithm", " %s: %s", VECSIM_ALGORITHM_BF, AC_GetStringNC(ac, NULL));
      return 0;
    }
    numParam++;
  }
  if (expNumParam > numParam) {
    QueryError_SetWithoutUserDataFmt(status, QUERY_ERROR_CODE_PARSE_ARGS, "Expected %d parameters but got %d", expNumParam * 2, numParam * 2);
    return 0;
  }
  if (!mandtype) {
    VECSIM_ERR_MANDATORY(status, VECSIM_ALGORITHM_BF, VECSIM_TYPE);
    return 0;
  }
  if (!mandsize) {
    VECSIM_ERR_MANDATORY(status, VECSIM_ALGORITHM_BF, VECSIM_DIM);
    return 0;
  }
  if (!mandmetric) {
    VECSIM_ERR_MANDATORY(status, VECSIM_ALGORITHM_BF, VECSIM_DISTANCE_METRIC);
    return 0;
  }
  // Calculating expected blob size of a vector in bytes.
  fs->vectorOpts.expBlobSize = params->algoParams.bfParams.dim * VecSimType_sizeof(params->algoParams.bfParams.type);

  return parseVectorField_validate_flat(&fs->vectorOpts.vecSimParams, status);
}

static int parseVectorField_svs(FieldSpec *fs, TieredIndexParams *tieredParams, ArgsCursor *ac, QueryError *status) {
  int rc;

  // SVS-VAMANA mandatory params.
  bool mandtype = false;
  bool mandsize = false;
  bool mandmetric = false;

  VecSimParams *params = tieredParams->primaryIndexParams;

  // Get number of parameters
  size_t expNumParam, numParam = 0;
  if ((rc = AC_GetSize(ac, &expNumParam, 0)) != AC_OK) {
    QERR_MKBADARGS_AC(status, "vector similarity number of parameters", rc);
    return 0;
  } else if (expNumParam % 2) {
    QueryError_SetWithUserDataFmt(status, QUERY_ERROR_CODE_PARSE_ARGS, "Bad number of arguments for vector similarity index:", " got %d but expected even number as algorithm parameters (should be submitted as named arguments)", expNumParam);
    return 0;
  } else {
    expNumParam /= 2;
  }

  while (expNumParam > numParam && !AC_IsAtEnd(ac)) {
    if (AC_AdvanceIfMatch(ac, VECSIM_TYPE)) {
      if ((rc = parseVectorField_GetType(ac, &params->algoParams.svsParams.type)) != AC_OK) {
        QERR_MKBADARGS_AC(status, VECSIM_ALGO_PARAM_MSG(VECSIM_ALGORITHM_SVS, VECSIM_TYPE), rc);
        return 0;
      } else if (params->algoParams.svsParams.type != VecSimType_FLOAT16 &&
                 params->algoParams.svsParams.type != VecSimType_FLOAT32){
            QueryError_SetWithUserDataFmt(status, QUERY_ERROR_CODE_PARSE_ARGS, "Not supported data type is given. ", "Expected: FLOAT16, FLOAT32");
            return 0;
      }
      mandtype = true;
    } else if (AC_AdvanceIfMatch(ac, VECSIM_DIM)) {
      if ((rc = AC_GetSize(ac, &params->algoParams.svsParams.dim, AC_F_GE1)) != AC_OK) {
        QERR_MKBADARGS_AC(status, VECSIM_ALGO_PARAM_MSG(VECSIM_ALGORITHM_SVS, VECSIM_DIM), rc);
        return 0;
      }
      mandsize = true;
    } else if (AC_AdvanceIfMatch(ac, VECSIM_DISTANCE_METRIC)) {
      if ((rc = parseVectorField_GetMetric(ac, &params->algoParams.svsParams.metric)) != AC_OK) {
        QERR_MKBADARGS_AC(status, VECSIM_ALGO_PARAM_MSG(VECSIM_ALGORITHM_SVS, VECSIM_DISTANCE_METRIC), rc);
        return 0;
      }
      mandmetric = true;
    } else if (AC_AdvanceIfMatch(ac, VECSIM_GRAPH_DEGREE)) {
      if ((rc = AC_GetSize(ac, &params->algoParams.svsParams.graph_max_degree, AC_F_GE1)) != AC_OK) {
        QERR_MKBADARGS_AC(status, VECSIM_ALGO_PARAM_MSG(VECSIM_ALGORITHM_SVS, VECSIM_GRAPH_DEGREE), rc);
        return 0;
      }
    } else if (AC_AdvanceIfMatch(ac, VECSIM_WINDOW_SIZE)) {
      if ((rc = AC_GetSize(ac, &params->algoParams.svsParams.construction_window_size, AC_F_GE1)) != AC_OK) {
        QERR_MKBADARGS_AC(status, VECSIM_ALGO_PARAM_MSG(VECSIM_ALGORITHM_SVS, VECSIM_WINDOW_SIZE), rc);
        return 0;
      }
    } else if (AC_AdvanceIfMatch(ac, VECSIM_COMPRESSION)) {
      if ((rc = parseVectorField_GetQuantBits(ac, &params->algoParams.svsParams.quantBits)) != AC_OK) {
        QERR_MKBADARGS_AC(status, VECSIM_ALGO_PARAM_MSG(VECSIM_ALGORITHM_SVS, VECSIM_COMPRESSION), rc);
        return 0;
      }
    } else if (AC_AdvanceIfMatch(ac, VECSIM_WSSEARCH)) {
      if ((rc = AC_GetSize(ac, &params->algoParams.svsParams.search_window_size, AC_F_GE1)) != AC_OK) {
        QERR_MKBADARGS_AC(status, VECSIM_ALGO_PARAM_MSG(VECSIM_ALGORITHM_SVS, VECSIM_WSSEARCH), rc);
        return 0;
      }
    } else if (AC_AdvanceIfMatch(ac, VECSIM_EPSILON)) {
      if ((rc = AC_GetDouble(ac, &params->algoParams.svsParams.epsilon, AC_F_GE0)) != AC_OK) {
        QERR_MKBADARGS_AC(status, VECSIM_ALGO_PARAM_MSG(VECSIM_ALGORITHM_SVS, VECSIM_EPSILON), rc);
        return 0;
      }
    } else if (AC_AdvanceIfMatch(ac, VECSIM_REDUCED_DIM)) {
      if ((rc = AC_GetSize(ac, &params->algoParams.svsParams.leanvec_dim, AC_F_GE1)) != AC_OK) {
        QERR_MKBADARGS_AC(status, VECSIM_ALGO_PARAM_MSG(VECSIM_ALGORITHM_SVS, VECSIM_REDUCED_DIM), rc);
        return 0;
      }
    } else if (AC_AdvanceIfMatch(ac, VECSIM_TRAINING_THRESHOLD)) {
      if ((rc = AC_GetSize(ac, &tieredParams->specificParams.tieredSVSParams.trainingTriggerThreshold, AC_F_GE1)) != AC_OK) {
        QERR_MKBADARGS_AC(status, VECSIM_ALGO_PARAM_MSG(VECSIM_ALGORITHM_SVS, VECSIM_TRAINING_THRESHOLD), rc);
        return 0;
      } else if (tieredParams->specificParams.tieredSVSParams.trainingTriggerThreshold < DEFAULT_BLOCK_SIZE) {
           QueryError_SetWithUserDataFmt(status, QUERY_ERROR_CODE_PARSE_ARGS, "Invalid TRAINING_THRESHOLD: cannot be lower than DEFAULT_BLOCK_SIZE ", "(%d)", DEFAULT_BLOCK_SIZE);
          return 0;
      }
    } else {
      QueryError_SetWithUserDataFmt(status, QUERY_ERROR_CODE_PARSE_ARGS, "Bad arguments for algorithm", " %s: %s", VECSIM_ALGORITHM_SVS, AC_GetStringNC(ac, NULL));
      return 0;
    }
    numParam++;
  }
  if (expNumParam > numParam) {
    QueryError_SetWithoutUserDataFmt(status, QUERY_ERROR_CODE_PARSE_ARGS, "Expected %d parameters but got %d", expNumParam * 2, numParam * 2);
    return 0;
  }
  if (!mandtype) {
    VECSIM_ERR_MANDATORY(status, VECSIM_ALGORITHM_SVS, VECSIM_TYPE);
    return 0;
  }
  if (!mandsize) {
    VECSIM_ERR_MANDATORY(status, VECSIM_ALGORITHM_SVS, VECSIM_DIM);
    return 0;
  }
  if (!mandmetric) {
    VECSIM_ERR_MANDATORY(status, VECSIM_ALGORITHM_SVS, VECSIM_DISTANCE_METRIC);
    return 0;
  }
  if (params->algoParams.svsParams.quantBits == 0 && tieredParams->specificParams.tieredSVSParams.trainingTriggerThreshold > 0) {
    QueryError_SetError(status, QUERY_ERROR_CODE_PARSE_ARGS, "TRAINING_THRESHOLD is irrelevant when compression was not requested");
    return 0;
  }
  if (!VecSim_IsLeanVecCompressionType(params->algoParams.svsParams.quantBits) && params->algoParams.svsParams.leanvec_dim > 0) {
    QueryError_SetError(status, QUERY_ERROR_CODE_PARSE_ARGS, "REDUCE is irrelevant when compression is not of type LeanVec");
    return 0;
  }
  // Calculating expected blob size of a vector in bytes.
  fs->vectorOpts.expBlobSize = params->algoParams.svsParams.dim * VecSimType_sizeof(params->algoParams.svsParams.type);

  return parseVectorField_validate_svs(params, status);
}

// Parse the arguments of a TEXT field
static int parseTextField(FieldSpec *fs, ArgsCursor *ac, QueryError *status) {
  int rc;
  fs->types |= INDEXFLD_T_FULLTEXT;

  // this is a text field
  // init default weight and type
  while (!AC_IsAtEnd(ac)) {
    if (AC_AdvanceIfMatch(ac, SPEC_NOSTEM_STR)) {
      fs->options |= FieldSpec_NoStemming;
      continue;

    } else if (AC_AdvanceIfMatch(ac, SPEC_WEIGHT_STR)) {
      double d;
      if ((rc = AC_GetDouble(ac, &d, 0)) != AC_OK) {
        QueryError_SetWithUserDataFmt(status, QUERY_ERROR_CODE_PARSE_ARGS, "Bad arguments", " for weight: %s", AC_Strerror(rc));
        return 0;
      }
      fs->ftWeight = d;
      continue;

    } else if (AC_AdvanceIfMatch(ac, SPEC_PHONETIC_STR)) {
      if (AC_IsAtEnd(ac)) {
        QueryError_SetError(status, QUERY_ERROR_CODE_PARSE_ARGS, SPEC_PHONETIC_STR " requires an argument");
        return 0;
      }

      const char *matcher = AC_GetStringNC(ac, NULL);
      // try and parse the matcher
      // currently we just make sure algorithm is double metaphone (dm)
      // and language is one of the following : English (en), French (fr), Portuguese (pt) and
      // Spanish (es)
      // in the future we will support more algorithms and more languages
      if (!checkPhoneticAlgorithmAndLang(matcher)) {
        QueryError_SetError(
            status, QUERY_ERROR_CODE_INVAL,
            "Matcher Format: <2 chars algorithm>:<2 chars language>. Support algorithms: "
            "double metaphone (dm). Supported languages: English (en), French (fr), "
            "Portuguese (pt) and Spanish (es)");
        return 0;
      }
      fs->options |= FieldSpec_Phonetics;
      continue;
    } else if (AC_AdvanceIfMatch(ac, SPEC_WITHSUFFIXTRIE_STR)) {
      if (!SearchDisk_MarkUnsupportedArgumentIfDiskEnabled(SPEC_WITHSUFFIXTRIE_STR, status)) {
        return 0;
      }
      fs->options |= FieldSpec_WithSuffixTrie;
    } else if (AC_AdvanceIfMatch(ac, SPEC_INDEXEMPTY_STR)) {
      fs->options |= FieldSpec_IndexEmpty;
    } else if (AC_AdvanceIfMatch(ac, SPEC_INDEXMISSING_STR)) {
      fs->options |= FieldSpec_IndexMissing;
    } else {
      break;
    }
  }
  return 1;
}

// Parse the arguments of a TAG field
static int parseTagField(FieldSpec *fs, ArgsCursor *ac, QueryError *status) {
    int rc = 1;
    fs->types |= INDEXFLD_T_TAG;

    while (!AC_IsAtEnd(ac)) {
      if (AC_AdvanceIfMatch(ac, SPEC_TAG_SEPARATOR_STR)) {
        if (AC_IsAtEnd(ac)) {
          QueryError_SetError(status, QUERY_ERROR_CODE_PARSE_ARGS, SPEC_TAG_SEPARATOR_STR " requires an argument");
          rc = 0;
          break;
        }
        const char *sep = AC_GetStringNC(ac, NULL);
        if (strlen(sep) != 1) {
          QueryError_SetWithUserDataFmt(status, QUERY_ERROR_CODE_PARSE_ARGS,
                                "Tag separator must be a single character. Got `%s`", sep);
          rc = 0;
          break;
        }
        fs->tagOpts.tagSep = *sep;
      } else if (AC_AdvanceIfMatch(ac, SPEC_TAG_CASE_SENSITIVE_STR)) {
        fs->tagOpts.tagFlags |= TagField_CaseSensitive;
      } else if (AC_AdvanceIfMatch(ac, SPEC_WITHSUFFIXTRIE_STR)) {
        if (!SearchDisk_MarkUnsupportedArgumentIfDiskEnabled(SPEC_WITHSUFFIXTRIE_STR, status)) {
          return 0;
        }
        fs->options |= FieldSpec_WithSuffixTrie;
      } else if (AC_AdvanceIfMatch(ac, SPEC_INDEXEMPTY_STR)) {
        fs->options |= FieldSpec_IndexEmpty;
      } else if (AC_AdvanceIfMatch(ac, SPEC_INDEXMISSING_STR)) {
        fs->options |= FieldSpec_IndexMissing;
      } else {
        break;
      }
    }

  return rc;
}

static int parseVectorField(IndexSpec *sp, StrongRef sp_ref, FieldSpec *fs, ArgsCursor *ac, QueryError *status) {
  // this is a vector field
  // init default type, size, distance metric and algorithm

  fs->types |= INDEXFLD_T_VECTOR;
  sp->flags |= Index_HasVecSim;

  memset(&fs->vectorOpts.vecSimParams, 0, sizeof(VecSimParams));
  memset(&fs->vectorOpts.diskCtx, 0, sizeof(VecSimDiskContext));

  // If the index is on JSON and the given path is dynamic, create a multi-value index.
  bool multi = false;
  if (isSpecJson(sp)) {
    RedisModuleString *err_msg;
    JSONPath jsonPath = pathParse(fs->fieldPath, &err_msg);
    if (!jsonPath) {
      if (err_msg) {
        JSONParse_error(status, err_msg, fs->fieldPath, fs->fieldName, sp->specName);
      }
      return 0;
    }
    multi = !(japi->pathIsSingle(jsonPath));
    japi->pathFree(jsonPath);
  }

  // parse algorithm
  const char *algStr;
  size_t len;
  int rc;
  int result;
  if ((rc = AC_GetString(ac, &algStr, &len, 0)) != AC_OK) {
    QueryError_SetWithUserDataFmt(status, QUERY_ERROR_CODE_PARSE_ARGS, "Bad arguments", " for vector similarity algorithm: %s", AC_Strerror(rc));
    return 0;
  }
  VecSimLogCtx *logCtx = rm_new(VecSimLogCtx);
  logCtx->index_field_name = HiddenString_GetUnsafe(fs->fieldName, NULL);
  fs->vectorOpts.vecSimParams.logCtx = logCtx;

  if (STR_EQCASE(algStr, len, VECSIM_ALGORITHM_BF)) {
    // Disk mode does not support FLAT algorithm
    if (isSpecOnDiskForValidation(sp)) {
      QueryError_SetWithoutUserDataFmt(status, QUERY_ERROR_CODE_INVAL,
        "Disk index does not support FLAT algorithm");
      rm_free(logCtx);
      fs->vectorOpts.vecSimParams.logCtx = NULL;  // Prevent double-free in cleanup
      return 0;
    }
    fs->vectorOpts.vecSimParams.algo = VecSimAlgo_BF;
    fs->vectorOpts.vecSimParams.algoParams.bfParams.initialCapacity = SIZE_MAX;
    fs->vectorOpts.vecSimParams.algoParams.bfParams.blockSize = 0;
    fs->vectorOpts.vecSimParams.algoParams.bfParams.multi = multi;
    result = parseVectorField_flat(fs, &fs->vectorOpts.vecSimParams, ac, status);
  } else if (STR_EQCASE(algStr, len, VECSIM_ALGORITHM_HNSW)) {
    fs->vectorOpts.vecSimParams.algo = VecSimAlgo_TIERED;
    VecSim_TieredParams_Init(&fs->vectorOpts.vecSimParams.algoParams.tieredParams, sp_ref);
    fs->vectorOpts.vecSimParams.algoParams.tieredParams.specificParams.tieredHnswParams.swapJobThreshold = 0; // Will be set to default value.

    VecSimParams *params = fs->vectorOpts.vecSimParams.algoParams.tieredParams.primaryIndexParams;
    params->algo = VecSimAlgo_HNSWLIB;
    params->algoParams.hnswParams.initialCapacity = SIZE_MAX;
    params->algoParams.hnswParams.blockSize = 0;
    params->algoParams.hnswParams.M = HNSW_DEFAULT_M;
    params->algoParams.hnswParams.efConstruction = HNSW_DEFAULT_EF_C;
    params->algoParams.hnswParams.efRuntime = HNSW_DEFAULT_EF_RT;
    params->algoParams.hnswParams.multi = multi;
    // Point to the same logCtx as the external wrapping VecSimParams object, which is the owner.
    params->logCtx = logCtx;
    bool rerank = false;
    result = parseVectorField_hnsw(sp, fs, params, ac, status, &rerank);
    // Build disk params if disk mode is enabled
    if (result && sp->diskSpec) {
      size_t nameLen;
      const char *namePtr = HiddenString_GetUnsafe(fs->fieldName, &nameLen);
      fs->vectorOpts.diskCtx = (VecSimDiskContext){
        .storage = sp->diskSpec,
        .indexName = rm_strndup(namePtr, nameLen),
        .indexNameLen = nameLen,
        .rerank = rerank,
      };
    }
  } else if (STR_EQCASE(algStr, len, VECSIM_ALGORITHM_SVS)) {
    // Disk mode does not support SVS algorithm
    if (isSpecOnDiskForValidation(sp)) {
      QueryError_SetWithoutUserDataFmt(status, QUERY_ERROR_CODE_INVAL,
        "Disk index does not support SVS algorithm");
      rm_free(logCtx);
      fs->vectorOpts.vecSimParams.logCtx = NULL;  // Prevent double-free in cleanup
      return 0;
    }
    fs->vectorOpts.vecSimParams.algo = VecSimAlgo_TIERED;
    VecSim_TieredParams_Init(&fs->vectorOpts.vecSimParams.algoParams.tieredParams, sp_ref);

    // primary index params allocated in VecSim_TieredParams_Init()
    TieredIndexParams *params = &fs->vectorOpts.vecSimParams.algoParams.tieredParams;
    // TODO: FT.INFO currently displays index attributes from this struct instead of
    // querying VecSim runtime info. Once vecsim provides runtime info for FT.INFO,
    // remove this duplication and pass 0 to let VecSim apply its own defaults.
    params->specificParams.tieredSVSParams.trainingTriggerThreshold = 0;  // will be set to default value if not specified by user.
    params->primaryIndexParams->algo = VecSimAlgo_SVS;
    params->primaryIndexParams->algoParams.svsParams.quantBits = VecSimSvsQuant_NONE;
    params->primaryIndexParams->algoParams.svsParams.graph_max_degree = SVS_VAMANA_DEFAULT_GRAPH_MAX_DEGREE;
    params->primaryIndexParams->algoParams.svsParams.construction_window_size = SVS_VAMANA_DEFAULT_CONSTRUCTION_WINDOW_SIZE;
    params->primaryIndexParams->algoParams.svsParams.multi = multi;
    params->primaryIndexParams->algoParams.svsParams.num_threads = workersThreadPool_NumThreads();
    params->primaryIndexParams->algoParams.svsParams.leanvec_dim = SVS_VAMANA_DEFAULT_LEANVEC_DIM;
    params->primaryIndexParams->logCtx = logCtx;
    result = parseVectorField_svs(fs, params, ac, status);
    if (!(params->primaryIndexParams->algoParams.svsParams.quantBits == VecSimSvsQuant_NONE)
      && (params->specificParams.tieredSVSParams.trainingTriggerThreshold == 0)) {
        params->specificParams.tieredSVSParams.trainingTriggerThreshold = SVS_VAMANA_DEFAULT_TRAINING_THRESHOLD;
    }
    if (VecSim_IsLeanVecCompressionType(params->primaryIndexParams->algoParams.svsParams.quantBits) &&
        params->primaryIndexParams->algoParams.svsParams.leanvec_dim == 0) {
      params->primaryIndexParams->algoParams.svsParams.leanvec_dim =
        params->primaryIndexParams->algoParams.svsParams.dim / 2;  // default value
    }

  } else {
    QueryError_SetWithUserDataFmt(status, QUERY_ERROR_CODE_PARSE_ARGS, "Bad arguments", " for vector similarity algorithm: %s", AC_Strerror(AC_ERR_ENOENT));
    return 0;
  }

  if(result != 0) {
    if (AC_AdvanceIfMatch(ac, SPEC_INDEXMISSING_STR)) {
      fs->options |= FieldSpec_IndexMissing;
    }
    return result;
  } else {
    return 0;
  }
}

static int parseGeometryField(IndexSpec *sp, FieldSpec *fs, ArgsCursor *ac, QueryError *status) {
  fs->types |= INDEXFLD_T_GEOMETRY;
  sp->flags |= Index_HasGeometry;

    if (AC_AdvanceIfMatch(ac, SPEC_GEOMETRY_FLAT_STR)) {
      fs->geometryOpts.geometryCoords = GEOMETRY_COORDS_Cartesian;
    } else if (AC_AdvanceIfMatch(ac, SPEC_GEOMETRY_SPHERE_STR)) {
      fs->geometryOpts.geometryCoords = GEOMETRY_COORDS_Geographic;
    } else {
      fs->geometryOpts.geometryCoords = GEOMETRY_COORDS_Geographic;
    }

    if (AC_AdvanceIfMatch(ac, SPEC_INDEXMISSING_STR)) {
      fs->options |= FieldSpec_IndexMissing;
    }

  return 1;
}

/* Parse a field definition from argv, at *offset. We advance offset as we progress.
 *  Returns 1 on successful parse, 0 otherwise */
int parseFieldSpec(ArgsCursor *ac, IndexSpec *sp, StrongRef sp_ref, FieldSpec *fs, QueryError *status) {
  if (AC_IsAtEnd(ac)) {
    QueryError_SetWithUserDataFmt(status, QUERY_ERROR_CODE_PARSE_ARGS, "Field", " `%s` does not have a type", HiddenString_GetUnsafe(fs->fieldName, NULL));
    return 0;
  }

  if (AC_AdvanceIfMatch(ac, SPEC_TEXT_STR)) {  // text field
    if (!parseTextField(fs, ac, status)) goto error;
    if (!FieldSpec_IndexesEmpty(fs)) {
      sp->flags |= Index_HasNonEmpty;
    }
  } else if (AC_AdvanceIfMatch(ac, SPEC_TAG_STR)) {  // tag field
    if (!parseTagField(fs, ac, status)) goto error;
    if (!FieldSpec_IndexesEmpty(fs)) {
      sp->flags |= Index_HasNonEmpty;
    }
  } else if (AC_AdvanceIfMatch(ac, SPEC_GEOMETRY_STR)) {  // geometry field
    if (!SearchDisk_MarkUnsupportedFieldIfDiskEnabled(SPEC_GEOMETRY_STR, fs, status)) goto error;
    if (!parseGeometryField(sp, fs, ac, status)) goto error;
  } else if (AC_AdvanceIfMatch(ac, SPEC_VECTOR_STR)) {  // vector field
    if (!parseVectorField(sp, sp_ref, fs, ac, status)) goto error;
    // Skip SORTABLE and NOINDEX options
    return 1;
  } else if (AC_AdvanceIfMatch(ac, SPEC_NUMERIC_STR)) {  // numeric field
    if (!SearchDisk_MarkUnsupportedFieldIfDiskEnabled(SPEC_NUMERIC_STR, fs, status)) goto error;
    fs->types |= INDEXFLD_T_NUMERIC;
    if (AC_AdvanceIfMatch(ac, SPEC_INDEXMISSING_STR)) {
      fs->options |= FieldSpec_IndexMissing;
    }
  } else if (AC_AdvanceIfMatch(ac, SPEC_GEO_STR)) {  // geo field
    if (!SearchDisk_MarkUnsupportedFieldIfDiskEnabled(SPEC_GEO_STR, fs, status)) goto error;
    fs->types |= INDEXFLD_T_GEO;
    if (AC_AdvanceIfMatch(ac, SPEC_INDEXMISSING_STR)) {
      fs->options |= FieldSpec_IndexMissing;
    }
  } else {
    QueryError_SetWithUserDataFmt(status, QUERY_ERROR_CODE_PARSE_ARGS, "Invalid field type for field", " `%s`", HiddenString_GetUnsafe(fs->fieldName, NULL));
    goto error;
  }

  while (!AC_IsAtEnd(ac)) {
    if (AC_AdvanceIfMatch(ac, SPEC_SORTABLE_STR)) {
      FieldSpec_SetSortable(fs);
      if (AC_AdvanceIfMatch(ac, SPEC_UNF_STR) ||      // Explicitly requested UNF
          FIELD_IS(fs, INDEXFLD_T_NUMERIC) ||         // We don't normalize numeric fields. Implicit UNF
          TAG_FIELD_IS(fs, TagField_CaseSensitive)) { // We don't normalize case sensitive tags. Implicit UNF
        fs->options |= FieldSpec_UNF;
      }
      continue;
    } else if (AC_AdvanceIfMatch(ac, SPEC_NOINDEX_STR)) {
      fs->options |= FieldSpec_NotIndexable;
      continue;
    } else {
      break;
    }
  }
  // We don't allow both NOINDEX and INDEXMISSING, since the missing values will
  // not contribute and thus this doesn't make sense.
  if (!FieldSpec_IsIndexable(fs) && FieldSpec_IndexesMissing(fs)) {
    QueryError_SetWithUserDataFmt(status, QUERY_ERROR_CODE_PARSE_ARGS, "'Field cannot be defined with both `NOINDEX` and `INDEXMISSING`", " `%s` '", HiddenString_GetUnsafe(fs->fieldName, NULL));
    goto error;
  }
  return 1;

error:
  if (!QueryError_HasError(status)) {
    QueryError_SetWithUserDataFmt(status, QUERY_ERROR_CODE_PARSE_ARGS, "Could not parse schema for field", " `%s`", HiddenString_GetUnsafe(fs->fieldName, NULL));
  }
  return 0;
}

/**
 * Add fields to an existing (or newly created) index. If the addition fails,
 */
int IndexSpec_AddFieldsInternal(IndexSpec *sp, StrongRef spec_ref, ArgsCursor *ac,
                                       QueryError *status, int isNew) {
  if (AC_IsAtEnd(ac)) {
    QueryError_SetError(status, QUERY_ERROR_CODE_PARSE_ARGS, "Fields arguments are missing");
    return 0;
  }

  const size_t prevNumFields = sp->numFields;
  const size_t prevSortLen = sp->numSortableFields;
  const IndexFlags prevFlags = sp->flags;

  while (!AC_IsAtEnd(ac)) {
    if (sp->numFields == SPEC_MAX_FIELDS) {
      QueryError_SetWithoutUserDataFmt(status, QUERY_ERROR_CODE_LIMIT, "Schema is limited to %d fields",
                             SPEC_MAX_FIELDS);
      goto reset;
    }

    // Parse path and name of field
    size_t pathlen, namelen;
    const char *fieldPath = AC_GetStringNC(ac, &pathlen);
    const char *fieldName = fieldPath;
    if (AC_AdvanceIfMatch(ac, SPEC_AS_STR)) {
      if (AC_IsAtEnd(ac)) {
        QueryError_SetError(status, QUERY_ERROR_CODE_PARSE_ARGS, SPEC_AS_STR " requires an argument");
        goto reset;
      }
      fieldName = AC_GetStringNC(ac, &namelen);
      sp->flags |= Index_HasFieldAlias;
    } else {
      // if `AS` is not used, set the path as name
      namelen = pathlen;
      fieldPath = NULL;
    }

    if (IndexSpec_GetFieldWithLength(sp, fieldName, namelen)) {
      QueryError_SetWithUserDataFmt(status, QUERY_ERROR_CODE_INVAL, "Duplicate field in schema", " - %s", fieldName);
      goto reset;
    }

    FieldSpec *fs = IndexSpec_CreateField(sp, fieldName, fieldPath);
    if (!fs) {
      QueryError_SetWithUserDataFmt(status, QUERY_ERROR_CODE_LIMIT, "Schema is currently limited", " to %d fields",
                             sp->numFields);
      goto reset;
    }
    if (!parseFieldSpec(ac, sp, spec_ref, fs, status)) {
      goto reset;
    }

    if (isSpecOnDiskForValidation(sp))
    {
      if (!FIELD_IS(fs, INDEXFLD_T_FULLTEXT) && !FIELD_IS(fs, INDEXFLD_T_VECTOR) && !FIELD_IS(fs, INDEXFLD_T_TAG)) {
        QueryError_SetWithoutUserDataFmt(status, QUERY_ERROR_CODE_INVAL, "Disk index does not support non-TEXT/VECTOR/TAG fields");
        goto reset;
      }
      if (fs->options & FieldSpec_NotIndexable) {
        QueryError_SetWithoutUserDataFmt(status, QUERY_ERROR_CODE_INVAL, "Disk index does not support NOINDEX fields");
        goto reset;
      }
      if (fs->options & FieldSpec_Sortable) {
        QueryError_SetWithoutUserDataFmt(status, QUERY_ERROR_CODE_INVAL, "Disk index does not support SORTABLE fields");
        goto reset;
      }
      if (fs->options & FieldSpec_IndexMissing) {
        QueryError_SetWithoutUserDataFmt(status, QUERY_ERROR_CODE_INVAL, "Disk index does not support INDEXMISSING fields");
        goto reset;
      }
      if (fs->options & FieldSpec_IndexEmpty) {
        QueryError_SetWithoutUserDataFmt(status, QUERY_ERROR_CODE_INVAL, "Disk index does not support INDEXEMPTY fields");
        goto reset;
      }
    }

    if (FIELD_IS(fs, INDEXFLD_T_FULLTEXT) && FieldSpec_IsIndexable(fs)) {
      int textId = IndexSpec_CreateTextId(sp, fs->index);
      if (textId < 0) {
        QueryError_SetWithoutUserDataFmt(status, QUERY_ERROR_CODE_LIMIT, "Schema is limited to %d TEXT fields",
                               SPEC_MAX_FIELD_ID);
        goto reset;
      }

      // If we need to store field flags and we have over 32 fields, we need to switch to wide
      // schema encoding
      if (textId >= SPEC_WIDEFIELD_THRESHOLD && (sp->flags & Index_StoreFieldFlags)) {
        if (isNew) {
          sp->flags |= Index_WideSchema;
        } else if ((sp->flags & Index_WideSchema) == 0) {
          QueryError_SetError(
              status, QUERY_ERROR_CODE_LIMIT,
              "Cannot add more fields. Declare index with wide fields to allow adding "
              "unlimited fields");
          goto reset;
        }
      }
      fs->ftId = textId;
      if isSpecJson(sp) {
        if ((sp->flags & Index_HasFieldAlias) && (sp->flags & Index_StoreTermOffsets)) {
          RedisModuleString *err_msg;
          JSONPath jsonPath = pathParse(fs->fieldPath, &err_msg);
          if (jsonPath && japi->pathHasDefinedOrder(jsonPath)) {
            // Ordering is well defined
            fs->options &= ~FieldSpec_UndefinedOrder;
          } else {
            // Mark FieldSpec
            fs->options |= FieldSpec_UndefinedOrder;
            // Mark IndexSpec
            sp->flags |= Index_HasUndefinedOrder;
          }
          if (jsonPath) {
            japi->pathFree(jsonPath);
          } else if (err_msg) {
            JSONParse_error(status, err_msg, fs->fieldPath, fs->fieldName, sp->specName);
            goto reset;
          } /* else {
            RedisModule_Log(RSDummyContext, "notice",
                            "missing RedisJSON API to parse JSONPath '%s' in attribute '%s' in index '%s', assuming undefined ordering",
                            fs->path, fs->name, sp->name);
          } */
        }
      }
    }

    if (FieldSpec_IsSortable(fs)) {
      if (isSpecJson(sp)) {
        // SORTABLE JSON field is always UNF
        fs->options |= FieldSpec_UNF;
      }

      if (fs->options & FieldSpec_Dynamic) {
        QueryError_SetWithUserDataFmt(status, QUERY_ERROR_CODE_BAD_OPTION,
                               "Cannot set dynamic field to sortable - %s", fieldName);
        goto reset;
      }

      fs->sortIdx = sp->numSortableFields++;
      if (fs->sortIdx == -1) {
        QueryError_SetWithoutUserDataFmt(status, QUERY_ERROR_CODE_LIMIT, "Schema is limited to %d Sortable fields",
                               SPEC_MAX_FIELDS);
        goto reset;
      }
    } else {
      fs->sortIdx = -1;
    }
    if (FieldSpec_IsPhonetics(fs)) {
      sp->flags |= Index_HasPhonetic;
    }
    if (FIELD_IS(fs, INDEXFLD_T_FULLTEXT) && FieldSpec_HasSuffixTrie(fs)) {
      sp->suffixMask |= FIELD_BIT(fs);
      if (!sp->suffix) {
        sp->flags |= Index_HasSuffixTrie;
        sp->suffix = NewTrie(suffixTrie_freeCallback, Trie_Sort_Lex);
      }
    }
  }

  // If we successfully modified the schema, we need to update the spec cache
  IndexSpecCache_Decref(sp->spcache);
  sp->spcache = IndexSpec_BuildSpecCache(sp);

  for (size_t ii = prevNumFields; ii < sp->numFields; ++ii) {
    FieldsGlobalStats_UpdateStats(sp->fields + ii, 1);
  }

  return 1;

reset:
  for (size_t ii = prevNumFields; ii < sp->numFields; ++ii) {
    IndexError_Clear(sp->fields[ii].indexError);
    FieldSpec_Cleanup(&sp->fields[ii]);
  }

  sp->numFields = prevNumFields;
  sp->numSortableFields = prevSortLen;
  // TODO: Why is this masking performed?
  sp->flags = prevFlags | (sp->flags & Index_HasSuffixTrie);
  return 0;
}

// Assumes the spec is locked for write
int IndexSpec_AddFields(StrongRef spec_ref, IndexSpec *sp, RedisModuleCtx *ctx, ArgsCursor *ac, bool initialScan,
                        QueryError *status) {
  setMemoryInfo(ctx);

  int rc = IndexSpec_AddFieldsInternal(sp, spec_ref, ac, status, 0);
  if (rc && initialScan) {
    IndexSpec_ScanAndReindex(ctx, spec_ref);
  }

  return rc;
}
