/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#pragma once

#ifdef __cplusplus
extern "C" {
#endif
#include <stddef.h>
#include <stdint.h>
#include <limits.h>
#include <stdbool.h>

// Common definitions
#define DEFAULT_BLOCK_SIZE 1024
#define INVALID_ID         UINT_MAX
#define INVALID_LABEL      SIZE_MAX
#define UNUSED(x)          (void)(x)

// Hybrid policy values
#define VECSIM_POLICY_ADHOC_BF "adhoc_bf"
#define VECSIM_POLICY_BATCHES  "batches"
#define VECSIM_POLICY_INVALID  "invalid_policy"

// HNSW default parameters
#define HNSW_DEFAULT_M       16
#define HNSW_DEFAULT_EF_C    200
#define HNSW_DEFAULT_EF_RT   10
#define HNSW_DEFAULT_EPSILON 0.01

#define HNSW_INVALID_LEVEL SIZE_MAX
#define INVALID_JOB_ID     UINT_MAX
#define INVALID_INFO       UINT_MAX

// SVS-Vamana default parameters
#define SVS_VAMANA_DEFAULT_ALPHA_L2                 1.2f
#define SVS_VAMANA_DEFAULT_ALPHA_IP                 0.95f
#define SVS_VAMANA_DEFAULT_GRAPH_MAX_DEGREE         32
#define SVS_VAMANA_DEFAULT_CONSTRUCTION_WINDOW_SIZE 200
#define SVS_VAMANA_DEFAULT_USE_SEARCH_HISTORY       true
#define SVS_VAMANA_DEFAULT_NUM_THREADS              1
// NOTE: optimal training threshold may depend on the SVSIndex compression mode.
// it might be good to implement a utility to compute default threshold based on index parameters
// DEFAULT_BLOCK_SIZE is used to round the training threshold to FLAT index blocks
#define SVS_VAMANA_DEFAULT_TRAINING_THRESHOLD (10 * DEFAULT_BLOCK_SIZE) // 10 * 1024 vectors
// Default batch update threshold for SVS index.
#define SVS_VAMANA_DEFAULT_UPDATE_THRESHOLD   (1 * DEFAULT_BLOCK_SIZE) // 1 * 1024 vectors
#define SVS_VAMANA_DEFAULT_SEARCH_WINDOW_SIZE 10
// NOTE: No need to have SVS_VAMANA_DEFAULT_SEARCH_BUFFER_CAPACITY
// as the default is determined by the search_window_size
#define SVS_VAMANA_DEFAULT_LEANVEC_DIM 0
#define SVS_VAMANA_DEFAULT_EPSILON     0.01f

// Datatypes for indexing.
typedef enum {
    VecSimType_FLOAT32,
    VecSimType_FLOAT64,
    VecSimType_BFLOAT16,
    VecSimType_FLOAT16,
    VecSimType_INT8,
    VecSimType_UINT8,
    VecSimType_INT32,
    VecSimType_INT64
} VecSimType;

// Quantization schemes supported by HNSW.
typedef enum {
    VecSimQuant_NONE = 0, // No quantization (default).
    // 8-bit scalar quantization with optional mean centering.
    VecSimQuant_SQ8 = 1,
} VecSimQuantType;

// Algorithm type/library.
typedef enum { VecSimAlgo_BF, VecSimAlgo_HNSWLIB, VecSimAlgo_TIERED, VecSimAlgo_SVS } VecSimAlgo;

typedef enum {
    VecSimOption_AUTO = 0,
    VecSimOption_ENABLE = 1,
    VecSimOption_DISABLE = 2,
} VecSimOptionMode;

typedef enum {
    VecSimBool_TRUE = 1,
    VecSimBool_FALSE = 0,
    VecSimBool_UNSET = -1,
} VecSimBool;

// Distance metric
typedef enum { VecSimMetric_L2, VecSimMetric_IP, VecSimMetric_Cosine } VecSimMetric;

typedef size_t labelType;
typedef unsigned int idType;

/**
 * @brief Query Runtime raw parameters.
 * Use VecSimIndex_ResolveParams to generate VecSimQueryParams from array of VecSimRawParams.
 *
 */
typedef struct {
    const char *name;
    size_t nameLen;
    const char *value;
    size_t valLen;
} VecSimRawParam;

#define VecSim_OK 0

typedef enum {
    VecSimParamResolver_OK = VecSim_OK, // for returning VecSim_OK as an enum value
    VecSimParamResolverErr_NullParam,
    VecSimParamResolverErr_AlreadySet,
    VecSimParamResolverErr_UnknownParam,
    VecSimParamResolverErr_BadValue,
    VecSimParamResolverErr_InvalidPolicy_NExits,
    VecSimParamResolverErr_InvalidPolicy_NHybrid,
    VecSimParamResolverErr_InvalidPolicy_NRange,
    VecSimParamResolverErr_InvalidPolicy_AdHoc_With_BatchSize,
    VecSimParamResolverErr_InvalidPolicy_AdHoc_With_EfRuntime
} VecSimResolveCode;

typedef enum {
    VecSimDebugCommandCode_OK = VecSim_OK, // for returning VecSim_OK as an enum value
    VecSimDebugCommandCode_BadIndex,
    VecSimDebugCommandCode_LabelNotExists,
    VecSimDebugCommandCode_MultiNotSupported
} VecSimDebugCommandCode;

// Outcome of `relabelVector`. The rejections are reported separately because a caller acts on them
// differently: `OldLabelMissing` and `SameLabel` mean there is nothing to do, `NewLabelTaken` is a
// caller-side conflict to resolve, and `Unsupported` says this index type never relabels - so the
// caller has to fall back to delete + insert rather than treat it as a no-op.
typedef enum {
    VecSimRelabel_OK = VecSim_OK,  // for returning VecSim_OK as an enum value
    VecSimRelabel_OldLabelMissing, // `old_label` is not in the index
    VecSimRelabel_NewLabelTaken,   // `new_label` is already in the index
    VecSimRelabel_SameLabel,       // `old_label` and `new_label` are equal
    VecSimRelabel_Unsupported      // this index type does not implement relabeling
} VecSimRelabelCode;

// Outcome of `updateVectors`. The refusals are told apart because a caller acts on them
// differently: `MultiNotSupported` is more vectors than a single-value index can hold under one
// label and is the caller's doing, `Unsupported` says this index type does not implement the
// operation at all - so the caller has to delete the label and insert the vectors itself - and
// `Failed` is the index having tried and not managed it. The first two leave the index untouched;
// `Failed` does not, since the label's vectors are removed before the new ones are stored, so the
// label may be left holding nothing and the caller has to re-insert to get back to a known state.
typedef enum {
    VecSimUpdate_OK = VecSim_OK,    // for returning VecSim_OK as an enum value
    VecSimUpdate_MultiNotSupported, // more than one vector was given to a single-value index
    VecSimUpdate_Unsupported,       // this index type does not implement updating a label
    VecSimUpdate_Failed // the index could not store them, and the label's contents are undefined
} VecSimUpdateCode;

typedef struct AsyncJob AsyncJob; // forward declaration.

// Write async/sync mode
typedef enum { VecSim_WriteAsync, VecSim_WriteInPlace } VecSimWriteMode;

/**
 * Callback signatures for asynchronous tiered index.
 */
typedef void (*JobCallback)(AsyncJob *);
typedef int (*SubmitCB)(void *job_queue, void *index_ctx, AsyncJob **jobs, JobCallback *CBs,
                        size_t jobs_len);

/**
 * Callback signature for throttle control in disk tiered index.
 */
typedef int (*ThrottleCB)(void);

/**
 * @brief Index initialization parameters.
 *
 */
typedef struct VecSimParams VecSimParams;
typedef struct {
    VecSimType type;        // Datatype to index.
    size_t dim;             // Vector's dimension.
    VecSimMetric metric;    // Distance metric to use in the index.
    bool multi;             // Determines if the index should multi-index or not.
    size_t initialCapacity; // Deprecated
    size_t blockSize;
    size_t M;
    size_t efConstruction;
    size_t efRuntime;
    double epsilon;
    VecSimQuantType quantType; // Defaults to VecSimQuant_NONE.
    // SQ8 mean vector (float[dim]); NULL disables mean centering. Copied during construction.
    // L2 centering keeps queries in FLOAT32, including when the input type is FLOAT16.
    const void *quantParams;
} HNSWParams;

typedef struct {
    VecSimType type;        // Datatype to index.
    size_t dim;             // Vector's dimension.
    VecSimMetric metric;    // Distance metric to use in the index.
    bool multi;             // Determines if the index should multi-index or not.
    size_t initialCapacity; // Deprecated.
    size_t blockSize;
} BFParams;

typedef enum {
    VecSimSvsQuant_NONE = 0,           // No quantization.
    VecSimSvsQuant_Scalar = 1,         // 8-bit scalar quantization
    VecSimSvsQuant_4 = 4,              // 4-bit quantization
    VecSimSvsQuant_8 = 8,              // 8-bit quantization
    VecSimSvsQuant_4x4 = 4 | (4 << 8), // 4-bit quantization with 4-bit residuals
    VecSimSvsQuant_4x8 = 4 | (8 << 8), // 4-bit quantization with 8-bit residuals
    VecSimSvsQuant_4x8_LeanVec = 4 | (8 << 8) | (1 << 16), // LeanVec 4x8 quantization
    VecSimSvsQuant_8x8_LeanVec = 8 | (8 << 8) | (1 << 16), // LeanVec 8x8 quantization
} VecSimSvsQuantBits;

typedef struct {
    VecSimType type;     // Datatype to index.
    size_t dim;          // Vector's dimension.
    VecSimMetric metric; // Distance metric to use in the index.
    bool multi;          // Determines if the index should multi-index or not.
    size_t blockSize;

    /* SVS-Vamana specifics. See Intel ScalableVectorSearch documentation */
    VecSimSvsQuantBits quantBits;    // Quantization level.
    float alpha;                     // The pruning parameter.
    size_t graph_max_degree;         // Maximum degree in the graph.
    size_t construction_window_size; // Search window size to use during graph construction.
    size_t max_candidate_pool_size;  // Limit on the number of neighbors considered during pruning.
    size_t prune_to;                 // Amount that candidates will be pruned.
    VecSimOptionMode use_search_history; // Either the contents of the search buffer can be used or
                                         // the entire search history.
    size_t num_threads;                  // DEPRECATED: ignored. Thread pool size is controlled
                                         // globally via VecSim_UpdateThreadPoolSize(). Setting
                                         // this field has no effect and will emit a warning log.
    size_t search_window_size;           // Search window size to use during search.
    size_t search_buffer_capacity;       // Search buffer capacity to use during search.
    size_t leanvec_dim;                  // Leanvec dimension to use when LeanVec is enabled.
    double epsilon; // Epsilon parameter for SVS graph accuracy/latency for range search.
} SVSParams;

// A struct that contains HNSW tiered index specific params.
typedef struct {
    size_t swapJobThreshold; // The minimum number of swap jobs to accumulate before applying
                             // all the ready swap jobs in a batch.
    size_t QuantNormalizationSetSize; // Number of vectors to accumulate before SQ initialization.
                                      // 0 = skip training; use quantParams as the mean if supplied.
                                      // Max: 100 * DEFAULT_BLOCK_SIZE (102400).
                                      // During accumulation, this threshold takes precedence over
                                      // flatBufferLimit, so the flat index may exceed that limit.
} TieredHNSWParams;

// A struct that contains HNSW Disk tiered index specific params.
// Consider removing and use TieredHNSWParams instead if they both share swapJobThreshold
typedef struct {
    size_t QuantNormalizationSetSize; // Training threshold, with the semantics of
                                      // TieredHNSWParams::QuantNormalizationSetSize.
} TieredHNSWDiskParams;

// A struct that contains SVS tiered index specific params.
typedef struct {
    size_t trainingTriggerThreshold; // The flat index size threshold to trigger the initialization
                                     // of backend index.
    size_t updateTriggerThreshold; // The flat index size threshold to trigger the vectors migration
                                   // to backend index.
    size_t updateJobWaitTime;      // The time (microseconds) to wait for Redis threads reservation
                                   // before executing the scheduled SVS Index update job.
} TieredSVSParams;

// A struct that contains the common tiered index params.
typedef struct {
    void *jobQueue;         // External queue that holds the jobs.
    void *jobQueueCtx;      // External context to be sent to the submit callback.
    SubmitCB submitCb;      // A callback that submits an array of jobs into a given jobQueue.
    size_t flatBufferLimit; // Maximum size allowed for the flat buffer. If flat buffer is full, use
                            // in-place insertion.
    VecSimParams *primaryIndexParams; // Parameters to initialize the index.
    union {
        TieredHNSWParams tieredHnswParams;
        TieredSVSParams tieredSVSParams;
        TieredHNSWDiskParams tieredHnswDiskParams;
    } specificParams;
} TieredIndexParams;

typedef union {
    HNSWParams hnswParams;
    BFParams bfParams;
    TieredIndexParams tieredParams;
    SVSParams svsParams;
} AlgoParams;

struct VecSimParams {
    VecSimAlgo algo; // Algorithm to use.
    AlgoParams algoParams;
    void *logCtx; // External context that stores the index log.
};

typedef struct {
    void *storage; // Opaque pointer to disk storage
    const char *indexName;
    size_t indexNameLen;
    uint32_t userData; // Embedder-defined; forwarded to the storage layer, never read by VecSim.
    bool rerank;       // Whether to enable reranking for disk-based HNSW
} VecSimDiskContext;

typedef struct {
    VecSimParams *indexParams;
    VecSimDiskContext *diskContext;
} VecSimParamsDisk;

/**
 * The specific job types in use (to be extended in the future by demand)
 */
typedef enum {
    HNSW_INSERT_VECTOR_JOB,
    HNSW_REPAIR_NODE_CONNECTIONS_JOB,
    HNSW_SEARCH_JOB,
    HNSW_SWAP_JOB,
    HNSW_DISK_JOB,
    SVS_BATCH_UPDATE_JOB,
    SVS_INSERT_VECTOR_JOB,
    SVS_GC_JOB,
    SVS_CONSOLIDATE_JOB,
    INVALID_JOB // to indicate that finding a JobType >= INVALID_JOB is an error
} JobType;

typedef struct {
    size_t efRuntime; // EF parameter for HNSW graph accuracy/latency for search.
    double epsilon;   // Epsilon parameter for HNSW graph accuracy/latency for range search.
} HNSWRuntimeParams;

typedef struct {
    size_t efRuntime;        // EF parameter for HNSW graph accuracy/latency for search.
    double epsilon;          // Epsilon parameter for HNSW graph accuracy/latency for range search.
    VecSimBool shouldRerank; // Whether to enable reranking for disk-based HNSW (True/False/Unset).
} HNSWDiskRuntimeParams;

typedef struct {
    size_t windowSize;     // Search window size for Vamana graph accuracy/latency tune.
    size_t bufferCapacity; // Search buffer capacity for Vamana graph accuracy/latency tune.
    VecSimOptionMode searchHistory; // Enabling of the visited set for search.
    double epsilon; // Epsilon parameter for SVS graph accuracy/latency for range search.
} SVSRuntimeParams;

/**
 * @brief Query runtime information - the search mode in RediSearch (used for debug/testing).
 *
 */
typedef enum {
    EMPTY_MODE,      // Default value to initialize the "lastMode" field with.
    STANDARD_KNN,    // Run k-nn query over the entire vector index.
    HYBRID_ADHOC_BF, // Measure ad-hoc the distance for every result that passes the filters,
                     // and take the top k results.
    HYBRID_BATCHES,  // Get the top vector results in batches upon demand, and keep the results that
                     // passes the filters until we reach k results.
    HYBRID_BATCHES_TO_ADHOC_BF, // Start with batches and dynamically switched to ad-hoc BF.
    RANGE_QUERY, // Run range query, to return all vectors that are within a given range from the
                 // query vector.
} VecSearchMode;

typedef enum {
    QUERY_TYPE_NONE, // Use when no params are given.
    QUERY_TYPE_KNN,
    QUERY_TYPE_HYBRID,
    QUERY_TYPE_RANGE,
} VecsimQueryType;

/**
 * @brief Query Runtime parameters.
 *
 */
typedef struct {
    union {
        HNSWRuntimeParams hnswRuntimeParams;
        HNSWDiskRuntimeParams hnswDiskRuntimeParams;
        SVSRuntimeParams svsRuntimeParams;
    };
    size_t batchSize;
    VecSearchMode searchMode;
    void *timeoutCtx; // This parameter is not exposed directly to the user, and we shouldn't expect
                      // to get it from the parameters resolve function.
} VecSimQueryParams;

/**
 * Index info that is static and immutable (cannot be changed over time)
 */
typedef struct {
    VecSimAlgo algo;     // Algorithm being used (if index is tiered, this is the backend index).
    VecSimMetric metric; // Index distance metric
    VecSimType type;     // Datatype the index holds.
    bool isMulti;        // Determines if the index should multi-index or not.
    bool isTiered;       // Is the index is tiered or not.
    bool isDisk;         // Is the index stored on disk.
    size_t blockSize;    // Brute force algorithm vector block (mini matrix) size
    size_t dim;          // Vector size (dimension).
} VecSimIndexBasicInfo;

/**
 * Index info for statistics - a thin and efficient (no locks, no calculations) info. Can be used in
 * production without worrying about performance
 */
typedef struct {
    size_t memory;                // Memory tracked by the index's own allocator. Does NOT include
                                  // process-wide allocations such as the shared SVS thread pool;
                                  // those are reported via VecSim_GetSharedMemory() so callers
                                  // that aggregate across indexes don't double-count them.
    size_t numberOfMarkedDeleted; // The number of vectors that are marked as deleted (HNSW/tiered
                                  // only).
    size_t directHNSWInsertions;  // Count of vectors inserted directly into HNSW by main thread
                                  // (bypassing flat buffer). Tiered HNSW only.
    size_t flatBufferSize;        // Current flat buffer size. Tiered indexes only.
} VecSimIndexStatsInfo;

typedef struct {
    VecSimIndexBasicInfo basicInfo; // Index immutable meta-data.
    size_t indexSize;               // Current count of vectors.
    size_t indexLabelCount;         // Current unique count of labels.
    uint64_t memory;                // Index memory consumption.
    VecSearchMode lastMode;         // The mode in which the last query ran.
} CommonInfo;

typedef struct {
    size_t M;              // Number of allowed edges per node in graph.
    size_t efConstruction; // EF parameter for HNSW graph accuracy/latency for indexing.
    size_t efRuntime;      // EF parameter for HNSW graph accuracy/latency for search.
    double epsilon;        // Epsilon parameter for HNSW graph accuracy/latency for range search.
    size_t max_level;      // Number of graph levels.
    size_t entrypoint;     // Entrypoint vector label.
    size_t visitedNodesPoolSize;       // The max number of parallel graph scans so far.
    size_t numberOfMarkedDeletedNodes; // The number of nodes that are marked as deleted.
} hnswInfoStruct;

typedef struct {
    char dummy; // For not having this as an empty struct, can be removed after we extend this.
} bfInfoStruct;

typedef struct {
    VecSimSvsQuantBits quantBits;  // Quantization flavor.
    float alpha;                   // The pruning parameter.
    size_t graphMaxDegree;         // Maximum degree in the graph.
    size_t constructionWindowSize; // Search window size to use during graph construction.
    size_t maxCandidatePoolSize;   // Limit on the number of neighbors considered during pruning.
    size_t pruneTo;                // Amount that candidates will be pruned.
    bool useSearchHistory;         // Either the contents of the search buffer can be used or
                                   // the entire search history.
    size_t numThreads;             // Maximum number of threads to be used by svs for ingestion.
    size_t lastReservedThreads;    // Number of threads that were successfully reserved by the last
                                   // ingestion operation.
    size_t numberOfMarkedDeletedNodes; // The number of nodes that are marked as deleted.
    size_t searchWindowSize;           // Search window size for Vamana graph accuracy/latency tune.
    size_t searchBufferCapacity; // Search buffer capacity for Vamana graph accuracy/latency tune.
    size_t leanvecDim;           // Leanvec dimension to use when LeanVec is enabled.
    double epsilon; // Epsilon parameter for SVS graph accuracy/latency for range search.
} svsInfoStruct;

typedef struct HnswTieredInfo {
    size_t pendingSwapJobsThreshold;
    size_t defragRuns;   // Number of defrag() calls so far.
    size_t defragTimeNs; // Wall time spent in defrag(), including the main index guard wait.
} HnswTieredInfo;

typedef struct SvsTieredInfo {
    size_t trainingTriggerThreshold;
    size_t updateTriggerThreshold;
    size_t updateJobWaitTime; // The time (microseconds) to wait for Redis threads reservation
                              // before executing the scheduled SVS Index update job.
    bool indexUpdateScheduled;
    size_t defragRuns;   // Number of backend GC runs so far.
    size_t defragTimeNs; // Wall time spent in backend GC, including lock waits.
} SvsTieredInfo;

typedef struct {

    // Since we cannot recursively have a struct that contains itself, we need this workaround.
    union {
        hnswInfoStruct hnswInfo;
        svsInfoStruct svsInfo;
    } backendInfo; // The backend index info.
    union {
        HnswTieredInfo hnswTieredInfo;
        SvsTieredInfo svsTieredInfo;
    } specificTieredBackendInfo;   // Info relevant for tiered index with a specific backend.
    CommonInfo backendCommonInfo;  // Common index info.
    CommonInfo frontendCommonInfo; // Common index info.
    bfInfoStruct bfInfo;           // The brute force index info.

    uint64_t management_layer_memory; // Memory consumption of the management layer.
    VecSimBool backgroundIndexing;    // Determines if the index is currently being indexed in the
                                      // background.
    size_t bufferLimit;               // Maximum number of vectors allowed in the flat buffer.
} tieredInfoStruct;

/**
 * @brief Index information. Should only be used for debug/testing.
 *
 */
typedef struct {
    CommonInfo commonInfo;
    union {
        bfInfoStruct bfInfo;
        hnswInfoStruct hnswInfo;
        svsInfoStruct svsInfo;
        tieredInfoStruct tieredInfo;
    };
} VecSimIndexDebugInfo;

// Memory function declarations.
typedef void *(*allocFn)(size_t n);
typedef void *(*callocFn)(size_t nelem, size_t elemsz);
typedef void *(*reallocFn)(void *p, size_t n);
typedef void (*freeFn)(void *p);
typedef char *(*strdupFn)(const char *s);

/**
 * @brief A struct to pass 3rd party memory functions to Vecsimlib.
 *
 */
typedef struct {
    allocFn allocFunction;     // Malloc like function.
    callocFn callocFunction;   // Calloc like function.
    reallocFn reallocFunction; // Realloc like function.
    freeFn freeFunction;       // Free function.
} VecSimMemoryFunctions;

/**
 * @brief A struct to pass 3rd party timeout function to Vecsimlib.
 * @param ctx some generic context to pass to the function
 * @return the function should return a non-zero value on timeout
 */
typedef int (*timeoutCallbackFunction)(void *ctx);

/**
 * @brief A struct to pass 3rd party logging function to Vecsimlib.
 * @param ctx some generic context to pass to the function
 * @param level loglevel (in redis we should choose from: "warning", "notice", "verbose", "debug")
 * @param message the message to log
 */
typedef void (*logCallbackFunction)(void *ctx, const char *level, const char *message);

// Round up to the nearest multiplication of blockSize.
static inline size_t RoundUpInitialCapacity(size_t initialCapacity, size_t blockSize) {
    return initialCapacity % blockSize ? initialCapacity + blockSize - initialCapacity % blockSize
                                       : initialCapacity;
}

#define VECSIM_TIMEOUT(ctx) (__builtin_expect(VecSimIndexInterface::timeoutCallback(ctx), false))

#ifdef __cplusplus
}
#endif
