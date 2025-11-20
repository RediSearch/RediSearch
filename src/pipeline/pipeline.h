#pragma once

#include "aggregate/aggregate_plan.h"
#include "query.h"

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Common parameters shared across different pipeline types in RediSearch.
 * This struct contains the core components needed by all pipeline operations,
 * whether they're for indexing, aggregation, or search queries.
 */
typedef struct CommonPipelineParams {
  /** Redis search context containing index spec and Redis module context.
   *  This context is owned by the request and provides access to the index
   *  configuration, field definitions, and Redis module APIs. */
  RedisSearchCtx *sctx;

  /** Bitfield flags controlling query execution behavior and output format.
   *  Includes flags like QEXEC_F_IS_SEARCH, QEXEC_F_SEND_SCORES, QEXEC_F_PROFILE, etc.
   *  These flags determine how results are processed, formatted, and returned. */
  uint32_t reqflags;

  /** Query optimizer instance that holds optimization parameters and state.
   *  Used to apply various query optimizations like iterator reordering,
   *  early termination, and scoring optimizations. */
  struct QOptimizer *optimizer;

  /** Name to use as the score alias, used by both scorer and sorter. */
  const char* scoreAlias;
} CommonPipelineParams;

/**
 * Parameters specific to result processing and output formatting pipeline construction.
 * This struct extends CommonPipelineParams with additional configuration needed for
 * processing search results through operations like filtering, sorting, grouping,
 * field loading, and output formatting. Used by both FT.SEARCH and FT.AGGREGATE
 * commands when building the result processing part of the query pipeline.
 */
typedef struct AggregationPipelineParams {
  /** Common pipeline parameters shared with other pipeline types */
  CommonPipelineParams common;

  /** List of fields to be included in the output and processed by result processors.
   *  This determines which document fields are loaded, transformed, and returned
   *  to the client. Used by RETURN, LOAD, and other field-specific operations. */
  FieldList *outFields;

  /** Maximum number of results that can be returned by this aggregation.
   *  This limit is enforced at various stages of the pipeline to prevent
   *  memory exhaustion and ensure reasonable response times. Takes precedence
   *  over individual step limits when smaller. */
  size_t maxResultsLimit;

  /** Language setting for text highlighting and language-specific processing.
   *  Used by highlighting result processors to apply proper stemming,
   *  tokenization, and markup for the specified language. */
  RSLanguage language;
} AggregationPipelineParams;


/**
 * Parameters specific to the document retrieval and scoring pipeline construction.
 * This struct extends CommonPipelineParams with components needed for the initial
 * phase of query execution, where the query is executed against the index to find
 * matching documents and calculate their relevance scores. This is the "search" part
 * that happens before aggregation, filtering, and result formatting.
 */
typedef struct QueryPipelineParams {
    /** Common pipeline parameters shared with other pipeline types */
    CommonPipelineParams common;

    /** Abstract syntax tree representing the parsed search query structure.
     *  Contains the logical query tree with search terms, boolean operators (AND/OR),
     *  and filters that will be used to create the iterator hierarchy for finding
     *  matching documents in the index. */
    const QueryAST *ast;

    /** Root iterator that searches through the index to find matching documents.
     *  This is the top-level iterator in the search iterator tree, typically a union
     *  or intersection iterator that coordinates child iterators for different
     *  search terms and filters. It produces the initial set of candidate documents. */
    const QueryIterator *rootiter;

    /** Slot ranges for the root iterator, used for cluster-aware query execution. */
    const SharedSlotRangeArray *slotRanges;
    const RedisModuleSlotRangeArray *querySlots;
    uint32_t slotsVersion;

    /** Name of the scoring function to use for document relevance calculation.
     *  Examples include "BM25", "TFIDF", or custom scorer names. This determines
     *  how documents are ranked by relevance. If NULL, the default scorer is used. */
    const char *scorerName;

    /** Request configuration containing timeout policies and execution settings.
     *  Determines how the search query behaves under timeout conditions and other
     *  execution constraints like memory limits. */
    RequestConfig *reqConfig;
} QueryPipelineParams;


/**
 * Parameters specific to hybrid search pipeline construction.
 * This struct extends the pipeline parameter system to support hybrid search operations
 * that combine multiple search requests (e.g., vector + text search) and merge their
 * results using sophisticated scoring algorithms. Used by HybridRequest_BuildPipeline.
 */
typedef struct HybridPipelineParams {
    /** Aggregation pipeline parameters for result processing and output formatting.
     *  Contains all the standard parameters needed for processing search results,
     *  including field loading, sorting, filtering, and output formatting that
     *  will be applied to the merged hybrid search results. */
    AggregationPipelineParams aggregationParams;

    /** Hybrid scoring context containing algorithms and parameters for result merging.
     *  This context defines how results from different search modalities (vector, text, etc.)
     *  are combined and scored. The pipeline takes ownership of this pointer and will
     *  free it during cleanup. Can be NULL for default scoring behavior. */
    HybridScoringContext *scoringCtx;
} HybridPipelineParams;

/**
 * Main query pipeline structure that orchestrates the entire query execution process.
 * This struct represents a complete query execution pipeline, combining both the
 * logical plan (what operations to perform) and the execution context (how to
 * perform them). It serves as the central coordination point for all query processing.
 */
typedef struct Pipeline {
  /** Aggregation plan containing the logical sequence of processing steps.
   *  This plan defines the operations to be performed (filtering, sorting,
   *  grouping, etc.) and their order. It's built from the parsed query and
   *  serves as the blueprint for result processor creation. */
  AGGPlan ap;

  /** Query processing context that manages the execution state and result processors.
   *  Contains the chain of result processors, error handling, timeout management,
   *  and execution statistics. This is where the actual query execution happens,
   *  with data flowing through the processor chain defined by the aggregation plan. */
  QueryProcessingCtx qctx;

} Pipeline;

/**
 * Initialize a query pipeline with the specified timeout policy and error handling.
 * This function sets up the basic pipeline structure, initializes the query processing
 * context, and prepares the pipeline for step addition and execution.
 *
 * @param pipeline The pipeline structure to initialize
 * @param timeoutPolicy Policy for handling query timeouts (fail vs. return partial results)
 * @param status Error status object for reporting initialization failures
 */
void Pipeline_Initialize(Pipeline *pipeline, RSTimeoutPolicy timeoutPolicy, QueryError *status);

/**
 * Clean up and free all resources associated with a query pipeline.
 * This function releases the result processor chain, frees all aggregation plan steps.
 * Should be called when the pipeline is no longer needed.
 *
 * @param pipeline The pipeline to clean up
 */
void Pipeline_Clean(Pipeline *pipeline);

#ifdef __cplusplus
}
#endif
