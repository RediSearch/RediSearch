/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#pragma once

#include "spec.h"
#include "rs_wall_clock.h"

#define DIALECT_OFFSET(d) (1ULL << (d - MIN_DIALECT_VERSION))// offset of the d'th bit. begins at MIN_DIALECT_VERSION (bit 0) up to MAX_DIALECT_VERSION.
#define GET_DIALECT(barr, d) (!!(barr & DIALECT_OFFSET(d)))  // return the truth value of the d'th dialect in the dialect bitarray.
#define SET_DIALECT(barr, d) (barr |= DIALECT_OFFSET(d))     // set the d'th dialect in the dialect bitarray to true.

// Coord/Shard error or warning
#define COORD_ERR_WARN true
#define SHARD_ERR_WARN !COORD_ERR_WARN

typedef struct {
  size_t numTextFields;
  size_t numTextFieldsSortable;
  size_t numTextFieldsNoIndex;
  size_t numNumericFields;
  size_t numNumericFieldsSortable;
  size_t numNumericFieldsNoIndex;
  size_t numGeoFields;
  size_t numGeoFieldsSortable;
  size_t numGeoFieldsNoIndex;
  size_t numGeometryFields;
  size_t numGeometryFieldsSortable;
  size_t numGeometryFieldsNoIndex;
  size_t numTagFields;
  size_t numTagFieldsSortable;
  size_t numTagFieldsNoIndex;
  size_t numTagFieldsCaseSensitive;
  size_t numVectorFields;
  size_t numVectorFieldsFlat;
  size_t numVectorFieldsHNSW;
  size_t numVectorFieldsSvsVamana;
  size_t numVectorFieldsSvsVamanaCompressed;
} FieldsGlobalStats;

typedef struct {
  size_t syntax; // Number of syntax errors
  size_t arguments; // Number of parse arguments errors
} QueryErrorsGlobalStats;


typedef struct {
  size_t total_queries_processed;       // Number of successful queries. If using cursors, not counting reading from the cursor
  size_t total_query_commands;          // Number of successful query commands, including `FT.CURSOR READ`
  rs_wall_clock_ns_t total_query_execution_time;   // Total time spent on queries, aggregated in ns and reported in ms

  QueryErrorsGlobalStats shard_errors;        // Shard query errors statistics
  QueryErrorsGlobalStats coord_errors;  // Coordinator query errors statistics
} QueriesGlobalStats;

typedef struct {
  QueriesGlobalStats queries;   // Queries statistics. values should be fetched by calling `TotalGlobalStats_GetQueryStats`, otherwise not safe.
  uint_least8_t used_dialects;  // bitarray of dialects used by all indices
  size_t logically_deleted;     // Number of logically deleted documents in all indices
                                // (i.e., marked with DELETED flag but their memory was not yet cleaned by the GC)
} TotalGlobalStats;

// The global stats object type
typedef struct {
  FieldsGlobalStats fieldsStats;
  TotalGlobalStats totalStats;
} GlobalStats;

extern GlobalStats RSGlobalStats;

/**
 * Check the type of the the given field and update RSGlobalConfig.fieldsStats
 * according to the given toAdd value.
 */
void FieldsGlobalStats_UpdateStats(FieldSpec *fs, int toAdd);

/**
 * Add or increase `toAdd` number of errors to the global index errors counter of field_type.
 * `toAdd` can be negative to decrease the counter.
 */
void FieldsGlobalStats_UpdateIndexError(FieldType field_type, int toAdd);

/**
 * Get the total count of index errors caused by field_type.
 * Assuming the GIL is locked.
 */
size_t FieldsGlobalStats_GetIndexErrorCount(FieldType field_type);

/**
 * Increase all relevant counters in the global stats object.
 * Note that duration is aggregated in nanoseconds but later converted to milliseconds.
 */
void TotalGlobalStats_CountQuery(uint32_t reqflags, rs_wall_clock_ns_t duration);

/**
 * Safely reads and returns a copy of the global queries stats.
 */
QueriesGlobalStats TotalGlobalStats_GetQueryStats();

/**
 * Increase the number of logically deleted documents in all indices by `toAdd`.
 */
void IndexsGlobalStats_UpdateLogicallyDeleted(int64_t toAdd);

/**
 * Get the number of logically deleted documents in all indices.
 */
size_t IndexesGlobalStats_GetLogicallyDeletedDocs();

/**
* Updates the global query errors statistics.
* `coord` indicates whether the error occurred on the coordinator or on a shard.
* Standalone shards are considered as shards.
* Will ignore not supported error codes.
* Currently supports : syntax, parse_args
* `toAdd` can be negative to decrease the counter.
*/
void QueryErrorsGlobalStats_UpdateError(QueryErrorCode error, int toAdd, bool coord);
