/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#pragma once

#include "spec.h"

#define DIALECT_OFFSET(d) (1ULL << (d - MIN_DIALECT_VERSION))// offset of the d'th bit. begins at MIN_DIALECT_VERSION (bit 0) up to MAX_DIALECT_VERSION.
#define GET_DIALECT(barr, d) (!!(barr & DIALECT_OFFSET(d)))  // return the truth value of the d'th dialect in the dialect bitarray.
#define SET_DIALECT(barr, d) (barr |= DIALECT_OFFSET(d))     // set the d'th dialect in the dialect bitarray to true.

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
} FieldsGlobalStats;

typedef struct {
  size_t total_queries_processed;     // Number of successful queries. If using cursors, not counting reading from the cursor
  size_t total_query_commands;        // Number of successful query commands, including `FT.CURSOR READ`
  clock_t total_query_execution_time; // Total time spent on queries (in clock ticks)
  uint_least8_t used_dialects;        // bitarray of dialects used by all indices
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
 * Exposing stats on all the field's type with existing field count > 0 to INFO command.
 */
void FieldsGlobalStats_AddToInfo(RedisModuleInfoCtx *ctx);

/**
 * Increase all relevant counters in the global stats object.
 */
void TotalGlobalStats_CountQuery(uint32_t reqflags, clock_t duration);

/**
 * Add all the query-related information to the INFO command.
 */
void TotalGlobalStats_Queries_AddToInfo(RedisModuleInfoCtx *ctx);

/**
 * Add all the dialect-related information to the INFO command.
 */
void DialectsGlobalStats_AddToInfo(RedisModuleInfoCtx *ctx);
