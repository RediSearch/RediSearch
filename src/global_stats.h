/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#pragma once

#include "spec.h"

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
  size_t total_unique_queries;  // Number of unique queries, not counting `FT.CURSOR READ` (an iteration of a previous query)
  size_t total_query_commands;  // Number of total query commands, including `FT.CURSOR READ`
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
 * Exposing all the fields that > 0 to INFO command.
 */
void FieldsGlobalStats_AddToInfo(RedisModuleInfoCtx *ctx);

/**
 * Increase all relevant counters in the global stats object.
 */
void TotalGlobalStats_CountQuery(uint32_t reqflags);
