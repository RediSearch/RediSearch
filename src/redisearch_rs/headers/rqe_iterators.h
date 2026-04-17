#pragma once

#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

/**
 * The different types of metrics.
 * At the moment, only vector distance is supported.
 * cbindgen:rename-all=ScreamingSnakeCase
 */
typedef enum MetricType {
  VECTOR_DISTANCE,
} MetricType;

/**
 * Profile counters collected during query execution.
 *
 * This struct is `#[repr(C)]` so that C code can access its fields directly.
 */
typedef struct ProfileCounters ProfileCounters;
