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

#include <string.h>

typedef struct VectorIndexStats {
  size_t memory;
  size_t marked_deleted;
  size_t direct_hnsw_insertions;  // Vectors inserted directly to HNSW (bypassing flat buffer)
  size_t flat_buffer_size;        // Current flat buffer size (tiered indexes only)
} VectorIndexStats;

typedef void (*VectorIndexStats_Setter)(VectorIndexStats*, size_t);
typedef size_t (*VectorIndexStats_Getter)(const VectorIndexStats*);

typedef struct {
    const char* name;
    VectorIndexStats_Setter setter;
} VectorIndexStats_SetterMapping;

typedef struct {
    const char* name;
    VectorIndexStats_Getter getter;
} VectorIndexStats_GetterMapping;

void VectorIndexStats_Agg(VectorIndexStats *first, const VectorIndexStats *second);
VectorIndexStats VectorIndexStats_Init();

VectorIndexStats_Setter VectorIndexStats_GetSetter(const char *name);
VectorIndexStats_Getter VectorIndexStats_GetGetter(const char *name);

//Metrics getters setters
size_t VectorIndexStats_GetMemory(const VectorIndexStats *stats);
size_t VectorIndexStats_GetMarkedDeleted(const VectorIndexStats *stats);
size_t VectorIndexStats_GetDirectHNSWInsertions(const VectorIndexStats *stats);
size_t VectorIndexStats_GetFlatBufferSize(const VectorIndexStats *stats);
void VectorIndexStats_SetMemory(VectorIndexStats *stats, size_t memory);
void VectorIndexStats_SetMarkedDeleted(VectorIndexStats *stats, size_t marked_deleted);
void VectorIndexStats_SetDirectHNSWInsertions(VectorIndexStats *stats, size_t direct_hnsw_insertions);
void VectorIndexStats_SetFlatBufferSize(VectorIndexStats *stats, size_t flat_buffer_size);

// metrics display strings:
static char* const VectorIndexStats_Metrics[] = {
    "memory",
    "marked_deleted",
    "direct_hnsw_insertions",
    "flat_buffer_size",
    NULL
};

#ifdef __cplusplus
    }
#endif
