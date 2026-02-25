/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "vector_index_stats.h"

static VectorIndexStats_SetterMapping VectorIndexStats_SetterMappingsContainer[] = {
    {"memory", VectorIndexStats_SetMemory},
    {"marked_deleted", VectorIndexStats_SetMarkedDeleted},
    {"direct_hnsw_insertions", VectorIndexStats_SetDirectHNSWInsertions},
    {"flat_buffer_size", VectorIndexStats_SetFlatBufferSize},
    {NULL, NULL} // Sentinel value to mark the end of the array
};

static VectorIndexStats_GetterMapping VectorIndexStats_GetterMappingContainer[] = {
    {"memory", VectorIndexStats_GetMemory},
    {"marked_deleted", VectorIndexStats_GetMarkedDeleted},
    {"direct_hnsw_insertions", VectorIndexStats_GetDirectHNSWInsertions},
    {"flat_buffer_size", VectorIndexStats_GetFlatBufferSize},
    {NULL, NULL} // Sentinel value to mark the end of the array
};

VectorIndexStats VectorIndexStats_Init() {
    VectorIndexStats stats = {0};
    return stats;
}

VectorIndexStats_Setter VectorIndexStats_GetSetter(const char* name) {
    for (int i = 0; VectorIndexStats_SetterMappingsContainer[i].name != NULL; ++i) {
        if (strcmp(VectorIndexStats_SetterMappingsContainer[i].name, name) == 0) {
            return VectorIndexStats_SetterMappingsContainer[i].setter;
        }
    }
    return NULL;
}

VectorIndexStats_Getter VectorIndexStats_GetGetter(const char* name){
    for (int i = 0; VectorIndexStats_GetterMappingContainer[i].name != NULL; ++i) {
        if (strcmp(VectorIndexStats_GetterMappingContainer[i].name, name) == 0) {
            return VectorIndexStats_GetterMappingContainer[i].getter;
        }
    }
    return NULL;
}

void VectorIndexStats_Agg(VectorIndexStats *first, const VectorIndexStats *second) {
    first->memory += second->memory;
    first->marked_deleted += second->marked_deleted;
    first->direct_hnsw_insertions += second->direct_hnsw_insertions;
    first->flat_buffer_size += second->flat_buffer_size;
}

size_t VectorIndexStats_GetMemory(const VectorIndexStats *stats){
    return stats->memory;
}
size_t VectorIndexStats_GetMarkedDeleted(const VectorIndexStats *stats){
    return stats->marked_deleted;
}
size_t VectorIndexStats_GetDirectHNSWInsertions(const VectorIndexStats *stats){
    return stats->direct_hnsw_insertions;
}
size_t VectorIndexStats_GetFlatBufferSize(const VectorIndexStats *stats){
    return stats->flat_buffer_size;
}

void VectorIndexStats_SetMemory(VectorIndexStats *stats, size_t memory) {
    stats->memory = memory;
}

void VectorIndexStats_SetMarkedDeleted(VectorIndexStats *stats, size_t marked_deleted) {
    stats->marked_deleted = marked_deleted;
}

void VectorIndexStats_SetDirectHNSWInsertions(VectorIndexStats *stats, size_t direct_hnsw_insertions) {
    stats->direct_hnsw_insertions = direct_hnsw_insertions;
}

void VectorIndexStats_SetFlatBufferSize(VectorIndexStats *stats, size_t flat_buffer_size) {
    stats->flat_buffer_size = flat_buffer_size;
}
