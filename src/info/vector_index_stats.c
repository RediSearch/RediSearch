
#include "vector_index_stats.h"

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
}

size_t VectorIndexStats_GetMemory(const VectorIndexStats *stats){
    return stats->memory;
}
size_t VectorIndexStats_GetMarkedDeleted(const VectorIndexStats *stats){
    return stats->marked_deleted;
}

void VectorIndexStats_SetMemory(VectorIndexStats *stats, size_t memory) {
    stats->memory = memory;
}

void VectorIndexStats_SetMarkedDeleted(VectorIndexStats *stats, size_t marked_deleted) {
    stats->marked_deleted = marked_deleted;
}