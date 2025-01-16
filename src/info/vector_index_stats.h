// #ifndef VECTOR_INDEX_STATS_H
// #define VECTOR_INDEX_STATS_H
// #include <stddef.h>
#include <string.h>


typedef struct {
  size_t memory;
  size_t marked_deleted;
} VectorIndexStats;

typedef void (*VectorIndexStats_Setter)(VectorIndexStats*, size_t);

typedef struct {
    const char* name;
    VectorIndexStats_Setter setter;
} VectorIndexStats_SetterMapping;

void VectorIndexStats_Agg(VectorIndexStats *first, const VectorIndexStats *second);
VectorIndexStats VectorIndexStats_Init();

VectorIndexStats_Setter VectorIndexStats_GetSetter(const char* name);
//Metrics setters
void VectorIndexStats_SetMemory(VectorIndexStats *stats, size_t memory);
void VectorIndexStats_SetMarkedDeleted(VectorIndexStats *stats, size_t marked_deleted);

// metrics display strings:
static char* const VectorIndexStats_Metrics[] = {
    "memory",
    "marked_deleted",
    NULL
};

static VectorIndexStats_SetterMapping VectorIndexStats_SetterMappingsContainer[] = {
    {"memory", VectorIndexStats_SetMemory},
    {"marked_deleted", VectorIndexStats_SetMarkedDeleted},
    {NULL, NULL} // Sentinel value to mark the end of the array
};
// #endif
