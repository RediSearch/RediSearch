#pragma once

#include <stdlib.h>
#include <stdbool.h>

typedef struct BFIndex BFIndex;
typedef struct HNSWIndex HNSWIndex;
typedef struct VecField {
    size_t id;
    float dist;
} VecField;

#ifdef __cplusplus
extern "C" {
#endif

BFIndex *InitBFIndex(size_t max_elements, int d);

HNSWIndex *InitHNSWIndex(size_t max_elements, int d);

bool AddVectorToBFIndex(BFIndex *index, const void* vector_data, size_t id);

bool AddVectorToHNSWIndex(HNSWIndex *index, const void* vector_data, size_t id);

bool RemoveVectorFromBFIndex(BFIndex *index, size_t id);

bool RemoveVectorFromHNSWIndex(HNSWIndex *index, size_t id);

size_t GetBFIndexSize(BFIndex *index);

size_t GetHNSWIndexSize(HNSWIndex *index);

VecField *BFSearch(BFIndex *index, const void* query_data, size_t k);

VecField *HNSWSearch(HNSWIndex *index, const void* query_data, size_t k);

void SaveHNSWIndex(HNSWIndex *index, const char *path);

void LoadHNSWIndex(HNSWIndex *index, const char *path, size_t max_elements);

void RemoveBFIndex(BFIndex *index);

void RemoveHNSWIndex(HNSWIndex *index);

#ifdef __cplusplus
}
#endif