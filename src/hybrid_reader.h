#pragma once

#include "index_iterator.h"
#include "vector_index.h"
#include "redisearch.h"
#include "spec.h"

IndexIterator *NewHybridVectorIterator(VecSimIndex *index, char *score_field, TopKVectorQuery query, VecSimQueryParams qParams, IndexIterator *child_it);
