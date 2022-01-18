#pragma once

#include "index_iterator.h"
#include "vector_index.h"
#include "redisearch.h"
#include "spec.h"

IndexIterator *NewHybridVectorIteratorImpl(VecSimIndex *index, TopKVectorQuery query, IndexIterator *child_it);
