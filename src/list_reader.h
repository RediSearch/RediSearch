
#pragma once

#include "index_iterator.h"
#include "redisearch.h"
#include "spec.h"

IndexIterator *NewListIterator(void *list, size_t len);

IndexIterator *NewHybridVectorIteratorImpl(VecSimBatchIterator *batch_it, size_t vec_index_size, size_t k, IndexIterator *child_it);