
#pragma once

#include "index_iterator.h"
#include "redisearch.h"
#include "spec.h"

IndexIterator *NewListIterator(void *list, size_t len);
