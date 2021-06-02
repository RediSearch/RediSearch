#include "index_iterator.h"
#include "redisearch.h"
#include "spec.h"

/* Create a new ListIterator */
IndexIterator *NewListIterator(void *list, size_t len);
