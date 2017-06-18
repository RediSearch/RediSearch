#ifndef __ID_FILTER_H__
#define __ID_FILTER_H__

#include "redismodule.h"
#include "index_iterator.h"
#include "doc_table.h"

/* An IdFilter is a generic filter that limits the results of a query to a given set of ids. It is
 * created from a list of keys in the index */
typedef struct idFilter {
  t_docId *ids;
  RedisModuleString **keys;
  t_offset size;
} IdFilter;

/* Create a new IdFilter from a list of redis strings. count is the number of strings, guaranteed to
 * be less than or equal to the length of args */
IdFilter *NewIdFilter(RedisModuleString **args, int count, DocTable *dt);

/* Free the filter's internal data, but not the filter itself, that is allocated on the stack */
void IdFilter_Free(IdFilter *f);

/** Return a new id filter iterator from a filter. If no ids are in the filter, we return NULL */
IndexIterator *NewIdFilterIterator(IdFilter *f);
#endif
