#ifndef __INDEX_ITERATOR_H__
#define __INDEX_ITERATOR_H__

#include <stdint.h>
#include "redisearch.h"
#include "index_result.h"

#define INDEXREAD_EOF 0
#define INDEXREAD_OK 1
#define INDEXREAD_NOTFOUND 2

/* An abstract interface used by readers / intersectors / unioners etc.
Basically query execution creates a tree of iterators that activate each other
recursively */
typedef struct indexIterator {
  void *ctx;

  RSIndexResult *(*Current)(void *ctx);

  /* Read the next entry from the iterator, into hit *e.
  *  Returns INDEXREAD_EOF if at the end */
  int (*Read)(void *ctx, RSIndexResult **e);

  /* Skip to a docid, potentially reading the entry into hit, if the docId
   * matches */
  int (*SkipTo)(void *ctx, t_docId docId, RSIndexResult **hit);

  /* the last docId read */
  t_docId (*LastDocId)(void *ctx);

  /* can we continue iteration? */
  int (*HasNext)(void *ctx);

  /* release the iterator's context and free everything needed */
  void (*Free)(struct indexIterator *self);

  /* Return the number of results in this iterator. Used by the query execution
   * on the top iterator */
  size_t (*Len)(void *ctx);

  /* Abort the execution of the iterator and mark it as EOF. This is used for early aborting in case
   * of data consistency issues due to multi threading */
  void (*Abort)(void *ctx);

  /* Rewinde the iterator to the beginning and reset its state */
  void (*Rewind)(void *ctx);
} IndexIterator;

#endif