/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#ifndef __INDEX_ITERATOR_H__
#define __INDEX_ITERATOR_H__

#include <stdint.h>
#include "redisearch.h"
#include "index_result.h"

struct RLookupKey; // Forward declaration

#define INDEXREAD_EOF 0
#define INDEXREAD_OK 1
#define INDEXREAD_NOTFOUND 2
#define INDEXREAD_TIMEOUT 3

enum iteratorType {
  READ_ITERATOR,
  HYBRID_ITERATOR,
  UNION_ITERATOR,
  INTERSECT_ITERATOR,
  NOT_ITERATOR,
  OPTIONAL_ITERATOR,
  WILDCARD_ITERATOR,
  EMPTY_ITERATOR,
  ID_LIST_ITERATOR,
  METRIC_ITERATOR,
  PROFILE_ITERATOR,
  OPTIMUS_ITERATOR,
  MAX_ITERATOR,
};

/* An abstract interface used by readers / intersectors / unioners etc.
Basically query execution creates a tree of iterators that activate each other
recursively */
typedef struct indexIterator {
  // Cached value - used if HasNext() is not set.
  uint8_t isValid;

  void *ctx;

  // Used by union iterator. Cached here for performance
  t_docId minId;

  // Cached value - used if Current() is not set
  RSIndexResult *current;

  enum iteratorType type;

  // Used if the iterator yields some value.
  // Consider placing in a union with an array of keys, if a field want to yield multiple metrics
  struct RLookupKey *ownKey;

  size_t (*NumEstimated)(void *ctx);

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

  /* Rewind the iterator to the beginning and reset its state */
  void (*Rewind)(void *ctx);
} IndexIterator;

// static inline int IITER_HAS_NEXT(IndexIterator *ii) {
//   /**
//    * Assume that this is false, in which case, we just need to perform a single
//    * comparison
//    */
//   if (ii->isValid) {
//     return 1;
//   }

//   if (ii->HasNext) {
//     return ii->HasNext(ii->ctx);
//   } else {
//     return 0;
//   }
// }
#define IITER_HAS_NEXT(ii) ((ii)->isValid ? 1 : (ii)->HasNext ? (ii)->HasNext((ii)->ctx) : 0)
#define IITER_CURRENT_RECORD(ii) ((ii)->current ? (ii)->current : 0)
#define IITER_NUM_ESTIMATED(ii) ((ii)->NumEstimated ? (ii)->NumEstimated((ii)->ctx) : 0)

#define IITER_SET_EOF(ii) (ii)->isValid = 0
#define IITER_CLEAR_EOF(ii) (ii)->isValid = 1

#define IITER_INVALID_NUM_ESTIMATED_RESULTS UINT32_MAX

// #define IITER_HAS_NEXT(ii) ((ii)->HasNext ? (ii)->HasNext((ii)->ctx) : (!(ii)->atEnd))

#endif
