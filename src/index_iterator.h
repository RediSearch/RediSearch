/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
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

#ifndef __ITERATOR_API_H__
enum IteratorType {
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
#endif

/* An abstract interface used by readers / intersectors / unioners etc.
Basically query execution creates a tree of iterators that activate each other
recursively */
typedef struct IndexIterator {
  enum iteratorType type;

  // Can the iterator yield more results?
  bool isValid;

  // Mark the iterator as aborted (permanent EOF, even if rewound)
  bool isAborted;

  /* the last docId read */
  t_docId LastDocId;

  // Cached value - used if Current() is not set
  RSIndexResult *current;


  // Used if the iterator yields some value.
  // Consider placing in a union with an array of keys, if a field want to yield multiple metrics
  struct RLookupKey *ownKey;

  size_t (*NumEstimated)(struct IndexIterator *self);

  /* Read the next entry from the iterator, into hit *e.
   *  Returns INDEXREAD_EOF if at the end */
  int (*Read)(struct IndexIterator *self, RSIndexResult **e);

  /* Skip to a docid, potentially reading the entry into hit, if the docId
   * matches */
  int (*SkipTo)(struct IndexIterator *self, t_docId docId, RSIndexResult **hit);

  /* release the iterator's context and free everything needed */
  void (*Free)(struct IndexIterator *self);

  /* Rewind the iterator to the beginning and reset its state */
  void (*Rewind)(struct IndexIterator *self);
} IndexIterator;

#define IITER_HAS_NEXT(ii) ((ii)->isValid)
#define IITER_SET_EOF(ii) ((ii)->isValid = false)
#define IITER_CLEAR_EOF(ii) ((ii)->isValid = true)

static inline void IndexIterator_Abort(IndexIterator *it) {
  it->isValid = false;
  it->isAborted = true;
}

#endif
