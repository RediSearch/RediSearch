/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#pragma once

#include "iterator_api.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
  QueryIterator base;
  t_docId *docIds;
  t_offset size;
  t_offset offset;
} IdListIterator;

/**
 * @param ids - the list of doc ids to iterate over
 * @param num - the number of doc ids in the list
 * @param weight - the weight of the node (assigned to the returned result)
 */
QueryIterator *IT_V2(NewIdListIterator)(t_docId *ids, t_offset num, double weight);

#ifdef __cplusplus
}
#endif
