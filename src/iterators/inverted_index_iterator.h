/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#pragma once

#ifdef __cplusplus
extern "C" {
#endif

#include "iterator_api.h"
#include "inverted_index.h"
#include "tag_index.h"

typedef struct InvIndIterator {
  QueryIterator base;

  IndexReader *reader;
  const RedisSearchCtx *sctx;

  // The context for the field/s filter, used to determine if the field/s is/are expired
  FieldFilterContext filterCtx;

  ValidateStatus (*CheckAbort)(struct InvIndIterator *self);

} InvIndIterator;

typedef struct {
  InvIndIterator base;
  const NumericRangeTree *rt;
  uint32_t revisionId;
  double rangeMin;
  double rangeMax;
} NumericInvIndIterator;

typedef struct {
  InvIndIterator base;
  const TagIndex *tagIdx; // not const, may reopen on revalidation
} TagInvIndIterator;

// Returns an iterator for a tag index, suitable for queries
QueryIterator *NewInvIndIterator_TagQuery(const InvertedIndex *idx, const TagIndex *tagIdx, const RedisSearchCtx *sctx, FieldMaskOrIndex fieldMaskOrIndex,
                                          RSQueryTerm *term, double weight);

#ifdef __cplusplus
}
#endif
