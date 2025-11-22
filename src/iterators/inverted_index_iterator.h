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

  // Whether to skip multi values from the same doc
  // Stores the original requested value, even if skipping multi values is not needed (based on other information),
  // so that we can re-choose the right implementation of the iterator API implementation
  bool skipMulti;

  // Whether this iterator is result of a wildcard query
  bool isWildcard;

  union {
    struct {
      double rangeMin;
      double rangeMax;
    } numeric;
  } profileCtx;

  const RedisSearchCtx *sctx;

  // The context for the field/s filter, used to determine if the field/s is/are expired
  FieldFilterContext filterCtx;

  ValidateStatus (*CheckAbort)(struct InvIndIterator *self);

} InvIndIterator;

typedef struct {
  InvIndIterator base;
  const NumericRangeTree *rt;
  uint32_t revisionId;
} NumericInvIndIterator;

typedef struct {
  InvIndIterator base;
  const TagIndex *tagIdx; // not const, may reopen on revalidation
} TagInvIndIterator;

// API for full index scan. Not suitable for queries
QueryIterator *NewInvIndIterator_NumericFull(const InvertedIndex *idx);
// API for full index scan. Not suitable for queries
QueryIterator *NewInvIndIterator_TermFull(const InvertedIndex *idx);

// Returns an iterator for a numeric index, suitable for queries
QueryIterator *NewInvIndIterator_NumericQuery(const InvertedIndex *idx, const RedisSearchCtx *sctx, const FieldFilterContext* fieldCtx,
                                              const NumericFilter *flt, const NumericRangeTree *rt, double rangeMin, double rangeMax);

// Returns an iterator for a term index, suitable for queries
QueryIterator *NewInvIndIterator_TermQuery(const InvertedIndex *idx, const RedisSearchCtx *sctx, FieldMaskOrIndex fieldMaskOrIndex,
                                           RSQueryTerm *term, double weight);

// Returns an iterator for a wildcard index (optimized for wildcard queries) - mainly to revalidate the index
QueryIterator *NewInvIndIterator_WildcardQuery(const InvertedIndex *idx, const RedisSearchCtx *sctx, double weight);

// Returns an iterator for a missing index - revalidate the missing index was not deleted
// Result is a virtual result with a weight of 0.0, and a field mask of RS_FIELDMASK_ALL
QueryIterator *NewInvIndIterator_MissingQuery(const InvertedIndex *idx, const RedisSearchCtx *sctx, t_fieldIndex fieldIndex);

// API for full index scan with TagIndex. Not suitable for queries
QueryIterator *NewInvIndIterator_TagFull(const InvertedIndex *idx, const TagIndex *tagIdx);

// Returns an iterator for a tag index, suitable for queries
QueryIterator *NewInvIndIterator_TagQuery(const InvertedIndex *idx, const TagIndex *tagIdx, const RedisSearchCtx *sctx, FieldMaskOrIndex fieldMaskOrIndex,
                                          RSQueryTerm *term, double weight);

#ifdef __cplusplus
}
#endif
