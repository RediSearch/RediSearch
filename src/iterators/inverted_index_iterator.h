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

typedef struct InvIndIterator {
  QueryIterator base;

  const InvertedIndex *idx;

  // the underlying data buffer iterator
  IndexBlockReader blockReader;

  /* The decoding function for reading the index */
  IndexDecoderProcs decoders;
  /* The decoder's filtering context. It may be a number or a pointer. The number is used for
   * filtering field masks, the pointer for numeric filtering */
  IndexDecoderCtx decoderCtx;

  uint32_t currentBlock;

  /* This marker lets us know whether the garbage collector has visited this index while the reading
   * thread was asleep, and reset the state in a deeper way
   */
  uint32_t gcMarker;

  // Whether to skip multi values from the same doc
  bool skipMulti;

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

//TODO(Joan): Maybe we need another one for TagIndex with the TagIdx as member
typedef struct {
  InvIndIterator base;
  uint32_t revisionId;
} NumericInvIndIterator;

// API for full index scan. Not suitable for queries
QueryIterator *NewInvIndIterator_NumericFull(InvertedIndex *idx);
// API for full index scan. Not suitable for queries
QueryIterator *NewInvIndIterator_TermFull(InvertedIndex *idx);

// Returns an iterator for a numeric index, suitable for queries
QueryIterator *NewInvIndIterator_NumericQuery(InvertedIndex *idx, const RedisSearchCtx *sctx, const FieldFilterContext* fieldCtx,
                                              const NumericFilter *flt, double rangeMin, double rangeMax);

// Returns an iterator for a term index, suitable for queries
QueryIterator *NewInvIndIterator_TermQuery(InvertedIndex *idx, const RedisSearchCtx *sctx, FieldMaskOrIndex fieldMaskOrIndex,
                                           RSQueryTerm *term, double weight);

// Returns an iterator for a generic index, suitable for queries
// The returned iterator will yield "virtual" records. For term/numeric indexes, it is best to use
// the specific functions NewInvIndIterator_TermQuery/NewInvIndIterator_NumericQuery
QueryIterator *NewInvIndIterator_GenericQuery(InvertedIndex *idx, const RedisSearchCtx *sctx, t_fieldIndex fieldIndex,
                                              enum FieldExpirationPredicate predicate);

#ifdef __cplusplus
}
#endif
