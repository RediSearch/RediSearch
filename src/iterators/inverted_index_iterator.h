/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#pragma once

#include "iterator_api.h"
#include "inverted_index.h"

typedef struct InvIndIterator {
  QueryIterator base;

  const InvertedIndex *idx;

  // the underlying data buffer
  BufferReader br;

  // last docId, used for delta encoding/decoding (different from LastDocId in base)
  t_docId lastId;
  // same docId, used for detecting same doc (with multi values)
  t_docId sameId;

  /* The decoding function for reading the index */
  IndexDecoderProcs decoders;
  /* The decoder's filtering context. It may be a number or a pointer. The number is used for
   * filtering field masks, the pointer for numeric filtering */
  IndexDecoderCtx decoderCtx;

  // Whether to skip multi values from the same doc
  bool skipMulti;
  uint32_t currentBlock;

  /* This marker lets us know whether the garbage collector has visited this index while the reading
   * thread was asleep, and reset the state in a deeper way
   */
  uint32_t gcMarker;

  union {
    struct {
      double rangeMin;
      double rangeMax;
    } numeric;
  } profileCtx;

  const RedisSearchCtx *sctx;

  FieldFilterContext filterCtx;
} InvIndIterator;

QueryIterator *NewInvIndIterator(); // TODO: API?
