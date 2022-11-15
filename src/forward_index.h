/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#ifndef __FORWARD_INDEX_H__
#define __FORWARD_INDEX_H__
#include "redisearch.h"
#include "util/block_alloc.h"
#include "util/khtable.h"
#include "util/mempool.h"
#include "triemap/triemap.h"
#include "varint.h"
#include "tokenize.h"
#include "document.h"

typedef struct ForwardIndexEntry {
  struct ForwardIndexEntry *next;
  t_docId docId;

  uint32_t freq;
  t_fieldMask fieldMask;

  const char *term;
  uint32_t len;
  uint32_t hash;
  VarintVectorWriter *vw;
} ForwardIndexEntry;

// the quantizationn factor used to encode normalized (0..1) frquencies in the index
#define FREQ_QUANTIZE_FACTOR 0xFFFF

typedef struct ForwardIndex {
  KHTable *hits;
  uint32_t maxFreq;
  uint32_t totalFreq;
  uint32_t idxFlags;
  Stemmer *stemmer;
  SynonymMap *smap;
  BlkAlloc terms;
  BlkAlloc entries;
  mempool_t *vvwPool;

} ForwardIndex;

typedef struct {
  const char *doc;
  VarintVectorWriter *allOffsets;
  ForwardIndex *idx;
  t_fieldId fieldId;
  float fieldScore;
} ForwardIndexTokenizerCtx;

static inline void ForwardIndexTokenizerCtx_Init(ForwardIndexTokenizerCtx *ctx, ForwardIndex *idx,
                                                 const char *doc, VarintVectorWriter *vvw,
                                                 t_fieldId fieldId, float score) {
  ctx->idx = idx;
  ctx->fieldId = fieldId;
  ctx->fieldScore = score;
  ctx->doc = doc;
  ctx->allOffsets = vvw;
}

typedef struct {
  KHTable *hits;
  KHTableEntry *curEnt;
  uint32_t curBucketIdx;
} ForwardIndexIterator;

int forwardIndexTokenFunc(void *ctx, const Token *tokInfo);
void ForwardIndexFree(ForwardIndex *idx);

void ForwardIndex_Reset(ForwardIndex *idx, Document *doc, uint32_t idxFlags);

ForwardIndex *NewForwardIndex(Document *doc, uint32_t idxFlags);
ForwardIndexIterator ForwardIndex_Iterate(ForwardIndex *i);
ForwardIndexEntry *ForwardIndexIterator_Next(ForwardIndexIterator *iter);

// Find an existing entry within the index
ForwardIndexEntry *ForwardIndex_Find(ForwardIndex *i, const char *s, size_t n, uint32_t hash);

void ForwardIndex_NormalizeFreq(ForwardIndex *, ForwardIndexEntry *);

#endif
