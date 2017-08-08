#ifndef __FORWARD_INDEX_H__
#define __FORWARD_INDEX_H__
#include "redisearch.h"
#include "util/block_alloc.h"
#include "dep/triemap/triemap.h"
#include "varint.h"
#include "tokenize.h"
#include "document.h"

struct fwIdxMemBlock;
struct khTable;

typedef struct {
  t_docId docId;
  const char *term;
  uint32_t len;
  uint32_t indexerState;
  uint32_t freq;
  float docScore;
  t_fieldMask fieldMask;
  VarintVectorWriter *vw;
} ForwardIndexEntry;

// the quantizationn factor used to encode normalized (0..1) frquencies in the index
#define FREQ_QUANTIZE_FACTOR 0xFFFF

typedef struct ForwardIndex {
  struct khTable *hits;
  t_docId docId;
  uint32_t totalFreq;
  uint32_t maxFreq;
  uint32_t idxFlags;
  float docScore;
  int uniqueTokens;
  Stemmer *stemmer;
  BlkAlloc terms;
} ForwardIndex;

struct bucketEntry;
typedef struct {
  struct khTable *hits;
  struct bucketEntry *curEnt;
  uint32_t curBucketIdx;
} ForwardIndexIterator;

int forwardIndexTokenFunc(void *ctx, const Token *t);

void ForwardIndexFree(ForwardIndex *idx);
ForwardIndex *NewForwardIndex(Document *doc, uint32_t idxFlags);
ForwardIndexIterator ForwardIndex_Iterate(ForwardIndex *i);
ForwardIndexEntry *ForwardIndexIterator_Next(ForwardIndexIterator *iter);

// Find an existing entry within the index
ForwardIndexEntry *ForwardIndex_Find(ForwardIndex *i, const char *s, size_t n);

void ForwardIndex_NormalizeFreq(ForwardIndex *, ForwardIndexEntry *);

#endif
