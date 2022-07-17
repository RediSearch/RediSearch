
#pragma once

#include "redisearch.h"
#include "util/block_alloc.h"
#include "util/khtable.h"
#include "util/mempool.h"
#include "triemap/triemap.h"
#include "varint.h"
#include "tokenize.h"
#include "document.h"

///////////////////////////////////////////////////////////////////////////////////////////////

struct ForwardIndexEntry : Object {
  ForwardIndexEntry();
  ~ForwardIndexEntry();

  struct ForwardIndexEntry *next;
  t_docId docId;

  uint32_t freq;
  t_fieldMask fieldMask;

  const char *term;
  uint32_t len;
  uint32_t hash;
  VarintVectorWriter *vw;
};

//---------------------------------------------------------------------------------------------

struct khIdxEntry {
  KHTableEntry khBase;
  ForwardIndexEntry ent;
};

//---------------------------------------------------------------------------------------------

struct ForwardIndexIterator {
  KHTable *hits;
  KHTableEntry *curEnt;
  uint32_t curBucketIdx;

  ForwardIndexEntry *Next();
};

// the quantizationn factor used to encode normalized (0..1) frquencies in the index
#define FREQ_QUANTIZE_FACTOR 0xFFFF

struct ForwardIndex : Object {
  //KHTable *hits;
  UnorderedMap<std::string, ForwardIndexEntry> hits;
  uint32_t maxFreq;
  uint32_t totalFreq;
  uint32_t idxFlags;
  Stemmer *stemmer;
  SynonymMap *smap;
  BlkAlloc terms;
  BlkAlloc entries;
  MemPool vvwPool;

  ForwardIndex(Document *doc, uint32_t idxFlags_);
  ~ForwardIndex();

  void Reset(Document *doc, uint32_t idxFlags_);

  ForwardIndexIterator Iterate();

  ForwardIndexEntry *Find(const char *s, size_t n, uint32_t hash);

  void NormalizeFreq(ForwardIndexEntry *); //@@ nobody is using this func

  void HandleToken(const char *tok, size_t tokLen, uint32_t pos,
                   float fieldScore, t_fieldId fieldId, int options);

  void InitCommon(Document *doc, uint32_t idxFlags_);

  // private
  int hasOffsets() const;
  char *copyTempString(const char *s, size_t n);
  khIdxEntry *makeEntry(const char *s, size_t n, uint32_t h, int *isNew);
};

//---------------------------------------------------------------------------------------------

struct ForwardIndexTokenizerCtx {
  const char *doc;
  VarintVectorWriter *allOffsets;
  ForwardIndex *idx;
  t_fieldId fieldId;
  float fieldScore;

  // ctor
  ForwardIndexTokenizerCtx(ForwardIndex *idx, const char *doc, VarintVectorWriter *vvw,
                           t_fieldId fieldId, float score) :
  idx(idx), fieldId(fieldId), fieldScore(score), doc(doc), allOffsets(vvw) {}

  int TokenFunc(const Token *tokInfo);
};

///////////////////////////////////////////////////////////////////////////////////////////////
