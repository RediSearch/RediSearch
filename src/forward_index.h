
#pragma once

#include "redisearch.h"
#include "util/block_alloc.h"
#include "util/khtable.h"
#include "util/mempool.h"
#include "triemap/triemap.h"
#include "rmutil/vector.h"
#include "varint.h"
#include "tokenize.h"
#include "document.h"

///////////////////////////////////////////////////////////////////////////////////////////////

struct VarintVectorWriterPool : MemPool<struct ForwardIndexEntry> {
};

struct ForwardIndexEntry {
  ForwardIndexEntry(ForwardIndex &idx, const char *tok, size_t tokLen, float fieldScore,
    t_fieldId fieldId, int options);

  struct ForwardIndexEntry *next;
  t_docId docId;

  uint32_t freq;
  t_fieldMask fieldMask;

  const char *term;
  uint32_t len;
  //uint32_t hash;
  VarintVectorWriter *vw;
};

//---------------------------------------------------------------------------------------------

typedef UnorderedMap<std::string, Vector<ForwardIndexEntry *>> ForwardIndexHitMap;

struct ForwardIndexIterator {
  ForwardIndexIterator(const ForwardIndex &idx);

  const ForwardIndexHitMap *hits;
  Vector<ForwardIndexEntry*> *curVec;
  uint32_t curBucketIdx;

  ForwardIndexEntry *Next();
};

struct ForwardIndex : Object {
  ForwardIndexHitMap hits;
  uint32_t maxFreq;
  uint32_t totalFreq;
  uint32_t idxFlags;
  Stemmer *stemmer;
  SynonymMap *smap;
  StringBlkAlloc terms;
  BlkAlloc<ForwardIndexEntry> entries;
  BlkAlloc<VarintVectorWriter> vvw_pool;

  void ctor(uint32_t idxFlags_);
  ForwardIndex(Document *doc, uint32_t idxFlags_);
  ~ForwardIndex();
  void Reset(Document *doc, uint32_t idxFlags_);

  ForwardIndexIterator Iterate() const { return ForwardIndexIterator(*this); }

  void HandleToken(const char *tok, size_t tokLen, uint32_t pos,
                   float fieldScore, t_fieldId fieldId, int options);

  // private
  int hasOffsets() const;
};

//---------------------------------------------------------------------------------------------

struct ForwardIndexTokenizer {
  const char *doc;
  VarintVectorWriter *allOffsets;
  ForwardIndex *idx;
  t_fieldId fieldId;
  float fieldScore;

  // ctor
  ForwardIndexTokenizer(ForwardIndex *idx, const char *doc, VarintVectorWriter *vvw,
                        t_fieldId fieldId, float score) :
  idx(idx), fieldId(fieldId), fieldScore(score), doc(doc), allOffsets(vvw) {}

  void tokenize(const Token &tok);
};

///////////////////////////////////////////////////////////////////////////////////////////////
