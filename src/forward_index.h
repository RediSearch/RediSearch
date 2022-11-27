
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

using VarintVectorWriterPool = MemPool<struct ForwardIndexEntry>;

struct ForwardIndexEntry {
  ForwardIndexEntry(ForwardIndex &idx, std::string_view tok, float fieldScore,
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

using ForwardIndexHits = Vector<ForwardIndexEntry*>;
using ForwardIndexHitMap = UnorderedMap<String, ForwardIndexHits>;

struct ForwardIndexIterator {
  ForwardIndexIterator() = delete;
  ForwardIndexIterator(const ForwardIndex &idx);

  const ForwardIndexHitMap *hitsMap;
  ForwardIndexHits *hits;

  ForwardIndexEntry *Next();
};

//---------------------------------------------------------------------------------------------

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

  void clear(uint32_t idxFlags);
  ForwardIndex(Document *doc, uint32_t idxFlags, SynonymMap *smap_ = nullptr);
  ~ForwardIndex();

  void Reset(Document *doc, uint32_t idxFlags);

  ForwardIndexIterator Iterate() const { return ForwardIndexIterator(*this); }

  void HandleToken(std::string_view tok, uint32_t pos, float fieldScore, t_fieldId fieldId, int options);

  bool hasOffsets() const;
};

//---------------------------------------------------------------------------------------------

struct ForwardIndexTokenizer {
  const char *doc;
  VarintVectorWriter *allOffsets;
  ForwardIndex *idx;
  t_fieldId fieldId;
  float fieldScore;

  ForwardIndexTokenizer(ForwardIndex *idx, const char *doc, VarintVectorWriter *vvw, t_fieldId fieldId,
    float score) : idx(idx), fieldId(fieldId), fieldScore(score), doc(doc), allOffsets(vvw) {}

  void tokenize(const Token &tok);
};

///////////////////////////////////////////////////////////////////////////////////////////////
