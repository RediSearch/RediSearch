
#pragma once

#include "varint.h"
#include "redisearch.h"
#include "rmalloc.h"
#include "forward_index.h"

///////////////////////////////////////////////////////////////////////////////////////////////

#define DEFAULT_RECORDLIST_SIZE 4

//---------------------------------------------------------------------------------------------

struct ScoreExplain;

///////////////////////////////////////////////////////////////////////////////////////////////

// We have two types of offset vector iterators - for terms and for aggregates.
// For terms we simply yield the encoded offsets one by one.
// For aggregates, we merge them on the fly in order.
// They are both encapsulated in an abstract iterator interface called RSOffsetIterator, with
// callbacks and context matching the appropriate implementation.

///////////////////////////////////////////////////////////////////////////////////////////////

class RSOffsetVectorIteratorPool : public MemPool {
public:
  RSOffsetVectorIteratorPool() : MemPool(8, 0, true) {}
};

// A raw offset vector iterator
struct RSOffsetVectorIterator : public RSOffsetIterator,
                                public MemPoolObject<RSOffsetVectorIteratorPool> {
  Buffer buf;
  BufferReader br;
  uint32_t lastValue;
  RSQueryTerm *term;

  RSOffsetVectorIterator(const RSOffsetVector *v, RSQueryTerm *t) : buf(v->data, v->len, v->len),
    br(&buf), lastValue(0), term(t) {}

  virtual uint32_t Next(RSQueryTerm **t);
  virtual void Rewind();
};

//---------------------------------------------------------------------------------------------

struct AggregateResult;

class AggregateOffsetIteratorPool : public MemPool {
public:
  AggregateOffsetIteratorPool() : MemPool(8, 0, true) {}
};

struct AggregateOffsetIterator : public RSOffsetIterator,
                                 public MemPoolObject<AggregateOffsetIteratorPool> {
  const AggregateResult *res;
  size_t size;
  RSOffsetIterator *iters;
  uint32_t *offsets;
  RSQueryTerm **terms;
  // uint32_t lastOffset; - TODO: Avoid duplicate offsets

  AggregateOffsetIterator() {
    size = 0;
    offsets = NULL;
    iters = NULL;
    terms = NULL;
  }

  AggregateOffsetIterator(const AggregateResult *agg);
  ~AggregateOffsetIterator();

  virtual uint32_t Next(RSQueryTerm **t);
  virtual void Rewind();
};

//---------------------------------------------------------------------------------------------

struct AggregateResult : IndexResult {
  Vector<IndexResult *> children;

  // A map of the aggregate type of the underlying results
  uint32_t typeMask;

  AggregateResult(RSResultType t, size_t cap, double weight) :
    IndexResult(t, t_docId{0}, 0, 0, weight) {
    typeMask = 0x0000;
    children.reserve(cap);
  }

  AggregateResult(const AggregateResult &src);

  void Reset();

  void AddChild(IndexResult *child);

  void Print(int depth) const;

  bool HasOffsets() const;

  void GetMatchedTerms(RSQueryTerm *arr[], size_t cap, size_t &len);

  int MinOffsetDelta() const;

  bool IsWithinRange(int maxSlop, bool inOrder) const;

  size_t NumChildren() const { return children.size(); }

  std::unique_ptr<RSOffsetIterator> IterateOffsets() const {
    // if we only have one sub result, just iterate that...
    if (children.size() == 1) {
      return std::make_unique<AggregateOffsetIterator>(children[0]);
    } else {
      return std::make_unique<AggregateOffsetIterator>(this);
    }
  }

  virtual double TFIDFScorer(const RSDocumentMetadata *dmd, ScoreExplain *explain) const;

  double BM25Scorer(const ScorerArgs *args, const RSDocumentMetadata *dmd) const;
};

//---------------------------------------------------------------------------------------------

struct IntersectResult : AggregateResult {
  IntersectResult(size_t cap, double weight) : AggregateResult(RSResultType_Intersection, cap, weight) {}

  double DisMaxScorer(const ScorerArgs *args) const;
};

//---------------------------------------------------------------------------------------------

struct UnionResult : AggregateResult {
  UnionResult(size_t cap, double weight) : AggregateResult(RSResultType_Union, cap, weight) {}

  double DisMaxScorer(const ScorerArgs *args) const;
};

//---------------------------------------------------------------------------------------------

struct TermResult : public IndexResult {
  // The term that brought up this record
  RSQueryTerm *term;

  // Encoded offsets in which the term appeared in the document
  RSOffsetVector offsets;

  TermResult(RSQueryTerm *term, double weight) :
    IndexResult(RSResultType_Term, t_docId{0}, 0, 0, weight), term(term) {} //@@ term ownership?

  TermResult(const TermResult &src);

  TermResult(const ForwardIndexEntry &ent) :
      IndexResult(RSResultType_Term, ent.docId, ent.fieldMask, ent.freq, 0) {
    offsetsSz = ent.vw ? ent.vw->GetByteLength() : 0;
    term = NULL;
    if (ent.vw) {
      offsets.data = ent.vw->GetByteData();
      offsets.len = ent.vw->GetByteLength();
    }
  }

  ~TermResult();

  void Print(int depth) const;

  bool HasOffsets() const;

  void GetMatchedTerms(RSQueryTerm *arr[], size_t cap, size_t &len);

  virtual double TFIDFScorer(const RSDocumentMetadata *dmd, ScoreExplain *explain) const;

  double BM25Scorer(const ScorerArgs *args, const RSDocumentMetadata *dmd) const;

  std::unique_ptr<RSOffsetIterator> IterateOffsets() const {
    return offsets.Iterate(term);
  }
};

//---------------------------------------------------------------------------------------------

struct ForwardIndexEntryResult : public TermResult {
  ForwardIndexEntryResult(const ForwardIndexEntry &ent) : TermResult(ent) {}
};

//---------------------------------------------------------------------------------------------

struct NumericResult : public IndexResult {
  double value;

  NumericResult(t_docId docId = t_docId{0}, double v = 0) : IndexResult(RSResultType_Numeric, docId, RS_FIELDMASK_ALL, 1, 1) {
    value = v;
  }

  void Print(int depth) const;
};

//---------------------------------------------------------------------------------------------

struct VirtualResult : IndexResult {
  char dummy;

  VirtualResult(double weight) : IndexResult(RSResultType_Virtual, t_docId{0}, 0, 0, weight) {}

  void Print(int depth) const;
};

///////////////////////////////////////////////////////////////////////////////////////////////
