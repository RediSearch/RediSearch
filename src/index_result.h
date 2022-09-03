
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
// They are both encapsulated in an abstract iterator interface called OffsetIterator, with
// callbacks and context matching the appropriate implementation.

///////////////////////////////////////////////////////////////////////////////////////////////

struct OffsetVectorIteratorPool : public MemPool<struct OffsetVectorIterator> {
  OffsetVectorIteratorPool() : MemPool(8, 0, true) {}
};

// A raw offset vector iterator
struct OffsetVectorIterator : OffsetIterator, Object {
                                //, MemPoolObject<OffsetVectorIteratorPool> { //@@POOL
  Buffer buf;
  BufferReader br;
  uint32_t lastValue;
  RSQueryTerm *term;

  OffsetVectorIterator(const OffsetVector *v, RSQueryTerm *t) : buf(v->data, v->len, v->len),
    br(&buf), lastValue(0), term(t) {}

  virtual uint32_t Next(RSQueryTerm **t);
  virtual void Rewind();
};

//---------------------------------------------------------------------------------------------

struct AggregateResult;

struct AggregateOffsetIteratorPool : MemPool<struct AggregateOffsetIterator> {
  AggregateOffsetIteratorPool() : MemPool(8, 0, true) {}
};

struct AggregateOffsetIterator : OffsetIterator, Object {
                                 //, public MemPoolObject<AggregateOffsetIteratorPool> { //@@POOL
  const AggregateResult *res;
  Vector<OffsetIterator> iters;
  Vector<uint32_t> offsets;
  Vector<RSQueryTerm *> terms;
  // uint32_t lastOffset; - TODO: Avoid duplicate offsets

  AggregateOffsetIterator() {}
  AggregateOffsetIterator(const AggregateResult *agg);
  ~AggregateOffsetIterator();

  virtual uint32_t Next(RSQueryTerm **t);
  virtual void Rewind();
};

//---------------------------------------------------------------------------------------------

struct AggregateResult : IndexResult {
  Vector<IndexResult *> children;

  Mask_i32(RSResultType) typeMask; // mask of aggregate types of underlying results

  AggregateResult(RSResultType t, size_t cap, double weight) :
      IndexResult(t, t_docId{0}, 0, 0, weight), typeMask(0x0) {
    children.reserve(cap);
  }

  ~AggregateResult();

  AggregateResult(const AggregateResult &src);

  IndexResult *Clone() const override { return new AggregateResult(*this); }

  void Reset();
  void AddChild(IndexResult *child);
  void GetMatchedTerms(RSQueryTerm *arr[], size_t cap, size_t &len);

  bool HasOffsets() const;
  int MinOffsetDelta() const;
  bool IsWithinRange(int maxSlop, bool inOrder) const;
  size_t NumChildren() const { return children.size(); }

  void Print(int depth) const;

  std::unique_ptr<OffsetIterator> IterateOffsets() const {
    // if we only have one sub result, just iterate that...
    if (children.size() == 1) {
      return std::make_unique<AggregateOffsetIterator>(children[0]);
    } else {
      return std::make_unique<AggregateOffsetIterator>(this);
    }
  }

  virtual double TFIDFScorer(const DocumentMetadata *dmd, ScoreExplain *explain) const;
  double BM25Scorer(const ScorerArgs *args, const DocumentMetadata *dmd) const;
};

//---------------------------------------------------------------------------------------------

struct IntersectResult : AggregateResult {
  IntersectResult(size_t cap, double weight) : AggregateResult(RSResultType_Intersection, cap, weight) {}

  double DisMaxScorer(const ScorerArgs *args) const;

  IndexResult *Clone() const override { return new IntersectResult(*this); }
};

//---------------------------------------------------------------------------------------------

struct UnionResult : AggregateResult {
  UnionResult(size_t cap, double weight) : AggregateResult(RSResultType_Union, cap, weight) {}
  //UnionResult(const UnionResult &result) : AggregateResult((const AggregateResult &)result) {}

  double DisMaxScorer(const ScorerArgs *args) const;

  IndexResult *Clone() const override { return new UnionResult(*this); }
};

//---------------------------------------------------------------------------------------------

struct TermResult : public IndexResult {
  // The term that brought up this record
  RSQueryTerm *term;

  // Encoded offsets in which the term appeared in the document
  OffsetVector offsets;

  TermResult(RSQueryTerm *term, double weight) :
    IndexResult(RSResultType_Term, t_docId{0}, 0, 0, weight), term(term) {} //@@ term ownership?

  TermResult(const TermResult &src);

  TermResult(const ForwardIndexEntry &ent) : IndexResult(RSResultType_Term, ent.docId, ent.fieldMask, ent.freq, 0) {
    offsetsSz = ent.vw ? ent.vw->GetByteLength() : 0;
    term = NULL;
    if (ent.vw) {
      offsets.data = ent.vw->GetByteData();
      offsets.len = ent.vw->GetByteLength();
    }
  }

  ~TermResult();

  IndexResult *Clone() const override { return new TermResult(*this); }

  void Print(int depth) const;

  bool HasOffsets() const;

  void GetMatchedTerms(RSQueryTerm *arr[], size_t cap, size_t &len);

  virtual double TFIDFScorer(const DocumentMetadata *dmd, ScoreExplain *explain) const;

  double BM25Scorer(const ScorerArgs *args, const DocumentMetadata *dmd) const;

  std::unique_ptr<OffsetIterator> IterateOffsets() const {
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

  NumericResult(const NumericResult &result) : IndexResult(result), value(result.value) {}

  IndexResult *Clone() const override { return new NumericResult(*this); }

  void Print(int depth) const;
};

//---------------------------------------------------------------------------------------------

struct VirtualResult : IndexResult {
  VirtualResult(t_docId docId) : IndexResult(RSResultType_Virtual, docId, 0, 0, 0) {} 
  VirtualResult(double weight) : IndexResult(RSResultType_Virtual, t_docId{0}, 0, 0, weight) {}
  VirtualResult(const VirtualResult &result) : IndexResult(result) {}

  IndexResult *Clone() const override { return new VirtualResult(*this); }
  void Print(int depth) const;
};

///////////////////////////////////////////////////////////////////////////////////////////////
