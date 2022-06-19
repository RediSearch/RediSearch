
#pragma once

#include "varint.h"
#include "redisearch.h"
#include "rmalloc.h"
#include "forward_index.h"
#include "offset_vector.c"

///////////////////////////////////////////////////////////////////////////////////////////////

#define DEFAULT_RECORDLIST_SIZE 4

//---------------------------------------------------------------------------------------------

struct AggregateResult : IndexResult {
  // number of child records
  int numChildren;
  // capacity of the records array. Has no use for extensions
  int childrenCap;
  // array of recods
  struct IndexResult **children;

  // A map of the aggregate type of the underlying results
  uint32_t typeMask;

  AggregateResult(RSResultType t, size_t cap, double weight) :
    IndexResult(t, 0, 0, 0, weight) {
    numChildren = 0;
    childrenCap = cap;
    typeMask = 0x0000;
    children = rm_calloc(cap, sizeof(IndexResult *));
  }
  AggregateResult(const AggregateResult &src);
  ~AggregateResult();

  void Reset();

  void AddChild(IndexResult *child);

  void Print(int depth) const;

  bool HasOffsets() const;

  void GetMatchedTerms(RSQueryTerm *arr[], size_t cap, size_t &len);

  int MinOffsetDelta() const;

  bool IsWithinRange(int maxSlop, bool inOrder) const;

  AggregateOffsetIterator IterateOffsetsInternal() const;

  RSOffsetIterator::Proxy IterateOffsets() const {
    // if we only have one sub result, just iterate that...
    if (numChildren == 1) {
      return children[0].IterateOffsetsInternal();
    }
    return IterateOffsetsInternal();
  }
};

//---------------------------------------------------------------------------------------------

struct IntersectResult : AggregateResult {
  IntersectResult(size_t cap, double weight) : AggregateResult(RSResultType_Intersection, cap, weight) {}
};

//---------------------------------------------------------------------------------------------

struct UnionResult : AggregateResult {
  UnionResult(size_t cap, double weight) : AggregateResult(RSResultType_Union, cap, weight) {}
};

//---------------------------------------------------------------------------------------------

struct TermResult : public IndexResult {
  // The term that brought up this record
  RSQueryTerm *term;

  // The encoded offsets in which the term appeared in the document
  RSOffsetVector offsets;

  TermResult(RSQueryTerm *term_, double weight) :
    IndexResult(RSResultType_Term, 0, 0, 0, weight) {
    term = term_; //@@ ownership?
    offsets.len = 0;
    offsets.data = 0;
  }

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

  RSOffsetIterator::Proxy IterateOffsets() const {
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

  NumericResult() : IndexResult(RSResultType_Numeric, 0, RS_FIELDMASK_ALL, 1, 1) {
    value = 0;
  }

  void Print(int depth) const;
};

//---------------------------------------------------------------------------------------------

struct VirtualResult : IndexResult {
  char dummy;

  VirtualResult(double weight) : IndexResult(RSResultType_Virtual, 0, 0, 0, weight) {}

  void Print(int depth) const;
};

///////////////////////////////////////////////////////////////////////////////////////////////
