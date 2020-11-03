
#pragma once

#include "varint.h"
#include "redisearch.h"
#include "rmalloc.h"

///////////////////////////////////////////////////////////////////////////////////////////////

#define DEFAULT_RECORDLIST_SIZE 4

//---------------------------------------------------------------------------------------------

// Allocate a new intersection result with a given capacity
RSIndexResult *NewIntersectResult(size_t cap, double weight);

// Allocate a new union result with a given capacity
RSIndexResult *NewUnionResult(size_t cap, double weight);

RSIndexResult *NewVirtualResult(double weight);

RSIndexResult *NewNumericResult();

// Allocate a new term record result for a given term
RSIndexResult *NewTermRecord(RSQueryTerm *term, double weight);

//---------------------------------------------------------------------------------------------

struct AggregateResult : RSIndexResult {
  AggregateResult(RSResultType t, size_t cap, double weight_) {
    type = t;
    docId = 0;
    freq = 0;
    fieldMask = 0;
    isCopy = 0;
    weight = weight;
    agg.numChildren = 0;
    agg.childrenCap = cap;
    agg.typeMask = 0x0000;
    agg.children = rm_calloc(cap, sizeof(RSIndexResult *));
  }

  // Reset aggregate result's child vector
  void Reset() {
    docId = 0;
    agg.numChildren = 0;
    agg.typeMask = (RSResultType) 0;
  }

  // Append a child to an aggregate result
  void AddChild(RSIndexResult *child) {
    // Increase capacity if needed
    if (agg.numChildren >= agg.childrenCap) {
      agg.childrenCap = agg.childrenCap ? agg.childrenCap * 2 : 1;
      agg.children = (__typeof__(agg.children)) rm_realloc(agg.children, agg.childrenCap * sizeof(RSIndexResult *));
    }
    agg.children[agg.numChildren++] = child;
  
    agg.typeMask |= child->type;
    freq += child->freq;
    docId = child->docId;
    fieldMask |= child->fieldMask;
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

struct TermResult : public RSIndexResult {
  TermResult(RSQueryTerm *term_, double weight) {
    type = RSResultType_Term;
    docId = 0;
    fieldMask = 0;
    isCopy = 0;
    freq = 0;
    weight = weight;
    term.term = term_; //@@ ownership?
    term.offsets.len = 0;
    term.offsets.data = 0;
  }
};

//---------------------------------------------------------------------------------------------

struct NumericResult : public RSIndexResult {
  NumericResult() {
    type = RSResultType_Numeric;
    docId = 0;
    isCopy = 0;
    fieldMask = RS_FIELDMASK_ALL;
    freq = 1;
    weight = 1;
    num.value = 0;
  }
};

//---------------------------------------------------------------------------------------------

struct VirtualResult : RSIndexResult {
  VirtualResult(double w) {
    type = RSResultType_Virtual;
    docId = 0;
    fieldMask = 0;
    freq = 0;
    weight = w;
    isCopy = 0;
  }
};

///////////////////////////////////////////////////////////////////////////////////////////////
