
#ifndef __INDEX_RESULT_H__
#define __INDEX_RESULT_H__

#include "varint.h"
#include "redisearch.h"
#include "rmalloc.h"

#ifdef __cplusplus
extern "C" {
#endif

///////////////////////////////////////////////////////////////////////////////////////////////

#define DEFAULT_RECORDLIST_SIZE 4

RSQueryTerm *NewQueryTerm(RSToken *tok, int id);

void Term_Free(RSQueryTerm *t);

//---------------------------------------------------------------------------------------------

// Allocate a new intersection result with a given capacity
RSIndexResult *NewIntersectResult(size_t cap, double weight);

// Allocate a new union result with a given capacity
RSIndexResult *NewUnionResult(size_t cap, double weight);

RSIndexResult *NewVirtualResult(double weight);

RSIndexResult *NewNumericResult();

// Allocate a new token record result for a given term
RSIndexResult *NewTokenRecord(RSQueryTerm *term, double weight);

//---------------------------------------------------------------------------------------------

// Reset state of an existing index hit. This can be used to recycle index hits during reads
void IndexResult_Init(RSIndexResult *h);

// Create a deep copy of the results that is totall thread safe. This is very slow so use it with caution
RSIndexResult *IndexResult_DeepCopy(const RSIndexResult *res);

// Debug print a result
void IndexResult_Print(RSIndexResult *r, int depth);

// Free an index result's internal allocations, does not free the result itself
void IndexResult_Free(RSIndexResult *r);

// Get the minimal delta between the terms in the result
int IndexResult_MinOffsetDelta(const RSIndexResult *r);

// Fill an array of max capacity cap with all the matching text terms for the result.
// The number of matching terms is returned.
size_t IndexResult_GetMatchedTerms(RSIndexResult *r, RSQueryTerm **arr, size_t cap);

// Return 1 if the the result is within a given slop range, inOrder determines whether the tokens
// need to be ordered as in the query or not
int IndexResult_IsWithinRange(RSIndexResult *r, int maxSlop, int inOrder);

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

struct UnionResult : AggregateResult {
  UnionResult(size_t cap, double weight) : AggregateResult(RSResultType_Union, cap, weight) {}
};

//---------------------------------------------------------------------------------------------

struct TokenResult : public RSIndexResult {
  TokenResult(RSQueryTerm *term_, double weight) {
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

#ifdef __cplusplus
}
#endif

#endif
