#pragma once

#include "doc_table.h"
#include "forward_index.h"
#include "index_result.h"
#include "index_iterator.h"
#include "redisearch.h"
#include "util/logging.h"
#include "varint.h"
#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

///////////////////////////////////////////////////////////////////////////////////////////////

typedef Vector<IndexIterator*> IndexIterators;

//---------------------------------------------------------------------------------------------

// Create a new UnionIterator over a list of underlying child iterators.
// It will return each document of the underlying iterators, exactly once

struct UnionIterator : public IndexIterator {
  UnionIterator(IndexIterators its, DocTable *dt, int quickExit, double weight);

  // We maintain two iterator arrays. One is the original iterator list, and
  // the other is the list of currently active iterators. When an iterator
  // reaches EOF, it is set to NULL in the `its` list, but is still retained in
  // the `origits` list, for the purpose of supporting things like Rewind() and Free().

  IndexIterators its;
  IndexIterators origits;
  uint32_t currIt;
  t_docId minDocId;

  // If set to 1, we exit skips after the first hit found and not merge further results
  int quickExit;
  size_t nexpected;
  double weight;
  uint64_t len;

  void SyncIterList();
  size_t RemoveExhausted(size_t badix);
  t_docId LastDocId() const;

  int (UnionIterator::*_Read)(IndexResult **hit);

  virtual int Read(IndexResult **hit) { return (this->*_Read)(hit); }
  int ReadSorted(IndexResult **hit);
  int ReadUnsorted(IndexResult **hit);

  virtual void Abort();
  virtual void Rewind();
  virtual int SkipTo(t_docId docId, IndexResult **hit);
  virtual size_t NumEstimated() const;
  virtual IndexCriteriaTester *GetCriteriaTester();
  virtual size_t Len() const { return len; }

  AggregateResult &result() { return reinterpret_cast<AggregateResult&>(*current); }
};

//---------------------------------------------------------------------------------------------

struct UnionCriteriaTester : public IndexCriteriaTester {
  UnionCriteriaTester(Vector<IndexCriteriaTester*> testers);

  Vector<IndexCriteriaTester*> children;

  bool Test(t_docId id);
};

///////////////////////////////////////////////////////////////////////////////////////////////

// Create a new intersect iterator over the given list of child iterators. If maxSlop is not a
// negative number, we will allow at most maxSlop intervening positions between the terms.
// If maxSlop is set and inOrder is 1, we assert that the terms are in order.
// I.e anexact match has maxSlop of 0 and inOrder 1.

struct IntersectIterator : IndexIterator {
  IntersectIterator(IndexIterators its, DocTable *dt, t_fieldMask fieldMask,
                    int maxSlop, int inOrder, double weight);
  ~IntersectIterator();

  IndexIterators its;
  IndexIterator *bestIt;
  Vector<IndexCriteriaTester*> testers;
  t_docId *docIds;
  int *rcs;
  size_t len;
  int maxSlop;
  int inOrder;
  t_docId lastDocId;   // last read docId from any child
  t_docId lastFoundId; // last id that was found on all children

  DocTable *docTable;
  t_fieldMask fieldMask;
  double weight;
  size_t nexpected;

  int (IntersectIterator::*_Read)(IndexResult **hit);

  virtual int Read(IndexResult **hit) { return (this->*_Read)(hit); }
  int ReadSorted(IndexResult **hit);
  int ReadUnsorted(IndexResult **hit);

  void SortChildren();

  virtual IndexCriteriaTester *GetCriteriaTester();
  virtual int SkipTo(t_docId docId, IndexResult **hit);
  virtual void Abort();
  virtual void Rewind();
  virtual size_t NumEstimated() const;
  virtual size_t Len() const;
  t_docId LastDocId() const;

  AggregateResult &result() { return reinterpret_cast<AggregateResult&>(*current); }

  //-------------------------------------------------------------------------------------------

  struct CriteriaTester : public IndexCriteriaTester {
    CriteriaTester(Vector<IndexCriteriaTester*> testers);

    Vector<IndexCriteriaTester*> children;

    bool Test(t_docId id);
  };
};

///////////////////////////////////////////////////////////////////////////////////////////////

// A Not iterator works by wrapping another iterator, and returning OK for misses, and NOTFOUND for hits

struct NotIterator : public IndexIterator {
  NotIterator(IndexIterator *it, t_docId maxDocId_, double weight_);
  ~NotIterator();

  IndexIterator *child;
  IndexCriteriaTester *childCT;
  t_docId lastDocId;
  t_docId maxDocId;
  size_t len;
  double weight;

  int (NotIterator::*_Read)(IndexResult **hit);

  virtual int Read(IndexResult **hit) { return (this->*_Read)(hit); }
  int ReadSorted(IndexResult **hit);
  int ReadUnsorted(IndexResult **hit);

  virtual void Abort();
  virtual void Rewind();
  virtual int SkipTo(t_docId docId, IndexResult **hit);
  virtual size_t NumEstimated() const ;
  virtual IndexCriteriaTester *GetCriteriaTester();
  virtual size_t Len() const ;
  virtual bool HasNext() const;

  t_docId LastDocId() const;
};

//---------------------------------------------------------------------------------------------

struct NI_CriteriaTester : public IndexCriteriaTester {
  NI_CriteriaTester(IndexCriteriaTester *childTester);
  ~NI_CriteriaTester();

  IndexCriteriaTester *child;

  bool Test(t_docId id);
};

///////////////////////////////////////////////////////////////////////////////////////////////

// Create an Optional clause iterator by wrapping another index iterator. An optional iterator
// always returns OK on skips, but a virtual hit with frequency of 0 if there is no hit

struct OptionalIterator : public IndexIterator {
  OptionalIterator(IndexIterator *it, t_docId maxDocId_, double weight_);
  ~OptionalIterator();

  IndexIterator *child;
  IndexCriteriaTester *childCT;
  IndexResult *virt;
  t_fieldMask fieldMask;
  t_docId lastDocId;
  t_docId maxDocId;
  t_docId nextRealId;
  double weight;

  int (OptionalIterator::*_Read)(IndexResult **hit);

  virtual int Read(IndexResult **hit) { return (this->*_Read)(hit); }
  int ReadSorted(IndexResult **hit);
  int ReadUnsorted(IndexResult **hit);

  virtual int SkipTo(t_docId docId, IndexResult **hit);
  virtual IndexCriteriaTester *GetCriteriaTester();
  virtual bool HasNext() const;
  virtual void Abort();
  virtual void Rewind();

  virtual size_t NumEstimated() const;
  virtual t_docId LastDocId() const;
  virtual size_t Len() const;
};

///////////////////////////////////////////////////////////////////////////////////////////////

// Wildcard iterator, matchin ALL documents in the index. This is used for one thing only -
// purely negative queries.
// If the root of the query is a negative expression, we cannot process it without a positive expression.
// So we create a wildcard iterator that basically just iterates all the incremental document ids,
// and matches every skip within its range.

struct WildcardIterator : public IndexIterator {
  WildcardIterator(t_docId maxId);

  t_docId topId;
  t_docId currentId;

  virtual int Read(IndexResult **hit);
  virtual bool HasNext() const;
  virtual int SkipTo(t_docId docId, IndexResult **hit);
  virtual void Abort();
  virtual void Rewind();
  virtual IndexCriteriaTester *GetCriteriaTester() { return NULL; }
  virtual size_t Len() const;
  virtual t_docId LastDocId() const;
  virtual size_t NumEstimated() const;
};

///////////////////////////////////////////////////////////////////////////////////////////////

struct EOFIterator : public IndexIterator {
  virtual IndexResult *GetCurrent() {}
  virtual size_t NumEstimated() const { return 0; }
  virtual IndexCriteriaTester *GetCriteriaTester() { return NULL; }
  virtual int Read(IndexResult **e) { return INDEXREAD_EOF; }
  virtual int SkipTo(t_docId docId, IndexResult **hit) { return INDEXREAD_EOF; }
  virtual t_docId LastDocId() const { return t_docId{0}; }
  virtual bool HasNext() const { return 0; }
  virtual size_t Len() const { return 0; }
  virtual void Abort() {}
  virtual void Rewind() {}
};

// Iterator which returns no results

typedef EOFIterator EmptyIterator;

///////////////////////////////////////////////////////////////////////////////////////////////
