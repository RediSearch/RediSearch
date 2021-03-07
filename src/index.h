#ifndef __INDEX_H__
#define __INDEX_H__

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

// Create a new UnionIterator over a list of underlying child iterators.
// It will return each document of the underlying iterators, exactly once

class UnionIterator : public IndexIterator {
public:
  UnionIterator(IndexIterator **its, int num, DocTable *dt, int quickExit, double weight);
  ~UnionIterator();

  // We maintain two iterator arrays. One is the original iterator list, and
  // the other is the list of currently active iterators. When an iterator
  // reaches EOF, it is set to NULL in the `its` list, but is still retained in
  // the `origits` list, for the purpose of supporting things like Rewind() and Free().

  IndexIterator **its;
  IndexIterator **origits;
  uint32_t num;
  uint32_t norig;
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

  int (UnionIterator::*_Read)(RSIndexResult **hit);

  virtual int Read(RSIndexResult **hit) { return (this->*_Read)(hit); }
  int ReadSorted(RSIndexResult **hit);
  int ReadUnsorted(RSIndexResult **hit);

  virtual void Abort();
  virtual void Rewind();
  virtual int SkipTo(t_docId docId, RSIndexResult **hit);
  virtual size_t NumEstimated();
  virtual IndexCriteriaTester *GetCriteriaTester();
  virtual size_t Len();

  AggregateResult &result() { return reinterpret_cast<AggregateResult&>(*current); }
};

//---------------------------------------------------------------------------------------------

class UnionCriteriaTester : public IndexCriteriaTester {
public:
  UnionCriteriaTester(IndexCriteriaTester **testers, int ntesters);
  ~UnionCriteriaTester();

  IndexCriteriaTester **children;
  int nchildren;

  int Test(t_docId id);
};

///////////////////////////////////////////////////////////////////////////////////////////////

// Create a new intersect iterator over the given list of child iterators. If maxSlop is not a
// negative number, we will allow at most maxSlop intervening positions between the terms.
// If maxSlop is set and inOrder is 1, we assert that the terms are in order.
// I.e anexact match has maxSlop of 0 and inOrder 1.

class IntersectIterator : public IndexIterator {
public:
  IntersectIterator(IndexIterator **its, size_t num, DocTable *dt, t_fieldMask fieldMask,
                    int maxSlop, int inOrder, double weight);
  ~IntersectIterator();

  IndexIterator **its;
  IndexIterator *bestIt;
  IndexCriteriaTester **testers;
  t_docId *docIds;
  int *rcs;
  unsigned num;
  size_t len;
  int maxSlop;
  int inOrder;
  t_docId lastDocId; // last read docId from any child
  t_docId lastFoundId; // last id that was found on all children

  DocTable *docTable;
  t_fieldMask fieldMask;
  double weight;
  size_t nexpected;

  int (IntersectIterator::*_Read)(RSIndexResult **hit);

  virtual int Read(RSIndexResult **hit) { return (this->*_Read)(hit); }
  int ReadSorted(RSIndexResult **hit);
  int ReadUnsorted(RSIndexResult **hit);

  void SortChildren();
  t_docId LastDocId();

  virtual IndexCriteriaTester *GetCriteriaTester();
  virtual int SkipTo(t_docId docId, RSIndexResult **hit);
  virtual void Abort();
  virtual void Rewind();
  virtual size_t NumEstimated();
  virtual size_t Len();

  AggregateResult &result() { return reinterpret_cast<AggregateResult&>(*current); }
};

//---------------------------------------------------------------------------------------------

class IICriteriaTester : public IndexCriteriaTester {
public:
  IICriteriaTester(IndexCriteriaTester **testers);
  ~IICriteriaTester();

  IndexCriteriaTester **children;

  int Test(t_docId id);
};


///////////////////////////////////////////////////////////////////////////////////////////////

// A Not iterator works by wrapping another iterator, and returning OK for misses, and NOTFOUND for hits

class NotIterator : public IndexIterator {
public:
  NotIterator(IndexIterator *it, t_docId maxDocId_, double weight_);
  ~NotIterator();

  IndexIterator *child;
  IndexCriteriaTester *childCT;
  t_docId lastDocId;
  t_docId maxDocId;
  size_t len;
  double weight;

  int (NotIterator::*_Read)(RSIndexResult **hit);

  virtual int Read(RSIndexResult **hit) { return (this->*_Read)(hit); }
  int ReadSorted(RSIndexResult **hit);
  int ReadUnsorted(RSIndexResult **hit);

  virtual void Abort();
  virtual void Rewind();
  virtual int SkipTo(t_docId docId, RSIndexResult **hit);
  virtual size_t NumEstimated();
  virtual IndexCriteriaTester *GetCriteriaTester();
  virtual size_t Len();
  virtual int HasNext();

  t_docId LastDocId();
};

typedef NotIterator NotContext;

//---------------------------------------------------------------------------------------------

class NI_CriteriaTester : public IndexCriteriaTester {
public:
  NI_CriteriaTester(IndexCriteriaTester *childTester);
  ~NI_CriteriaTester();

  IndexCriteriaTester *child;

  int Test(t_docId id);
};

///////////////////////////////////////////////////////////////////////////////////////////////

// Create an Optional clause iterator by wrapping another index iterator. An optional iterator
// always returns OK on skips, but a virtual hit with frequency of 0 if there is no hit

class OptionalIterator : public IndexIterator {
public:
  OptionalIterator(IndexIterator *it, t_docId maxDocId_, double weight_);
  ~OptionalIterator();

  IndexIterator *child;
  IndexCriteriaTester *childCT;
  RSIndexResult *virt;
  t_fieldMask fieldMask;
  t_docId lastDocId;
  t_docId maxDocId;
  t_docId nextRealId;
  double weight;

  int (OptionalIterator::*_Read)(RSIndexResult **hit);

  virtual int Read(RSIndexResult **hit) { return (this->*_Read)(hit); }
  int ReadSorted(RSIndexResult **hit);
  int ReadUnsorted(RSIndexResult **hit);

  virtual int SkipTo(t_docId docId, RSIndexResult **hit);
  virtual IndexCriteriaTester *GetCriteriaTester();
  virtual size_t NumEstimated();
  virtual int HasNext();
  virtual void Abort();
  virtual size_t Len();
  virtual void Rewind();

  t_docId LastDocId();
};

typedef OptionalIterator OptionalMatchContext;

//---------------------------------------------------------------------------------------------


///////////////////////////////////////////////////////////////////////////////////////////////

// Wildcard iterator, matchin ALL documents in the index. This is used for one thing only -
// purely negative queries.
// If the root of the query is a negative expression, we cannot process it without a positive expression. 
// So we create a wildcard iterator that basically just iterates all the incremental document ids, 
// and matches every skip within its range.

class WildcardIterator : public IndexIterator {
  WildcardIterator(t_docId maxId);

  t_docId topId;
  t_docId currentId;

  int Read(RSIndexResult **hit);
  int SkipTo(t_docId docId, RSIndexResult **hit);
  void Abort();
  int HasNext();
  size_t Len();
  t_docId LastDocId();
  void Rewind();
  size_t NumEstimated();
};

typedef WildcardIterator WildcardIteratorCtx;

//---------------------------------------------------------------------------------------------


///////////////////////////////////////////////////////////////////////////////////////////////

class EOFIterator : public IndexIterator {
public:
  virtual RSIndexResult *GetCurrent() {}
  virtual size_t NumEstimated() { return 0; }
  virtual IndexCriteriaTester *GetCriteriaTester() { return NULL; }
  virtual int Read(RSIndexResult **e) { return INDEXREAD_EOF; }
  virtual int SkipTo(t_docId docId, RSIndexResult **hit) { return INDEXREAD_EOF; }
  virtual t_docId LastDocId() { return 0; }
  virtual int HasNext() { return 0; }
  virtual size_t Len() { return 0; }
  virtual void Abort() {}
  virtual void Rewind() {}
};

// Iterator which returns no results

typedef EOFIterator EmptyIterator;

///////////////////////////////////////////////////////////////////////////////////////////////

// Load document metadata for an index hit, marking it as having metadata.
// Currently has no effect due to performance issues
int IndexResult_LoadMetadata(RSIndexResult *h, DocTable *dt);

// Create a new IdListIterator from a pre populated list of document ids of size num. The doc ids
// are sorted in this function, so there is no need to sort them. They are automatically freed in
// the end and assumed to be allocated using rm_malloc
IndexIterator *NewIdListIterator(t_docId *ids, t_offset num, double weight);

// IndexIterator *NewEmptyIterator();

///////////////////////////////////////////////////////////////////////////////////////////////

#endif
