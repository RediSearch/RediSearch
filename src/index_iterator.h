
#pragma once

#include <stdint.h>
#include "redisearch.h"
#include "index_result.h"
#include "object.h"

///////////////////////////////////////////////////////////////////////////////////////////////

#define INDEXREAD_EOF 0
#define INDEXREAD_OK 1
#define INDEXREAD_NOTFOUND 2

enum class IndexIteratorMode {
  Sorted = 0,
  Unsorted = 1
};

// #define MODE_SORTED 0
// #define MODE_UNSORTED 1

class IndexReader;

//---------------------------------------------------------------------------------------------

class IndexCriteriaTester {
public:
  IndexCriteriaTester();
  ~IndexCriteriaTester();

  int Test(t_docId id) { return 1; }
};

//---------------------------------------------------------------------------------------------

// An abstract interface used by readers / intersectors / unioners etc.
// Basically query execution creates a tree of iterators that activate each other recursively

class IndexIterator : public Object {
  void init(IndexReader *ir_);

public:
  IndexIterator();
  IndexIterator(IndexReader *ir_);

  virtual ~IndexIterator();

  //-------------------------------------------------------------------------------------------

  // Cached value - used if HasNext() is not set
  bool isValid;

  IndexReader *ir;

  // Used by union iterator. Cached here for performance
  t_docId minId;

  // Cached value - used if Current() is not set
  IndexResult *current;

  IndexIteratorMode mode;

  // Return a string containing the type of the iterator
  const char *GetTypeString() const;

  //-------------------------------------------------------------------------------------------

  virtual IndexResult *GetCurrent() { return NULL; }

  virtual size_t NumEstimated() const;

  virtual IndexCriteriaTester *GetCriteriaTester();

  // Read the next entry from the iterator, into hit *e
  // Returns INDEXREAD_EOF if at the end
  virtual int Read(IndexResult **e);

  // Skip to a docid, potentially reading the entry into hit, if the docId matches
  virtual int SkipTo(t_docId docId, IndexResult **hit);

  // the last docId read
  virtual t_docId LastDocId() const ;

  // can we continue iteration?
  virtual bool HasNext() const { return false; }

  // Return the number of results in this iterator. Used by the query execution on the top iterator
  virtual size_t Len() const ;

  // Abort the execution of the iterator and mark it as EOF.
  // This is used for early aborting in case of data consistency issues due to multi threading
  virtual void Abort();

  // Rewinde the iterator to the beginning and reset its state
  virtual void Rewind();
};

//---------------------------------------------------------------------------------------------

#define IITER_HAS_NEXT(ii)             ((ii)->isValid ? 1 : (ii)->HasNext())
#define IITER_CURRENT_RECORD(ii)       ((ii)->current ? (ii)->current : (ii)->GetCurrent())
// #define IITER_NUM_ESTIMATED(ii)        (ii)->NumEstimated()
// #define IITER_GET_CRITERIA_TESTER(ii)  (ii)->GetCriteriaTester()

#define IITER_SET_EOF(ii)   (ii)->isValid = 0
#define IITER_CLEAR_EOF(ii) (ii)->isValid = 1

///////////////////////////////////////////////////////////////////////////////////////////////
