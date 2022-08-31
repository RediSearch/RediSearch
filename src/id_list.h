
#pragma once

#include "index_result.h"
#include "index_iterator.h"
#include "rmalloc.h"

///////////////////////////////////////////////////////////////////////////////////////////////

struct IdListIterator : public IndexIterator {
  Vector<t_docId> docIds;
  t_docId lastDocId;
  t_offset size;
  t_offset offset;

  IdListIterator(Vector<t_docId> &ids, double weight);
  ~IdListIterator();

  virtual IndexCriteriaTester *GetCriteriaTester() { return new CriteriaTester(this); }
  virtual int Read(IndexResult **e);
  virtual int SkipTo(t_docId docId, IndexResult **hit);
  virtual void Abort();
  virtual void Rewind();

  virtual size_t NumEstimated() const;
  virtual t_docId LastDocId() const;
  virtual size_t Len() const;

  void setEof(int value) { isValid = !value; }
  int isEof() const { return !isValid; }

  //-------------------------------------------------------------------------------------------

  struct CriteriaTester : public IndexCriteriaTester {
    Vector<t_docId> &docIds;

    CriteriaTester(IdListIterator *it);

    bool Test(t_docId id);
  };
};

///////////////////////////////////////////////////////////////////////////////////////////////
