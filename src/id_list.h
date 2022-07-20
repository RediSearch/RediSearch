
#include "index_result.h"
#include "index_iterator.h"
#include "rmalloc.h"

///////////////////////////////////////////////////////////////////////////////////////////////

struct IdListIterator : public IndexIterator {
  t_docId *docIds;
  t_docId lastDocId;
  t_offset size;
  t_offset offset;

  IdListIterator(t_docId *ids, t_offset num, double weight);
  ~IdListIterator();

  virtual IndexResult *GetCurrent();
  virtual size_t NumEstimated();
  virtual IndexCriteriaTester *GetCriteriaTester();
  virtual int Read(IndexResult **e);
  virtual int SkipTo(t_docId docId, IndexResult **hit);
  virtual t_docId LastDocId();
  virtual int HasNext();
  virtual size_t Len();
  virtual void Abort();
  virtual void Rewind();

  void setEof(int value) { isValid = !value; }
  int isEof() const { return !isValid; }

  static int cmp_docids(const t_docId *d1, const t_docId *d2);
};

//---------------------------------------------------------------------------------------------

struct ILCriteriaTester : public IndexCriteriaTester {
  t_docId *docIds;
  t_offset size;

  ILCriteriaTester(IdListIterator *it);
  ~ILCriteriaTester();

  int Test(t_docId id);
};

///////////////////////////////////////////////////////////////////////////////////////////////
