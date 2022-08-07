
#include "index_result.h"
#include "index_iterator.h"
#include "rmalloc.h"

///////////////////////////////////////////////////////////////////////////////////////////////

struct IdListIterator : public IndexIterator {
  Vector<t_docId> docIds;
  t_docId lastDocId;
  t_offset size;
  t_offset offset;

  IdListIterator(Vector<t_docId> docIds, double weight);
  ~IdListIterator();

  virtual IndexResult *GetCurrent();
  virtual size_t NumEstimated() const ;
  virtual IndexCriteriaTester *GetCriteriaTester();
  virtual int Read(IndexResult **e);
  virtual int SkipTo(t_docId docId, IndexResult **hit);
  virtual t_docId LastDocId() const ;
  virtual bool HasNext() const ;
  virtual size_t Len() const ;
  virtual void Abort();
  virtual void Rewind();

  void setEof(int value) { isValid = !value; }
  int isEof() const { return !isValid; }

  static int cmp_docids(const t_docId *d1, const t_docId *d2);

  //-------------------------------------------------------------------------------------------

  struct CriteriaTester : public IndexCriteriaTester {
    Vector<t_docId> &docIds;

    CriteriaTester(IdListIterator *it);

    int Test(t_docId id);
  };
};

///////////////////////////////////////////////////////////////////////////////////////////////
