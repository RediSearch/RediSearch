
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

  virtual RSIndexResult *GetCurrent();
  virtual size_t NumEstimated();
  virtual IndexCriteriaTester *GetCriteriaTester();
  virtual int Read(RSIndexResult **e);
  virtual int SkipTo(t_docId docId, RSIndexResult **hit);
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

int ILCriteriaTester::Test(t_docId id) {
  return bsearch((void *)id, docIds, (size_t)size, sizeof(t_docId), IdListIterator::cmp_docids) != NULL;
}

ILCriteriaTester::~ILCriteriaTester() {
  rm_free(docIds);
}

ILCriteriaTester::ILCriteriaTester(IdListIterator *it) {
  docIds = rm_malloc(sizeof(t_docId) * size);
  memcpy(docIds, docIds, size);
  size = it->size;
}

//---------------------------------------------------------------------------------------------

size_t IdListIterator::NumEstimated() {
  return size;
}

// Read the next entry from the iterator, into hit *e.
// Returns INDEXREAD_EOF if at the end */
int IdListIterator::Read(RSIndexResult **r) {
  if (isEof() || offset >= size) {
    setEof(1);
    return INDEXREAD_EOF;
  }

  lastDocId = docIds[offset++];

  // TODO: Filter here
  current->docId = lastDocId;
  *r = current;
  return INDEXREAD_OK;
}

void IdListIterator::Abort() {
  isValid = 0;
}

// Skip to a docid, potentially reading the entry into hit, if the docId matches
int IdListIterator::SkipTo(t_docId docId, RSIndexResult **r) {
  if (isEof() || offset >= size) {
    return INDEXREAD_EOF;
  }

  if (docId > docIds[size - 1]) {
    isValid = 0;
    return INDEXREAD_EOF;
  }

  t_offset top = size - 1, bottom = offset;
  t_offset i = bottom;

  while (bottom <= top) {
    t_docId did = docIds[i];

    if (did == docId) {
      break;
    }
    if (docId < did) {
      if (i == 0) break;
      top = i - 1;
    } else {
      bottom = i + 1;
    }
    i = (bottom + top) / 2;
  }
  offset = i + 1;
  if (offset >= size) {
    setEof(1);
  }

  lastDocId = docIds[i];
  current->docId = lastDocId;

  *r = current;

  if (lastDocId == docId) {
    return INDEXREAD_OK;
  }
  return INDEXREAD_NOTFOUND;
}

// the last docId read
t_docId IdListIterator::LastDocId() {
  return lastDocId;
}

// release the iterator's context and free everything needed
IdListIterator::~IdListIterator() {
  delete current;
  if (docIds) {
    rm_free(docIds);
  }
}

// Return the number of results in this iterator. Used by the query execution on the top iterator
size_t IdListIterator::Len() {
  return size;
}

static int IdListIterator::cmp_docids(const t_docId *d1, const t_docId *d2) {
  return (int)(*d1 - *d2);
}

void IdListIterator::Rewind() {
  setEof(0);
  lastDocId = 0;
  current->docId = 0;
  offset = 0;
}

IdListIterator::IdListIterator(t_docId *ids, t_offset num, double weight) {
  // first sort the ids, so the caller will not have to deal with it
  qsort(ids, (size_t)num, sizeof(t_docId), (int (*)(const void *, const void*)) cmp_docids);

  size = num;
  docIds = rm_calloc(num, sizeof(t_docId));
  if (num > 0) memcpy(docIds, ids, num * sizeof(t_docId));
  setEof(0);
  lastDocId = 0;
  current = NewVirtualResult(weight);
  current->fieldMask = RS_FIELDMASK_ALL;

  offset = 0;

  mode = Mode::Sorted;
}

///////////////////////////////////////////////////////////////////////////////////////////////
