
#pragma once

#include "id_list.h"

///////////////////////////////////////////////////////////////////////////////////////////////

bool IdListIterator::CriteriaTester::Test(t_docId id) {
  return binary_search(docIds.begin(), docIds.end(), id);
}

IdListIterator::CriteriaTester::CriteriaTester(IdListIterator *it) {
  //@@
  // docIds = rm_malloc(sizeof(t_docId) * size);
  // memcpy(docIds, docIds, size);
  // size = it->size;
  docIds = std::move(it->docIds);
}

///////////////////////////////////////////////////////////////////////////////////////////////

size_t IdListIterator::NumEstimated() const {
  return size;
}

//---------------------------------------------------------------------------------------------

// Read the next entry from the iterator, into hit *e.
// Returns INDEXREAD_EOF if at the end */

int IdListIterator::Read(IndexResult **r) {
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

//---------------------------------------------------------------------------------------------

void IdListIterator::Abort() {
  isValid = 0;
}

//---------------------------------------------------------------------------------------------

// Skip to a docid, potentially reading the entry into hit, if the docId matches

int IdListIterator::SkipTo(t_docId docId, IndexResult **r) {
  if (isEof() || offset >= size) {
    return INDEXREAD_EOF;
  }

  if (docId > docIds[size - 1]) {
    isValid = 0;
    return INDEXREAD_EOF;
  }

  t_offset top = t_offset{size - 1}, bottom = offset;
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

//---------------------------------------------------------------------------------------------

// the last docId read

t_docId IdListIterator::LastDocId() const {
  return lastDocId;
}

//---------------------------------------------------------------------------------------------

// release the iterator's context and free everything needed

IdListIterator::~IdListIterator() {
  delete current;
}

//---------------------------------------------------------------------------------------------

// Return the number of results in this iterator. Used by the query execution on the top iterator

size_t IdListIterator::Len() const {
  return size;
}

//---------------------------------------------------------------------------------------------

void IdListIterator::Rewind() {
  setEof(0);
  lastDocId = 0;
  current->docId = 0;
  offset = 0;
}

//---------------------------------------------------------------------------------------------

// Create a new IdListIterator from a pre populated list of document ids of size num. The doc ids
// are sorted in this function, so there is no need to sort them. They are automatically freed in
// the end and assumed to be allocated using rm_malloc

//@@@ TODO: fix this
IdListIterator::IdListIterator(Vector<t_docId> &ids, double weight) {
  // first sort the ids, so the caller will not have to deal with it
  std::sort(ids.begin(), ids.end(), [](const t_docId &a, const t_docId &b) {
    return a > b;
  });

  setEof(0);
  lastDocId = 0;
  current = new VirtualResult(weight);
  current->fieldMask = RS_FIELDMASK_ALL;
  offset = 0;
  mode = IndexIteratorMode::Sorted;
}

///////////////////////////////////////////////////////////////////////////////////////////////
