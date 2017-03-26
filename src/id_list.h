#ifndef __ID_LIST_H__
#define __ID_LIST_H__

#include "index_iterator.h"

/* A generic iterator over a pre-sorted list of document ids. This is used by the geo index and the
 * id filter. */
typedef struct {
  t_docId *docIds;
  t_docId lastDocId;
  t_offset size;
  t_offset offset;
  int atEOF;
  RSIndexResult *res;
} IdListIterator;

/* Create a new IdListIterator from a pre populated list of document ids of size num. The doc ids
 * are sorted in this function, so there is no need to sort them. They are automatically freed in
 * the end and assumed to be allocated using rm_malloc */
IndexIterator *NewIdListIterator(t_docId *ids, t_offset num);

#endif