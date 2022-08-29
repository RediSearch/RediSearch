#pragma once

#include "object.h"
#include "rmutil/vector.h"

#include <stdlib.h>

///////////////////////////////////////////////////////////////////////////////////////////////

struct SparseVectorEntry {
  int idx, val;
  SparseVectorEntry(int idx = 0, int val = 0) : idx(idx), val(val) {}
};

//---------------------------------------------------------------------------------------------

// sparseVector is a crude implementation of a sparse vector for our needs

struct SparseVector : Vector<SparseVectorEntry> {
  typedef Vector<SparseVectorEntry> Super;
  typedef SparseVectorEntry Entry;

  SparseVector(size_t size = 0) : Super(size) {}
  SparseVector(int *values, int len);
  SparseVector(SparseVector &&v) : Super(v) {}

  bool operator==(const SparseVector &v) const;

  // append appends another sparse vector entry with the given index and value.
  // NOTE: We do not check that an entry with the same index is present in the vector
  void append(int index, int value);
};

///////////////////////////////////////////////////////////////////////////////////////////////
