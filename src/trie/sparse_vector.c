#include "sparse_vector.h"
#include <stdio.h>
#include "rmalloc.h"

///////////////////////////////////////////////////////////////////////////////////////////////

// creates a new sparse vector with the initial values of the dense int slice given to it

SparseVector::SparseVector(int *values, int len) : Super() {
  for (int i = 0; i < len; i++) {
    emplace_back(i, values[i]);
  }
}

//---------------------------------------------------------------------------------------------

// append appends another sparse vector entry with the given index and value.
// NOTE: We do not check an entry with the same index is present in the vector

void SparseVector::append(int index, int value) {
  emplace_back(index, value);
}

//---------------------------------------------------------------------------------------------

bool SparseVector::operator==(const SparseVector &v) const {
  if (size() != v.size()) return false;

  SparseVector::const_iterator j = v.begin();
  for (auto &entry: *this) {
    if (entry.idx != j->idx || entry.val != j->val) {
      return false;
    }
  }

  return true;
}

///////////////////////////////////////////////////////////////////////////////////////////////
