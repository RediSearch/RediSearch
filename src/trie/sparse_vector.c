#include "sparse_vector.h"
#include <stdio.h>
#include "rmalloc.h"

static inline size_t sparseVector::sizeof(size_t cap_) {
  return sizeof(sparseVector) + cap_ * sizeof(sparseVectorEntry);
}

inline void sparseVector::resize(size_t cap_) {
  this = rm_realloc(this, sizeof(cap_));
  cap = cap_;
}

sparseVector::sparseVector(size_t cap_) {
  cap = cap_;
  len = 0;
}

// creates a new sparse vector with the initial values of the
// dense int slice given to it
sparseVector::sparseVector(int *values, int len_) {
  cap = len_ * 2;
  len = len_;

  for (int i = 0; i < len_; i++) {
    entries[i] = (sparseVectorEntry){i, values[i]};
  }
}

// append appends another sparse vector entry with the given index and value.
// NOTE: We do not check
// that an entry with the same index is present in the vector
void sparseVector_append(sparseVector **vp, int index, int value) {
  sparseVector *v = *vp;
  if (v->len == v->cap) {
    v->cap = v->cap ? v->cap * 2 : 1;
    v = v->resize(v->cap);
  }

  v->entries[v->len++] = (sparseVectorEntry){index, value};
  *vp = v;
}
