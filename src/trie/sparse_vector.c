#include "sparse_vector.h"
#include <stdio.h>
#include "rmalloc.h"

inline size_t __sv_sizeof(size_t cap) {
  return sizeof(sparseVector) + cap * sizeof(sparseVectorEntry);
}

inline sparseVector *__sv_resize(sparseVector *v, size_t cap) {
  v = rm_realloc(v, __sv_sizeof(cap));
  v->cap = cap;
  return v;
}

inline sparseVector *newSparseVectorCap(size_t cap) {
  sparseVector *v = rm_malloc(__sv_sizeof(cap));

  v->cap = cap;
  v->len = 0;
  return v;
}
// newSparseVector creates a new sparse vector with the initial values of the
// dense int slice given to it
sparseVector *newSparseVector(int *values, int len) {
  sparseVector *v = newSparseVectorCap(len * 2);
  v->len = len;

  for (int i = 0; i < len; i++) {
    v->entries[i] = (sparseVectorEntry){i, values[i]};
  }

  return v;
}

// append appends another sparse vector entry with the given index and value.
// NOTE: We do not check
// that an entry with the same index is present in the vector
void sparseVector_append(sparseVector **vp, int index, int value) {
  sparseVector *v = *vp;
  if (v->len == v->cap) {
    v->cap = v->cap ? v->cap * 2 : 1;
    v = __sv_resize(v, v->cap);
  }

  v->entries[v->len++] = (sparseVectorEntry){index, value};
  *vp = v;
}

void sparseVector_free(sparseVector *v) {
  rm_free(v);
}
