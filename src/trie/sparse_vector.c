/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#include "sparse_vector.h"

#include "rmalloc.h"

inline size_t sv_sizeof(size_t cap) {
  return sizeof(sparseVector) + cap * sizeof(sparseVectorEntry);
}

inline sparseVector *sv_resize(sparseVector *v, size_t cap) {
  v = rm_realloc(v, sv_sizeof(cap));
  v->cap = cap;
  return v;
}

inline sparseVector *newSparseVectorCap(size_t cap) {
  sparseVector *v = rm_malloc(sv_sizeof(cap));

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
    v = sv_resize(v, v->cap);
  }

  v->entries[v->len++] = (sparseVectorEntry){index, value};
  *vp = v;
}

void sparseVector_free(sparseVector *v) {
  rm_free(v);
}

// sv_equals returns 1 iff sv1 and sv2 have identical (idx, val) entry
// sequences. The cap field is ignored — it is allocation bookkeeping,
// not part of the vector's value.
int sv_equals(sparseVector *sv1, sparseVector *sv2) {
  if (sv1->len != sv2->len) return 0;

  for (int i = 0; i < sv1->len; i++) {
    if (sv1->entries[i].idx != sv2->entries[i].idx || sv1->entries[i].val != sv2->entries[i].val) {
      return 0;
    }
  }

  return 1;
}
