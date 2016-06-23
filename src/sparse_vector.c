#include "sparse_vector.h"
#include <stdio.h>

static sparseVector **vectorPool = NULL;
static size_t vp_len = 0;
static size_t vp_cap = 0;

sparseVector *__svpool_get(size_t cap) {
    if (vp_len == 0) {
      //printf("allocating new one!");
      return malloc(__sv_sizeof(cap)); 
    }
    
    sparseVector *v = vectorPool[--vp_len];
    //printf("getting %p\n", v);
    if (v->cap < cap) {
      v = __sv_resize(v, cap);
    }
    return v;
}

void __svpool_put(sparseVector *v) {
  //printf("%ld, %ld putting %p\n", vp_len, vp_cap, v);
  if (vectorPool == NULL || vp_len >= vp_cap) {
    vp_cap = vp_cap ? vp_cap*2 : 2;
    vectorPool = realloc(vectorPool, vp_cap*sizeof(sparseVector*));
  }
  
  vectorPool[vp_len] = v;
  vp_len++;
}

inline size_t __sv_sizeof(size_t cap) {
  return sizeof(sparseVector) + cap*sizeof(sparseVectorEntry); 
}



inline sparseVector *__sv_resize(sparseVector *v, size_t cap) {

  v = realloc(v, __sv_sizeof(cap));
  v->cap = cap;
  return v;
}

inline sparseVector *newSparseVectorCap(size_t cap) {
   sparseVector *v = __svpool_get(cap);
  
  v->cap = cap;
  v->len = 0;
  return v;
}
// newSparseVector creates a new sparse vector with the initial values of the
// dense int slice given to it
sparseVector *newSparseVector(int *values, int len) {

  sparseVector *v = newSparseVectorCap(len*2);
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
  //ree(v);
  __svpool_put(v);
  
}
