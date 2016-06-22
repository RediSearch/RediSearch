#ifndef __SPARSEVECTOR_H__
#define __SPARSEVECTOR_H__

#include <stdlib.h>

typedef struct { 
    int idx, val; 
} sparseVectorEntry;

// sparseVector is a crude implementation of a sparse vector for our needs
typedef struct {
    size_t len;
    size_t cap;
    sparseVectorEntry entries[];
} sparseVector;

size_t __sv_sizeof(size_t cap);

sparseVector *__sv_resize(sparseVector *v, size_t cap);
sparseVector *newSparseVectorCap(size_t cap);

// append appends another sparse vector entry with the given index and value.
// NOTE: We do not check
// that an entry with the same index is present in the vector
void sparseVector_append(sparseVector **v, int index, int value);

// newSparseVector creates a new sparse vector with the initial values of the
// dense int slice given to it
sparseVector *newSparseVector(int *values, int len);

void sparseVector_free(sparseVector *v);
#endif