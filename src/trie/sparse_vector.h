#ifndef __SPARSEVECTOR_H__
#define __SPARSEVECTOR_H__

#include <stdlib.h>

struct sparseVectorEntry {
    int idx, val;
};

// sparseVector is a crude implementation of a sparse vector for our needs
struct sparseVector {
    size_t len;
    size_t cap;
    sparseVectorEntry entries[];

    sparseVector(size_t cap_);
    sparseVector(int *values, int len_);

    void resize(size_t cap_);
    static size_t sizeof(size_t cap_);
};

// append appends another sparse vector entry with the given index and value.
// NOTE: We do not check
// that an entry with the same index is present in the vector
//@@ What sould we do with double pointer (?)
void sparseVector_append(sparseVector **v, int index, int value);

#endif