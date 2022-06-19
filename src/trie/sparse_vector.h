#pragma once

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

    static bool equals(sparseVector *sv1, sparseVector *sv2);

    // append appends another sparse vector entry with the given index and value.
    // NOTE: We do not check
    // that an entry with the same index is present in the vector
    void append(int index, int value);

};
