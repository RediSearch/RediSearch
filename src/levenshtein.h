#ifndef __LEVENSHTEIN_H__
#define __LEVENSHTEIN_H__

#include <stdlib.h>
#include "sparse_vector.h"

// SparseAutomaton is a naive Go implementation of a levenshtein automaton using
// sparse vectors, as described
// and implemented here:
// http://julesjacobs.github.io/2015/06/17/disqus-levenshtein-simple-and-fast.html
typedef struct {
    const char *string;
    size_t len;
    int max;
} SparseAutomaton;

SparseAutomaton NewSparseAutomaton(const char *s, size_t len, int maxEdits);

sparseVector *SparseAutomaton_Start(SparseAutomaton *a);

sparseVector *SparseAutomaton_Step(SparseAutomaton *a, sparseVector *state, char c);

int SparseAutomaton_IsMatch(SparseAutomaton *a, sparseVector *v);

int SparseAutomaton_CanMatch(SparseAutomaton *a, sparseVector *v);

#endif