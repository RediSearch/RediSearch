#ifndef __LEVENSHTEIN_H__
#define __LEVENSHTEIN_H__

#include <stdlib.h>
#include "sparse_vector.h"
#include "rmutil/vector.h"
#include "trie.h"

// SparseAutomaton is a naive Go implementation of a levenshtein automaton using
// sparse vectors, as described
// and implemented here:
// http://julesjacobs.github.io/2015/06/17/disqus-levenshtein-simple-and-fast.html
typedef struct {
    const char *string;
    size_t len;
    int max;
} SparseAutomaton;

typedef struct dfaNode {
    int distance;
    sparseVector *v;
    struct dfaNode *edges[255];
    struct dfaNode *fallback;
} dfaNode;

dfaNode *__newDfaNode(int distance, sparseVector *state);

void dfa_build(dfaNode *parent, SparseAutomaton *a, Vector *cache);

SparseAutomaton NewSparseAutomaton(const char *s, size_t len, int maxEdits);

sparseVector *SparseAutomaton_Start(SparseAutomaton *a);

sparseVector *SparseAutomaton_Step(SparseAutomaton *a, sparseVector *state, char c);

int SparseAutomaton_IsMatch(SparseAutomaton *a, sparseVector *v);

int SparseAutomaton_CanMatch(SparseAutomaton *a, sparseVector *v);

typedef struct {
    Vector *cache;
    Vector *stack;
} FilterCtx;

FilterCtx NewFilterCtx(char *str, size_t len, int maxDist);
FilterCode FilterFunc(unsigned char b, void *ctx, int *matched);
void FilterCtx_Free(FilterCtx *fc);

#endif