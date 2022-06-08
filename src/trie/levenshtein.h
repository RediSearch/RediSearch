#pragma once

#include <stdlib.h>
#include "sparse_vector.h"
#include "rmutil/vector.h"
#include "trie.h"

/*
* SparseAutomaton is a C implementation of a levenshtein automaton using
* sparse vectors, as described and implemented here:
* http://julesjacobs.github.io/2015/06/17/disqus-levenshtein-simple-and-fast.html
*
* We then convert the automaton to a simple DFA that is faster to evaluate during the query stage.
* This DFA is used while traversing a Trie to decide where to stop.
*/
struct SparseAutomaton {
    const rune *string;
    size_t len;
    int max;

    /* Create a new Sparse Levenshtein Automaton  for string s and length len, with a maximal edit
    * distance of maxEdits */
    SparseAutomaton(const rune *s, size_t len, int maxEdits) : string(s), len(len), max(maxEdits) {}

    sparseVector *Start();
    sparseVector *Step(sparseVector *state, rune c);

    bool IsMatch(sparseVector *v) const;
    bool CanMatch(sparseVector *v) const;
};

struct dfaEdge;

/* dfaNode is DFA graph node constructed using the Levenshtein automaton */
struct dfaNode {
    int distance;

    int match;
    sparseVector *v;
    struct dfaEdge *edges;
    size_t numEdges;
    struct dfaNode *fallback;

    /* Create a new DFA node */
    dfaNode(int distance, sparseVector *state);
    ~dfaNode();

    /* Recusively build the DFA node and all its descendants */
    void build(SparseAutomaton *a, Vector *cache);

    void putCache(Vector *cache);
    void addEdge(rune r, dfaNode *child);

    dfaNode *getEdge(rune r);

    static dfaNode *getCache(Vector<dfaNode *> *cache, sparseVector *v);
};

struct dfaEdge {
    dfaNode *n;
    rune r;

    dfaEdge(dfaNode *n, rune r) : n(n), r(r) {}
};

/* DFAFilter is a constructed DFA used to filter the traversal on the trie */
struct DFAFilter {
    // a cache of the DFA states, allowing us to re-use the same state whenever we need it
    Vector *cache<dfaNode *>;
    // A stack of the states leading up to the current state
    Vector *stack<dfaNode *>;
    // A stack of the minimal distance for each state, used for prefix matching
    Vector *distStack<int>;
    // whether the filter works in prefix mode or not
    int prefixMode;

    SparseAutomaton a;

    DFAFilter(rune *str, size_t len, int maxDist, int prefixMode);
    ~DFAFilter();
};

/* A callback function for the DFA Filter, passed to the Trie iterator */
FilterCode FilterFunc(rune b, void *ctx, int *matched, void *matchCtx);

/* A stack-pop callback, passed to the trie iterator. It's called when we reach a dead end and need
 * to rewind the stack of the filter */
void StackPop(void *ctx, int numLevels);
