#ifndef __LEVENSHTEIN_H__
#define __LEVENSHTEIN_H__

#include <stdlib.h>
#include "sparse_vector.h"
#include "../rmutil/vector.h"
#include "trie.h"

/*
* SparseAutomaton is a C implementation of a levenshtein automaton using
* sparse vectors, as described and implemented here:
* http://julesjacobs.github.io/2015/06/17/disqus-levenshtein-simple-and-fast.html
*
* We then convert the automaton to a simple DFA that is faster to evaluate during the query stage.
* This DFA is used while traversing a Trie to decide where to stop.
*/
typedef struct {
    const rune *string;
    size_t len;
    int max;
} SparseAutomaton;

struct dfaEdge; 

/* dfaNode is DFA graph node constructed using the Levenshtein automaton */
typedef struct dfaNode {
    int distance;

    int match;
    sparseVector *v;
    struct dfaEdge *edges;
    size_t numEdges;
    struct dfaNode *fallback;
} dfaNode;

typedef struct dfaEdge {
    dfaNode *n;
    rune r;
} dfaEdge;

/* Get an edge for a dfa node given the next rune */
dfaNode *__dfn_getEdge(dfaNode *n, rune r);


/* Create a new DFA node */
dfaNode *__newDfaNode(int distance, sparseVector *state);

/* Recusively build the DFA node and all its descendants */
void dfa_build(dfaNode *parent, SparseAutomaton *a, Vector *cache);

/* Create a new Sparse Levenshtein Automaton  for string s and length len, with a maximal edit
 * distance of maxEdits */
SparseAutomaton NewSparseAutomaton(const rune *s, size_t len, int maxEdits);

/* Create the initial state vector of the root automaton node */
sparseVector *SparseAutomaton_Start(SparseAutomaton *a);

/* Step from a given state of the automaton to the next step given a specific character */
sparseVector *SparseAutomaton_Step(SparseAutomaton *a, sparseVector *state, rune c);

/* Is the current state of the automaton a match for the query? */
int SparseAutomaton_IsMatch(SparseAutomaton *a, sparseVector *v);

/* Can the current state lead to a possible match, or is this a dead end? */
int SparseAutomaton_CanMatch(SparseAutomaton *a, sparseVector *v);

/* DFAFilter is a constructed DFA used to filter the traversal on the trie */
typedef struct {
    // a cache of the DFA states, allowing us to re-use the same state whenever we need it
    Vector *cache;
    // A stack of the states leading up to the current state
    Vector *stack;
    // A stack of the minimal distance for each state, used for prefix matching
    Vector *distStack;
    // whether the filter works in prefix mode or not
    int prefixMode;

    SparseAutomaton a;
} DFAFilter;

/* Create a new DFA filter  using a Levenshtein automaton, for the given string  and maximum
 * distance. If prefixMode is 1, we match prefixes within the given distance, and then continue
 * onwards to all suffixes. */
DFAFilter NewDFAFilter(rune *str, size_t len, int maxDist, int prefixMode);

/* A callback function for the DFA Filter, passed to the Trie iterator */
FilterCode FilterFunc(rune b, void *ctx, int *matched, void *matchCtx);

/* A stack-pop callback, passed to the trie iterator. It's called when we reach a dead end and need
 * to rewind the stack of the filter */
void StackPop(void *ctx, int numLevels);

/* Free the underlying data of the DFA Filter. Note that since DFAFilter is created on the stack, it
 * is not freed by itself. */
void DFAFilter_Free(DFAFilter *fc);

#endif