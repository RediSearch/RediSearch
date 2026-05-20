/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#ifndef __LEVENSHTEIN_H__
#define __LEVENSHTEIN_H__

#include <stdlib.h>

#include "sparse_vector.h"
#include "rmutil/vector.h"
#include "trie_node.h"

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

/* Recursively build the DFA node and all its descendants */
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
    // a cache of the DFA states, allowing us to reuse the same state whenever we need it
    Vector *cache;
    // A stack of the states leading up to the current state
    Vector *stack;
    // A stack tracking, per DFA state on `stack`, the running minimum of
    // accept-state distances along the path leading to it. Used to report the
    // cost of the best prefix match seen so far via matchCtx in FilterFunc.
    Vector *distStack;
    // whether the filter works in prefix mode or not
    int prefixMode;

    SparseAutomaton a;
} DFAFilter;

/* Create a new DFA filter  using a Levenshtein automaton, for the given string  and maximum
 * distance. If prefixMode is 1, we match prefixes within the given distance, and then continue
 * onwards to all suffixes. */
DFAFilter *NewDFAFilter(rune *str, size_t len, int maxDist, int prefixMode);

/* A callback function for the DFA Filter, passed to the Trie iterator.
 *
 * `matchCtx` is an `int *`. On every accept state reached along the DFA path,
 * the filter writes `MIN(state->distance, running_min_so_far)` into *matchCtx,
 * where running_min_so_far is tracked on a parallel `distStack` that mirrors
 * the DFA state stack and pops in sync with it on backtrack. At yield time
 * *matchCtx therefore holds the minimum accept cost reached on the DFA path
 * leading to the yielded term — i.e. the edit distance of the best query
 * match against any consumed prefix of the term. In prefix mode, once the
 * prefix has been accepted, distStack carries this value forward across the
 * remaining runes so *matchCtx reflects the cost at the accept boundary.
 *
 * NOTE: This is NOT Levenshtein(query, yielded_term) in the general case;
 * it is the minimum cost reached among all accept states encountered along
 * the consumed-input prefix of the term. For prefix-mode FUZZY ranking this
 * is the desired metric (FT.SUGGET FUZZY scoring depends on it via
 * `score *= exp(-2 * dist)`). */
// FilterCode FilterFunc(rune b, void *ctx, int *matched, void *matchCtx);
FilterCode FoldingFilterFunc(rune b, void *ctx, int *matched, void *matchCtx);
FilterCode LoweringFilterFunc(rune b, void *ctx, int *matched, void *matchCtx);

/* A stack-pop callback, passed to the trie iterator. It's called when we reach a dead end and need
 * to rewind the stack of the filter */
void StackPop(void *ctx, int numLevels);

/* Free the underlying data of the DFA Filter. Note that since DFAFilter is created on the stack, it
 * is not freed by itself. */
void DFAFilter_Free(DFAFilter *fc);

#endif
