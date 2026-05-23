/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#ifndef __LEVENSHTEIN_AUTOMATON_H__
#define __LEVENSHTEIN_AUTOMATON_H__

#include <stdlib.h>

#include "sparse_vector.h"
#include "rmutil/vector.h"
#include "trie_node.h"

/*
 * Sparse Levenshtein automaton: SparseAutomaton (NFA over sparse vectors)
 * compiled into a dfaNode graph via dfa_build. A DFA over query Q and
 * threshold k that accepts string S iff Levenshtein(Q, S) <= k.
 *
 * The trie-walk filter layer that uses this automaton lives in dfa_filter.h.
 */

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

/* Create a new DFA node */
dfaNode *dfaNode_new(int distance, sparseVector *state);

/* Free a DFA node and its owned state vector and edges */
void dfaNode_free(dfaNode *d);

/* Get an edge for a dfa node given the next rune */
dfaNode *dfaNode_getEdge(dfaNode *n, rune r);

/* Add a built dfa node to the cache so it can be reused for equivalent state vectors */
void dfaCache_put(Vector *cache, dfaNode *dfn);

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

#endif
