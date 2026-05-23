/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#ifndef __DFA_FILTER_H__
#define __DFA_FILTER_H__

#include <stdlib.h>

#include "levenshtein_automaton.h"
#include "rmutil/vector.h"
#include "trie_node.h"

/*
 * Trie-walk filter built on top of the Levenshtein automaton in
 * levenshtein_automaton.h. The automaton is used as a building block to
 * compute prefix edit distance (PED) — the metric reported through `matchCtx`:
 *     PED(Q, T) = min over k in [0..|T|] of Levenshtein(Q, T[0..k])
 * See `FilterFunc` docs (dfa_filter.c) for details and worked examples. PED is
 * the standard scoring metric for approximate autocomplete and the basis of
 * FT.SUGGET FUZZY ranking.
 *
 * Admission criteria differ by mode (DFAFilter.prefixMode):
 *   - non-prefix: term T is admitted iff Lev(Q, T) <= maxDist.
 *   - prefix:     term T is admitted iff PED(Q, T) <= maxDist (some prefix
 *                 of T is within maxDist of Q; the tail is unconstrained).
 * *matchCtx reports PED in both modes; only in prefix mode does PED also
 * gate admission. This is why the prefix example in FilterFunc can yield
 * a term whose Lev(query, term) exceeds maxDist.
 */

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
 * Computes prefix edit distance (see file header) for the yielded term and
 * writes it through `matchCtx` (typed `int *`). Mechanics:
 *
 *   - The DFAFilter keeps a `distStack` parallel to its DFA state stack,
 *     each entry holding the running minimum of accept-state costs along
 *     the current DFA path. Both stacks pop in sync on backtrack
 *     (`StackPop`).
 *   - On each rune step that reaches an accept state, *matchCtx is set to
 *     `MIN(state->distance, running_min_so_far)`.
 *   - In prefix mode, once a prefix has accepted, the filter pushes NULL
 *     state-stack frames for the remaining runes; distStack carries the
 *     running minimum forward unchanged, so *matchCtx at yield time
 *     reflects the cost at the accept boundary.
 *
 * Examples (maxDist=2):
 *   non-prefix: query "ab", term "abc" → *matchCtx=0 (exact accept at "ab"),
 *               Levenshtein("ab","abc")=1.
 *   prefix:     query "abc", term "abzzz" → *matchCtx=1 (cost at "ab" accept,
 *               carried across NULL frames), Levenshtein("abc","abzzz")=4.
 *
 * FT.SUGGET FUZZY ranks results by `score *= exp(-2 * dist)` using this
 * metric. */
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
