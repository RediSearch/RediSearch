#include <stdlib.h>
#include <stdio.h>
#include <sys/param.h>
#include <string.h>
#include "levenshtein.h"

// NewSparseAutomaton creates a new automaton for the string s, with a given max
// edit distance check
SparseAutomaton NewSparseAutomaton(const char *s, size_t len, int maxEdits) {
    return (SparseAutomaton){s, len, maxEdits};
}

// Start initializes the automaton's state vector and returns it for further
// iteration
sparseVector *SparseAutomaton_Start(SparseAutomaton *a) {
    int vals[a->max + 1];
    for (int i = 0; i < a->max + 1; i++) {
        vals[i] = i;
    }

    return newSparseVector(vals, a->max + 1);
}

// Step returns the next state of the automaton given a previous state and a
// character to check
sparseVector *SparseAutomaton_Step(SparseAutomaton *a, sparseVector *state, char c) {
    sparseVector *newVec = newSparseVectorCap(state->len);

    sparseVectorEntry e = state->entries[0];
    if (state->len > 0 && e.idx == 0 && e.val < a->max) {
        sparseVector_append(&newVec, 0, e.val + 1);
    }

    for (int j = 0; j < state->len; j++) {
        sparseVectorEntry *entry = &state->entries[j];

        if (entry->idx == a->len) {
            break;
        }

        register int val = state->entries[j].val;
        if (a->string[entry->idx] != c) ++val;

        if (newVec->len && newVec->entries[newVec->len - 1].idx == entry->idx) {
            val = MIN(val, newVec->entries[newVec->len - 1].val + 1);
        }

        if (state->len > j + 1 && state->entries[j + 1].idx == entry->idx + 1) {
            val = MIN(val, state->entries[j + 1].val + 1);
        }

        if (val <= a->max) {
            sparseVector_append(&newVec, entry->idx + 1, val);
        }
    }
    return newVec;
}

// IsMatch returns true if the current state vector represents a string that is
// within the max
// edit distance from the initial automaton string
inline int SparseAutomaton_IsMatch(SparseAutomaton *a, sparseVector *v) {
    return v->len != 0 && v->entries[v->len - 1].idx == a->len;
}

// CanMatch returns true if there is a possibility that feeding the automaton
// with more steps will
// yield a match. Once CanMatch is false there is no point in continuing
// iteration
inline int SparseAutomaton_CanMatch(SparseAutomaton *a, sparseVector *v) { return v->len > 0; }

dfaNode *__newDfaNode(int distance, sparseVector *state) {
    dfaNode *ret = calloc(1, sizeof(dfaNode));
    ret->fallback = NULL;
    ret->distance = 0;
    ret->v = state;
    return ret;
}

inline int __sv_equals(sparseVector *sv1, sparseVector *sv2) {
    if (sv1->len != sv2->len) return 0;

    for (int i = 0; i < sv1->len; i++) {
        if (sv1->entries[i].idx != sv2->entries[i].idx ||
            sv1->entries[i].val != sv2->entries[i].val) {
            return 0;
        }
    }

    return 1;
}

dfaNode *__dfn_getCache(Vector *cache, sparseVector *v) {
    size_t n = Vector_Size(cache);
    for (int i = 0; i < n; i++) {
        dfaNode *dfn;
        Vector_Get(cache, i, &dfn);

        if (__sv_equals(v, dfn->v)) {
            return dfn;
        }
    }
    return NULL;
}

void __dfn_putCache(Vector *cache, dfaNode *dfn) { Vector_Push(cache, dfn); }

static cunt = 0;

void dfa_build(dfaNode *parent, SparseAutomaton *a, Vector *cache) {
    printf("cunt: %d\n", ++cunt);
    if (SparseAutomaton_IsMatch(a, parent->v)) {
        printf("MATCH!\n");
        parent->distance = -1;
        return;
    }
    for (int i = 0; i < parent->v->len; i++) {
        if (parent->v->entries[i].idx < a->len) {
            int c = a->string[parent->v->entries[i].idx];

            // if (parent->edges[c] == NULL) {
            sparseVector *nv = SparseAutomaton_Step(a, parent->v, c);
            for (int j = 0; j < nv->len; j++) {
                printf("%d: %d,%d (%c)\n", j, nv->entries[j].idx, nv->entries[j].val,
                       a->string[nv->entries[j].idx]);
            }
            if (nv->len > 0) {
                dfaNode *dfn = __dfn_getCache(cache, nv);
                if (dfn == NULL) {
                    int dist = nv->entries[nv->len - 1].val;

                    parent->edges[c] = __newDfaNode(dist, nv);
                    printf("edge %s, %c - dist %d\n", a->string, c, dist);
                    __dfn_putCache(cache, parent->edges[c]);
                    dfa_build(parent->edges[c], a, cache);
                } else {
                    parent->edges[c] = dfn;
                }
            } else {
                printf("no more match\n");
            }
        }
    }
    sparseVector *nv = SparseAutomaton_Step(a, parent->v, '*');

    if (nv->len > 0) {
        dfaNode *dfn = __dfn_getCache(cache, nv);
        if (dfn) {
            parent->fallback = dfn;
        } else {
            int dist = nv->entries[nv->len - 1].val;
            printf("DEFAULT EDGE! edge %s - dist %d\n", a->string, dist);
            parent->fallback = __newDfaNode(dist, nv);
            __dfn_putCache(cache, parent->fallback);
            dfa_build(parent->fallback, a, cache);
        }
    } else
        printf("default edge out of range\n");
}