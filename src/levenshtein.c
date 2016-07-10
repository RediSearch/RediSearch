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
    return v->len && v->entries[v->len - 1].idx == a->len;
}

// CanMatch returns true if there is a possibility that feeding the automaton
// with more steps will
// yield a match. Once CanMatch is false there is no point in continuing
// iteration
inline int SparseAutomaton_CanMatch(SparseAutomaton *a, sparseVector *v) { return v->len > 0; }

dfaNode *__newDfaNode(int distance, sparseVector *state) {
    dfaNode *ret = calloc(1, sizeof(dfaNode));
    ret->fallback = NULL;
    ret->distance = distance;
    ret->v = state;
    // memset(ret->edges, 0, 255 * sizeof(dfaNode *));
    return ret;
}

void __dfaNode_free(dfaNode *d) {
    sparseVector_free(d->v);

    free(d);
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

void dfa_build(dfaNode *parent, SparseAutomaton *a, Vector *cache) {
    // printf("building dfa node dist %d\n", parent->distance);
    if (SparseAutomaton_IsMatch(a, parent->v)) {
        parent->distance = -1;
    }
    for (int i = 0; i < parent->v->len; i++) {
        if (parent->v->entries[i].idx < a->len) {
            int c = a->string[parent->v->entries[i].idx];
            // printf("%c ---> ", c);
            if (parent->edges[c] == NULL) {
                sparseVector *nv = SparseAutomaton_Step(a, parent->v, c);
                if (nv->len > 0) {
                    dfaNode *dfn = __dfn_getCache(cache, nv);
                    if (dfn == NULL) {
                        int dist = nv->entries[nv->len - 1].val;

                        parent->edges[c] = __newDfaNode(dist, nv);
                        // printf("edge %s, %c - dist %d\n", a->string, c, dist);
                        __dfn_putCache(cache, parent->edges[c]);
                        dfa_build(parent->edges[c], a, cache);
                        continue;
                    } else {
                        parent->edges[c] = dfn;
                    }
                }
                sparseVector_free(nv);
            }
        }
    }

    // if (parent->distance < a->max) {
    sparseVector *nv = SparseAutomaton_Step(a, parent->v, 1);

    if (nv->len > 0) {
        dfaNode *dfn = __dfn_getCache(cache, nv);
        if (dfn) {
            parent->fallback = dfn;
        } else {
            int dist = nv->entries[nv->len - 1].val;
            // printf("DEFAULT EDGE! edge %s - dist %d\n", a->string, dist);
            parent->fallback = __newDfaNode(dist, nv);
            __dfn_putCache(cache, parent->fallback);
            dfa_build(parent->fallback, a, cache);
            return;
        }
    }
    sparseVector_free(nv);

    //}
}

FilterCtx NewFilterCtx(char *str, size_t len, int maxDist) {
    Vector *cache = NewVector(dfaNode *, 8);

    SparseAutomaton a = NewSparseAutomaton(str, len, maxDist);

    sparseVector *v = SparseAutomaton_Start(&a);
    dfaNode *dr = __newDfaNode(0, v);
    __dfn_putCache(cache, dr);
    dfa_build(dr, &a, cache);

    FilterCtx ret;
    ret.cache = cache;
    ret.stack = NewVector(dfaNode *, 8);
    ret.a = a;
    Vector_Push(ret.stack, dr);

    return ret;
}

void FilterCtx_Free(FilterCtx *fc) {
    for (int i = 0; i < Vector_Size(fc->cache); i++) {
        dfaNode *n = NULL;
        Vector_Get(fc->cache, i, &n);

        if (n) __dfaNode_free(n);
    }

    Vector_Free(fc->cache);
    Vector_Free(fc->stack);
}

FilterCode FilterFunc(unsigned char b, void *ctx, int *matched) {
    FilterCtx *fc = ctx;
    dfaNode *dn;

    Vector_Get(fc->stack, Vector_Size(fc->stack) - 1, &dn);

    // printf("offset %d, len %d dist %d\n", Vector_Size(fc->stack), fc->a.len, dn->distance);
    *matched =
        dn->distance == -1 || Vector_Size(fc->stack) + (fc->a.max - dn->distance) >= fc->a.len;

    dfaNode *next = dn->edges[b] ? dn->edges[b] : dn->fallback;
    // we can continue - push the state on the stack
    if (next) {
        Vector_Push(fc->stack, next);
        return F_CONTINUE;
    }

    return F_STOP;
}

void StackPop(void *ctx, int numLevels) {
    FilterCtx *fc = ctx;

    for (int i = 0; i < numLevels; i++) Vector_Pop(fc->stack, NULL);
}