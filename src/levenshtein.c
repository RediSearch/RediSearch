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

    if (state->len > 0 && state->entries[0].idx == 0 && state->entries[0].val < a->max) {
        sparseVector_append(&newVec, 0, state->entries[0].val + 1);
    }

    for (int j = 0; j < state->len; j++) {
        sparseVectorEntry entry = state->entries[j];

        if (entry.idx == a->len) {
            break;
        }

        int cost = 0;
        if (a->string[entry.idx] != c) {
            cost = 1;
        }

        int val = state->entries[j].val + cost;

        if (newVec->len != 0 && newVec->entries[newVec->len - 1].idx == entry.idx) {
            val = MIN(val, newVec->entries[newVec->len - 1].val + 1);
        }

        if (state->len > j + 1 && state->entries[j + 1].idx == entry.idx + 1) {
            val = MIN(val, state->entries[j + 1].val + 1);
        }

        if (val <= a->max) {
            sparseVector_append(&newVec, entry.idx + 1, val);
        }
    }
    return newVec;
}

// IsMatch returns true if the current state vector represents a string that is
// within the max
// edit distance from the initial automaton string
int SparseAutomaton_IsMatch(SparseAutomaton *a, sparseVector *v) {
    return v->len != 0 && v->entries[v->len - 1].idx == a->len;
}

// CanMatch returns true if there is a possibility that feeding the automaton
// with more steps will
// yield a match. Once CanMatch is false there is no point in continuing
// iteration
int SparseAutomaton_CanMatch(SparseAutomaton *a, sparseVector *v) { return v->len > 0; }
