#include "trie.h"
#include "levenshtein.h"
#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <unistd.h>
#include <sys/time.h>
#include <sys/param.h>
#include <time.h>

void *stepFilter(char b, void *ctx, void *stackCtx) {
    SparseAutomaton *a = ctx;
    dfaNode *dn = stackCtx;
    unsigned char c = b;
    if (SparseAutomaton_IsMatch(a, dn->v)) {
        // printf("Match!");
        return NULL;
    }
    return dn->edges[c];
    // // if (!SparseAutomaton_CanMatch(a,v)) {

    // //     return NULL;
    // // }
    // sparseVector *nv = SparseAutomaton_Step(a, v, b);

    // // we should continue
    // if (SparseAutomaton_CanMatch(a, nv)) {
    //     return nv;
    // }
    // sparseVector_free(nv);
    // return NULL;
}

int testTrie() {
    printf("%ld\n", sizeof(sparseVector));
    // char *str;
    //     t_len len;
    //     t_len numChildren;
    //     struct t_node *children;
    //     void *value;
    TrieNode *root = __newTrieNode("root", 0, 4, 0, 1);

    root = Trie_Add(root, "hello", 5, 1);
    root = Trie_Add(root, "help", 4, 2);

    root = Trie_Add(root, "helter skelter", 14, 3);
    printf("find: %f\n", Trie_Find(root, "helter skelter", 14));
    root = Trie_Add(root, "heltar skelter", 14, 4);
    root = Trie_Add(root, "helter shelter", 14, 5);
    root = Trie_Add(root, "helter skelter", 14, 6);

    printf("find: %f\n", Trie_Find(root, "helter skelter", 14));

    const char *term = "helo";
    SparseAutomaton a = NewSparseAutomaton(term, strlen(term), 2);
    sparseVector *v = SparseAutomaton_Start(&a);
    TrieIterator *it = Trie_Iterate(root, stepFilter, &a, v);
    char *s;
    t_len len;
    float score;

    while (TrieIterator_Next(it, &s, &len, &score)) {
        printf("Found %.*s -> %f\n", len, s, score);
    }

    // Trie_Free(root);
    return 0;
}

int testWithData() {
    FILE *fp = fopen("../titles.csv", "r");
    assert(fp != NULL);

    char *line = NULL;
    size_t len = 0;
    ssize_t read;
    TrieNode *root = __newTrieNode("root", 0, 4, 0, 1);
    int i = 0;
    while ((read = getline(&line, &len, fp)) != -1) {
        char *sep = strchr(line, ',');
        if (!sep) continue;

        *sep = 0;
        double score = atof(sep + 1);
        // if (i % 10 == 0)
        root = Trie_Add(root, line, strlen(line), (float)score);

        i++;

        // if (i > 500000) {
        //     break;
        // }

        // printf("%s => %d\n", line, score);
    }

    printf("loaded %d entries\n", i);

    char *terms[] = {"united states", NULL};
    struct timespec start_time, end_time;
    // for (int j = 0; j < 1000; j++) {
    for (i = 0; terms[i] != NULL; i++) {
        // float score = Trie_Find(root, terms[i], strlen(terms[i]));
        Vector *cache = NewVector(dfaNode *, 8);
        SparseAutomaton a =
            NewSparseAutomaton(terms[i], strlen(terms[i]), 2);  // strlen(terms[i]) <= 4 ? 1 : 2);
        sparseVector *v = SparseAutomaton_Start(&a);
        dfaNode *dr = __newDfaNode(0, v);
        dfa_build(dr, &a, cache);

        TrieIterator *it = Trie_Iterate(root, stepFilter, &a, dr);
        clock_gettime(CLOCK_REALTIME, &start_time);
        char *s;
        t_len len;
        float score;
        int matches = 0;
        while (TrieIterator_Next(it, &s, &len, &score)) {
            // printf("Found %s -> %.*s -> %f\n", terms[i], len, s, score);
            matches++;
        }

        clock_gettime(CLOCK_REALTIME, &end_time);
        long diffInNanos = end_time.tv_nsec - start_time.tv_nsec;

        printf("%d matches for %s. Time elapsed: %ldnano\n", matches, terms[i], diffInNanos);

        // printf("find: %s => %f, nanotime: %ld, \n", terms[i], score, diffInNanos);
    }
    // }

    // sleep(15);

    return 0;
}

int main(int argc, char **argv) { testWithData(); }
