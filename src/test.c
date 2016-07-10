#include "trie.h"
#include "levenshtein.h"
#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <unistd.h>
#include <sys/time.h>
#include <sys/param.h>
#include <time.h>

int count = 0;

FilterCode stepFilter(unsigned char b, void *ctx, int *matched) { return F_CONTINUE; }
// void *stepFilter(char b, void *ctx, void *stackCtx) {
//     SparseAutomaton *a = ctx;
//     dfaNode *dn = stackCtx;
//     unsigned char c = b;
//     if (dn->distance == -1) {
//         count++;
//         return NULL;
//     }
//     return dn->edges[c] ? dn->edges[c] : dn->fallback;
//     // // if (!SparseAutomaton_CanMatch(a,v)) {

//     // //     return NULL;
//     // // }
//     // sparseVector *nv = SparseAutomaton_Step(a, v, b);

//     // // we should continue
//     // if (SparseAutomaton_CanMatch(a, nv)) {
//     //     return nv;
//     // }
//     // sparseVector_free(nv);
//     // return NULL;
// }

int testTrie() {
    printf("%ld\n", sizeof(sparseVector));
    // char *str;
    //     t_len len;
    //     t_len numChildren;
    //     struct t_node *children;
    //     void *value;
    TrieNode *root = __newTrieNode("", 0, 0, 0, 1);

    Trie_Add(&root, "hello", 5, 1);
    Trie_Add(&root, "help", 4, 2);

    Trie_Add(&root, "helter skelter", 14, 3);
    printf("find: %f\n", Trie_Find(root, "helter skelter", 14));
    Trie_Add(&root, "heltar skelter", 14, 4);
    Trie_Add(&root, "helter shelter", 14, 5);
    Trie_Add(&root, "helter skelter", 14, 6);

    printf("find: %f\n", Trie_Find(root, "helter skelter", 14));

    const char *term = "helo";
    SparseAutomaton a = NewSparseAutomaton(term, strlen(term), 2);
    TrieIterator *it = Trie_Iterate(root, stepFilter, NULL, &a);
    char *s;
    t_len len;
    float score;

    while (TrieIterator_Next(it, &s, &len, &score)) {
        printf("Found %.*s -> %f\n", len, s, score);
    }
    TrieIterator_Free(it);
    Trie_Free(root);
    return 0;
}

int testWithData() {
    FILE *fp = fopen("../titles.csv", "r");
    assert(fp != NULL);

    char *line = NULL;

    size_t len = 0;
    ssize_t read;
    TrieNode *root = __newTrieNode("root", 0, 4, 0, 0);
    int i = 0;
    while ((read = getline(&line, &len, fp)) != -1) {
        char *sep = strchr(line, ',');
        if (!sep) continue;

        *sep = 0;
        double score = atof(sep + 1) + 1;
        sep--;
        while (*sep == ' ') {
            *sep-- = 0;
        }

        // if (i % 10 == 0)
        Trie_Add(&root, line, strlen(line), (float)score);

        i++;
    }

    if (line) free(line);

    printf("loaded %d entries\n", i);

    char *terms[] = {"barack obama",

                     "hello",        "hello world",      "israel", "united states of america",
                     "barack obama", "computer science", NULL};
    struct timespec start_time, end_time;
    clock_gettime(CLOCK_REALTIME, &start_time);
    unsigned long long totalns = 0;
    int N = 10;
    for (int j = 0; j < N; j++) {
        for (i = 0; terms[i] != NULL; i++) {
            count = 0;
            // float score = Trie_Find(root, terms[i], strlen(terms[i]));
            clock_gettime(CLOCK_REALTIME, &start_time);
            FilterCtx fc = NewFilterCtx(terms[i], strlen(terms[i]), 2);
            clock_gettime(CLOCK_REALTIME, &end_time);
            long diffInNanos = end_time.tv_nsec - start_time.tv_nsec;
            totalns += diffInNanos / 1000;

            // printf("building automaton took %ldns\n", diffInNanos);

            // printf("building dfa took %ldns\n", end_time.tv_nsec - start_time.tv_nsec);
            TrieIterator *it = Trie_Iterate(root, FilterFunc, StackPop, &fc);
            char *s;
            t_len len;
            float score;
            int matches = 0;
            clock_gettime(CLOCK_REALTIME, &start_time);

            while (TrieIterator_Next(it, &s, &len, &score)) {
                // printf("Found %s -> %.*s -> %f\n", terms[i], len, s, score);
                matches++;
            }
            clock_gettime(CLOCK_REALTIME, &end_time);

            FilterCtx_Free(&fc);
            TrieIterator_Free(it);
            //..

            diffInNanos = end_time.tv_nsec - start_time.tv_nsec;
            totalns += diffInNanos / 1000;

            printf("%d matches for %s. Time elapsed: %ldnano\n", matches, terms[i], diffInNanos);

            // printf("find: %s => %f, nanotime: %ld, \n", terms[i], score, diffInNanos);
        }
    }

    printf("avg %lld", (totalns / N) * 1000);
    // clock_gettime(CLOCK_REALTIME, &end_time);
    // printf("took %zd seconds", end_time.tv_sec - start_time.tv_sec);

    // sleep(15);
    Trie_Free(root);

    return 0;
}

int main(int argc, char **argv) {
    testWithData();
    //    testTrie();
}
