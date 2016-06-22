#include "trie.h"
#include "levenshtein.h"



void *stepFilter(char b, void *ctx, void *stackCtx) {
    SparseAutomaton *a = ctx;
    sparseVector *v = stackCtx;

    sparseVector *nv = SparseAutomaton_Step(a, v, b);

    // we should continue
    if (SparseAutomaton_CanMatch(a, v)) {
        return nv;
    }
    sparseVector_free(nv);
    return NULL;
}

int main(int argc, char **argv) {
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
}
