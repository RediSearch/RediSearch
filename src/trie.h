#ifndef __TRIE_H__
#define __TRIE_H__

#include <stdlib.h>
#include <string.h>
#include <stdio.h>

typedef u_int8_t t_len;
#define MAX_STRING_LEN 255

#pragma pack(1)

typedef struct {
    t_len len;
    t_len numChildren;

    float score;
    char str[];
} nodeHeader;

typedef union {
    nodeHeader node;
    void *data;
} TrieNode;
#pragma pack()
size_t __trieNode_Sizeof(t_len numChildren, t_len slen);
TrieNode *__newTrieNode(char *str, t_len offset, t_len len, t_len numChildren, float score);
#define __trieNode_children(n) ((TrieNode **)((void *)n + sizeof(nodeHeader) + n->node.len + 1))
TrieNode *__trie_AddChild(TrieNode *n, char *str, t_len offset, t_len len, float score);
TrieNode *__trie_SplitNode(TrieNode *n, t_len offset);

TrieNode *Trie_Add(TrieNode *n, char *str, t_len len, float score);
float Trie_Find(TrieNode *n, char *str, t_len len);
void Trie_Free(TrieNode *n);

typedef struct {
    TrieNode *n;
    t_len stringOffset;
    t_len childOffset;
    int state;
    // context for an iterator filter function
    void *filterCtx;
} stackNode;

// A callback for an automaton that receives the current state, evaluates the next byte,
// and returns the next state of the automaton. If we should not continue down,
// return NULL
typedef void *(*StepFilter)(char b, void *ctx);

#define ITERSTATE_SELF 0
#define ITERSTATE_CHILDREN 1

typedef struct {
    char buf[MAX_STRING_LEN];
    t_len bufOffset;

    stackNode stack[MAX_STRING_LEN];
    t_len stackOffset;
    StepFilter filter;
} TrieIterator;
void __ti_Push(TrieIterator *it, TrieNode *node, void *filterCtx);
#define __ti_current(it) &it->stack[it->stackOffset - 1]

TrieNode *__ti_Pop(TrieIterator *it);
int __ti_step(TrieIterator *it);
TrieIterator *Trie_Iterate(TrieNode *n, StepFilter f, void *initialContext); 
void TrieIterator_Free(TrieIterator *it);
int TrieIterator_Next(TrieIterator *it, char **ptr, t_len *len, float *score);


#endif