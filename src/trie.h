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
} TrieNode;

#pragma pack()
size_t __trieNode_Sizeof(t_len numChildren, t_len slen);
TrieNode *__newTrieNode(char *str, t_len offset, t_len len, t_len numChildren, float score);
#define __trieNode_children(n) ((TrieNode **)((void *)n + sizeof(TrieNode) + n->len + 1))
TrieNode *__trie_AddChild(TrieNode *n, char *str, t_len offset, t_len len, float score);
TrieNode *__trie_SplitNode(TrieNode *n, t_len offset);

TrieNode *Trie_Add(TrieNode *n, char *str, t_len len, float score);
float Trie_Find(TrieNode *n, char *str, t_len len);
void Trie_Free(TrieNode *n);

typedef struct {
    int state;
    TrieNode *n;
    t_len stringOffset;
    t_len childOffset;

} stackNode;

typedef enum { F_CONTINUE = 0, F_STOP = 1 } FilterCode;

#define FILTER_STACK_POP 0
// A callback for an automaton that receives the current state, evaluates the next byte,
// and returns the next state of the automaton. If we should not continue down,
// return NULL
typedef FilterCode (*StepFilter)(unsigned char b, void *ctx, int *match);

#define ITERSTATE_SELF 0
#define ITERSTATE_CHILDREN 1
#define ITERSTATE_MATCH 2

typedef struct {
    char buf[MAX_STRING_LEN];
    t_len bufOffset;

    stackNode stack[MAX_STRING_LEN];
    t_len stackOffset;
    StepFilter filter;
    void *ctx;
} TrieIterator;

void __ti_Push(TrieIterator *it, TrieNode *node);
#define __ti_current(it) &it->stack[it->stackOffset - 1]

void __ti_Pop(TrieIterator *it);

#define __STEP_STOP 0
#define __STEP_CONT 1
#define __STEP_NEXT 2
#define __STEP_MATCH 3

int __ti_step(TrieIterator *it);
TrieIterator *Trie_Iterate(TrieNode *n, StepFilter f, void *ctx);
void TrieIterator_Free(TrieIterator *it);
int TrieIterator_Next(TrieIterator *it, char **ptr, t_len *len, float *score);

#endif