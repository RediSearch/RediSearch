/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#ifndef __TRIE_NODE_H__
#define __TRIE_NODE_H__

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <stdbool.h>
#include "rune_util.h"
#include "redisearch.h"
#ifdef __cplusplus
extern "C" {
#endif

typedef uint16_t t_len;

#define TRIE_INITIAL_STRING_LEN 256
#define TRIE_MAX_PREFIX 100

/* TrieNode_Add return codes */
#define TRIE_OK_NEW                1   /* Successfully added a new entry */
#define TRIE_OK_UPDATED            0   /* Entry already existed, score/payload updated */
#define TRIE_ERR_PAYLOAD_OVERFLOW -1   /* Payload too large or allocation overflow */

typedef enum {
  Trie_Sort_Lex = 0,
  Trie_Sort_Score = 1,
} TrieSortMode;

typedef void (*TrieFreeCallback)(void *node);
struct timespec;

/* Opaque types. Layouts live in trie_node_internal.h and are not visible to
 * callers outside src/trie/. Use the accessors below. */
typedef struct TriePayload TriePayload;
typedef struct TrieNode TrieNode;
typedef struct TrieIterator TrieIterator;

/* Opaque accessors over TrieNode struct internals. These are the only
 * supported way to read TrieNode fields from outside src/trie/. */
t_len TrieNode_NumChildren(const TrieNode *n);
bool TrieNode_IsTerminal(const TrieNode *n);
/* Number of documents associated with this terminal entry (0 for non-terminal
 * or deleted nodes). */
size_t TrieNode_NumDocs(const TrieNode *n);
/* Pointer to the children array (count is TrieNode_NumChildren). */
TrieNode **TrieNode_Children(const TrieNode *n);
TrieNode *TrieNode_ChildAt(const TrieNode *n, t_len i);
/* Return the node's payload data pointer, or NULL if the node has no payload
 * (or n is NULL). */
char *TrieNode_GetPayloadData(const TrieNode *n);

/* Return the raw data bytes of a TriePayload, or NULL if p is NULL. Used by
 * range/contains callbacks that receive a TriePayload* in their payload
 * argument. */
char *TriePayload_Data(TriePayload *p);

typedef enum {
  ADD_REPLACE,
  ADD_INCR,
} TrieAddOp;
/* Add a new string to a trie. We pass a pointer to the node because it may
 * actually change when splitting.
 * numDocs: the value to add to the existing numDocs
 *
 * Returns:
 *   TRIE_OK_NEW (1)                - String was added (new entry)
 *   TRIE_OK_UPDATED (0)            - String already existed, score/payload updated
 *   TRIE_ERR_PAYLOAD_OVERFLOW (-1) - Payload too large or allocation overflow
 *
 * Note: The return value is used by Trie_InsertRune to update the Trie size
 * member (incremented only when TRIE_OK_NEW is returned).
 */
int TrieNode_Add(TrieNode **n, const rune *str, t_len len, RSPayload *payload,
                 float score, TrieAddOp op, TrieFreeCallback freecb,
                 size_t numDocs);

/* Find the entry with a given string and length, and return it. */
TrieNode *TrieNode_Get(TrieNode *n, const rune *str, t_len len, bool exact, int *offsetOut);

/* Mark a node as deleted. For simplicity for now we don't actually delete
 * anything,
 * but the node will not be persisted to disk, thus deleted after reload.
 * Returns 1 if the node was indeed deleted, 0 otherwise */
int TrieNode_Delete(TrieNode *n, const rune *str, t_len len, TrieFreeCallback freecb);

/* Free the trie's root and all its children recursively */
void TrieNode_Free(TrieNode *n, TrieFreeCallback freecb);

typedef enum { F_CONTINUE = 0, F_STOP = 1 } FilterCode;

// A callback for an automaton that receives the current state, evaluates the
// next byte,
// and returns the next state of the automaton. If we should not continue down,
// return F_STOP
typedef FilterCode (*StepFilter)(rune b, void *ctx, int *match, void *matchCtx);

typedef void (*StackPopCallback)(void *ctx, int num);

/* Iterate the tree with a step filter, which tells the iterator whether to
 * continue down the trie
 * or not. This can be a levenshtein automaton, a regex automaton, etc. A NULL
 * filter means just
 * continue iterating the entire trie. ctx is the filter's context */
TrieIterator *TrieNode_Iterate(TrieNode *n, StepFilter f, StackPopCallback pf, void *ctx);

/* Free a trie iterator */
void TrieIterator_Free(TrieIterator *it);

/* Iterate to the next matching entry in the trie. Returns 1 if we can continue,
 * or 0 if we're done
 * and should exit */
int TrieIterator_Next(TrieIterator *it, rune **ptr, t_len *len, RSPayload *payload, float *score,
                      size_t *numDocs, void *matchCtx);

TrieNode *TrieNode_RandomWalk(TrieNode *n, int minSteps, rune **str, t_len *len);

typedef int(TrieRangeCallback)(const rune *, size_t, void *, void *, size_t);
typedef int(TrieSuffixCallback)(const char *, size_t, void *, void *);

/**
 * Iterate all nodes within range.
 * @param n the node to iterateo
 * @param min the minimum lexical string to check from
 * @param minlen the length of min
 * @param includeMin is min included
 * @param max the maximum lexical string to check until
 * @param maxlen the maximum length of the max
 * @param includeMax is max included
 * @param callback the callback to invoke
 * @param ctx data to be passed to the callback
 */

void TrieNode_IterateRange(TrieNode *n, const rune *min, int minlen, bool includeMin,
                           const rune *max, int maxlen, bool includeMax, TrieRangeCallback callback,
                           void *ctx);

/**
 * Iterate all nodes within range.
 * @param n the node to iterateo
 * @param str the string to check
 * @param nstr the length of str
 * @param prefix is the string prefix
 * @param suffix is the string suffix
 * @param callback the callback to invoke
 * @param ctx data to be passed to the callback
 */
void TrieNode_IterateContains(TrieNode *n, const rune *str, int nstr, bool prefix, bool suffix,
                              TrieRangeCallback callback, void *ctx, struct timespec *timeout,
                              bool skipTimeoutChecks);

void TrieNode_IterateWildcard(TrieNode *n, const rune *str, int nstr,
                              TrieRangeCallback callback, void *ctx, struct timespec *timeout,
                              bool skipTimeoutChecks);

#ifdef __cplusplus
}
#endif
#endif
