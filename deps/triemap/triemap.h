#ifndef __TRIEMAP_H__
#define __TRIEMAP_H__

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>

#include "util/arr.h"
#include "util/timeout.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef uint16_t tm_len_t;

#define TM_NODE_DELETED 0x01
#define TM_NODE_TERMINAL 0x02

/* This special pointer is returned when TrieMap_Find cannot find anything */
extern void *TRIEMAP_NOTFOUND;

#pragma pack(1)

/* TrieMapNode represents a single node in a trie. The actual size of it is
 * bigger, as the children are allocated after str[].
 * The value pointer is optional, and NULL can be used if you are just
 * interested in the triemap as a set for strings
 */
typedef struct {
  // the string length of this node. can be 0
  tm_len_t len;
  // the number of child nodes
  tm_len_t numChildren : 9;

  uint8_t flags : 7;

  void *value;

  // the string of the current node
  char str[];
  // ... here come the first letters of each child childChars[]
  // ... now come the children, to be accessed with __trieMapNode_children
} TrieMapNode;

#pragma pack()

typedef struct {
  TrieMapNode *root;
  size_t cardinality;  // number of terms
  size_t size;         // number of nodes
} TrieMap;

typedef void (*freeCB)(void *);

TrieMap *NewTrieMap();

typedef void *(*TrieMapReplaceFunc)(void *oldval, void *newval);

/* Add a new string to a trie. Returns 1 if the key is new to the trie or 0 if
 * it already existed.
 *
 * If value is given, it is saved as a pyaload inside the trie node.
 * If the key already exists, we replace the old value with the new value, using
 * free() to free the old value.
 *
 * If cb is given, instead of replacing and freeing, we call the callback with
 * the old and new value, and the function should return the value to set in the
 * node, and take care of freeing any unwanted pointers. The returned value
 * can be NULL and doesn't have to be either the old or new value.
 */
int TrieMap_Add(TrieMap *t, char *str, tm_len_t len, void *value, TrieMapReplaceFunc cb);

/* Find the entry with a given string and length, and return its value, even if
 * that was NULL.
 *
 * NOTE: If the key does not exist in the trie, we return the special
 * constant value TRIEMAP_NOTFOUND, so checking if the key exists is done by
 * comparing to it, becase NULL can be a valid result.
 */
void *TrieMap_Find(TrieMap *t, const char *str, tm_len_t len);

/* Find nodes that have a given prefix. Results are placed in an array.
 */
int TrieMap_FindPrefixes(TrieMap *t, const char *str, tm_len_t len, arrayof(void*) *results);

/* Mark a node as deleted. It also optimizes the trie by merging nodes if
 * needed. If freeCB is given, it will be used to free the value of the deleted
 * node. If it doesn't, we simply call free() */
int TrieMap_Delete(TrieMap *t, const char *str, tm_len_t len, freeCB func);

/* Free the trie's root and all its children recursively. If freeCB is given, we
 * call it to free individual payload values. If not, free() is used instead. */
void TrieMap_Free(TrieMap *t, freeCB func);

size_t TrieMap_MemUsage(TrieMap *t);

/**************  Iterator API  - not ported from the textual trie yet
 * ***********/
/* trie iterator stack node. for internal use only */
typedef struct {
  int state;
  bool found;
  TrieMapNode *n;
  tm_len_t stringOffset;
  tm_len_t childOffset;
} __tmi_stackNode;

/* Use by TrieMapIterator to determine type of query */
typedef enum {
  TM_PREFIX_MODE = 0,
  TM_CONTAINS_MODE = 1,
  TM_SUFFIX_MODE = 2,
  TM_WILDCARD_MODE = 3,
  TM_WILDCARD_FIXED_LEN_MODE = 4,
} tm_iter_mode;

typedef struct TrieMapIterator{
  arrayof(char) buf;

  arrayof(__tmi_stackNode) stack;

  const char *prefix;
  tm_len_t prefixLen;

  tm_iter_mode mode;

  struct TrieMapIterator *matchIter;

  struct timespec timeout;
  size_t timeoutCounter;
} TrieMapIterator;

void __tmi_Push(TrieMapIterator *it, TrieMapNode *node, tm_len_t stringOffset,
                bool found);
void __tmi_Pop(TrieMapIterator *it);

/* Iterate the trie for all the suffixes of a given prefix. This returns an
 * iterator object even if the prefix was not found, and subsequent calls to
 * TrieMapIterator_Next are needed to get the results from the iteration. If the
 * prefix is not found, the first call to next will return 0 */
TrieMapIterator *TrieMap_Iterate(TrieMap *t, const char *prefix, tm_len_t prefixLen);

/* Set timeout limit used for affix queries */
void TrieMapIterator_SetTimeout(TrieMapIterator *it, struct timespec timeout);

/* Free a trie iterator */
void TrieMapIterator_Free(TrieMapIterator *it);

/* Iterate to the next matching entry in the trie. Returns 1 if we can continue,
 * or 0 if we're done and should exit */
int TrieMapIterator_Next(TrieMapIterator *it, char **ptr, tm_len_t *len, void **value);

/* Iterate to the next matching entry in the trie. Returns 1 if we can continue,
 * or 0 if we're done and should exit 
 * NextContains is used by Contains and Suffix queries.
 * Wildcard is used by Wildcard queries.
 */
int TrieMapIterator_NextContains(TrieMapIterator *it, char **ptr, tm_len_t *len, void **value);
int TrieMapIterator_NextWildcard(TrieMapIterator *it, char **ptr, tm_len_t *len, void **value);

typedef int (*TrieMapIterator_NextFunc)(TrieMapIterator *it, char **ptr, tm_len_t *len, void **value);

typedef void(TrieMapRangeCallback)(const char *, size_t, void *, void *);

void TrieMap_IterateRange(TrieMap *trie, const char *min, int minlen, bool includeMin,
                          const char *max, int maxlen, bool includeMax,
                          TrieMapRangeCallback callback, void *ctx);

void *TrieMap_RandomValueByPrefix(TrieMap *t, const char *prefix, tm_len_t pflen);

#ifdef __cplusplus
}
#endif

#endif
