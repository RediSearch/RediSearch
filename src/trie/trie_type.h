#ifndef __TRIE_TYPE_H__
#define __TRIE_TYPE_H__

#include "../redismodule.h"

#include "trie.h"
#include "levenshtein.h"

extern RedisModuleType *TrieType;

#define TRIE_ENCVER_CURRENT 1
#define TRIE_ENCVER_NOPAYLOADS 0

typedef struct {
  TrieNode *root;
  size_t size;
} Trie;

typedef struct {
  char *str;
  size_t len;
  float score;
  char *payload;
  size_t plen;
} TrieSearchResult;

#define SCORE_TRIM_FACTOR 10.0

Trie *NewTrie();
int Trie_Insert(Trie *t, RedisModuleString *s, double score, int incr, RSPayload *payload);
int Trie_InsertStringBuffer(Trie *t, char *s, size_t len, double score, int incr,
                            RSPayload *payload);
/* Delete the string from the trie. Return 1 if the node was found and deleted, 0 otherwise */
int Trie_Delete(Trie *t, char *s, size_t len);

void TrieSearchResult_Free(TrieSearchResult *e);
Vector *Trie_Search(Trie *tree, char *s, size_t len, size_t num, int maxDist, int prefixMode,
                    int trim, int optimize);

/* Iterate a prefix in the trie, using maxDist edit distance, returning a trie iterator that the
 * caller needs to free */
TrieIterator *Trie_IteratePrefix(Trie *t, char *prefix, size_t len, int maxDist);

/* Commands related to the redis TrieType registration */
int TrieType_Register(RedisModuleCtx *ctx);

/**
 * These functions save and load payloads with a specified 'loader' and 'saver' routine.
 */

/**
 * Serialize the trie payload provided.
 * Note that 'payload' may be NULL. This function should write something that
 * a corresponding 'loader' can decode later on
 */
typedef void (*TrieTypePayloadSave)(RedisModuleIO *rdb, void *payload);

// Options should be set to 0, and is used internally
void TrieType_Save(RedisModuleIO *rdb, Trie *tree, TrieTypePayloadSave saver, int options);

typedef void *(*TrieTypePayloadLoad)(RedisModuleIO *rdb, int encver);
// Options should be set to 0, and is used internally.
Trie *TrieType_Load(RedisModuleIO *rdb, TrieTypePayloadLoad loader, int options);

#define TrieType_GenericLoad(rdb, loadPayloads) TrieType_Load(rdb, NULL, loadPayloads)
#define TrieType_GenericSave(rdb, t, savePayloads) TrieType_Save(rdb, t, NULL, savePayloads)

void *TrieType_RdbLoad(RedisModuleIO *rdb, int encver);
void TrieType_RdbSave(RedisModuleIO *rdb, void *value);
void TrieType_AofRewrite(RedisModuleIO *aof, RedisModuleString *key, void *value);
void TrieType_Digest(RedisModuleDigest *digest, void *value);
void TrieType_Free(void *value);

#endif
