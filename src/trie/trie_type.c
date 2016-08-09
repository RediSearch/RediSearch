#include <sys/param.h>
#include <math.h>
#include <time.h>
#include "../rmutil/util.h"
#include "../rmutil/strings.h"
#include "../util/heap.h"
#include "trie_type.h"

Trie *NewTrie() {
    Trie *tree = RedisModule_Alloc(sizeof(Trie));
    tree->root = __newTrieNode("", 0, 0, 0, 0, 0);
    tree->size = 0;
    return tree;
}

void Trie_Insert(Trie *t, RedisModuleString *s, double score, int incr) {
    size_t len;
    char *str = (char *)RedisModule_StringPtrLen(s, &len);
    t->size += TrieNode_Add(&t->root, str, len, (float)score, incr ? ADD_INCR : ADD_REPLACE);
}
void Trie_InsertStringBuffer(Trie *t, char *s, size_t len, double score, int incr) {
    t->size += TrieNode_Add(&t->root, s, len, (float)score, incr ? ADD_INCR : ADD_REPLACE);
}

void TrieSearchResult_Free(TrieSearchResult *e) {
    free(e->str);
    free(e);
}

static int cmpEntries(const void *p1, const void *p2, const void *udata) {
    const TrieSearchResult *e1 = p1, *e2 = p2;

    if (e1->score < e2->score) {
        return 1;
    } else if (e1->score > e2->score) {
        return -1;
    }
    return 0;
}

Vector *Trie_Search(Trie *tree, char *s, size_t len, size_t num, int maxDist, int prefixMode,
                    int trim, int optimize) {
    heap_t *pq = malloc(heap_sizeof(num));
    heap_init(pq, cmpEntries, NULL, num);

    DFAFilter fc = NewDFAFilter(s, len, maxDist, prefixMode);

    TrieIterator *it = TrieNode_Iterate(tree->root, FilterFunc, StackPop, &fc);
    char *str;
    t_len slen;
    float score;

    TrieSearchResult *pooledEntry = NULL;
    int dist = maxDist + 1;
    while (TrieIterator_Next(it, &str, &slen, &score, &dist)) {
        if (pooledEntry == NULL) {
            pooledEntry = malloc(sizeof(TrieSearchResult));
        }
        TrieSearchResult *ent = pooledEntry;
        ent->len = slen;

        ent->score = score;
        if (maxDist > 0) {
            // factor the distance into the score
            ent->score *= exp((double)-(2 * dist));
        }
        // in prefix mode we also factor in the total length of the suffix
        if (prefixMode) {
            ent->score /= sqrt(1 + fabs(slen - len));
        }

        if (heap_count(pq) < heap_size(pq)) {
            ent->str = strndup(str, slen);
            heap_offerx(pq, ent);
            pooledEntry = NULL;

            if (heap_count(pq) == heap_size(pq)) {
                TrieSearchResult *qe = heap_peek(pq);
                it->minScore = qe->score;
            }

        } else {
            if (ent->score >= it->minScore) {
                pooledEntry = heap_poll(pq);
                free(pooledEntry->str);
                ent->str = strndup(str, slen);
                heap_offerx(pq, ent);

                // get the new minimal score
                TrieSearchResult *qe = heap_peek(pq);
                if (qe->score > it->minScore) {
                    it->minScore = qe->score;
                }

            } else {
                pooledEntry = ent;
            }
        }

        // dist = maxDist + 3;
    }

    // printf("Nodes consumed: %d/%d (%.02f%%)\n", it->nodesConsumed,
    //        it->nodesConsumed + it->nodesSkipped,
    //        100.0 * (float)(it->nodesConsumed) / (float)(it->nodesConsumed + it->nodesSkipped));

    // put the results from the heap on a vector to return
    size_t n = MIN(heap_count(pq), num);
    Vector *ret = NewVector(TrieSearchResult *, n);
    for (int i = 0; i < n; ++i) {
        TrieSearchResult *h = heap_poll(pq);
        Vector_Put(ret, n - i - 1, h);
    }

    // trim the results to remove irrelevant results
    if (trim) {
        float maxScore = 0;
        for (int i = 0; i < n; ++i) {
            TrieSearchResult *h;
            Vector_Get(ret, i, &h);

            if (trim) {
                if (maxScore && h->score < maxScore / SCORE_TRIM_FACTOR) {
                    // TODO: Fix trimming the vector
                    ret->top = i;
                    break;
                }
            }
            maxScore = MAX(maxScore, h->score);
        }
    }

    TrieIterator_Free(it);
    DFAFilter_Free(&fc);
    heap_free(pq);

    return ret;
}

/***************************************************************
*
*                       Trie type methods
*
***************************************************************/

/* declaration of the type for redis registration. */
RedisModuleType *TrieType;

void *TrieType_RdbLoad(RedisModuleIO *rdb, int encver) {
    if (encver != 0) {
        return NULL;
    }
    uint64_t elements = RedisModule_LoadUnsigned(rdb);
    Trie *tree = NewTrie();

    while (elements--) {
        size_t len;
        char *str = RedisModule_LoadStringBuffer(rdb, &len);
        double score = RedisModule_LoadDouble(rdb);
        Trie_InsertStringBuffer(tree, str, len, score, 0);
    }
    // TrieNode_Print(tree->root, 0, 0);
    return tree;
}

void TrieType_RdbSave(RedisModuleIO *rdb, void *value) {
    Trie *tree = (Trie *)value;
    RedisModule_SaveUnsigned(rdb, tree->size);
    int count = 0;
    if (tree->root) {
        TrieIterator *it = TrieNode_Iterate(tree->root, NULL, NULL, NULL);
        char *str;
        t_len len;
        float score;

        while (TrieIterator_Next(it, &str, &len, &score, NULL)) {
            RedisModule_SaveStringBuffer(rdb, str, len);
            RedisModule_SaveDouble(rdb, (double)score);
            count++;
        }

        TrieIterator_Free(it);
    }
}

void TrieType_AofRewrite(RedisModuleIO *aof, RedisModuleString *key, void *value) {
    Trie *tree = (Trie *)value;

    if (tree->root) {
        TrieIterator *it = TrieNode_Iterate(tree->root, NULL, NULL, NULL);
        char *str;
        t_len len;
        float score;

        while (TrieIterator_Next(it, &str, &len, &score, NULL)) {
            RedisModule_EmitAOF(aof, TRIE_ADD_CMD, "sbd", key, str, len, (double)score);
        }

        TrieIterator_Free(it);
    }
}

void TrieType_Digest(RedisModuleDigest *digest, void *value) {
    /* TODO: The DIGEST module interface is yet not implemented. */
}

void TrieType_Free(void *value) {
    Trie *tree = value;
    if (tree->root) {
        TrieNode_Free(tree->root);
    }

    RedisModule_Free(tree);
}

int TrieType_Register(RedisModuleCtx *ctx) {
    TrieType = RedisModule_CreateDataType(ctx, "trietype0", 0, TrieType_RdbLoad, TrieType_RdbSave,
                                          TrieType_AofRewrite, TrieType_Digest, TrieType_Free);
    if (TrieType == NULL) {
        return REDISMODULE_ERR;
    }

    return REDISMODULE_OK;
}
