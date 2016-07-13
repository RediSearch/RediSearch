#include <sys/param.h>
#include <math.h>
#include <time.h>
#include "../rmutil/util.h"
#include "../rmutil/strings.h"
#include "../util/heap.h"
#include "trie_type.h"

Trie *NewTrie() {
    Trie *tree = RedisModule_Alloc(sizeof(Trie));
    tree->root = __newTrieNode("", 0, 0, 0, 0);
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

Vector *Trie_Search(Trie *tree, char *s, size_t len, size_t num, int maxDist, int prefixMode) {
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

        // factor the distance into the score
        ent->score = score * exp((double)-(2 * dist));

        // in prefix mode we also factor in the total length of the suffix
        if (prefixMode) {
            ent->score /= sqrt(1 + fabs(slen - len));
        }

        // printf("%.*s - dist %d, totaldist: %f, score %f, refactored score %f\n", slen, str,
        // dist,
        //        fabs(slen - len), score, ent->score);
        if (heap_count(pq) < heap_size(pq)) {
            ent->str = strndup(str, slen);
            heap_offerx(pq, ent);
            pooledEntry = NULL;
        } else {
            TrieSearchResult *qe = heap_peek(pq);
            if (qe->score < ent->score) {
                pooledEntry = heap_poll(pq);
                free(pooledEntry->str);
                ent->str = strndup(str, slen);
                heap_offerx(pq, ent);

            } else {
                pooledEntry = ent;
            }
        }
        // dist = maxDist + 3;
    }

    // put the results from the heap on a vector to return
    size_t n = MIN(heap_count(pq), num);
    Vector *ret = NewVector(TrieSearchResult *, n);
    for (int i = 0; i < n; ++i) {
        TrieSearchResult *h = heap_poll(pq);
        Vector_Put(ret, n - i - 1, h);
    }

    // trim the results to remove irrelevant results
    float maxScore = 0;
    for (int i = 0; i < n; ++i) {
        TrieSearchResult *h;
        Vector_Get(ret, i, &h);

        if (maxScore && h->score < maxScore / SCORE_TRIM_FACTOR) {
            // TODO: Fix trimming the vector
            ret->top = i;
            break;
        }
        maxScore = MAX(maxScore, h->score);
    }

    TrieIterator_Free(it);
    DFAFilter_Free(&fc);
    heap_free(pq);

    return ret;
}

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
    printf("saved %d elemens\n", count);
}

void TrieType_AofRewrite(RedisModuleIO *aof, RedisModuleString *key, void *value) {
    Trie *tree = (Trie *)value;

    if (tree->root) {
        TrieIterator *it = TrieNode_Iterate(tree->root, NULL, NULL, NULL);
        char *str;
        t_len len;
        float score;

        while (TrieIterator_Next(it, &str, &len, &score, NULL)) {
            RedisModule_EmitAOF(aof, "TRIE.ADD", "sbd", key, str, len, (double)score);
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

#ifdef __TRIEMODULE__

/* TRIE.ADD key string score [INCR] */
int TrieAddCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc < 4 || argc > 5) return RedisModule_WrongArity(ctx);

    RedisModule_AutoMemory(ctx); /* Use automatic memory management. */

    RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ | REDISMODULE_WRITE);
    int type = RedisModule_KeyType(key);
    if (type != REDISMODULE_KEYTYPE_EMPTY && RedisModule_ModuleTypeGetType(key) != TrieType) {
        return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
    }

    RedisModuleString *val = argv[2];
    double score;
    if ((RedisModule_StringToDouble(argv[3], &score) != REDISMODULE_OK)) {
        return RedisModule_ReplyWithError(ctx, "ERR invalid score");
    }

    int incr = RMUtil_ArgExists("INCR", argv, argc, 4);

    /* Create an empty value object if the key is currently empty. */
    Trie *tree;
    if (type == REDISMODULE_KEYTYPE_EMPTY) {
        tree = NewTrie();
        RedisModule_ModuleTypeSetValue(key, TrieType, tree);
    } else {
        tree = RedisModule_ModuleTypeGetValue(key);
    }

    /* Insert the new element. */
    Trie_Insert(tree, val, score, incr);

    RedisModule_ReplyWithLongLong(ctx, tree->size);
    RedisModule_ReplicateVerbatim(ctx);
    return REDISMODULE_OK;
}

/* Trie.LEN key */
int TrieLenCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx); /* Use automatic memory management. */

    if (argc != 2) return RedisModule_WrongArity(ctx);
    RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ);
    int type = RedisModule_KeyType(key);
    if (type != REDISMODULE_KEYTYPE_EMPTY && RedisModule_ModuleTypeGetType(key) != TrieType) {
        return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
    }

    Trie *tree = RedisModule_ModuleTypeGetValue(key);
    return RedisModule_ReplyWithLongLong(ctx, tree ? tree->size : 0);
}

/* TRIE.MATCH key string [DIST (default 0)] [PREFIX] */
int TrieMatchCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx); /* Use automatic memory management. */

    if (argc < 3 || argc > 6) return RedisModule_WrongArity(ctx);
    RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ);

    int type = RedisModule_KeyType(key);
    if (type != REDISMODULE_KEYTYPE_EMPTY && RedisModule_ModuleTypeGetType(key) != TrieType) {
        return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
    }

    Trie *tree = RedisModule_ModuleTypeGetValue(key);
    if (tree == NULL) {
        return RedisModule_ReplyWithNull(ctx);
    }

    // get the string to search for
    size_t len;
    char *s = (char *)RedisModule_StringPtrLen(argv[2], &len);

    // get optional DIST argument
    long maxDist = 0;
    RMUtil_ParseArgsAfter("DIST", argv, argc, "l", &maxDist);

    int prefixMode = RMUtil_ArgExists("PREFIX", argv, argc, 3);
    printf("prefix? %d\n", prefixMode);

    size_t num = 10;
    struct timespec start_time, end_time;
    clock_gettime(CLOCK_REALTIME, &start_time);

    Vector *res = Trie_Search(tree, s, len, num, maxDist, prefixMode != 0);
    clock_gettime(CLOCK_REALTIME, &end_time);
    long diffInNanos = end_time.tv_nsec - start_time.tv_nsec;
    printf("Search took %dns\n", diffInNanos);

    RedisModule_ReplyWithArray(ctx, Vector_Size(res) * 2);

    for (int i = 0; i < Vector_Size(res); i++) {
        TrieSearchResult *e;
        Vector_Get(res, i, &e);

        RedisModule_ReplyWithStringBuffer(ctx, e->str, e->len);
        RedisModule_ReplyWithDouble(ctx, e->score);

        TrieSearchResult_Free(e);
    }
    Vector_Free(res);

    return REDISMODULE_OK;
}

/* This function must be present on each Redis module. It is used in order to
 * register the commands into the Redis server. */
int RedisModule_OnLoad(RedisModuleCtx *ctx) {
    if (RedisModule_Init(ctx, "Trie", 1, REDISMODULE_APIVER_1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (TrieType_Register(ctx) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, "trie.add", TrieAddCommand, "write deny-oom", 1, 1, 1) ==
        REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx, "trie.len", TrieLenCommand, "readonly", 1, 1, 1) ==
        REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx, "trie.match", TrieMatchCommand, "readonly", 1, 1, 1) ==
        REDISMODULE_ERR)
        return REDISMODULE_ERR;
    // if (RedisModule_CreateCommand(ctx, "trie.prefixmatch", TrieMatchPrefix, "readonly", 1, 1,
    // 1)
    // ==
    //     REDISMODULE_ERR)
    //     return REDISMODULE_ERR;

    return REDISMODULE_OK;
}

#endif