#include "redismodule.h"
#include "rmutil/util.h"
#include "rmutil/strings.h"
#include "trie.h"
#include "levenshtein.h"

static RedisModuleType *TrieType;

typedef struct {
    TrieNode *root;
    size_t size;
} Trie;

Trie *NewTrie() {
    Trie *tree = RedisModule_Alloc(sizeof(Trie));
    tree->root = __newTrieNode("", 0, 0, 0, 0);
    tree->size = 0;
    return tree;
}

void Trie_Insert(Trie *t, RedisModuleString *s, double score, int incr) {
    size_t len;
    char *str = (char *)RedisModule_StringPtrLen(s, &len);
    t->size += Trie_Add(&t->root, str, len, (float)score, incr ? ADD_INCR : ADD_REPLACE);
}
void Trie_InsertStringBuffer(Trie *t, char *s, size_t len, double score, int incr) {
    t->size += Trie_Add(&t->root, s, len, (float)score, incr ? ADD_INCR : ADD_REPLACE);
}

void *TrieTypeRdbLoad(RedisModuleIO *rdb, int encver) {
    if (encver != 0) {
        printf("Can't load data with version %d\n", encver);
        return NULL;
    }
    uint64_t elements = RedisModule_LoadUnsigned(rdb);
    printf("About to load %d elements\n", elements);
    Trie *tree = NewTrie();

    while (elements--) {
        size_t len;
        char *str = RedisModule_LoadStringBuffer(rdb, &len);
        double score = RedisModule_LoadDouble(rdb);
        Trie_InsertStringBuffer(tree, str, len, score, 0);
    }
    return tree;
}

void TrieTypeRdbSave(RedisModuleIO *rdb, void *value) {
    Trie *tree = (Trie *)value;
    RedisModule_SaveUnsigned(rdb, tree->size);
    printf("About to save %d elements\n", tree->size);
    int count = 0;
    if (tree->root) {
        TrieIterator *it = Trie_Iterate(tree->root, NULL, NULL, NULL);
        char *str;
        t_len len;
        float score;

        while (TrieIterator_Next(it, &str, &len, &score)) {
            RedisModule_SaveStringBuffer(rdb, str, len);
            RedisModule_SaveDouble(rdb, (double)score);
            count++;
        }

        TrieIterator_Free(it);
    }
    printf("saved %d elemens\n", count);
}

void TrieTypeAofRewrite(RedisModuleIO *aof, RedisModuleString *key, void *value) {
    Trie *tree = (Trie *)value;

    if (tree->root) {
        TrieIterator *it = Trie_Iterate(tree->root, NULL, NULL, NULL);
        char *str;
        t_len len;
        float score;

        while (TrieIterator_Next(it, &str, &len, &score)) {
            RedisModule_EmitAOF(aof, "TRIE.ADD", "sbd", key, str, len, (double)score);
        }

        TrieIterator_Free(it);
    }
}

void TrieTypeDigest(RedisModuleDigest *digest, void *value) {
    /* TODO: The DIGEST module interface is yet not implemented. */
}

void TrieTypeFree(void *value) {
    Trie *tree = value;
    if (tree->root) {
        Trie_Free(tree->root);
    }

    RedisModule_Free(tree);
}

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

    RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);

    FilterCtx fc = NewFilterCtx(s, len, maxDist, prefixMode != 0);

    TrieIterator *it = Trie_Iterate(tree->root, FilterFunc, StackPop, &fc);
    char *str;
    t_len slen;
    float score;
    int matches = 0;

    while (TrieIterator_Next(it, &str, &slen, &score)) {
        RedisModule_ReplyWithStringBuffer(ctx, str, (size_t)slen);
        RedisModule_ReplyWithDouble(ctx, (double)score);
        // printf("Found %s -> %.*s -> %f\n", terms[i], len, s, score);
        matches++;
    }
    FilterCtx_Free(&fc);
    TrieIterator_Free(it);

    RedisModule_ReplySetArrayLength(ctx, matches * 2);

    return REDISMODULE_OK;
}

/* This function must be present on each Redis module. It is used in order to
 * register the commands into the Redis server. */
int RedisModule_OnLoad(RedisModuleCtx *ctx) {
    if (RedisModule_Init(ctx, "Trie", 1, REDISMODULE_APIVER_1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    TrieType = RedisModule_CreateDataType(ctx, "trietype0", 0, TrieTypeRdbLoad, TrieTypeRdbSave,
                                          TrieTypeAofRewrite, TrieTypeDigest, TrieTypeFree);

    if (TrieType == NULL) {
        printf("No Trie type!\n");
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
    // if (RedisModule_CreateCommand(ctx, "trie.prefixmatch", TrieMatchPrefix, "readonly", 1, 1, 1)
    // ==
    //     REDISMODULE_ERR)
    //     return REDISMODULE_ERR;

    return REDISMODULE_OK;
}
