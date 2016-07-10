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

void Trie_Insert(Trie *t, RedisModuleString *s, double score) {
    size_t len;
    char *str = (char *)RedisModule_StringPtrLen(s, &len);
    t->size += Trie_Add(&t->root, str, len, (float)score);
}

void *TrieTypeRdbLoad(RedisModuleIO *rdb, int encver) {
    if (encver != 0) {
        /* RedisModule_Log("warning","Can't load data with version %d", encver);*/
        return NULL;
    }
    uint64_t elements = RedisModule_LoadUnsigned(rdb);
    Trie *tree = NewTrie();

    while (elements--) {
        RedisModuleString *k = RedisModule_LoadString(rdb);
        double score = RedisModule_LoadDouble(rdb);
        Trie_Insert(tree, k, score);
    }
    return tree;
}

void TrieTypeRdbSave(RedisModuleIO *rdb, void *value) {
    Trie *tree = (Trie *)value;
    RedisModule_SaveUnsigned(rdb, tree->size);

    if (tree->root) {
        TrieIterator *it = Trie_Iterate(tree->root, NULL, NULL, NULL);
        char *str;
        t_len len;
        float score;

        while (TrieIterator_Next(it, &str, &len, &score)) {
            RedisModule_SaveStringBuffer(rdb, str, len);
            RedisModule_SaveDouble(rdb, (double)score);
        }

        TrieIterator_Free(it);
    }
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

    // if (RedisModule_CreateCommand(ctx, "trie.add", TrieAddCommand, "write deny-oom", 1, 1, 1) ==
    //     REDISMODULE_ERR)
    //     return REDISMODULE_ERR;

    // if (RedisModule_CreateCommand(ctx, "trie.len", TrieLenCommand, "readonly", 1, 1, 1) ==
    //     REDISMODULE_ERR)
    //     return REDISMODULE_ERR;

    // if (RedisModule_CreateCommand(ctx, "trie.match", TrieMatch, "readonly", 1, 1, 1) ==
    //     REDISMODULE_ERR)
    //     return REDISMODULE_ERR;
    // if (RedisModule_CreateCommand(ctx, "trie.prefixmatch", TrieMatchPrefix, "readonly", 1, 1, 1)
    // ==
    //     REDISMODULE_ERR)
    //     return REDISMODULE_ERR;

    return REDISMODULE_OK;
}
