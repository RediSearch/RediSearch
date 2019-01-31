#ifndef SRC_DICTIONARY_H_
#define SRC_DICTIONARY_H_

#define DICT_KEY_PREFIX "dict:"
#define DICT_KEY_FMT DICT_KEY_PREFIX "%s"

#include "trie/trie_type.h"

Trie *SpellCheck_OpenDict(RedisModuleCtx *ctx, const char *dictName, int mode, RedisModuleKey **k);

int Dictionary_Add(RedisModuleCtx* ctx, const char* dictName, RedisModuleString** values,
                       int len, char** err);

int Dictionary_Del(RedisModuleCtx* ctx, const char* dictName, RedisModuleString** values,
                       int len, char** err);

int Dictionary_Dump(RedisModuleCtx* ctx, const char* dictName, char** err);

int DictDumpCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int DictDelCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int DictAddCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);


#endif /* SRC_DICTIONARY_H_ */
