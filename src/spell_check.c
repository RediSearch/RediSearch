#include "spell_check.h"
#include "util/arr.h"
#include <stdbool.h>

static RS_Suggestion* RS_SuggestionCreate(char* suggestion, double score){
  RS_Suggestion* res = calloc(1, sizeof(RS_Suggestion));
  res->suggestion = suggestion;
  res->score = score;
  return res;
}

static void RS_SuggestionFree(RS_Suggestion* suggestion){
  free(suggestion->suggestion);
  free(suggestion);
}

static Trie* SpellCheck_OpenDict(RedisModuleCtx *ctx, const char* dictName, int mode, RedisModuleKey **k){
  RedisModuleString *keyName = RedisModule_CreateStringPrintf(ctx, DICT_KEY_FMT, dictName);

  *k = RedisModule_OpenKey(ctx, keyName, mode);

  int type = RedisModule_KeyType(*k);
  if(type == REDISMODULE_KEYTYPE_EMPTY){
    Trie *t = NULL;
    if(mode == REDISMODULE_WRITE){
      t = NewTrie();
      RedisModule_ModuleTypeSetValue(*k, TrieType, t);
    }else{
      RedisModule_CloseKey(*k);
    }
    return t;
  }

  if (type != REDISMODULE_KEYTYPE_MODULE || RedisModule_ModuleTypeGetType(*k) != TrieType) {
    RedisModule_CloseKey(*k);
    return NULL;
  }

  return RedisModule_ModuleTypeGetValue(*k);

}

static bool SpellCheck_ReplyTermSuggestions(RedisSearchCtx *sctx, char* term, size_t len, char** includeDict, char** excludeDict){
#define NO_SUGGESTIONS_REPLY "no spelling corrections found"
#define TERM "TERM"
#define SUGGESTIONS_ARRAY_INITIAL_SIZE 10

  rune *rstr = NULL;
  t_len slen = 0;
  float score = 0;
  int dist = 0;
  size_t suggestionLen;

  // searching the term on the exclude list, if its there we just return false
  // because there is no need to return suggestions on it.
  for(int i = 0 ; i < array_len(excludeDict) ; ++i){
    RedisModuleKey *k = NULL;
    Trie* t = SpellCheck_OpenDict(sctx->redisCtx, excludeDict[i], REDISMODULE_READ, &k);
    if(t == NULL){
      continue;
    }
    TrieIterator *it = Trie_Iterate(t, term, len, 0, 0);
    if(TrieIterator_Next(it, &rstr, &slen, NULL, &score, &dist)){
      TrieIterator_Free(it);
      RedisModule_CloseKey(k);
      return false;
    }
    TrieIterator_Free(it);
    RedisModule_CloseKey(k);
  }

  RS_Suggestion** suggestions = array_new(RS_Suggestion*, SUGGESTIONS_ARRAY_INITIAL_SIZE);

  RedisModule_ReplyWithArray(sctx->redisCtx, 3);
  RedisModule_ReplyWithStringBuffer(sctx->redisCtx, TERM, strlen(TERM));
  RedisModule_ReplyWithStringBuffer(sctx->redisCtx, term, len);

  TrieIterator *it = Trie_Iterate(sctx->spec->terms, term, len, 1, 0);
  while(TrieIterator_Next(it, &rstr, &slen, NULL, &score, &dist)){
    char* res = runesToStr(rstr, slen, &suggestionLen);
    // todo: support score, currently all scores are zero
    array_append(suggestions, RS_SuggestionCreate(res, 0));
  }
  TrieIterator_Free(it);


  // searching the term on the include list for more suggestions.
  for(int i = 0 ; i < array_len(includeDict) ; ++i){
    RedisModuleKey *k = NULL;
    Trie* t = SpellCheck_OpenDict(sctx->redisCtx, includeDict[i], REDISMODULE_READ, &k);
    if(t == NULL){
      continue;
    }
    TrieIterator *it = Trie_Iterate(t, term, len, 1, 0);
    while(TrieIterator_Next(it, &rstr, &slen, NULL, &score, &dist)){
      char* res = runesToStr(rstr, slen, &suggestionLen);
      array_append(suggestions, RS_SuggestionCreate(res, 0));
    }
    TrieIterator_Free(it);
    RedisModule_CloseKey(k);
  }

  // todo: sort suggestions by score

  if(array_len(suggestions) == 0){
    RedisModule_ReplyWithStringBuffer(sctx->redisCtx, NO_SUGGESTIONS_REPLY, strlen(NO_SUGGESTIONS_REPLY));
  }else{
    RedisModule_ReplyWithArray(sctx->redisCtx, array_len(suggestions));
    for(int i = 0 ; i < array_len(suggestions) ; ++i){
      RedisModule_ReplyWithArray(sctx->redisCtx, 2);
      RedisModule_ReplyWithDouble(sctx->redisCtx, suggestions[i]->score);
      RedisModule_ReplyWithStringBuffer(sctx->redisCtx, suggestions[i]->suggestion, strlen(suggestions[i]->suggestion));
    }
  }

  array_free_ex(suggestions, RS_SuggestionFree(*(RS_Suggestion**)ptr));

  return true;

}

void SpellCheck_Reply(RedisSearchCtx *sctx, QueryParseCtx *q, char** includeDict, char** excludeDict){
#define NODES_INITIAL_SIZE 5
  size_t results = 0;

  QueryNode **nodes = array_new(QueryNode*, NODES_INITIAL_SIZE);
  nodes = array_append(nodes, q->root);
  QueryNode *currNode = NULL;

  RedisModule_ReplyWithArray(sctx->redisCtx, REDISMODULE_POSTPONED_ARRAY_LEN);

  while(array_len(nodes) > 0){
    currNode = array_pop(nodes);

    switch (currNode->type) {
    case QN_PHRASE:
        for (int i = 0; i < currNode->pn.numChildren; i++) {
          nodes = array_append(nodes, currNode->pn.children[i]);
        }
        break;
      case QN_TOKEN:
        if(SpellCheck_ReplyTermSuggestions(sctx, currNode->tn.str, currNode->tn.len, includeDict, excludeDict)){
          ++results;
        }
        break;

      case QN_NOT:
        nodes = array_append(nodes, currNode->not.child);
        break;

      case QN_OPTIONAL:
        nodes = array_append(nodes, currNode->opt.child);
        break;

      case QN_UNION:
        for (int i = 0; i < currNode->un.numChildren; i++) {
          nodes = array_append(nodes, currNode->un.children[i]);
        }
        break;

      case QN_TAG:
        // todo: do we need to do enything here?
        for (int i = 0; i < currNode->tag.numChildren; i++) {
          nodes = array_append(nodes, currNode->tag.children[i]);
        }
        break;

      case QN_PREFX:
      case QN_NUMERIC:
      case QN_GEO:
      case QN_IDS:
      case QN_WILDCARD:
      case QN_FUZZY:
        break;
    }

  }

  RedisModule_ReplySetArrayLength(sctx->redisCtx, results);
}

int SpellCheck_DictAdd(RedisModuleCtx *ctx, const char* dictName, RedisModuleString **values, int len, char** err){
  int valuesAdded = 0;
  RedisModuleKey *k = NULL;
  Trie *t = SpellCheck_OpenDict(ctx, dictName, REDISMODULE_WRITE, &k);
  if(t == NULL){
    *err = "could not open dict key";
    return -1;
  }

  for(int i = 0 ; i < len ; ++i){
    valuesAdded += Trie_Insert(t, values[i], 1, 1, NULL);
  }

  RedisModule_CloseKey(k);

  return valuesAdded;
}

int SpellCheck_DictDel(RedisModuleCtx *ctx, const char* dictName, RedisModuleString **values, int len, char** err){
  int valuesDeleted = 0;
  RedisModuleKey *k = NULL;
  Trie *t = SpellCheck_OpenDict(ctx, dictName, REDISMODULE_WRITE, &k);
  if(t == NULL){
    *err = "could not open dict key";
    return -1;
  }

  for(int i = 0 ; i < len ; ++i){
    size_t len;
    const char* val = RedisModule_StringPtrLen(values[i], &len);
    valuesDeleted += Trie_Delete(t, (char*)val, len);
  }

  if(t->size == 0){
    RedisModule_DeleteKey(k);
  }

  RedisModule_CloseKey(k);

  return valuesDeleted;
}

int SpellCheck_DictDump(RedisModuleCtx *ctx, const char* dictName, char** err){
  RedisModuleKey *k = NULL;
  Trie *t = SpellCheck_OpenDict(ctx, dictName, REDISMODULE_READ, &k);
  if(t == NULL){
    *err = "could not open dict key";
    return -1;
  }

  rune *rstr = NULL;
  t_len slen = 0;
  float score = 0;
  int dist = 0;
  size_t termLen;

  RedisModule_ReplyWithArray(ctx, t->size);

  TrieIterator *it = Trie_Iterate(t, "", 0, 0, 1);
  while(TrieIterator_Next(it, &rstr, &slen, NULL, &score, &dist)){
    char* res = runesToStr(rstr, slen, &termLen);
    RedisModule_ReplyWithStringBuffer(ctx, res, termLen);
  }

  RedisModule_CloseKey(k);

  return 1;
}
