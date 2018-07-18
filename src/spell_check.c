#include "spell_check.h"
#include "util/arr.h"
#include <stdbool.h>

/** Forward declaration **/
static bool SpellCheck_IsTermExistsInTrie(Trie *t, const char *term, size_t len);

static int RS_SuggestionCompare(const RS_Suggestion* a, const RS_Suggestion* b){
  if(a->score > b->score){
    return 1;
  }
  if(a->score < b->score){
    return -1;
  }
  return 0;
}

static RS_Suggestion* RS_SuggestionCreate(char* suggestion, size_t len, double score){
  RS_Suggestion* res = calloc(1, sizeof(RS_Suggestion));
  res->suggestion = suggestion;
  res->len = len;
  res->score = score;
  return res;
}

static void RS_SuggestionFree(RS_Suggestion* suggestion){
  free(suggestion->suggestion);
  free(suggestion);
}

static RS_Suggestions* RS_SuggestionsCreate(){
#define SUGGESTIONS_ARRAY_INITIAL_SIZE 10
  RS_Suggestions* ret = calloc(1, sizeof(RS_Suggestions));
  ret->suggestionsTrie = NewTrie();
  ret->suggestions = array_new(RS_Suggestion*, SUGGESTIONS_ARRAY_INITIAL_SIZE);
  return ret;
}

static void RS_SuggestionsAdd(RS_Suggestions* s, char* term, size_t len, double score){
  if(SpellCheck_IsTermExistsInTrie(s->suggestionsTrie, term, len)){
    return;
  }

  Trie_InsertStringBuffer(s->suggestionsTrie, term, len, score, 1, NULL);
  s->suggestions = array_append(s->suggestions, RS_SuggestionCreate(term, len, score));
}

static void RS_SuggestionsFree(RS_Suggestions* s){
  array_free_ex(s->suggestions, RS_SuggestionFree(*(RS_Suggestion**)ptr));
  TrieType_Free(s->suggestionsTrie);
  free(s);
}

static Trie* SpellCheck_OpenDict(RedisModuleCtx *ctx, const char* dictName, int mode, RedisModuleKey **k){
  RedisModuleString *keyName = RedisModule_CreateStringPrintf(ctx, DICT_KEY_FMT, dictName);

  *k = RedisModule_OpenKey(ctx, keyName, mode);

  RedisModule_FreeString(ctx, keyName);

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

/**
 * Return the score for the given suggestion (number between 0 to 1).
 * In case the suggestion should not be added return -1.
 */
static double SpellCheck_GetScore(SpellCheckCtx *scCtx, char* suggestion, size_t len, t_fieldMask fieldMask){
  RedisModuleKey *keyp = NULL;
  InvertedIndex *invidx = Redis_OpenInvertedIndexEx(scCtx->sctx, suggestion, len, 0, &keyp);
  double retVal = 0;
  if (!invidx) {
    // can not find inverted index key, score is 0.
    goto end;
  }
  IndexReader *reader = NewTermIndexReader(invidx, NULL, fieldMask, NULL, 1);
  IndexIterator *iter = NewReadIterator(reader);
  RSIndexResult *r;
  if(iter->Read(iter->ctx, &r) != INDEXREAD_EOF) {
    // we have at least one result, the suggestion is relevant.
    retVal = invidx->numDocs / (double)(scCtx->sctx->spec->docs.size - 1);
  }else{
    // fieldMask has filtered all docs, this suggestions should not be returned
    retVal = -1;
  }
  ReadIterator_Free(iter);

end:
  if (keyp) {
    RedisModule_CloseKey(keyp);
  }
  return retVal;
}

static bool SpellCheck_IsTermExistsInTrie(Trie *t, const char *term, size_t len){
  rune *rstr = NULL;
  t_len slen = 0;
  float score = 0;
  int dist = 0;
  bool retVal = false;
  TrieIterator *it = Trie_Iterate(t, term, len, 0, 0);
  if(TrieIterator_Next(it, &rstr, &slen, NULL, &score, &dist)){
    retVal = true;
  }
  DFAFilter_Free(it->ctx);
  free(it->ctx);
  TrieIterator_Free(it);
  return retVal;
}

static void SpellCheck_FindSuggestions(SpellCheckCtx *scCtx, Trie *t, const char *term, size_t len,
                                       t_fieldMask fieldMask, RS_Suggestions* s){
  rune *rstr = NULL;
  t_len slen = 0;
  float score = 0;
  int dist = 0;
  size_t suggestionLen;

  TrieIterator *it = Trie_Iterate(t, term, len, (int)scCtx->distance, 0);
  while(TrieIterator_Next(it, &rstr, &slen, NULL, &score, &dist)){
    char* res = runesToStr(rstr, slen, &suggestionLen);
    double score;
    if((score = SpellCheck_GetScore(scCtx, res, suggestionLen, fieldMask)) != -1){
      RS_SuggestionsAdd(s, res, suggestionLen, score);
    }else{
      free(res);
    }
  }
  DFAFilter_Free(it->ctx);
  free(it->ctx);
  TrieIterator_Free(it);
}

static bool SpellCheck_ReplyTermSuggestions(SpellCheckCtx *scCtx, char* term, size_t len, t_fieldMask fieldMask){
#define NO_SUGGESTIONS_REPLY "no spelling corrections found"
#define TERM "TERM"

  // searching the term on the term trie, if its there we just return false
  // because there is no need to return suggestions on it.
  if(SpellCheck_IsTermExistsInTrie(scCtx->sctx->spec->terms, term, len)){
    return false;
  }

  // searching the term on the exclude list, if its there we just return false
  // because there is no need to return suggestions on it.
  for(int i = 0 ; i < array_len(scCtx->excludeDict) ; ++i){
    RedisModuleKey *k = NULL;
    Trie* t = SpellCheck_OpenDict(scCtx->sctx->redisCtx, scCtx->excludeDict[i], REDISMODULE_READ, &k);
    if(t == NULL){
      continue;
    }
    if(SpellCheck_IsTermExistsInTrie(t, term, len)){
      RedisModule_CloseKey(k);
      return false;
    }
    RedisModule_CloseKey(k);
  }

  RS_Suggestions* s = RS_SuggestionsCreate();

  RedisModule_ReplyWithArray(scCtx->sctx->redisCtx, 3);
  RedisModule_ReplyWithStringBuffer(scCtx->sctx->redisCtx, TERM, strlen(TERM));
  RedisModule_ReplyWithStringBuffer(scCtx->sctx->redisCtx, term, len);

  SpellCheck_FindSuggestions(scCtx, scCtx->sctx->spec->terms, term, len, fieldMask, s);

  // sorting results by score
  qsort(s->suggestions, array_len(s->suggestions), sizeof(RS_Suggestion*), (__compar_fn_t)RS_SuggestionCompare);

  // searching the term on the include list for more suggestions.
  for(int i = 0 ; i < array_len(scCtx->includeDict) ; ++i){
    RedisModuleKey *k = NULL;
    Trie* t = SpellCheck_OpenDict(scCtx->sctx->redisCtx, scCtx->includeDict[i], REDISMODULE_READ, &k);
    if(t == NULL){
      continue;
    }
    SpellCheck_FindSuggestions(scCtx, t, term, len, fieldMask, s);
    RedisModule_CloseKey(k);
  }

  if(array_len(s->suggestions) == 0){
    RedisModule_ReplyWithStringBuffer(scCtx->sctx->redisCtx, NO_SUGGESTIONS_REPLY, strlen(NO_SUGGESTIONS_REPLY));
  }else{
    RedisModule_ReplyWithArray(scCtx->sctx->redisCtx, array_len(s->suggestions));
    for(int i = 0 ; i < array_len(s->suggestions) ; ++i){
      RedisModule_ReplyWithArray(scCtx->sctx->redisCtx, 2);
      RedisModule_ReplyWithDouble(scCtx->sctx->redisCtx, s->suggestions[i]->score);
      RedisModule_ReplyWithStringBuffer(scCtx->sctx->redisCtx, s->suggestions[i]->suggestion, s->suggestions[i]->len);
    }
  }

  RS_SuggestionsFree(s);

  return true;

}

void SpellCheck_Reply(SpellCheckCtx *scCtx, QueryParseCtx *q){
#define NODES_INITIAL_SIZE 5
  size_t results = 0;

  QueryNode **nodes = array_new(QueryNode*, NODES_INITIAL_SIZE);
  nodes = array_append(nodes, q->root);
  QueryNode *currNode = NULL;

  RedisModule_ReplyWithArray(scCtx->sctx->redisCtx, REDISMODULE_POSTPONED_ARRAY_LEN);

  while(array_len(nodes) > 0){
    currNode = array_pop(nodes);

    switch (currNode->type) {
    case QN_PHRASE:
        for (int i = 0; i < currNode->pn.numChildren; i++) {
          nodes = array_append(nodes, currNode->pn.children[i]);
        }
        break;
      case QN_TOKEN:
        if(SpellCheck_ReplyTermSuggestions(scCtx, currNode->tn.str, currNode->tn.len,
                                           currNode->opts.fieldMask)){
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

  array_free(nodes);

  RedisModule_ReplySetArrayLength(scCtx->sctx->redisCtx, results);
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
    free(res);
  }
  DFAFilter_Free(it->ctx);
  free(it->ctx);
  TrieIterator_Free(it);

  RedisModule_CloseKey(k);

  return 1;
}
