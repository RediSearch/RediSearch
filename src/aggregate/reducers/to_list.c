#include <aggregate/reducer.h>

struct tolistCtx {
  TrieMap *values;
  const char *property;
  RSSortingTable *sortables;
};

void *tolist_NewInstance(ReducerCtx *rctx) {
  const char *property = rctx->privdata;

  struct tolistCtx *ctx = malloc(sizeof(*ctx));
  ctx->values = NewTrieMap();
  ctx->property = property;
  ctx->sortables = rctx->ctx->spec->sortables;
  return ctx;
}

int tolist_Add(void *ctx, SearchResult *res) {
  struct tolistCtx *tlc = ctx;

  RSValue *v = SearchResult_GetValue(res, tlc->sortables, tlc->property);
  if (v) {
    uint64_t hval = RSValue_Hash(v, 0);
    if (TrieMap_Find(tlc->values, (char *)&hval, sizeof(hval)) == TRIEMAP_NOTFOUND) {
      RSValue *sv = malloc(sizeof(RSValue));
      RSValue_DeepCopy(sv, v);
      TrieMap_Add(tlc->values, (char *)&hval, sizeof(hval), sv, NULL);
    }
  }
  return 1;
}

int tolist_Finalize(void *ctx, const char *key, SearchResult *res) {
  struct tolistCtx *tlc = ctx;
  TrieMapIterator *it = TrieMap_Iterate(tlc->values, "", 0);
  char *c;
  tm_len_t l;
  void *ptr;
  RSValue *arr = calloc(tlc->values->cardinality, sizeof(RSValue));
  size_t i = 0;
  while (TrieMapIterator_Next(it, &c, &l, &ptr)) {
    if (ptr) {
      arr[i++] = *(RSValue *)ptr;
    }
  }
  RSFieldMap_Set(&res->fields, key, RS_ArrVal(arr, i));
  TrieMapIterator_Free(it);
  return 1;
}

void freeValues(void *ptr) {
  RSValue_Free(ptr);
  free(ptr);
}
// Free just frees up the processor. If left as NULL we simply use free()
void tolist_Free(Reducer *r) {
  free(r->ctx.privdata);
  free(r);
}
void tolist_FreeInstance(void *p) {
  struct tolistCtx *tlc = p;

  TrieMap_Free(tlc->values, freeValues);
  free(tlc);
}

Reducer *NewToList(RedisSearchCtx *sctx, const char *property, const char *alias) {
  Reducer *r = malloc(sizeof(*r));
  r->Add = tolist_Add;
  r->Finalize = tolist_Finalize;
  r->Free = tolist_Free;
  r->FreeInstance = tolist_FreeInstance;
  r->NewInstance = tolist_NewInstance;

  r->alias = alias ? alias : property;
  r->ctx = (ReducerCtx){.privdata = strdup(property), .ctx = sctx};

  return r;
}