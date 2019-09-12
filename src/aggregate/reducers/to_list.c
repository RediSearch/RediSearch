#include <aggregate/reducer.h>

typedef struct {
  TrieMap *values;
  const RLookupKey *srckey;
} tolistCtx;

static void *tolistNewInstance(Reducer *rbase) {
  tolistCtx *ctx = Reducer_BlkAlloc(rbase, sizeof(*ctx), 100 * sizeof(*ctx));
  ctx->values = NewTrieMap();
  ctx->srckey = rbase->srckey;
  return ctx;
}

static int tolistAdd(Reducer *rbase, void *ctx, const RLookupRow *srcrow) {
  tolistCtx *tlc = ctx;
  RSValue *v = RLookup_GetItem(tlc->srckey, srcrow);
  if (!v) {
    return 1;
  }

  // for non array values we simply add the value to the list */
  if (v->t != RSValue_Array) {
    uint64_t hval = RSValue_Hash(v, 0);
    if (TrieMap_Find(tlc->values, (char *)&hval, sizeof(hval)) == TRIEMAP_NOTFOUND) {

      TrieMap_Add(tlc->values, (char *)&hval, sizeof(hval),
                  RSValue_IncrRef(RSValue_MakePersistent(v)), NULL);
    }
  } else {  // For array values we add each distinct element to the list
    uint32_t len = RSValue_ArrayLen(v);
    for (uint32_t i = 0; i < len; i++) {
      RSValue *av = RSValue_ArrayItem(v, i);
      uint64_t hval = RSValue_Hash(av, 0);
      if (TrieMap_Find(tlc->values, (char *)&hval, sizeof(hval)) == TRIEMAP_NOTFOUND) {

        TrieMap_Add(tlc->values, (char *)&hval, sizeof(hval),
                    RSValue_IncrRef(RSValue_MakePersistent(av)), NULL);
      }
    }
  }
  return 1;
}

static RSValue *tolistFinalize(Reducer *rbase, void *ctx) {
  tolistCtx *tlc = ctx;
  TrieMapIterator *it = TrieMap_Iterate(tlc->values, "", 0);
  char *c;
  tm_len_t l;
  void *ptr;
  RSValue **arr = rm_calloc(tlc->values->cardinality, sizeof(RSValue));
  size_t i = 0;
  while (TrieMapIterator_Next(it, &c, &l, &ptr)) {
    if (ptr) {
      arr[i++] = ptr;
    }
  }

  RSValue *ret = RSValue_NewArrayEx(arr, i, RSVAL_ARRAY_ALLOC);
  TrieMapIterator_Free(it);
  return ret;
}

static void freeValues(void *ptr) {
  RSValue_Decref((RSValue *)ptr);
}

static void tolistFreeInstance(Reducer *parent, void *p) {
  tolistCtx *tlc = p;
  TrieMap_Free(tlc->values, freeValues);
}

Reducer *RDCRToList_New(const ReducerOptions *opts) {
  Reducer *r = rm_calloc(1, sizeof(*r));
  if (!ReducerOptions_GetKey(opts, &r->srckey)) {
    rm_free(r);
    return NULL;
  }
  r->Add = tolistAdd;
  r->Finalize = tolistFinalize;
  r->Free = Reducer_GenericFree;
  r->FreeInstance = tolistFreeInstance;
  r->NewInstance = tolistNewInstance;
  return r;
}
