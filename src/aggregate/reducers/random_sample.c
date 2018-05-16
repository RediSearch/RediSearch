#include <aggregate/reducer.h>

struct randomSampleProperties {
  RSKey property;
  RSSortingTable *sortables;
  int len;
};

struct randomSampleCtx {
  struct randomSampleProperties *props;
  int seen;  // how many items we've seen
  RSValue *samples[];
};

void *sample_NewInstance(ReducerCtx *rctx) {
  struct randomSampleProperties *props = rctx->privdata;

  struct randomSampleCtx *ctx =
      ReducerCtx_Alloc(rctx, sizeof(*ctx) + props->len * sizeof(RSValue *), 10000);
  ctx->props = props;
  ctx->seen = 0;
  return ctx;
}

int sample_Add(void *ctx, SearchResult *res) {
  struct randomSampleCtx *sc = ctx;

  RSValue *v = SearchResult_GetValue(res, sc->props->sortables, &sc->props->property);
  if (v) {
    if (sc->seen < sc->props->len) {
      sc->samples[sc->seen++] = RSValue_IncrRef(RSValue_MakePersistent(v));
    } else {
      int i = rand() % sc->seen++;
      if (i < sc->props->len) {
        RSValue_Free(sc->samples[i]);
        sc->samples[i] = RSValue_IncrRef(RSValue_MakePersistent(v));
      }
    }
  }

  return 1;
}

int sample_Finalize(void *ctx, const char *key, SearchResult *res) {
  struct randomSampleCtx *sc = ctx;

  int top = MIN(sc->props->len, sc->seen);
  RSValue **arr = calloc(top, sizeof(RSValue *));
  memcpy(arr, sc->samples, top * sizeof(RSValue *));

  RSFieldMap_Set(&res->fields, key, RS_ArrVal(arr, top));
  // set len to 0 so we won't try to free the values on destruction
  sc->seen = 0;
  return 1;
}

void sample_FreeInstance(void *p) {
  struct randomSampleCtx *sc = p;
  int top = MIN(sc->props->len, sc->seen);

  for (int i = 0; i < top; i++) {
    RSValue_Free(sc->samples[i]);
  }
}

Reducer *NewRandomSample(RedisSearchCtx *sctx, int size, const char *property, const char *alias) {
  Reducer *r = malloc(sizeof(*r));
  r->Add = sample_Add;
  r->Finalize = sample_Finalize;
  r->Free = Reducer_GenericFree;
  r->FreeInstance = sample_FreeInstance;
  r->NewInstance = sample_NewInstance;
  r->alias = FormatAggAlias(alias, "random_sample", property);
  struct randomSampleProperties *props = malloc(sizeof(*props));
  props->sortables = SEARCH_CTX_SORTABLES(sctx);
  props->property = RS_KEY(RSKEY(property));
  props->len = size;
  r->ctx = (ReducerCtx){.property = property, .ctx = sctx, .privdata = props};

  return r;
}