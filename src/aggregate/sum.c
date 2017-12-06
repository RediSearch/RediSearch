#include "reducer.h"

struct sumCtx {
  size_t count;
  double total;
  const char *property;
};

void *sum_NewInstance(void *privdata) {
  const char *property = privdata;

  struct sumCtx *ctx = malloc(sizeof(*ctx));
  ctx->count = 0;
  ctx->total = 0;
  ctx->property = property;
  return ctx;
}

int sum_Add(void *ctx, SearchResult *res) {
  struct sumCtx *ctr = ctx;
  ctr->count++;

  RSValue *v = RSFieldMap_Get(res->fields, ctr->property);
  if (v->t == RSValue_Number) {
    ctr->total += v->numval;
  }

  return 1;
}

int sum_Finalize(void *ctx, const char *key, SearchResult *res) {
  struct sumCtx *ctr = ctx;
  RSFieldMap_Set(&res->fields, key, RS_NumVal(ctr->total));
  return 1;
}

// Free just frees up the processor. If left as NULL we simply use free()
void sum_Free(Reducer *r) {
  free(r->privdata);
  free(r);
}
void sum_FreeInstance(void *p) {
  struct sumCtx *c = p;
  free(c);
}

Reducer *NewSummer(const char *property, const char *alias) {
  Reducer *r = malloc(sizeof(*r));
  r->Add = sum_Add;
  r->Finalize = sum_Finalize;
  r->Free = sum_Free;
  r->FreeInstance = sum_FreeInstance;
  r->NewInstance = sum_NewInstance;
  if (!alias) {
    asprintf((char **)&r->alias, "sum(%s)", property);
  } else {
    r->alias = alias;
  }
  r->privdata = strdup(property);

  return r;
}