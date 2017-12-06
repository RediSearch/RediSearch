#include "reducer.h"

struct counter {
  size_t count;
};

void *counter_NewInstance(void *privdata) {

  struct counter *ctr = malloc(sizeof(*ctr));
  ctr->count = 0;
  return ctr;
}

int counter_Add(void *ctx, SearchResult *res) {
  struct counter *ctr = ctx;
  ctr->count++;
  return 1;
}

int counter_Finalize(void *ctx, const char *key, SearchResult *res) {
  struct counter *ctr = ctx;
  RSFieldMap_Set(&res->fields, key, RS_NumVal(ctr->count));
  return 1;
}

// Free just frees up the processor. If left as NULL we simply use free()
void counter_Free(Reducer *r) {
  free(r->privdata);
  free(r);
}
void counter_FreeInstance(void *p) {
  struct counter *c = p;
  free(c);
}

Reducer *NewCounter(const char *alias) {
  Reducer *r = malloc(sizeof(*r));
  r->Add = counter_Add;
  r->Finalize = counter_Finalize;
  r->Free = counter_Free;
  r->FreeInstance = counter_FreeInstance;
  r->NewInstance = counter_NewInstance;
  r->privdata = NULL;
  r->alias = alias ? alias : "count";
  return r;
}