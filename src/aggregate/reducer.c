#include "reducer.h"

struct counter {
  size_t count;
  char *name;
};

void *counter_NewInstance(void *privdata, const char *name) {
  struct counterCtx *ctx = privdata;
  struct counter *ctr = malloc(sizeof(*ctr));
  ctr->count = 0;
  ctr->name = strdup(name);
  return ctr;
}

int counter_Add(void *ctx, SearchResult *res) {
  struct counter *ctr = ctx;
  ctr->count++;
  return 1;
}

int counter_Finalize(void *ctx, SearchResult *res) {
  struct counter *ctr = ctx;
  RSFieldMap_Set(&res->fields, ctr->name, RS_NumVal(ctr->count));
  return 1;
}

// Free just frees up the processor. If left as NULL we simply use free()
void counter_Free(Reducer *r) {
  free(r->privdata);
  free(r);
}
void counter_FreeInstance(void *p) {
  struct counter *c = p;
  free(c->name);
  free(c);
}

Reducer *NewCounter() {
  Reducer *r = malloc(sizeof(*r));
  r->Add = counter_Add;
  r->Finalize = counter_Finalize;
  r->Free = counter_Free;
  r->FreeInstance = counter_FreeInstance;
  r->NewInstance = counter_NewInstance;
  r->privdata = NULL;
  return r;
}