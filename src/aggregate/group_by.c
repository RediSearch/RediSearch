#include <redisearch.h>
#include <result_processor.h>
#include <dep/triemap/triemap.h>
#include "reducer.h"

typedef struct {
  void *ptr;
  void (*free)(void *);
} GroupCtx;

typedef struct {
  size_t len;
  GroupCtx ptrs[];
} Group;

Group *NewGroup(size_t len) {
  Group *g = malloc(sizeof(Group) + len * sizeof(GroupCtx));
  g->len = len;
  return g;
}

void Group_Free(void *p) {
  Group *g = p;
  for (size_t i = 0; i < g->len; i++) {
    g->ptrs[i].free(g->ptrs[i].ptr);
  }
  free(g);
}

#define GROUP_CTX(g, i) (g->ptrs[i].ptr)

typedef struct {
  TrieMap *groups;
  TrieMapIterator *iter;
  const char *property;
  const char *alias;
  Reducer **reducers;
  size_t numReducers;
  int accumulating;
  int sortKeyIdx;
} Grouper;

/* Yield - pops the current top result from the heap */
int grouper_Yield(Grouper *g, SearchResult *r) {
  if (!g->iter) {
    g->iter = TrieMap_Iterate(g->groups, "", 0);
    // if for some reason we couldn't create an iterator, end here
    if (!g->iter) {
      return RS_RESULT_EOF;
    }
  }

  char *s;
  tm_len_t l;
  void *p;
  do {
    // try the next group
    if (!TrieMapIterator_Next(g->iter, &s, &l, &p)) {
      // nothing to read further from the groups...
      return RS_RESULT_EOF;
    }

    // Add a property with the group name
    RSFieldMap_Add(&r->fields, g->alias ? g->alias : g->property, RS_CStringVal(strndup(s, l)));
    Group *gr = p;
    for (size_t i = 0; i < g->numReducers; i++) {
      g->reducers[i]->Finalize(GROUP_CTX(gr, i), g->reducers[i]->alias, r);
    }
    return RS_RESULT_OK;

  } while (1);
}

int grouper_EncodeKey(Grouper *g, SearchResult *res, char *buf, size_t maxlen) {

  // First try to get the group value by sortables
  if (g->sortKeyIdx != -1 && res->md->sortVector) {
    RSSortableValue *tv = &res->md->sortVector->values[g->sortKeyIdx];
    if (tv->type == RS_SORTABLE_STR) {
      strncpy(buf, tv->str, MIN(maxlen, strlen(tv->str)));
      return 1;
    }
  }
  // if no sortable available - load by field
  RSValue *v = RSFieldMap_Get(res->fields, g->property);
  if (!v || v->t != RSValue_String) {
    return 0;
  }
  strncpy(buf, v->strval.str, MIN(v->strval.len, maxlen));
  return 1;
}

int Grouper_Next(ResultProcessorCtx *ctx, SearchResult *res) {

  static char buf[1024];
  // static SearchResult up;

  Grouper *g = ctx->privdata;
  if (!g->accumulating) {
    return grouper_Yield(g, res);
  }

  int rc = ResultProcessor_Next(ctx->upstream, res, 1);
  // if our upstream has finished - just change the state to not accumulating, and yield
  if (rc == RS_RESULT_EOF) {
    g->accumulating = 0;
    return grouper_Yield(g, res);
  }

  if (grouper_EncodeKey(g, res, buf, sizeof(buf))) {

    Group *group = TrieMap_Find(g->groups, buf, strlen(buf));
    if (!group || (void *)group == TRIEMAP_NOTFOUND) {

      group = NewGroup(g->numReducers);
      for (size_t i = 0; i < g->numReducers; i++) {
        group->ptrs[i].ptr = g->reducers[i]->NewInstance(g->reducers[i]->privdata);
        group->ptrs[i].free = g->reducers[i]->FreeInstance;
      }
      TrieMap_Add(g->groups, buf, strlen(buf), group, NULL);
    }
    for (size_t i = 0; i < g->numReducers; i++) {
      g->reducers[i]->Add(GROUP_CTX(group, i), res);
    }
  }
  return RS_RESULT_QUEUED;
}

// Free just frees up the processor. If left as NULL we simply use free()
void Grouper_Free(struct resultProcessor *p) {
  Grouper *g = p->ctx.privdata;

  TrieMap_Free(g->groups, Group_Free);
  for (size_t i = 0; i < g->numReducers; i++) {
    g->reducers[i]->Free(g->reducers[i]);
  }
  free(g->reducers);
  free(g);
  free(p);
}

Grouper *NewGrouper(const char *property, const char *alias, RSSortingTable *tbl) {
  Grouper *g = malloc(sizeof(*g));
  g->groups = NewTrieMap();
  g->iter = NULL;
  g->sortKeyIdx = RSSortingTable_GetFieldIdx(tbl, property);
  g->property = property;
  g->alias = alias ? alias : property;
  g->reducers = NULL;
  g->numReducers = 0;
  g->accumulating = 1;

  return g;
}
ResultProcessor *NewGrouperProcessor(Grouper *g, ResultProcessor *upstream) {

  ResultProcessor *p = NewResultProcessor(upstream, g);
  p->Next = Grouper_Next;
  p->Free = Grouper_Free;
  return p;
}

void Grouper_AddReducer(Grouper *g, Reducer *r) {
  if (!r) return;

  g->numReducers++;
  g->reducers = realloc(g->reducers, g->numReducers * sizeof(Reducer *));
  g->reducers[g->numReducers - 1] = r;
}