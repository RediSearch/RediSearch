#include <redisearch.h>
#include <result_processor.h>
#include <dep/triemap/triemap.h>

#define GROUPBY_C_
#include "reducer.h"

/* A group context is used by the reducer to manage context for a single group */
typedef struct {
  void *ptr;
  void (*free)(void *);
} GroupCtx;

/* A group represents the allocated context of all reducers in a group, and the selected values of
 * that group */
typedef struct {
  size_t len;
  RSFieldMap *values;
  GroupCtx ptrs[];
} Group;

Group *NewGroup(size_t len, RSFieldMap *groupVals) {
  Group *g = malloc(sizeof(Group) + len * sizeof(GroupCtx));
  g->values = groupVals;
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

typedef struct Grouper {
  TrieMap *groups;
  TrieMapIterator *iter;
  RSMultiKey *keys;
  RSSortingTable *sortTable;
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

    r->fields = RS_NewFieldMap(g->numReducers + g->keys->len);

    // Add a property with the group name
    // RSFieldMap_Add(&r->fields, g->alias ? g->alias : g->property, RS_CStringVal(strndup(s, l)));
    Group *gr = p;
    // Copy the group fields to the field map
    for (int i = 0; i < g->keys->len; i++) {
      // TODO: Watch string copy here
      RSFieldMap_Add(&r->fields, g->keys->keys[i], *RSFieldMap_Item(gr->values, i));
    }
    // Copy the reducer values to the group
    for (size_t i = 0; i < g->numReducers; i++) {
      g->reducers[i]->Finalize(GROUP_CTX(gr, i), g->reducers[i]->alias, r);
    }
    return RS_RESULT_OK;

  } while (1);
}

uint64_t grouper_EncodeGroupKey(Grouper *g, SearchResult *res) {

  uint64_t ret = 0;
  for (size_t i = 0; i < g->keys->len; i++) {
    // TODO: Init sorting table
    RSValue v = SearchResult_GetValue(res, NULL, RSKEY(g->keys->keys[i]));
    ret = RSValue_Hash(&v, ret);
  }

  return ret;
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

  uint64_t hash = grouper_EncodeGroupKey(g, res);
  // printf("Got group %s\n", buf);
  Group *group = TrieMap_Find(g->groups, (char *)&hash, sizeof(uint64_t));
  if (!group || (void *)group == TRIEMAP_NOTFOUND) {

    // Copy the group keys to the new group
    RSFieldMap *gm = RS_NewFieldMap(g->keys->len);
    for (size_t i = 0; i < g->keys->len; i++) {
      // TODO: Init sorting table
      RSValue src = SearchResult_GetValue(res, g->sortTable, g->keys->keys[i]);
      RSValue dst;
      RSValue_DeepCopy(&dst, &src);
      RSFieldMap_Add(&gm, g->keys->keys[i], dst);
    }

    // create the group
    group = NewGroup(g->numReducers, gm);
    for (size_t i = 0; i < g->numReducers; i++) {
      group->ptrs[i].ptr = g->reducers[i]->NewInstance(g->reducers[i]->privdata);
      group->ptrs[i].free = g->reducers[i]->FreeInstance;
    }
    TrieMap_Add(g->groups, (char *)&hash, sizeof(hash), group, NULL);
  }
  for (size_t i = 0; i < g->numReducers; i++) {
    g->reducers[i]->Add(GROUP_CTX(group, i), res);
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

Grouper *NewGrouper(RSMultiKey *keys, RSSortingTable *tbl) {
  Grouper *g = malloc(sizeof(*g));
  g->groups = NewTrieMap();
  g->iter = NULL;
  g->sortTable = tbl;
  g->keys = keys;
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