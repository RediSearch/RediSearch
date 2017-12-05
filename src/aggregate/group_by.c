#include <redisearch.h>
#include <result_processor.h>
#include <dep/triemap/triemap.h>
#include "reducer.h"

typedef struct {
  TrieMap *groups;
  TrieMapIterator *iter;
  const char *property;
  Reducer *reducer;
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

    if (!g->reducer->Finalize(p, r)) {
      continue;
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

    void *group = TrieMap_Find(g->groups, buf, strlen(buf));
    if (!group || group == TRIEMAP_NOTFOUND) {
      group = g->reducer->NewInstance(g->reducer->privdata, buf);
      TrieMap_Add(g->groups, buf, strlen(buf), group, NULL);
    }

    g->reducer->Add(group, res);
  }
  return RS_RESULT_QUEUED;
}

// Free just frees up the processor. If left as NULL we simply use free()
void Grouper_Free(struct resultProcessor *p) {
  Grouper *g = p->ctx.privdata;

  TrieMap_Free(g->groups, g->reducer->FreeInstance);
  g->reducer->Free(g->reducer);
  free(g);
  free(p);
}

ResultProcessor *NewGrouper(ResultProcessor *upstream, const char *property, Reducer *reducer,
                            RSSortingTable *stbl) {
  Grouper *g = malloc(sizeof(*g));
  g->groups = NewTrieMap();
  g->iter = NULL;
  g->sortKeyIdx = RSSortingTable_GetFieldIdx(stbl, property);
  g->property = property;
  g->reducer = reducer;
  g->accumulating = 1;

  ResultProcessor *p = NewResultProcessor(upstream, g);
  p->Next = Grouper_Next;
  p->Free = Grouper_Free;
  return p;
}