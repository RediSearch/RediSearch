#include <redisearch.h>
#include <result_processor.h>
#include <util/block_alloc.h>
#include <util/khash.h>

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
  size_t len;  // Number of contexts
  RSFieldMap *values;
  GroupCtx ctxs[0];
} Group;

#define kh_set(kname, hash, key, val)   \
  ({                                    \
    int ret;                            \
    k = kh_put(kname, hash, key, &ret); \
    kh_value(hash, k) = val;            \
    ret;                                \
  })

static const int khid = 33;
KHASH_MAP_INIT_INT64(khid, Group *);

#define GROUP_CTX(g, i) (g->ctxs[i].ptr)
#define GROUP_BYTESIZE(parent) (sizeof(Group) + (sizeof(GroupCtx) * (parent)->numReducers))
#define GROUPS_PER_BLOCK 1024
typedef struct Grouper {
  khash_t(khid) * groups;
  BlkAlloc groupsAlloc;
  RSMultiKey *keys;
  RSSortingTable *sortTable;
  Reducer **reducers;
  size_t numReducers;
  size_t capReducers;
  int accumulating;
  int sortKeyIdx;
  khiter_t iter;
  int hasIter;

} Grouper;

static Group *GroupAlloc(void *ctx) {
  Grouper *g = ctx;
  size_t elemSize = sizeof(Group) + (sizeof(GroupCtx) * g->numReducers);
  Group *group = BlkAlloc_Alloc(&g->groupsAlloc, elemSize, GROUPS_PER_BLOCK * elemSize);
  memset(group, 0, elemSize);

  for (size_t ii = 0; ii < g->numReducers; ++ii) {
    group->ctxs[ii].ptr = g->reducers[ii]->NewInstance(&g->reducers[ii]->ctx);
    group->ctxs[ii].free = g->reducers[ii]->FreeInstance;
  }
  return group;
}

static void Group_Init(Group *group, Grouper *g, SearchResult *res, uint64_t hash) {
  // Copy the group keys to the new group
  group->len = g->numReducers;
  group->values = RS_NewFieldMap(g->keys->len + g->numReducers + 1);

  for (size_t i = 0; i < g->keys->len; i++) {

    // We must do a deep copy of the group values since they may be deleted during processing
    RSValue *src = SearchResult_GetValue(res, g->sortTable, &g->keys->keys[i]);
    RSFieldMap_Add(&group->values, g->keys->keys[i].key, src);
  }
}

static void gtGroupClean(Group *group, void *unused_a, void *unused_b) {
  for (size_t i = 0; i < group->len; i++) {
    group->ctxs[i].free(group->ctxs[i].ptr);
  }
  group->len = 0;
  if (group->values) {
    RSFieldMap_Free(group->values, 0);
    group->values = NULL;
  }
}

static void baGroupClean(void *ptr, void *arg) {
  gtGroupClean(ptr, NULL, NULL);
}

/* Yield - pops the current top result from the heap */
static int grouper_Yield(Grouper *g, SearchResult *r) {
  if (!g->hasIter) {
    g->iter = kh_begin(g->groups);
    g->hasIter = 1;
  }

  char *s;
  tm_len_t l;
  Group *gr;

  for (; g->iter != kh_end(g->groups); ++g->iter) {
    if (kh_exist(g->groups, g->iter)) {
      gr = kh_value(g->groups, g->iter);

      if (r->fields) {
        RSFieldMap_Free(r->fields, 0);
        r->fields = NULL;
      }

      // We copy the group field map (containing the group keys) as is to the result as a field map!
      r->fields = gr->values;
      r->indexResult = NULL;
      gr->values = NULL;

      // Copy the reducer values to the group
      for (size_t i = 0; i < g->numReducers; i++) {
        g->reducers[i]->Finalize(GROUP_CTX(gr, i), g->reducers[i]->alias, r);
      }
      ++g->iter;
      return RS_RESULT_OK;
    }
  }
  return RS_RESULT_EOF;
}

static uint64_t grouper_EncodeGroupKey(Grouper *g, SearchResult *res) {

  uint64_t ret = 0;
  for (size_t i = 0; i < g->keys->len; i++) {

    RSValue *v = SearchResult_GetValue(res, g->sortTable, &g->keys->keys[i]);
    ret = RSValue_Hash(v, ret);
  }

  return ret;
}

static int Grouper_Next(ResultProcessorCtx *ctx, SearchResult *res) {
  // static SearchResult up;

  Grouper *g = ctx->privdata;
  if (!g->accumulating) {
    return grouper_Yield(g, res);
  }

  int rc = ResultProcessor_Next(ctx->upstream, res, 1);
  // if our upstream has finished - just change the state to not accumulating, and yield
  if (rc == RS_RESULT_EOF) {
    // Set the number of results to the total number of groups we found
    if (ctx->qxc) {
      ctx->qxc->totalResults = kh_size(g->groups);
    }
    g->accumulating = 0;
    return grouper_Yield(g, res);
  }

  Group *group;
  uint64_t hash = grouper_EncodeGroupKey(g, res);
  int isNew = 0;
  khiter_t k = kh_get(khid, g->groups, hash);  // first have to get ieter
  if (k == kh_end(g->groups)) {                // k will be equal to kh_end if key not present
    group = GroupAlloc(g);
    Group_Init(group, g, res, hash);
    kh_set(khid, g->groups, hash, group);
  } else {
    group = kh_value(g->groups, k);
  }

  for (size_t i = 0; i < g->numReducers; i++) {
    g->reducers[i]->Add(GROUP_CTX(group, i), res);
  }

  return RS_RESULT_QUEUED;
}

// Free just frees up the processor. If left as NULL we simply use free()
static void Grouper_Free(struct resultProcessor *p) {
  Grouper *g = p->ctx.privdata;
  // KHTable_FreeEx(&g->groups, NULL, gtGroupClean);

  kh_destroy(khid, g->groups);
  BlkAlloc_FreeAll(&g->groupsAlloc, baGroupClean, g, GROUP_BYTESIZE(g));

  for (size_t i = 0; i < g->numReducers; i++) {
    g->reducers[i]->Free(g->reducers[i]);
  }
  RSMultiKey_Free(g->keys);

  free(g->reducers);
  free(g);
  free(p);
}

Grouper *NewGrouper(RSMultiKey *keys, RSSortingTable *tbl) {
  Grouper *g = malloc(sizeof(*g));
  BlkAlloc_Init(&g->groupsAlloc);
  g->groups = kh_init(khid);
  g->sortTable = tbl;
  g->keys = keys;
  g->capReducers = 2;
  g->reducers = calloc(g->capReducers, sizeof(Reducer *));
  g->numReducers = 0;
  g->accumulating = 1;
  g->hasIter = 0;

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
  if (g->numReducers == g->capReducers) {
    g->capReducers *= 2;
    g->reducers = realloc(g->reducers, g->capReducers * sizeof(Reducer *));
  }
  g->reducers[g->numReducers - 1] = r;
}