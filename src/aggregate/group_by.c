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

static inline void Group_Init(Group *group, Grouper *g, RSValue **arr, uint64_t hash) {
  // Copy the group keys to the new group
  group->len = g->numReducers;
  group->values = RS_NewFieldMap(g->keys->len + g->numReducers + 1);

  for (size_t i = 0; i < g->keys->len; i++) {

    // We must do a deep copy of the group values since they may be deleted during processing
    RSFieldMap_Add(&group->values, g->keys->keys[i].key, RSValue_MakePersistent(arr[i]));
  }
  // RSFieldMap_Print(group->values);
}

static void gtGroupClean(Group *group, void *unused_a, void *unused_b) {
  for (size_t i = 0; i < group->len; i++) {
    if (group->ctxs[i].free) {
      group->ctxs[i].free(group->ctxs[i].ptr);
    } 
  }
  group->len = 0;
  if (group->values) {
    RSFieldMap_Free(group->values, 0);
    group->values = NULL;
  }
}

/* Wrapper for block allocator callback */
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

static inline void Group_HandleValues(Grouper *g, Group *gr, SearchResult *res) {
  for (size_t i = 0; i < g->numReducers; i++) {
    g->reducers[i]->Add(GROUP_CTX(gr, i), res);
  }
}

/* Extract the group keys from the result and add to them */
static void Grouper_ExtractGroups(Grouper *g, SearchResult *res, RSValue **arr, int idx, int arridx,
                                  int len, uint64_t hval) {
  // end of the line - create/add to group
  if (idx == len) {
    Group *group = NULL;

    // Get or create the group
    khiter_t k = kh_get(khid, g->groups, hval);  // first have to get ieter
    if (k == kh_end(g->groups)) {                // k will be equal to kh_end if key not present
      group = GroupAlloc(g);
      kh_set(khid, g->groups, hval, group);
      Group_Init(group, g, arr, hval);
    } else {
      group = kh_value(g->groups, k);
    }

    // send the result to the group and its reducers
    Group_HandleValues(g, group, res);
    return;
  }

  // get the value
  RSValue *v = RSValue_Dereference(arr[idx]);
  // regular value - just move one step
  if (v->t != RSValue_Array) {
    Grouper_ExtractGroups(g, res, arr, idx + 1, 0, len, RSValue_Hash(v, hval));
  } else {

    // advance one in the overall array hashing current value in current array
    RSValue *tmp = arr[idx];
    arr[idx] = RSValue_ArrayItem(v, arridx);
    uint64_t hh = RSValue_Hash(arr[idx], hval);
    Grouper_ExtractGroups(g, res, arr, idx + 1, 0, len, hh);
    arr[idx] = tmp;
    if (arridx + 1 < RSValue_ArrayLen(v)) {
      // advance to the next array element
      Grouper_ExtractGroups(g, res, arr, idx, arridx + 1, len, hval);
    }
  }
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

  // Group *group;
  RSValue *vals[g->keys->len];
  for (size_t i = 0; i < g->keys->len; i++) {
    vals[i] = SearchResult_GetValue(res, g->sortTable, &g->keys->keys[i]);
  }
  Grouper_ExtractGroups(g, res, vals, 0, 0, g->keys->len, 0);

  res->indexResult = NULL;
  SearchResult_FreeInternal(res);

  return RS_RESULT_QUEUED;
}

void Grouper_Free(Grouper *g) {
  kh_destroy(khid, g->groups);
  BlkAlloc_FreeAll(&g->groupsAlloc, baGroupClean, g, GROUP_BYTESIZE(g));

  for (size_t i = 0; i < g->numReducers; i++) {
    g->reducers[i]->Free(g->reducers[i]);
  }
  RSMultiKey_Free(g->keys);

  free(g->reducers);
  free(g);
}
// Free just frees up the processor. If left as NULL we simply use free()
static void Grouper_FreeProcessor(struct resultProcessor *p) {
  Grouper *g = p->ctx.privdata;
  Grouper_Free(g);
  free(p);
  // KHTable_FreeEx(&g->groups, NULL, gtGroupClean);
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
  p->Free = Grouper_FreeProcessor;
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