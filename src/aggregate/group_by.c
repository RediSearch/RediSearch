#include <redisearch.h>
#include <result_processor.h>
#include "util/khtable.h"
#include "util/block_alloc.h"

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
  KHTableEntry entBase;  // For entry into the table
  uint64_t h64key;       // "Key" of the group - using a hash rather than the actual string
  size_t len;            // Number of contexts
  RSFieldMap *values;
  GroupCtx ctxs[0];
} Group;

#define GROUP_CTX(g, i) (g->ctxs[i].ptr)
#define GROUP_BYTESIZE(parent) (sizeof(Group) + (sizeof(GroupCtx) * (parent)->numReducers))
#define GROUPS_PER_BLOCK 64
typedef struct Grouper {
  KHTable groups;
  KHTableIterator giter;
  BlkAlloc groupsAlloc;
  RSMultiKey *keys;
  RSSortingTable *sortTable;
  Reducer **reducers;
  size_t numReducers;
  int accumulating;
  int sortKeyIdx;
  int hasIter;
} Grouper;

static KHTableEntry *gtGroupAlloc(void *ctx) {
  Grouper *g = ctx;
  size_t elemSize = sizeof(Group) + (sizeof(GroupCtx) * g->numReducers);
  Group *group = BlkAlloc_Alloc(&g->groupsAlloc, elemSize, GROUPS_PER_BLOCK * elemSize);
  memset(group, 0, elemSize);

  for (size_t ii = 0; ii < g->numReducers; ++ii) {
    group->ctxs[ii].ptr = g->reducers[ii]->NewInstance(&g->reducers[ii]->ctx);
    group->ctxs[ii].free = g->reducers[ii]->FreeInstance;
  }
  return &group->entBase;
}

static int gtGroupCompare(const KHTableEntry *item, const void *s, size_t n, uint32_t h) {
  assert(n == 8);
  // Return 0 if a match; invert the condition
  return *(uint64_t *)s != ((Group *)item)->h64key;
}

uint32_t gtGroupHash(const KHTableEntry *item) {
  return (uint32_t)((Group *)item)->h64key;
}

static KHTableProcs gtGroupProcs_g = {
    .Alloc = gtGroupAlloc, .Compare = gtGroupCompare, .Hash = gtGroupHash};

static void Group_Init(Group *group, Grouper *g, SearchResult *res, uint64_t hash) {
  // Copy the group keys to the new group
  group->len = g->numReducers;
  assert(group->values == NULL);
  group->values = RS_NewFieldMap(group->len);

  for (size_t i = 0; i < g->keys->len; i++) {
    // TODO: Init sorting table
    RSValue *src = SearchResult_GetValue(res, g->sortTable, g->keys->keys[i]);
    static RSValue dst;
    if (src) {
      RSValue_DeepCopy(&dst, src);
      RSFieldMap_Add(&group->values, g->keys->keys[i], dst);
    }
  }

  // Set the 64 bit hash to compare for later
  group->h64key = hash;
}

static void gtGroupClean(KHTableEntry *ent, void *unused_a, void *unused_b) {
  Group *group = (Group *)ent;
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
int grouper_Yield(Grouper *g, SearchResult *r) {
  if (!g->hasIter) {
    KHTableIter_Init(&g->groups, &g->giter);
    g->hasIter = 1;
  }

  char *s;
  tm_len_t l;
  KHTableEntry *p;

  do {
    // try the next group
    if (!(p = KHtableIter_Next(&g->giter))) {
      // nothing to read further from the groups...
      return RS_RESULT_EOF;
    }
    if (r->fields) {
      r->fields->len = 0;
    } else {
      r->fields = RS_NewFieldMap(2 + g->numReducers + g->keys->len);
    }
    // Add a property with the group name
    // RSFieldMap_Add(&r->fields, g->alias ? g->alias : g->property, RS_CStringVal(strndup(s, l)));
    Group *gr = (Group *)p;
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

  static RSValue nil = {.t = RSValue_Null};
  for (size_t i = 0; i < g->keys->len; i++) {
    // TODO: Init sorting table
    RSValue *v = SearchResult_GetValue(res, g->sortTable, RSKEY(g->keys->keys[i]));

    ret = RSValue_Hash(v ? v : &nil, ret);
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

    g->accumulating = 0;
    return grouper_Yield(g, res);
  }

  uint64_t hash = grouper_EncodeGroupKey(g, res);
  int isNew = 0;
  Group *group = (Group *)KHTable_GetEntry(&g->groups, &hash, sizeof hash, (uint32_t)hash, &isNew);
  assert(group);
  if (isNew) {
    Group_Init(group, g, res, hash);
  }
  for (size_t i = 0; i < g->numReducers; i++) {
    g->reducers[i]->Add(GROUP_CTX(group, i), res);
  }

  return RS_RESULT_QUEUED;
}

// Free just frees up the processor. If left as NULL we simply use free()
static void Grouper_Free(struct resultProcessor *p) {
  Grouper *g = p->ctx.privdata;
  KHTable_FreeEx(&g->groups, NULL, gtGroupClean);
  BlkAlloc_FreeAll(&g->groupsAlloc, baGroupClean, g, GROUP_BYTESIZE(g));

  for (size_t i = 0; i < g->numReducers; i++) {
    g->reducers[i]->Free(g->reducers[i]);
  }

  free(g->reducers);
  free(g);
  free(p);
}

Grouper *NewGrouper(RSMultiKey *keys, RSSortingTable *tbl) {
  Grouper *g = malloc(sizeof(*g));
  BlkAlloc_Init(&g->groupsAlloc);
  KHTable_Init(&g->groups, &gtGroupProcs_g, g, 512);

  g->sortTable = tbl;
  g->keys = keys;
  g->reducers = NULL;
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
  g->reducers = realloc(g->reducers, g->numReducers * sizeof(Reducer *));
  g->reducers[g->numReducers - 1] = r;
}