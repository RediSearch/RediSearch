#include <redisearch.h>
#include <result_processor.h>
#include <util/block_alloc.h>
#include <util/khash.h>
#include "reducer.h"

/**
 * A group represents the allocated context of all reducers in a group, and the
 * selected values of that group.
 *
 * Because one of these is created for every single group (i.e. every single
 * unique key) we want to keep this quite small!
 */
typedef struct {
  /** Contains the selected 'out' values used by the reducers output functions */
  RLookupRow rowdata;

  /**
   * Contains the actual per-reducer data for the group, in an accumulating
   * fashion (e.g. how many records seen, and so on). This is created by
   * Reducer::NewInstance()
   */
  void *accumdata[0];
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

#define GROUPER_NREDUCERS(g) (array_len((g)->reducers))
#define GROUP_BYTESIZE(parent) (sizeof(Group) + (sizeof(void *) * GROUPER_NREDUCERS(parent)))
#define GROUPS_PER_BLOCK 1024
#define GROUPER_NSRCKEYS(g) ((g)->nkeys)

typedef struct Grouper {
  // Result processor base, for use in row processing
  ResultProcessor base;

  // Map of group_name => `Group` structure
  khash_t(khid) * groups;

  // Backing store for the groups themselves
  BlkAlloc groupsAlloc;

  /**
   * Keys to group by. Both srckeys and dstkeys are used because different lookups
   * are employed. The srckeys are the lookup keys for the properties as they
   * appear in the row received from the upstream processor, and the dstkeys are
   * the keys as they are expected in the output row.
   */
  const RLookupKey **srckeys;
  const RLookupKey **dstkeys;
  size_t nkeys;

  // array of reducers
  Reducer **reducers;

  // Used for maintaining state when yielding groups
  khiter_t iter;
} Grouper;

/**
 * Create a new group. groupvals is the key of the group. This will be the
 * number of field arguments passed to GROUPBY, e.g.
 * GROUPBY 2 @foo @bar will have a `groupvals` of `{"foo", "bar"}`.
 *
 * These will be placed in the output row.
 */
static Group *createGroup(Grouper *g, RSValue **groupvals, size_t ngrpvals) {
  size_t numReducers = array_len(g->reducers);
  size_t elemSize = GROUP_BYTESIZE(g);
  Group *group = BlkAlloc_Alloc(&g->groupsAlloc, elemSize, GROUPS_PER_BLOCK * elemSize);
  memset(group, 0, elemSize);

  for (size_t ii = 0; ii < numReducers; ++ii) {
    group->accumdata[ii] = g->reducers[ii]->NewInstance(g->reducers[ii]);
  }

  /** Initialize the row data! */
  for (size_t ii = 0; ii < ngrpvals; ++ii) {
    const RLookupKey *dstkey = g->dstkeys[ii];
    RLookup_WriteKey(dstkey, &group->rowdata, groupvals[ii]);
    // printf("Write: %s => ", dstkey->name);
    // RSValue_Print(groupvals[ii]);
    // printf("\n");
  }
  return group;
}

static int Grouper_rpYield(ResultProcessor *base, SearchResult *r) {
  Grouper *g = (Grouper *)base;

  while (g->iter != kh_end(g->groups)) {
    if (!kh_exist(g->groups, g->iter)) {
      g->iter++;
      continue;
    }

    Group *gr = kh_value(g->groups, g->iter);
    for (size_t ii = 0; ii < GROUPER_NREDUCERS(g); ++ii) {
      Reducer *rd = g->reducers[ii];
      RSValue *v = rd->Finalize(rd, gr->accumdata[ii]);
      if (v) {
        RLookup_WriteKey(rd->dstkey, &r->rowdata, v);
        RSValue_Decref(v);

        for (size_t ii = 0; ii < g->nkeys; ++ii) {
          const RLookupKey *dstkey = g->dstkeys[ii];
          RSValue *groupval = RLookup_GetItem(dstkey, &gr->rowdata);
          if (groupval) {
            RLookup_WriteKey(dstkey, &r->rowdata, groupval);
          } else {
            printf("No such group value??? Sad!\n");
          }
        }
      } else {
        // Error! Couldn't get value? Handle me here!
        printf("Finalize() returned bad value!\n");
      }
    }
    ++g->iter;
    return RS_RESULT_OK;
  }

  return RS_RESULT_EOF;
}

static void invokeGroupReducers(Grouper *g, RLookupRow *srcrow) {
  uint64_t hval = 0;
  size_t nkeys = GROUPER_NSRCKEYS(g);
  RSValue *groupvals[nkeys];

  for (size_t ii = 0; ii < nkeys; ++ii) {
    const RLookupKey *srckey = g->srckeys[ii];
    RSValue *v = RLookup_GetItem(srckey, srcrow);
    if (v == NULL) {
      printf("Couldn't get value for %s\n", srckey->name);
      v = RS_NullVal();
    }
    hval = RSValue_Hash(v, hval);
    // printf("Hash is %llu. Value is", hval);
    // RSValue_Print(v);
    // printf("\n");
    groupvals[ii] = v;
  }

  Group *gr = NULL;
  // Got all the values. Now let's see if the group for this set of values
  // exists:
  khiter_t k = kh_get(khid, g->groups, hval);  // first have to get ieter
  if (k == kh_end(g->groups)) {                // k will be equal to kh_end if key not present
    gr = createGroup(g, groupvals, nkeys);
    kh_set(khid, g->groups, hval, gr);
  } else {
    gr = kh_value(g->groups, k);
  }

  size_t nreducers = GROUPER_NREDUCERS(g);
  for (size_t ii = 0; ii < nreducers; ii++) {
    g->reducers[ii]->Add(g->reducers[ii], gr->accumdata[ii], srcrow);
  }
}

static int Grouper_rpAccum(ResultProcessor *base, SearchResult *res) {
  // static SearchResult up;
  Grouper *g = (Grouper *)base;

  int rc;

  while ((rc = base->upstream->Next(base->upstream, res)) == RS_RESULT_OK) {
    invokeGroupReducers(g, &res->rowdata);
    SearchResult_Clear(res);
  }
  if (rc == RS_RESULT_EOF) {
    base->Next = Grouper_rpYield;
    base->parent->totalResults = kh_size(g->groups);
    g->iter = kh_begin(khid);
    return Grouper_rpYield(base, res);
  } else {
    return rc;
  }
}

static void cleanCallback(void *ptr, void *arg) {
  Group *group = ptr;
  Grouper *parent = arg;
  // Call the reducer's FreeInstance
  for (size_t ii = 0; ii < GROUPER_NREDUCERS(parent); ++ii) {
    parent->reducers[ii]->FreeInstance(parent->reducers[ii], group->accumdata[ii]);
  }
}

static void Grouper_rpFree(ResultProcessor *grrp) {
  Grouper *g = (Grouper *)grrp;
  kh_destroy(khid, g->groups);
  BlkAlloc_FreeAll(&g->groupsAlloc, cleanCallback, g, GROUP_BYTESIZE(g));

  for (size_t i = 0; i < GROUPER_NREDUCERS(g); i++) {
    g->reducers[i]->Free(g->reducers[i]);
  }
  free(g->srckeys);
  free(g->dstkeys);
  free(g);
}

Grouper *Grouper_New(const RLookupKey **srckeys, const RLookupKey **dstkeys, size_t nkeys) {
  Grouper *g = calloc(1, sizeof(*g));
  BlkAlloc_Init(&g->groupsAlloc);
  g->groups = kh_init(khid);

  g->srckeys = calloc(1, sizeof(*g->srckeys));
  g->dstkeys = calloc(1, sizeof(*g->dstkeys));
  g->nkeys = nkeys;
  for (size_t ii = 0; ii < nkeys; ++ii) {
    g->srckeys[ii] = srckeys[ii];
    g->dstkeys[ii] = dstkeys[ii];
  }

  g->base.name = "Grouper";
  g->base.Next = Grouper_rpAccum;
  g->base.Free = Grouper_rpFree;
  return g;
}

void Grouper_AddReducer(Grouper *g, Reducer *r) {
  Reducer **rpp = array_ensure_tail(&g->reducers, Reducer *);
  *rpp = r;
}

ResultProcessor *Grouper_GetRP(Grouper *g) {
  return &g->base;
}