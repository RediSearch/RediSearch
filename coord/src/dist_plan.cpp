#include <aggregate/aggregate.h>
#include <aggregate/aggregate_plan.h>
#include <aggregate/reducer.h>
#include <util/arr.h>
#include <vector>
#include <string>
#include <sstream>
#include <algorithm>
#include "dist_plan.h"

static char *getLastAlias(const PLN_GroupStep *gstp) {
  return gstp->reducers[array_len(gstp->reducers) - 1].alias;
}

static const char *stripAtPrefix(const char *s) {
  while (*s && *s == '@') {
    s++;
  }
  return s;
}

struct ReducerDistCtx {
  AGGPlan *localPlan;
  AGGPlan *remotePlan;
  PLN_GroupStep *localGroup;
  PLN_GroupStep *remoteGroup;
  PLN_Reducer *srcReducer;

  /**
   * If a reduce distributor needs to add another step, place it here so we
   * can skip this step as not being an old local step
   */
  PLN_BaseStep *currentLocal;

  // Keep a list of steps added; so they can be removed upon error
  std::vector<PLN_BaseStep *> addedLocalSteps;   // To pop from local plan..
  std::vector<PLN_BaseStep *> addedRemoteSteps;  // To pop from remote plan
  BlkAlloc *alloc;

  ArgsCursor *copyArgs(ArgsCursor *args) {
    // Because args are only in temporary storage
    size_t allocsz = sizeof(void *) * args->argc;
    void **arr = (void **)BlkAlloc_Alloc(alloc, allocsz, std::max(allocsz, size_t(32)));
    memcpy(arr, args->objs, args->argc * sizeof(*args->objs));
    args->objs = arr;
    return args;
  }

  template <typename... T>
  bool add(PLN_GroupStep *gstp, const char *name, const char **alias, QueryError *status,
           T... uargs) {
    ArgsCursorCXX args(uargs...);
    ArgsCursor *cargs = copyArgs(&args);
    if (PLNGroupStep_AddReducer(gstp, name, cargs, status) != REDISMODULE_OK) {
      return false;
    }
    if (alias) {
      *alias = getLastAlias(gstp);
    }
    return true;
  }

  template <typename... T>
  bool addLocal(const char *name, QueryError *status, T... uargs) {
    return add(localGroup, name, NULL, status, uargs...);
  }
  template <typename... T>
  bool addRemote(const char *name, const char **alias, QueryError *status, T... uargs) {
    return add(remoteGroup, name, alias, status, uargs...);
  }

  const char *srcarg(size_t n) const {
    auto *s = (const char *)srcReducer->args.objs[n];
    return stripAtPrefix(s);
  }
};

typedef int (*reducerDistributionFunc)(ReducerDistCtx *rdctx, QueryError *status);
reducerDistributionFunc getDistributionFunc(const char *key);

static PLN_BaseStep *distributeGroupStep(AGGPlan *origPlan, AGGPlan *remote, PLN_BaseStep *step,
                                         PLN_DistributeStep *dstp, int *cont, QueryError *status) {
  PLN_GroupStep *gr = (PLN_GroupStep *)step;
  PLN_GroupStep *grLocal = PLNGroupStep_New(gr->properties, gr->nproperties);
  PLN_GroupStep *grRemote = PLNGroupStep_New(gr->properties, gr->nproperties);

  size_t nreducers = array_len(gr->reducers);
  grLocal->reducers = array_new(PLN_Reducer, nreducers);
  grRemote->reducers = array_new(PLN_Reducer, nreducers);

  // Add new local step
  AGPLN_AddAfter(origPlan, step, &grLocal->base);  // Add the new local step
  AGPLN_PopStep(origPlan, step);

  ReducerDistCtx rdctx;
  rdctx.alloc = &dstp->alloc;
  rdctx.localPlan = origPlan;
  rdctx.remotePlan = remote;
  rdctx.localGroup = grLocal;
  rdctx.remoteGroup = grRemote;
  rdctx.currentLocal = &grLocal->base;

  int rc = REDISMODULE_OK;
  bool ok = true;

  for (size_t ii = 0; ii < nreducers; ii++) {
    rdctx.srcReducer = gr->reducers + ii;
    reducerDistributionFunc fn = getDistributionFunc(gr->reducers[ii].name);
    if (fn) {
      rc = fn(&rdctx, status);
    } else {
      // printf("Couldn't find distribution implementationf for %s\n", gr->reducers[ii].name);
      AGPLN_AddBefore(origPlan, &grLocal->base, step);
      AGPLN_PopStep(origPlan, &grLocal->base);
      grLocal->base.dtor(&grLocal->base);
      grRemote->base.dtor(&grRemote->base);

      // Clear any added steps..
      for (auto stp : rdctx.addedRemoteSteps) {
        AGPLN_PopStep(remote, stp);
        stp->dtor(stp);
      }

      for (auto stp : rdctx.addedLocalSteps) {
        AGPLN_PopStep(origPlan, stp);
        stp->dtor(stp);
      }
      *cont = 0;
      return NULL;
    }

    if (rc != REDISMODULE_OK) {
      return NULL;
    }
  }

  *cont = 0;
  // Once we're sure we want to discard the local group step and replace it with
  // our own
  array_ensure_append(dstp->oldSteps, &step, 1, PLN_GroupStep *);

  // we didn't manage to distribute all reducers, we have to revert to
  // classic "get all rows" mode
  if (rc != REDISMODULE_OK) {
    // printf("Couldn't distribute: %s\n", QueryError_GetError(status));
    grRemote->base.dtor(&grRemote->base);
    return NULL;
  }

  // Add remote step
  AGPLN_AddStep(remote, &grRemote->base);

  // Return step after current local step
  if (!ok) {
    return NULL;
  }
  return PLN_NEXT_STEP(rdctx.currentLocal);
}

/**
 * Moves a step from the source to the destination; returns the next step in the
 * source
 */
static PLN_BaseStep *moveStep(AGGPlan *dst, AGGPlan *src, PLN_BaseStep *step) {
  PLN_BaseStep *next = PLN_NEXT_STEP(step);
  assert(next != step);
  AGPLN_PopStep(src, step);
  AGPLN_AddStep(dst, step);
  return next;
}

static void freeDistStep(PLN_BaseStep *bstp) {
  PLN_DistributeStep *dstp = (PLN_DistributeStep *)bstp;
  if (dstp->plan) {
    AGPLN_FreeSteps(dstp->plan);
    free(dstp->plan);
    dstp->plan = NULL;
  }
  if (dstp->serialized) {
    auto &v = *dstp->serialized;
    for (auto s : v) {
      rm_free(s);
    }
    delete &v;
  }
  if (dstp->oldSteps) {
    for (size_t ii = 0; ii < array_len(dstp->oldSteps); ++ii) {
      dstp->oldSteps[ii]->base.dtor(&dstp->oldSteps[ii]->base);
    }
    array_free(dstp->oldSteps);
  }
  BlkAlloc_FreeAll(&dstp->alloc, NULL, NULL, 0);
  RLookup_Cleanup(&dstp->lk);
  free(dstp);
}

static RLookup *distStepGetLookup(PLN_BaseStep *bstp) {
  return &((PLN_DistributeStep *)bstp)->lk;
}

#define CHECK_ARG_COUNT(N)                                                               \
  if (src->args.argc != N) {                                                             \
    QueryError_SetErrorFmt(status, QUERY_EPARSEARGS, "Invalid arguments for reducer %s", \
                           src->name);                                                   \
    return REDISMODULE_ERR;                                                              \
  }

/* Distribute COUNT into remote count and local SUM */
static int distributeCount(ReducerDistCtx *rdctx, QueryError *status) {
  if (rdctx->srcReducer->args.argc != 0) {
    QueryError_SetErrorFmt(status, QUERY_EPARSEARGS, "Count accepts 0 values only");
    return REDISMODULE_ERR;
  }
  const char *countAlias;
  if (!rdctx->addRemote("COUNT", &countAlias, status, "0")) {
    return REDISMODULE_ERR;
  }
  if (!rdctx->addLocal("SUM", status, "1", countAlias, "AS", rdctx->srcReducer->alias)) {
    return REDISMODULE_ERR;
  }
  return REDISMODULE_OK;
}

/* Generic function to distribute an aggregator with a single argument as itself. This is the most
 * common case */
static int distributeSingleArgSelf(ReducerDistCtx *rdctx, QueryError *status) {
  // MAX must have a single argument
  PLN_Reducer *src = rdctx->srcReducer;
  CHECK_ARG_COUNT(1);

  const char *alias;
  if (!rdctx->addRemote(src->name, &alias, status, "1", rdctx->srcarg(0))) {
    return REDISMODULE_ERR;
  }
  if (!rdctx->addLocal(src->name, status, "1", alias, "AS", src->alias)) {
    return REDISMODULE_ERR;
  }
  return REDISMODULE_OK;
}

#define RANDOM_SAMPLE_SIZE 500

#define STRINGIFY_(a) STRINGIFY__(a)
#define STRINGIFY__(a) #a
#define RANDOM_SAMPLE_SIZE_STR STRINGIFY_(RANDOM_SAMPLE_SIZE)

/* Distribute QUANTILE into remote RANDOM_SAMPLE and local QUANTILE */
static int distributeQuantile(ReducerDistCtx *rdctx, QueryError *status) {
  PLN_Reducer *src = rdctx->srcReducer;
  CHECK_ARG_COUNT(2);
  const char *alias = NULL;

  if (!rdctx->addRemote("RANDOM_SAMPLE", &alias, status, "2", rdctx->srcarg(0),
                        RANDOM_SAMPLE_SIZE_STR)) {
    return REDISMODULE_ERR;
  }

  if (!rdctx->addLocal("QUANTILE", status, "2", alias, rdctx->srcarg(1), "AS", src->alias)) {
    return REDISMODULE_ERR;
  }

  return REDISMODULE_OK;
}

/* Distribute STDDEV into remote RANDOM_SAMPLE and local STDDEV */
static int distributeStdDev(ReducerDistCtx *rdctx, QueryError *status) {
  PLN_Reducer *src = rdctx->srcReducer;
  const char *alias = NULL;
  CHECK_ARG_COUNT(1);
  if (!rdctx->addRemote("RANDOM_SAMPLE", &alias, status, "2", rdctx->srcarg(0),
                        RANDOM_SAMPLE_SIZE_STR)) {
    return REDISMODULE_ERR;
  }
  if (!rdctx->addLocal("STDDEV", status, "1", alias, "AS", src->alias)) {
    return REDISMODULE_ERR;
  }
  return REDISMODULE_OK;
}

/* Distribute COUNT_DISTINCTISH into HLL and MERGE_HLL */
static int distributeCountDistinctish(ReducerDistCtx *rdctx, QueryError *status) {
  PLN_Reducer *src = rdctx->srcReducer;
  CHECK_ARG_COUNT(1);
  const char *alias;
  if (!rdctx->addRemote("HLL", &alias, status, "1", rdctx->srcarg(0))) {
    return REDISMODULE_ERR;
  }
  if (!rdctx->addLocal("HLL_SUM", status, "1", alias, "AS", src->alias)) {
    return REDISMODULE_ERR;
  }
  return REDISMODULE_OK;
}

static int distributeAvg(ReducerDistCtx *rdctx, QueryError *status) {
  PLN_Reducer *src = rdctx->srcReducer;
  PLN_GroupStep *local = rdctx->localGroup, *remote = rdctx->remoteGroup;
  CHECK_ARG_COUNT(1);

  // COUNT to know how many results
  const char *remoteCountAlias;
  if (!rdctx->addRemote("COUNT", &remoteCountAlias, status, "0")) {
    return REDISMODULE_ERR;
  }

  const char *remoteSumAlias;
  if (!rdctx->addRemote("SUM", &remoteSumAlias, status, "1", rdctx->srcarg(0))) {
    return REDISMODULE_ERR;
  }

  // These are the two numbers, the sum and the count...
  const char *localCountSumAlias;
  const char *localSumSumAlias;
  if (!rdctx->add(local, "SUM", &localCountSumAlias, status, "1", remoteCountAlias)) {
    return REDISMODULE_ERR;
  }
  if (!rdctx->add(local, "SUM", &localSumSumAlias, status, "1", remoteSumAlias)) {
    return REDISMODULE_ERR;
  }
  std::string ss = std::string("(@") + localSumSumAlias + "/@" + localCountSumAlias + ")";
  char *expr = rm_strdup(ss.c_str());
  PLN_MapFilterStep *applyStep = PLNMapFilterStep_New(expr, PLN_T_APPLY);
  applyStep->shouldFreeRaw = 1;
  applyStep->base.alias = rm_strdup(src->alias);

  assert(rdctx->currentLocal);
  AGPLN_AddAfter(rdctx->localPlan, rdctx->currentLocal, &applyStep->base);
  rdctx->currentLocal = PLN_NEXT_STEP(rdctx->currentLocal);
  rdctx->addedLocalSteps.push_back(&applyStep->base);
  return REDISMODULE_OK;
}

// Registry of available distribution functions
static struct {
  const char *key;
  reducerDistributionFunc func;
} reducerDistributors_g[] = {
    {"COUNT", distributeCount},
    {"SUM", distributeSingleArgSelf},
    {"MAX", distributeSingleArgSelf},
    {"MIN", distributeSingleArgSelf},
    {"AVG", distributeAvg},
    {"TOLIST", distributeSingleArgSelf},
    {"STDDEV", distributeStdDev},
    {"COUNT_DISTINCTISH", distributeCountDistinctish},
    {"QUANTILE", distributeQuantile},

    {NULL, NULL}  // sentinel value

};

reducerDistributionFunc getDistributionFunc(const char *key) {
  for (int i = 0; reducerDistributors_g[i].key != NULL; i++) {
    if (!strcasecmp(key, reducerDistributors_g[i].key)) {
      return reducerDistributors_g[i].func;
    }
  }

  return NULL;
}

int AGGPLN_Distribute(AGGPlan *src, QueryError *status) {
  AGGPlan *remote = (AGGPlan *)malloc(sizeof(*remote));
  AGPLN_Init(remote);

  auto current = const_cast<PLN_BaseStep *>(AGPLN_FindStep(src, NULL, NULL, PLN_T_ROOT));
  int cont = 1;

  PLN_DistributeStep *dstp = (PLN_DistributeStep *)calloc(1, sizeof(*dstp));
  dstp->base.type = PLN_T_DISTRIBUTE;
  dstp->plan = remote;
  dstp->serialized = new std::vector<char *>();
  dstp->base.dtor = freeDistStep;
  dstp->base.getLookup = distStepGetLookup;
  BlkAlloc_Init(&dstp->alloc);

  while (current && &current->llnodePln != &src->steps && cont) {
    switch (current->type) {
      case PLN_T_ROOT:
        current = PLN_NEXT_STEP(current);
        break;
      case PLN_T_LOAD:
      case PLN_T_APPLY: {
        current = moveStep(remote, src, current);
        break;
      }
      case PLN_T_ARRANGE: {
        PLN_ArrangeStep *astp = (PLN_ArrangeStep *)current;
        PLN_ArrangeStep *newStp = (PLN_ArrangeStep *)rm_calloc(1, sizeof(*newStp));

        *newStp = *astp;
        AGPLN_AddStep(remote, &newStp->base);
        if (astp->sortKeys) {
          newStp->sortKeys = array_new(const char *, array_len(astp->sortKeys));
          for (size_t ii = 0; ii < array_len(astp->sortKeys); ++ii) {
            newStp->sortKeys = array_append(newStp->sortKeys, astp->sortKeys[ii]);
          }

          if (array_len(astp->sortKeys)) {
            cont = 0;
            current = NULL;
            break;
          }
        }
        current = PLN_NEXT_STEP(&astp->base);
        break;
      }
      case PLN_T_GROUP:
        current = distributeGroupStep(src, remote, current, dstp, &cont, status);
        if (!current && QueryError_HasError(status)) {
          return REDISMODULE_ERR;
        }
        break;
      default:
        cont = 0;
        break;
    }
  }

  RLookup_Init(&dstp->lk, nullptr);

  // Find the bottom-most step with the current lookup and progress onwards
  PLN_BaseStep *lastLkStep = DLLIST_ITEM(remote->steps.prev, PLN_BaseStep, llnodePln);

  while (&lastLkStep->llnodePln != &remote->steps) {
    if (lastLkStep->getLookup && lastLkStep->getLookup(lastLkStep)) {
      break;
    }
    lastLkStep = PLN_PREV_STEP(lastLkStep);
  }

  /**
   * Start iterating over the remote steps, beginning from the most recent
   * lookup-containing step. Gather the names of aliases that this step will
   * produce and place inside the result set. This is later used to associate
   * it with the "missing" keys in the local step.
   */
  RLookup *lookup = &dstp->lk;
  for (DLLIST_node *nn = &lastLkStep->llnodePln; nn != &remote->steps; nn = nn->next) {
    PLN_BaseStep *cur = DLLIST_ITEM(nn, PLN_BaseStep, llnodePln);
    switch (cur->type) {
      case PLN_T_LOAD: {
        PLN_LoadStep *lstp = (PLN_LoadStep *)cur;
        for (size_t ii = 0; ii < AC_NumArgs(&lstp->args); ++ii) {
          const char *s = stripAtPrefix(AC_StringArg(&lstp->args, ii));
          RLookup_GetKey(lookup, s, RLOOKUP_F_OCREAT);
        }
        break;
      }
      case PLN_T_GROUP: {
        PLN_GroupStep *gstp = (PLN_GroupStep *)cur;
        for (size_t ii = 0; ii < gstp->nproperties; ++ii) {
          const char *propname = stripAtPrefix(gstp->properties[ii]);
          RLookup_GetKey(lookup, propname, RLOOKUP_F_OCREAT);
        }
        for (size_t ii = 0; ii < array_len(gstp->reducers); ++ii) {
          PLN_Reducer *r = gstp->reducers + ii;
          // Register the aliases they are registered under as well
          RLookup_GetKey(lookup, r->alias, RLOOKUP_F_OCREAT);
        }
        break;
      }
      case PLN_T_APPLY: {
        PLN_MapFilterStep *mstp = (PLN_MapFilterStep *)cur;
        RLookup_GetKey(lookup, mstp->base.alias, RLOOKUP_F_OCREAT);
        break;
      }
      case PLN_T_FILTER:
      case PLN_T_DISTRIBUTE:
      case PLN_T_ARRANGE:
      default:
        break;
    }
  }

  AGPLN_PopStep(src, &src->firstStep_s.base);
  AGPLN_Prepend(src, &dstp->base);
  auto tmp = (char **)AGPLN_Serialize(dstp->plan);
  auto &v = *dstp->serialized;
  for (size_t ii = 0; ii < array_len(tmp); ++ii) {
    v.push_back(tmp[ii]);
  }
  array_free(tmp);
  return REDISMODULE_OK;
}

int AREQ_BuildDistributedPipeline(AREQ *r, AREQDIST_UpstreamInfo *us, QueryError *status) {

  auto dstp = (PLN_DistributeStep *)AGPLN_FindStep(&r->ap, NULL, NULL, PLN_T_DISTRIBUTE);
  assert(dstp);

  dstp->lk.options |= RLOOKUP_OPT_UNRESOLVED_OK;
  int rc = AREQ_BuildPipeline(r, AREQ_BUILDPIPELINE_NO_ROOT, status);
  dstp->lk.options &= ~RLOOKUP_OPT_UNRESOLVED_OK;
  if (rc != REDISMODULE_OK) {
    return REDISMODULE_ERR;
  }

  std::vector<const RLookupKey *> loadFields;
  for (RLookupKey *kk = dstp->lk.head; kk != NULL; kk = kk->next) {
    if (kk->flags & RLOOKUP_F_UNRESOLVED) {
      loadFields.push_back(kk);
    }
  }

  auto &serargs = *dstp->serialized;
  if (!loadFields.empty()) {
    serargs.push_back(rm_strdup("LOAD"));
    char *ldsze;
    rm_asprintf(&ldsze, "%lu", (unsigned long)loadFields.size());
    serargs.push_back(ldsze);
    for (auto kk : loadFields) {
      serargs.push_back(rm_strdup(kk->name));
    }
  }

  us->lookup = &dstp->lk;
  us->serialized = const_cast<const char **>(&serargs[0]);
  us->nserialized = serargs.size();
  return REDISMODULE_OK;
}
