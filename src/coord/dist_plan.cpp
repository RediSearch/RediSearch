/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "aggregate/aggregate.h"
#include "aggregate/aggregate_plan.h"
#include "pipeline/pipeline_construction.h"
#include "aggregate/reducer.h"
#include "aggregate/reducers/collect_parse.h"
#include "util/arr.h"
#include "util/stringify.h"
#include "dist_plan.h"

#include <vector>
#include <string>
#include <sstream>
#include <algorithm>

// Minimum block size for the reducer-rewriter scratch allocator
// (`ReducerDistCtx::alloc`).
#define DIST_REDUCER_BLOCK_SIZE ((size_t)128)

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
    void **arr = (void **)BlkAlloc_Alloc(alloc, allocsz, std::max(allocsz, DIST_REDUCER_BLOCK_SIZE));
    memcpy(arr, args->objs, args->argc * sizeof(*args->objs));
    args->objs = arr;
    return args;
  }

  bool add(PLN_GroupStep *gstp, const char *name, const char **alias, QueryError *status, ArgsCursor *cargs) {
    if (PLNGroupStep_AddReducer(gstp, name, cargs, status) != REDISMODULE_OK) {
      return false;
    }
    array_tail(gstp->reducers).isLocal = (gstp == this->localGroup);
    if (alias) {
      *alias = getLastAlias(gstp);
    }
    return true;
  }

  template <typename... T>
  bool add(PLN_GroupStep *gstp, const char *name, const char **alias, QueryError *status,
           T... uargs) {
    ArgsCursorCXX args(uargs...);
    ArgsCursor *cargs = copyArgs(&args);
    return add(gstp, name, alias, status, cargs);
  }

  template <typename... T>
  bool addLocal(const char *name, QueryError *status, T... uargs) {
    return add(localGroup, name, NULL, status, uargs...);
  }
  template <typename... T>
  bool addRemote(const char *name, const char **alias, QueryError *status, T... uargs) {
    ArgsCursorCXX args(uargs...);
    ArgsCursor tmp = args;
    // Check if the reducer already exists in the remote group. This may happen NOT AS SYNTAX ERROR if the client
    // sends, for example, a query with COUNT and AVG, which we send to the shards as COUNT, COUNT and SUM. In this case,
    // we don't want or need to add the same reducer twice.
    auto existing = PLNGroupStep_FindReducer(remoteGroup, name, &tmp);
    if (existing) {
      if (alias) *alias = existing->alias;
      return true;
    } else {
      ArgsCursor *cargs = copyArgs(&args);
      return add(remoteGroup, name, alias, status, cargs);
    }
  }

  const char *srcarg(size_t n) const {
    auto *s = (const char *)srcReducer->args.objs[n];
    return stripAtPrefix(s);
  }
};

typedef int (*reducerDistributionFunc)(ReducerDistCtx *rdctx, QueryError *status);
reducerDistributionFunc getDistributionFunc(const char *key);

static void distributeGroupStep(AGGPlan *origPlan, AGGPlan *remote, PLN_BaseStep *step,
                                PLN_DistributeStep *dstp, QueryError *status) {
  PLN_GroupStep *gr = (PLN_GroupStep *)step;
  PLN_GroupStep *grLocal = PLNGroupStep_New(StrongRef_Clone(gr->properties_ref), gr->strictPrefix);
  PLN_GroupStep *grRemote = PLNGroupStep_New(StrongRef_Clone(gr->properties_ref), gr->strictPrefix);

  size_t nreducers = array_len(gr->reducers);
  grLocal->reducers = array_new(PLN_Reducer, nreducers);
  grRemote->reducers = array_new(PLN_Reducer, nreducers);

  // Add new local step
  AGPLN_AddAfter(origPlan, step, &grLocal->base);  // Add the new local step
  AGPLN_PopStep(step);

  ReducerDistCtx rdctx;
  rdctx.alloc = &dstp->alloc;
  rdctx.localPlan = origPlan;
  rdctx.remotePlan = remote;
  rdctx.localGroup = grLocal;
  rdctx.remoteGroup = grRemote;
  rdctx.currentLocal = &grLocal->base;

  for (size_t ii = 0; ii < nreducers; ii++) {
    rdctx.srcReducer = gr->reducers + ii;
    reducerDistributionFunc fn = getDistributionFunc(gr->reducers[ii].name);
    if (!fn || fn(&rdctx, status) != REDISMODULE_OK) {
      goto cleanup;
    }
  }

  // Once we're sure we want to discard the local group step and replace it with
  // our own
  array_ensure_append(dstp->oldSteps, &step, 1, PLN_GroupStep *);

  // Add remote step
  AGPLN_AddStep(remote, &grRemote->base);
  return;

cleanup:
    AGPLN_AddBefore(origPlan, &grLocal->base, step);
    AGPLN_PopStep(&grLocal->base);
    grLocal->base.dtor(&grLocal->base);
    grRemote->base.dtor(&grRemote->base);

    // Clear any added steps..
    for (auto stp : rdctx.addedRemoteSteps) {
      AGPLN_PopStep(stp);
      stp->dtor(stp);
    }

    for (auto stp : rdctx.addedLocalSteps) {
      AGPLN_PopStep(stp);
      stp->dtor(stp);
    }
}

/**
 * Moves a step from the source to the destination; returns the next step in the
 * source
 */
static PLN_BaseStep *moveStep(AGGPlan *dst, AGGPlan *src, PLN_BaseStep *step) {
  PLN_BaseStep *next = PLN_NEXT_STEP(step);
  RS_ASSERT(next != step);
  AGPLN_PopStep(step);
  AGPLN_AddStep(dst, step);
  return next;
}

static void freeDistStep(PLN_BaseStep *bstp) {
  PLN_DistributeStep *dstp = (PLN_DistributeStep *)bstp;
  if (dstp->plan) {
    AGPLN_FreeSteps(dstp->plan);
    rm_free(dstp->plan);
    dstp->plan = NULL;
  }
  if (dstp->serialized) {
    array_free_ex(dstp->serialized, rm_free(*(char **)ptr));
  }
  if (dstp->oldSteps) {
    for (size_t ii = 0; ii < array_len(dstp->oldSteps); ++ii) {
      dstp->oldSteps[ii]->base.dtor(&dstp->oldSteps[ii]->base);
    }
    array_free(dstp->oldSteps);
  }
  BlkAlloc_FreeAll(&dstp->alloc, NULL, NULL, 0);
  RLookup_Cleanup(&dstp->lk);
  rm_free(dstp);
}

static RLookup *distStepGetLookup(PLN_BaseStep *bstp) {
  return &((PLN_DistributeStep *)bstp)->lk;
}

#define CHECK_ARG_COUNT(N)                                                               \
  if (src->args.argc != N) {                                                             \
    QueryError_SetWithoutUserDataFmt(status, QUERY_ERROR_CODE_PARSE_ARGS, "Invalid arguments for reducer %s", \
                           src->name);                                                   \
    return REDISMODULE_ERR;                                                              \
  }

/* Distribute COUNT into remote count and local SUM */
static int distributeCount(ReducerDistCtx *rdctx, QueryError *status) {
  if (rdctx->srcReducer->args.argc != 0) {
    QueryError_SetError(status, QUERY_ERROR_CODE_PARSE_ARGS, "Count accepts 0 values only");
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

#define RANDOM_SAMPLE_SIZE_STR STRINGIFY(RANDOM_SAMPLE_SIZE)

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
  array_tail(rdctx->localGroup->reducers).isHidden = 1; // Don't show this in the output
  if (!rdctx->add(local, "SUM", &localSumSumAlias, status, "1", remoteSumAlias)) {
    return REDISMODULE_ERR;
  }
  array_tail(rdctx->localGroup->reducers).isHidden = 1; // Don't show this in the output
  std::string ss = std::string("(@") + localSumSumAlias + "/@" + localCountSumAlias + ")";
  HiddenString *expr = NewHiddenString(ss.c_str(), ss.length(), false);
  PLN_MapFilterStep *applyStep = PLNMapFilterStep_New(expr, PLN_T_APPLY);
  HiddenString_Free(expr, false);
  applyStep->noOverride = 1; // Don't override the alias. Usually we do, but in this case we don't because reducers
                             // are not allowed to override aliases
  applyStep->base.alias = rm_strdup(src->alias);

  RS_ASSERT(rdctx->currentLocal);
  AGPLN_AddAfter(rdctx->localPlan, rdctx->currentLocal, &applyStep->base);
  rdctx->currentLocal = PLN_NEXT_STEP(rdctx->currentLocal);
  rdctx->addedLocalSteps.push_back(&applyStep->base);
  return REDISMODULE_OK;
}

/* Distribute COLLECT.
 *
 * Both halves of the split GROUPBY are emitted from the parsed `CollectArgs`
 * struct rather than from the raw argv. The remote command is reconstructed
 * from scratch — FIELDS, optional SORTBY, optional LIMIT — and the local
 * command is reconstructed the same way and decorated with `AS <alias>`.
 *
 * Limited path: whenever the user supplied `LIMIT offset count`, the remote
 * `LIMIT` is rewritten to `0 (offset+count)` so each shard returns enough rows
 * for the coordinator to perform the final offset/count trim. The local
 * COLLECT keeps the original offset/count. This mirrors `serializeArrange`'s
 * behavior for top-level LIMIT and concentrates offset semantics at the
 * coordinator — internal shards never see a non-zero offset.
 */
static int distributeCollect(ReducerDistCtx *rdctx, QueryError *status) {
  const PLN_Reducer *src = rdctx->srcReducer;
  RS_ASSERT(src->alias);

  // Parse the original COLLECT args into a pure data struct. No keys are
  // opened; the parse cursor uses a synthesized lookup-less `ReducerOptions`.
  ArgsCursor parseAc = src->args;
  ReducerOptions opts = REDUCEROPTS_INIT(src->name, &parseAc, /*srclookup*/nullptr,
                                         /*loadKeys*/nullptr, status, /*strictPrefix*/false,
                                         /*is_local*/false, /*input_key*/nullptr,
                                         /*reqflags*/0);
  CollectArgs args = {0};
  if (!CollectArgs_Parse(&opts, &args)) {
    CollectArgs_Free(&args);
    return REDISMODULE_ERR;
  }

  auto dupCStr = [&](const char *s) -> const char * {
    size_t n = strlen(s) + 1;
    char *p = (char *)BlkAlloc_Alloc(rdctx->alloc, n, std::max(n, DIST_REDUCER_BLOCK_SIZE));
    memcpy(p, s, n);
    return p;
  };
  auto allocU64Str = [&](uint64_t v) -> const char * {
    char buf[32];
    int n = snprintf(buf, sizeof(buf), "%llu", (unsigned long long)v);
    char *p = (char *)BlkAlloc_Alloc(rdctx->alloc, (size_t)n + 1,
                                     std::max(size_t(n + 1), DIST_REDUCER_BLOCK_SIZE));
    memcpy(p, buf, (size_t)n + 1);
    return p;
  };
  auto allocAtName = [&](const char *stripped) -> const char * {
    size_t n = strlen(stripped);
    char *p = (char *)BlkAlloc_Alloc(rdctx->alloc, n + 2, std::max(n + 2, DIST_REDUCER_BLOCK_SIZE));
    p[0] = '@';
    memcpy(p + 1, stripped, n + 1);
    return p;
  };

  // ---- Build the FIELDS section (shared between remote and local). ----
  // Layout: ["FIELDS", "*"]  OR  ["FIELDS", <n>, "@f1", "@f2", ...].
  std::vector<const char *> fieldsTokens;
  fieldsTokens.push_back(dupCStr("FIELDS"));
  if (args.load_all) {
    fieldsTokens.push_back(dupCStr("*"));
  } else {
    size_t n = args.field_names ? array_len(args.field_names) : 0;
    fieldsTokens.push_back(allocU64Str(n));
    for (size_t i = 0; i < n; i++) {
      fieldsTokens.push_back(allocAtName(args.field_names[i]));
    }
  }

  // ---- Build the SORTBY section (shared between remote and local). ----
  // Each sort key is emitted with an explicit ASC/DESC token; the resulting
  // count is `2 * sort_names_len`.
  std::vector<const char *> sortbyTokens;
  const bool has_sortby = (args.sort_names != nullptr);
  if (has_sortby) {
    size_t m = array_len(args.sort_names);
    sortbyTokens.push_back(dupCStr("SORTBY"));
    sortbyTokens.push_back(allocU64Str(m * 2));
    for (size_t i = 0; i < m; i++) {
      sortbyTokens.push_back(allocAtName(args.sort_names[i]));
      sortbyTokens.push_back(dupCStr(SORTASCMAP_GETASC(args.sortAscMap, i) ? "ASC" : "DESC"));
    }
  }

  // ---- Helper: build the argv shape that `PLNGroupStep_AddReducer` expects.
  //   Output layout: <nargs=tokens.size()> <tokens...> [<trailing...>]
  // Trailing tokens sit *outside* the counted slice — used by the local path
  // to append `AS <user_alias>`, which `PLNGroupStep_AddReducer` consumes
  // after the args slice.
  auto buildReducerArgv = [&](const std::vector<const char *> &tokens,
                              std::initializer_list<const char *> trailing = {}) -> ArgsCursor {
    size_t n = tokens.size();
    size_t total = n + 1 + trailing.size();
    const char **objs = (const char **)BlkAlloc_Alloc(
        rdctx->alloc, sizeof(const char *) * total,
        std::max(sizeof(const char *) * total, DIST_REDUCER_BLOCK_SIZE));
    objs[0] = allocU64Str(n);  // leading <nargs>
    for (size_t i = 0; i < n; i++) {
      objs[i + 1] = tokens[i];
    }
    size_t k = n + 1;
    for (const char *t : trailing) {
      objs[k++] = t;
    }
    ArgsCursor out;
    ArgsCursor_InitCString(&out, objs, (int)total);
    return out;
  };

  // ---- Remote argv: FIELDS [SORTBY ...] [LIMIT 0 (offset+count)]
  std::vector<const char *> remoteTokens = fieldsTokens;
  remoteTokens.insert(remoteTokens.end(), sortbyTokens.begin(), sortbyTokens.end());
  if (args.has_limit) {
    remoteTokens.push_back(dupCStr("LIMIT"));
    remoteTokens.push_back(allocU64Str(0));
    remoteTokens.push_back(allocU64Str(args.limit_offset + args.limit_count));
  }
  ArgsCursor remoteArgs = buildReducerArgv(remoteTokens);

  const char *alias = nullptr;
  if (!rdctx->add(rdctx->remoteGroup, "COLLECT", &alias, status, &remoteArgs)) {
    CollectArgs_Free(&args);
    return REDISMODULE_ERR;
  }
  RS_ASSERT(alias)
  // ---- Local argv: same shape as remote, but with the original LIMIT preserved
  //     and an `AS <user_alias>` suffix.
  std::vector<const char *> localTokens = fieldsTokens;
  localTokens.insert(localTokens.end(), sortbyTokens.begin(), sortbyTokens.end());
  if (args.has_limit) {
    localTokens.push_back(dupCStr("LIMIT"));
    localTokens.push_back(allocU64Str(args.limit_offset));
    localTokens.push_back(allocU64Str(args.limit_count));
  }
  ArgsCursor localArgs = buildReducerArgv(localTokens,
                                          {dupCStr("AS"), dupCStr(src->alias)});

  if (!rdctx->add(rdctx->localGroup, "COLLECT", nullptr, status, &localArgs)) {
    CollectArgs_Free(&args);
    return REDISMODULE_ERR;
  }
  array_tail(rdctx->localGroup->reducers).inputAlias = rm_strdup(alias);

  CollectArgs_Free(&args);
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
    {"COLLECT", distributeCollect},

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

static void finalize_distribution(AGGPlan *src, AGGPlan *remote, PLN_DistributeStep *dstp);

int AGGPLN_Distribute(AGGPlan *src, QueryError *status) {
  AGGPlan *remote = (AGGPlan *)rm_malloc(sizeof(*remote));
  AGPLN_Init(remote);

  auto current = const_cast<PLN_BaseStep *>(AGPLN_FindStep(src, NULL, NULL, PLN_T_ROOT));
  bool hadArrange = false;

  PLN_DistributeStep *dstp = (PLN_DistributeStep *)rm_calloc(1, sizeof(*dstp));
  dstp->base.type = PLN_T_DISTRIBUTE;
  dstp->plan = remote;
  dstp->serialized = array_new(char *, 1);
  dstp->base.dtor = freeDistStep;
  dstp->base.getLookup = distStepGetLookup;
  BlkAlloc_Init(&dstp->alloc);

  // TODO: The while condition is buggy, since it returns the `AGGPlan`, not the `PLN_BaseStep` that is actually needed
  // Should be fixed to `DLLIST_FOREACH(it, ll) {}`.
  while (current != PLN_END_STEP(src)) {
    switch (current->type) {
      case PLN_T_ROOT:
        current = PLN_NEXT_STEP(current);
        break;
      case PLN_T_FILTER:
        ///////////////// Part of non-breaking solution for MOD-5267. ///////////////////////////////
        // TODO: remove, and enable (or verify that) a FILTER step can implicitly load missing keys
        //       that are part of the index schema.
        if (!hadArrange) {
          // Step 1: parse the filter expression and extract the required keys
          PLN_MapFilterStep *fstp = (PLN_MapFilterStep *)current;
          RSExpr *tmpExpr = ExprAST_Parse(fstp->expr, status);
          if (tmpExpr == NULL) {
            goto error;
          }
          RLookup filter_keys = RLookup_New();
          RLookup_EnableOptions(&filter_keys, RLOOKUP_OPT_ALLOWUNRESOLVED);
          ExprAST_GetLookupKeys(tmpExpr, &filter_keys, status);
          if (QueryError_HasError(status)) {
            RLookup_Cleanup(&filter_keys);
            ExprAST_Free(tmpExpr);
            goto error;
          }
          // Step 2: generate a LOAD step for the keys. If the keys are already loaded (or sortable),
          //         this step will be optimized out.
          if (RLookup_GetRowLen(&filter_keys)) {
            PLN_LoadStep *load = (PLN_LoadStep *)rm_calloc(1, sizeof(*load));
            load->base.type = PLN_T_LOAD;
            load->base.dtor = [](PLN_BaseStep *stp) {
              PLN_LoadStep *load = (PLN_LoadStep *)stp;
              for (size_t ii = 0; ii < load->args.argc; ++ii) {
                rm_free(load->args.objs[ii]);
              }
              rm_free(load->args.objs);
              rm_free(stp);
            };
            const char **argv = (const char**)rm_malloc(sizeof(*argv) * RLookup_GetRowLen(&filter_keys));
            size_t argc = 0;

            RLOOKUP_FOREACH(kk, &filter_keys, {
              argv[argc++] = rm_strndup(RLookupKey_GetName(kk), RLookupKey_GetNameLen(kk));
            });
            ArgsCursor_InitCString(&load->args, argv, argc);
            AGPLN_AddStep(remote, &load->base);
          }
          // Step 3: cleanup
          RLookup_Cleanup(&filter_keys);
          ExprAST_Free(tmpExpr);
        }
        ///////////////// End of non-breaking MOD-5267 solution /////////////////////////////////////
        // If we had an arrange step, it was split into a remote and local steps, and we must
        // have the filter step locally, otherwise we will move the filter step into in between
        // the remote and local arrange steps, which is logically incorrect.
        // Otherwise (if there was no arrange step), we can move the filter step from local to remote
        current = hadArrange ? PLN_NEXT_STEP(current) : moveStep(remote, src, current);
        break;
      case PLN_T_VECTOR_NORMALIZER:
      case PLN_T_LOAD:
      case PLN_T_APPLY:
        current = moveStep(remote, src, current);
        break;
      case PLN_T_ARRANGE: {
        PLN_ArrangeStep *astp = (PLN_ArrangeStep *)current;
        // If we already had an arrange step, or this arrange step should only run local,
        // we shouldn't distribute the next arrange steps.
        if (!hadArrange && !astp->runLocal) {
          PLN_ArrangeStep *newStp = (PLN_ArrangeStep *)rm_calloc(1, sizeof(*newStp));

          *newStp = *astp;
          AGPLN_AddStep(remote, &newStp->base);
          if (astp->sortKeys) {
            newStp->sortKeys = array_new(const char *, array_len(astp->sortKeys));
            for (size_t ii = 0; ii < array_len(astp->sortKeys); ++ii) {
              array_append(newStp->sortKeys, astp->sortKeys[ii]);
            }
          }
        }
        hadArrange = true;
        // whether we pushed an arrange step to the remote or not, we still need to move on
        current = PLN_NEXT_STEP(current);
        break;
      }
      case PLN_T_GROUP:
        // If we had an arrange step, we must have the group step locally
        if (!hadArrange) {
          distributeGroupStep(src, remote, current, dstp, status);
          if (QueryError_HasError(status)) {
            goto error;
          }
        }
        // After the group step, the rest of the steps are local only.
      default:
        goto loop_break;
    }
  }
loop_break:

  finalize_distribution(src, remote, dstp);
  return REDISMODULE_OK;

error:
  freeDistStep((PLN_BaseStep *)dstp);
  return REDISMODULE_ERR;
}

// We have split the logic plan into a remote and local plans. Now we need to make final
// preparations and setups for the plans and the distributed step.
static void finalize_distribution(AGGPlan *local, AGGPlan *remote, PLN_DistributeStep *dstp) {
  RLookup_SetCache(&dstp->lk, nullptr);

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
      case PLN_T_VECTOR_NORMALIZER: {
        PLN_VectorNormalizerStep *vnStep = (PLN_VectorNormalizerStep *)cur;
        RLookup_GetKey_Write(lookup, vnStep->distanceFieldAlias, RLOOKUP_F_NOFLAGS);
        break;
      }
      case PLN_T_LOAD: {
        PLN_LoadStep *lstp = (PLN_LoadStep *)cur;
        // Use the original ArgsCursor directly
        ArgsCursor ac = lstp->args;

        // Process all arguments in the ArgsCursor
        while (!AC_IsAtEnd(&ac)) {
          const char *name = AC_GetStringNC(&ac, NULL);

          // Check for AS alias
          if (AC_AdvanceIfMatch(&ac, SPEC_AS_STR)) {
            RS_ASSERT(!AC_IsAtEnd(&ac));
            name = AC_GetStringNC(&ac, NULL); // structure is validated earlier, can safely assume it's not at the end
          }
          name = stripAtPrefix(name);
          RLookup_GetKey_Write(lookup, name, RLOOKUP_F_NOFLAGS);
        }
        break;
      }
      case PLN_T_GROUP: {
        PLN_GroupStep *gstp = (PLN_GroupStep *)cur;
        arrayof(const char*) properties = PLNGroupStep_GetProperties(gstp);
        for (size_t ii = 0; ii < array_len(properties); ++ii) {
          const char *propname = stripAtPrefix(properties[ii]);
          RLookup_GetKey_Write(lookup, propname, RLOOKUP_F_NOFLAGS);
        }
        for (size_t ii = 0; ii < array_len(gstp->reducers); ++ii) {
          PLN_Reducer *r = gstp->reducers + ii;
          // Register the aliases they are registered under as well
          RLookup_GetKey_Write(lookup, r->alias, RLOOKUP_F_NOFLAGS);
        }
        break;
      }
      case PLN_T_APPLY: {
        PLN_MapFilterStep *mstp = (PLN_MapFilterStep *)cur;
        RLookup_GetKey_Write(lookup, mstp->base.alias, RLOOKUP_F_NOFLAGS);
        break;
      }
      case PLN_T_FILTER:
      case PLN_T_DISTRIBUTE:
      case PLN_T_ARRANGE:
      default:
        break;
    }
  }

  AGPLN_PopStep(&local->firstStep_s.base);
  AGPLN_Prepend(local, &dstp->base);
  AGPLN_Serialize(dstp->plan, &dstp->serialized);
}

int AREQ_BuildDistributedPipeline(AREQ *r, AREQDIST_UpstreamInfo *us, QueryError *status) {

  auto dstp = (PLN_DistributeStep *)AGPLN_FindStep(AREQ_AGGPlan(r), NULL, NULL, PLN_T_DISTRIBUTE);
  RS_ASSERT(dstp);

  RLookup_EnableOptions(&dstp->lk, RLOOKUP_OPT_ALLOWUNRESOLVED);
  int rc = AREQ_BuildPipeline(r, status);
  RLookup_DisableOptions(&dstp->lk, RLOOKUP_OPT_ALLOWUNRESOLVED);

  if (rc != REDISMODULE_OK) {
    return REDISMODULE_ERR;
  }

  std::vector<const RLookupKey *> loadFields;
  RLOOKUP_FOREACH(kk, &dstp->lk, {
    if (RLookupKey_GetFlags(kk) & RLOOKUP_F_UNRESOLVED) {
      loadFields.push_back(kk);
    }
  });

  if (!loadFields.empty()) {
    array_append(dstp->serialized, rm_strndup("LOAD", 4));
    char *ldsze;
    rm_asprintf(&ldsze, "%lu", (unsigned long)loadFields.size());
    array_append(dstp->serialized, ldsze);
    for (auto kk : loadFields) {
      array_append(dstp->serialized, rm_strndup(RLookupKey_GetName(kk), RLookupKey_GetNameLen(kk)));
    }
  }

  us->lookup = &dstp->lk;
  us->serialized = dstp->serialized;
  return REDISMODULE_OK;
}
