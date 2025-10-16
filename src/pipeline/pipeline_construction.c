#include "pipeline/pipeline_construction.h"
#include "ext/default.h"
#include "query_optimizer.h"
#include "vector_normalization.h"
#include "vector_index.h"

#ifdef __cplusplus
extern "C" {
#endif

static ResultProcessor *buildGroupRP(PLN_GroupStep *gstp, RLookup *srclookup,
                                     const RLookupKey ***loadKeys, QueryError *err) {
  arrayof(const char*) properties = PLNGroupStep_GetProperties(gstp);
  size_t nproperties = array_len(properties);
  const RLookupKey *srckeys[nproperties], *dstkeys[nproperties];
  for (size_t ii = 0; ii < nproperties; ++ii) {
    const char *fldname = properties[ii] + 1;  // account for the @-
    size_t fldname_len = strlen(fldname);
    srckeys[ii] = RLookup_GetKey_ReadEx(srclookup, fldname, fldname_len, RLOOKUP_F_NOFLAGS);
    if (!srckeys[ii]) {
      if (loadKeys) {
        // We failed to get the key for reading, so we know getting it for loading will succeed.
        srckeys[ii] = RLookup_GetKey_LoadEx(srclookup, fldname, fldname_len, fldname, RLOOKUP_F_NOFLAGS);
        *loadKeys = array_ensure_append_1(*loadKeys, srckeys[ii]);
      }
      // We currently allow implicit loading only for known fields from the schema.
      // If we can't load keys, or the key we loaded is not in the schema, we fail.
      if (!loadKeys || !(srckeys[ii]->flags & RLOOKUP_F_SCHEMASRC)) {
        QueryError_SetWithUserDataFmt(err, QUERY_ENOPROPKEY, "No such property", " `%s`", fldname);
        return NULL;
      }
    }
    dstkeys[ii] = RLookup_GetKey_WriteEx(&gstp->lookup, fldname, fldname_len, RLOOKUP_F_NOFLAGS);
    if (!dstkeys[ii]) {
      QueryError_SetWithUserDataFmt(err, QUERY_EDUPFIELD, "Property", " `%s` specified more than once", fldname);
      return NULL;
    }
  }

  Grouper *grp = Grouper_New(srckeys, dstkeys, nproperties);

  size_t nreducers = array_len(gstp->reducers);
  for (size_t ii = 0; ii < nreducers; ++ii) {
    // Build the actual reducer
    PLN_Reducer *pr = gstp->reducers + ii;
    ReducerOptions options = REDUCEROPTS_INIT(pr->name, &pr->args, srclookup, loadKeys, err);
    ReducerFactory ff = RDCR_GetFactory(pr->name);
    if (!ff) {
      // No such reducer!
      Grouper_Free(grp);
      QueryError_SetWithUserDataFmt(err, QUERY_ENOREDUCER, "No such reducer", ": %s", pr->name);
      return NULL;
    }
    Reducer *rr = ff(&options);
    if (!rr) {
      Grouper_Free(grp);
      return NULL;
    }

    // Set the destination key for the grouper!
    uint32_t flags = pr->isHidden ? RLOOKUP_F_HIDDEN : RLOOKUP_F_NOFLAGS;
    RLookupKey *dstkey = RLookup_GetKey_Write(&gstp->lookup, pr->alias, flags);
    // Adding the reducer before validating the key, so we free the reducer if the key is invalid
    Grouper_AddReducer(grp, rr, dstkey);
    if (!dstkey) {
      Grouper_Free(grp);
      QueryError_SetWithUserDataFmt(err, QUERY_EDUPFIELD, "Property", " `%s` specified more than once", pr->alias);
      return NULL;
    }
  }

  return Grouper_GetRP(grp);
}

/** Pushes a processor up the stack. Returns the newly pushed processor
 * @param req the request
 * @param rp the processor to push
 * @param rpUpstream previous processor (used as source for rp)
 * @return the processor passed in `rp`.
 */
static ResultProcessor *pushRP(QueryProcessingCtx *ctx, ResultProcessor *rp, ResultProcessor *rpUpstream) {
  rp->upstream = rpUpstream;
  rp->parent = ctx;
  ctx->endProc = rp;
  return rp;
}

static ResultProcessor *getGroupRP(Pipeline *pipeline, const AggregationPipelineParams *params, PLN_GroupStep *gstp, ResultProcessor *rpUpstream,
                                   QueryError *status, bool forceLoad, uint32_t *outStateFlags) {
  RLookup *lookup = AGPLN_GetLookup(&pipeline->ap, &gstp->base, AGPLN_GETLOOKUP_PREV);
  RLookup *firstLk = AGPLN_GetLookup(&pipeline->ap, &gstp->base, AGPLN_GETLOOKUP_FIRST); // first lookup can load fields from redis
  const RLookupKey **loadKeys = NULL;
  ResultProcessor *groupRP = buildGroupRP(gstp, lookup, (firstLk == lookup && firstLk->spcache) ? &loadKeys : NULL, status);

  if (!groupRP) {
    array_free(loadKeys);
    return NULL;
  }

  // See if we need a LOADER group here...?
  if (loadKeys) {
    ResultProcessor *rpLoader = RPLoader_New(params->common.sctx, params->common.reqflags, firstLk, loadKeys, array_len(loadKeys), forceLoad, outStateFlags);
    array_free(loadKeys);
    RS_LOG_ASSERT(rpLoader, "RPLoader_New failed");
    rpUpstream = pushRP(&pipeline->qctx, rpLoader, rpUpstream);
  }

  return pushRP(&pipeline->qctx, groupRP, rpUpstream);
}

static ResultProcessor *getAdditionalMetricsRP(RedisSearchCtx* sctx, const QueryAST* ast, RLookup *rl, QueryError *status) {
  MetricRequest *requests = ast->metricRequests;
  for (size_t i = 0; i < array_len(requests); i++) {
    const char *name = requests[i].metric_name;
    size_t name_len = strlen(name);
    if (IndexSpec_GetFieldWithLength(sctx->spec, name, name_len)) {
      QueryError_SetWithUserDataFmt(status, QUERY_EINDEXEXISTS, "Property", " `%s` already exists in schema", name);
      return NULL;
    }
    // Set HIDDEN flag for internal metrics
    uint32_t flags = requests[i].isInternal ? RLOOKUP_F_HIDDEN : RLOOKUP_F_NOFLAGS;

    RLookupKey *key = RLookup_GetKey_WriteEx(rl, name, name_len, flags);
    if (!key) {
      QueryError_SetWithUserDataFmt(status, QUERY_EDUPFIELD, "Property", " `%s` specified more than once", name);
      return NULL;
    }

    // In some cases the iterator that requested the additional field can be NULL (if some other iterator knows early
    // that it has no results), but we still want the rest of the pipeline to know about the additional field name,
    // because there is no syntax error and the sorter should be able to "sort" by this field.
    // If there is a pointer to the node's RLookupKey, write the address.
    if (requests[i].key_ptr)
      *requests[i].key_ptr = key;
  }
  return RPMetricsLoader_New();
}

static ResultProcessor *getArrangeRP(Pipeline *pipeline, const AggregationPipelineParams *params, const PLN_BaseStep *stp,
                                     QueryError *status, ResultProcessor *up, bool forceLoad, uint32_t *outStateFlags) {
  ResultProcessor *rp = NULL;
  PLN_ArrangeStep astp_s = {.base = {.type = PLN_T_ARRANGE}};
  PLN_ArrangeStep *astp = (PLN_ArrangeStep *)stp;
  IndexSpec *spec = params->common.sctx ? params->common.sctx->spec : NULL; // check for sctx?
  // Store and count keys that require loading from Redis.
  const RLookupKey **loadKeys = NULL;

  if (!astp) {
    astp = &astp_s;
  }

  size_t maxResults = astp->offset + astp->limit;
  if (!maxResults) {
    maxResults = DEFAULT_LIMIT;
  }

  // TODO: unify if when req holds only maxResults according to the query type.
  //(SEARCH / AGGREGATE)
  maxResults = MIN(maxResults, params->maxResultsLimit);

  if (IsCount(&params->common) || !maxResults) {
    rp = RPCounter_New();
    up = pushRP(&pipeline->qctx, rp, up);
    return up;
  }

  if (IsHybrid(&params->common) || (params->common.optimizer->type != Q_OPT_NO_SORTER)) { // Don't optimize hybrid queries
    if (astp->sortKeys) {
      size_t nkeys = array_len(astp->sortKeys);
      astp->sortkeysLK = rm_malloc(sizeof(*astp->sortKeys) * nkeys);

      const RLookupKey **sortkeys = astp->sortkeysLK;

      RLookup *lk = AGPLN_GetLookup(&pipeline->ap, stp, AGPLN_GETLOOKUP_PREV);

      for (size_t ii = 0; ii < nkeys; ++ii) {
        const char *keystr = astp->sortKeys[ii];
        RLookupKey *sortkey = RLookup_GetKey_Read(lk, keystr, RLOOKUP_F_NOFLAGS);
        if (!sortkey) {
          // if the key is not sortable, and also not loaded by another result processor,
          // add it to the loadkeys list.
          // We failed to get the key for reading, so we can't fail to get it for loading.
          sortkey = RLookup_GetKey_Load(lk, keystr, keystr, RLOOKUP_F_NOFLAGS);
          // We currently allow implicit loading only for known fields from the schema.
          // If the key we loaded is not in the schema, we fail.
          if (!(sortkey->flags & RLOOKUP_F_SCHEMASRC)) {
            QueryError_SetWithUserDataFmt(status, QUERY_ENOPROPKEY, "Property", " `%s` not loaded nor in schema", keystr);
            goto end;
          }
          *array_ensure_tail(&loadKeys, const RLookupKey *) = sortkey;
        }
        sortkeys[ii] = sortkey;
      }
      if (loadKeys) {
        // If we have keys to load, add a loader step.
        ResultProcessor *rpLoader = RPLoader_New(params->common.sctx, params->common.reqflags, lk, loadKeys, array_len(loadKeys), forceLoad, outStateFlags);
        up = pushRP(&pipeline->qctx, rpLoader, up);
      }
      rp = RPSorter_NewByFields(maxResults, sortkeys, nkeys, astp->sortAscMap);
      up = pushRP(&pipeline->qctx, rp, up);
    } else if (IsHybrid(&params->common) ||
               IsSearch(&params->common) && !IsOptimized(&params->common) ||
               HasScorer(&params->common)) {
      // No sort? then it must be sort by score, which is the default.
      // In optimize mode, add sorter for queries with a scorer.
      rp = RPSorter_NewByScore(maxResults);
      up = pushRP(&pipeline->qctx, rp, up);
    }
  }

  if (astp->offset || (astp->limit && !rp)) {
    rp = RPPager_New(astp->offset, astp->limit);
    up = pushRP(&pipeline->qctx, rp, up);
  } else if (IsSearch(&params->common) && IsOptimized(&params->common) && !rp) {
    rp = RPPager_New(0, maxResults);
    up = pushRP(&pipeline->qctx, rp, up);
  }

end:
  array_free(loadKeys);
  return rp;
}

// Assumes that the spec is locked
static ResultProcessor *getScorerRP(Pipeline *pipeline, RLookup *rl, const RLookupKey *scoreKey, const QueryPipelineParams *params) {
  const char *scorer = params->scorerName;
  if (!scorer) {
    scorer = DEFAULT_SCORER_NAME;
  }
  ScoringFunctionArgs scargs = {0};
  if (params->common.reqflags & QEXEC_F_SEND_SCOREEXPLAIN) {
    scargs.scrExp = rm_calloc(1, sizeof(RSScoreExplain));
  }
  if (!strcmp(scorer, BM25_STD_NORMALIZED_TANH_SCORER_NAME)) {
    // Add the tanh factor to the scoring function args
    scargs.tanhFactor = params->reqConfig->BM25STD_TanhFactor;
  }
  ExtScoringFunctionCtx *fns = Extensions_GetScoringFunction(&scargs, scorer);
  RS_LOG_ASSERT(fns, "Extensions_GetScoringFunction failed");
  IndexSpec_GetStats(params->common.sctx->spec, &scargs.indexStats);
  scargs.qdata = params->ast->udata;
  scargs.qdatalen = params->ast->udatalen;
  ResultProcessor *rp = RPScorer_New(fns, &scargs, scoreKey);
  return rp;
}

bool hasQuerySortby(const AGGPlan *pln) {
  const PLN_BaseStep *bstp = AGPLN_FindStep(pln, NULL, NULL, PLN_T_GROUP);
  const PLN_ArrangeStep *arng = (PLN_ArrangeStep *)AGPLN_FindStep(pln, NULL, bstp, PLN_T_ARRANGE);
  return arng && arng->sortKeys;
}

static int processLoadStepArgs(PLN_LoadStep *loadStep, RLookup *lookup, uint32_t loadFlags,
                               QueryError *status) {
  if (!loadStep || !lookup) {
    return REDISMODULE_ERR;
  }

  // Use the original ArgsCursor directly
  ArgsCursor *ac = &loadStep->args;

    // Process all arguments in the ArgsCursor
  while (!AC_IsAtEnd(ac)) {
    size_t name_len;
    const char *name, *path = AC_GetStringNC(ac, &name_len);

    // Handle path prefix (@)
    if (*path == '@') {
      path++;
      name_len--;
    }

    // Check for AS alias
    if (AC_AdvanceIfMatch(ac, SPEC_AS_STR)) {
      int rv = AC_GetString(ac, &name, &name_len, 0);
      if (rv != AC_OK) {
        if (status) {
          QueryError_SetError(status, QUERY_EPARSEARGS, "LOAD path AS name - must be accompanied with NAME");
        }
        return REDISMODULE_ERR;
      } else if (!strcasecmp(name, SPEC_AS_STR)) {
        if (status) {
          QueryError_SetError(status, QUERY_EPARSEARGS, "Alias for LOAD cannot be `AS`");
        }
        return REDISMODULE_ERR;
      }
    } else {
      // Set the name to the path. name_len is already the length of the path.
      name = path;
    }

    // Create the RLookupKey
    RLookupKey *kk = RLookup_GetKey_LoadEx(lookup, name, name_len, path, loadFlags);
    // We only get a NULL return if the key already exists, which means
    // that we don't need to retrieve it again.
    if (kk && loadStep->nkeys < loadStep->args.argc) {
      loadStep->keys[loadStep->nkeys++] = kk;
    }
  }

  return REDISMODULE_OK;
}

ResultProcessor *processLoadStep(PLN_LoadStep *loadStep, RLookup *lookup,
                                RedisSearchCtx *sctx, uint32_t reqflags, uint32_t loadFlags,
                                bool forceLoad, uint32_t *outStateFlags, QueryError *status) {
  if (!loadStep || !lookup || !sctx) {
    return NULL;
  }

  // Process the LOAD step arguments to populate keys array
  if (processLoadStepArgs(loadStep, lookup, loadFlags, status) != REDISMODULE_OK) {
    return NULL;
  }

  // Create RPLoader if we have keys to load or LOAD ALL flag is set
  if (loadStep->nkeys || loadStep->base.flags & PLN_F_LOAD_ALL) {
    ResultProcessor *rp = RPLoader_New(sctx, reqflags, lookup, loadStep->keys, loadStep->nkeys, forceLoad, outStateFlags);

    // Handle JSON spec case
    if (isSpecJson(sctx->spec)) {
      // On JSON, load all gets the serialized value of the doc, and doesn't make the fields available.
      lookup->options &= ~RLOOKUP_OPT_ALL_LOADED;
    }

    return rp;
  }

  return NULL;
}

#define PUSH_RP()                                      \
  rpUpstream = pushRP(&pipeline->qctx, rp, rpUpstream); \
  rp = NULL;

/**
 * Builds the document search and scoring pipeline that executes queries against the index.
 * This creates the initial pipeline components that find matching documents and calculate
 * their relevance scores, providing the foundation for subsequent aggregation and filtering stages.
 */
void Pipeline_BuildQueryPart(Pipeline *pipeline, const QueryPipelineParams *params) {
  IndexSpecCache *cache = IndexSpec_GetSpecCache(params->common.sctx->spec);
  RS_LOG_ASSERT(cache, "IndexSpec_GetSpecCache failed")
  RLookup *first = AGPLN_GetLookup(&pipeline->ap, NULL, AGPLN_GETLOOKUP_FIRST);

  RLookup_Init(first, cache);

  ResultProcessor *rp = RPQueryIterator_New(params->rootiter, params->common.sctx);
  ((QueryPipelineParams *)params)->rootiter = NULL; // Ownership of the root iterator is now with the pipeline.
  ResultProcessor *rpUpstream = NULL;
  pipeline->qctx.rootProc = pipeline->qctx.endProc = rp;
  PUSH_RP();

  // Load results metrics according to their RLookup key.
  // We need this RP only if metricRequests is not empty.
  if (params->ast->metricRequests) {
    rp = getAdditionalMetricsRP(params->common.sctx, params->ast, first, pipeline->qctx.err);
    if (!rp) {
      return;
    }
    PUSH_RP();
  }

  /** Create a scorer if:
   *  * WITHSCORES/ADDSCORES is defined
   *  * there is no subsequent sorter within this grouping */
  const int reqflags = params->common.reqflags;

  // Check if scores are explicitly requested (WITHSCORES/ADDSCORES)
  const bool scoresExplicitlyRequested = (reqflags & (QEXEC_F_SEND_SCORES | QEXEC_F_SEND_SCORES_AS_FIELD));

  // Check if this is a search or hybrid search subquery that returns rows
  const bool isSearchReturningRows = (IsSearch(&params->common) || IsHybridSearchSubquery(&params->common)) &&
                               !(reqflags & QEXEC_F_NOROWS);

  // Check if scoring is needed based on optimization settings or sorting requirements
  bool scoringNeeded = false;
  if (isSearchReturningRows) {
    if (reqflags & QEXEC_OPTIMIZE) {
      // When optimized, check if optimizer has a scorer
      scoringNeeded = (params->common.optimizer->scorerType != SCORER_TYPE_NONE);
    } else {
      // When not optimized, check if there's no explicit sorting (which would handle scoring)
      scoringNeeded = !hasQuerySortby(&pipeline->ap);
    }
  }

  if (scoresExplicitlyRequested || (isSearchReturningRows && scoringNeeded)) {
    const RLookupKey *scoreKey = NULL;
    if (HasScoreInPipeline(&params->common)) {
      if (params->common.scoreAlias) {
        scoreKey = RLookup_GetKey_Write(first, params->common.scoreAlias, RLOOKUP_F_NOFLAGS);
        if (!scoreKey) {
          QueryError_SetWithUserDataFmt(pipeline->qctx.err, QUERY_EDUPFIELD, "Could not create score alias, name already exists in query", "%s", params->common.scoreAlias);
          return;
        }
      } else {
        scoreKey = RLookup_GetKey_Write(first, UNDERSCORE_SCORE, RLOOKUP_F_OVERRIDE);
      }
    }

    rp = getScorerRP(pipeline, first, scoreKey, params);
    PUSH_RP();
    const char *scorerName = params->scorerName;
    if (scorerName && !strcmp(scorerName, BM25_STD_NORMALIZED_MAX_SCORER_NAME)) {
      rp = RPMaxScoreNormalizer_New(scoreKey);
      PUSH_RP();
    }
  }
}

/**
 * This handles the RETURN and SUMMARIZE keywords, which operate on the result
 * which is about to be returned. It is only used in FT.SEARCH mode
 */
int buildOutputPipeline(Pipeline *pipeline, const AggregationPipelineParams* params, uint32_t loadFlags, QueryError *status, bool forceLoad, uint32_t *outStateFlags) {
  AGGPlan *pln = &pipeline->ap;
  ResultProcessor *rp = NULL, *rpUpstream = pipeline->qctx.endProc;

  RLookup *lookup = AGPLN_GetLookup(pln, NULL, AGPLN_GETLOOKUP_LAST);
  // Add a LOAD step...
  const RLookupKey **loadkeys = NULL;
  if (params->outFields->explicitReturn) {
    // Go through all the fields and ensure that each one exists in the lookup stage
    loadFlags |= RLOOKUP_F_EXPLICITRETURN;
    for (size_t ii = 0; ii < params->outFields->numFields; ++ii) {
      const ReturnedField *rf = params->outFields->fields + ii;
      RLookupKey *lk = RLookup_GetKey_Load(lookup, rf->name, rf->path, loadFlags);
      if (lk) {
        *array_ensure_tail(&loadkeys, const RLookupKey *) = lk;
      }
    }
  }

  // If we have explicit return and some of the keys' values are missing,
  // or if we don't have explicit return, meaning we use LOAD ALL
  if (loadkeys || !params->outFields->explicitReturn) {
    rp = RPLoader_New(params->common.sctx, params->common.reqflags, lookup, loadkeys, array_len(loadkeys), forceLoad, outStateFlags);
    if (isSpecJson(params->common.sctx->spec)) {
      // On JSON, load all gets the serialized value of the doc, and doesn't make the fields available.
      lookup->options &= ~RLOOKUP_OPT_ALL_LOADED;
    }
    array_free(loadkeys);
    PUSH_RP();
  }

  if (params->common.reqflags & QEXEC_F_SEND_HIGHLIGHT) {
    RLookup *lookup = AGPLN_GetLookup(pln, NULL, AGPLN_GETLOOKUP_LAST);
    for (size_t ii = 0; ii < params->outFields->numFields; ++ii) {
      ReturnedField *ff = params->outFields->fields + ii;
      if (params->outFields->defaultField.mode == SummarizeMode_None && ff->mode == SummarizeMode_None) {
        // Ignore - this is a field for `RETURN`, not `SUMMARIZE`
        // (Default mode is not any of the summarize modes, and also there is no mode explicitly specified for this field)
        continue;
      }
      RLookupKey *kk = RLookup_GetKey_Read(lookup, ff->name, RLOOKUP_F_NOFLAGS);
      if (!kk) {
        QueryError_SetWithUserDataFmt(status, QUERY_ENOPROPKEY, "No such property", " `%s`", ff->name);
        goto error;
      } else if (!(kk->flags & RLOOKUP_F_SCHEMASRC)) {
        QueryError_SetWithUserDataFmt(status, QUERY_EINVAL, "Property", " `%s` is not in schema", ff->name);
        goto error;
      }
      ff->lookupKey = kk;
    }
    rp = RPHighlighter_New(params->language, params->outFields, lookup);
    PUSH_RP();
  }

  return REDISMODULE_OK;
error:
  return REDISMODULE_ERR;
}

int Pipeline_BuildAggregationPart(Pipeline *pipeline, const AggregationPipelineParams *params, uint32_t *outStateFlags) {
  AGGPlan *pln = &pipeline->ap;
  ResultProcessor *rp = NULL, *rpUpstream = pipeline->qctx.endProc;
  RedisSearchCtx *sctx = params->common.sctx;
  int requestFlags = params->common.reqflags;
  QueryError *status = pipeline->qctx.err;

  // If we have a JSON spec, and an "old" API version (DIALECT < 3), we don't store all the data of a multi-value field
  // in the SV as we want to return it, so we need to load and override all requested return fields that are SV source.
  bool forceLoad = sctx && isSpecJson(sctx->spec) && (sctx->apiVersion < APIVERSION_RETURN_MULTI_CMP_FIRST);
  uint32_t loadFlags = forceLoad ? RLOOKUP_F_FORCE_LOAD : RLOOKUP_F_NOFLAGS;

  // Whether we've applied a SORTBY yet..
  int hasArrange = 0;

  for (const DLLIST_node *nn = pln->steps.next; nn != &pln->steps; nn = nn->next) {
    const PLN_BaseStep *stp = DLLIST_ITEM(nn, PLN_BaseStep, llnodePln);

    switch (stp->type) {
      case PLN_T_GROUP: {
        // Adds group result processor and loader if needed.
        rpUpstream = getGroupRP(pipeline, params, (PLN_GroupStep *)stp, rpUpstream, status, forceLoad, outStateFlags);
        if (!rpUpstream) {
          goto error;
        }
        break;
      }

      case PLN_T_ARRANGE: {
        rp = getArrangeRP(pipeline, params, stp, status, rpUpstream, forceLoad, outStateFlags);
        if (!rp) {
          goto error;
        }
        hasArrange = 1;
        rpUpstream = rp;

        break;
      }

      case PLN_T_APPLY:
      case PLN_T_FILTER: {
        PLN_MapFilterStep *mstp = (PLN_MapFilterStep *)stp;
        mstp->parsedExpr = ExprAST_Parse(mstp->expr, status);
        if (!mstp->parsedExpr) {
          goto error;
        }

        // Ensure the lookups can actually find what they need
        RLookup *curLookup = AGPLN_GetLookup(pln, stp, AGPLN_GETLOOKUP_PREV);
        if (!ExprAST_GetLookupKeys(mstp->parsedExpr, curLookup, status)) {
          goto error;
        }

        if (stp->type == PLN_T_APPLY) {
          uint32_t flags = mstp->noOverride ? RLOOKUP_F_NOFLAGS : RLOOKUP_F_OVERRIDE;
          RLookupKey *dstkey = RLookup_GetKey_Write(curLookup, stp->alias, flags);
          if (!dstkey) {
            // Can only happen if we're in noOverride mode
            QueryError_SetWithUserDataFmt(status, QUERY_EDUPFIELD, "Property", " `%s` specified more than once", stp->alias);
            goto error;
          }
          rp = RPEvaluator_NewProjector(mstp->parsedExpr, curLookup, dstkey);
        } else {
          rp = RPEvaluator_NewFilter(mstp->parsedExpr, curLookup);
        }
        PUSH_RP();
        break;
      }

      case PLN_T_LOAD: {
        PLN_LoadStep *lstp = (PLN_LoadStep *)stp;
        RLookup *curLookup = AGPLN_GetLookup(pln, stp, AGPLN_GETLOOKUP_PREV);
        RLookup *rootLookup = AGPLN_GetLookup(pln, NULL, AGPLN_GETLOOKUP_FIRST);
        if (curLookup != rootLookup) {
          QueryError_SetError(status, QUERY_EINVAL,
                              "LOAD cannot be applied after projectors or reducers");
          goto error;
        }

        // Process the complete LOAD step
        rp = processLoadStep(lstp, curLookup, params->common.sctx, params->common.reqflags,
                            loadFlags, forceLoad, outStateFlags, status);
        if (QueryError_HasError(status)) {
          return REDISMODULE_ERR;
        }
        if (rp) {
          PUSH_RP();
        }
        break;
      }

      case PLN_T_VECTOR_NORMALIZER: {
        PLN_VectorNormalizerStep *vnStep = (PLN_VectorNormalizerStep *)stp;

        // Resolve vector field to get distance metric
        const FieldSpec *vectorField = IndexSpec_GetFieldWithLength(params->common.sctx->spec,
                                                                     vnStep->vectorFieldName,
                                                                     strlen(vnStep->vectorFieldName));
        if (!vectorField || !FIELD_IS(vectorField, INDEXFLD_T_VECTOR)) {
          QueryError_SetError(status, QUERY_ESYNTAX, "Invalid vector field for normalization");
          goto error;
        }

        // Extract distance metric from vector field
        VecSimMetric metric = getVecSimMetricFromVectorField(vectorField);

        // Get appropriate normalization function
        VectorNormFunction normFunc = getVectorNormalizationFunction(metric);

        // Get score key for writing normalized scores
        RLookup *curLookup = AGPLN_GetLookup(pln, stp, AGPLN_GETLOOKUP_PREV);
        RS_ASSERT(curLookup);
        const RLookupKey *scoreKey = RLookup_GetKey_Read(curLookup, vnStep->distanceFieldAlias, RLOOKUP_F_NOFLAGS);
        // Create vector normalizer result processor
        rp = RPVectorNormalizer_New(normFunc, scoreKey);
        PUSH_RP();
        break;
      }

      case PLN_T_ROOT:
        // Placeholder step for initial lookup
        break;
      case PLN_T_DISTRIBUTE:
        // This is the root already
        break;
      case PLN_T_INVALID:
      case PLN_T__MAX:
        // not handled yet
        RS_ABORT("Oops");
        break;
    }
  }

  // If no LIMIT or SORT has been applied, do it somewhere here so we don't
  // return the entire matching result set!
  if (!hasArrange && (IsSearch(&params->common) || IsHybridSearchSubquery(&params->common))) {
    rp = getArrangeRP(pipeline, params, NULL, status, rpUpstream, forceLoad, outStateFlags);
    if (!rp) {
      goto error;
    }
    rpUpstream = rp;
  }

  // If this is an FT.SEARCH command which requires returning of some of the
  // document fields, handle those options in this function
  if ((requestFlags & QEXEC_F_IS_SEARCH) && !(requestFlags & QEXEC_F_SEND_NOFIELDS)) {
    if (buildOutputPipeline(pipeline, params, loadFlags, status, forceLoad, outStateFlags) != REDISMODULE_OK) {
      goto error;
    }
  }

  // In profile mode, we need to add RP_Profile before each RP
  if ((requestFlags & QEXEC_F_PROFILE) && pipeline->qctx.endProc) {
    Profile_AddRPs(&pipeline->qctx);
  }

  //pipeline->stateflags |= outStateflags;
  return REDISMODULE_OK;
error:
  return REDISMODULE_ERR;
}

#ifdef __cplusplus
}
#endif
