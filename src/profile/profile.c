/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#include "profile.h"
#include "iterators/iterator_api.h"
#include "iterators/profile_iterator.h"
#include "iterators/inverted_index_iterator.h"
#include "iterators/not_iterator.h"
#include "iterators/optional_iterator.h"
#include "iterators/union_iterator.h"
#include "iterators/intersection_iterator.h"
#include "iterators/hybrid_reader.h"
#include "iterators/optimizer_reader.h"
#include "iterators_rs.h"
#include "reply_macros.h"
#include "util/units.h"
#include "coord/rmr/rmr.h"
#include "hybrid/hybrid_request.h"

typedef struct {
    IteratorsConfig *iteratorsConfig;
    int printProfileClock;
} PrintProfileConfig;


void printIteratorProfile(RedisModule_Reply *reply, QueryIterator *root, ProfileCounters *counters,
                          double cpuTime, int depth, int limited, PrintProfileConfig *config);

void printInvIdxIt(RedisModule_Reply *reply, QueryIterator *root, ProfileCounters *counters, double cpuTime, PrintProfileConfig *config) {
  const InvIndIterator *it = (const InvIndIterator *)root;
  IndexFlags readerFlags = InvIndIterator_GetReaderFlags(it);

  RedisModule_Reply_Map(reply);
  if (readerFlags == Index_DocIdsOnly) {
    RSQueryTerm *term = IndexResult_QueryTermRef(root->current);
    if (term != NULL) {
      printProfileType("TAG");
      REPLY_KVSTR_SAFE("Term", QueryTerm_GetStr(term));
    }
  } else if (readerFlags & Index_StoreNumeric) {
    NumericInvIndIterator *numIt = (NumericInvIndIterator *)it;
    const NumericFilter *flt = NumericInvIndIterator_GetNumericFilter(numIt);
    if (!flt || flt->geoFilter == NULL) {
      printProfileType("NUMERIC");
      RedisModule_Reply_SimpleString(reply, "Term");
      RedisModule_Reply_SimpleStringf(reply, "%g - %g", NumericInvIndIterator_GetProfileRangeMin(numIt), NumericInvIndIterator_GetProfileRangeMax(numIt));
    } else {
      printProfileType("GEO");
      RedisModule_Reply_SimpleString(reply, "Term");
      double se[2];
      double nw[2];
      decodeGeo(NumericInvIndIterator_GetProfileRangeMin(numIt), se);
      decodeGeo(NumericInvIndIterator_GetProfileRangeMax(numIt), nw);
      RedisModule_Reply_SimpleStringf(reply, "%g,%g - %g,%g", se[0], se[1], nw[0], nw[1]);
    }
  } else {
    printProfileType("TEXT");
    RSQueryTerm *term = IndexResult_QueryTermRef(root->current);
    REPLY_KVSTR_SAFE("Term", QueryTerm_GetStr(term));
  }

  // print counter and clock
  if (config->printProfileClock) {
    printProfileTime(cpuTime);
  }

  printProfileCounters(counters);
  RedisModule_ReplyKV_LongLong(reply, "Estimated number of matches", root->NumEstimated(root));

  RedisModule_Reply_MapEnd(reply);
}

static double _recursiveProfilePrint(RedisModule_Reply *reply, ResultProcessor *rp, int printProfileClock) {
  if (rp == NULL) {
    return 0;
  }
  double upstreamTime = _recursiveProfilePrint(reply, rp->upstream, printProfileClock);

  if (rp->type > RP_MAX) {
    RS_LOG_ASSERT_FMT(rp->type < RP_MAX_DEBUG, "RPType error, type: %d", rp->type);
    return upstreamTime;
  }

  // Array is filled backward in pair of [common, profile] result processors
  if (rp->type != RP_PROFILE) {
    RedisModule_Reply_Map(reply); // start of recursive map

    switch (rp->type) {
      case RP_INDEX:
      case RP_METRICS:
      case RP_LOADER:
      case RP_KEY_NAME_LOADER:
      case RP_SCORER:
      case RP_SORTER:
      case RP_COUNTER:
      case RP_PAGER_LIMITER:
      case RP_HIGHLIGHTER:
      case RP_GROUP:
      case RP_MAX_SCORE_NORMALIZER:
      case RP_NETWORK:
      case RP_SAFE_DEPLETER:
      case RP_VECTOR_NORMALIZER:
      case RP_HYBRID_MERGER:
      case RP_DEPLETER:
        printProfileType(RPTypeToString(rp->type));
        break;

      case RP_PROJECTOR:
      case RP_FILTER:
        RPEvaluator_Reply(reply, "Type", rp);
        break;

      case RP_SAFE_LOADER:
        printProfileType(RPTypeToString(rp->type));
        printProfileGILTime(rs_wall_clock_convert_ns_to_ms_d(rp->rpGILTime));
        break;

      default:
        RS_ABORT("RPType error");
        break;
    }

    return upstreamTime;
  }
  double totalRPTime = rs_wall_clock_convert_ns_to_ms_d(RPProfile_GetClock(rp));

  // For RP_SAFE_DEPLETER, use depletion time as the total time instead of
  // RPProfile time because the actual work happens in the background thread
  if (rp->upstream && rp->upstream->type == RP_SAFE_DEPLETER) {
    totalRPTime = rs_wall_clock_convert_ns_to_ms_d(RPSafeDepleter_GetDepletionTime(rp->upstream));
  }

  if (printProfileClock) {
    double deltaTime = totalRPTime - upstreamTime;
    printProfileTime(deltaTime);
  }
  printProfileRPCounter(RPProfile_GetCount(rp) - 1);
  RedisModule_Reply_MapEnd(reply); // end of recursive map
  return totalRPTime;
}

static double printProfileRP(RedisModule_Reply *reply, ResultProcessor *rp, int printProfileClock) {
  return _recursiveProfilePrint(reply, rp, printProfileClock);
}

void Profile_PrintResultProcessors(RedisModule_Reply *reply, ResultProcessor *rp, bool verbose) {
  printProfileRP(reply, rp, verbose);
}

// Internal implementation that supports an optional callback to print extra
// content before the result processors section.
// Used in hybrid search profile to print the hybrid search subqueries profile.
static void Profile_PrintCommon(RedisModule_Reply *reply,
                                ProfileRequest *request,
                                ProfilePrinterCB printbeforeRPSectionCB,
                                void *beforeRPSectionCtx) {
  ProfilePrinterCtx *profileCtx = NULL;
  ProfileClocks *clocks = NULL;
  QueryProcessingCtx *qctx = NULL;
  QEFlags reqFlags = 0;
  bool profile_verbose = false;
  AREQ *req = NULL;  // Keep for iterator access

  switch (request->type) {
    case PROFILE_REQUEST_TYPE_AREQ:
      req = request->req;
      profileCtx = AREQ_ProfilePrinterCtx(req);
      profile_verbose = req->reqConfig.printProfileClock;
      clocks = &(req->profileClocks);
      qctx = AREQ_QueryProcessingCtx(req);
      reqFlags = AREQ_RequestFlags(req);
      break;

    case PROFILE_REQUEST_TYPE_HYBRID: {
      HybridRequest *hreq = request->hreq;
      profileCtx = &(hreq->profileCtx);
      profile_verbose = hreq->reqConfig.printProfileClock;
      clocks = &(hreq->profileClocks);
      qctx = &hreq->tailPipeline->qctx;
      reqFlags = (QEFlags)hreq->reqflags;
      break;
    }
  }

  bool timedout = ProfileWarnings_Has(&profileCtx->warnings, PROFILE_WARNING_TYPE_TIMEOUT);
  bool reachedMaxPrefixExpansions = ProfileWarnings_Has(&profileCtx->warnings, PROFILE_WARNING_TYPE_MAX_PREFIX_EXPANSIONS);
  bool bgScanOOM = ProfileWarnings_Has(&profileCtx->warnings, PROFILE_WARNING_TYPE_BG_SCAN_OOM);
  bool queryOOM = ProfileWarnings_Has(&profileCtx->warnings, PROFILE_WARNING_TYPE_QUERY_OOM);
  bool asmTrimmingDelayTimeout = ProfileWarnings_Has(&profileCtx->warnings, PROFILE_WARNING_TYPE_ASM_INACCURATE_RESULTS);

  clocks->profileTotalTime += rs_wall_clock_elapsed_ns(&clocks->initClock);

  RedisModule_Reply_Map(reply);

  // Get and add the Shard ID string to the profile reply (guarded by a ref count).
  const char *node_id = MR_GetLocalNodeId();
  if (node_id) {
    RedisModule_ReplyKV_SimpleString(reply, "Shard ID", node_id);
  }
  MR_ReleaseLocalNodeIdReadLock();

  // Print total time
  if (profile_verbose) {
    RedisModule_ReplyKV_Double(reply, "Total profile time",
                               rs_wall_clock_convert_ns_to_ms_d(clocks->profileTotalTime));
  }

  // Print query parsing time
  if (profile_verbose) {
    RedisModule_ReplyKV_Double(reply, "Parsing time",
                               rs_wall_clock_convert_ns_to_ms_d(clocks->profileParseTime));
  }

  if (profile_verbose) {
    RedisModule_ReplyKV_Double(reply, "Workers queue time",
                               rs_wall_clock_convert_ns_to_ms_d(clocks->profileQueueTime));
  }

  // Print iterators creation time
  if (profile_verbose) {
    RedisModule_ReplyKV_Double(reply, "Pipeline creation time",
                               rs_wall_clock_convert_ns_to_ms_d(clocks->profilePipelineBuildTime));
  }

  // Print total GIL time
  if (profile_verbose) {
    if (reqFlags & QEXEC_F_RUN_IN_BACKGROUND) {
      RedisModule_ReplyKV_Double(reply, "Total GIL time",
                                 rs_wall_clock_convert_ns_to_ms_d(qctx->queryGILTime));
    }
  }

  bool isInternal = reqFlags & QEXEC_F_INTERNAL;
  // Print coord dispatch time if this is a shard handling a coordinator request.
  if (profile_verbose && isInternal && req) {
    RedisModule_ReplyKV_Double(reply, "Coordinator dispatch time [ms]",
                               rs_wall_clock_convert_ns_to_ms_d(req->profileClocks.coordDispatchTime));
  }

  // Print whether a warning was raised throughout command execution
  bool warningRaised = bgScanOOM || queryOOM || timedout || reachedMaxPrefixExpansions || asmTrimmingDelayTimeout;
  RedisModule_ReplyKV_Array(reply, "Warning");
  if (!warningRaised) {
    RedisModule_Reply_SimpleString(reply, "None");
  } else {
    if (bgScanOOM) {
      RedisModule_Reply_SimpleString(reply, QUERY_WINDEXING_FAILURE);
    }
    if (queryOOM) {
      // This function is called by Shard or SA, so always return SHARD warning.
      RedisModule_Reply_SimpleString(reply, QUERY_WOOM_SHARD);
    }
    if (timedout) {
      RedisModule_Reply_SimpleString(reply, QueryWarning_Strwarning(QUERY_WARNING_CODE_TIMED_OUT));
    }
    if (reachedMaxPrefixExpansions) {
      RedisModule_Reply_SimpleString(reply, QUERY_WMAXPREFIXEXPANSIONS);
    }
    if (asmTrimmingDelayTimeout) {
      RedisModule_Reply_SimpleString(reply, QUERY_ASM_INACCURATE_RESULTS);
    }
  }
  RedisModule_Reply_ArrayEnd(reply); // >warnings

  // Print cursor reads count if this is a cursor request.
  if (req && IsCursor(req)) {
    // Only internal requests can use profile with cursor.
    RS_ASSERT(IsInternal(req));
    RedisModule_ReplyKV_LongLong(reply, "Internal cursor reads", profileCtx->cursor_reads);
  }

  // Print profile of iterators
  QueryIterator *root = QITR_GetRootFilter(qctx);
  // Coordinator does not have iterators
  if (req && root) {
    RedisModule_Reply_SimpleString(reply, "Iterators profile");
    PrintProfileConfig config = {.iteratorsConfig = &req->ast.config,
                                 .printProfileClock = profile_verbose};
    printIteratorProfile(reply, root, 0, 0, 2,
                         AREQ_RequestFlags(req) & QEXEC_F_PROFILE_LIMITED, &config);
  }

  // Call printbeforeRPSectionCB if provided (before printing main result processors)
  if (printbeforeRPSectionCB) {
    printbeforeRPSectionCB(reply, beforeRPSectionCtx);
  }

  // Print profile of result processors
  ResultProcessor *rp = qctx->endProc;
  RedisModule_ReplyKV_Array(reply, "Result processors profile");
  printProfileRP(reply, rp, profile_verbose);
  RedisModule_Reply_ArrayEnd(reply);
  RedisModule_Reply_MapEnd(reply);
}

void Profile_PrintHybrid(RedisModule_Reply *reply, void *ctx) {
  HybridRequest *hreq = ctx;
  ProfileRequest request = {
    .type = PROFILE_REQUEST_TYPE_HYBRID,
    .hreq = hreq
  };
  Profile_PrintCommon(reply, &request, NULL, NULL);
}

void Profile_PrintHybridExtra(RedisModule_Reply *reply, void *ctx,
                              ProfilePrinterCB printbeforeRPSectionCB,
                              void *beforeRPSectionCtx) {
  HybridRequest *hreq = ctx;
  ProfileRequest request = {
    .type = PROFILE_REQUEST_TYPE_HYBRID,
    .hreq = hreq
  };
  Profile_PrintCommon(reply, &request, printbeforeRPSectionCB, beforeRPSectionCtx);
}

void Profile_Print(RedisModule_Reply *reply, void *ctx) {
  AREQ *req = ctx;
  ProfileRequest request = {
    .type = PROFILE_REQUEST_TYPE_AREQ,
    .req = req
  };
  Profile_PrintCommon(reply, &request, NULL, NULL);
}

void Profile_PrepareMapForReply(RedisModule_Reply *reply) {
  if (reply->resp3) {
    RedisModule_ReplyKV_Map(reply, "Results");
  } else {
    RedisModule_Reply_Map(reply);
  }
}

void Profile_PrintInFormat(RedisModule_Reply *reply,
                           ProfilePrinterCB shards_cb, void *shards_ctx,
                           ProfilePrinterCB coordinator_cb, void *coordinator_ctx) {
  if (reply->resp3) {
    RedisModule_ReplyKV_Map(reply, PROFILE_STR); /* >profile */
  } else {
    RedisModule_Reply_Map(reply); /* >profile */
  }
  /* Print shards profile */
  RedisModule_ReplyKV_Array(reply, PROFILE_SHARDS_STR); /* >Shards */
  if (shards_cb) shards_cb(reply, shards_ctx);
  RedisModule_Reply_ArrayEnd(reply); /* Shards */
  /* Print coordinator profile */
  RedisModule_Reply_SimpleString(reply, PROFILE_COORDINATOR_STR); /* >coordinator */
  if (coordinator_cb) {
    coordinator_cb(reply, coordinator_ctx); /* reply is already a map */
  } else {
    RedisModule_Reply_EmptyMap(reply);
  }
  RedisModule_Reply_MapEnd(reply); /* >profile */
}

// Receives context as void*
// Will be used as ProfilePrinterCtx*
void Profile_PrintDefault(RedisModule_Reply *reply, void *ctx) {
  Profile_PrintInFormat(reply, Profile_Print, ctx, NULL, NULL);
}

/** Add Profile iterator before any iterator in the tree */
void Profile_AddIters(QueryIterator **root) {
  if (*root == NULL) return;

  // Add profile iterator before child iterators
  switch((*root)->type) {
    case NOT_ITERATOR: {
      QueryIterator *child = ((NotIterator *)(*root))->child;
      Profile_AddIters(&child);
      ((NotIterator *)(*root))->child = child;
      break;
    }
    case OPTIONAL_ITERATOR:
    case OPTIONAL_OPTIMIZED_ITERATOR: {
      QueryIterator *child = TakeOptionalIteratorChild(*root);
      Profile_AddIters(&child);
      SetOptionalIteratorChild(*root, child);
      break;
    }
    case HYBRID_ITERATOR: {
      QueryIterator *child = ((HybridIterator *)(*root))->child;
      Profile_AddIters(&child);
      ((HybridIterator *)(*root))->child = child;
      break;
    }
    case OPTIMUS_ITERATOR: {
      QueryIterator *child = ((OptimizerIterator *)(*root))->child;
      Profile_AddIters(&child);
      ((OptimizerIterator *)(*root))->child = child;
      break;
    }
    case UNION_ITERATOR: {
      UnionIterator *ui = (UnionIterator *)(*root);
      for (int i = 0; i < ui->num_orig; i++) {
        Profile_AddIters(&(ui->its_orig[i]));
      }
      UI_SyncIterList(ui);
      break;
    }
    case INTERSECT_ITERATOR: {
      IntersectionIterator *ii = (IntersectionIterator *)(*root);
      for (int i = 0; i < ii->num_its; i++) {
        Profile_AddIters(&(ii->its[i]));
      }
      break;
    }
    case WILDCARD_ITERATOR:
    case INV_IDX_NUMERIC_ITERATOR:
    case INV_IDX_TERM_ITERATOR:
    case INV_IDX_WILDCARD_ITERATOR:
    case INV_IDX_MISSING_ITERATOR:
    case INV_IDX_TAG_ITERATOR:
    case EMPTY_ITERATOR:
    case ID_LIST_SORTED_ITERATOR:
    case ID_LIST_UNSORTED_ITERATOR:
    case METRIC_SORTED_BY_ID_ITERATOR:
    case METRIC_SORTED_BY_SCORE_ITERATOR:
      break;
    // LCOV_EXCL_START
    case PROFILE_ITERATOR:
    case MAX_ITERATOR:
      RS_ABORT("Error");
      break;
    // LCOV_EXCL_STOP
  }

  // Create a profile iterator and update outparam pointer
  *root = NewProfileIterator(*root);
}

#define PRINT_PROFILE_FUNC(name) static void name(RedisModule_Reply *reply,   \
                                                  QueryIterator *root,        \
                                                  ProfileCounters *counters,  \
                                                  double cpuTime,             \
                                                  int depth,                  \
                                                  int limited,                \
                                                  PrintProfileConfig *config)

PRINT_PROFILE_FUNC(printUnionIt) {
  UnionIterator *ui = (UnionIterator *)root;
  int printFull = !limited  || (ui->type & QN_UNION);

  RedisModule_Reply_Map(reply);

  printProfileType("UNION");

  RedisModule_Reply_SimpleString(reply, "Query type");
  char *unionTypeStr;
  switch (ui->type) {
  case QN_GEO : unionTypeStr = "GEO"; break;
  case QN_TAG : unionTypeStr = "TAG"; break;
  case QN_UNION : unionTypeStr = "UNION"; break;
  case QN_FUZZY : unionTypeStr = "FUZZY"; break;
  case QN_PREFIX : unionTypeStr = "PREFIX"; break;
  case QN_NUMERIC : unionTypeStr = "NUMERIC"; break;
  case QN_LEXRANGE : unionTypeStr = "LEXRANGE"; break;
  case QN_WILDCARD_QUERY : unionTypeStr = "WILDCARD"; break;
  // LCOV_EXCL_START
  default:
    RS_ABORT_ALWAYS("Invalid type for union");
  // LCOV_EXCL_STOP
  }
  if (!ui->q_str) {
    RedisModule_Reply_SimpleString(reply, unionTypeStr);
  } else {
    const char *qstr = ui->q_str;
    if (isUnsafeForSimpleString(qstr)) qstr = escapeSimpleString(qstr);
    RedisModule_Reply_SimpleStringf(reply, "%s - %s", unionTypeStr, qstr);
    if (qstr != ui->q_str) rm_free((char*)qstr);
  }

  if (config->printProfileClock) {
    printProfileTime(cpuTime);
  }

  printProfileCounters(counters);

  RedisModule_Reply_SimpleString(reply, "Child iterators");
  if (printFull) {
    RedisModule_Reply_Array(reply);
      for (int i = 0; i < ui->num_orig; i++) {
        printIteratorProfile(reply, ui->its_orig[i], 0, 0, depth + 1, limited, config);
      }
    RedisModule_Reply_ArrayEnd(reply);
  } else {
    RedisModule_Reply_SimpleStringf(reply, "The number of iterators in the union is %d", ui->num_orig);
  }

  RedisModule_Reply_MapEnd(reply);
}

PRINT_PROFILE_FUNC(printIntersectIt) {
  IntersectionIterator *ii = (IntersectionIterator *)root;

  RedisModule_Reply_Map(reply);

  printProfileType("INTERSECT");

  if (config->printProfileClock) {
    printProfileTime(cpuTime);
  }

  printProfileCounters(counters);

  RedisModule_ReplyKV_Array(reply, "Child iterators");
    for (int i = 0; i < ii->num_its; i++) {
      printIteratorProfile(reply, ii->its[i], 0, 0, depth + 1, limited, config);
    }
  RedisModule_Reply_ArrayEnd(reply);

  RedisModule_Reply_MapEnd(reply);
}

#define PRINT_PROFILE_METRIC(name, text)                           \
  PRINT_PROFILE_FUNC(name) {                                       \
      RedisModule_Reply_Map(reply);                                \
      MetricType type = GetMetricType(root);                       \
                                                                   \
      switch (type) {                                              \
        case VECTOR_DISTANCE: {                                    \
          printProfileType(text " - VECTOR DISTANCE");             \
          break;                                                   \
        }                                                          \
        /* LCOV_EXCL_START */                                      \
        default: {                                                 \
          RS_ABORT("Invalid type for metric");                     \
          break;                                                   \
        }                                                          \
        /* LCOV_EXCL_STOP  */                                      \
      }                                                            \
                                                                   \
      if (config->printProfileClock) {                             \
        printProfileTime(cpuTime);                                 \
      }                                                            \
                                                                   \
      printProfileCounters(counters);                              \
                                                                   \
      if (type == VECTOR_DISTANCE) {                               \
        printProfileVectorSearchMode(VECSIM_RANGE_QUERY);          \
      }                                                            \
                                                                   \
      RedisModule_Reply_MapEnd(reply);                             \
  }

PRINT_PROFILE_METRIC(printMetricSortedByIdIt, "METRIC SORTED BY ID");
PRINT_PROFILE_METRIC(printMetricSortedByScoreIt, "METRIC SORTED BY SCORE");

void PrintIteratorChildProfile(RedisModule_Reply *reply, QueryIterator *root, ProfileCounters *counters, double cpuTime,
                  int depth, int limited, PrintProfileConfig *config, QueryIterator *child, const char *text) {
  size_t nlen = 0;
  RedisModule_Reply_Map(reply);
    printProfileType(text);
    if (config->printProfileClock) {
      printProfileTime(cpuTime);
    }
    printProfileCounters(counters);

    if (root->type == HYBRID_ITERATOR) {
      HybridIterator *hi = (HybridIterator *)root;
      printProfileVectorSearchMode(hi->searchMode);
      if (hi->searchMode == VECSIM_HYBRID_BATCHES ||
          hi->searchMode == VECSIM_HYBRID_BATCHES_TO_ADHOC_BF) {
        printProfileNumBatches(hi);
        printProfileMaxBatchSize(hi);
        printProfileMaxBatchIteration(hi);
      }
    }

    if (root->type == OPTIMUS_ITERATOR) {
      OptimizerIterator *oi = (OptimizerIterator *)root;
      printProfileOptimizationType(oi);
    }

    if (child) {
      RedisModule_Reply_SimpleString(reply, "Child iterator");
      printIteratorProfile(reply, child, 0, 0, depth + 1, limited, config);
    }
  RedisModule_Reply_MapEnd(reply);
}

#define PRINT_PROFILE_SINGLE_NO_CHILD(name, text)                                      \
  PRINT_PROFILE_FUNC(name) {                                                           \
    PrintIteratorChildProfile(reply, (root), counters, cpuTime, depth, limited, config, \
      NULL, (text));                                                                   \
  }

#define PRINT_PROFILE_SINGLE(name, IterType, text)                                     \
  PRINT_PROFILE_FUNC(name) {                                                           \
    PrintIteratorChildProfile(reply, (root), counters, cpuTime, depth, limited, config, \
      ((IterType *)(root))->child, (text));                                            \
  }

PRINT_PROFILE_SINGLE_NO_CHILD(printWildcardIt,                  "WILDCARD");
PRINT_PROFILE_SINGLE_NO_CHILD(printIdListSortedIt,              "ID-LIST-SORTED");
PRINT_PROFILE_SINGLE_NO_CHILD(printIdListUnsortedIt,            "ID-LIST-UNSORTED");
PRINT_PROFILE_SINGLE_NO_CHILD(printEmptyIt,                     "EMPTY");
PRINT_PROFILE_SINGLE(printNotIt, NotIterator,                   "NOT");
PRINT_PROFILE_SINGLE(printHybridIt, HybridIterator,             "VECTOR");
PRINT_PROFILE_SINGLE(printOptimusIt, OptimizerIterator,         "OPTIMIZER");

PRINT_PROFILE_FUNC(printOptionalIt) {
  // Cast is safe: PrintIteratorChildProfile only reads from the child iterator.
  PrintIteratorChildProfile(reply, root, counters, cpuTime, depth, limited, config,
    (QueryIterator *)GetOptionalIteratorChild(root), "OPTIONAL");
}

PRINT_PROFILE_FUNC(printProfileIt) {
  ProfileIterator *pi = (ProfileIterator *)root;
  printIteratorProfile(reply, pi->child, &pi->counters,
    rs_wall_clock_convert_ns_to_ms_d(pi->wallTime), depth, limited, config);
}

void printIteratorProfile(RedisModule_Reply *reply, QueryIterator *root, ProfileCounters *counters,
                          double cpuTime, int depth, int limited, PrintProfileConfig *config) {
  if (root == NULL) return;

  switch (root->type) {
    // Reader
    case INV_IDX_NUMERIC_ITERATOR:
    case INV_IDX_TERM_ITERATOR:
    case INV_IDX_WILDCARD_ITERATOR:
    case INV_IDX_MISSING_ITERATOR:
    case INV_IDX_TAG_ITERATOR:
                                            { printInvIdxIt(reply, root, counters, cpuTime, config);                                break; }
    // Multi values
    case UNION_ITERATOR:                    { printUnionIt(reply, root, counters, cpuTime, depth, limited, config);                 break; }
    case INTERSECT_ITERATOR:                { printIntersectIt(reply, root, counters, cpuTime, depth, limited, config);             break; }
    // Single value
    case NOT_ITERATOR:                      { printNotIt(reply, root, counters, cpuTime, depth, limited, config);                   break; }
    case OPTIONAL_ITERATOR: // fallthrough
    case OPTIONAL_OPTIMIZED_ITERATOR:       { printOptionalIt(reply, root, counters, cpuTime, depth, limited, config);              break; }
    case WILDCARD_ITERATOR:                 { printWildcardIt(reply, root, counters, cpuTime, depth, limited, config);              break; }
    case EMPTY_ITERATOR:                    { printEmptyIt(reply, root, counters, cpuTime, depth, limited, config);                 break; }
    case ID_LIST_SORTED_ITERATOR:           { printIdListSortedIt(reply, root, counters, cpuTime, depth, limited, config);          break; }
    case ID_LIST_UNSORTED_ITERATOR:         { printIdListUnsortedIt(reply, root, counters, cpuTime, depth, limited, config);        break; }
    case PROFILE_ITERATOR:                  { printProfileIt(reply, root, 0, 0, depth, limited, config);                            break; }
    case HYBRID_ITERATOR:                   { printHybridIt(reply, root, counters, cpuTime, depth, limited, config);                break; }
    case METRIC_SORTED_BY_ID_ITERATOR:      { printMetricSortedByIdIt(reply, root, counters, cpuTime, depth, limited, config);      break; }
    case METRIC_SORTED_BY_SCORE_ITERATOR:   { printMetricSortedByScoreIt(reply, root, counters, cpuTime, depth, limited, config);   break; }
    case OPTIMUS_ITERATOR:                  { printOptimusIt(reply, root, counters, cpuTime, depth, limited, config);               break; }
    case MAX_ITERATOR:                      { RS_ABORT("nope");                                                                     break; } // LCOV_EXCL_LINE
  }
}
