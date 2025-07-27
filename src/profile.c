/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#include "profile.h"
#include "iterators/profile_iterator.h"
#include "reply_macros.h"
#include "util/units.h"

void printReadIt(RedisModule_Reply *reply, QueryIterator *root, ProfileCounters *counters, double cpuTime, PrintProfileConfig *config) {
  IndexReader *ir = root->ctx;

  RedisModule_Reply_Map(reply);
  if (ir->idx->flags == Index_DocIdsOnly) {
    if (ir->record->data.term.term != NULL) {
      printProfileType("TAG");
      REPLY_KVSTR_SAFE("Term", ir->record->data.term.term->str);
    }
  } else if (ir->idx->flags & Index_StoreNumeric) {
    const NumericFilter *flt = ir->decoderCtx.filter;
    if (!flt || flt->geoFilter == NULL) {
      printProfileType("NUMERIC");
      RedisModule_Reply_SimpleString(reply, "Term");
      RedisModule_Reply_SimpleStringf(reply, "%g - %g", ir->profileCtx.numeric.rangeMin, ir->profileCtx.numeric.rangeMax);
    } else {
      printProfileType("GEO");
      RedisModule_Reply_SimpleString(reply, "Term");
      double se[2];
      double nw[2];
      decodeGeo(ir->profileCtx.numeric.rangeMin, se);
      decodeGeo(ir->profileCtx.numeric.rangeMax, nw);
      RedisModule_Reply_SimpleStringf(reply, "%g,%g - %g,%g", se[0], se[1], nw[0], nw[1]);
    }
  } else {
    printProfileType("TEXT");
    REPLY_KVSTR_SAFE("Term", ir->record->data.term.term->str);
  }

  // print counter and clock
  if (config->printProfileClock) {
    printProfileTime(cpuTime);
  }

  printProfileCounters(counters);
  RedisModule_ReplyKV_LongLong(reply, "Size", root->NumEstimated(ir));

  RedisModule_Reply_MapEnd(reply);
}

static double _recursiveProfilePrint(RedisModule_Reply *reply, ResultProcessor *rp, int printProfileClock) {
  if (rp == NULL) {
    return 0;
  }
  double upstreamTime = _recursiveProfilePrint(reply, rp->upstream, printProfileClock);

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
        printProfileType(RPTypeToString(rp->type));
        break;

      case RP_PROJECTOR:
      case RP_FILTER:
        RPEvaluator_Reply(reply, "Type", rp);
        break;

      case RP_SAFE_LOADER:
        printProfileType(RPTypeToString(rp->type));
        printProfileGILTime(rp->GILTime);
        break;

      case RP_PROFILE:
      case RP_MAX:
        RS_ABORT("RPType error");
        break;
    }

    return upstreamTime;
  }

  double totalRPTime = (double)(RPProfile_GetClock(rp) / CLOCKS_PER_MILLISEC);
  if (printProfileClock) {
    printProfileTime(totalRPTime - upstreamTime);
  }
  printProfileCounter(RPProfile_GetCount(rp) - 1);
  RedisModule_Reply_MapEnd(reply); // end of recursive map
  return totalRPTime;
}

static double printProfileRP(RedisModule_Reply *reply, ResultProcessor *rp, int printProfileClock) {
  return _recursiveProfilePrint(reply, rp, printProfileClock);
}

void Profile_Print(RedisModule_Reply *reply, void *ctx) {
  ProfilePrinterCtx *profileCtx = ctx;
  AREQ *req = profileCtx->req;
  bool timedout = profileCtx->timedout;
  bool reachedMaxPrefixExpansions = profileCtx->reachedMaxPrefixExpansions;
  bool bgScanOOM = profileCtx->bgScanOOM;
  req->totalTime += clock() - req->initClock;

  //-------------------------------------------------------------------------------------------
  RedisModule_Reply_Map(reply);
      int profile_verbose = req->reqConfig.printProfileClock;
      // Print total time
      if (profile_verbose)
        RedisModule_ReplyKV_Double(reply, "Total profile time",
          (double)(req->totalTime / CLOCKS_PER_MILLISEC));

      // Print query parsing time
      if (profile_verbose)
        RedisModule_ReplyKV_Double(reply, "Parsing time",
          (double)(req->parseTime / CLOCKS_PER_MILLISEC));

      // Print iterators creation time
        if (profile_verbose)
          RedisModule_ReplyKV_Double(reply, "Pipeline creation time",
            (double)(req->pipelineBuildTime / CLOCKS_PER_MILLISEC));

      //Print total GIL time
        if (profile_verbose){
          if (RunInThread()){
            RedisModule_ReplyKV_Double(reply, "Total GIL time",
            rs_timer_ms(&req->qiter.GILTime));
          } else {
            struct timespec rpEndTime;
            clock_gettime(CLOCK_MONOTONIC, &rpEndTime);
            rs_timersub(&rpEndTime, &req->qiter.initTime, &rpEndTime);
            RedisModule_ReplyKV_Double(reply, "Total GIL time", rs_timer_ms(&rpEndTime));
          }
        }

      // Print whether a warning was raised throughout command execution
      if (bgScanOOM) {
        RedisModule_ReplyKV_SimpleString(reply, "Warning", QUERY_WINDEXING_FAILURE);
      }
      if (timedout) {
        RedisModule_ReplyKV_SimpleString(reply, "Warning", QueryError_Strerror(QUERY_ETIMEDOUT));
      } else if (reachedMaxPrefixExpansions) {
        RedisModule_ReplyKV_SimpleString(reply, "Warning", QUERY_WMAXPREFIXEXPANSIONS);
      } else {
        RedisModule_ReplyKV_SimpleString(reply, "Warning", "None");
      }

      // print into array with a recursive function over result processors

      // Print profile of iterators
      QueryIterator *root = QITR_GetRootFilter(&req->qiter);
      // Coordinator does not have iterators
      if (root) {
        RedisModule_Reply_SimpleString(reply, "Iterators profile");
        PrintProfileConfig config = {.iteratorsConfig = &req->ast.config,
                                     .printProfileClock = profile_verbose};
        printIteratorProfile(reply, root, 0, 0, 2, req->reqflags & QEXEC_F_PROFILE_LIMITED, &config);
      }

      // Print profile of result processors
      ResultProcessor *rp = req->qiter.endProc;
      RedisModule_ReplyKV_Array(reply, "Result processors profile");
        printProfileRP(reply, rp, req->reqConfig.printProfileClock);
      RedisModule_Reply_ArrayEnd(reply);
  RedisModule_Reply_MapEnd(reply);
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
      QueryIterator *child = ((NotIterator *)((*root)->ctx))->child;
      Profile_AddIters(&child);
      ((NotIterator *)((*root)->ctx))->child = child;
      break;
    }
    case OPTIONAL_ITERATOR: {
      QueryIterator *child = ((OptionalIterator *)((*root)->ctx))->child;
      Profile_AddIters(&child);
      ((OptionalIterator *)((*root)->ctx))->child = child;
      break;
    }
    case HYBRID_ITERATOR: {
      QueryIterator *child = ((HybridIterator *)((*root)->ctx))->child;
      Profile_AddIters(&child);
      ((HybridIterator *)((*root)->ctx))->child = child;
      break;
    }
    case OPTIMUS_ITERATOR: {
      QueryIterator *child = ((OptimizerIterator *)((*root)->ctx))->child;
      Profile_AddIters(&child);
      ((OptimizerIterator *)((*root)->ctx))->child = child;
      break;
    }
    case UNION_ITERATOR: {
      UnionIterator *ui = (*root)->ctx;
      for (int i = 0; i < ui->norig; i++) {
        Profile_AddIters(&(ui->origits[i]));
      }
      UI_SyncIterList(ui);
      break;
    }
    case INTERSECT_ITERATOR: {
      IntersectIterator *ini = (*root)->ctx;
      for (int i = 0; i < ini->num; i++) {
        Profile_AddIters(&(ini->its[i]));
      }
      break;
    }
    case WILDCARD_ITERATOR:
    case READ_ITERATOR:
    case EMPTY_ITERATOR:
    case ID_LIST_ITERATOR:
    case METRIC_ITERATOR:
      break;
    case PROFILE_ITERATOR:
    case MAX_ITERATOR:
      RS_ABORT("Error");
      break;
  }

  // Create a profile iterator and update outparam pointer
  *root = NewProfileIterator(*root);
}

#define PRINT_PROFILE_FUNC(name) static void name(RedisModule_Reply *reply,   \
                                                  IndexIterator *root,        \
                                                  ProfileCounters *counters,  \
                                                  double cpuTime,             \
                                                  int depth,                  \
                                                  int limited,                \
                                                  PrintProfileConfig *config)

PRINT_PROFILE_FUNC(printUnionIt) {
  UnionIterator *ui = (UnionIterator *)root;
  int printFull = !limited  || (ui->origType & QN_UNION);

  RedisModule_Reply_Map(reply);

  printProfileType("UNION");

  RedisModule_Reply_SimpleString(reply, "Query type");
  char *unionTypeStr;
  switch (ui->origType) {
  case QN_GEO : unionTypeStr = "GEO"; break;
  case QN_GEOMETRY : unionTypeStr = "GEOSHAPE"; break;
  case QN_TAG : unionTypeStr = "TAG"; break;
  case QN_UNION : unionTypeStr = "UNION"; break;
  case QN_FUZZY : unionTypeStr = "FUZZY"; break;
  case QN_PREFIX : unionTypeStr = "PREFIX"; break;
  case QN_NUMERIC : unionTypeStr = "NUMERIC"; break;
  case QN_LEXRANGE : unionTypeStr = "LEXRANGE"; break;
  case QN_WILDCARD_QUERY : unionTypeStr = "WILDCARD"; break;
  default:
    RS_ABORT_ALWAYS("Invalid type for union");
  }
  if (!ui->qstr) {
    RedisModule_Reply_SimpleString(reply, unionTypeStr);
  } else {
    const char *qstr = ui->qstr;
    if (isUnsafeForSimpleString(qstr)) qstr = escapeSimpleString(qstr);
    RedisModule_Reply_SimpleStringf(reply, "%s - %s", unionTypeStr, qstr);
    if (qstr != ui->qstr) rm_free((char*)qstr);
  }

  if (config->printProfileClock) {
    printProfileTime(cpuTime);
  }

  printProfileCounters(counters);

  RedisModule_Reply_SimpleString(reply, "Child iterators");
  if (printFull) {
    RedisModule_Reply_Array(reply);
      for (int i = 0; i < ui->norig; i++) {
        printIteratorProfile(reply, ui->origits[i], 0, 0, depth + 1, limited, config);
      }
    RedisModule_Reply_ArrayEnd(reply);
  } else {
    RedisModule_Reply_SimpleStringf(reply, "The number of iterators in the union is %d", ui->norig);
  }

  RedisModule_Reply_MapEnd(reply);
}

PRINT_PROFILE_FUNC(printIntersectIt) {
  IntersectIterator *ii = (IntersectIterator *)root;

  RedisModule_Reply_Map(reply);

  printProfileType("INTERSECT");

  if (config->printProfileClock) {
    printProfileTime(cpuTime);
  }

  printProfileCounters(counters);

  RedisModule_ReplyKV_Array(reply, "Child iterators");
    for (int i = 0; i < ii->num; i++) {
      if (ii->its[i]) {
        printIteratorProfile(reply, ii->its[i], 0, 0, depth + 1, limited, config);
      } else {
        RedisModule_Reply_Null(reply);
      }
    }
  RedisModule_Reply_ArrayEnd(reply);

  RedisModule_Reply_MapEnd(reply);
}

PRINT_PROFILE_FUNC(printMetricIt) {
  RedisModule_Reply_Map(reply);

  switch (GetMetric(root)) {
    case VECTOR_DISTANCE: {
      printProfileType("METRIC - VECTOR DISTANCE");
      break;
    }
    default: {
      RS_ABORT("Invalid type for metric");
      break;
    }
  }

  if (config->printProfileClock) {
    printProfileTime(cpuTime);
  }

  printProfileCounters(counters);

  RedisModule_Reply_MapEnd(reply);
}

void PrintIteratorChildProfile(RedisModule_Reply *reply, IndexIterator *root, ProfileCounters *counters, double cpuTime,
                  int depth, int limited, PrintProfileConfig *config, IndexIterator *child, const char *text) {
  size_t nlen = 0;
  RedisModule_Reply_Map(reply);
    printProfileType(text);
    if (config->printProfileClock) {
      printProfileTime(cpuTime);
    }
    printProfileCounters(counters);

    if (root->type == HYBRID_ITERATOR) {
      HybridIterator *hi = root->ctx;
      if (hi->searchMode == VECSIM_HYBRID_BATCHES ||
          hi->searchMode == VECSIM_HYBRID_BATCHES_TO_ADHOC_BF) {
        printProfileNumBatches(hi);
      }
    }

    if (root->type == OPTIMUS_ITERATOR) {
      OptimizerIterator *oi = root->ctx;
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

PRINT_PROFILE_SINGLE_NO_CHILD(printWildcardIt,          "WILDCARD");
PRINT_PROFILE_SINGLE_NO_CHILD(printIdListIt,            "ID-LIST");
PRINT_PROFILE_SINGLE_NO_CHILD(printEmptyIt,             "EMPTY");
PRINT_PROFILE_SINGLE(printNotIt, NotIterator,           "NOT");
PRINT_PROFILE_SINGLE(printOptionalIt, OptionalIterator, "OPTIONAL");
PRINT_PROFILE_SINGLE(printHybridIt, HybridIterator,     "VECTOR");
PRINT_PROFILE_SINGLE(printOptimusIt, OptimizerIterator, "OPTIMIZER");

PRINT_PROFILE_FUNC(printProfileIt) {
  ProfileIteratorCtx *pi = (ProfileIteratorCtx *)root;
  printIteratorProfile(reply, pi->child, &pi->counters,
    (double)(pi->cpuTime / CLOCKS_PER_MILLISEC), depth, limited, config);
}

void printIteratorProfile(RedisModule_Reply *reply, IndexIterator *root, ProfileCounters *counters,
                          double cpuTime, int depth, int limited, PrintProfileConfig *config) {
  if (root == NULL) return;

  // protect against limit of 7 reply layers
  if (depth == REDIS_ARRAY_LIMIT && !isFeatureSupported(NO_REPLY_DEPTH_LIMIT)) {
    RedisModule_Reply_Null(reply);
    return;
  }

  switch (root->type) {
    // Reader
    case READ_ITERATOR:       { printReadIt(reply, root, counters, cpuTime, config);                       break; }
    // Multi values
    case UNION_ITERATOR:      { printUnionIt(reply, root, counters, cpuTime, depth, limited, config);      break; }
    case INTERSECT_ITERATOR:  { printIntersectIt(reply, root, counters, cpuTime, depth, limited, config);  break; }
    // Single value
    case NOT_ITERATOR:        { printNotIt(reply, root, counters, cpuTime, depth, limited, config);        break; }
    case OPTIONAL_ITERATOR:   { printOptionalIt(reply, root, counters, cpuTime, depth, limited, config);   break; }
    case WILDCARD_ITERATOR:   { printWildcardIt(reply, root, counters, cpuTime, depth, limited, config);   break; }
    case EMPTY_ITERATOR:      { printEmptyIt(reply, root, counters, cpuTime, depth, limited, config);      break; }
    case ID_LIST_ITERATOR:    { printIdListIt(reply, root, counters, cpuTime, depth, limited, config);     break; }
    case PROFILE_ITERATOR:    { printProfileIt(reply, root, 0, 0, depth, limited, config);                break; }
    case HYBRID_ITERATOR:     { printHybridIt(reply, root, counters, cpuTime, depth, limited, config);     break; }
    case METRIC_ITERATOR:     { printMetricIt(reply, root, counters, cpuTime, depth, limited, config);     break; }
    case OPTIMUS_ITERATOR:    { printOptimusIt(reply, root, counters, cpuTime, depth, limited, config);    break; }
    case MAX_ITERATOR:        { RS_ABORT("nope");   break; }
  }
}
