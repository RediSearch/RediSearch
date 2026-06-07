/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#pragma once

#include "search_options.h"
#include "util/timeout.h"

struct MetricRequest;
struct AREQ;

typedef struct QueryEvalCtx {
  RedisSearchCtx *sctx;
  const RSSearchOptions *opts;
  QueryError *status;
  struct MetricRequest **metricRequestsP;
  uint32_t tokenId;
  DocTable *docTable;
  // Max doc id captured once at query-snapshot establishment (top of QAST_Iterate), so all
  // snapshot-bounded iterators share a single boundary that is consistent with the SpeedB
  // snapshot the disk iterators read from. Disk: SearchDisk_GetMaxDocId; RAM: docs.maxDocId.
  t_docId maxDocId;
  uint32_t reqFlags;
  IteratorsConfig *config;
  bool notSubtree;
  // AREQ to use for the Blocked Client Timeout dispatch. Non-NULL means
  // iterators poll `AREQ_CheckTimedOut` against this request; NULL means
  // iterators fall back to the in-pipeline clock-based timeout (low-level
  // C API, tests, or any request whose `skipTimeoutChecks` is false).
  // Set via `AREQ_TimeoutAreqOrNull` in `QAST_Iterate`.
  struct AREQ *bcTimeoutAreq;
} QueryEvalCtx;
