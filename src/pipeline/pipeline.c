/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "pipeline/pipeline.h"

#ifdef __cplusplus
extern "C" {
#endif

void Pipeline_Initialize(Pipeline *pipeline, RSTimeoutPolicy timeoutPolicy, QueryError *status) {
  pipeline->qctx.err = status;
  pipeline->qctx.rootProc = pipeline->qctx.endProc = NULL;
  pipeline->qctx.timeoutPolicy = timeoutPolicy;
}

void Pipeline_Clean(Pipeline *pipeline) {
  // Free result processors
  QITR_FreeChain(&pipeline->qctx);
  // Go through each of the steps and free it..
  AGPLN_FreeSteps(&pipeline->ap);
}

#ifdef __cplusplus
}
#endif
