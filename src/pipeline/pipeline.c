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
