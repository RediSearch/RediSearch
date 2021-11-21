
#pragma once

#include "aggregate/aggregate_plan.h"
#include "aggregate/aggregate.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
  PLN_BaseStep base;
  RLookup lk;
  AGGPlan *plan;
  PLN_GroupStep **oldSteps;  // Old step which this distribute breaks down
#ifdef __cplusplus
  typedef std::vector<char *> SerializedArray;
  SerializedArray *serialized;
#else
  void *serialized;
#endif
  BlkAlloc alloc;
} PLN_DistributeStep;

int AGGPLN_Distribute(AGGPlan *src, QueryError *status);

typedef struct {
  // Arguments to upstream FT.AGGREGATE
  const char **serialized;
  // Length of those arguments
  size_t nserialized;
  // The lookup structure containing the fields that are to be received from upstream
  RLookup *lookup;
} AREQDIST_UpstreamInfo;

/**
 * Builds the static portion of the distributed pipeline
 * @param r the request
 * @param[out] us upstream parameters
 * @param status if there is an error
 */
int AREQ_BuildDistributedPipeline(AREQ *r, AREQDIST_UpstreamInfo *us, QueryError *status);

#ifdef __cplusplus
}
#endif
