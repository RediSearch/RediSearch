#ifndef __HYBRID_SCORING_H__
#define __HYBRID_SCORING_H__

#include "redisearch.h"

 typedef enum {
   HYBRID_SCORING_LINEAR,
   HYBRID_SCORING_RRF
 } HybridScoringType;

typedef struct {
  double *linearWeights;
  size_t numWeights;
} HybridLinearContext;

typedef struct {
  size_t k;
  size_t window;
} HybridRRFContext;

typedef struct {
  HybridScoringType scoringType;
  union {
    HybridLinearContext linearCtx;
    HybridRRFContext rrfCtx;
  };
} HybridScoringContext;


 typedef double (*HybridScoringFunction)(ScoringFunctionArgs *ctx, HybridScoringContext *scoringCtx, const double *scores, const bool *hasScores, const size_t numUpstreams);

 /* Get scoring function based on scoring type */
HybridScoringFunction GetScoringFunction(HybridScoringType scoringType);

double HybridRRFScore(ScoringFunctionArgs *ctx, HybridScoringContext *scoringCtx, const double *ranks, const bool *has_rank, const size_t num_sources);

double HybridLinearScore(ScoringFunctionArgs *ctx, HybridScoringContext *scoringCtx, const double *scores, const bool *has_score, const size_t num_sources);

#endif // __HYBRID_SCORING_H__
