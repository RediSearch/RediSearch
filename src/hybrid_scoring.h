#include "redisearch.h"

 typedef enum {
   HYBRID_SCORING_LINEAR,
   HYBRID_SCORING_RRF
 } HybridScoringType;

 typedef double (*HybridScoringFunction)(ScoringFunctionArgs *ctx, const double *scores, const bool *hasScores, const size_t numUpstreams);

 /* Get scoring function based on scoring type */
HybridScoringFunction GetScoringFunction(HybridScoringType scoringType);

double RRFScore(ScoringFunctionArgs *ctx,const double *ranks, const bool *has_rank, const size_t num_sources);

double HybridLinearScore(ScoringFunctionArgs *ctx, const double *scores, const bool *has_score, const size_t num_sources);