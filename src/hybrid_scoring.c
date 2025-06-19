#include "hybrid_scoring.h"
#include "rmutil/rm_assert.h"

 /* Get scoring function based on scoring type */
 HybridScoringFunction GetScoringFunction(HybridScoringType scoringType) {
   switch (scoringType) {
     case HYBRID_SCORING_LINEAR:
       // Linear scoring not implemented yet
       return HybridLinearScore;
     case HYBRID_SCORING_RRF:
       return RRFScore;
     default:
       return RRFScore; // Default fallback to RRF
   }
 }

/**
 * Compute Reciprocal Rank Fusion (RRF) score for a document.
 *
 * RRF is used to combine multiple ranked lists into a single score.
 * Each system contributes to the score as 1 / (k + rank), where lower
 * ranks (i.e., higher relevance) contribute more.
 *
 * Formula:
 *     RRF_score = Σ (1 / (k + rank_i)) for all i where has_rank[i] is true
 *
 * - ranks[i] is assumed to be 1-based (i.e., 1 is the best rank).
 * - If a document is not ranked by system i, has_rank[i] should be false.
 * - A typical value for k is 60, which dampens the effect of lower rankings.
 *
 * @param ranks       Array of rank values (e.g., ranks[i] is the rank assigned by system i).
 * @param has_rank    Array of booleans indicating whether a rank was assigned by system i.
 * @param num_sources Number of rank sources (i.e., size of ranks[] and has_rank[]).
 * @param k           Damping constant, Higher values reduce the influence of low-ranked documents.
 *
 * @return RRF score as a double. Higher scores indicate higher relevance.
 */
double RRFScore(ScoringFunctionArgs *ctx,const double *ranks, const bool *has_rank, const size_t num_sources) {
    double score = 0.0;
    for (size_t i = 0; i < num_sources; ++i) {
        if (has_rank[i]) {
            score += 1.0 / (ctx->RRF_k + ranks[i]);
        }
    }
    return score;
}

/**
 * Compute linear hybrid score for a document.
 *
 * The linear score is a weighted sum of scores from multiple sources.
 * Each source's score is multiplied by its corresponding weight.
 *
 * Formula:
 *     linear_score = Σ (weights[i] * scores[i]) for all i where has_score[i] is true
 *
 * - scores[i] is the score assigned by source i.
 * - weights[i] is the weight assigned to source i.
 * - If a document is not scored by system i, has_score[i] should be false.
 *
 * @param ctx         Scoring context containing weights,
 * @param scores      Array of score values (e.g., scores[i] is the score assigned by system i).
 * @param has_score   Array of booleans indicating whether a score was assigned by system i.
 * @param num_sources Number of score sources (i.e., size of scores[] and has_score[]).
 *
 * @return Linear hybrid score as a double.
 */
double HybridLinearScore(ScoringFunctionArgs *ctx, const double *scores, const bool *has_score, const size_t num_sources) {
    RS_ASSERT(ctx->numScores == num_sources);
    double score = 0.0;
    for (size_t i = 0; i < num_sources; ++i) {
        if (has_score[i]) {
            score += ctx->linearWeights[i] * scores[i];
        }
    }
    return score;
}
