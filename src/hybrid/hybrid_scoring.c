#include "hybrid/hybrid_scoring.h"
#include "rmutil/rm_assert.h"
#include "rmalloc.h"

 /* Get scoring function based on scoring type */
 HybridScoringFunction GetScoringFunction(HybridScoringType scoringType) {
   switch (scoringType) {
     case HYBRID_SCORING_LINEAR:
       return HybridLinearScore;
     case HYBRID_SCORING_RRF:
       return HybridRRFScore;
     default:
       RS_ASSERT(0); // Shouldn't get here
   }
 }

/**
 * Compute Reciprocal Rank Fusion (RRF) score for a document.
 *
 * RRF is used to combine multiple ranked lists into a single score.
 * Each system contributes to the score as 1 / (constant + rank), where lower
 * ranks (i.e., higher relevance) contribute more.
 *
 * Formula:
 *     RRF_score = Σ (1 / (constant + rank_i)) for all i where has_rank[i] is true
 *
 * - ranks[i] is assumed to be 1-based (i.e., 1 is the best rank).
 * - If a document is not ranked by system i, has_rank[i] should be false.
 * - A typical value for constant is 60, which dampens the effect of lower rankings.
 *
 * @param scoringCtx  Scoring context containing RRF parameters (constant, window)
 * @param ranks       Array of rank values (e.g., ranks[i] is the rank assigned by system i).
 * @param has_rank    Array of booleans indicating whether a rank was assigned by system i.
 * @param num_sources Number of rank sources (i.e., size of ranks[] and has_rank[]).
 *
 * @return RRF score as a double. Higher scores indicate higher relevance.
 */
double HybridRRFScore(HybridScoringContext *scoringCtx, const double *ranks, const bool *has_rank, const size_t num_sources) {

    RS_ASSERT(scoringCtx->scoringType == HYBRID_SCORING_RRF);

    double score = 0.0;
    for (size_t i = 0; i < num_sources; ++i) {
        if (has_rank[i]) {
            score += 1.0 / (scoringCtx->rrfCtx.constant + ranks[i]);
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
 * @param scoringCtx  Scoring context containing weights
 * @param scores      Array of score values (e.g., scores[i] is the score assigned by system i).
 * @param has_score   Array of booleans indicating whether a score was assigned by system i.
 * @param num_sources Number of score sources (i.e., size of scores[] and has_score[]).
 *
 * @return Linear hybrid score as a double.
 */
double HybridLinearScore(HybridScoringContext *scoringCtx, const double *scores, const bool *has_score, const size_t num_sources) {
    RS_ASSERT(scoringCtx->scoringType == HYBRID_SCORING_LINEAR);
    RS_ASSERT(scoringCtx->linearCtx.numWeights == num_sources);
    double score = 0.0;
    for (size_t i = 0; i < num_sources; ++i) {
        if (has_score[i]) {
            score += scoringCtx->linearCtx.linearWeights[i] * scores[i];
        }
    }
    return score;
}

/**
 * Constructor for RRF scoring context.
 * Creates a new HybridScoringContext configured for RRF scoring.
 *
 * @param constant RRF constant parameter (supports floating-point values)
 * @param window Window size for result processing
 * @param hasExplicitWindow Whether window was explicitly set by user
 * @return Allocated HybridScoringContext or NULL on failure
 */
HybridScoringContext* HybridScoringContext_NewRRF(double constant, size_t window, bool hasExplicitWindow) {
    HybridScoringContext *ctx = rm_calloc(1, sizeof(HybridScoringContext));

    ctx->scoringType = HYBRID_SCORING_RRF;
    ctx->rrfCtx.constant = constant;
    ctx->rrfCtx.window = window;
    ctx->rrfCtx.hasExplicitWindow = hasExplicitWindow;

    return ctx;
}

/**
 * Constructor for Linear scoring context.
 * Creates a new HybridScoringContext configured for Linear scoring.
 *
 * @param weights Array of weight values to copy
 * @param numWeights Number of weights in the array
 * @param window Window size for result processing
 * @return Allocated HybridScoringContext or NULL on failure
 */
HybridScoringContext* HybridScoringContext_NewLinear(const double *weights, size_t numWeights, size_t window) {
    if (!weights || numWeights == 0) return NULL;

    HybridScoringContext *ctx = rm_calloc(1, sizeof(HybridScoringContext));

    ctx->linearCtx.linearWeights = rm_calloc(numWeights, sizeof(double));
    if (!ctx->linearCtx.linearWeights) {
        rm_free(ctx);
        return NULL;
    }

    ctx->scoringType = HYBRID_SCORING_LINEAR;
    memcpy(ctx->linearCtx.linearWeights, weights, numWeights * sizeof(double));
    ctx->linearCtx.numWeights = numWeights;
    ctx->linearCtx.window = window;

    return ctx;
}

/**
 * Constructor with default RRF values.
 * Creates a new HybridScoringContext with default RRF configuration.
 *
 * @return Allocated HybridScoringContext or NULL on failure
 */
HybridScoringContext* HybridScoringContext_NewDefault(void) {
    return HybridScoringContext_NewRRF(HYBRID_DEFAULT_RRF_CONSTANT, HYBRID_DEFAULT_WINDOW, false);
}

/**
 * Generic free function for HybridScoringContext.
 * Frees internal resources based on the scoring type.
 */
void HybridScoringContext_Free(HybridScoringContext *hybridCtx) {
    if (!hybridCtx) return;

    switch (hybridCtx->scoringType) {
        case HYBRID_SCORING_LINEAR:
            if (hybridCtx->linearCtx.linearWeights) {
                rm_free(hybridCtx->linearCtx.linearWeights);
            }
            break;
        case HYBRID_SCORING_RRF:
            // RRF context doesn't have dynamically allocated members
            break;
        default:
            RS_ASSERT(0); // Shouldn't get here
    }
    rm_free(hybridCtx);
}
