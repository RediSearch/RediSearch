#ifndef __HYBRID_SCORING_H__
#define __HYBRID_SCORING_H__

#include "redisearch.h"

#ifdef __cplusplus
extern "C" {
#endif


// Default constants for hybrid search parameters
#define HYBRID_DEFAULT_WINDOW 20
#define HYBRID_DEFAULT_RRF_CONSTANT 60

typedef enum {
  HYBRID_SCORING_LINEAR,
  HYBRID_SCORING_RRF
} HybridScoringType;

typedef struct {
  double *linearWeights;
  size_t numWeights;
  size_t window;
} HybridLinearContext;

typedef struct {
  double constant;
  size_t window;
  bool hasExplicitWindow;           // Flag to track if window was explicitly set in the query args
} HybridRRFContext;

typedef struct {
  HybridScoringType scoringType;
  union {
    HybridLinearContext linearCtx;
    HybridRRFContext rrfCtx;
  };
} HybridScoringContext;

/* Constructor functions for HybridScoringContext */
HybridScoringContext* HybridScoringContext_NewRRF(double constant, size_t window, bool hasExplicitWindow);
HybridScoringContext* HybridScoringContext_NewLinear(const double *weights, size_t numWeights, size_t window);
HybridScoringContext* HybridScoringContext_NewDefault(void);

/* Generic free function for HybridScoringContext */
void HybridScoringContext_Free(HybridScoringContext *hybridCtx);

typedef double (*HybridScoringFunction)(HybridScoringContext *scoringCtx, const double *scores, const bool *hasScores, const size_t numUpstreams);

 /* Get scoring function based on scoring type */
HybridScoringFunction GetScoringFunction(HybridScoringType scoringType);

double HybridRRFScore(HybridScoringContext *scoringCtx, const double *ranks, const bool *has_rank, const size_t num_sources);

double HybridLinearScore(HybridScoringContext *scoringCtx, const double *scores, const bool *has_score, const size_t num_sources);

/**
 * Format the hybrid score node label (the inner node of the EXPLAINSCORE
 * wrapper that holds the text + vector branches).
 *
 * For RRF: "Hybrid score (RRF: window=W, constant=K)".
 * For LINEAR: "Hybrid score (LINEAR: alpha=A, beta=B, window=W)" (assumes the
 * two-source hybrid layout — text=weight[0], vector=weight[1]).
 *
 * The returned string is heap-allocated with rm_malloc and must be freed by
 * the caller (or transferred to an RSScoreExplain node, which frees it).
 */
char *HybridScoring_FormatHybridScoreNode(const HybridScoringContext *scoringCtx);

/**
 * Format the outer "final score: …" line as a formula, mirroring the style
 * of existing TEXT EXPLAINSCORE lines (which show how the score was
 * computed, not just its value).
 *
 *   RRF, both matched:
 *     "final score: 1 / (constant K + rank N1) + 1 / (constant K + rank N2) = S"
 *   RRF, text-only:
 *     "final score: 1 / (constant K + rank N1) + 0 [vector: no match] = S"
 *   LINEAR, both matched:
 *     "final score: A * X + B * Y = S"
 *   LINEAR, text-only:
 *     "final score: A * X + 0 [vector: no match] = S"
 *
 * The returned string is heap-allocated with rm_malloc and must be freed by
 * the caller (or transferred to an RSScoreExplain node, which frees it).
 */
char *HybridScoring_FormatFinalScoreLine(const HybridScoringContext *scoringCtx,
                                         const double *values, const bool *has_value,
                                         size_t num_sources, double finalScore);



#ifdef __cplusplus
}
#endif

#endif // __HYBRID_SCORING_H__
