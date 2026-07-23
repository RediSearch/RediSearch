/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include <stdio.h>
#include <stdint.h>

#include "search_result_ffi.h"
#include "hybrid_search_result.h"
#include "score_explain.h"
#include "hybrid_scoring.h"
#include "hybrid_request.h"
#include "aggregate/aggregate.h"
#include "vector_index.h"
#include "config.h"
#include "hybrid/hybrid_lookup_context.h"
#include "hybrid/vector_query_utils.h"
#include "pipeline/pipeline.h"
#include "rlookup_ffi.h"
#include "rmalloc.h"
#include "rmutil/rm_assert.h"
#include "search_result.h"

void HybridExplainContext_Free(HybridExplainContext *ctx) {
  if (!ctx) return;
  rm_free(ctx->textScorerName);
  rm_free(ctx->vectorBranchEnvelope);
  rm_free(ctx);
}

HybridExplainContext *HybridExplainContext_Build(const struct AREQ *searchReq, const struct AREQ *vectorReq) {
  RS_ASSERT(searchReq && vectorReq);

  HybridExplainContext *ctx = rm_calloc(1, sizeof(HybridExplainContext));

  // Duplicate the resolved scorer name: searchopts.scorerName may point into
  // RSGlobalConfig.defaultScorer, which FT.CONFIG SET DEFAULT_SCORER frees and
  // replaces. AREQ_ApplyContext resolves scorerName to the configured default
  // before we get here, but we fall back defensively in case that ever changes.
  const char *resolved = searchReq->searchopts.scorerName
                             ? searchReq->searchopts.scorerName
                             : RSGlobalConfig.defaultScorer;
  ctx->textScorerName = resolved ? rm_strdup(resolved) : NULL;

  // Vector branch envelope: identify the retrieval mode and any salient params.
  // Read from the parsed-time snapshot on ParsedVectorData, since the live
  // VectorQuery has already been transferred into the AST by AREQ_ApplyContext.
  const ParsedVectorData *pvd = vectorReq->parsedVectorData;
  ctx->vectorMode = pvd ? pvd->explainQueryType : VECSIM_QT_KNN;

  if (pvd && pvd->explainQueryType == VECSIM_QT_RANGE) {
    if (pvd->explainRangeEpsilon) {
      rm_asprintf(&ctx->vectorBranchEnvelope,
                  "vector branch (RANGE: radius=%.4f, epsilon=%s)",
                  pvd->explainRangeRadius, pvd->explainRangeEpsilon);
    } else {
      rm_asprintf(&ctx->vectorBranchEnvelope,
                  "vector branch (RANGE: radius=%.4f)", pvd->explainRangeRadius);
    }
  } else {
    ctx->vectorBranchEnvelope = rm_strdup("vector branch (KNN)");
  }
  return ctx;
}

/**
 * Constructor for HybridSearchResult.
 * Allocates memory for storing SearchResults from numSources sources.
 */
HybridSearchResult* HybridSearchResult_New(size_t numSources) {
  RS_ASSERT(numSources > 0);

  HybridSearchResult* result = rm_calloc(1, sizeof(HybridSearchResult));
  result->searchResults = array_newlen(SearchResult*, numSources);
  result->hasResults = array_newlen(bool, numSources);
  for (size_t i = 0; i < numSources; ++i) {
    result->searchResults[i] = NULL;
    result->hasResults[i] = false;
  }
  result->numSources = numSources;
  return result;
}

/**
 * Destructor for HybridSearchResult.
 * Frees all stored SearchResults and the structure itself.
 */
void HybridSearchResult_Free(HybridSearchResult* result) {
  if (!result) {
    return;
  }

  // Free individual SearchResults with array_free_ex
  array_free_ex(result->searchResults, {
    SearchResult **sr = (SearchResult**)ptr;
    if (*sr) {
      SearchResult_Destroy(*sr);
      rm_free(*sr);
    }
  });

  array_free(result->hasResults);

  rm_free(result);
}

/**
 * Store a SearchResult from a source into the HybridSearchResult.
 * Updates the score of the SearchResult and marks the source as having results.
 */
void HybridSearchResult_StoreResult(HybridSearchResult* hybridResult, SearchResult* searchResult, int sourceIndex) {
  RS_ASSERT(hybridResult && searchResult);
  RS_ASSERT(sourceIndex < hybridResult->numSources);

  // Store the SearchResult from this source (preserving all data)
  hybridResult->searchResults[sourceIndex] = searchResult;
  hybridResult->hasResults[sourceIndex] = true;
}


/**
 * Calculate hybrid score from multiple sources by combining their individual scores.
 * Supports both RRF (with ranks) and Linear (with scores) hybrid scoring.
 */
double calculateHybridScore(HybridSearchResult *hybridResult, HybridScoringContext *scoringCtx) {
  RS_ASSERT(scoringCtx && hybridResult)

  // Extract values from SearchResults
  arrayof(double) values = array_newlen(double, hybridResult->numSources);
  for (size_t i = 0; i < hybridResult->numSources; i++) {
    if (hybridResult->hasResults[i]) {
      RS_ASSERT(hybridResult->searchResults[i]);
      // Note: SearchResult->score contains ranks for RRF, scores for Linear
      // This is set correctly by upstream processors based on scoring type
      values[i] = SearchResult_GetScore(hybridResult->searchResults[i]);
    } else {
      values[i] = 0.0;  // Default value for missing results
    }
  }

  // Calculate hybrid score using generic scoring function
  HybridScoringFunction scoringFunc = GetScoringFunction(scoringCtx->scoringType);
  double result = scoringFunc(scoringCtx, values, hybridResult->hasResults, hybridResult->numSources);

  array_free(values);
  return result;
}

/**
 * Merge field data from multiple source SearchResults into target SearchResult's rowdata.
 */
static void mergeRLookupRowsFromSourcesIntoTarget(HybridSearchResult *hybridResult,
                            HybridLookupContext *lookupCtx,
                            SearchResult *targetResult) {
  RS_ASSERT(hybridResult && lookupCtx);

  // Write fields from each source SearchResult
  for (size_t i = 0; i < hybridResult->numSources; i++) {
    if (hybridResult->hasResults[i]) {
      SearchResult *sourceResult = hybridResult->searchResults[i];
      RS_ASSERT(sourceResult);

      // move fields from source row to destination row
      RLookupRow_MoveFieldsFrom(lookupCtx->tailLookup, SearchResult_GetRowDataMut(sourceResult), SearchResult_GetRowDataMut(targetResult));
    }
  }
}

// Shallow-copy `src`'s root struct into `dst` and free the now-empty `src`
// container. The string + children pointers are transferred; `src` must not
// be used after this call.
static void moveScoreExplainInto(RSScoreExplain *dst, RSScoreExplain *src) {
  *dst = *src;
  rm_free(src);
}

// Detach and return the search sub-result's RSScoreExplain (or NULL).
// The caller takes ownership; the source no longer points to it.
static RSScoreExplain *takeTextSubresultScoreExplain(const HybridSearchResult *hybridResult) {
  SearchResult *textResult = hybridResult->searchResults[SEARCH_INDEX];
  RSScoreExplain *scorerSubtree = SearchResult_GetScoreExplainMut(textResult);
  if (scorerSubtree) {
    SearchResult_SetScoreExplain(textResult, NULL);
  }
  return scorerSubtree;
}

static char *formatTextBranchLabel(const HybridScoringContext *scoringCtx, bool isRRF,
                                   double textValue) {
  char *out = NULL;
  if (isRRF) {
    rm_asprintf(&out, "text rank = %.0f", textValue);
  } else {
    const double alpha = scoringCtx->linearCtx.numWeights > 0
                             ? scoringCtx->linearCtx.linearWeights[0]
                             : 0.0;
    rm_asprintf(&out, "text contribution = %.4f * %.4f = %.4f",
                alpha, textValue, alpha * textValue);
  }
  return out;
}

// Takes ownership of `scorerSubtree` (may be NULL).
static RSScoreExplain *buildTextScorerNode(const HybridExplainContext *explainCtx, bool isRRF,
                                           double textValue, RSScoreExplain *scorerSubtree) {
  RSScoreExplain *node = rm_calloc(1, sizeof(RSScoreExplain));
  rm_asprintf(&node->str, "Text scorer: %s",
              explainCtx->textScorerName ? explainCtx->textScorerName : "<default>");

  if (isRRF) {
    if (scorerSubtree) {
      node->numChildren = 1;
      node->children = rm_calloc(1, sizeof(RSScoreExplain));
      moveScoreExplainInto(&node->children[0], scorerSubtree);
    }
    return node;
  }

  // LINEAR: extra "normalized text score = X" sibling so the formula in the
  // parent line is fully derivable from the children.
  const int n = scorerSubtree ? 2 : 1;
  node->numChildren = n;
  node->children = rm_calloc(n, sizeof(RSScoreExplain));
  rm_asprintf(&node->children[0].str, "normalized text score = %.4f", textValue);
  if (scorerSubtree) {
    moveScoreExplainInto(&node->children[1], scorerSubtree);
  }
  return node;
}

// Build the text branch sub-tree.
//
//   RRF:    "text rank = N"
//             └── "Text scorer: <NAME>"
//                  └── <existing scorer subtree>
//
//   LINEAR: "text contribution = alpha * X = alpha*X"
//             └── "Text scorer: <NAME>"
//                  ├── "normalized text score = X"
//                  └── <existing scorer subtree>
//
// When the text sub-result is absent (no match), emit a leaf placeholder.
static RSScoreExplain *buildTextBranch(const HybridSearchResult *hybridResult,
                                       const HybridScoringContext *scoringCtx,
                                       const HybridExplainContext *explainCtx,
                                       double textValue) {
  const bool hasText = hybridResult->hasResults[SEARCH_INDEX];
  const bool isRRF = (scoringCtx->scoringType == HYBRID_SCORING_RRF);

  RSScoreExplain *parent = rm_calloc(1, sizeof(RSScoreExplain));

  if (!hasText) {
    parent->str = rm_strdup(isRRF ? "text rank = <no match>"
                                  : "text contribution = <no match>");
    return parent;
  }

  parent->str = formatTextBranchLabel(scoringCtx, isRRF, textValue);

  RSScoreExplain *scorerSubtree = takeTextSubresultScoreExplain(hybridResult);
  RSScoreExplain *scorerNode =
      buildTextScorerNode(explainCtx, isRRF, textValue, scorerSubtree);

  parent->numChildren = 1;
  parent->children = rm_calloc(1, sizeof(RSScoreExplain));
  moveScoreExplainInto(&parent->children[0], scorerNode);
  return parent;
}

// Build the vector branch sub-tree.
//
//   RRF:    "vector branch (KNN)" or "vector branch (RANGE: …)"
//             ├── "matched within radius = true/false"   # RANGE only
//             └── "vector rank = M"                     # or "<no match>"
//
//   LINEAR: "vector branch (KNN)" or "vector branch (RANGE: …)"
//             ├── "matched within radius = true/false"   # RANGE only
//             ├── "vector contribution = beta * Y = beta*Y"
//             └── "normalized vector score = Y"
static RSScoreExplain *buildVectorBranch(const HybridSearchResult *hybridResult,
                                         const HybridScoringContext *scoringCtx,
                                         const HybridExplainContext *explainCtx,
                                         double vectorValue) {
  const bool hasVector = hybridResult->hasResults[VECTOR_INDEX];
  const bool isRRF = (scoringCtx->scoringType == HYBRID_SCORING_RRF);
  const bool isRange = (explainCtx->vectorMode == VECSIM_QT_RANGE);

  RSScoreExplain *parent = rm_calloc(1, sizeof(RSScoreExplain));
  parent->str = rm_strdup(explainCtx->vectorBranchEnvelope);

  // Compute child slots:
  //   RANGE: + "matched within radius = …"
  //   RRF: + "vector rank = M" / "<no match>"
  //   LINEAR: + "vector contribution = …" + "normalized vector score = …" (when matched)
  //   LINEAR no-match: + single placeholder
  size_t slot = 0;
  RSScoreExplain children[3] = {0};

  if (isRange) {
    rm_asprintf(&children[slot++].str, "matched within radius = %s",
                hasVector ? "true" : "false");
  }

  if (isRRF) {
    if (hasVector) {
      rm_asprintf(&children[slot++].str, "vector rank = %.0f", vectorValue);
    } else {
      children[slot++].str = rm_strdup("vector rank = <no match>");
    }
  } else {
    if (hasVector) {
      const double beta = scoringCtx->linearCtx.numWeights > 1
                              ? scoringCtx->linearCtx.linearWeights[1]
                              : 0.0;
      rm_asprintf(&children[slot++].str,
                  "vector contribution = %.4f * %.4f = %.4f", beta, vectorValue, beta * vectorValue);
      rm_asprintf(&children[slot++].str, "normalized vector score = %.4f", vectorValue);
    } else {
      children[slot++].str = rm_strdup("vector contribution = <no match>");
    }
  }

  parent->numChildren = (int)slot;
  parent->children = rm_calloc(slot, sizeof(RSScoreExplain));
  for (size_t i = 0; i < slot; i++) {
    parent->children[i] = children[i];
  }
  return parent;
}

// Build the per-document RSScoreExplain wrapper for a hybrid result.
//
// Layout (MOD-10044):
//   "final score: <S>"
//     └── hybrid score node from HybridScoring_FormatHybridScoreNode
//           ├── text branch sub-tree
//           └── vector branch sub-tree
static RSScoreExplain *buildHybridScoreExplain(const HybridSearchResult *hybridResult,
                                               const HybridScoringContext *scoringCtx,
                                               const HybridExplainContext *explainCtx,
                                               double textValue, double vectorValue,
                                               double finalScore) {
  // Local stack array threaded into HybridScoring_FormatFinalScoreLine, which
  // takes the generic (values, hasResults, numSources) tuple.
  const double values[HYBRID_REQUEST_NUM_SUBQUERIES] = {textValue, vectorValue};

  RSScoreExplain *outer = rm_calloc(1, sizeof(RSScoreExplain));
  // Outer line is a formula, not just the value — matches the existing TEXT
  // EXPLAINSCORE convention (e.g. "(0.29 = Weight 1.00 * IDF 0.29 * …)").
  outer->str = HybridScoring_FormatFinalScoreLine(
      scoringCtx, values, hybridResult->hasResults,
      hybridResult->numSources, finalScore);

  RSScoreExplain *hybridNode = rm_calloc(1, sizeof(RSScoreExplain));
  hybridNode->str = HybridScoring_FormatHybridScoreNode(scoringCtx);

  RSScoreExplain *textBranch = buildTextBranch(hybridResult, scoringCtx, explainCtx, textValue);
  RSScoreExplain *vectorBranch = buildVectorBranch(hybridResult, scoringCtx, explainCtx, vectorValue);

  hybridNode->numChildren = 2;
  hybridNode->children = rm_calloc(2, sizeof(RSScoreExplain));
  moveScoreExplainInto(&hybridNode->children[0], textBranch);
  moveScoreExplainInto(&hybridNode->children[1], vectorBranch);

  outer->numChildren = 1;
  outer->children = rm_calloc(1, sizeof(RSScoreExplain));
  moveScoreExplainInto(&outer->children[0], hybridNode);

  return outer;
}

SearchResult* mergeSearchResults(HybridSearchResult *hybridResult, HybridScoringContext *scoringCtx,
                                 HybridLookupContext *lookupCtx, const HybridExplainContext *explainCtx) {
  RS_ASSERT(hybridResult && scoringCtx);

  // Pick the "primary" — first non-null source in index order. This biases
  // toward the text branch (SEARCH_INDEX=0) when both branches matched, so
  // the merged result keeps the search-side RSIndexResult.
  SearchResult *primary = NULL;
  int8_t targetIndex = -1;
  for (size_t i = 0; i < hybridResult->numSources; i++) {
    if (hybridResult->hasResults[i]) {
      RS_ASSERT(hybridResult->searchResults[i]);
      primary = hybridResult->searchResults[i];
      targetIndex = (int8_t)i;
      break;
    }
  }

  RS_ASSERT(primary && targetIndex != -1);

  // Snapshot per-source values as scalars before SearchResult_SetScore
  // overwrites the primary. For RRF these are 1-based ranks; for LINEAR they
  // are raw scores. Only needed (and only valid) when EXPLAINSCORE is on —
  // FT.HYBRID always pairs exactly two sub-queries (text + vector); the
  // generic N-upstream form has no EXPLAINSCORE rendering.
  double textValue = 0.0, vectorValue = 0.0;
  if (explainCtx) {
    RS_ASSERT(hybridResult->numSources == HYBRID_REQUEST_NUM_SUBQUERIES);
    if (hybridResult->hasResults[SEARCH_INDEX]) {
      textValue = SearchResult_GetScore(hybridResult->searchResults[SEARCH_INDEX]);
    }
    if (hybridResult->hasResults[VECTOR_INDEX]) {
      vectorValue = SearchResult_GetScore(hybridResult->searchResults[VECTOR_INDEX]);
    }
  }

  double finalScore = calculateHybridScore(hybridResult, scoringCtx);
  SearchResult_SetScore(primary, finalScore);

  for (size_t i = 0; i < hybridResult->numSources; i++) {
    if (hybridResult->hasResults[i] && i != targetIndex) {
      RS_ASSERT(hybridResult->searchResults[i]);
      SearchResult_MergeFlags(primary, hybridResult->searchResults[i]);
    }
  }

  // Build the wrapper before transferring ownership: it moves the search
  // sub-result's RSScoreExplain (if any) into one of its children.
  if (explainCtx) {
    RSScoreExplain *wrapper = buildHybridScoreExplain(hybridResult, scoringCtx, explainCtx,
                                                      textValue, vectorValue, finalScore);
    SearchResult_SetScoreExplain(primary, wrapper);
  }

  // Detach the primary so the dict destructor (HybridSearchResult_Free) does
  // not double-free it.
  hybridResult->searchResults[targetIndex] = NULL;
  hybridResult->hasResults[targetIndex] = false;
  // Use a temporary row in the merge to avoid modifying `primary` while we
  // still read from it.
  mergeRLookupRowsFromSourcesIntoTarget(hybridResult, lookupCtx, primary);
  return primary;
}
