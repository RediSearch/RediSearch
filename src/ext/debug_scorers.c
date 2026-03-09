/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

/******************************************************************************************
 *
 * Test Scoring Functions (for testing purposes only)
 *
 * These are simple scoring functions that return individual components of scoring data:
 * - TEST_NUM_DOCS: returns the number of documents in the index
 * - TEST_NUM_TERMS: returns the number of unique terms in the index
 * - TEST_AVG_DOC_LEN: returns the average document length
 * - TEST_SUM_IDF: returns the sum of IDF values from all terms in the result
 * - TEST_SUM_BM25_IDF: returns the sum of BM25 IDF values from all terms in the result
 *
 * They are used for testing the scoring function registration mechanism via debug commands.
 *
 ******************************************************************************************/

#include "debug_scorers.h"
#include "redisearch.h"
#include "types_rs.h"
#include "score_explain.h"
#include "extension.h"

/* Recursively sum IDF values from all terms in the result */
static double sumIdfRecursive(const RSIndexResult *r) {
  if (r->data.tag == RSResultData_Term) {
    RSQueryTerm *term = IndexResult_QueryTermRef(r);
    return term ? QueryTerm_GetIDF(term) : 0;
  }
  if (r->data.tag & (RSResultData_Intersection | RSResultData_Union | RSResultData_HybridMetric)) {
    double sum = 0;
    const RSAggregateResult *agg = IndexResult_AggregateRefUnchecked(r);
    AggregateRecordsSlice children = AggregateResult_GetRecordsSlice(agg);
    for (int i = 0; i < children.len; i++) {
      sum += sumIdfRecursive(children.ptr[i]);
    }
    return sum;
  }
  return 0;
}

/* Recursively sum BM25 IDF values from all terms in the result */
static double sumBm25IdfRecursive(const RSIndexResult *r) {
  if (r->data.tag == RSResultData_Term) {
    RSQueryTerm *term = IndexResult_QueryTermRef(r);
    return term ? QueryTerm_GetBM25_IDF(term) : 0;
  }
  if (r->data.tag & (RSResultData_Intersection | RSResultData_Union | RSResultData_HybridMetric)) {
    double sum = 0;
    const RSAggregateResult *agg = IndexResult_AggregateRefUnchecked(r);
    AggregateRecordsSlice children = AggregateResult_GetRecordsSlice(agg);
    for (int i = 0; i < children.len; i++) {
      sum += sumBm25IdfRecursive(children.ptr[i]);
    }
    return sum;
  }
  return 0;
}

/* Test scoring function that returns the number of documents in the index */
static double TestNumDocsScorer(const ScoringFunctionArgs *ctx, const RSIndexResult *r,
                                const RSDocumentMetadata *dmd, double minScore) {
  RSScoreExplain *scrExp = (RSScoreExplain *)ctx->scrExp;
  double score = (double)ctx->indexStats.numDocs;
  EXPLAIN(scrExp, "TEST_NUM_DOCS: numDocs(%zu) = %.2f", ctx->indexStats.numDocs, score);
  return score;
}

/* Test scoring function that returns the number of unique terms in the index */
static double TestNumTermsScorer(const ScoringFunctionArgs *ctx, const RSIndexResult *r,
                                 const RSDocumentMetadata *dmd, double minScore) {
  RSScoreExplain *scrExp = (RSScoreExplain *)ctx->scrExp;
  double score = (double)ctx->indexStats.numTerms;
  EXPLAIN(scrExp, "TEST_NUM_TERMS: numTerms(%zu) = %.2f", ctx->indexStats.numTerms, score);
  return score;
}

/* Test scoring function that returns the average document length */
static double TestAvgDocLenScorer(const ScoringFunctionArgs *ctx, const RSIndexResult *r,
                                  const RSDocumentMetadata *dmd, double minScore) {
  RSScoreExplain *scrExp = (RSScoreExplain *)ctx->scrExp;
  double score = ctx->indexStats.avgDocLen;
  EXPLAIN(scrExp, "TEST_AVG_DOC_LEN: avgDocLen(%.2f) = %.2f", ctx->indexStats.avgDocLen, score);
  return score;
}

/* Test scoring function that returns the sum of IDF values from all terms */
static double TestSumIdfScorer(const ScoringFunctionArgs *ctx, const RSIndexResult *r,
                               const RSDocumentMetadata *dmd, double minScore) {
  RSScoreExplain *scrExp = (RSScoreExplain *)ctx->scrExp;
  double score = sumIdfRecursive(r);
  EXPLAIN(scrExp, "TEST_SUM_IDF: sumIdf(%.2f) = %.2f", score, score);
  return score;
}

/* Test scoring function that returns the sum of BM25 IDF values from all terms */
static double TestSumBm25IdfScorer(const ScoringFunctionArgs *ctx, const RSIndexResult *r,
                                   const RSDocumentMetadata *dmd, double minScore) {
  RSScoreExplain *scrExp = (RSScoreExplain *)ctx->scrExp;
  double score = sumBm25IdfRecursive(r);
  EXPLAIN(scrExp, "TEST_SUM_BM25_IDF: sumBm25Idf(%.2f) = %.2f", score, score);
  return score;
}

/* Register the test scorers - to be called from debug command */
int Ext_RegisterTestScorers(void) {
  int result = REDISEARCH_OK;

  if (Ext_RegisterScoringFunction(TEST_NUM_DOCS_SCORER_NAME, TestNumDocsScorer, NULL, NULL) != REDISEARCH_OK) {
    result = REDISEARCH_ERR;
  }
  if (Ext_RegisterScoringFunction(TEST_NUM_TERMS_SCORER_NAME, TestNumTermsScorer, NULL, NULL) != REDISEARCH_OK) {
    result = REDISEARCH_ERR;
  }
  if (Ext_RegisterScoringFunction(TEST_AVG_DOC_LEN_SCORER_NAME, TestAvgDocLenScorer, NULL, NULL) != REDISEARCH_OK) {
    result = REDISEARCH_ERR;
  }
  if (Ext_RegisterScoringFunction(TEST_SUM_IDF_SCORER_NAME, TestSumIdfScorer, NULL, NULL) != REDISEARCH_OK) {
    result = REDISEARCH_ERR;
  }
  if (Ext_RegisterScoringFunction(TEST_SUM_BM25_IDF_SCORER_NAME, TestSumBm25IdfScorer, NULL, NULL) != REDISEARCH_OK) {
    result = REDISEARCH_ERR;
  }

  return result;
}

