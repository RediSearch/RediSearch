#include <string.h>
#include <stdio.h>
#include <sys/param.h>
#include "../redisearch.h"
#include "../dep/snowball/include/libstemmer.h"
#include "default.h"

/******************************************************************************************
 *
 * TF-IDF Scoring Functions
 *
 * We have 2 TF-IDF scorers - one where TF is normalized by max frequency, the other where it is
 * normalized by total weighted number of terms in the document
 *
 ******************************************************************************************/

// normalize TF by max frequency
#define NORM_MAXFREQ 1
// normalize TF by number of tokens (weighted)
#define NORM_DOCLEN 2

// recursively calculate tf-idf
double tfidfRecursive(RSIndexResult *r, RSDocumentMetadata *dmd) {

  if (r->type == RSResultType_Term) {
    return ((double)r->freq) * (r->term.term ? r->term.term->idf : 0);
  }
  if (r->type & (RSResultType_Intersection | RSResultType_Union)) {
    double ret = 0;
    for (int i = 0; i < r->agg.numChildren; i++) {
      ret += tfidfRecursive(r->agg.children[i], dmd);
    }
    return ret;
  }
  return (double)r->freq;
}

/* internal common tf-idf function, where just the normalization method changes */
static inline double tfIdfInternal(RSScoringFunctionCtx *ctx, RSIndexResult *h,
                                   RSDocumentMetadata *dmd, double minScore, int normMode) {
  if (dmd->score == 0) return 0;
  double norm = normMode == NORM_MAXFREQ ? (double)dmd->maxFreq : dmd->len;

  double tfidf = dmd->score * tfidfRecursive(h, dmd) / norm;
  // no need to factor the distance if tfidf is already below minimal score
  if (tfidf < minScore) {
    return 0;
  }

  tfidf /= (double)ctx->GetSlop(h);
  return tfidf;
}

/* Calculate sum(TF-IDF)*document score for each result, where TF is normalized by maximum frequency
 * in this document*/
double TFIDFScorer(RSScoringFunctionCtx *ctx, RSIndexResult *h, RSDocumentMetadata *dmd,
                   double minScore) {
  return tfIdfInternal(ctx, h, dmd, minScore, NORM_MAXFREQ);
}

/* Identical scorer to TFIDFScorer, only the normalization is by total weighted frequency in the doc
 */
double TFIDFNormDocLenScorer(RSScoringFunctionCtx *ctx, RSIndexResult *h, RSDocumentMetadata *dmd,
                             double minScore) {

  return tfIdfInternal(ctx, h, dmd, minScore, NORM_DOCLEN);
}

/******************************************************************************************
 *
 * BM25 Scoring Functions
 *
 * https://en.wikipedia.org/wiki/Okapi_BM25
 *
 ******************************************************************************************/

/* recursively calculate score for each token, summing up sub tokens */
static double bm25Recursive(RSScoringFunctionCtx *ctx, RSIndexResult *r, RSDocumentMetadata *dmd) {
  static const float b = 0.5;
  static const float k1 = 1.2;
  double f = (double)r->freq;

  if (r->type == RSResultType_Term) {
    double idf = (r->term.term ? r->term.term->idf : 0);

    double ret = idf * f / (f + k1 * (1.0f - b + b * ctx->indexStats.avgDocLen));
    return ret;
  }

  if (r->type & (RSResultType_Intersection | RSResultType_Union)) {
    double ret = 0;
    for (int i = 0; i < r->agg.numChildren; i++) {
      ret += bm25Recursive(ctx, r->agg.children[i], dmd);
    }
    return ret;
  }
  // default for virtual type -just disregard the idf
  return r->freq ? f / (f + k1 * (1.0f - b + b * ctx->indexStats.avgDocLen)) : 0;
}

/* BM25 scoring function */
double BM25Scorer(RSScoringFunctionCtx *ctx, RSIndexResult *r, RSDocumentMetadata *dmd,
                  double minScore) {
  double score = dmd->score * bm25Recursive(ctx, r, dmd);

  // no need to factor the distance if tfidf is already below minimal score
  if (score < minScore) {
    return 0;
  }

  score /= (double)ctx->GetSlop(r);
  return score;
}

/******************************************************************************************
 *
 * Raw document-score scorer. Just returns the document score
 *
 ******************************************************************************************/
double DocScoreScorer(RSScoringFunctionCtx *ctx, RSIndexResult *r, RSDocumentMetadata *dmd,
                      double minScore) {
  return dmd->score;
}

/******************************************************************************************
 *
 * DISMAX-style scorer
 *
 ******************************************************************************************/
double _dismaxRecursive(RSIndexResult *r) {
  // for terms - we return the term frequency
  double ret = 0;
  switch (r->type) {
    case RSResultType_Term:
    case RSResultType_Numeric:
    case RSResultType_Virtual:
      ret = r->freq;
      break;
    // for intersections - we sum up the term scores
    case RSResultType_Intersection:
      for (int i = 0; i < r->agg.numChildren; i++) {
        ret += _dismaxRecursive(r->agg.children[i]);
      }
      break;
    // for unions - we take the max frequency
    case RSResultType_Union:
      for (int i = 0; i < r->agg.numChildren; i++) {
        ret = MAX(ret, _dismaxRecursive(r->agg.children[i]));
      }
      break;
  }
  return ret;
}
/* Calculate sum(TF-IDF)*document score for each result */
double DisMaxScorer(RSScoringFunctionCtx *ctx, RSIndexResult *h, RSDocumentMetadata *dmd,
                    double minScore) {
  // printf("score for %d: %f\n", h->docId, dmd->score);
  // if (dmd->score == 0 || h == NULL) return 0;
  return _dismaxRecursive(h);
}

/******************************************************************************************
 *
 * Stemmer based query expander
 *
 ******************************************************************************************/
void DefaultStemmerExpand(RSQueryExpanderCtx *ctx, RSToken *token) {

  // we store the stemmer as private data on the first call to expand
  if (!ctx->privdata) {
    ctx->privdata = sb_stemmer_new(ctx->language, NULL);
  }
  struct sb_stemmer *sb = ctx->privdata;
  // No stemmer available for this language - just return the node so we won't
  // be called again
  if (!sb) {
    return;
  }

  const sb_symbol *b = (const sb_symbol *)token->str;
  const sb_symbol *stemmed = sb_stemmer_stem(sb, b, token->len);

  if (stemmed && strncasecmp((const char *)stemmed, token->str, token->len)) {

    int sl = sb_stemmer_length(sb);
    ctx->ExpandToken(ctx, strndup((const char *)stemmed, sl), sl,
                     0x0);  // TODO: Set proper flags here
  }

  // sb_stemmer_delete(sb);
}

void defaultExpanderFree(void *p) {
  if (p) {

    sb_stemmer_delete(p);
  }
}

/* Register the default extension */
int DefaultExtensionInit(RSExtensionCtx *ctx) {

  /* TF-IDF scorer is the default scorer */
  if (ctx->RegisterScoringFunction(DEFAULT_SCORER_NAME, TFIDFScorer, NULL, NULL) ==
      REDISEARCH_ERR) {
    return REDISEARCH_ERR;
  }

  /* DisMax-alike scorer */
  if (ctx->RegisterScoringFunction(DISMAX_SCORER_NAME, DisMaxScorer, NULL, NULL) ==
      REDISEARCH_ERR) {
    return REDISEARCH_ERR;
  }

  /* Register BM25 scorer */
  if (ctx->RegisterScoringFunction(BM25_SCORER_NAME, BM25Scorer, NULL, NULL) == REDISEARCH_ERR) {
    return REDISEARCH_ERR;
  }

  /* Register TFIDF.DOCNORM */
  if (ctx->RegisterScoringFunction(TFIDF_DOCNORM_SCORER_NAME, TFIDFNormDocLenScorer, NULL, NULL) ==
      REDISEARCH_ERR) {
    return REDISEARCH_ERR;
  }

  /* Register DOCSCORE scorer */
  if (ctx->RegisterScoringFunction(DOCSCORE_SCORER, DocScoreScorer, NULL, NULL) == REDISEARCH_ERR) {
    return REDISEARCH_ERR;
  }

  /* Snowball Stemmer is the default expander */
  if (ctx->RegisterQueryExpander(DEFAULT_EXPANDER_NAME, DefaultStemmerExpand, defaultExpanderFree,
                                 NULL) == REDISEARCH_ERR) {
    return REDISEARCH_ERR;
  }

  return REDISEARCH_OK;
}