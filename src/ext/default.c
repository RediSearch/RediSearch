#include <string.h>
#include <stdio.h>
#include <sys/param.h>
#include "../redisearch.h"
#include "../dep/snowball/include/libstemmer.h"
#include "default.h"
#include "../tokenize.h"
#include "../rmutil/vector.h"
#include "../stemmer.h"

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
/* taken from redis - bitops.c */
static const unsigned char bitsinbyte[256] = {
    0, 1, 1, 2, 1, 2, 2, 3, 1, 2, 2, 3, 2, 3, 3, 4, 1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
    1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
    1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
    2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
    1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
    2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
    2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
    3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7, 4, 5, 5, 6, 5, 6, 6, 7, 5, 6, 6, 7, 6, 7, 7, 8};

/* HAMMING - Scorer using Hamming distance between the query payload and the document payload. Only
 * works if both have the payloads the same length */
double HammingDistanceScorer(RSScoringFunctionCtx *ctx, RSIndexResult *h, RSDocumentMetadata *dmd,
                             double minScore) {
  // the strings must be of the same length > 0
  if (!dmd->payload || !dmd->payload->len || dmd->payload->len != ctx->payload.len) {
    return 0;
  }
  size_t ret = 0;
  size_t len = ctx->payload.len;
  // if the strings are not aligned to 64 bit - calculate the diff byte by

  const unsigned char *a = (unsigned char *)ctx->payload.data;
  const unsigned char *b = (unsigned char *)dmd->payload->data;
  for (size_t i = 0; i < len; i++) {
    ret += bitsinbyte[(unsigned char)(a[i] ^ b[i])];
  }
  // we inverse the distance, and add 1 to make sure a distance of 0 yields a perfect score of 1
  return 1.0 / (double)(ret + 1);
}

typedef struct {
  int isCn;
  union {
    struct {
      RSTokenizer *tokenizer;
      Vector *tokList;
    } cn;
    struct sb_stemmer *latin;
  } data;
} defaultExpanderCtx;

static void expandCn(RSQueryExpanderCtx *ctx, RSToken *token) {
  defaultExpanderCtx *dd = ctx->privdata;
  RSTokenizer *tokenizer;
  if (!dd) {
    dd = ctx->privdata = calloc(1, sizeof(*dd));
    dd->isCn = 1;
  }
  if (!dd->data.cn.tokenizer) {
    tokenizer = dd->data.cn.tokenizer = NewChineseTokenizer(NULL, NULL, 0);
    dd->data.cn.tokList = NewVector(char *, 4);
  }

  tokenizer = dd->data.cn.tokenizer;
  Vector *tokVec = dd->data.cn.tokList;

  tokVec->top = 0;
  tokenizer->Start(tokenizer, token->str, token->len, 0);

  Token tTok;
  while (tokenizer->Next(tokenizer, &tTok)) {
    char *s = strndup(tTok.tok, tTok.tokLen);
    Vector_Push(tokVec, s);
  }

  // Now expand the token with a phrase
  if (tokVec->top > 1) {
    // for (size_t ii = 0; ii < tokVec->top; ++ii) {
    //   const char *s;
    //   Vector_Get(tokVec, ii, &s);
    //   printf("Split => %s\n", s);
    // }
    ctx->ExpandTokenWithPhrase(ctx, (const char **)tokVec->data, tokVec->top, token->flags, 1, 0);
  } else {
    for (size_t ii = 0; ii < tokVec->top; ++ii) {
      // Note, top <= 1; but just for simplicity
      char *s;
      Vector_Get(tokVec, ii, &s);
      free(s);
    }
  }
}

/******************************************************************************************
 *
 * Stemmer based query expander
 *
 ******************************************************************************************/
void DefaultStemmerExpand(RSQueryExpanderCtx *ctx, RSToken *token) {
  // printf("Enter: %.*s\n", (int)token->len, token->str);

  // we store the stemmer as private data on the first call to expand
  defaultExpanderCtx *dd = ctx->privdata;
  struct sb_stemmer *sb;

  if (!ctx->privdata) {
    if (!strcasecmp(ctx->language, "chinese")) {
      expandCn(ctx, token);
      return;
    } else {
      dd = ctx->privdata = calloc(1, sizeof(*dd));
      dd->isCn = 0;
      sb = dd->data.latin = sb_stemmer_new(ctx->language, NULL);
    }
  }

  if (dd->isCn) {
    expandCn(ctx, token);
    return;
  }

  sb = dd->data.latin;

  // No stemmer available for this language - just return the node so we won't
  // be called again
  if (!sb) {
    return;
  }

  const sb_symbol *b = (const sb_symbol *)token->str;
  const sb_symbol *stemmed = sb_stemmer_stem(sb, b, token->len);

  if (stemmed) {
    int sl = sb_stemmer_length(sb);

    // Make a copy of the stemmed buffer with the + prefix given to stems
    char *dup = malloc(sl + 2);
    dup[0] = STEM_PREFIX;
    memcpy(dup + 1, stemmed, sl + 1);
    ctx->ExpandToken(ctx, dup, sl + 1, 0x0);  // TODO: Set proper flags here
    if (sl != token->len || strncmp((const char *)stemmed, token->str, token->len)) {
      ctx->ExpandToken(ctx, strndup((const char *)stemmed, sl), sl, 0x0);
    }
  }
}

void defaultExpanderFree(void *p) {
  if (!p) {
    return;
  }
  defaultExpanderCtx *dd = p;
  if (dd->isCn) {
    dd->data.cn.tokenizer->Free(dd->data.cn.tokenizer);
    Vector_Free(dd->data.cn.tokList);
  } else if (dd->data.latin) {
    sb_stemmer_delete(dd->data.latin);
  }
  free(dd);
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

  /* Register HAMMING scorer */
  if (ctx->RegisterScoringFunction(HAMMINGDISTANCE_SCORER, HammingDistanceScorer, NULL, NULL) ==
      REDISEARCH_ERR) {
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