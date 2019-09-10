#include <string.h>
#include <stdbool.h>
#include <stdio.h>
#include <sys/param.h>
#include "../redisearch.h"
#include "../spec.h"
#include "../query.h"
#include "../synonym_map.h"
#include "../dep/snowball/include/libstemmer.h"
#include "default.h"
#include "../tokenize.h"
#include "../rmutil/vector.h"
#include "../stemmer.h"
#include "../phonetic_manager.h"

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

#define EXPLAIN(indexResult, fmt, args...)                      \
        if (explain) {                                           \
          asprintf((char ** restrict)&indexResult->scoreExplainStr, fmt, ##args);      \
        } 

// recursively calculate tf-idf
static double tfidfRecursive(const RSIndexResult *r, const RSDocumentMetadata *dmd, bool explain) {
  if (r->type == RSResultType_Term) {
    double idf = r->term.term ? r->term.term->idf : 0;
    double res = r->weight * ((double)r->freq) * idf;
    EXPLAIN(r, "TFIDF %.2f = Weight %.2f * TF %d * IDF %.2f", res, r->weight, r->freq, idf);
    return res;
  }
  if (r->type & (RSResultType_Intersection | RSResultType_Union)) {
    double ret = 0;
    for (int i = 0; i < r->agg.numChildren; i++) {
      ret += tfidfRecursive(r->agg.children[i], dmd, explain);
    }
    EXPLAIN(r, "%.2f = Weight %.2f * children TFIDF %.2f",  r->weight * ret, r->weight, ret)
    return r->weight * ret;
  }
  EXPLAIN(r, "TFIDF %.2f = Weight %.2f * Frequency %d", r->weight * (double)r->freq, r->weight, r->freq);
  return r->weight * (double)r->freq;
}

/* internal common tf-idf function, where just the normalization method changes */
static inline double tfIdfInternal(const ScoringFunctionArgs *ctx, const RSIndexResult *h,
                                   const RSDocumentMetadata *dmd, double minScore, int normMode) {
  bool explain = ctx->scoreflags & SCORE_F_WITH_EXPLANATION;
  if (dmd->score == 0) {
    EXPLAIN(h, "Document score is 0");
    return 0;
  }
  uint32_t norm = normMode == NORM_MAXFREQ ? dmd->maxFreq : dmd->len;
  double rawTfidf = tfidfRecursive(h, dmd, explain);
  double tfidf = dmd->score * rawTfidf / norm;

  // no need to factor the distance if tfidf is already below minimal score
  if (tfidf < minScore) {
    EXPLAIN(h, "TFIDF score of %.2f is smaller than minimum score %.2f", tfidf, minScore);
    return 0;
  }

  int slop = ctx->GetSlop(h);
  tfidf /= slop;
  EXPLAIN(h, "%s. Final TFIDF : %.2f * document score %.2f / norm %d / slop %d",
          h->scoreExplainStr, rawTfidf, dmd->score, norm, slop);
  return tfidf;
}

/* Calculate sum(TF-IDF)*document score for each result, where TF is normalized by maximum frequency
 * in this document*/
static double TFIDFScorer(const ScoringFunctionArgs *ctx, const RSIndexResult *h,
                   const RSDocumentMetadata *dmd, double minScore) {
  return tfIdfInternal(ctx, h, dmd, minScore, NORM_MAXFREQ);
}

/* Identical scorer to TFIDFScorer, only the normalization is by total weighted frequency in the doc
 */
static double TFIDFNormDocLenScorer(const ScoringFunctionArgs *ctx, const RSIndexResult *h,
                                    const RSDocumentMetadata *dmd, double minScore) {

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
static double bm25Recursive(const ScoringFunctionArgs *ctx, const RSIndexResult *r,
                            const RSDocumentMetadata *dmd, bool explain) {
  static const float b = 0.5;
  static const float k1 = 1.2;
  double f = (double)r->freq;
  double ret = 0;
  
  if (r->type == RSResultType_Term) {
    double idf = (r->term.term ? r->term.term->idf : 0);

    ret = idf * f / (f + k1 * (1.0f - b + b * ctx->indexStats.avgDocLen));
    EXPLAIN(r, "%.2f = IDF %.2f * F %d / (F %d + k1 1.2 * (1 - b 0.5 + b 0.5 * Average Len %.2f))", 
            ret, idf, r->freq, r->freq, ctx->indexStats.avgDocLen);  
  } else if (r->type & (RSResultType_Intersection | RSResultType_Union)) {
    for (int i = 0; i < r->agg.numChildren; i++) {
      ret += bm25Recursive(ctx, r->agg.children[i], dmd, explain);
    }
    EXPLAIN(r, "%.2f = Weight %.2f * children BM25 %.2f",  r->weight * ret, r->weight, ret);
    ret *= r->weight;
  } else if (f) {   // default for virtual type -just disregard the idf
    ret = r->weight * f / (f + k1 * (1.0f - b + b * ctx->indexStats.avgDocLen));
    EXPLAIN(r, "%.2f = Weight %.2f * F %d / (F %d + k1 1.2 * (1 - b 0.5 + b 0.5 * Average Len %.2f))", 
            ret, r->weight, r->freq, r->freq, ctx->indexStats.avgDocLen);
  } else {
    EXPLAIN(r, "Frequency 0 -> value 0");
  }

  return ret;
}

/* BM25 scoring function */
static double BM25Scorer(const ScoringFunctionArgs *ctx, const RSIndexResult *r,
                         const RSDocumentMetadata *dmd, double minScore) {
  bool explain = ctx->scoreflags & SCORE_F_WITH_EXPLANATION;
  double bm25res = bm25Recursive(ctx, r, dmd, explain);
  double score = dmd->score * bm25res;

  // no need to factor the distance if tfidf is already below minimal score
  if (score < minScore) {
    EXPLAIN(r, "BM25 score of %.2f is smaller than minimum score %.2f", bm25res, score);
    return 0;
  }
  int slop = ctx->GetSlop(r);
  score /= slop;
  EXPLAIN(r, "%s. Final BM25 : %.2f = document score %.2f / slop %d",
          r->scoreExplainStr, bm25res, dmd->score, slop);
  return score;
}

/******************************************************************************************
 *
 * Raw document-score scorer. Just returns the document score
 *
 ******************************************************************************************/
static double DocScoreScorer(const ScoringFunctionArgs *ctx, const RSIndexResult *r,
                      const RSDocumentMetadata *dmd, double minScore) {
  bool explain = ctx->scoreflags & SCORE_F_WITH_EXPLANATION;
  EXPLAIN(r, "Document's score is %.2f", dmd->score);
  return dmd->score;
}

/******************************************************************************************
 *
 * DISMAX-style scorer
 *
 ******************************************************************************************/
static double _dismaxRecursive(const ScoringFunctionArgs *ctx, const RSIndexResult *r) {
  // for terms - we return the term frequency
  bool explain = ctx->scoreflags & SCORE_F_WITH_EXPLANATION;
  double ret = 0;
  switch (r->type) {
    case RSResultType_Term:
    case RSResultType_Numeric:
    case RSResultType_Virtual:
      ret = r->freq;
      EXPLAIN(r, "DISMAX %.2f = Weight %.2f * Frequency %d", r->weight * ret, r->weight, r->freq);
      break;
    // for intersections - we sum up the term scores
    case RSResultType_Intersection:
      for (int i = 0; i < r->agg.numChildren; i++) {
        ret += _dismaxRecursive(ctx, r->agg.children[i]);
      }
      EXPLAIN(r, "%.2f = Weight %.2f * children DISMAX %.2f",  r->weight * ret, r->weight, ret);
      break;
    // for unions - we take the max frequency
    case RSResultType_Union:
      for (int i = 0; i < r->agg.numChildren; i++) {
        ret = MAX(ret, _dismaxRecursive(ctx, r->agg.children[i]));
      }
      EXPLAIN(r, "%.2f = Weight %.2f * children DISMAX %.2f",  r->weight * ret, r->weight, ret);
      break;
  }
  return r->weight * ret;
}
/* Calculate sum(TF-IDF)*document score for each result */
static double DisMaxScorer(const ScoringFunctionArgs *ctx, const RSIndexResult *h,
                    const RSDocumentMetadata *dmd, double minScore) {
  // printf("score for %d: %f\n", h->docId, dmd->score);
  // if (dmd->score == 0 || h == NULL) return 0;
  return _dismaxRecursive(ctx, h);
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
static double HammingDistanceScorer(const ScoringFunctionArgs *ctx, const RSIndexResult *h,
                                    const RSDocumentMetadata *dmd, double minScore) {
  bool explain = ctx->scoreflags & SCORE_F_WITH_EXPLANATION;
  // the strings must be of the same length > 0
  if (!dmd->payload || !dmd->payload->len || dmd->payload->len != ctx->qdatalen) {
    EXPLAIN(h, "Issues with strings");
    return 0;
  }
  size_t ret = 0;
  size_t len = ctx->qdatalen;
  // if the strings are not aligned to 64 bit - calculate the diff byte by

  const unsigned char *a = (unsigned char *)ctx->qdata;
  const unsigned char *b = (unsigned char *)dmd->payload->data;
  for (size_t i = 0; i < len; i++) {
    ret += bitsinbyte[(unsigned char)(a[i] ^ b[i])];
  }
  double result = 1.0 / (double)(ret + 1);
  EXPLAIN(h, "String length is %zu. Bit count is %zu. Result is (1 / count + 1) = %.2f", len, ret, result);
  // we inverse the distance, and add 1 to make sure a distance of 0 yields a perfect score of 1
  return result;
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
    dd = ctx->privdata = rm_calloc(1, sizeof(*dd));
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
    char *s = rm_strndup(tTok.tok, tTok.tokLen);
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
      rm_free(s);
    }
  }
}

/******************************************************************************************
 *
 * Stemmer based query expander
 *
 ******************************************************************************************/
int StemmerExpander(RSQueryExpanderCtx *ctx, RSToken *token) {

  // we store the stemmer as private data on the first call to expand
  defaultExpanderCtx *dd = ctx->privdata;
  struct sb_stemmer *sb;

  if (!ctx->privdata) {
    if (!strcasecmp(ctx->language, "chinese")) {
      expandCn(ctx, token);
      return REDISMODULE_OK;
    } else {
      dd = ctx->privdata = rm_calloc(1, sizeof(*dd));
      dd->isCn = 0;
      sb = dd->data.latin = sb_stemmer_new(ctx->language, NULL);
    }
  }

  if (dd->isCn) {
    expandCn(ctx, token);
    return REDISMODULE_OK;
  }

  sb = dd->data.latin;

  // No stemmer available for this language - just return the node so we won't
  // be called again
  if (!sb) {
    return REDISMODULE_OK;
  }

  const sb_symbol *b = (const sb_symbol *)token->str;
  const sb_symbol *stemmed = sb_stemmer_stem(sb, b, token->len);

  if (stemmed) {
    int sl = sb_stemmer_length(sb);

    // Make a copy of the stemmed buffer with the + prefix given to stems
    char *dup = rm_malloc(sl + 2);
    dup[0] = STEM_PREFIX;
    memcpy(dup + 1, stemmed, sl + 1);
    ctx->ExpandToken(ctx, dup, sl + 1, 0x0);  // TODO: Set proper flags here
    if (sl != token->len || strncmp((const char *)stemmed, token->str, token->len)) {
      ctx->ExpandToken(ctx, rm_strndup((const char *)stemmed, sl), sl, 0x0);
    }
  }
  return REDISMODULE_OK;
}

void StemmerExpanderFree(void *p) {
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
  rm_free(dd);
}

/******************************************************************************************
 *
 * phonetic based query expander
 *
 ******************************************************************************************/
int PhoneticExpand(RSQueryExpanderCtx *ctx, RSToken *token) {
  char *primary = NULL;

  PhoneticManager_ExpandPhonetics(NULL, token->str, token->len, &primary, NULL);

  if (primary) {
    ctx->ExpandToken(ctx, primary, strlen(primary), 0x0);
  }
  return REDISMODULE_OK;
}

/******************************************************************************************
 *
 * Synonyms based query expander
 *
 ******************************************************************************************/
int SynonymExpand(RSQueryExpanderCtx *ctx, RSToken *token) {
#define BUFF_LEN 100
  IndexSpec *spec = ctx->handle->spec;
  if (!spec->smap) {
    return REDISMODULE_OK;
  }

  TermData *t_data = SynonymMap_GetIdsBySynonym(spec->smap, token->str, token->len);

  if (t_data == NULL) {
    return REDISMODULE_OK;
  }

  for (int i = 0; i < array_len(t_data->ids); ++i) {
    char buff[BUFF_LEN];
    int len = SynonymMap_IdToStr(t_data->ids[i], buff, BUFF_LEN);
    ctx->ExpandToken(ctx, rm_strdup((const char *)buff), len, 0x0);
  }
  return REDISMODULE_OK;
}

/******************************************************************************************
 *
 * Default query expander
 *
 ******************************************************************************************/
int DefaultExpander(RSQueryExpanderCtx *ctx, RSToken *token) {
  int phonetic = (*(ctx->currentNode))->opts.phonetic;
  SynonymExpand(ctx, token);

  if (phonetic == PHONETIC_DEFAULT) {
    // Eliminate the phonetic expansion if we know that none of the fields
    // actually use phonetic matching
    if (IndexSpec_CheckPhoneticEnabled(ctx->handle->spec, (*ctx->currentNode)->opts.fieldMask)) {
      phonetic = PHONETIC_ENABLED;
    }
  } else if (phonetic == PHONETIC_ENABLED || phonetic == PHONETIC_DESABLED) {
    // Verify that the field is actually phonetic
    int isValid = 0;
    if ((*ctx->currentNode)->opts.fieldMask == RS_FIELDMASK_ALL) {
      if (ctx->handle->spec->flags & Index_HasPhonetic) {
        isValid = 1;
      }
    } else {
      t_fieldMask fm = (*ctx->currentNode)->opts.fieldMask;
      for (size_t ii = 0; ii < ctx->handle->spec->numFields; ++ii) {
        if (!(fm & (t_fieldMask)1 << ii)) {
          continue;
        }
        const FieldSpec *fs = ctx->handle->spec->fields + ii;
        if (FieldSpec_IsPhonetics(fs)) {
          isValid = 1;
        }
      }
    }
    if (!isValid) {
      QueryError_SetError(ctx->status, QUERY_EINVAL, "field does not support phonetics");
      return REDISMODULE_ERR;
    }
  }
  if (phonetic == PHONETIC_ENABLED) {
    PhoneticExpand(ctx, token);
  }

  // stemmer is happenning last because it might free the given 'RSToken *token'
  // this is a bad solution and should be fixed, but for now its good enough
  // todo: fix the free of the 'RSToken *token' by the stemmer and allow any
  //       expnders ordering!!
  StemmerExpander(ctx, token);
  return REDISMODULE_OK;
}

void DefaultExpanderFree(void *p) {
  StemmerExpanderFree(p);
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
  if (ctx->RegisterQueryExpander(STEMMER_EXPENDER_NAME, StemmerExpander, StemmerExpanderFree,
                                 NULL) == REDISEARCH_ERR) {
    return REDISEARCH_ERR;
  }

  /* Synonyms expender */
  if (ctx->RegisterQueryExpander(SYNONYMS_EXPENDER_NAME, SynonymExpand, NULL, NULL) ==
      REDISEARCH_ERR) {
    return REDISEARCH_ERR;
  }

  /* Phonetic expender */
  if (ctx->RegisterQueryExpander(PHONETIC_EXPENDER_NAME, PhoneticExpand, NULL, NULL) ==
      REDISEARCH_ERR) {
    return REDISEARCH_ERR;
  }

  /* Default expender */
  if (ctx->RegisterQueryExpander(DEFAULT_EXPANDER_NAME, DefaultExpander, DefaultExpanderFree,
                                 NULL) == REDISEARCH_ERR) {
    return REDISEARCH_ERR;
  }

  return REDISEARCH_OK;
}
