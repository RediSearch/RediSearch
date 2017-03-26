#include <string.h>
#include <stdio.h>
#include <sys/param.h>
#include "../redisearch.h"
#include "../dep/snowball/include/libstemmer.h"
#include "default.h"

double _tfidfRecursive(RSIndexResult *r) {

  if (r->type == RSResultType_Term) {
    return r->freq * (r->term.term ? r->term.term->idf : 0);
  }

  double ret = 0;
  for (int i = 0; i < r->agg.numChildren; i++) {
    ret += _tfidfRecursive(r->agg.children[i]);
  }
  return ret;
}
/* Calculate sum(TF-IDF)*document score for each result */
double TFIDFScorer(RSScoringFunctionCtx *ctx, RSIndexResult *h, RSDocumentMetadata *dmd,
                   double minScore) {
  if (dmd->score == 0) return 0;

  double tfidf = _tfidfRecursive(h);
  tfidf *= dmd->score / (double)dmd->maxFreq;

  // no need to factor the distance if tfidf is already below minimal score
  if (tfidf < minScore) {
    return 0;
  }
  tfidf /= (double)ctx->GetSlop(h);
  // printf("tfidf: %f\n", tfidf);
  return tfidf;
}

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

  if (stemmed && strncasecmp(stemmed, token->str, token->len)) {

    int sl = sb_stemmer_length(sb);
    ctx->ExpandToken(ctx, strndup(stemmed, sl), sl, 0x0);  // TODO: Set proper flags here
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

  /* Snowball Stemmer is the default expander */
  if (ctx->RegisterQueryExpander(DEFAULT_EXPANDER_NAME, DefaultStemmerExpand, defaultExpanderFree,
                                 NULL) == REDISEARCH_ERR) {
    return REDISEARCH_ERR;
  }

  return REDISEARCH_OK;
}