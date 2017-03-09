#include <string.h>
#include <stdio.h>
#include <sys/param.h>
#include "../redisearch.h"
#include "../dep/snowball/include/libstemmer.h"
#include "default.h"

/* Calculate sum(TF-IDF)*document score for each result */
double TFIDFScorer(RSScoringFunctionCtx *ctx, RSIndexResult *h, RSDocumentMetadata *dmd,
                   double minScore) {
  if (dmd->score == 0) return 0;

  double tfidf = 0;
  for (int i = 0; i < h->numRecords; i++) {
    tfidf += (float)h->records[i].freq * (h->records[i].term ? h->records[i].term->idf : 0);
  }
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
  printf("expanding %s\n", token->str);

  struct sb_stemmer *sb = sb_stemmer_new(token->language, NULL);
  // No stemmer available for this language - just return the node so we won't
  // be called again
  if (!sb) {
    return;
  }

  const sb_symbol *b = (const sb_symbol *)token->str;
  const sb_symbol *stemmed = sb_stemmer_stem(sb, b, token->len);

  if (stemmed && strncasecmp(stemmed, token->str, token->len)) {

    int sl = sb_stemmer_length(sb);
    printf("Extended token %s with stem %.*s\n", token->str, sl, stemmed);
    ctx->ExpandToken(ctx, strndup(stemmed, sl), sl, 0x0);  // TODO: Set proper flags here
  }

  sb_stemmer_delete(sb);
}

/* Register the default extension */
int DefaultExtensionInit(RSExtensionCtx *ctx) {

  /* TF-IDF scorer is the default scorer */
  if (ctx->RegisterScoringFunction(DEFAULT_SCORER_NAME, TFIDFScorer, NULL) == REDISEARCH_ERR) {
    return REDISEARCH_ERR;
  }

  /* Snowball Stemmer is the default expander */
  if (ctx->RegisterQueryExpander(DEFAULT_EXPANDER_NAME, DefaultStemmerExpand, NULL) ==
      REDISEARCH_ERR) {
    return REDISEARCH_ERR;
  }

  return REDISEARCH_OK;
}