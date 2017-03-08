#include "../redisearch.h"
#include <stdio.h>

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

int DefaultExtensionInit(RSExtensionCtx *ctx) {
  return ctx->RegisterScoringFunction("TFIDF", TFIDFScorer, NULL);
}