
#include "redisearch.h"
#include "spec.h"
#include "query.h"
#include "synonym_map.h"
#include "snowball/include/libstemmer.h"
#include "default.h"
#include "tokenize.h"
#include "rmutil/vector.h"
#include "stemmer.h"
#include "phonetic_manager.h"
#include "score_explain.h"

#include <string.h>
#include <stdbool.h>
#include <stdio.h>
#include <sys/param.h>

/////////////////////////////////////////////////////////////////////////////////////////////////////////
// TF-IDF Scoring Functions

// We have 2 TF-IDF scorers - one where TF is normalized by max frequency, the other where it is
// normalized by total weighted number of terms in the document

// normalize TF by max frequency
#define NORM_MAXFREQ 1
// normalize TF by number of tokens (weighted)
#define NORM_DOCLEN 2

#define EXPLAIN(exp, fmt, args...) \
  {                                \
    if (exp) {                     \
      explain(exp, fmt, ##args);   \
    }                              \
  }

static inline void explain(RSScoreExplain *expl, char *fmt, ...) {
  va_list ap;
  va_start(ap, fmt);
  char *p;
  rm_vasprintf((char ** __restrict) &p, fmt, ap);
  va_end(ap);

  expl->str = p;
  rm_free(p);
}

//-------------------------------------------------------------------------------------------------------

RSScoreExplain *ScorerArgs::strExpCreateParent(RSScoreExplain **child) const {
  if (*child) {
    RSScoreExplain *finalScoreExplain = new RSScoreExplain(*child);
    finalScoreExplain->children.push_back(*child);
    scoreExplain = *child = finalScoreExplain;
  }
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////

// recursively calculate tf-idf

double TermResult::TFIDFScorer(const RSDocumentMetadata *dmd, RSScoreExplain *scrExp) const {
  double idf = term ? term->idf : 0;
  double res = weight * ((double)freq) * idf;
  EXPLAIN(scrExp, "(TFIDF %.2f = Weight %.2f * TF %d * IDF %.2f)", res, weight, freq, idf);
  return res;
}

//-------------------------------------------------------------------------------------------------------

double AggregateResult::TFIDFScorer(const RSDocumentMetadata *dmd, RSScoreExplain *scrExp) const {
  double ret = 0;
  if (!scrExp) {
    for (int i = 0; i < numChildren; i++) {
      ret += children[i]->TFIDFScorer(dmd, NULL);
    }
  } else {
    scrExp->numChildren = numChildren;
    scrExp->children = rm_calloc(numChildren, sizeof(RSScoreExplain));
    for (int i = 0; i < numChildren; i++) {
      ret += children[i]->TFIDFScorer(dmd, &scrExp->children[i]);
    }
    EXPLAIN(scrExp, "(Weight %.2f * total children TFIDF %.2f)", weight, ret);
  }
  return weight * ret;
}

//-------------------------------------------------------------------------------------------------------

double IndexResult::TFIDFScorer(const RSDocumentMetadata *dmd, RSScoreExplain *scrExp) const {
  EXPLAIN(scrExp, "(TFIDF %.2f = Weight %.2f * Frequency %d)", weight * (double)freq, weight, freq);
  return weight * (double)freq;
}

//-------------------------------------------------------------------------------------------------------

// internal common tf-idf function, where just the normalization method changes

double IndexResult::TFIDFScorer(const ScorerArgs *args, const RSDocumentMetadata *dmd, double minScore, int normMode) const {
  RSScoreExplain *scrExp = args->scroeExplain;
  if (dmd->score == 0) {
    EXPLAIN(scrExp, "Document score is 0");
    return 0;
  }
  uint32_t norm = normMode == NORM_MAXFREQ ? dmd->maxFreq : dmd->len;
  double rawTfidf = TFIDFScorer(dmd, scrExp);
  double tfidf = dmd->score * rawTfidf / norm;
  strExpCreateParent(ctx, &scrExp);

  // no need to factor the distance if tfidf is already below minimal score
  if (tfidf < minScore) {
    EXPLAIN(scrExp, "TFIDF score of %.2f is smaller than minimum score %.2f", tfidf, minScore);
    return 0;
  }

  int slop = ctx->GetSlop(this);
  tfidf /= slop;

  EXPLAIN(scrExp, "Final TFIDF : words TFIDF %.2f * document score %.2f / norm %d / slop %d",
          rawTfidf, dmd->score, norm, slop);

  return tfidf;
}

//-------------------------------------------------------------------------------------------------------

// Calculate sum(TF-IDF)*document score for each result, where TF is normalized by maximum frequency
// in this document.

double TFIDFScorer(const ScorerArgs *ctx, const IndexResult *h, const RSDocumentMetadata *dmd, double minScore) {
  return h->TFIDFScorer(ctx, dmd, minScore, NORM_MAXFREQ);
}

//-------------------------------------------------------------------------------------------------------

// Identical scorer to TFIDFScorer, only the normalization is by total weighted frequency in the doc

double TFIDFNormDocLenScorer(const ScorerArgs *ctx, const IndexResult *h, const RSDocumentMetadata *dmd, double minScore) {

  return h->TFIDFScorer(ctx, dmd, minScore, NORM_DOCLEN);
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////
// BM25 Scoring Functions
// https://en.wikipedia.org/wiki/Okapi_BM25

// recursively calculate score for each token, summing up sub tokens

double TermResult::bm25Recursive(const ScorerArgs *ctx, const RSDocumentMetadata *dmd, RSScoreExplain *scrExp) const {
  static const float b = 0.5;
  static const float k1 = 1.2;
  double f = (double)freq;
  double idf = (term ? term->idf : 0);

  double ret = idf * f / (f + k1 * (1.0f - b + b * ctx->indexStats.avgDocLen));
  EXPLAIN(scrExp,
          "(%.2f = IDF %.2f * F %d / (F %d + k1 1.2 * (1 - b 0.5 + b 0.5 * Average Len %.2f)))",
          ret, idf, freq, freq, ctx->indexStats.avgDocLen);

  return ret;
}

//-------------------------------------------------------------------------------------------------------

double AggregateResult::bm25Recursive(const ScorerArgs *ctx, const RSDocumentMetadata *dmd, RSScoreExplain *scrExp) const {
  static const float b = 0.5;
  static const float k1 = 1.2;
  double f = (double)freq;
  double ret = 0;

  if (!scrExp) {
    for (int i = 0; i < numChildren; i++) {
      ret += children[i]->bm25Recursive(ctx, dmd, NULL);
    }
  } else {
    scrExp->numChildren = numChildren;
    scrExp->children = rm_calloc(numChildren, sizeof(RSScoreExplain));
    for (int i = 0; i < numChildren; i++) {
      ret += children[i]->bm25Recursive(ctx, dmd, &scrExp->children[i]);
    }
    EXPLAIN(scrExp, "(Weight %.2f * children BM25 %.2f)", weight, ret);
  }
  ret *= weight;
  return ret;
}

//-------------------------------------------------------------------------------------------------------

double IndexResult::bm25Recursive(const ScorerArgs *ctx, const RSDocumentMetadata *dmd, RSScoreExplain *scrExp) const {
  double f = (double)freq;
  double ret = 0;

  if (f) {  // default for virtual type -just disregard the idf
    static const float b = 0.5;
    static const float k1 = 1.2;
    ret = weight * f / (f + k1 * (1.0f - b + b * ctx->indexStats.avgDocLen));
    EXPLAIN(scrExp,
        "(%.2f = Weight %.2f * F %d / (F %d + k1 1.2 * (1 - b 0.5 + b 0.5 * Average Len %.2f)))",
        ret, weight, freq, freq, ctx->indexStats.avgDocLen);
  } else {
    EXPLAIN(scrExp, "Frequency 0 -> value 0");
  }

  return ret;
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////
// BM25 scoring function

double BM25Scorer(const ScorerArgs *args, const IndexResult *r, const RSDocumentMetadata *dmd, double minScore) {
  RSScoreExplain *scrExp = (RSScoreExplain *)args->scrExp;
  double bm25res = r->bm25Recursive(ctx, dmd, scrExp);
  double score = dmd->score * bm25res;
  strExpCreateParent(ctx, &scrExp);

  // no need to factor the distance if tfidf is already below minimal score
  if (score < minScore) {
    EXPLAIN(scrExp, "BM25 score of %.2f is smaller than minimum score %.2f", bm25res, score);
    return 0;
  }
  int slop = ctx->GetSlop(r);
  score /= slop;

  EXPLAIN(scrExp, "Final BM25 : words BM25 %.2f * document score %.2f / slop %d", bm25res,
          dmd->score, slop);

  return score;
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////
// Raw document-score scorer. Just returns the document score

double DocScoreScorer(const ScorerArgs *ctx, const IndexResult *r, const RSDocumentMetadata *dmd, double minScore) {
  RSScoreExplain *scrExp = (RSScoreExplain *)args->scrExp;
  EXPLAIN(scrExp, "Document's score is %.2f", dmd->score);
  return dmd->score;
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////
// DISMAX-style scorer

double IndexResult::dismaxRecursive(const ScorerArgs *ctx, RSScoreExplain *scrExp) const {
  // for terms - we return the term frequency
  double ret = freq;
  EXPLAIN(scrExp, "DISMAX %.2f = Weight %.2f * Frequency %d", weight * ret, weight, freq);

  return weight * ret;
}

//-------------------------------------------------------------------------------------------------------

double IntersectResult::dismaxRecursive(const ScorerArgs *ctx, RSScoreExplain *scrExp) const {
  // for terms - we return the term frequency
  double ret = 0;
  if (!scrExp) {
    for (int i = 0; i < numChildren; i++) {
      ret += children[i]->dismaxRecursive(ctx, NULL);
    }
  } else {
    scrExp->numChildren = numChildren;
    scrExp->children = rm_calloc(numChildren, sizeof(RSScoreExplain));
    for (int i = 0; i < numChildren; i++) {
      ret += children[i]->dismaxRecursive(ctx, &scrExp->children[i]);
    }
    EXPLAIN(scrExp, "%.2f = Weight %.2f * children DISMAX %.2f", weight * ret, weight, ret);
  }

  return weight * ret;
}

//-------------------------------------------------------------------------------------------------------

double UnionResult::dismaxRecursive(const ScorerArgs *ctx, RSScoreExplain *scrExp) const {
  double ret = 0;
  if (!scrExp) {
    for (int i = 0; i < numChildren; i++) {
      ret = MAX(ret, children[i]->dismaxRecursive(ctx, NULL));
    }
  } else {
    scrExp->numChildren = numChildren;
    scrExp->children = rm_calloc(numChildren, sizeof(RSScoreExplain));
    for (int i = 0; i < numChildren; i++) {
      ret = MAX(ret, children[i]->dismaxRecursive(ctx, &scrExp->children[i]));
    }
    EXPLAIN(scrExp, "%.2f = Weight %.2f * children DISMAX %.2f", weight * ret, weight, ret);
  }

  return weight * ret;
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////

// Calculate sum(TF-IDF)*document score for each result

double DisMaxScorer(const ScorerArgs *ctx, const IndexResult *h, const RSDocumentMetadata *dmd, double minScore) {
  return h->dismaxRecursive(ctx, ctx->scrExp);
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////

// taken from redis - bitops.c

static const unsigned char bitsinbyte[256] = {
    0, 1, 1, 2, 1, 2, 2, 3, 1, 2, 2, 3, 2, 3, 3, 4, 1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
    1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
    1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
    2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
    1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
    2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
    2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
    3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7, 4, 5, 5, 6, 5, 6, 6, 7, 5, 6, 6, 7, 6, 7, 7, 8};

//-------------------------------------------------------------------------------------------------------

// HAMMING - Scorer using Hamming distance between the query payload and the document payload.
// Only works if both have the payloads the same length

double HammingDistanceScorer(const ScorerArgs *args, const IndexResult *h, const RSDocumentMetadata *dmd, double minScore) {
  RSScoreExplain *scrExp = (RSScoreExplain *)args->scrExp;
  // the strings must be of the same length > 0
  if (!dmd->payload || !dmd->payload->len || dmd->payload->len != ctx->qdatalen) {
    EXPLAIN(scrExp, "Payloads provided to scorer vary in length");
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
  EXPLAIN(scrExp, "String length is %zu. Bit count is %zu. Result is (1 / count + 1) = %.2f", len,
          ret, result);
  // we inverse the distance, and add 1 to make sure a distance of 0 yields a perfect score of 1
  return result;
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////

void DefaultExpander::expandCn(RSToken *token) {
  if (!data.cn.tokenizer) {
    data.cn.tokenizer = new ChineseTokenizer(NULL, NULL, 0);
    data.cn.tokList.clear();
  }

  ChineseTokenizer *tokenizer = data.cn.tokenizer;
  Vector<char *> tokVec = data.cn.tokList;

  tokenizer->Start(token->str, token->len, 0);

  Token tTok;
  while (tokenizer->Next(&tTok)) {
    char *s = rm_strndup(tTok.tok, tTok.tokLen);
    tokVec.push_back(s);
  }

  ExpandTokenWithPhrase((const char **)tokVec.data(), tokVec.size(), token->flags, true, false);
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////

// Stemmer based query expander

int StemmerExpander::Expand(RSToken *token) {
  // we store the stemmer as private data on the first call to expand
  DefaultExpander *dd = ctx->privdata;
  struct sb_stemmer *sb;

  if (!ctx->privdata) {
    if (ctx->language == RS_LANG_CHINESE) {
      dd->expandCn(token);
      return REDISMODULE_OK;
    } else {
      dd = ctx->privdata = rm_calloc(1, sizeof(*dd));
      dd->isCn = false;
      sb = dd->data.latin = sb_stemmer_new(RSLanguage_ToString(ctx->language), NULL);
    }
  }

  if (dd->isCn) {
    dd->expandCn(token);
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
    dd->ExpandToken(dup, sl + 1, 0x0);  // TODO: Set proper flags here
    if (sl != token->len || strncmp((const char *)stemmed, token->str, token->len)) {
      dd->ExpandToken(rm_strndup((const char *)stemmed, sl), sl, 0x0);
    }
  }
  return REDISMODULE_OK;
}

//---------------------------------------------------------------------------------------------

StemmerExpander::StemmerExpander() {
  if (!p) {
    return;
  }
  DefaultExpander *dd = p;
  if (dd->isCn) {
    delete dd->data.cn.tokenizer;
  } else if (dd->data.latin) {
    sb_stemmer_delete(dd->data.latin);
  }
  rm_free(dd);
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////

// Phonetic-based query expander

PhoneticExpander::Expand(RSToken *token) {
  char *primary = NULL;

  PhoneticManager::ExpandPhonetics(token->str, token->len, &primary, NULL);

  if (primary) {
    ctx->ExpandToken(primary, strlen(primary), 0x0);
  }
  return REDISMODULE_OK;
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////

// Synonyms-based query expander

#define BUFF_LEN 100

int SynonymExpand::Expand(RSToken *token) {
  IndexSpec *spec = ctx->sctx.spec;
  if (!spec->smap) {
    return REDISMODULE_OK;
  }

  TermData *t_data = spec->smap->GetIdsBySynonym(token->str, token->len);
  if (t_data == NULL) {
    return REDISMODULE_OK;
  }

  for (int i = 0; i < array_len(t_data->ids); ++i) {
    char buff[BUFF_LEN];
    int len = SynonymMap::IdToStr(t_data->ids[i], buff, BUFF_LEN);
    ctx->ExpandToken(rm_strdup((const char *)buff), len, 0x0);
  }
  return REDISMODULE_OK;
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////

// Default query expander

//@@ need to change the name - there is a struct with that name

int DefaultExpander::Expand(RSToken *token) {
  int phonetic = ctx.currentNode->opts.phonetic;
  SynonymExpand(&ctx, token);

  if (phonetic == PHONETIC_DEFAULT) {
    // Eliminate the phonetic expansion if we know that none of the fields
    // actually use phonetic matching
    if (ctx.sctx.spec->CheckPhoneticEnabled(ctx.currentNode->opts.fieldMask)) {
      phonetic = PHONETIC_ENABLED;
    }
  } else if (phonetic == PHONETIC_ENABLED || phonetic == PHONETIC_DESABLED) {
    // Verify that the field is actually phonetic
    int isValid = 0;
    if (ctx.currentNode->opts.fieldMask == RS_FIELDMASK_ALL) {
      if (ctx.sctx.spec->flags & Index_HasPhonetic) {
        isValid = 1;
      }
    } else {
      t_fieldMask fm = ctx.currentNode->opts.fieldMask;
      for (size_t ii = 0; ii < ctx.sctx.spec->fields.size(); ++ii) {
        if (!(fm & (t_fieldMask)1 << ii)) {
          continue;
        }
        const FieldSpec fs = ctx.sctx.spec->fields[ii];
        if (fs.IsPhonetics()) {
          isValid = 1;
        }
      }
    }
    if (!isValid) {
      ctx.status->SetError(QUERY_EINVAL, "field does not support phonetics");
      return REDISMODULE_ERR;
    }
  }
  if (phonetic == PHONETIC_ENABLED) {
    PhoneticExpand(&ctx, token);
  }

  // stemmer is happenning last because it might free the given 'RSToken *token'
  // this is a bad solution and should be fixed, but for now its good enough
  // todo: fix the free of the 'RSToken *token' by the stemmer and allow any
  //       expnders ordering!!
  StemmerExpander(&ctx, token);
  return REDISMODULE_OK;
}

//---------------------------------------------------------------------------------------------

void DefaultExpander::~DefaultExpander() {
  StemmerExpanderFree(p);
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////

// Register the default extension

DefaultExtension::DefaultExtension() {
  // TF-IDF scorer is the default scorer
  if (RegisterScorer(DEFAULT_SCORER_NAME, TFIDFScorer) == REDISEARCH_ERR) {
    throw Error("Cannot register " DEFAULT_SCORER_NAME);
  }

  // DisMax-alike scorer
  if (RegisterScorer(DISMAX_SCORER_NAME, DisMaxScorer) == REDISEARCH_ERR) {
    throw Error("Cannot register " DISMAX_SCORER_NAME);
  }

  // Register BM25 scorer
  if (RegisterScorer(BM25_SCORER_NAME, BM25Scorer) == REDISEARCH_ERR) {
    throw Error("Cannot register " BM25_SCORER_NAME);
  }

  // Register HAMMING scorer
  if (RegisterScorer(HAMMINGDISTANCE_SCORER_NAME, HammingDistanceScorer) == REDISEARCH_ERR) {
    throw Error("Cannot register " HAMMINGDISTANCE_SCORER_NAME);
  }

  // Register TFIDF.DOCNORM
  if (RegisterScorer(TFIDF_DOCNORM_SCORER_NAME, TFIDFNormDocLenScorer) == REDISEARCH_ERR) {
    throw Error("Cannot register " TFIDF_DOCNORM_SCORER_NAME);
  }

  // Register DOCSCORE scorer
  if (RegisterScorer(DOCSCORE_SCORER_NAME, DocScoreScorer) == REDISEARCH_ERR) {
    throw Error("Cannot register " DOCSCORE_SCORER_NAME);
  }

  // Snowball Stemmer is the default expander
  if (RegisterQueryExpander(STEMMER_EXPENDER_NAME, StemmerExpander, StemmerExpanderFree, NULL) == REDISEARCH_ERR) {
    throw Error("Cannot register " STEMMER_EXPENDER_NAME);
  }

  // Synonyms expender
  if (RegisterQueryExpander(SYNONYMS_EXPENDER_NAME, SynonymExpand) == REDISEARCH_ERR) {
    throw Error("Cannot register " SYNONYMS_EXPENDER_NAME);
  }

  // Phonetic expender
  if (RegisterQueryExpander(PHONETIC_EXPENDER_NAME, PhoneticExpand) == REDISEARCH_ERR) {
    throw Error("Cannot register " PHONETIC_EXPENDER_NAME);
  }

  // Default expender
  if (RegisterQueryExpander(DEFAULT_EXPANDER_NAME, DefaultExpander, DefaultExpanderFree, NULL) == REDISEARCH_ERR) {
    throw Error("Cannot register " DEFAULT_EXPANDER_NAME);
  }
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////
