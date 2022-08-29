
#pragma once

#include "extension.h"
#include "redisearch.h"

///////////////////////////////////////////////////////////////////////////////////////////////

#define PHONETIC_EXPENDER_NAME "PHONETIC"
#define SYNONYMS_EXPENDER_NAME "SYNONYM"
#define STEMMER_EXPENDER_NAME "SBSTEM"
#define DEFAULT_EXPANDER_NAME "DEFAULT"

#define DEFAULT_SCORER_NAME "TFIDF"
#define TFIDF_DOCNORM_SCORER_NAME "TFIDF.DOCNORM"
#define DISMAX_SCORER_NAME "DISMAX"
#define BM25_SCORER_NAME "BM25"
#define DOCSCORE_SCORER_NAME "DOCSCORE"
#define HAMMINGDISTANCE_SCORER_NAME "HAMMING"

//---------------------------------------------------------------------------------------------

double TFIDFScorer(const ScorerArgs *args, const IndexResult *result, const RSDocumentMetadata *dmd, double minScore);
double TFIDFNormDocLenScorer(const ScorerArgs *args, const IndexResult *result, const RSDocumentMetadata *dmd, double minScore);
double BM25Scorer(ScorerArgs *args, const IndexResult *r, const RSDocumentMetadata *dmd, double minScore);
double DocScoreScorer(const ScorerArgs *args, const IndexResult *r, const RSDocumentMetadata *dmd, double minScore);
double DisMaxScorer(const ScorerArgs *args, const IndexResult *h, const RSDocumentMetadata *dmd, double minScore);
double HammingDistanceScorer(const ScorerArgs *args, const IndexResult *h, const RSDocumentMetadata *dmd, double minScore);

//---------------------------------------------------------------------------------------------

struct PhoneticExpander : virtual QueryExpander {
  PhoneticExpander(QueryAST *qast, RedisSearchCtx &sctx, RSLanguage lang, QueryError *status) :
    QueryExpander(qast, sctx, lang, status) {}

  bool PhoneticEnabled() const;
  virtual int Expand(RSToken *token);

  static QueryExpander *Factory(QueryAST *qast, RedisSearchCtx &sctx, RSLanguage lang, QueryError *status) {
    return new PhoneticExpander(qast, sctx, lang, status);
  }
};

//---------------------------------------------------------------------------------------------

struct SynonymExpander : virtual QueryExpander {
  SynonymExpander(QueryAST *qast, RedisSearchCtx &sctx, RSLanguage lang, QueryError *status) :
    QueryExpander(qast, sctx, lang, status) {}

  virtual int Expand(RSToken *token);

  static QueryExpander *Factory(QueryAST *qast, RedisSearchCtx &sctx, RSLanguage lang, QueryError *status) {
    return new SynonymExpander(qast, sctx, lang, status);
  }
};

//---------------------------------------------------------------------------------------------

struct StemmerExpander : virtual QueryExpander {
  StemmerExpander(QueryAST *qast, RedisSearchCtx &sctx, RSLanguage lang, QueryError *status);
  ~StemmerExpander();

  ChineseTokenizer *cn_tokenizer;
  Vector<String> tokens;
  struct sb_stemmer *latin_stemmer;

  void expandCn(RSToken *token);
  virtual int Expand(RSToken *token);

  static QueryExpander *Factory(QueryAST *qast, RedisSearchCtx &sctx, RSLanguage lang, QueryError *status) {
    return new StemmerExpander(qast, sctx, lang, status);
  }
};

//---------------------------------------------------------------------------------------------

struct DefaultExpander : virtual QueryExpander, StemmerExpander, SynonymExpander, PhoneticExpander {
  DefaultExpander(QueryAST *qast, RedisSearchCtx &sctx, RSLanguage lang, QueryError *status) :
    QueryExpander(qast, sctx, lang, status),
    StemmerExpander(qast, sctx, lang, status),
    SynonymExpander(qast, sctx, lang, status),
    PhoneticExpander(qast, sctx, lang, status) {}

  ~DefaultExpander();

//  int PhoneticExpand(RSToken *token) { return PhoneticExpander::Expand(token); }
//  int StemmerExpand(RSToken *token) { return StemmerExpander::Expand(token); }
//  int SynonymExpand(RSToken *token) { return SynonymExpander::Expand(token); }

  virtual int Expand(RSToken *token);

  static QueryExpander *Factory(QueryAST *qast, RedisSearchCtx &sctx, RSLanguage lang, QueryError *status) {
    return new DefaultExpander(qast, sctx, lang, status);
  }
};

///////////////////////////////////////////////////////////////////////////////////////////////
