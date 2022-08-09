
#pragma once

#include "redisearch.h"

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


struct PhoneticExpander : RSQueryExpander {
  int Expand(RSToken *token);
};

struct SynonymExpander : RSQueryExpander {
  SynonymExpander();

  int Expand(RSToken *token);
};

struct StemmerExpander : RSQueryExpander {
  StemmerExpander();

  int Expand(RSToken *token);
};

struct DefaultExpander : RSQueryExpander {
  bool isCn;

  ChineseTokenizer *tokenizer;
  Vector<char *> tokList;
  struct sb_stemmer *latin;

  void expandCn(RSToken *token);
  int Expand(RSToken *token);
};

struct DefaultExtension : RSExtensions {
  DefaultExtension();
};
