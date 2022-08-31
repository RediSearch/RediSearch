
#pragma once

#include "object.h"
#include <stdlib.h>

///////////////////////////////////////////////////////////////////////////////////////////////

enum RSLanguage {
  RS_LANG_ENGLISH = 0,
  RS_LANG_ARABIC,
  RS_LANG_CHINESE,
  RS_LANG_DANISH,
  RS_LANG_DUTCH,
  RS_LANG_FINNISH,
  RS_LANG_FRENCH,
  RS_LANG_GERMAN,
  RS_LANG_HINDI,
  RS_LANG_HUNGARIAN,
  RS_LANG_ITALIAN,
  RS_LANG_NORWEGIAN,
  RS_LANG_PORTUGUESE,
  RS_LANG_ROMANIAN,
  RS_LANG_RUSSIAN,
  RS_LANG_SPANISH,
  RS_LANG_SWEDISH,
  RS_LANG_TAMIL,
  RS_LANG_TURKISH,
  RS_LANG_UNSUPPORTED
};

//---------------------------------------------------------------------------------------------

enum class StemmerType {
  Snowball
};

#define DEFAULT_LANGUAGE RS_LANG_ENGLISH
#define STEM_PREFIX "+"
#define STEMMER_EXPANDER_NAME "stem"

//---------------------------------------------------------------------------------------------

struct Stemmer : public Object {
  Stemmer(StemmerType type, RSLanguage language);

  virtual std::string_view Stem(std::string_view word);
  virtual bool Reset(StemmerType type, RSLanguage language);

  RSLanguage language;
  StemmerType type;
};

//---------------------------------------------------------------------------------------------

// check if a language is supported by our stemmers
RSLanguage RSLanguage_Find(const char *language);
const char *RSLanguage_ToString(RSLanguage language);

// Get a stemmer expander instance for registering it
void RegisterStemmerExpander();

//---------------------------------------------------------------------------------------------

struct SnowballStemmer : Stemmer {
  SnowballStemmer(RSLanguage language);
  ~SnowballStemmer();

  struct sb_stemmer *sb;
  String str;

  std::string_view Stem(std::string_view word);
};

//---------------------------------------------------------------------------------------------

struct langPair_s
{
  const char *str;
  RSLanguage lang;
};

extern langPair_s __langPairs[];

///////////////////////////////////////////////////////////////////////////////////////////////
