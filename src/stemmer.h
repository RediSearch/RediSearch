
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
  //Stemmer();
  Stemmer(StemmerType type, RSLanguage language);
  virtual ~Stemmer();

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

langPair_s __langPairs[] = {
  { "arabic",     RS_LANG_ARABIC },
  { "danish",     RS_LANG_DANISH },
  { "dutch",      RS_LANG_DUTCH },
  { "english",    RS_LANG_ENGLISH },
  { "finnish",    RS_LANG_FINNISH },
  { "french",     RS_LANG_FRENCH },
  { "german",     RS_LANG_GERMAN },
  { "hindi",      RS_LANG_HINDI },
  { "hungarian",  RS_LANG_HUNGARIAN },
  { "italian",    RS_LANG_ITALIAN },
  { "norwegian",  RS_LANG_NORWEGIAN },
  { "portuguese", RS_LANG_PORTUGUESE },
  { "romanian",   RS_LANG_ROMANIAN },
  { "russian",    RS_LANG_RUSSIAN },
  { "spanish",    RS_LANG_SPANISH },
  { "swedish",    RS_LANG_SWEDISH },
  { "tamil",      RS_LANG_TAMIL },
  { "turkish",    RS_LANG_TURKISH },
  { "chinese",    RS_LANG_CHINESE },
  { NULL,         RS_LANG_UNSUPPORTED }
};

///////////////////////////////////////////////////////////////////////////////////////////////
