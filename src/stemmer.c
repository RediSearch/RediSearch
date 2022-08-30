#include "stemmer.h"
#include "snowball/include/libstemmer.h"
#include "rmalloc.h"
#include "query_error.h"

#include <string.h>
#include <stdio.h>
#include <sys/param.h>

///////////////////////////////////////////////////////////////////////////////////////////////

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

const char *RSLanguage_ToString(RSLanguage language) {
  char *ret = NULL;
  switch (language) {
    case  RS_LANG_ARABIC:      ret = "arabic";     break;
    case  RS_LANG_DANISH:      ret = "danish";     break;
    case  RS_LANG_DUTCH:       ret = "dutch";      break;
    case  RS_LANG_ENGLISH:     ret = "english";    break;
    case  RS_LANG_FINNISH:     ret = "finnish";    break;
    case  RS_LANG_FRENCH:      ret = "french";     break;
    case  RS_LANG_GERMAN:      ret = "german";     break;
    case  RS_LANG_HINDI:       ret = "hindi";      break;
    case  RS_LANG_HUNGARIAN:   ret = "hungarian";  break;
    case  RS_LANG_ITALIAN:     ret = "italian";    break;
    case  RS_LANG_NORWEGIAN:   ret = "norwegian";  break;
    case  RS_LANG_PORTUGUESE:  ret = "portuguese"; break;
    case  RS_LANG_ROMANIAN:    ret = "romanian";   break;
    case  RS_LANG_RUSSIAN:     ret = "russian";    break;
    case  RS_LANG_SPANISH:     ret = "spanish";    break;
    case  RS_LANG_SWEDISH:     ret = "swedish";    break;
    case  RS_LANG_TAMIL:       ret = "tamil";      break;
    case  RS_LANG_TURKISH:     ret = "turkish";    break;
    case  RS_LANG_CHINESE:     ret = "chinese";    break;
    case  RS_LANG_UNSUPPORTED:
    default: break;
  }
  return (const char *)ret;
}

//---------------------------------------------------------------------------------------------

RSLanguage RSLanguage_Find(const char *language) {
  if (language == NULL)
    return DEFAULT_LANGUAGE;

  for (size_t i = 0; __langPairs[i].str != NULL; i++) {
    if (!strcasecmp(language, __langPairs[i].str)) {
      return __langPairs[i].lang;
    }
  }
  return RS_LANG_UNSUPPORTED;
}

///////////////////////////////////////////////////////////////////////////////////////////////

std::string_view SnowballStemmer::Stem(std::string_view word) {
  constexpr size_t prefix_len = strlen(STEM_PREFIX);

  const sb_symbol *b = (const sb_symbol *) word.data();
  const sb_symbol *stem = sb_stemmer_stem(sb, b, (int) word.length());
  if (!stem) {
    return std::string_view{};
  }
  size_t outlen = sb_stemmer_length(sb);

  // if the stem and its origin are the same - don't do anything
  if (outlen == word.length() && strncasecmp(word.data(), (const char *)stem, word.length()) == 0) {
    return std::string_view{};
  }
  outlen += prefix_len;

  str = STEM_PREFIX;
  str += (const char *) stem;
  return str;
}

//---------------------------------------------------------------------------------------------

SnowballStemmer::SnowballStemmer(RSLanguage lang) : Stemmer(StemmerType::Snowball, lang) {
  sb = sb_stemmer_new(RSLanguage_ToString(lang), NULL);
  // No stemmer available for this language
  if (!sb) {
    throw Error("No stemmer available for this language");
  }

  str.reserve(24);
  str = STEM_PREFIX;
}

//---------------------------------------------------------------------------------------------

SnowballStemmer::~SnowballStemmer() {
  sb_stemmer_delete(sb);
}

///////////////////////////////////////////////////////////////////////////////////////////////

Stemmer::Stemmer(StemmerType type, RSLanguage language) : type(type), language(language) {
}

//---------------------------------------------------------------------------------------------

// Attempts to reset the stemmer using the given language and type. 
// Returns false if this stemmer cannot be reused.

bool Stemmer::Reset(StemmerType type_, RSLanguage language_) {
  if (type != type_ || language == RS_LANG_UNSUPPORTED || language == language_) {
    return false;
  }
  return true;
}

///////////////////////////////////////////////////////////////////////////////////////////////
