/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "language.h"
#include "rmutil/alloc.h"
#include "util/strconv.h"
#include <string.h>

typedef struct langPair_s
{
  const char *str;
  RSLanguage lang;
} langPair_t;

langPair_t __langPairs[] = {
  { "arabic",     RS_LANG_ARABIC },
  { "armenian",   RS_LANG_ARMENIAN },
  { "basque",     RS_LANG_BASQUE },
  { "catalan",    RS_LANG_CATALAN },
  { "danish",     RS_LANG_DANISH },
  { "dutch",      RS_LANG_DUTCH },
  { "english",    RS_LANG_ENGLISH },
  { "finnish",    RS_LANG_FINNISH },
  { "french",     RS_LANG_FRENCH },
  { "german",     RS_LANG_GERMAN },
  { "greek",      RS_LANG_GREEK },
  { "hindi",      RS_LANG_HINDI },
  { "hungarian",  RS_LANG_HUNGARIAN },
  { "indonesian", RS_LANG_INDONESIAN },
  { "irish",      RS_LANG_IRISH },
  { "italian",    RS_LANG_ITALIAN },
  { "lithuanian", RS_LANG_LITHUANIAN },
  { "nepali",     RS_LANG_NEPALI },
  { "norwegian",  RS_LANG_NORWEGIAN },
  { "portuguese", RS_LANG_PORTUGUESE },
  { "romanian",   RS_LANG_ROMANIAN },
  { "russian",    RS_LANG_RUSSIAN },
  { "serbian",    RS_LANG_SERBIAN },
  { "spanish",    RS_LANG_SPANISH },
  { "swedish",    RS_LANG_SWEDISH },
  { "tamil",      RS_LANG_TAMIL },
  { "turkish",    RS_LANG_TURKISH },
  { "yiddish",    RS_LANG_YIDDISH },
  { "chinese",    RS_LANG_CHINESE },
  { NULL,         RS_LANG_UNSUPPORTED }
};

const char *RSLanguage_ToString(RSLanguage language) {
  char *ret = NULL;
  switch (language) {
    case  RS_LANG_ARABIC:      ret = "arabic";     break;
    case  RS_LANG_ARMENIAN:    ret = "armenian";   break;
    case  RS_LANG_BASQUE:      ret = "basque";     break;
    case  RS_LANG_CATALAN:     ret = "catalan";    break;
    case  RS_LANG_DANISH:      ret = "danish";     break;
    case  RS_LANG_DUTCH:       ret = "dutch";      break;
    case  RS_LANG_ENGLISH:     ret = "english";    break;
    case  RS_LANG_FINNISH:     ret = "finnish";    break;
    case  RS_LANG_FRENCH:      ret = "french";     break;
    case  RS_LANG_GERMAN:      ret = "german";     break;
    case  RS_LANG_GREEK:       ret = "greek";      break;
    case  RS_LANG_HINDI:       ret = "hindi";      break;
    case  RS_LANG_HUNGARIAN:   ret = "hungarian";  break;
    case  RS_LANG_INDONESIAN:  ret = "indonesian"; break;
    case  RS_LANG_IRISH:       ret = "irish";      break;
    case  RS_LANG_ITALIAN:     ret = "italian";    break;
    case  RS_LANG_LITHUANIAN:  ret = "lithuanian"; break;
    case  RS_LANG_NEPALI:      ret = "nepali";     break;
    case  RS_LANG_NORWEGIAN:   ret = "norwegian";  break;
    case  RS_LANG_PORTUGUESE:  ret = "portuguese"; break;
    case  RS_LANG_ROMANIAN:    ret = "romanian";   break;
    case  RS_LANG_RUSSIAN:     ret = "russian";    break;
    case  RS_LANG_SERBIAN:     ret = "serbian";    break;
    case  RS_LANG_SPANISH:     ret = "spanish";    break;
    case  RS_LANG_SWEDISH:     ret = "swedish";    break;
    case  RS_LANG_TAMIL:       ret = "tamil";      break;
    case  RS_LANG_TURKISH:     ret = "turkish";    break;
    case  RS_LANG_YIDDISH:     ret = "yiddish";    break;
    case  RS_LANG_CHINESE:     ret = "chinese";    break;
    case  RS_LANG_UNSUPPORTED:
    default: break;
  }
  return (const char *)ret;
}

RSLanguage RSLanguage_Find(const char *language, size_t len) {
  if (language == NULL)
    return DEFAULT_LANGUAGE;

  if (!len) {
    for (size_t i = 0; __langPairs[i].str != NULL; i++) {
      if (!strcasecmp(language, __langPairs[i].str)) {
        return __langPairs[i].lang;
      }
    }
  } else {
    for (size_t i = 0; __langPairs[i].str != NULL; i++) {
      if (STR_EQCASE(language, len, __langPairs[i].str)) {
        return __langPairs[i].lang;
      }
    }
  }
  return RS_LANG_UNSUPPORTED;
}

const char *RSLanguage_ToLocale(RSLanguage language) {
char *ret = NULL;
  switch (language) {
    case  RS_LANG_ARABIC:      ret = "ar_AE.UTF-8";   break;
    case  RS_LANG_ARMENIAN:    ret = "hy_AM";         break;
    case  RS_LANG_BASQUE:      ret = "eu_ES.UTF-8";   break;
    case  RS_LANG_CATALAN:     ret = "ca_ES.UTF-8";   break;
    case  RS_LANG_DANISH:      ret = "da_DK.UTF-8";   break;
    case  RS_LANG_DUTCH:       ret = "nl_NL.UTF-8";   break;
    case  RS_LANG_ENGLISH:     ret = "en_US.UTF-8";   break;
    case  RS_LANG_FINNISH:     ret = "fi_FI.UTF-8";   break;
    case  RS_LANG_FRENCH:      ret = "fr_FR.UTF-8";   break;
    case  RS_LANG_GERMAN:      ret = "de_DE.UTF-8";   break;
    case  RS_LANG_GREEK:       ret = "el_GR.UTF-8";   break;
    case  RS_LANG_HINDI:       ret = "hi_IN";         break;
    case  RS_LANG_HUNGARIAN:   ret = "hu_HU.UTF-8";   break;
    case  RS_LANG_INDONESIAN:  ret = "id_ID.UTF-8";   break;
    case  RS_LANG_IRISH:       ret = "ga_IE.UTF-8";   break;
    case  RS_LANG_ITALIAN:     ret = "it_IT.UTF-8";   break;
    case  RS_LANG_LITHUANIAN:  ret = "lt_LT.UTF-8";   break;
    case  RS_LANG_NEPALI:      ret = "ne_NP";         break;
    case  RS_LANG_NORWEGIAN:   ret = "en_US.UTF-8";   break;
    case  RS_LANG_PORTUGUESE:  ret = "pt_PT.UTF-8";   break;
    case  RS_LANG_ROMANIAN:    ret = "ro_RO.UTF-8";   break;
    case  RS_LANG_RUSSIAN:     ret = "ru_RU.UTF-8";   break;
    case  RS_LANG_SERBIAN:     ret = "sr_RS";         break;
    case  RS_LANG_SPANISH:     ret = "es_ES.UTF-8";   break;
    case  RS_LANG_SWEDISH:     ret = "sv_SE.UTF-8";   break;
    case  RS_LANG_TAMIL:       ret = "ta_IN";         break;
    case  RS_LANG_TURKISH:     ret = "tr_TR.UTF-8";   break;
    case  RS_LANG_YIDDISH:     ret = "en_US.UTF-8";   break;
    case  RS_LANG_CHINESE:     ret = "";   break;
    case  RS_LANG_UNSUPPORTED:
    default:
      ret = "";   break;
      break;
  }
  return (const char *)ret;
}
