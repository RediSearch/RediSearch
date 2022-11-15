/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#pragma once
#include <stdlib.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef enum {
  RS_LANG_ENGLISH = 0,
  RS_LANG_ARABIC,
  RS_LANG_BASQUE,
  RS_LANG_CATALAN,
  RS_LANG_CHINESE,
  RS_LANG_DANISH,
  RS_LANG_DUTCH,
  RS_LANG_FINNISH,
  RS_LANG_FRENCH,
  RS_LANG_GERMAN,
  RS_LANG_GREEK,
  RS_LANG_HINDI,
  RS_LANG_HUNGARIAN,
  RS_LANG_ITALIAN,
  RS_LANG_INDONESIAN,
  RS_LANG_IRISH,
  RS_LANG_LITHUANIAN,
  RS_LANG_NEPALI,
  RS_LANG_NORWEGIAN,
  RS_LANG_PORTUGUESE,
  RS_LANG_ROMANIAN,
  RS_LANG_RUSSIAN,
  RS_LANG_SPANISH,
  RS_LANG_SWEDISH,
  RS_LANG_TAMIL,
  RS_LANG_TURKISH,
  RS_LANG_ARMENIAN,
  RS_LANG_SERBIAN,
  RS_LANG_YIDDISH,
  RS_LANG_UNSUPPORTED
} RSLanguage;

#define DEFAULT_LANGUAGE RS_LANG_ENGLISH

/* check if a language is supported by our stemmers */
RSLanguage RSLanguage_Find(const char *language, size_t len);
const char *RSLanguage_ToString(RSLanguage language);

#ifdef __cplusplus
}
#endif
