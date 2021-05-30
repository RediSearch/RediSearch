#ifndef __RS_STEMMER_H__
#define __RS_STEMMER_H__
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
  RS_LANG_UNSUPPORTED
} RSLanguage;

typedef enum { SnowballStemmer } StemmerType;

#define DEFAULT_LANGUAGE RS_LANG_ENGLISH
#define STEM_PREFIX '+'
#define STEMMER_EXPANDER_NAME "stem"

/* Abstract "interface" for a pluggable stemmer, ensuring we can use multiple
 * stemmer libs */
typedef struct stemmer {
  void *ctx;
  const char *(*Stem)(void *ctx, const char *word, size_t len, size_t *outlen);
  void (*Free)(struct stemmer *);

  // Attempts to reset the stemmer using the given language and type. Returns 0
  // if this stemmer cannot be reused.
  int (*Reset)(struct stemmer *, StemmerType type, RSLanguage language);

  RSLanguage language;
  StemmerType type;  // Type of stemmer
} Stemmer;

Stemmer *NewStemmer(StemmerType type, RSLanguage language);

int ResetStemmer(Stemmer *stemmer, StemmerType type, RSLanguage language);

/* check if a language is supported by our stemmers */
RSLanguage RSLanguage_Find(const char *language, size_t len);
const char *RSLanguage_ToString(RSLanguage language);

/* Get a stemmer expander instance for registering it */
void RegisterStemmerExpander();

/* Snoball Stemmer wrapper implementation */
const char *__sbstemmer_Stem(void *ctx, const char *word, size_t len, size_t *outlen);
void __sbstemmer_Free(Stemmer *s);
Stemmer *__newSnowballStemmer(RSLanguage language);

#ifdef __cplusplus
}
#endif
#endif
