#ifndef __RS_STEMMER_H__
#define __RS_STEMMER_H__
#include <stdlib.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef enum LANGUAGE {
  ENGLISH = 0,
  ARABIC,
  CHINESE,
  DANISH,
  DUTCH,
  FINNISH,
  FRENCH,
  GERMAN,
  HUNGARIAN,
  ITALIAN,
  NORWEGIAN,
  PORTUGUESE,
  ROMANIAN,
  RUSSIAN,
  SPANISH,
  SWEDISH,
  TAMIL,
  TURKISH,
  UNSUPPORTED_LANGUAGE 
} language_t;

typedef enum { SnowballStemmer } StemmerType;

#define DEFAULT_LANGUAGE ENGLISH
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
  int (*Reset)(struct stemmer *, StemmerType type, language_t language);

  language_t language;
  StemmerType type;  // Type of stemmer
} Stemmer;

Stemmer *NewStemmer(StemmerType type, language_t language);

int ResetStemmer(Stemmer *stemmer, StemmerType type, language_t language);

/* check if a language is supported by our stemmers */
language_t GetLanguageEnum(const char *language);
const char *GetLanguageStr(language_t language);

/* Get a stemmer expander instance for registering it */
void RegisterStemmerExpander();

/* Snoball Stemmer wrapper implementation */
const char *__sbstemmer_Stem(void *ctx, const char *word, size_t len, size_t *outlen);
void __sbstemmer_Free(Stemmer *s);
Stemmer *__newSnowballStemmer(language_t language);

#ifdef __cplusplus
}
#endif
#endif