#ifndef __RS_STEMMER_H__
#define __RS_STEMMER_H__
#include <stdlib.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef enum { SnowballStemmer } StemmerType;

#define DEFAULT_LANGUAGE "english"
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
  int (*Reset)(struct stemmer *, StemmerType type, const char *language);

  const char *language;
  StemmerType type;  // Type of stemmer
} Stemmer;

Stemmer *NewStemmer(StemmerType type, const char *language);

int ResetStemmer(Stemmer *stemmer, StemmerType type, const char *language);

/* check if a language is supported by our stemmers */
int IsSupportedLanguage(const char *language, size_t len);

/* Get a stemmer expander instance for registering it */
void RegisterStemmerExpander();

/* Snoball Stemmer wrapper implementation */
const char *__sbstemmer_Stem(void *ctx, const char *word, size_t len, size_t *outlen);
void __sbstemmer_Free(Stemmer *s);
Stemmer *__newSnowballStemmer(const char *language);

#ifdef __cplusplus
}
#endif
#endif