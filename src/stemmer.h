#ifndef __RS_STEMMER_H__
#define __RS_STEMMER_H__
#include <stdlib.h>

typedef enum { SnowballStemmer } StemmerType;

#define DEFAULT_LANGUAGE "english"

/* Abstract "interface" for a pluggable stemmer, ensuring we can use multiple
 * stemmer libs */
typedef struct stemmer {
  void *ctx;
  const char *(*Stem)(void *ctx, const char *word, size_t len, size_t *outlen);
  void (*Free)(struct stemmer *);
} Stemmer;

Stemmer *NewStemmer(StemmerType type, const char *language);

/* check if a language is supported by our stemmers */
int IsSupportedLanguage(const char *language, size_t len);

#define STEMMER_EXPANDER_NAME "stem"
/* Get a stemmer expander instance for registering it */
void RegisterStemmerExpander();

/* Snoball Stemmer wrapper implementation */
const char *__sbstemmer_Stem(void *ctx, const char *word, size_t len,
                             size_t *outlen);
void __sbstemmer_Free(Stemmer *s);
Stemmer *__newSnowballStemmer(const char *language);

#endif