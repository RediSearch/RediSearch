/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#ifndef __RS_STEMMER_H__
#define __RS_STEMMER_H__
#include "language.h"

#ifdef __cplusplus
extern "C" {
#endif

#include "rmalloc.h"

typedef enum { SnowballStemmer } StemmerType;

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

Stemmer *NewStemmer(StemmerType type, RSLanguage language, alloc_context *actx);

int ResetStemmer(Stemmer *stemmer, StemmerType type, RSLanguage language);

/* Get a stemmer expander instance for registering it */
void RegisterStemmerExpander();

/* Snoball Stemmer wrapper implementation */
const char *__sbstemmer_Stem(void *ctx, const char *word, size_t len, size_t *outlen, alloc_context *actx);
void __sbstemmer_Free(Stemmer *s);
Stemmer *__newSnowballStemmer(RSLanguage language, alloc_context *actx);

#ifdef __cplusplus
}
#endif
#endif
