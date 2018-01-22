#include "stemmer.h"
#include <string.h>
#include <stdio.h>
#include <sys/param.h>
#include "dep/snowball/include/libstemmer.h"

const char *__supportedLanguages[] = {"arabic",     "danish",   "dutch",     "english", "finnish",
                                      "french",     "german",   "hungarian", "italian", "norwegian",
                                      "portuguese", "romanian", "russian",   "spanish", "swedish",
                                      "tamil",      "turkish",  "chinese",   NULL};

int IsSupportedLanguage(const char *language, size_t len) {
  for (int i = 0; __supportedLanguages[i] != NULL; i++) {
    if (!strncasecmp(language, __supportedLanguages[i],
                     MAX(len, strlen(__supportedLanguages[i])))) {
      return 1;
    }
  }
  return 0;
}

struct sbStemmerCtx {
  struct sb_stemmer *sb;
  char *buf;
  size_t cap;
};

const char *__sbstemmer_Stem(void *ctx, const char *word, size_t len, size_t *outlen) {
  const sb_symbol *b = (const sb_symbol *)word;
  struct sbStemmerCtx *stctx = ctx;
  struct sb_stemmer *sb = stctx->sb;

  const sb_symbol *stemmed = sb_stemmer_stem(sb, b, (int)len);
  if (stemmed) {
    *outlen = sb_stemmer_length(sb);

    // if the stem and its origin are the same - don't do anything
    if (*outlen == len && strncasecmp(word, (const char *)stemmed, len) == 0) {
      return NULL;
    }
    // reserver one character for the '+' prefix
    *outlen += 1;

    // make sure the expansion plus the 1 char prefix fit in our static buffer
    if (*outlen + 2 > stctx->cap) {
      stctx->cap = *outlen + 2;
      stctx->buf = realloc(stctx->buf, stctx->cap);
    }
    // the first location is saved for the + prefix
    memcpy(stctx->buf + 1, stemmed, *outlen + 1);
    return (const char *)stctx->buf;
  }
  return NULL;
}

void __sbstemmer_Free(Stemmer *s) {
  struct sbStemmerCtx *ctx = s->ctx;
  sb_stemmer_delete(ctx->sb);
  free(ctx->buf);
  free(ctx);
  free(s);
}

Stemmer *__newSnowballStemmer(const char *language) {
  struct sb_stemmer *sb = sb_stemmer_new(language, NULL);
  // No stemmer available for this language
  if (!sb) {
    return NULL;
  }

  struct sbStemmerCtx *ctx = malloc(sizeof(*ctx));
  ctx->sb = sb;
  ctx->cap = 24;
  ctx->buf = malloc(ctx->cap);
  ctx->buf[0] = STEM_PREFIX;

  Stemmer *ret = malloc(sizeof(Stemmer));
  ret->ctx = ctx;
  ret->Stem = __sbstemmer_Stem;
  ret->Free = __sbstemmer_Free;
  return ret;
}

Stemmer *NewStemmer(StemmerType type, const char *language) {
  switch (type) {
    case SnowballStemmer:

      return __newSnowballStemmer(language);
  }

  fprintf(stderr, "Invalid stemmer type");
  return NULL;
}
