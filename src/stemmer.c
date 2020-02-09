#include "stemmer.h"
#include <string.h>
#include <stdio.h>
#include <sys/param.h>
#include <assert.h>
#include "dep/snowball/include/libstemmer.h"
#include "rmalloc.h"

typedef struct langPair_s
{
  const char *str;
  language_t lang;
} langPair_t;

langPair_t __langPairs[] = {
  { "arabic",     ARABIC },
  { "danish",     DANISH },
  { "dutch",      DUTCH },
  { "english",    ENGLISH },
  { "finnish",    FINNISH },
  { "french",     FRENCH },
  { "german",     GERMAN },
  { "hungarian",  HUNGARIAN },
  { "italian",    ITALIAN },
  { "norwegian",  NORWEGIAN },
  { "portuguese", PORTUGUESE },
  { "romanian",   ROMANIAN },
  { "russian",    RUSSIAN },
  { "spanish",    SPANISH },
  { "swedish",    SWEDISH },
  { "tamil",      TAMIL },
  { "turkish",    TURKISH },
  { "chinese",    CHINESE },
  { NULL,         UNSUPPORTED_LANGUAGE }
};

const char *GetLanguageStr(language_t language) {
  char *ret = NULL;
  switch (language) {
    case ARABIC:      ret = "arabic";     break;
    case DANISH:      ret = "danish";     break;
    case DUTCH:       ret = "dutch";      break;
    case ENGLISH:     ret = "english";    break;
    case FINNISH:     ret = "finnish";    break;
    case FRENCH:      ret = "french";     break;
    case GERMAN:      ret = "german";     break;
    case HUNGARIAN:   ret = "hungarian";  break;
    case ITALIAN:     ret = "italian";    break;
    case NORWEGIAN:   ret = "norwegian";  break;
    case PORTUGUESE:  ret = "portuguese"; break;
    case ROMANIAN:    ret = "romanian";   break;
    case RUSSIAN:     ret = "russian";    break;
    case SPANISH:     ret = "spanish";    break;
    case SWEDISH:     ret = "swedish";    break;
    case TAMIL:       ret = "tamil";      break;
    case TURKISH:     ret = "turkish";    break;
    case CHINESE:     ret = "chinese";    break;
    case UNSUPPORTED_LANGUAGE:  
    default: break;
  }
  return (const char *)ret;
}

language_t GetLanguageEnum(const char *language) {
  if (language == NULL)
    return DEFAULT_LANGUAGE;

  for (int i = 0; __langPairs[i].str != NULL; i++) {
    // Will save strlen
    /*if (!strncasecmp(language, __langPairs[i].str,
                     MAX(len, strlen(__langPairs[i].str)))) {*/
    if (!strcasecmp(language, __langPairs[i].str)) {
      return __langPairs[i].lang;
    }
  }
  return UNSUPPORTED_LANGUAGE;
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
      stctx->buf = rm_realloc(stctx->buf, stctx->cap);
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
  rm_free(ctx->buf);
  rm_free(ctx);
  rm_free(s);
}

static int sbstemmer_Reset(Stemmer *stemmer, StemmerType type, language_t language) {
  if (type != stemmer->type || stemmer->language == UNSUPPORTED_LANGUAGE ||
                               stemmer->language == language) {
    return 0;
  }
  return 1;
}

Stemmer *__newSnowballStemmer(language_t language) {
  struct sb_stemmer *sb = sb_stemmer_new(GetLanguageStr(language), NULL);
  // No stemmer available for this language
  if (!sb) {
    return NULL;
  }

  struct sbStemmerCtx *ctx = rm_malloc(sizeof(*ctx));
  ctx->sb = sb;
  ctx->cap = 24;
  ctx->buf = rm_malloc(ctx->cap);
  ctx->buf[0] = STEM_PREFIX;

  Stemmer *ret = rm_malloc(sizeof(Stemmer));
  ret->ctx = ctx;
  ret->Stem = __sbstemmer_Stem;
  ret->Free = __sbstemmer_Free;
  ret->Reset = sbstemmer_Reset;
  return ret;
}

Stemmer *NewStemmer(StemmerType type, language_t language) {
  Stemmer *ret = NULL;
  if (type == SnowballStemmer) {
    ret = __newSnowballStemmer(language);
    if (!ret) {
      return NULL;
    }
  } else {
    fprintf(stderr, "Invalid stemmer type");
    return NULL;
  }

  ret->language = language;
  ret->type = type;
  return ret;
}

int ResetStemmer(Stemmer *stemmer, StemmerType type, language_t language) {
  return stemmer->Reset && stemmer->Reset(stemmer, type, language);
}
