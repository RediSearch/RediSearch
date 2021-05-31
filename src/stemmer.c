#include "stemmer.h"
#include <string.h>
#include <stdio.h>
#include <sys/param.h>
#include "dep/snowball/include/libstemmer.h"
#include "rmalloc.h"

typedef struct langPair_s
{
  const char *str;
  RSLanguage lang;
} langPair_t;

langPair_t __langPairs[] = {
  { "arabic",     RS_LANG_ARABIC },
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
  { "spanish",    RS_LANG_SPANISH },
  { "swedish",    RS_LANG_SWEDISH },
  { "tamil",      RS_LANG_TAMIL },
  { "turkish",    RS_LANG_TURKISH },
  { "chinese",    RS_LANG_CHINESE },
  { NULL,         RS_LANG_UNSUPPORTED }
};

const char *RSLanguage_ToString(RSLanguage language) {
  char *ret = NULL;
  switch (language) {
    case  RS_LANG_ARABIC:      ret = "arabic";     break;
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
    case  RS_LANG_NEPALI:      ret = "napali";     break; 
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
      if (!strncasecmp(language, __langPairs[i].str, len)) {
        return __langPairs[i].lang;
      }
    }
  }
  return RS_LANG_UNSUPPORTED;
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

static int sbstemmer_Reset(Stemmer *stemmer, StemmerType type, RSLanguage language) {
  if (type != stemmer->type || stemmer->language == RS_LANG_UNSUPPORTED ||
                               stemmer->language != language) {
    return 0;
  }
  return 1;
}

Stemmer *__newSnowballStemmer(RSLanguage language) {
  struct sb_stemmer *sb = sb_stemmer_new(RSLanguage_ToString(language), NULL);
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

Stemmer *NewStemmer(StemmerType type, RSLanguage language) {
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

int ResetStemmer(Stemmer *stemmer, StemmerType type, RSLanguage language) {
  return stemmer->Reset && stemmer->Reset(stemmer, type, language);
}
