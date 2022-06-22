#include "stemmer.h"
#include "snowball/include/libstemmer.h"
#include "rmalloc.h"

#include <string.h>
#include <stdio.h>
#include <sys/param.h>

///////////////////////////////////////////////////////////////////////////////////////////////

struct langPair_s
{
  const char *str;
  RSLanguage lang;
};

//---------------------------------------------------------------------------------------------

langPair_t __langPairs[] = {
  { "arabic",     RS_LANG_ARABIC },
  { "danish",     RS_LANG_DANISH },
  { "dutch",      RS_LANG_DUTCH },
  { "english",    RS_LANG_ENGLISH },
  { "finnish",    RS_LANG_FINNISH },
  { "french",     RS_LANG_FRENCH },
  { "german",     RS_LANG_GERMAN },
  { "hindi",      RS_LANG_HINDI },
  { "hungarian",  RS_LANG_HUNGARIAN },
  { "italian",    RS_LANG_ITALIAN },
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

//---------------------------------------------------------------------------------------------

const char *RSLanguage_ToString(RSLanguage language) {
  char *ret = NULL;
  switch (language) {
    case  RS_LANG_ARABIC:      ret = "arabic";     break;
    case  RS_LANG_DANISH:      ret = "danish";     break;
    case  RS_LANG_DUTCH:       ret = "dutch";      break;
    case  RS_LANG_ENGLISH:     ret = "english";    break;
    case  RS_LANG_FINNISH:     ret = "finnish";    break;
    case  RS_LANG_FRENCH:      ret = "french";     break;
    case  RS_LANG_GERMAN:      ret = "german";     break;
    case  RS_LANG_HINDI:       ret = "hindi";      break;
    case  RS_LANG_HUNGARIAN:   ret = "hungarian";  break;
    case  RS_LANG_ITALIAN:     ret = "italian";    break;
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

//---------------------------------------------------------------------------------------------

RSLanguage RSLanguage_Find(const char *language) {
  if (language == NULL)
    return DEFAULT_LANGUAGE;

  for (size_t i = 0; __langPairs[i].str != NULL; i++) {
    if (!strcasecmp(language, __langPairs[i].str)) {
      return __langPairs[i].lang;
    }
  }
  return RS_LANG_UNSUPPORTED;
}

//---------------------------------------------------------------------------------------------

struct sbStemmerCtx {
  struct sb_stemmer *sb;
  char *buf;
  size_t cap;
};

//---------------------------------------------------------------------------------------------

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

//---------------------------------------------------------------------------------------------

Stemmer::~Stemmer() {
  struct sbStemmerCtx *ctx_ = ctx;
  sb_stemmer_delete(ctx_->sb);
  rm_free(ctx_->buf);
  rm_free(ctx_);
}

//---------------------------------------------------------------------------------------------

int Stemmer::__sbstemmer_Reset(StemmerType type, RSLanguage language) {
  if (type != type || language == RS_LANG_UNSUPPORTED || language == language) {
    return 0;
  }
  return 1;
}

//---------------------------------------------------------------------------------------------

void Stemmer::__newSnowballStemmer(RSLanguage language) {
  struct sb_stemmer *sb = sb_stemmer_new(RSLanguage_ToString(language), NULL);
  // No stemmer available for this language
  if (!sb) {
    throw Error("No stemmer available for this language");
  }

  struct sbStemmerCtx *ctx = rm_malloc(sizeof(*ctx));
  ctx->sb = sb;
  ctx->cap = 24;
  ctx->buf = rm_malloc(ctx->cap);
  ctx->buf[0] = STEM_PREFIX;

  ctx = ctx;
  Stem = __sbstemmer_Stem;
}

//---------------------------------------------------------------------------------------------

Stemmer::Stemmer(StemmerType type, RSLanguage language) {
  if (type == SnowballStemmer) {
    __newSnowballStemmer(language);
  } else {
    throw Error("Invalid stemmer type");
  }

  language = language;
  type = type;
}

//---------------------------------------------------------------------------------------------

int Stemmer::Reset(StemmerType type, RSLanguage language) {
  return __sbstemmer_Reset && __sbstemmer_Reset(type, language);
}

///////////////////////////////////////////////////////////////////////////////////////////////
