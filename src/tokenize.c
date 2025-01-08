/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "forward_index.h"
#include "stopwords.h"
#include "tokenize.h"
#include "toksep.h"
#include "rmalloc.h"
#include <ctype.h>
#include <stdlib.h>
#include <strings.h>
#include "phonetic_manager.h"
#include <wctype.h>
#include <wchar.h>
#include <locale.h>
#include <language.h>
#if defined(__APPLE__)
#include <xlocale.h>
#endif

typedef struct {
  RSTokenizer base;
  char *pos;
  Stemmer *stemmer;
} simpleTokenizer;

static void simpleTokenizer_Start(RSTokenizer *base, char *text, size_t len, uint16_t options) {
  simpleTokenizer *self = (simpleTokenizer *)base;
  TokenizerCtx *ctx = &base->ctx;
  ctx->text = text;
  ctx->options = options;
  ctx->len = len;
  ctx->empty_input = len == 0;
  self->pos = ctx->text;
}

// Normalization buffer
#define MAX_NORMALIZE_SIZE 128

/**
 * Normalize a single-byte character string, converting it to lower case and
 * removing control characters.
 * - s contains the raw token
 * - dst is the destination buffer which contains the normalized text
 * - len on input contains the length of the raw token. On output contains the
 *   length of the normalized token
 */
static char *DefaultNormalize_singleByteChars(char *s, char *dst, size_t *len) {
  size_t origLen = *len;  // original length of the token
  char *realDest = s;     // pointer to the current destination buffer
  size_t dstLen = 0;      // length of the destination buffer

#define SWITCH_DEST()        \
  if (realDest != dst) {     \
    realDest = dst;          \
    memcpy(realDest, s, ii); \
  }
  // set to 1 if the previous character was a backslash escape
  int escaped = 0;
  for (size_t ii = 0; ii < origLen; ++ii) {
    if (isupper(s[ii])) {
      // convert the character to lower case
      SWITCH_DEST();
      realDest[dstLen++] = tolower(s[ii]);
    } else if ((isblank(s[ii]) && !escaped) || iscntrl(s[ii])) {
      // skip blank characters and control characters
      SWITCH_DEST();
    } else if (s[ii] == '\\' && !escaped) {
      // handle backslash escapes
      SWITCH_DEST();
      escaped = 1;
      continue;
    } else {
      // copy the character as is
      dst[dstLen++] = s[ii];
    }
    // reset the escape flag
    escaped = 0;
  }

  *len = dstLen;
  return dst;
}


/**
 * Normalize a UTF-8 string, converting it to lower case and removing control
 * characters.
 * - s contains the raw token
 * - dst is the destination buffer which contains the normalized text
 * - len on input contains the length of the raw token. On output contains the
 *   length of the normalized token
 */
static char *DefaultNormalize_utf8(char *s, char *dst, size_t *len,
                                   const char *locale) {
  locale_t old_locale_t = (locale_t)0;
  locale_t new_locale_t = (locale_t)0;

  // Get the current locale settings
  const char *currentLocale = setlocale(LC_ALL, NULL);

  if (strcmp(currentLocale, locale) != 0) {
    new_locale_t = newlocale(LC_ALL_MASK, locale, (locale_t)0);
    if (new_locale_t == (locale_t)0) {
      RedisModule_Log(NULL, "warning", "Unable to set locale - current:%s new:%s error:%s", currentLocale, locale, strerror(errno));
      return NULL;
    }
    // Save the current locale and switch to the new locale
    old_locale_t = uselocale(new_locale_t);
  }

  size_t origLen = *len;   // original length of the token
  char *realDest = s;      // pointer to the current destination buffer
  size_t dstLen = 0;       // length of the destination buffer

#define SWITCH_DEST()        \
  if (realDest != dst) {     \
    realDest = dst;          \
    memcpy(realDest, s, ii); \
  }
  // set to 1 if the previous character was a backslash escape
  int escaped = 0;

  // Initialize the state for multi-byte character conversion
  mbstate_t state;
  memset(&state, 0, sizeof(state));

  wchar_t wc;       // wide character
  size_t len_wc;    // number of bytes consumed by the wide character

  for (size_t ii = 0; ii < origLen;) {
    // Convert the next character to a wide character
    // len_wc is the number of bytes consumed
    len_wc = mbrtowc(&wc, &s[ii], MB_CUR_MAX, &state);
    if (len_wc == (size_t)-1 || len_wc == (size_t)-2) {
      // Handle invalid multi-byte sequence
      ii++;
      continue;
    }

    if (iswupper(wc)) {
      // If the character is upper case, convert it to lower case
      SWITCH_DEST();
      wchar_t lower_wc = towlower(wc);
      wcrtomb(&realDest[dstLen], lower_wc, &state);
      dstLen += len_wc;
    } else if ((iswblank(wc) && !escaped) || iswcntrl(wc)) {
      // Skip blank characters and control characters
      SWITCH_DEST();
    } else if (wc == L'\\' && !escaped) {
      // Handle backslash escapes
      SWITCH_DEST();
      escaped = 1;
      ii += len_wc;
      continue;
    } else {
      // Copy the character as is
      memcpy(&dst[dstLen], &s[ii], len_wc);
      dstLen += len_wc;
    }
    // Move to the next character
    ii += len_wc;
    // Reset the escape flag
    escaped = 0;
  }

  if (old_locale_t != (locale_t)0) {
    // Restore the original thread-specific locale
    uselocale(old_locale_t);
    // Free the new locale object
    freelocale(new_locale_t);
  }

  *len = dstLen;
  return dst;
}

static char *DefaultNormalize(char *s, char *dst, size_t *len, const char *locale) {
  if (RSGlobalConfig.multibyteChars) {
    return DefaultNormalize_utf8(s, dst, len, locale);
  } else {
    return DefaultNormalize_singleByteChars(s, dst, len);
  }
}

// tokenize the text in the context
uint32_t simpleTokenizer_Next(RSTokenizer *base, Token *t) {
  TokenizerCtx *ctx = &base->ctx;
  simpleTokenizer *self = (simpleTokenizer *)base;
  while (self->pos != NULL) {
    // get the next token
    size_t origLen;
    char *tok = toksep(&self->pos, &origLen);
    // normalize the token
    size_t normLen = origLen;
    if (normLen > MAX_NORMALIZE_SIZE) {
      normLen = MAX_NORMALIZE_SIZE;
    }
    char normalized_s[MAX_NORMALIZE_SIZE];
    char *normBuf;
    if (ctx->options & TOKENIZE_NOMODIFY) {
      normBuf = normalized_s;
    } else {
      normBuf = tok;
    }

    const char *locale = RSLanguage_ToLocale(base->ctx.language);
    char *normalized = DefaultNormalize(tok, normBuf, &normLen, locale);

    // ignore tokens that turn into nothing, unless the whole string is empty.
    if ((normalized == NULL || normLen == 0) && !ctx->empty_input) {
      continue;
    }

    // skip stopwords
    if (!ctx->empty_input && StopWordList_Contains(ctx->stopwords, normalized, normLen)) {
      continue;
    }

    *t = (Token){.tok = normalized,
                 .tokLen = normLen,
                 .raw = tok,
                 .rawLen = origLen,
                 .pos = ++ctx->lastOffset,
                 .flags = Token_CopyStem,
                 .phoneticsPrimary = t->phoneticsPrimary};

    // if we support stemming - try to stem the word
    if (!(ctx->options & TOKENIZE_NOSTEM) && self->stemmer &&
          normLen >= RSGlobalConfig.iteratorsConfigParams.minStemLength) {
      size_t sl;
      const char *stem = self->stemmer->Stem(self->stemmer->ctx, tok, normLen, &sl);
      if (stem) {
        t->stem = stem;
        t->stemLen = sl;
      }
    }

    if ((ctx->options & TOKENIZE_PHONETICS) && normLen >= RSGlobalConfig.minPhoneticTermLen) {
      // VLA: eww
      if (t->phoneticsPrimary) {
        rm_free(t->phoneticsPrimary);
        t->phoneticsPrimary = NULL;
      }
      PhoneticManager_ExpandPhonetics(NULL, tok, normLen, &t->phoneticsPrimary, NULL);
    }

    return ctx->lastOffset;
  }

  return 0;
}

void simpleTokenizer_Free(RSTokenizer *self) {
  rm_free(self);
}

static void doReset(RSTokenizer *tokbase, Stemmer *stemmer, StopWordList *stopwords,
                    uint16_t opts) {
  simpleTokenizer *t = (simpleTokenizer *)tokbase;
  t->stemmer = stemmer;
  t->base.ctx.stopwords = stopwords;
  t->base.ctx.options = opts;
  t->base.ctx.lastOffset = 0;
  if (stopwords) {
    // Initially this function is called when we receive it from the mempool;
    // in which case stopwords is NULL.
    StopWordList_Ref(stopwords);
  }
}

RSTokenizer *NewSimpleTokenizer(Stemmer *stemmer, StopWordList *stopwords, uint16_t opts) {
  simpleTokenizer *t = rm_calloc(1, sizeof(*t));
  t->base.Free = simpleTokenizer_Free;
  t->base.Next = simpleTokenizer_Next;
  t->base.Start = simpleTokenizer_Start;
  t->base.Reset = doReset;
  t->base.Reset(&t->base, stemmer, stopwords, opts);
  return &t->base;
}

static mempool_t *tokpoolLatin_g = NULL;
static mempool_t *tokpoolCn_g = NULL;

static void *newLatinTokenizerAlloc() {
  return NewSimpleTokenizer(NULL, NULL, 0);
}
static void *newCnTokenizerAlloc() {
  return NewChineseTokenizer(NULL, NULL, 0);
}
static void tokenizerFree(void *p) {
  RSTokenizer *t = p;
  t->Free(t);
}

RSTokenizer *GetTokenizer(RSLanguage language, Stemmer *stemmer,
                        StopWordList *stopwords) {
  if (language == RS_LANG_CHINESE) {
    return GetChineseTokenizer(stemmer, stopwords);
  } else {
    return GetSimpleTokenizer(language, stemmer, stopwords);
  }
}

RSTokenizer *GetChineseTokenizer(Stemmer *stemmer, StopWordList *stopwords) {
  if (!tokpoolCn_g) {
    mempool_options opts = {
        .initialCap = 16, .alloc = newCnTokenizerAlloc, .free = tokenizerFree};
    mempool_test_set_global(&tokpoolCn_g, &opts);
  }

  RSTokenizer *t = mempool_get(tokpoolCn_g);
  t->ctx.language = RS_LANG_CHINESE;
  t->Reset(t, stemmer, stopwords, 0);
  return t;
}

RSTokenizer *GetSimpleTokenizer(RSLanguage language, Stemmer *stemmer,
                                StopWordList *stopwords) {
  if (!tokpoolLatin_g) {
    mempool_options opts = {
        .initialCap = 16, .alloc = newLatinTokenizerAlloc, .free = tokenizerFree};
    mempool_test_set_global(&tokpoolLatin_g, &opts);
  }
  RSTokenizer *t = mempool_get(tokpoolLatin_g);
  t->ctx.language = language;
  t->Reset(t, stemmer, stopwords, 0);
  return t;
}

void Tokenizer_Release(RSTokenizer *t) {
  // In the future it would be nice to have an actual ID field or w/e, but for
  // now we can just compare callback pointers
  if (t->Next == simpleTokenizer_Next) {
    if (t->ctx.stopwords) {
      StopWordList_Unref(t->ctx.stopwords);
      t->ctx.stopwords = NULL;
    }
    mempool_release(tokpoolLatin_g, t);
  } else {
    mempool_release(tokpoolCn_g, t);
  }
}
