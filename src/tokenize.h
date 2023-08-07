/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */


#ifndef __TOKENIZE_H__
#define __TOKENIZE_H__

#include "stemmer.h"
#include "stopwords.h"
#include "separators.h"
#include "redisearch.h"
#include "varint.h"
#include <ctype.h>
#include <stdlib.h>
#include <strings.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef enum { Token_CopyRaw = 0x01, Token_CopyStem = 0x02 } TokenFlags;

/* Represents a token found in a document */
typedef struct {
  // Normalized string
  const char *tok;

  // token string length
  uint32_t tokLen;

  // Token needs to be copied. Don't rely on `raw` pointer.
  uint32_t flags;

  // Stem. May be NULL
  const char *stem;

  char *phoneticsPrimary;

  // stem length
  uint32_t stemLen;

  // Raw token as present in the source document.
  // Only relevant if TOKENIZE_NOMODIFY is set.
  const char *raw;

  // Length of raw token
  uint32_t rawLen;

  // position in the document - this is written to the inverted index
  uint32_t pos;
} Token;

#define Token_Destroy(t) rm_free((t)->phoneticsPrimary)

// A NormalizeFunc converts a raw token to the normalized form in which it will be stored
typedef char *(*NormalizeFunc)(char *, size_t *);

#define STEM_TOKEN_FACTOR 0.2

typedef struct {
  char *text;
  size_t len;
  StopWordList *stopwords;
  SeparatorList *separators;
  uint32_t lastOffset;
  uint32_t options;
} TokenizerCtx;

typedef struct RSTokenizer {
  TokenizerCtx ctx;
  // read the next token. Return its position or 0 if we can't read anymore
  uint32_t (*Next)(struct RSTokenizer *self, Token *tok);
  void (*Free)(struct RSTokenizer *self);
  void (*Start)(struct RSTokenizer *self, char *txt, size_t len, uint32_t options);
  void (*Reset)(struct RSTokenizer *self, Stemmer *stemmer,
      StopWordList *stopwords, uint32_t opts, SeparatorList *separators);
} RSTokenizer;

RSTokenizer *NewSimpleTokenizer(Stemmer *stemmer, StopWordList *stopwords,
    uint32_t opts, SeparatorList *separators);
RSTokenizer *NewChineseTokenizer(Stemmer *stemmer, StopWordList *stopwords,
    uint32_t opts, SeparatorList *separators);

#define TOKENIZE_DEFAULT_OPTIONS 0x00
// Don't modify buffer at all during tokenization.
#define TOKENIZE_NOMODIFY 0x01
// don't stem a field
#define TOKENIZE_NOSTEM 0x02
// perform phonetic matching
#define TOKENIZE_PHONETICS 0x04

/**
 * Pooled tokenizer functions:
 * These functions retrieve tokenizers using pools.
 *
 * These should all be called when the GIL is held.
 */

/**
 * Retrieves a tokenizer based on the language string. When this tokenizer
 * is no longer needed, return to the pool using Tokenizer_Release()
 */
RSTokenizer *GetTokenizer(RSLanguage language, Stemmer *stemmer,
    StopWordList *stopwords, SeparatorList *separators);
RSTokenizer *GetChineseTokenizer(Stemmer *stemmer, StopWordList *stopwords,
    SeparatorList *separators);
RSTokenizer *GetSimpleTokenizer(Stemmer *stemmer, StopWordList *stopwords,
    SeparatorList *separators);
void Tokenizer_Release(RSTokenizer *t);

#ifdef __cplusplus
}
#endif
#endif
