
#ifndef __TOKENIZE_H__
#define __TOKENIZE_H__

#include "stemmer.h"
#include "stopwords.h"
#include "redisearch.h"
#include "varint.h"
#include <ctype.h>
#include <stdlib.h>
#include <strings.h>

/* Represents a token found in a document */
typedef struct {
  // Normalized string
  const char *tok;

  // token string length
  size_t tokLen;

  // Stem. May be NULL
  const char *stem;

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

// A NormalizeFunc converts a raw token to the normalized form in which it will be stored
typedef char *(*NormalizeFunc)(char *, size_t *);

#define STEM_TOKEN_FACTOR 0.2

typedef struct {
  void *privdata;
  char *text;
  char **pos;
  size_t len;
  Stemmer *stemmer;
  StopWordList *stopwords;
  uint32_t lastOffset;
  uint32_t options;
} TokenizerCtx;

void TokenizerCtx_Init(TokenizerCtx *ctx, void *privdata, Stemmer *stemmer, StopWordList *stopwords,
                       uint32_t opts);

typedef struct rsTokenizer {
  TokenizerCtx ctx;
  // read the next token. Return its position or 0 if we can't read anymore
  uint32_t (*Next)(TokenizerCtx *ctx, Token *tok);
  void (*Free)(struct rsTokenizer *self);
  void (*Start)(struct rsTokenizer *self, char *txt, size_t len, uint32_t options);
} RSTokenizer;

RSTokenizer *NewSimpleTokenizer(Stemmer *stemmer, StopWordList *stopwords, uint32_t opts);

#define TOKENIZE_DEFAULT_OPTIONS 0x00
// Don't modify buffer at all during tokenization.
#define TOKENIZE_NOMODIFY 0x01
// don't stem a field
#define TOKENIZE_NOSTEM 0x02

#endif