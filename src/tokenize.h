
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

// A TokenFunc handles tokens in a tokenizer, for example aggregates them
// or builds the query tree.
// t - is the normalized token, which may be used for comparisons, processing, etc.
// tOrig - is the original token in the input text. Used if TOKENIZE_NOMODIFY was
// requested
typedef int (*TokenFunc)(void *ctx, const Token *tokInfo);

// A NormalizeFunc converts a raw token to the normalized form in which it will be stored
typedef char *(*NormalizeFunc)(char *, size_t *);

#define STEM_TOKEN_FACTOR 0.2

typedef struct {
  char **pos;
  TokenFunc tokenFunc;
  void *tokenFuncCtx;
  Stemmer *stemmer;
  StopWordList *stopwords;
  u_int lastOffset;
  uint32_t options;
} TokenizerCtx;

/* The actual tokenizing process runner */
int _tokenize(TokenizerCtx *ctx);

// Don't modify buffer at all during tokenization.
#define TOKENIZE_NOMODIFY 0x01

/** The extenral API. Tokenize text, and create tokens with the given score and fieldId.
TokenFunc is a callback that will be called for each token found
if doStem is 1, we will add stemming extraction for the text
*/
int tokenize(const char *text, void *ctx, TokenFunc f, Stemmer *s, unsigned int offset,
             StopWordList *stopwords, uint32_t options);

#endif