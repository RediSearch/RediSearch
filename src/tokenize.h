
#ifndef __TOKENIZE_H__
#define __TOKENIZE_H__

#include "stemmer.h"
#include "redisearch.h"
#include "util/khash.h"
#include "varint.h"
#include <ctype.h>
#include <stdlib.h>
#include <strings.h>

typedef enum {
  DT_WORD,
  DT_STEM,
} DocTokenType;
/* Represents a token found in a document */
typedef struct {
  // token string
  const char *s;
  // token string length
  size_t len;

  // position in the document - this is written to the inverted index
  u_int pos;

  // the token's score
  float score;

  // Field id - used later for filtering.
  t_fieldMask fieldId;

  int stringFreeable;

  DocTokenType type;
} Token;

// A TokenFunc handles tokens in a tokenizer, for example aggregates them, or builds the query tree
typedef int (*TokenFunc)(void *ctx, Token t);

// A NormalizeFunc converts a raw token to the normalized form in which it will be stored
typedef char *(*NormalizeFunc)(char *, size_t *);

//! " # $ % & ' ( ) * + , - . / : ; < = > ? @ [ \ ] ^ _ ` { | } ~
#define DEFAULT_SEPARATORS " \t,./(){}[]:;/\\~!@#$%^&*-=+|'`\"<>?";

#define STEM_TOKEN_FACTOR 0.2

// TODO: Optimize this with trie or something...
int isStopword(const char *w, size_t len, const char **stopwords);

typedef struct {
  const char *text;
  char **pos;
  const char *separators;
  double fieldScore;
  int fieldId;
  TokenFunc tokenFunc;
  void *tokenFuncCtx;
  NormalizeFunc normalize;
  Stemmer *stemmer;
  u_int lastOffset;
} TokenizerCtx;

/* The actual tokenizing process runner */
int _tokenize(TokenizerCtx *ctx);

/** The extenral API. Tokenize text, and create tokens with the given score and fieldId.
TokenFunc is a callback that will be called for each token found
if doStem is 1, we will add stemming extraction for the text
*/
int tokenize(const char *text, float fieldScore, t_fieldMask fieldId, void *ctx, TokenFunc f,
             Stemmer *s, u_int offset);

/** A simple text normalizer that convertes all tokens to lowercase and removes accents.
Does NOT normalize unicode */
char *DefaultNormalize(char *s, size_t *len);

#endif