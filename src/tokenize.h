
#pragma once

#include "stemmer.h"
#include "stopwords.h"
#include "redisearch.h"
#include "varint.h"

#include "dep/friso/friso.h"

#include <ctype.h>
#include <stdlib.h>
#include <strings.h>

///////////////////////////////////////////////////////////////////////////////////////////////

enum TokenFlags {
  Token_CopyRaw = 0x01,
  Token_CopyStem = 0x02
};

// Represents a token found in a document
struct Token {
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

  Token() : tok(NULL), tokLen(0), flags(0), stem(NULL), phoneticsPrimary(NULL), stemLen(0),
            raw(NULL), rawLen(0) {}

  ~Token() {
    rm_free(phoneticsPrimary);
  }
};

//---------------------------------------------------------------------------------------------

// A NormalizeFunc converts a raw token to the normalized form in which it will be stored
typedef char *(*NormalizeFunc)(char *, size_t *);

#define STEM_TOKEN_FACTOR 0.2

struct Tokenizer {
  char *text;
  size_t len;
  StopWordList *stopwords;
  uint32_t lastOffset;
  uint32_t options;

  virtual ~Tokenizer();

  // read the next token. Return its position or 0 if we can't read anymore
  virtual uint32_t Next(Token *tok);
  virtual void Start(char *txt, size_t len, uint32_t options);
  virtual void Reset(Stemmer *stemmer, StopWordList *stopwords, uint32_t opts);
};

//---------------------------------------------------------------------------------------------

struct SimpleTokenizer : public Tokenizer {
  char **pos;
  Stemmer *stemmer;

  SimpleTokenizer(Stemmer *stemmer, StopWordList *stopwords, uint32_t opts);

  virtual uint32_t Next(Token *tok);
  virtual void Start(char *txt, size_t len, uint32_t options);
  virtual void Reset(Stemmer *stemmer, StopWordList *stopwords, uint32_t opts);
};

//---------------------------------------------------------------------------------------------

#define CNTOKENIZE_BUF_MAX 256

struct ChineseTokenizer : public Tokenizer {
  friso_task_t friso_task;
  char escapebuf[CNTOKENIZE_BUF_MAX];
  size_t nescapebuf;

  ChineseTokenizer(Stemmer *stemmer, StopWordList *stopwords, uint32_t opts);
  virtual ~ChineseTokenizer();

  virtual uint32_t Next(Token *tok);
  virtual void Start(char *txt, size_t len, uint32_t options);
  virtual void Reset(Stemmer *stemmer, StopWordList *stopwords, uint32_t opts);

protected:
  static void maybeFrisoInit();
  int appendToEscbuf(const char *s, size_t n);
  int appendEscapedChars(friso_token_t ftok, int mode);
  void initToken(Token *t, const friso_token_t from);
};

//---------------------------------------------------------------------------------------------

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
Tokenizer *GetTokenizer(RSLanguage language, Stemmer *stemmer, StopWordList *stopwords);
Tokenizer *GetChineseTokenizer(Stemmer *stemmer, StopWordList *stopwords);
Tokenizer *GetSimpleTokenizer(Stemmer *stemmer, StopWordList *stopwords);

///////////////////////////////////////////////////////////////////////////////////////////////
