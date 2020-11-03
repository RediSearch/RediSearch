#include "forward_index.h"
#include "stopwords.h"
#include "tokenize.h"
#include "toksep.h"
#include "rmalloc.h"
#include "phonetic_manager.h"

#include <ctype.h>
#include <stdlib.h>
#include <strings.h>

///////////////////////////////////////////////////////////////////////////////////////////////

void SimpleTokenizer::Start(char *text_, size_t len_, uint32_t options_) {
  text = text_;
  options = options_;
  len = len_;
  pos = text;
}

//---------------------------------------------------------------------------------------------

// Shortest word which can/should actually be stemmed
#define MIN_STEM_CANDIDATE_LEN 4

// Normalization buffer
#define MAX_NORMALIZE_SIZE 128

//---------------------------------------------------------------------------------------------

/**
 * Normalizes text.
 * - s contains the raw token
 * - dst is the destination buffer which contains the normalized text
 * - len on input contains the length of the raw token. on output contains the
 * on output contains the length of the normalized token
 */

static char *DefaultNormalize(char *s, char *dst, size_t *len) {
  size_t origLen = *len;
  char *realDest = s;
  size_t dstLen = 0;

#define SWITCH_DEST()        \
  if (realDest != dst) {     \
    realDest = dst;          \
    memcpy(realDest, s, ii); \
  }
  // set to 1 if the previous character was a backslash escape
  int escaped = 0;
  for (size_t ii = 0; ii < origLen; ++ii) {
    if (isupper(s[ii])) {
      SWITCH_DEST();
      realDest[dstLen++] = tolower(s[ii]);
    } else if ((isblank(s[ii]) && !escaped) || iscntrl(s[ii])) {
      SWITCH_DEST();
    } else if (s[ii] == '\\' && !escaped) {
      SWITCH_DEST();
      escaped = 1;
      continue;
    } else {
      dst[dstLen++] = s[ii];
    }
    escaped = 0;
  }

  *len = dstLen;
  return dst;
}

//---------------------------------------------------------------------------------------------

// tokenize the text in the context
uint32_t SimpleTokenizer::Next(Token *t) {
  while (*pos != NULL) {
    // get the next token
    size_t origLen;
    char *tok = toksep(pos, &origLen);

    // normalize the token
    size_t normLen = origLen;

    char normalized_s[MAX_NORMALIZE_SIZE];
    char *normBuf;
    if (options & TOKENIZE_NOMODIFY) {
      normBuf = normalized_s;
      if (normLen > MAX_NORMALIZE_SIZE) {
        normLen = MAX_NORMALIZE_SIZE;
      }
    } else {
      normBuf = tok;
    }

    char *normalized = DefaultNormalize(tok, normBuf, &normLen);
    // ignore tokens that turn into nothing
    if (normalized == NULL || normLen == 0) {
      continue;
    }

    // skip stopwords
    if (StopWordList_Contains(stopwords, normalized, normLen)) {
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
    if (!(options & TOKENIZE_NOSTEM) && stemmer && normLen >= MIN_STEM_CANDIDATE_LEN) {
      size_t sl;
      const char *stem = stemmer->Stem(stemmer->ctx, tok, normLen, &sl);
      if (stem) {
        t->stem = stem;
        t->stemLen = sl;
      }
    }

    if (!!(options & TOKENIZE_PHONETICS) && normLen >= RSGlobalConfig.minPhoneticTermLen) {
      // VLA: eww
      if (t->phoneticsPrimary) {
        rm_free(t->phoneticsPrimary);
        t->phoneticsPrimary = NULL;
      }
      PhoneticManager_ExpandPhonetics(NULL, tok, normLen, &t->phoneticsPrimary, NULL);
    }

    return lastOffset;
  }

  return 0;
}

//---------------------------------------------------------------------------------------------

void SimpleTokenizer::Reset(Stemmer *stemmer_, StopWordList *stopwords_, uint32_t opts_) {
  stemmer = stemmer_;
  stopwords = stopwords_;
  options = opts_;
  lastOffset = 0;
  if (stopwords) {
    // Initially this function is called when we receive it from the mempool;
    // in which case stopwords is NULL.
    StopWordList_Ref(stopwords);
  }
}

//---------------------------------------------------------------------------------------------

SimpleTokenizer::SimpleTokenizer(Stemmer *stemmer, StopWordList *stopwords, uint32_t opts) {
  Tokenizer::Reset(stemmer, stopwords, opts);
}

//---------------------------------------------------------------------------------------------

static mempool_t *tokpoolLatin_g = NULL;
static mempool_t *tokpoolCn_g = NULL;

//---------------------------------------------------------------------------------------------

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

//---------------------------------------------------------------------------------------------

RSTokenizer *GetTokenizer(RSLanguage language, Stemmer *stemmer, StopWordList *stopwords) {
  if (language == RS_LANG_CHINESE) {
    return GetChineseTokenizer(stemmer, stopwords);
  } else {
    return GetSimpleTokenizer(stemmer, stopwords);
  }
}

//---------------------------------------------------------------------------------------------

RSTokenizer *GetChineseTokenizer(Stemmer *stemmer, StopWordList *stopwords) {
  if (!tokpoolCn_g) {
    mempool_options opts = {
        .isGlobal = 1, .initialCap = 16, .alloc = newCnTokenizerAlloc, .free = tokenizerFree};
    tokpoolCn_g = mempool_new(&opts);
  }

  RSTokenizer *t = mempool_get(tokpoolCn_g);
  t->Reset(t, stemmer, stopwords, 0);
  return t;
}

//---------------------------------------------------------------------------------------------

RSTokenizer *GetSimpleTokenizer(Stemmer *stemmer, StopWordList *stopwords) {
  if (!tokpoolLatin_g) {
    mempool_options opts = {
        .isGlobal = 1, .initialCap = 16, .alloc = newLatinTokenizerAlloc, .free = tokenizerFree};
    tokpoolLatin_g = mempool_new(&opts);
  }
  RSTokenizer *t = mempool_get(tokpoolLatin_g);
  t->Reset(t, stemmer, stopwords, 0);
  return t;
}

//---------------------------------------------------------------------------------------------

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

///////////////////////////////////////////////////////////////////////////////////////////////
