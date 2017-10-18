#include "forward_index.h"
#include "stopwords.h"
#include "tokenize.h"
#include "toksep.h"
#include "rmalloc.h"
#include <ctype.h>
#include <stdlib.h>
#include <strings.h>

int tokenize(const char *text, void *ctx, TokenFunc f, Stemmer *s, unsigned int offset,
             StopWordList *stopwords, uint32_t options) {
  TokenizerCtx tctx;
  tctx.pos = (char **)&text;
  tctx.tokenFunc = f;
  tctx.tokenFuncCtx = ctx;
  tctx.stemmer = s;
  tctx.lastOffset = offset;
  tctx.stopwords = stopwords;
  tctx.options = options;
  return _tokenize(&tctx);
}

// Shortest word which can/should actually be stemmed
#define MIN_STEM_CANDIDATE_LEN 4

// Normalization buffer
#define MAX_NORMALIZE_SIZE 128

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

  for (size_t ii = 0; ii < origLen; ++ii) {
    if (isupper(s[ii])) {
      SWITCH_DEST();
      realDest[dstLen++] = tolower(s[ii]);
    } else if (isblank(s[ii]) || iscntrl(s[ii])) {
      SWITCH_DEST();
    } else {
      dst[dstLen++] = s[ii];
    }
  }

  *len = dstLen;
  return dst;
}

// tokenize the text in the context
int _tokenize(TokenizerCtx *ctx) {
  u_int pos = ctx->lastOffset + 1;

  while (*ctx->pos != NULL) {
    // get the next token
    char *tok = toksep(ctx->pos);
    // this means we're at the end
    if (tok == NULL) break;

    // normalize the token
    size_t origLen = *ctx->pos ? (*ctx->pos - 1) - tok : strlen(tok);
    size_t normLen = origLen;

    char normalized_s[MAX_NORMALIZE_SIZE];
    char *normBuf;
    if (ctx->options & TOKENIZE_NOMODIFY) {
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
    if (StopWordList_Contains(ctx->stopwords, normalized, normLen)) {
      continue;
    }
    // create the token struct
    ++pos;
    Token tokInfo = {.tok = normalized,
                     .tokLen = normLen,
                     .raw = tok,
                     .rawLen = origLen,
                     .pos = pos,
                     .stem = NULL};

    // if we support stemming - try to stem the word
    if (ctx->stemmer && normLen >= MIN_STEM_CANDIDATE_LEN) {
      size_t sl;
      const char *stem = ctx->stemmer->Stem(ctx->stemmer->ctx, tok, normLen, &sl);
      if (stem && strncmp(stem, tok, normLen)) {
        tokInfo.stem = stem;
        tokInfo.stemLen = sl;
      }
    }

    // let it be handled - and break on non zero response
    if (ctx->tokenFunc(ctx->tokenFuncCtx, &tokInfo) != 0) {
      break;
    }
  }

  return pos;
}