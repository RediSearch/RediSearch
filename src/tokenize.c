#include "forward_index.h"
#include "stopwords.h"
#include "tokenize.h"
#include "rmalloc.h"
#include <ctype.h>
#include <stdlib.h>
#include <strings.h>

int tokenize(const char *text, float score, t_fieldMask fieldId, void *ctx, TokenFunc f, Stemmer *s,
             unsigned int offset, StopWordList *stopwords) {
  TokenizerCtx tctx;
  tctx.text = text;
  tctx.pos = (char **)&text;
  tctx.separators = DEFAULT_SEPARATORS;
  tctx.fieldScore = score;
  tctx.tokenFunc = f;
  tctx.tokenFuncCtx = ctx;
  tctx.normalize = DefaultNormalize;
  tctx.fieldId = fieldId;
  tctx.stemmer = s;
  tctx.lastOffset = offset;
  tctx.stopwords = stopwords;

  return _tokenize(&tctx);
}

static const char sepMap[256] = {
        [' '] = 1, ['\t'] = 1, [','] = 1, ['.'] = 1, ['/'] = 1, ['('] = 1, [')'] = 1,
        ['{'] = 1, ['}'] = 1,  ['['] = 1, [']'] = 1, [':'] = 1, [';'] = 1, ['\\'] = 1,
        ['~'] = 1, ['!'] = 1,  ['@'] = 1, ['#'] = 1, ['$'] = 1, ['%'] = 1, ['^'] = 1,
        ['&'] = 1, ['*'] = 1,  ['-'] = 1, ['='] = 1, ['+'] = 1, ['|'] = 1, ['\''] = 1,
        ['`'] = 1, ['"'] = 1,  ['<'] = 1, ['>'] = 1, ['?'] = 1,
};

static inline char *mySep(char **s) {
  uint8_t *pos = (uint8_t *)*s;
  char *orig = *s;
  for (; *pos; ++pos) {
    if (sepMap[*pos]) {
      *pos = '\0';
      *s = (char *)++pos;
      break;
    }
  }
  if (!*pos) {
    *s = NULL;
  }
  return orig;
}

// Shortest word which can/should actually be stemmed
#define MIN_STEM_CANDIDATE_LEN 4

// tokenize the text in the context
int _tokenize(TokenizerCtx *ctx) {
  u_int pos = ctx->lastOffset + 1;

  while (*ctx->pos != NULL) {
    // get the next token
    char *tok = mySep(ctx->pos);
    // this means we're at the end
    if (tok == NULL) break;

    // normalize the token
    size_t tlen;
    tok = ctx->normalize(tok, &tlen);

    // ignore tokens that turn into nothing
    if (tok == NULL || tlen == 0) {
      continue;
    }

    // skip stopwords
    if (StopWordList_Contains(ctx->stopwords, tok, tlen)) {
      continue;
    }
    // create the token struct
    Token t = {tok, tlen, ++pos, ctx->fieldScore, ctx->fieldId, DT_WORD, 0};

    // let it be handled - and break on non zero response
    if (ctx->tokenFunc(ctx->tokenFuncCtx, &t) != 0) {
      break;
    }

    // if we support stemming - try to stem the word
    if (ctx->stemmer && tlen >= MIN_STEM_CANDIDATE_LEN) {
      size_t sl;
      const char *stem = ctx->stemmer->Stem(ctx->stemmer->ctx, tok, tlen, &sl);
      if (stem && strncmp(stem, tok, tlen)) {
        t.s = stem;
        t.type = DT_STEM;
        t.len = sl;
        t.fieldId = ctx->fieldId;
        t.stringFreeable = 1;
        if (ctx->tokenFunc(ctx->tokenFuncCtx, &t) != 0) {
          break;
        }
      }
    }
  }

  return pos;
}

char *DefaultNormalize(char *s, size_t *len) {
  char *dst = s, *src = s;
  *len = 0;
  while (*src != '\0') {
    if (isupper(*src)) {
      *dst++ = tolower(*src++);
    } else if (isblank(*src) || iscntrl(*src)) {
      src++;
      continue;
    } else {
      *dst++ = *src++;
    }
    ++(*len);
  }
  *dst = 0;
  return s;
}
