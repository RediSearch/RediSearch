#include "forward_index.h"
#include "stopwords.h"
#include "tokenize.h"
#include "toksep.h"
#include "rmalloc.h"
#include <ctype.h>
#include <stdlib.h>
#include <strings.h>

int tokenize(const char *text, void *ctx, TokenFunc f, Stemmer *s, unsigned int offset,
             StopWordList *stopwords) {
  TokenizerCtx tctx;
  tctx.pos = (char **)&text;
  tctx.tokenFunc = f;
  tctx.tokenFuncCtx = ctx;
  tctx.stemmer = s;
  tctx.lastOffset = offset;
  tctx.stopwords = stopwords;

  return _tokenize(&tctx);
}

// Shortest word which can/should actually be stemmed
#define MIN_STEM_CANDIDATE_LEN 4

// tokenize the text in the context
int _tokenize(TokenizerCtx *ctx) {
  u_int pos = ctx->lastOffset + 1;

  while (*ctx->pos != NULL) {
    // get the next token
    char *tok = toksep(ctx->pos);
    // this means we're at the end
    if (tok == NULL) break;

    // normalize the token
    size_t tlen;
    tok = DefaultNormalize(tok, &tlen);

    // ignore tokens that turn into nothing
    if (tok == NULL || tlen == 0) {
      continue;
    }

    // skip stopwords
    if (StopWordList_Contains(ctx->stopwords, tok, tlen)) {
      continue;
    }
    // create the token struct
    Token t = {.s = tok, .len = tlen, .pos = ++pos, .type = DT_WORD};

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
